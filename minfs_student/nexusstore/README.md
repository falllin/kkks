# NexusStore：面试用分布式小文件 demo

Java 17 + Maven，三个相同的进程组成一个集群。上传、下载、覆盖、删除小文件；一致性哈希路由到固定分片，每个分片运行一组手写 Raft，通过 Netty 复制到三个节点。支持快照、日志压缩和落后副本的分块恢复。只需 JDK/Maven 就能运行，Nacos 可单独启用。

## 先跑起来（Windows PowerShell）

在本目录执行。原仓库根 `pom.xml` 是旧 minFS；**请使用本目录的 `pom.xml`**。

```powershell
mvn -B verify
.\scripts\start-cluster.ps1
.\scripts\smoke-test.ps1
.\scripts\failover-test.ps1
.\scripts\snapshot-test.ps1
.\scripts\stop-cluster.ps1
```

如果本机脚本执行策略禁止运行，可对单次命令使用 `powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\start-cluster.ps1`。脚本不会修改系统执行策略。首次 Maven 下载依赖需要联网。也可 `.\scripts\start-cluster.ps1 -Build` 先构建再启动。

启动脚本在后台启动三个 Java 进程，HTTP 为 8081/8082/8083，Netty 为 9091/9092/9093。日志和 PID 在 `.runtime/`，持久化数据在 `data/node1` 等目录；停止不删数据。端口被其他程序占用会拒绝启动。默认只监听本机回环地址。

```powershell
# 使用 curl.exe 避免 PowerShell 的 curl 别名
[IO.File]::WriteAllText((Join-Path $PWD 'hello.txt'), 'hello NexusStore')
curl.exe -X PUT "http://127.0.0.1:8081/api/files?key=/demo/hello.txt" -H "Content-Type: application/octet-stream" -H "X-Request-Id: hello-v1" --data-binary "@hello.txt"
curl.exe "http://127.0.0.1:8082/api/files?key=/demo/hello.txt" -o downloaded.txt
curl.exe "http://127.0.0.1:8081/api/cluster"
curl.exe "http://127.0.0.1:8081/api/route?key=/demo/hello.txt"
curl.exe -X DELETE "http://127.0.0.1:8083/api/files?key=/demo/hello.txt" -H "X-Request-Id: delete-hello-v1"
```

也可以手动开三个终端（其余配置有默认值）：

```powershell
java -jar nexus-server/target/nexus-server-1.0.0.jar --server.port=8081 --nexus.node-id=node1 --nexus.rpc-port=9091 --nexus.data-dir=./data/node1
java -jar nexus-server/target/nexus-server-1.0.0.jar --server.port=8082 --nexus.node-id=node2 --nexus.rpc-port=9092 --nexus.data-dir=./data/node2
java -jar nexus-server/target/nexus-server-1.0.0.jar --server.port=8083 --nexus.node-id=node3 --nexus.rpc-port=9093 --nexus.data-dir=./data/node3
```

手动启动的进程由对应终端管理，脚本不会接管/终止它们。Linux/macOS 使用相同 Maven/Java 命令即可。

## 故障演示做了什么

`smoke-test.ps1` 验证二进制上传、从另一个节点下载并比较 SHA-256、删除后读到 404。

`failover-test.ps1` 会实际终止本项目启动的节点进程：

1. 写入文件，找到该文件分片的 Leader。
2. 停掉 Leader，等待剩余两个节点选主，验证原文件可读且能继续写。
3. 再停掉新 Leader，只留一个节点；读写都应返回 503。
4. 重启两个节点，检查日志追平、原文件与多数派阶段的新文件仍可读。

脚本最终恢复三节点，不会删除数据。失败超时的写入**可能已提交，也可能随后提交**，不能把 503 理解成事务回滚；读不到返回值与状态机未执行是两回事。

## API

| 方法 | 地址 | 行为 |
|---|---|---|
| PUT | `/api/files?key=...` | 原始字节上传/整文件覆盖，最大 1 MiB |
| GET | `/api/files?key=...` | 多数派确认后返回字节；不存在 404；返回 SHA-256 ETag |
| DELETE | `/api/files?key=...` | 删除；不存在也返回 200，`deleted=false` |
| GET | `/api/route?key=...` | 查看文件映射到的分片 `s0..s3` |
| GET | `/api/cluster` | 本节点每个分片的 term、role、leader、commitIndex、lastApplied |
| POST | `/api/admin/compact` | 独立压缩当前节点各分片已应用的日志，返回快照和保留日志统计 |
| GET | `/actuator/health` | 本地进程健康；不是多数派可用性证明 |

key 是最多 512 UTF-8 字节的逻辑标识，区分大小写，不做路径规范化，不映射本地磁盘路径。`/a/b` 中的目录仅是命名习惯；没有 mkdir、目录遍历、append 或 POSIX 语义。文件元数据和文件内容在**同一条 Raft 命令中原子更新**，不做大文件分块。

PUT/DELETE 支持 `X-Request-Id`。同一分片中重复的 ID 与相同命令返回第一次提交结果，ID 与不同命令冲突报 400。快照保留命令指纹和第一次执行结果，压缩或重启后仍可去重。建议客户端预先生成 UUID，并在超时后保留它重试同一操作；省略时服务端生成 ID，但响应丢失后客户端无法取回它。GET 每次进行新的多数派确认。

## 快照与日志压缩

默认每个分片累计应用 128 条新日志，或已应用日志达到 4 MiB，就自动生成快照。快照保存当前文件值、写请求去重结果及日志边界，随后释放已应用前缀；后续日志仍使用递增的绝对索引。

`/api/cluster.shards` 新增 `snapshotIndex`、`snapshotTerm`、`retainedLogEntries`、`logBytes`、`snapshotBytes`。`snapshotBytes` 是编码后的快照大小，不是全部内存用量。手动触发示例：

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8081/api/admin/compact | ConvertTo-Json -Depth 6
.\scripts\snapshot-test.ps1
```

快照脚本会停掉一个 Follower，在多数派反复覆盖文件并压缩日志，再恢复离线副本，检查它的**本地快照位置**追平；随后停掉快照发送者，验证恢复节点能参与多数派继续提供正确的文件内容。整个快照通过最多 256 KiB 的块传输，不受单个 8 MiB RPC 帧限制。

原版 `formatVersion=1` 数据可直接加载，新版写入 `formatVersion=2`。升级时先停止三节点并备份整个 `data/`，再统一使用新 JAR；不支持混合新旧协议滚动升级。旧 JAR 不能读取新版状态文件。不要删除现有数据来完成升级。

协议细节与边界见 [快照讲解](docs/snapshots.md)。

## Nacos 可选运行

项目集成 Spring Cloud Alibaba Nacos Discovery，默认关闭。已有 Nacos 2.x 时：

```powershell
$env:NACOS_ADDR = '127.0.0.1:8848'
.\scripts\start-cluster.ps1 -Nacos
```

也可使用 `docker compose -f compose.nacos.yml up -d` 启动本机实验用 Nacos，再运行以上命令。注册服务名 `nexus-store`，group 为 `NEXUS_DEMO`；metadata 带 `node-id` 和 `netty-port`。`/api/cluster.discoveredPeers` 展示从 DiscoveryClient 取得且与固定拓扑匹配的健康实例。需要可达 8848 和 9848 端口。

Raft 自己做心跳和选主。Nacos 下线通知不会删除 Raft 成员，也不会改变哈希环；注册中心不可达时仍使用固定 peer 地址。否则“掉一台机器就把多数派分母变小”会破坏 CP。脚本保留上次使用的 profile；切回不依赖 Nacos 的模式，先停止集群，再执行 `.\scripts\start-cluster.ps1 -Local`。

## 代码阅读顺序

| 文件/模块 | 看什么 |
|---|---|
| `nexus-common/ConsistentHashRing` | SHA-256、虚拟节点、key → 固定分片 |
| `nexus-common/RpcClient`、`RpcServer` | 长连接、requestId、CompletableFuture、4 字节长度帧、超时与背压 |
| `nexus-raft/RaftNode` | RequestVote、AppendEntries、日志冲突、多数派提交、状态机应用 |
| `nexus-raft` 持久化实现、`SnapshotCodec` | term/vote、快照和日志尾部原子落盘，快照安装与恢复 |
| `nexus-server/NodeRuntime` | 多组 Raft、Netty 接线、Leader 路由与重试 |
| `nexus-server/FileController` | HTTP 文件接口及幂等请求编号 |
| `nexus-server/PeerDirectory` | Spring Cloud DiscoveryClient + Nacos |

所有 Java 源码都在各模块 `src/main/java/io/nexusstore/` 下。[面试讲解](docs/interview.md)；[当前技术设计](../NexusStore-技术设计.md)。

[实际验证记录](docs/validation.md) 包含构建、协议测试、真实 Netty 通信、三进程故障演示及 Nacos 注册发现结果。

## 明确边界

- 三节点、默认四分片、每分片三副本。三个节点都保存全部文件；分片用于逻辑路由、独立选主及并行处理，不代表总容量扩展，也不保证 Leader 数平均。
- 文件值随 Raft 日志复制；快照和保留日志封装在同一状态文件，流式写入后 `force` 和原子替换。面向进程崩溃/重启演示，不承诺整机掉电时目录项持久性或拜占庭容错。
- 为减少协议分支，GET 也复制日志。没有 ReadIndex、Lease Read、动态成员变更、自动迁移、RocksDB、PD、Redis/MySQL。订单和缓存场景不在本 demo 中。
- 自动压缩释放已应用日志；未提交日志不能压缩，仍有 64 MiB / 20,000 条上限。每分片的当前文件与永久去重历史另有 64 MiB 编码上限、100,000 条记录上限，容量满会拒绝增加状态的新写请求，快照不提供无限容量。内存开销大于编码大小，各分片还会分别占用资源。
- 不能直接修改已有目录的分片数/成员表。旧副本重启会通过快照和日志追赶恢复，快照不等于成员变更或自动扩容。
- 没有性能提升数字。JSON/base64、整文件入日志、每次重写持久化状态都限制吞吐；“使用 Netty”本身不是“已经证明高性能”。

原版设想已归档到 `docs/archive/`，其中的性能数字和未实现组件不能作为本项目成果。
