# 实际验证记录

## 2026-09-09：快照版

本轮在本机 Windows、Java 17、Maven 环境验证快照压缩、分块传输和副本恢复；2026-09-08 的初版验证历史保留在后文。所有结果均为功能与故障验证，不是性能压测。

### Maven 验证：44 项通过

`mvn -B -ntp -f nexusstore/pom.xml verify`：**BUILD SUCCESS，44 tests，0 failures，0 errors，0 skipped**。

| 模块 | 数量 | 本轮测试组成 |
|---|---:|---|
| nexus-common | 12 | ConsistentHashRingTest 3、RpcTransportTest 9 |
| nexus-raft | 24 | RaftNodeTest 9、RaftSnapshotTest 15 |
| nexus-server | 8 | FileControllerTest 6、NodeRuntimeIntegrationTest 1、SnapshotRuntimeIntegrationTest 1 |

新增 Raft 测试覆盖重复压缩后的绝对日志索引和幂等结果、落后副本分块接收与丢失 ACK 重试、Leader 故障与全体重启、条数/字节自动压缩阈值、已满日志副本先提交已验证前缀再压缩接收、状态容量预留、旧格式迁移、快照边界与尾部保留、过期快照、校验和与 offset 错误、中途重启及损坏快照拒绝加载。

`SnapshotRuntimeIntegrationTest` 使用三个 `NodeRuntime` 和真实 Netty TCP 连接：停一个 follower，在多数派写入 **9 个各 1 MiB 文件**，生成 **大于 8 MiB 单帧上限**的快照；重启副本后确认其本地 `snapshotIndex` 与 `lastApplied` 追平。随后关闭快照发送节点，验证恢复副本参与剩余多数派时九个文件逐字节一致，且覆盖后重试旧请求 ID 不会回滚新值。该测试验证快照分块确实通过 Netty 传输；独立 JVM 故障演示见下一节。

详细测试报告位于各模块 `target/surefire-reports/`；这些目录会被后续构建更新。

### 三进程快照演示：六阶段通过

`scripts/snapshot-test.ps1 -Local` 已完成，过程日志为 `.runtime/snapshot-demo.log`。本轮目标分片为 **s1**，记录如下：

1. 初始值提交后，停止 follower **node1**；其本地 `lastApplied=7`。
2. 建立快照基线后，在剩余多数派对同一临时 key 覆盖 **24 次**；node3 的 `snapshotIndex` 从 **7 推进至 31**，保留日志 **24 → 0 条**，`logBytes` **38,607 → 0**，`snapshotBytes=4,076`。
3. 重启 node1，直接检查其本地状态为 **`snapshotIndex=31`、`lastApplied=31`**，确认完成快照安装。
4. 通过三个 HTTP 节点读取最终值，SHA-256 一致，并检查本地应用位置追平。
5. 停止快照发送节点 **node3**，**node1 当选新 Leader**；剩余两节点仍能多数派确认读取，内容正确。
6. 恢复 node3，三个节点均可读到最新内容；脚本清理本轮唯一临时 key，并保留节点持久化数据。

节点名称和分片随每次运行变化，日志会被重跑覆盖。本次运行日志结尾为 `Snapshot demo passed`。

### 启动诊断与最终回归

首轮冷启动曾在健康检查等待 **120 秒**后超时，启动脚本执行了回滚。原因尚未定位；重启后上述六阶段快照演示通过。没有证据将该次异常归因于 Raft 协议、脚本作用域或其他具体原因。

已在健康检查最终超时信息中保留每个节点最后的 HTTP 状态/异常，并为 Raft fail-closed 增加节点、分片和原因堆栈日志。修改后再次执行完整 `mvn verify`，于本机 11:48 构建成功，仍为 44 项测试全部通过。

随后使用更新后的 JAR，从三个节点全部停止的状态依次运行以下脚本，整体执行退出码为 0：

| 回归 | 结果 | 本地日志 |
|---|---|---|
| `snapshot-test.ps1 -Local` | 冷启动及六阶段全部通过；s2 快照 16 → 40，node1 安装本地快照 40，停止发送者 node2 后 node3 选主并继续读取 | `.runtime/snapshot-cold.log` |
| `smoke-test.ps1` | 4096 字节二进制文件跨节点 SHA-256 一致；删除后 404 | `.runtime/snapshot-smoke.log` |
| `failover-test.ps1` | s3 Leader node1 停止后 node3 选主，旧文件和新写入均正确；再停 node3 后仅 node2 存活，PUT/GET 均为 503；恢复后三个副本应用位置追平，文件可读 | `.runtime/snapshot-failover.log` |

首次超时原因仍未确定；后续冷启动通过不能替代对首次原因的证明。测试结束已停止三个演示进程，并确认 HTTP 8081..8083、RPC 9091..9093 全部释放。现有持久化数据保留；旧格式升级前备份位于 `.runtime/pre-snapshot-upgrade-20260909-105650/`。下方 Nacos 验证属于 2026-09-08 初版历史，本轮使用不依赖 Nacos 的本地 profile。

## 2026-09-08：初版验证历史

验证日期：2026-09-08。环境：本机 Windows 11、OpenJDK 17.0.2、Maven 3.9.11，三个独立 Java 进程；Nacos 使用官方 2.4.3 standalone 包运行。以下是功能与故障验证，不是性能压测。

### 自动化构建

`mvn -B -ntp -f nexusstore/pom.xml verify`：**BUILD SUCCESS，28 tests，0 failures，0 errors**。

| 测试类 | 数量 | 覆盖重点 |
|---|---:|---|
| ConsistentHashRingTest | 3 | 稳定映射、分布、哈希环新增分片的映射变化；后者仅算法测试，不是在线迁移实现 |
| RpcTransportTest | 9 | 实际 TCP RPC、并发、1 MiB 消息、拆包粘包、帧上限、超时、取消、断连重连及过载 |
| RaftNodeTest | 9 | 选主、投票持久化、旧任期提交约束、分区旧 Leader 拒绝读写、尾部修复、重启、幂等、磁盘错误 |
| FileControllerTest | 6 | HTTP 413/404/503；PUT/DELETE 失败保留指定或生成的请求 ID |
| NodeRuntimeIntegrationTest | 1 | 实际 Netty + Raft 的二进制文件、Leader 故障、重试去重、全体重启、删除 |

详细报告由 Maven 生成在各模块 `target/surefire-reports/`，构建输出在 `build.log`。

### 三进程 HTTP 演示

1. `scripts/start-cluster.ps1`：三个进程健康，四个分片均选出 Leader。
2. `scripts/smoke-test.ps1`：从 node1 上传 4096 字节二进制文件，通过 node2 下载 SHA-256 一致；删除后 404。
3. `scripts/failover-test.ps1`：目标分片 s0 原 Leader node1 被强制停止；node3 选主后原文件可读、新写成功；再停止 node3，只剩 node2，PUT 和 GET 都返回 503；恢复后所有副本 lastApplied 追到故障前 commitIndex，确认文件通过每个 HTTP 节点读取一致。
4. Nacos profile 下验证 0 字节、恰好 1,048,576 字节文件上传并跨节点下载，SHA-256 一致；1,048,577 字节返回 413。

脚本中的 nodeId/分片选择随每次选举变化，上面的名字仅记录本次运行。故障脚本没有断言失败写入最终不存在，因为超时写的结果是不确定的。

### Nacos 实测

- 三个节点以 nacos profile 启动成功，Nacos 返回 `NEXUS_DEMO@@nexus-store` 下三个 healthy 实例。
- 实例 HTTP 端口 8081/8082/8083，metadata 的 node-id 与 netty-port（9091/9092/9093）逐一对应。
- `/api/cluster.discoveredPeers` 实际展示三个 peer，证明 DiscoveryClient 查询已接通。
- 停止 Nacos 后再次运行文件 smoke 测试，上传、下载、删除、404 均通过；数据面不依赖注册中心在线。
- 停止集群后 `start-cluster.ps1 -Local` 可恢复不依赖 Nacos 的本地模式。

本次 Nacos 使用本地 Java standalone 运行；`compose.nacos.yml` 提供相同版本容器配置，但本机没有 Docker，因此没有执行 Docker Compose。

## 结论边界

已验证限定场景下的小文件操作、共识安全边界和故障恢复。没有性能提升数字，没有跨机器/机架测试、真实断电实验、Jepsen 验证、动态扩容或生产可用性承诺。全部分片仍三副本复制到相同三台节点，不能据此声称容量水平扩展。
