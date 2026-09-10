# NexusStore · 模拟面试 QA

> 归档：以下是实施前参考问答，不能作为已完成工作或实际压测结果复述。当前代码问答见 ../interview.md。

> 配套《NexusStore-技术设计.md》。按**面试轮次**组织，难度递增。
> 每道题给：**题目 → 考察点 → 参考答案（可直接口述）→ 追问 → 加分/踩坑**。
> 建议先自己口述一遍再看答案，答不上的题号标记后重点复习。

---

# 第 0 轮 · 项目开场（3 分钟定生死）

## Q0-1 「用 3 分钟介绍一下这个项目」

**考察**：结构化表达、点明技术难点与个人贡献。

**参考话术**：

> 这是一个**分布式强一致 KV 与小文件存储原型**，我自己从零设计实现的，代号 NexusStore。
>
> **背景**是我在学习分布式系统时先写了一个简化版分布式文件系统（类似 mini HDFS，用 ZooKeeper 做服务发现和选主、HTTP 做节点通信），能跑通但有三个硬伤：没有分片所以没法水平扩展、HTTP 短连接 QPS 只有两千、副本写入靠串行扇出没有一致性协议。所以我用工业界主流方案做了第二代重构。
>
> **架构**上分三个角色：一个元数据调度节点 PD，负责分片路由和调度；若干存储节点，每个节点承载上千个分片的副本；还有客户端 SDK。数据面用一致性哈希把 key 映射到分片，每个分片是一个独立的 Raft 复制组，3 副本多数派提交，也就是 TiKV 那套 Multi-Raft。
>
> **技术栈**：治理用 SpringCloud + Nacos 做注册发现、健康检查和优雅上下线；数据面不走 HTTP，用 Netty 自己做了一套二进制协议和长连接，全异步、池化、零拷贝；元数据存 MySQL，数据面状态机用 RocksDB；Redis 主要做 PD 路由表的旁路缓存。
>
> **结果**：3 副本 fsync 下单分片写入约 9k QPS、P99 16ms，32 分片并行约 11w QPS；读走 Lease Read，P99 1.8ms。相比第一代的 HTTP 方案写入提升了大概 5 倍。
>
> **我重点负责**的是 Netty 通信层和 Raft 一致性层这两块，也是这个项目里我认为最有价值的部分。

**加分**：说完主动抛钩子——"如果您对 Raft 的实现细节或者 Netty 的优化点感兴趣，我可以展开讲讲。"

---

## Q0-2 「这个项目几个人做的？你负责哪些？」

**考察**：真实性。

**参考**：核心是我一个人设计和编码的（原型项目，历时约 2~3 个月业余时间）。Nacos / MySQL / RocksDB 都是现成组件直接集成，我写的是**分片路由、Raft 协议实现、Netty 协议与通信层、PD 调度器**这几块。压测脚本和监控也是我自己搭的。

**踩坑**：**不要把开源项目说成自己的**。如果面试官追问细节答不上来，比承认"参考了 TiKV 的设计"严重得多。提前准备好"哪些是借鉴、哪些是自己写"的边界。

---

## Q0-3 「遇到的最大技术难题是什么？」

挑一个有深度的讲，推荐"Region 分裂 + Raft 的一致性配合"：

> 印象最深的是**分片分裂**。一开始我以为分裂就是本地把数据切成两半，后来发现不对：分裂是改变状态机的操作，如果只在 Leader 本地切了，副本没切，一旦切主，新 Leader 的分裂状态和别人不一样，数据区间就乱了。正确做法是**把分裂当作一条 Raft 日志提交**，所有副本 apply 时各自做本地分裂，靠 Raft 保证分裂点一致。
>
> 还有第二个坑：分裂完成后客户端缓存的还是旧路由，请求会打到旧 Region，如果服务端不校验就直接处理，数据就写错分片了。所以我引入了 **RegionEpoch（confVer + version）**，请求带 epoch，服务端发现版本落后就返回 `REGION_SPLIT` 让客户端重拉路由。这个是我在压测时发现数据分布对不上才暴露出来的。

**为什么选这题**：能同时体现 Raft 理解、工程严谨性、debug 能力。

---

# 第 1 轮 · Netty 深挖

## Q1-1 Netty 的线程模型？为什么 Boss 只要 1 个线程？

**参考**：
- 主从 Reactor：`BossGroup` 只负责 accept，把连接注册到 `WorkerGroup`；`WorkerGroup` 负责 read/write；另配业务线程池处理阻塞逻辑。
- Boss 一般 1 个线程：一个 ServerSocketChannel 只绑定一个 Selector，accept 极轻量，多线程反而多一次上下文切换和锁竞争。只有监听多个端口时才需要多个 Boss 线程。
- Worker 默认 `CPU * 2`。
- **关键点**：一个 Channel 一旦注册到某个 EventLoop 就**终身绑定**，pipeline 里所有 handler 都由这一个线程串行执行 → **无锁化**，这是 Netty 高性能的核心原因之一，也是为什么 handler 里不能有耗时阻塞操作。

**追问：业务线程池怎么切？**
> 用 `pipeline.addLast(bizGroup, handler)`，Netty 会把这个 handler 及之后的 handler 放到 bizGroup 执行。判断标准：只要 handler 里有 RocksDB 读写、Raft 状态机 apply、同步 RPC 这类阻塞操作，就必须切出去，否则会卡死整条 EventLoop 上的所有连接。

---

## Q1-2 Netty 为什么快？

分层回答，别只说"异步非阻塞"：

1. **I/O 模型**：基于 epoll 的 IO 多路复用（Linux 下可切 `EpollEventLoopGroup` 获得边缘触发），一个线程处理成千上万连接，避免 BIO 的一连接一线程。
2. **无锁串行化**：Channel 绑定 EventLoop，pipeline 串行执行，避免锁竞争。
3. **零拷贝**：池化 DirectBuffer（减少一次堆内→堆外拷贝）、`CompositeByteBuf` 合并 buffer 不复制、`FileRegion` 走 `sendfile` 传文件不进用户态。
4. **内存池化**：`PooledByteBufAllocator` 按 arena/chunk/page 分级管理，避免频繁申请释放和 GC。
5. **高效数据结构**：`HashedWheelTimer`（O(1) 定时任务）、`MpscUnboundedArrayQueue`（多生产者单消费者无锁队列）。
6. **减少系统调用**：写操作合并 flush、`RecvByteBufAllocator` 自适应扩容减少读次数。

---

## Q1-3 什么是 TCP 拆包粘包？Netty 怎么解决？

**参考**：
- TCP 是**字节流协议**，没有消息边界。发送方写入的两个包可能被合并（粘包），或一个大包被拆成多次收到（拆包）。
- Netty 方案：
  - `LengthFieldBasedFrameDecoder`：协议头放长度字段，按长度切帧（**项目里用的这个**，offset=16、长度 4 字节、单包上限 8MB 防 OOM）；
  - `FixedLengthFrameDecoder`：固定长度（浪费空间）；
  - `DelimiterBasedFrameDecoder` / `LineBasedFrameDecoder`：分隔符（需转义，正文含分隔符就麻烦）。

**追问：为什么不用分隔符？**
> 正文里可能出现与分隔符相同的字节，需要转义，既麻烦又容易出 bug；长度字段方案解析确定、性能最好，二进制协议基本都用它（Dubbo、gRPC、Redis RESP 都是长度/类型前缀思路）。

---

## Q1-4 异步模型下，响应怎么和请求对应上？

**参考**：
1. 每个请求分配全局唯一 `requestId`（`AtomicLong` 自增 + 节点 ID 前缀，避免多连接冲突）；
2. 发送前 `PENDING.put(requestId, RequestFuture)`，`RequestFuture` 继承 `CompletableFuture`；通过 `writeAndFlush().addListener()` 在发送失败时移除并异常完成；
3. 响应回来，dispatcher 用 `PENDING.remove(resp.requestId)` 取出并 `complete(response)`；
4. 超时用 **HashedWheelTimer** 扫描（tick 100ms）。为什么不用 `ScheduledExecutorService`：后者在海量超时任务下是 O(log n) 堆操作，时间轮是 O(1)，且精度要求不高（100ms 误差可接受）。

**追问：PENDING 一直增长怎么办？**
> 每个请求必然走"正常完成 / 异常完成 / 超时"三条路之一，都会 remove。另外要监控 `PENDING.size()`，持续增长说明有漏 remove 的路径（比如连接断开时未清理）。我在压测时加过这个指标，抓到过一次 bug。

---

## Q1-5 你的私有协议是怎么设计的？

画得出来才算会：

```
+--------+--------+--------+--------+--------+--------+
| magic  |version | codec  | type   | flags  | status |   12B 定长头
| 4B     | 1B     | 1B     | 1B     | 1B     | 1B     |
+--------+--------+--------+--------+--------+--------+
| requestId (8B)            | bodyLength (4B)         |
+---------------------------+-------------------------+
| body (Protobuf / 原始字节)                           |
+-----------------------------------------------------+
```

- **magic**（4B）：快速识别非法数据，端口被扫/错连时直接丢弃，不进入解析流程（防脏数据导致 OOM）；
- **version**：协议升级兼容；
- **codec**：序列化算法（0=Protobuf，1=raw bytes，2=JSON 调试用）；
- **type**：`HEARTBEAT / PUT / GET / RAFT_APPEND / RAFT_VOTE / SNAPSHOT / NOT_LEADER`；
- **requestId**：异步关联；
- **status**：错误码，其中 `NOT_LEADER`、`REGION_SPLIT`、`EPOCH_STALE` 是给**客户端路由纠错**用的。

---

## Q1-6 讲讲 Netty 的零拷贝？

**OS 层**：
- `FileRegion` 包装 `FileChannel.transferTo` → Linux `sendfile`，数据从磁盘经 DMA 直接到网卡缓冲区，**不经过用户态**，省 2 次 CPU 拷贝和 2 次上下文切换（传统 read+write 是 4 次拷贝 4 次切换）。

**Netty 层（减少用户态内拷贝）**：
- `CompositeByteBuf`：把协议头和 body 逻辑聚合，避免合并时的内存复制；
- `ByteBuf.slice()` / `duplicate()` / `retainedSlice()`：共享底层内存，只复制指针；
- 池化 **DirectBuffer**：读写 socket 时省一次堆内→堆外拷贝（JVM 要求 socket 读写必须用堆外内存，堆内存会临时拷贝）。

**追问：DirectBuffer 有什么坑？**
> 不受 GC 直接管理（只有 Cleaner 兜底），靠**引用计数**释放。忘 release → 堆外内存泄漏，表现为进程 RSS 一直涨但堆内很干净。措施：① 用 `SimpleChannelInboundHandler`（自动 release）；② `ReferenceCountUtil.release()`；③ 压测期开 `-Dio.netty.leakDetection.level=PARANOID`；④ 监控 `PlatformDependent.usedDirectMemory()`。

---

## Q1-7 怎么做背压/流控？为什么要做？

- **原因**：网络收包速度 >> RocksDB 写入速度。若无限堆积任务，队列吃满堆内存 → OOM，且延迟不可控。
- **三层防护**：
  1. Netty 水位：`setWriteBufferWaterMark(32MB, 64MB)`，超过高水位 `channel.isWritable()` 变 false，监听 `channelWritabilityChanged` 暂停从队列取任务；
  2. 应用层有界队列 `ArrayBlockingQueue(50000)` + `Semaphore(10000)` 限制在途请求；
  3. 快速失败：超限直接返回 `SERVER_BUSY`，让客户端退避重试，而不是无限排队。
- **为什么不用无界队列**：无界队列把"过载"变成"延迟飙升 + OOM"，有界队列把过载暴露成对端可感知的错误，前者更难排查。

---

## Q1-8 心跳怎么做的？和 Raft 心跳有什么区别？

- **连接层心跳**：客户端 10s 发 `HEARTBEAT`，服务端 `IdleStateHandler(0, 0, 60)` 检测 60s 无读则关闭连接。目的是**探活 TCP 连接**（防半开连接、NAT 老化），顺便回收僵尸连接。
- **Raft 心跳**：Leader 每 100ms 发空的 `AppendEntries`。目的是**维持 leadership**（Follower 收到就重置选举超时），顺带携带 `leaderCommit` 推进 Follower 提交。
- **两者职责完全不同，不能互相替代**：TCP 心跳慢（秒级）且不带状态；Raft 心跳快（百毫秒级）且携带一致性信息。
- 重连用**指数退避 + 抖动**（100ms → 5s 上限，加随机抖动），防止节点重启后所有客户端同时重连造成**重连风暴**。

---

# 第 2 轮 · 一致性哈希深挖

## Q2-1 什么是一致性哈希？解决了什么问题？

- 传统取模 `hash(key) % N`：节点数从 N 变 N+1 时，**几乎全部 key 的映射都变**，命中率只有 1/(N+1)，扩容时缓存全部失效、数据全量迁移。
- 一致性哈希：把节点和 key 都哈希到 `[0, 2^32)` 的**环**上，key 顺时针找到的第一个节点就是归属节点。
- 效果：新增一个节点，只影响该节点逆时针方向到上一个节点之间的 key，**迁移量 K/(N+1)**，其余 K·N/(N+1) 不动。
- 实现：`TreeMap<Long, VNode>` + `ceilingEntry(hash)`，O(log N)；取不到就 `firstEntry()` 环回绕。

---

## Q2-2 什么是虚拟节点？解决什么问题？数量多少合适？

- 定义：一个物理节点在环上映射成多个虚拟节点（key = `nodeId#i`，i ∈ [0, V)）。
- 解决两个问题：
  1. **数据倾斜**：节点少时环上分布极不均匀（实测 3 节点 1 虚拟节点，最大/最小负载能差 3~5 倍）；
  2. **异构权重**：高配机器分配更多虚拟节点 = 承担更多流量。
- 数量：经验值 **150~200/节点**。我实测：100 个时标准差约 8%，200 个约 4%，再往上收益递减但内存和构建成本线性增加。
- 哈希函数用 **MurmurHash3**：比 MD5 快约 10 倍，分布均匀；不用 `String.hashCode`（碰撞率高、分布差）。

---

## Q2-3 【重点】为什么你的哈希环上放的是「分片」而不是「节点」？

这题是拉开差距的关键。

> 朴素一致性哈希是 key → node 直接映射，有三个问题：
> 1. 节点上下线，它负责的那批 key 要**真实搬运数据**，迁移量与数据量成正比，扩容很慢；
> 2. 副本放置不可控，做不了机架感知；
> 3. 节点数一变，环上位置全变，客户端缓存大量失效。
>
> 我的做法是**两层映射**：
> ```
> key --hash--> Region(虚拟分片) --PD 映射表--> Raft Group [nodeA, nodeB, nodeC]
> ```
> 好处：
> - **节点上下线不搬 key**。只需改 Region 的副本归属（PD 改一行 + Raft 成员变更），数据由 Raft 快照/日志自动同步，业务侧零搬运；
> - **扩缩容时 key→Region 映射完全不变**，客户端路由缓存几乎零失效；
> - **副本位置完全可控**，PD 可按 zone/rack 精确调度三副本。
>
> 一句话：**一致性哈希负责"确定性地把 key 钉在某个分片上"，PD 负责"这个分片放在哪几台机器上"。路由稳定与负载均衡解耦。**

**追问：这不就是 Redis Cluster 的哈希槽吗？**
> 思想同源，都是"虚拟分片作为中间层"。区别：① Redis Cluster 是固定 16384 槽，我的 Region 是**可分裂的动态区间**，能按数据量自适应；② Redis Cluster 是异步主从复制（**不保证强一致**，切主可能丢数据），我每个 Region 是 **Raft 组，强一致**；③ Redis Cluster 的槽迁移是业务侧逐 key 搬运（`MIGRATE`），我的迁移靠 Raft 成员变更自动同步。

---

## Q2-4 一致性哈希解决不了什么问题？热点 key 怎么办？

- 一致性哈希只解决"**数据分布均匀**"，解决不了"**访问不均匀**"。某个 key 被高频写，它必然只落在一个 Region，热到一定程度会打爆单个 Leader。
- 方案：
  1. **识别**：客户端/服务端按 key 前缀做滑动窗口计数，超阈值上报 PD；
  2. **读热点**：开启 Follower Read（但要 ReadIndex 保证线性一致）；
  3. **写热点**：计数类场景在客户端本地聚合/批量合并再写；
  4. **终极方案：key 打散**——给热点 key 加随机后缀（如 `key_0..7`）写到不同 Region，读时并发查 8 个再聚合。
- **注意**：打散后同一逻辑 key 的多个分片**不再有跨分片事务保证**，只适用于无强一致要求的计数/点赞类场景。

---

## Q2-5 路由缓存失效怎么办？怎么防止写错分片？

**RegionEpoch = (confVer, version)**：
- `version` 变 = Region 分裂/合并；`confVer` 变 = 副本成员变更。
- 请求带 `(regionId, epoch)`，服务端比对：

| 情况 | 返回 |
| --- | --- |
| epoch 匹配 | 正常处理 |
| version 落后（已分裂） | `REGION_SPLIT` + 新 Region 列表 |
| confVer 落后（成员变了/换主） | `NOT_LEADER` + 新 leader 地址 |
| epoch 超前 | `EPOCH_STALE`，客户端强制重拉 |

- 客户端收到这三类错误 → 标记路由失效 → 向 PD 重拉 → 重试（最多 3 次，指数退避 + jitter）。
- **没有 epoch 的后果**：Region 分裂后客户端拿旧路由请求，服务端若不校验就会把 key 写到错误区间 → **静默数据错误**，比宕机还严重。

---

## Q2-6 分片分裂的流程？为什么要走 Raft 日志？

1. Leader 采样 SST 估算中点作为 `splitKey`（保证两半数据量均衡，而不是简单取 key 中值）；
2. **把 Split 作为一条 Raft 日志提交**；
3. 各副本 apply 时本地执行 RocksDB CF 分裂，生成新 Region，复用已有数据（**无需拷贝**）；
4. 新 Region 在原副本集合上组建自己的 Raft Group；
5. Leader 上报 PD：`ns_region` 分裂 + `version++`。

**为什么必须走 Raft**：分裂是对状态机的**变更操作**，必须和读写一样经过一致性协议。只在 Leader 本地切 → 副本分裂点不一致 → 切主后区间错乱 → 数据丢失。

**追问：分裂期间能读写吗？**
> 能。分裂只是本地操作 + 一条日志，耗时毫秒级，期间只在 apply split 日志的瞬间对该 Region 短暂串行，不影响其他 Region。

---

# 第 3 轮 · Raft 深挖（最难，题最多）

## Q3-1 Raft 是什么？和 Paxos 有什么区别？为什么选 Raft？

- Raft 是一种**管理复制日志的一致性算法**，让一组机器对"操作序列"达成一致，从而让状态机得到相同结果。
- 与 Paxos 的核心区别：**Raft 强 Leader + 日志连续性**。Paxos 允许日志有空洞、多 Proposer 并存（Multi-Paxos 才引入 Leader），工程实现极复杂，论文与工程实现差距大（Google Chubby 有著名吐槽）。Raft 的做法：
  - 问题分解（领导选举 / 日志复制 / 安全性）；
  - 强制日志连续（只能 append + 截断追加），减少状态空间；
  - 强 Leader，日志只能从 Leader 流向 Follower。
- **为什么选**：可理解性 + 工程可实现性。工业界事实标准：etcd、TiKV、Consul、Nacos(JRaft)、Kafka(KRaft)、CockroachDB 全部是 Raft 系。
- 与 **ZAB**（ZooKeeper）区别：ZAB 是主备模式 + 原子广播，不保证日志连续（靠发现+同步阶段补齐）、依赖 ZK session；Raft 的成员变更和快照模型更清晰，更适合"每个分片一组"的场景。

---

## Q3-2 讲讲 Raft 的三种角色和选举流程

- 角色：Leader / Follower / Candidate。
- **任期 term** 是逻辑时钟，单调递增，用于识别过期信息（收到更高 term 立即退为 Follower）。
- 流程：
  1. Follower 维护**随机选举超时**（150~300ms）；
  2. 超时未收到心跳 → 转 Candidate：`term++`、投自己、并行发 `RequestVote(term, candidateId, lastLogIndex, lastLogTerm)`；
  3. 拿到**多数派选票** → 成为 Leader，立即发心跳宣示；
  4. 收到更高 term → 退回 Follower；
  5. 平票 → 超时后 term++ 重新选举。
- **为什么要随机超时**：避免多个 Follower 同时超时瓜分选票导致反复选不出 Leader，随机让它们**错峰**。

---

## Q3-3 【必考】投票时为什么要比较日志新旧？不比较会怎样？

- 投票规则：候选人日志必须**至少和我一样新**才投票。比较 `(lastLogTerm, lastLogIndex)`：**lastLogTerm 大的更新；相等则 lastLogIndex 大的更新**。
- **为什么**：这条限制是 **Leader 完备性**（已提交日志一定出现在后续所有 Leader 中）的证明基础。
- **证明思路（面试要能讲）**：
  > 一条日志被提交 ⇒ 它被**多数派**复制了。
  > 新 Leader 要当选 ⇒ 拿到**多数派**选票。
  > 两个多数派**必有交集** ⇒ 交集里至少有一个节点持有这条已提交日志。
  > 该节点只会把票投给"日志不比自己旧"的候选人 ⇒ **新 Leader 必然持有这条日志**。
- **不比较的后果**：落后节点可能当选，用自己的日志覆盖其他副本 → **已提交（已响应客户端成功）的数据丢失**，直接违反线性一致。

---

## Q3-4 【必考】日志复制流程？四个索引是什么？

- Leader 收到写请求 → 本地 append（**不提交**）→ 并行 `AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit)`。
- Follower 做**一致性检查**：`log[prevLogIndex].term == prevLogTerm`？
  - 不匹配 → 拒绝，Leader `nextIndex--` 重试（优化：Follower 返回 `conflictTerm + firstIndexOfTerm`，Leader 直接跳过整个 term，避免逐条回退）；
  - 匹配 → **删除该位置之后的所有日志，再追加**（Leader 日志权威）。
- Leader 收到**多数派**成功 → `commitIndex = N` → **apply 到状态机** → 响应客户端。
- 后续 AppendEntries/心跳带 `leaderCommit`，Follower 设 `commitIndex = min(leaderCommit, 自己最后日志 index)`。

| 索引 | 含义 | 持久化 |
| --- | --- | --- |
| `commitIndex` | 已知被多数派复制的最大日志 index（可以 apply 的上界） | 内存 |
| `lastApplied` | 已应用到状态机的 index（≤ commitIndex） | 内存 |
| `nextIndex[i]` | 给 Follower i 下一条要发的日志（Leader 内存，乐观估计，失败递减） | 内存 |
| `matchIndex[i]` | Follower i 已复制的最大 index（用于计算 commit） | 内存 |

**必须持久化的是**：`currentTerm`、`votedFor`、`log[]` —— 否则宕机后可能重复投票或丢日志。

---

## Q3-5 【进阶】为什么 Leader 只能提交当前 term 的日志？（Figure 8 问题）

Raft 论文 Figure 8 的反例：
> (a) S1 是 term2 Leader，把日志 2 复制到 S2，还没提交；
> (b) S1 挂，S5 成为 term3 Leader（拿到 S3/S4/S5 的票），写入日志 3，还没复制就挂了；
> (c) S1 重新成为 term4 Leader，把日志 2 复制到 S3，**此时日志 2 被多数派复制了**——如果 S1 就此提交日志 2，然后挂掉；
> (d) S5 又成为 term5 Leader（它的 lastLogTerm=3 比 S2/S3/S4 的 2 新，能拿到多数派票）→ 用 term3 的日志**覆盖**日志 2 → **已提交数据被覆盖**！

- **根因**：只按"多数派复制"计数来提交，无法区分这条日志是否安全。
- **Raft 的解法**：Leader **只能通过计数副本的方式提交当前 term 的日志**；前任 term 的日志只能在当前 term 有新日志被提交时**被间接提交**（日志连续，commitIndex 推进会带着前面的日志一起提交）。
- **工程解法（no-op）**：新 Leader 上任后**立即提交一条空的 no-op 日志**，一旦被多数派确认，前面所有前任日志就被安全间接提交了。附带好处：快速确认 leadership、让状态机追上。

---

## Q3-6 读请求怎么处理？直接读 Leader 不行吗？

**这题极易答错。**

- **直接读 Leader 状态机不行**。原因：如果旧 Leader 被网络隔离，集群已选出新 Leader 并接受了新写入，但旧 Leader 还以为自己是 Leader → 返回**陈旧数据**，违反线性一致。
- **方案一：ReadIndex（项目默认）**
  1. Leader 记录 `readIndex = 当前 commitIndex`；
  2. 向全体发一轮心跳，确认自己仍是合法 Leader（多数派响应 + term 未变）；
  3. 等本地 `lastApplied >= readIndex`；
  4. 读状态机返回。
  代价：一次 RTT。
- **方案二：Lease Read（优化）**
  Leader 在上次心跳得到多数派响应后的一段时间（lease，取**心跳间隔 × 0.9** 或**选举超时 - 时钟漂移余量**）内认定不会有新 Leader → 省掉心跳轮，读性能接近本地读（P99 1.8ms）。
  **风险**：依赖**时钟**，NTP 跳变会破坏 lease。防护：租约时长远小于选举超时、监控时钟偏移、超阈值自动降级回 ReadIndex。
- **方案三：Follower Read**：Follower 向 Leader 要 ReadIndex，等自己 `appliedIndex` 追上再本地读 → 分担读压力，且保证线性一致。

---

## Q3-7 什么是 PreVote？解决什么问题？

- **问题场景**：某节点被网络隔离 → 选举超时 → 不断 `term++` 发起选举（当然都失败）→ term 变得远大于集群。网络恢复后，它的高 term 消息会让**所有节点立刻退为 Follower**，现任 Leader 被"踢下台" → 触发**无谓的重新选举**，集群抖动甚至短暂无主。
- **PreVote**：分两步
  1. **预投票**：Candidate 用 `term+1`（**不真正递增自己的 term、不落盘 votedFor**）发 `PreRequestVote`，问"如果我参选你会投我吗"；
  2. 只有拿到**多数派预投票**，才真正 `term++` 发起正式选举。
- 效果：被孤立节点拿不到多数派预投票，永远不会真正提升 term → 骚扰不到集群。
- **代价**：多一轮 RPC，选举略慢（可接受，正确性优先）。

---

## Q3-8 日志不能无限增长，怎么办？

**日志压缩 + 快照**：
- 触发：日志大小 > 128MB 或 条数 > 100w（可配）。
- 流程：
  1. 对当前状态机做快照（RocksDB `Checkpoint`，**硬链接共享 SST 文件 → 近乎零额外空间、零阻塞**）；
  2. 记录 `lastIncludedIndex` / `lastIncludedTerm`；
  3. 删除快照点之前的日志；
  4. 落后太多的 Follower（所需日志已被压缩）→ 走 **InstallSnapshot RPC**，分块（1MB/块）流式传输。
- **注意**：快照期间仍可正常读写；快照是重 IO 操作，要限速避免影响在线流量。

---

## Q3-9 成员变更怎么做？为什么？

- **单节点变更（Single-node Change）**：一次只增/删**一个**节点。
- **为什么安全**：旧配置 N 的 quorum = ⌊N/2⌋+1，新配置 N±1 的 quorum 与旧 quorum **必然有交集** → 不可能同时存在两个多数派、不可能同时选出两个 Leader。
- 对比 **Joint Consensus**（两阶段：`C_old,new` 共存提交 → 提交 `C_new`）：正确但状态机复杂，工程上少用。
- **关键约束**：① 同一时刻只允许一个变更进行；② 新 Leader 必须**先提交一条 no-op 日志**后才能发起下一次变更。
- 加副本实际流程：**AddLearner（只同步日志不参与投票）→ 追平 → 变 Follower → 删除源端旧副本**。先 Learner 是为了避免"加副本瞬间 quorum 变大导致选不出 Leader"。

---

## Q3-10 什么是 Multi-Raft？为什么这么设计？

- 定义：**每个分片（Region）一个独立的 Raft Group**，各自独立 term、独立选举、独立日志。
- 为什么不是"整个集群一个 Raft 组"：
  1. **吞吐无法扩展**：单 Raft 组写吞吐被单 Leader 的 CPU/磁盘/网络卡死；Multi-Raft 下不同 Region 的 Leader 分散在不同节点，**写压力天然打散**，加机器 = 加吞吐；
  2. **故障域隔离**：一个 Region 选主不影响其他 Region；
  3. **可分片迁移**：独立 Raft 组才能做分裂和调度。
- **代价与优化**：Region 数上千时 RPC 量爆炸 → **心跳合并**：同一 target 节点上所有 Region 的心跳合并成一个 RPC，RPC 量从 O(Region) 降到 O(Node)；不同 Region 并行 apply，同 Region 内严格串行。

---

## Q3-11 3 副本够吗？为什么副本数是奇数？

- N 副本，quorum = ⌊N/2⌋+1。
- 3 副本 → quorum 2 → **容忍 1 节点故障**；
- 4 副本 → quorum 3 → **也只容忍 1 故障**，但多一份存储成本 + 多数派从 2 变 3（写延迟更高）→ **偶数没有意义**；
- 5 副本 → quorum 3 → 容忍 2 故障（金融级常见）。
- 跨机房常用 5 副本（2+2+1）或 3 副本 + 异地 Learner 灾备。

---

## Q3-12 【陷阱】Leader 复制给多数派之后、响应客户端之前挂了，会怎样？

- 新 Leader 会选出，且**一定拥有这条日志**（已被多数派复制，见 Q3-3 证明）。
- 客户端没收到成功响应 → 会**重试**。
- 问题：**重试会产生重复写**。幂等操作（`put(k,v)`）无害；非幂等（`incr(k)`）会重复执行。
- **解法**：请求携带唯一 `requestId`，状态机侧维护 `requestId → result` 去重表（保留最近 N 条 / TTL），重复请求直接返回上次结果。**分布式系统通用要求：客户端重试 + 服务端幂等**。
- 若日志未被多数派复制就挂 → 未提交 → 新 Leader 会截断覆盖 → 客户端收到失败/超时，重试即可，不违反一致性。

---

## Q3-13 【陷阱】网络分区恢复后，旧 Leader 的日志比新的长，怎么办？

- 旧 Leader 恢复后收到新 Leader（更高 term）的 `AppendEntries` → **立即退为 Follower**。
- 它多出来的未提交日志会被**一致性检查发现冲突并截断删除**（prevLogIndex/prevLogTerm 匹配后，删除该位置之后的所有日志再追加）。
- 那些日志从未被多数派确认 → 从未提交 → **丢弃是安全的**。
- 这也解释了为什么**未提交的数据不能返回给客户端成功**。

---

## Q3-14 Raft 的工程性能优化有哪些？

| 优化 | 说明 | 代价 |
| --- | --- | --- |
| **Batch** | 多个请求合并成一条 Raft 日志提交 | 延迟上升（实测 9k → 3.5w QPS，P99 16ms → 40ms） |
| **Pipeline** | 不等上一条 ACK 就发下一条，重叠 RTT | 实现复杂度 |
| **Group Commit** | 多个 append 合并一次 fsync | 整机断电可能多丢一点未 fsync 数据 |
| **并行 Apply** | 不同 Region 并行 apply 状态机 | 需保证同 Region 串行 |
| **心跳合并** | 同 target 所有 Region 心跳合并成一个 RPC | - |
| **异步 fsync** | 写 PageCache 即返回 + 定时 fsync | 掉电可能丢几百 ms 数据 |
| **Leader 均衡调度** | PD 把 Leader 打散到各节点 | - |

**追问：Batch 会影响延迟，怎么权衡？**
> 做成**自适应**：低负载时 batch 窗口为 0（来一条发一条，延迟最低）；QPS 超阈值后逐步增大窗口（默认最大 1ms / 512 条）。低负载保延迟、高负载保吞吐。

---

# 第 4 轮 · CAP 与一致性权衡

## Q4-1 CAP 定理是什么？你的系统选了什么？

- CAP：网络分区（P）发生时，只能在一致性（C）和可用性（A）中二选一。（**注意：不是三选二**，正常情况下 C 和 A 可同时满足，P 是前提条件。）
- 数据面选 **CP**：分区时少数派侧**拒绝读写**（返回 `NOT_LEADER`），多数派侧继续服务。
- **为什么不选 AP**：存储系统核心职责是"存进去的东西不能错、不能丢"。AP 系统（Dynamo/Cassandra）在分区时两侧都能写，恢复后靠向量时钟/最后写入胜利解决冲突，可能**静默丢数据**，对存储不可接受。

---

## Q4-2 【加分】CAP 是不是太粗了？你怎么看？

- CAP 只在**分区发生时**才需要取舍，且只描述二元（可用/不可用），无法描述正常情况的权衡。所以有 **PACELC**：
  - **PAC**：分区时在 A/C 间权衡 → 我选 **C**；
  - **ELC**：**E**lse（正常情况）在 **L**atency 和 **C**onsistency 间权衡 → 我仍为一致性付出延迟（每次写等多数派 fsync ~10ms），用 Batch/Pipeline/Lease Read 降低代价。
- 另外，**CAP 不是整个系统非此即彼，而是按层次、按数据类别分别取舍**：
  - 服务发现（Nacos Naming）→ **AP**，容忍短暂不一致；
  - 配置中心（Nacos CP 模式，内部 JRaft）→ **CP**；
  - 数据面（Multi-Raft）→ **CP**；
  - 路由缓存（客户端/Redis）→ **最终一致 + epoch 校验兜底**。

---

## Q4-3 强一致、弱一致、最终一致、线性一致的区别？

- **最终一致**：停写一段时间后最终收敛（DNS、异步主从）。
- **因果一致**：有因果关系的顺序保证（评论先于回复）。
- **顺序一致**：所有进程看到同一操作顺序，但不保证与真实时间一致。
- **线性一致（Linearizability，最强）**：每个操作看起来在**调用与返回之间的某个瞬间原子生效**，且**与真实时间顺序一致**。
- 我的系统通过 **Raft + ReadIndex** 达到线性一致：写走 Raft 多数派，读走 ReadIndex 保证不返回陈旧值。

---

## Q4-4 少数派分区期间完全不可用，是不是太严格？

- 是**主动选择**。但可通过副本放置降低发生概率：3 副本跨 3 rack / 2 zone，单 rack 故障不触发分区；5 副本（2+2+1）跨机房，容忍单机房故障。
- 也可提供**降级读**：给客户端 `stale_read` 选项（读 Follower 不加 ReadIndex），用于可容忍旧数据的监控/分析查询，明确告知不保证线性一致。**把选择权交给业务，但默认最强**。

---

# 第 5 轮 · SpringCloud / Nacos

## Q5-1 Nacos 的服务注册发现原理？和 Eureka 有什么区别？

- **注册**：实例启动通过 OpenAPI 注册（临时实例写内存 + Distro 同步；持久实例写 Raft）。
- **心跳**：临时实例 5s 一次；服务端 15s 未收到标记不健康，30s 剔除（健康保护阈值防止全部剔除）。
- **发现**：客户端拉取 + **UDP Push 推送变更** + 定时拉取兜底（默认 10s 一次增量）。
- **Nacos vs Eureka**：
  - Nacos 支持 **AP（Distro）/ CP（Raft）切换**，Eureka 只有 AP；
  - Nacos 支持 DNS 协议 + 权重 + 元数据 + 分组 + 命名空间隔离；
  - Nacos 同时是**配置中心**，Eureka 需配合 Config Server；
  - Eureka 1.x 已停更，Nacos 活跃。

## Q5-2 你的项目里 Nacos 具体怎么用的？

要说出"混合协议"这个亮点：
- 服务的 **HTTP 端口**由 Nacos 管理（SpringCloud Discovery）；
- **Netty 数据面端口**通过 **metadata** 透出（`metadata.netty-port`），SDK 拉到实例列表后读 metadata 建长连接；
- `zone` 也放 metadata，供 PD 做机架感知调度；
- 配置中心放动态参数：Region 分裂阈值、心跳间隔、选举超时区间、Raft 批大小、快照阈值；
- 自定义 `HealthIndicator` 上报 leader 数、apply lag、磁盘水位；
- **优雅下线**：先 deregister，再主动把本节点所有 Region 的 leadership transfer 出去（避免触发选举抖动），最后关连接。

## Q5-3 Nacos 是 CP 还是 AP？

**都支持，按实例类型区分**：
- **临时实例**（`ephemeral: true`，默认，客户端心跳保活）→ **AP**，用 Nacos 自研 **Distro 协议**（类 Gossip，每节点负责部分数据，最终一致）；
- **持久实例**（`ephemeral: false`，服务端主动探测）→ **CP**，用 **JRaft** 保证一致。
- 我们用的是**临时实例（AP）**：服务发现短暂不一致只影响"新节点晚一点被发现"，可接受；注册中心自身故障时也要保证已有调用不中断。

## Q5-4 健康检查有几层？为什么 Raft 心跳比 Nacos 更快？

- 三层：① Nacos 客户端心跳（5s/15s/30s）；② Nacos 服务端 TCP/HTTP 主动探测；③ 自定义 HealthIndicator（leader 数、apply lag、磁盘）。
- **故障转移主链路是 Raft 那条**：Raft 心跳 100ms、选举超时 150~300ms → **~1s 内选出新 Leader**；Nacos 剔除要 30s，慢得多。
- **两条通道互为备份**：Raft 管"数据面能不能切换"，Nacos 管"新流量会不会打到坏节点"。

---

# 第 6 轮 · 存储与系统工程

## Q6-1 为什么数据面用 RocksDB（LSM）而不是 MySQL（B+Tree）？

- **LSM 优势**：写是**顺序写**（WAL + MemTable + 后台 Compaction 合并成 SST），随机写吞吐比 B+Tree 高一个数量级；SST 只读有序，压缩率高（前缀压缩 + zstd）；天然支持快照（SST 不可变 → Checkpoint 用硬链接，O(1)）。
- **LSM 代价**：
  - **读放大**：多层 SST 多次 IO → **Bloom Filter** + **Block Cache** 缓解；
  - **写放大**：Compaction 重复写 → leveled compaction + 限速；
  - **空间放大**：旧版本未回收。
- **B+Tree 适用**：读多写少、要事务和复杂 SQL —— 这正是 PD 元数据用 MySQL 的原因。

## Q6-2 MySQL 在你的项目里存什么？为什么不存数据？

- 存 **PD 侧元数据**：`ns_store_node`（节点）、`ns_region`（分片区间）、`ns_region_peer`（副本角色）、`ns_operator`（调度审计）。
- 特点：QPS 低（只有调度和客户端重拉路由时读）、要事务（分裂同时改 3 张表）、要 SQL 查询（运维排查）。
- **不存数据**：数据面 QPS 十万级、要水平扩展、要顺序写 —— MySQL 单机 B+Tree 随机写扛不住，也不好分片。

## Q6-3 如果要支持大文件（比如 1GB）怎么办？

- 服务端**自动分块**（如 64MB/chunk），每个 chunk 用内容哈希（SHA-256）作为 key 存进 KV；文件元数据记录 `chunkHash[]` 有序列表。
- 好处：**秒传**（内容寻址去重）、**断点续传**（按 chunk 记进度）、**并行上传/下载**。
- 传输层：chunk 走 `FileRegion` + `sendfile` **零拷贝**，1GB 文件不进 JVM 堆。
- 对比 minFS：整文件单块写会把内存和网络打满，且不便于并行。

## Q6-4 跨分片事务怎么做？

**诚实回答 + 给出方案**：
- **现状**：原型只保证**单 key 原子**（同一 Region，天然由 Raft 保证），不支持跨分片事务。
- **方案（TiDB/Percolator 模型）**：
  1. PD 提供**全局 TSO**（时间戳发号器，保证单调递增）；
  2. **Prewrite**：协调者对所有参与 Region 写入 `lock + data`，选一个作为 **Primary**；
  3. **Commit**：先提交 Primary（原子性的关键点），再异步提交 Secondaries；
  4. **冲突处理**：读时遇到未提交 lock，查 Primary 状态决定回滚还是推进提交（**由 Primary 的提交状态作全局仲裁**）；
  5. 底层依赖每个 Region 的 Raft 保证单个 lock/commit 操作的持久化与一致。
- 简化替代：业务容忍时可用**补偿事务（TCC/Saga）**。

---

# 第 7 轮 · 压力面 / 真实性追问

## Q7-1 「这些性能数据你怎么测的？真实吗？」

讲方法论，别硬撑：
- 环境：3 台 8C16G 云主机、千兆内网、SSD、堆 8G/G1；
- 压测客户端**独立部署**（否则客户端先被打满：端口耗尽、`CLOSE_WAIT`、GC）；
- 压测前**预热 5 分钟**（JIT、RocksDB Compaction、连接池、页缓存）；
- 看 **P99/P999 而不是均值**，均值会掩盖 fsync 毛刺；
- 同时观测 `iostat -x`（磁盘 util）、网络带宽、GC 次数、RocksDB pending compaction bytes；
- **单变量对比**：先关 Raft 测纯 RocksDB 上限，再开 Raft 看一致性代价，最后加分片看扩展比。
- 诚实补充："这是原型环境的数字，生产还要考虑混部、网络抖动、更大数据集导致 Cache 命中率下降。"

**加分**：主动说出数字的**前提条件**和**偏差来源**，比报一个漂亮数字更可信。

## Q7-2 「Raft 你是自己实现的还是用了现成库？」

**诚实为上**：
- "核心状态机（选举、日志复制、快照、成员变更）是我**自己按论文实现的**，目的是真正理解它；RocksDB、Netty、Nacos 这些基础设施是集成的。"
- 补一句："如果是生产，我会直接用成熟的 SOFAJRaft / etcd raft 库，自己实现的主要价值在于理解边界条件和调试过程。"
- **不要**把 SOFAJRaft 说成自己写的——面试官只要问一个 no-op 或 PreVote 的实现细节就会露馅。

## Q7-3 「你这套和直接用 TiKV 有什么区别？为什么不用现成的？」

- 目标不同：TiKV 是生产级分布式事务 KV，几十万行 Rust 代码；我这个是**学习/验证性质的原型**，目标是跑通"分片路由 + 一致性 + 高性能通信"这条链路并可观测。
- 差距：无分布式事务、无完善热点治理、PD 单实例、无跨机房多活、无完善备份恢复。
- 价值：完整经历过设计权衡和 debug 过程，对每个取舍的代价有体感（为什么分裂要走 Raft 日志、为什么读不能直接读 Leader）——这些是直接用现成组件学不到的。

## Q7-4 「如果现在让你重做，你会改什么？」

挑 3 条，体现反思能力：
1. **PD 高可用**：现在 PD 单实例 + MySQL 主备，是潜在单点。应该让 PD 也组 Raft，或元数据放 etcd。
2. **热点治理前置**：一开始只考虑数据分布均匀，没考虑访问热点，压测时才发现单 key 热点会把一个 Region 打爆。应该设计阶段就埋好热点统计和自动打散。
3. **可观测性后置了**：Raft term 变化、apply lag、选举次数、Region 分布这些指标是后来才补的，导致前期排查问题很痛苦。应该**先建监控再写代码**。
4. （备选）**协议设计**：magic + version 一开始没加，调试时被错连的脏数据坑过一次。

---

# 第 8 轮 · 快问快答（进场前抢记）

| 问题 | 一句话答案 |
| --- | --- |
| Raft 一个 term 最多几个 Leader？ | 1 个（选举安全） |
| 已提交日志会被覆盖吗？ | 不会（Leader 完备性，靠投票时的日志新旧比较） |
| 新 Leader 上任第一件事？ | 提交一条 **no-op** 日志 |
| 为什么选举超时要随机？ | 错峰，避免选票瓜分导致反复选不出 |
| 3 副本 quorum 是几？容忍几台故障？ | 2；容忍 1 台 |
| 4 副本为什么没意义？ | quorum 3，也只容忍 1 台，却多一份成本 |
| Follower 日志冲突怎么处理？ | 找到冲突点，**截断**之后所有日志再追加 |
| 读 Leader 为什么可能读到旧数据？ | 该 Leader 可能已被隔离（stale leader），集群已有新 Leader |
| ReadIndex 比 Lease Read 慢在哪？ | 多一轮心跳 RTT；Lease 靠时钟省掉它 |
| 一致性哈希扩容迁移多少数据？ | K/(N+1)（取模是 N/(N+1)） |
| 虚拟节点解决什么？ | 数据倾斜 + 节点权重 |
| 哈希环上放什么？ | 放 **Region（分片）**，不是节点 |
| Netty Boss 为什么 1 个线程？ | 只做 accept，一个端口绑一个 Selector，多线程反而增加切换 |
| 一个 Channel 绑定几个 EventLoop？ | 1 个，终身绑定 → pipeline 串行无锁 |
| 拆包粘包怎么解？ | 长度字段 + `LengthFieldBasedFrameDecoder` |
| 异步响应怎么关联请求？ | requestId + PENDING Map + 时间轮超时 |
| DirectBuffer 不 release 会怎样？ | 堆外内存泄漏（RSS 涨、堆内正常） |
| 背压不做会怎样？ | 任务堆积 → 延迟飙升 → OOM |
| Nacos 临时实例是 CP 还是 AP？ | AP（Distro 协议） |
| Raft 怎么防脑裂？ | 多数派 quorum，少数派选不出 Leader |
| LSM 的三大放大？ | 读放大 / 写放大 / 空间放大 |
| 单 key 热点一致性哈希能解决吗？ | 不能，需要 key 打散或 Follower Read |
| 为什么分裂要走 Raft 日志？ | 分裂是状态机变更，副本间必须一致，否则切主后区间错乱 |
| PreVote 解决什么？ | 防止被隔离节点用高 term 骚扰集群 |

---

# 第 9 轮 · 反问面试官（准备 3 个）

1. "贵司的存储/中间件团队目前主要用自研还是开源方案？如果是自研，一致性协议这块选的是什么？"
2. "团队在 CAP 上一般怎么取舍？有没有遇到过因为一致性级别选择导致的线上问题？"
3. "这个岗位对新人的前 3 个月期望是什么？您觉得做好这份工作最需要补的是哪块能力？"

---

## 附：30 秒复习清单（进场前看一遍）

```
1. 三层映射：key --一致性哈希--> Region --PD--> RaftGroup(3 副本)
2. RegionEpoch = confVer + version，防 stale 路由写错分片
3. 环上放 Region 不放 Node（与 Redis Cluster 槽同源；但 Region 可分裂 + Raft 强一致）
4. Netty：Boss1 / Worker2N / Biz 池；长度字段切帧；requestId + PENDING + 时间轮；
          DirectBuffer 引用计数；水位背压
5. Raft：term + 随机超时 + 日志新旧比较（安全性根基）+ 多数派提交
         + no-op + PreVote + ReadIndex + 快照 + 单节点变更 + Multi-Raft
6. 选 CP：少数派拒绝服务；分层 CAP（Nacos AP / 数据面 CP）；PACELC
7. 数字：单 Region 写 9k QPS、P99 16ms；32 Region 并行 11w；Lease Read P99 1.8ms；
         对比一代 HTTP 方案 2.3k QPS / P99 120ms
8. 三个故事：分裂要走 Raft 日志 / epoch 防写错分片 / 堆外内存泄漏排查
```
