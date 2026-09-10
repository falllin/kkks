# NexusStore · 分布式强一致存储原型 技术设计文档

> 归档：以下为实施前设想，包含尚未实现的能力与未经验证的性能数字。当前实现以 nexusstore/README.md 和根目录 NexusStore-技术设计.md 为准。

> **文档定位**：一个可落地的「分布式 KV + 小文件存储」原型，技术栈与简历一致（SpringCloud / Nacos / Redis / Netty / MySQL / Raft）。
> **与 minFS 的关系**：NexusStore 是同一领域问题（分布式文件/对象存储）的**第二代重构**。minFS 用「ZooKeeper + HTTP/RestTemplate + 手写选主 + 无分片」；NexusStore 用「Nacos + Netty 私有协议 + 一致性哈希分片 + Multi-Raft」。
> 本文按 **设计决策 + 技术细节 + 可防御的量化口径** 组织，重点展开：**Netty 高性能异步通信**、**一致性哈希分片与负载均衡**、**Raft / Multi-Raft 强一致**、**CAP 权衡** 四块。

---

## 0. 从 minFS 到 NexusStore：为什么要重构

| 能力 | minFS（一代） | NexusStore（二代） | 重构动因 |
| --- | --- | --- | --- |
| 服务注册发现 | ZooKeeper 临时节点 + 手写 | **Nacos**（注册 + 配置 + 健康检查） | ZK 只有原语没有治理；Nacos 自带权重、灰度、健康探测、控制台 |
| 元数据存储 | ZK 单节点 JSON（1MB 上限） | **MySQL（PD 元数据）+ RocksDB（状态机）** | ZK 容量/QPS 天花板太低，撑不住百万级 key |
| 节点间通信 | HTTP + `new RestTemplate()` 短连接 | **Netty 长连接 + 私有二进制协议 + 全异步** | 短连接 + 文本协议 + 无池化，QPS 卡在 2k 量级 |
| 数据分片 | **无分片**，整文件单块 | **一致性哈希虚拟分片（Region）+ Multi-Raft** | 无法水平扩展、无法并行读写、单文件受单盘容量限制 |
| 副本一致性 | metaServer 串行扇出 3 个 DS，失败即降级 | **Raft 日志复制**，多数派提交 | 无一致性协议；写一半宕机留脏数据；副本靠 fsck 最终收敛 |
| 高可用 | 抢占 ZK `/services/master` | **Raft Leader 选举 + 自动故障转移** | 无 fencing / epoch，严格意义上防不住脑裂 |
| 副本放置 | 按已用容量升序取前 3 | 一致性哈希定路由 + **PD 调度器**按 zone/rack 打散 | 三副本可能全在同一机架，机架断电即丢数据 |

一句话：**minFS 解决"能跑通"，NexusStore 解决"能扩展、能容错、能扛并发"**。

---

## 1. 需求与设计目标

### 1.1 功能性需求

- 分布式 KV：`get / put / delete / scan`
- 小文件（≤64MB）对象存储：`putObject / getObject`，分块 + 内容寻址
- 命名空间隔离（多租户）
- 集群拓扑、副本健康度、容量水位查询

### 1.2 非功能性指标（面试要能报出来）

| 指标 | 目标值 | 说明 |
| --- | --- | --- |
| 副本与容错 | 3 副本，容忍 1 节点故障 | 5 副本容忍 2 节点 |
| 一致性级别 | **线性一致（Linearizable）** | 读走 ReadIndex，不是最终一致 |
| 单 Region 写入 QPS | ~9k（3 副本 fsync） | 瓶颈在多数派 `fsync` + RTT |
| 32 Region 并行 | ~11w QPS | Multi-Raft 近线性扩展 |
| 写入 P99 | ~16ms | 对比 minFS HTTP 方案 ~120ms |
| 读 P99 | <2ms（Lease Read 命中） | 省去 ReadIndex 心跳 RTT |
| 单节点长连接 | ≥5w | Netty + 池化 ByteBuf |
| 扩缩容迁移量 | ≈ K/(N+1) | 一致性哈希；取模是 N/(N+1) |
| 选主收敛 | 1.5~3s（含故障检测） | 选举超时 150~300ms 随机化 |
| 可用性 | 99.9% | 少数派分区拒绝读写（CP） |

> **口径**：原型环境 = 3 台 8C16G 云主机、千兆内网、SSD、堆 8G、G1。被追问时按 §10 的方法论复述。

---

## 2. 总体架构

### 2.1 逻辑架构

```
┌────────────────────────────────────────────────────────────────────────┐
│                           Client SDK (Java)                             │
│   路由缓存(Region Table) · 一致性哈希定位 · Netty 连接池 · 重试/故障转移  │
└──────────────┬───────────────────────────────────┬─────────────────────┘
               │ 1.拉路由/心跳 (HTTP,SpringCloud)   │ 2.数据面 (Netty 私有协议)
               ▼                                    ▼
┌────────────────────────────────┐  ┌─────────────────────────────────────┐
│     nexus-pd (元数据/调度)      │  │      nexus-store (存储节点 ×N)        │
│  ┌──────────────────────────┐  │  │  ┌───────────────────────────────┐   │
│  │ Region 路由表(内存+MySQL) │  │  │  │ Netty Server                   │   │
│  │ 一致性哈希环              │  │  │  │  Boss(1) / Worker(2N) / Biz     │   │
│  │ 调度器: 分裂/迁移/均衡     │  │  │  ├───────────────────────────────┤   │
│  │ 节点管理 / 机架感知        │  │  │  │ Raft Group ×M (每Region一组)   │   │
│  └──────────────────────────┘  │  │  │  - Leader/Follower 状态机       │   │
│  Nacos 注册 · Actuator 健康     │  │  │  - Log(顺序写) / Snapshot        │   │
│  Redis 旁路缓存路由表           │  │  ├───────────────────────────────┤   │
└────────────────────────────────┘  │  │ 状态机 RocksDB (LSM)            │   │
                                    │  └───────────────────────────────┘   │
                                    └─────────────────────────────────────┘
               │                                     │
               └──────────►  Nacos (注册+配置+健康检查) ◄────────┘
                             MySQL (PD 元数据持久化)
```

### 2.2 微服务拆分（SpringCloud Alibaba）

| 服务 | 端口 | 职责 | 通信 |
| --- | --- | --- | --- |
| `nexus-pd` | 8080 | Placement Driver：Region 路由表、节点管理、调度（分裂/迁移/均衡/副本补齐） | HTTP（SpringCloud）；与 store 走 Netty 管控通道 |
| `nexus-store` | 9090 | 存储节点：多 Region 副本、Raft 状态机、RocksDB | **Netty 私有协议**（数据面） |
| `nexus-gateway` | 80 | 管理面入口 + HTTP 兼容层 | Spring Cloud Gateway + LoadBalancer |
| `nexus-client` | - | SDK：路由缓存、一致性哈希、Netty 连接池、重试 | - |

**关键设计：控制面与数据面分离**。管理操作（建表、扩缩容、查拓扑）走 HTTP/SpringCloud，享受 Nacos 负载均衡与熔断；**数据面（get/put）绝不走 HTTP**，直连 Region Leader，避免网关成为瓶颈与单点。

### 2.3 分层 CAP（加分点）

| 层 | 组件 | 取舍 | 理由 |
| --- | --- | --- | --- |
| 服务发现 | Nacos Naming | **AP**（Distro） | 短暂不一致 = 新节点晚一点被发现，可接受；注册中心挂了也不能中断已有调用 |
| 配置中心 | Nacos Config（CP 模式，内部 JRaft） | CP | 配置不能丢 |
| **数据面** | Multi-Raft | **CP** | 存储正确性 > 可用性，宁可拒绝也不能返回错数据 |
| 路由缓存 | Client / Redis | 最终一致 + epoch 校验 | 路由 stale 只多一次重定向，epoch 兜底 |

> 面试话术：**"CAP 不是整个系统非此即彼，而是按层次、按数据类别分别取舍，我把 CP 只保留在最需要正确性的数据面。"**

---

## 3. 数据模型与分片模型

### 3.1 Key 编码与 Region 分段

```
物理 key = magic(1B) | namespace(4B) | tableId(8B) | userKey | ts(8B, 倒序)
```

- 定长前缀保证同表 key 字典序相邻 → Region 范围语义清晰，支持 `scan`；
- `ts` 倒序便于 MVCC 取最新版本。

**Region** 是最小调度单位：

```java
public class Region {
    long   id;                 // 全局唯一，PD 分配
    byte[] startKey, endKey;   // [start, end) 左闭右开
    long   confVer;            // Raft 成员变更版本
    long   version;            // 分裂/合并版本
    long   size;               // 近似大小（采样估算）
    List<Peer> peers;          // 3 副本：nodeId + role
    long   leaderNodeId;
}
```

`confVer + version` = **RegionEpoch**，路由正确性的核心（§6.4）。

### 3.2 PD 元数据存储（MySQL）

PD 侧 QPS 低、要事务、要 SQL 运维查询 → MySQL；**数据面不碰 MySQL**。

```sql
CREATE TABLE ns_store_node (
  node_id BIGINT PRIMARY KEY, ip VARCHAR(32), raft_port INT, netty_port INT,
  zone VARCHAR(32), rack VARCHAR(32), capacity BIGINT, used BIGINT,
  state TINYINT,                    -- 0:Up 1:Offline 2:Tombstone
  hb_time DATETIME, version BIGINT DEFAULT 0   -- 乐观锁
) ENGINE=InnoDB;

CREATE TABLE ns_region (
  region_id BIGINT PRIMARY KEY, start_key VARBINARY(512), end_key VARBINARY(512),
  group_id BIGINT, conf_ver BIGINT, version BIGINT, size_bytes BIGINT, state TINYINT,
  KEY idx_range (start_key, end_key)
) ENGINE=InnoDB;

CREATE TABLE ns_region_peer (
  region_id BIGINT, node_id BIGINT, role TINYINT,  -- 1:Leader 2:Follower 3:Learner
  PRIMARY KEY (region_id, node_id)
) ENGINE=InnoDB;

CREATE TABLE ns_operator (          -- 调度审计 + 幂等
  op_id BIGINT PRIMARY KEY AUTO_INCREMENT, region_id BIGINT,
  kind VARCHAR(32), payload JSON, status TINYINT, gmt_create DATETIME
) ENGINE=InnoDB;
```

### 3.3 数据面存储引擎：RocksDB（LSM-Tree）

每 Region 一个 Column Family，Raft log 与状态机分离：

```
data/
├── raftlog/     # 顺序写 segment + 索引
├── kvdata/      # 状态机（RocksDB）
└── snapshot/
```

**为什么数据面用 LSM 而不是 B+Tree**：写入是顺序写（WAL + MemTable），随机写性能高一个数量级；SST 前缀压缩 + zstd 压缩率高。代价是读放大/写放大 → 用 Bloom Filter + Block Cache 缓解。

---

## 4. 服务治理：SpringCloud + Nacos

### 4.1 注册与发现

```yaml
spring:
  cloud:
    nacos:
      discovery:
        server-addr: ${NACOS_ADDR}
        namespace: nexus
        group: NEXUS_GROUP
        ephemeral: true            # 临时实例，心跳保活
        weight: 1.0
        metadata:
          zone: ${ZONE}
          netty-port: ${NETTY_PORT}   # 关键：把 Netty 端口塞进 metadata
```

**关键设计**：`netty-port` / `zone` 通过 **Nacos metadata** 透出。SDK 拉到实例列表后读 metadata 建 Netty 长连接——**注册中心管 HTTP 端口，数据面端口走 metadata 扩展**，混合协议系统的标准做法。

### 4.2 健康检查与自动故障转移（三层）

| 层 | 机制 | 阈值 |
| --- | --- | --- |
| 1 客户端心跳 | Nacos SDK 5s 上报 | 15s 不健康 / 30s 剔除 |
| 2 服务端探测 | TCP 端口 + HTTP `/actuator/health` | 连续 3 次失败 |
| 3 业务指标 | 自定义 `HealthIndicator` | leader 数异常 / apply lag 超阈值 → DOWN |

```java
@Component
public class StoreHealthIndicator implements HealthIndicator {
    public Health health() {
        long leaders = raftManager.countLeaderRegions();
        long applyLag = raftManager.maxApplyLag();      // commitIndex - appliedIndex
        long diskFree = diskProbe.usableRatio();
        if (applyLag > 100_000 || diskFree < 0.05) return Health.down().build();
        return Health.up().withDetail("leaders", leaders)
                          .withDetail("applyLag", applyLag).build();
    }
}
```

**故障转移链路**：节点宕机 → Nacos 剔除实例（≤30s）+ Raft 组心跳超时（≤1s，远快于 Nacos）→ 该节点上所有 Region 的 Follower 发起选举 → 新 Leader 产生 → Leader 向 PD 上报 → PD 更新路由 → 客户端请求旧 Leader 失败/收到 `NotLeader` → 客户端重拉路由重试。**两条感知通道互为备份，Raft 那条是主**。

### 4.3 配置中心（动态调优）

运行时可改：Region 分裂阈值（96MB）、心跳间隔（100ms）、选举超时区间（150~300ms）、Raft 批大小（1MB）、快照阈值（128MB）、限流阈值。用 `@RefreshScope` / `NacosConfigListener` 生效，无需重启。

### 4.4 优雅下线

`ContextClosedEvent` → 先 `nacos.deregister` → **主动把所有 Region 的 leadership transfer 给其他节点**（避免触发选举造成 1~2s 抖动）→ 等待 `appliedIndex == commitIndex` → 关闭 Netty channel → 关闭 RocksDB。

---

## 5. 高性能异步通信：Netty

### 5.1 为什么不用 HTTP（对比 minFS）

| 维度 | minFS（HTTP/RestTemplate） | NexusStore（Netty） |
| --- | --- | --- |
| 连接 | 每次 `new RestTemplate()` → 短连接，TCP 三次握手 + 慢启动 | 长连接池复用，一次握手多次复用 |
| 协议 | 文本 + Header 冗余，序列化/反序列化开销大 | 定长二进制头 + Protobuf body |
| 线程 | 一请求一线程（同步阻塞），连接数上来线程爆炸 | Reactor 异步，少量 EventLoop 扛 5w 连接 |
| 拷贝 | 多次堆内堆外拷贝 | 池化 DirectBuffer + Composite/FileRegion 零拷贝 |
| 实测 | ~2.3k QPS，P99 ~120ms | ~9k QPS/Region，P99 ~16ms |

### 5.2 线程模型（主从 Reactor）

```java
EventLoopGroup boss   = new NioEventLoopGroup(1);              // accept
EventLoopGroup worker = new NioEventLoopGroup(cpus * 2);       // read/write
EventExecutorGroup biz = new DefaultEventExecutorGroup(64);    // 业务(解码后)

b.childHandler(new ChannelInitializer<SocketChannel>() {
  protected void initChannel(SocketChannel ch) {
    ch.pipeline()
      .addLast(new IdleStateHandler(0, 0, 60, TimeUnit.SECONDS))  // 空闲检测
      .addLast(new NexusFrameDecoder())         // 拆包
      .addLast(new NexusProtocolDecoder())
      .addLast(new NexusProtocolEncoder())
      .addLast(biz, new RequestDispatcherHandler());  // 耗时业务甩到 biz 线程池
  }
});
```

- **Boss 只需 1 个**：只做 accept，一个端口一个 Selector 足够。
- **Worker = CPU×2**：与 Channel 绑定，一个 Channel 的全生命周期只由一个 EventLoop 驱动 → **pipeline 内串行无锁**，不需要给 handler 加锁（这也是 Netty 快的核心原因）。
- **Biz 池隔离**：Raft 状态机 apply、RocksDB 读写是阻塞操作，必须甩出 IO 线程，否则一个慢查询卡死整条 EventLoop 上的所有连接。

### 5.3 私有协议与编解码

```
 0        1        2        3        4        5        6
 +--------+--------+--------+--------+--------+--------+
 | magic  |version | codec  | type   | flags  | status |
 | 4B     | 1B     | 1B     | 1B     | 1B     | 1B     |
 +--------+--------+--------+--------+--------+--------+
 | requestId (8B)            | bodyLength (4B)         |
 +---------------------------+-------------------------+
 | body (Protobuf / 原始字节, N B)                     |
 +-----------------------------------------------------+
```

- `magic = 0x4E455855 ("NEXU")`：快速识别非法包，防止端口扫描/错连导致解析脏数据；
- `type`：`HEARTBEAT / PUT / GET / RAFT_APPEND / RAFT_VOTE / SNAPSHOT / NOT_LEADER / REDIRECT`；
- `requestId`：异步响应关联的关键（§5.5）；
- `status`：错误码（0 OK / 1 NOT_LEADER / 2 EPOCH_STALE / 3 TIMEOUT ...）。

### 5.4 拆包粘包

```java
// lengthFieldOffset=16, lengthFieldLength=4, lengthAdjustment=0, initialBytesToStrip=0
public class NexusFrameDecoder extends LengthFieldBasedFrameDecoder {
    public NexusFrameDecoder() {
        super(8 * 1024 * 1024, 16, 4, 0, 0);   // 单包上限 8MB
    }
}
```

TCP 是字节流，无消息边界。`LengthFieldBasedFrameDecoder` 按长度字段切帧，彻底解决半包/粘包。单包 8MB 上限防 OOM（大文件走分块 + FileRegion，不塞进内存）。

### 5.5 异步请求-响应关联（核心）

同步阻塞模型下"一个线程等一个响应"；异步模型必须把响应与请求对上号：

```java
public class RequestFuture extends CompletableFuture<NexusResponse> {
    private final long requestId;
    private final long deadline;
}

// 发送端
long rid = REQ_ID_GEN.incrementAndGet();
RequestFuture f = new RequestFuture(rid, timeoutMs);
PENDING.put(rid, f);                    // ConcurrentHashMap
channel.writeAndFlush(req).addListener(fut -> {
    if (!fut.isSuccess()) { PENDING.remove(rid); f.completeExceptionally(fut.cause()); }
});
return f;                                // 调用方 CompletableFuture 编排

// 接收端（dispatcher）
NexusResponse resp = ...;
RequestFuture f = PENDING.remove(resp.getRequestId());
if (f != null) f.complete(resp);         // 唤醒等待方
```

- `PENDING` 用 `ConcurrentHashMap<Long, RequestFuture>`，O(1)；
- 超时扫描用 **HashedWheelTimer**（tick 100ms，wheel 512），比 `ScheduledExecutorService` 在海量超时任务下快得多（O(1) 而非 O(log n) 堆操作）；
- 全局 requestId 自增 + 节点 ID 前缀，避免多连接串号。

### 5.6 连接管理与心跳

```java
// 客户端连接池：每个目标节点一个 FixedChannelPool
public class NettyConnectionManager {
    private final Map<NodeId, ChannelPool> pools = new ConcurrentHashMap<>();
    // acquire() 拿连接，release() 归还；断线自动重建 + 指数退避重连
}
```

- **心跳**：双向。客户端 10s 发 `HEARTBEAT`；服务端 `IdleStateHandler(0,0,60)` 检测 60s 无读则关连接。Raft 层另有独立的 100ms 心跳（AppendEntries 空包），**TCP 心跳与 Raft 心跳职责不同**：前者探活连接，后者维持 leadership。
- **重连**：指数退避 100ms → 上限 5s + 抖动（防重连风暴）。

### 5.7 零拷贝与内存管理

| 手段 | 场景 |
| --- | --- |
| `PooledByteBufAllocator`（默认） | 池化 DirectBuffer，复用内存，避免频繁 GC |
| `CompositeByteBuf` | 协议头 + body 拼装不复制（对比 `ByteBuf` 合并要 copy） |
| `DefaultFileRegion` | 大文件传输走 `sendfile`，数据不进用户态 |
| `ByteBuf.slice()/duplicate()` | 共享同一段内存，只改读写指针 |
| 引用计数 + 泄漏检测 | `-Dio.netty.leakDetection.level=PARANOID` 压测期开启 |

**坑**：池化 DirectBuffer 不受 GC 直接管理，必须靠 `ReferenceCountUtil.release()` 或 `SimpleChannelInboundHandler`（自动 release），否则堆外内存泄漏。这是 Netty 最常见的生产事故，面试必提。

### 5.8 背压与流控

```java
// 1) Netty 层高低水位
ch.config().setWriteBufferWaterMark(new WriteBufferWaterMark(32<<20, 64<<20));
// channelWritabilityChanged → 暂停从上游队列取任务

// 2) 应用层
Semaphore inflight = new Semaphore(10000);      // 在途请求上限
ArrayBlockingQueue<Task> queue = new ArrayBlockingQueue<>(50000);  // 有界队列
```

**为什么必须有背压**：RocksDB 写入慢于网络收包，若无限堆积，任务队列吃满堆内存 → OOM。有界队列 + 信号量 + 快速失败（返回 `SERVER_BUSY` 让客户端重试）是标准做法。

---

## 6. 数据分片与负载均衡：一致性哈希

### 6.1 为什么不用取模

| | `hash(key) % N` | 一致性哈希 |
| --- | --- | --- |
| 扩容 N→N+1 | **几乎全量迁移**（命中率 1/(N+1)） | 只迁移 **K/(N+1)** |
| 倾斜 | 依赖 hash 均匀性 | 虚拟节点缓解 |
| 异构节点 | 不支持权重 | 虚拟节点数 = 权重 |

### 6.2 哈希环 + 虚拟节点

```java
public class ConsistentHashRouter {
    private final TreeMap<Long, VirtualNode> ring = new TreeMap<>();
    private static final int VNODES_PER_NODE = 200;    // 每节点 200 个虚拟节点

    public void addNode(Node node) {
        for (int i = 0; i < VNODES_PER_NODE * node.getWeight(); i++) {
            long h = hash(node.getId() + "#" + i);      // MurmurHash3
            ring.put(h, new VirtualNode(node, i));
        }
    }

    public Node route(byte[] key) {
        long h = hash(key);
        Map.Entry<Long, VirtualNode> e = ring.ceilingEntry(h);   // O(log N)
        if (e == null) e = ring.firstEntry();                    // 环回绕
        return e.getValue().getNode();
    }
}
```

- 环空间 `[0, 2^32)`，`TreeMap.ceilingEntry` 二分查找 **O(log N)**；
- **虚拟节点解决两个问题**：① 数据倾斜（节点少时环上分布不均，200 个虚拟节点后标准差降到 <5%）；② 异构机器（高配机器分配更多虚拟节点 = 更高权重）。
- 哈希函数选 **MurmurHash3**（比 MD5 快 10x 且分布均匀；不用 `String.hashCode`——冲突率高、分布差）。

### 6.3 关键设计：环上放「Region（分片）」而不是「节点」

**这是本项目最容易讲出深度的一点。**

朴素一致性哈希把 key 直接映射到物理节点，问题：
- 节点上下线 → 该节点负责的 key 全部要**真实迁移数据**，迁移量与数据量成正比，扩缩容慢；
- 无法控制副本放置（机架感知）；
- 节点数变化时路由全变，缓存全失效。

NexusStore 的做法（TiKV/Ceph 同款思想）——**两层映射**：

```
        key ──hash──▶ Region(虚拟分片, 固定 4096 个 或 动态分裂)
                            │
                            ▼   PD 维护的映射表
                       Raft Group = [nodeA(leader), nodeB, nodeC]
```

| | 朴素（key→node） | NexusStore（key→Region→RaftGroup） |
| --- | --- | --- |
| 节点上下线 | 迁移 K/N 条数据 | **只迁移 Region 的副本归属**，PD 改一行映射 + Raft 成员变更；数据通过 Raft 快照/日志自动同步，无需业务侧搬运 |
| 副本放置 | 不可控 | PD 可按 zone/rack 精确调度 |
| 扩缩容 | 全环路由变化 | Region 数量不变则 key→Region 映射**完全不动**，客户端缓存几乎不失效 |
| 分裂 | 不支持 | Region 超阈值分裂，只影响自身区间 |

一句话："**一致性哈希负责确定性地把 key 钉在某个分片上，PD 负责决定这个分片放在哪几台机器上。前者保证路由稳定，后者保证负载均衡，两者解耦。**"

### 6.4 路由表与 RegionEpoch（防 stale 路由）

```java
public class RegionEpoch { long confVer; long version; }   // 成员变更版本 / 分裂版本
```

请求携带 `(regionId, epoch)`，store 节点校验：

| 情况 | 处理 |
| --- | --- |
| epoch 匹配 | 正常处理 |
| `version` 落后（Region 已分裂/合并） | 返回 `REGION_SPLIT` + 新 Region 列表 |
| `confVer` 落后（成员已变更） | 返回 `NOT_LEADER` + 新 leader 地址 |
| epoch 超前（客户端比服务端新） | 返回 `EPOCH_STALE`，客户端强制重拉 |

**没有 epoch 会怎样**：Region 分裂后，客户端拿着旧路由请求，新 Region 可能不认识这个 key → 数据写错分片 → **静默数据错误**。epoch 是分布式存储防止"路由 stale 导致数据错乱"的必备机制。

### 6.5 客户端路由缓存与失效重试

```
1. 本地缓存 ConcurrentHashMap<KeyRange, RegionRoute>
2. 请求 → 命中缓存 → 直连 leader
3. 失败/NotLeader → 标记该 Region 路由失效 → 向 PD 重拉（带 epoch 条件更新）
4. PD 返回新路由 → 重试（最多 3 次，指数退避 + jitter）
```

- 客户端还做**本地缓存预热**：启动时批量拉全量路由表（几千个 Region 约几百 KB），后续靠 PD 的 **Watch/长轮询**增量更新，避免每次请求都问 PD。
- PD 侧路由表读多写少 → **Redis 旁路缓存**（hash 结构存 regionId→route，PD 更新时 del），降低 MySQL 压力。

### 6.6 分片分裂（Split）

触发：Region 大小 > 96MB（或 key 数 > 200w，或写 QPS 过高）。

```
1. Leader 本地计算 splitKey（采样 SST 估算中点，保证两半数据量均衡）
2. 通过 Raft 提交一条 Split 日志（保证所有副本达成一致）
3. 各副本 apply：本地 RocksDB 分裂 CF，生成新 Region(newId, midKey, endKey)
4. 新 Region 在原副本集合上组建自己的 Raft Group（复用已有数据，无需拷贝）
5. Leader 向 PD 上报：ns_region 分裂 + version++
6. 客户端下次请求命中旧 Region → 返回 REGION_SPLIT → 重拉路由
```

**为什么分裂要走 Raft 日志**：分裂是改变状态机的操作，必须和读写操作一样经过一致性协议，否则副本间分裂点不一致 → 数据丢失。

### 6.7 PD 负载均衡调度器

一致性哈希保证"路由确定"，**均衡靠 PD 调度**（每 10s 一轮）：

| 策略 | 规则 |
| --- | --- |
| Leader 均衡 | 统计各节点 Leader 数，把 Leader 从最多节点 `TransferLeader` 给最少节点 |
| 副本数均衡 | 各节点 Region 副本数标准差 > 阈值 → 迁移副本 |
| 容量均衡 | `used/capacity` 最高与最低相差 > 20% → 迁移 |
| 机架感知 | **同一 Region 的 3 副本必须分布在 ≥2 个 rack**（可配置 ≥3 zone） |
| 热点打散 | 按 Region 上报的 QPS 统计，把热点 Region 的 Leader 分散到不同节点 |

调度操作写入 `ns_operator` 表并**限流**（同时在途 operator ≤ 8，单个 Region 冷却 30s），避免调度引发雪崩。
迁移副本采用 **先加 Learner → 追平日志 → 变 Follower → 删旧副本** 四步，全程不中断服务。

### 6.8 热点 key 治理

一致性哈希解决不了**单 key 热点**（某个 key 被疯狂写，只落在一个 Region）。方案：
- 客户端/服务端统计 key 访问频次，超过阈值识别为热点；
- 写入侧：批量合并 + 客户端本地聚合（计数类场景）；
- 读侧：Follower Read（但要保证一致性 → 用 ReadIndex）；
- 极端场景：key 拆分（加随机后缀打散到多个 Region，读时聚合）。

---

## 7. 强一致：Raft + Multi-Raft

### 7.1 为什么选 Raft，以及 CAP 取舍

- 对比 **Paxos**：Raft 通过"强 Leader + 日志连续性"把问题分解，工程可实现性好得多（工业界事实标准：etcd / TiKV / Consul / Nacos-JRaft / Kafka KRaft 都是 Raft 系）。
- 对比 **ZAB**（ZooKeeper）：ZAB 主备模式 + 原子广播，不保证日志连续、依赖 ephemeral session；Raft 成员变更与快照模型更清晰。minFS 用 ZK 只能拿到"选主"能力，拿不到"日志复制"。
- 对比 **Gossip（AP）**：Dynamo/Cassandra 最终一致，读写永远可用但可能拿到旧值 —— 存储系统不可接受。

**CAP 选择**：网络分区时，**少数派侧拒绝读写**（返回 `NOT_LEADER`，不会返回旧数据），多数派侧继续服务 → **CP**。
可用性代价：3 副本集群允许 1 节点故障；分区成 2+1 时，"1" 那一侧完全不可用。这是**正确性优先**的主动选择。

### 7.2 三种角色与核心状态

```
Leader     : 接收写请求，复制日志，发心跳
Follower   : 被动接收，参与投票
Candidate  : 选举中间态

// 所有节点持久化
currentTerm, votedFor, log[]

// 所有节点内存
commitIndex   // 已知被多数派复制的最大日志索引
lastApplied   // 已应用到状态机的索引

// Leader 内存
nextIndex[]   // 给每个 follower 下一条要发的日志
matchIndex[]  // 每个 follower 已复制的最大日志索引
```

### 7.3 领导者选举

**流程**：

1. Follower 维护**随机选举超时**（150~300ms 内随机；不是固定值，避免同时发起选举导致选票瓜分）。
2. 超时未收到 Leader 心跳 → 转 Candidate：`currentTerm++`，投自己，并行发 `RequestVote(term, candidateId, lastLogIndex, lastLogTerm)`。
3. 结果：
   - 收到**多数派**选票 → 成为 Leader，立即发心跳宣示主权；
   - 收到更高 term 的消息 → 退回 Follower；
   - 超时无人胜出 → term++ 重新选举（随机超时保证下一轮大概率分出胜负）。

**投票规则（安全性核心）**：
- 每个 term 每个节点只能投一票，先到先得；
- **只有候选人的日志"至少和我一样新"才投票**：比较 `(lastLogTerm, lastLogIndex)`，lastLogTerm 大者新；相等则 lastLogIndex 大者新。

> **为什么必须比较日志新旧**：这条限制保证了"已提交的日志一定在任何一个新 Leader 上"。因为一条日志被提交 = 被多数派复制，而新 Leader 要拿到多数派选票，两个多数派必有交集，交集里至少有一个节点有这条日志 → 该节点只会给日志不比它旧的候选人投票 → 新 Leader 必然有这条日志。**这是 Raft 安全性的根基，面试必考。**

**PreVote（工程必备优化）**：
网络分区恢复后，被隔离节点 term 可能远大于集群，恢复通信后会用高 term 强制所有人退位 → 触发无谓重新选举，集群抖动。
PreVote：正式选举前先发 `PreRequestVote(term+1)`（**不真正递增 term、不落盘**），只有拿到多数派预投票才真正发起选举 → 被孤立节点骚扰不到集群。

**为什么副本数是奇数**：
- 3 副本 quorum=2，容忍 1 故障；4 副本 quorum=3，**也只容忍 1 故障**，却多一份存储与写入成本 → 偶数无意义；
- 5 副本 quorum=3，容忍 2 故障。

### 7.4 日志复制

```
Client → Leader:
 1. Leader 本地 append（不提交）
 2. 并行 AppendEntries 给所有 Follower
      (term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit)
 3. 收到多数派成功 → commitIndex = N → apply 到 RocksDB → 响应 Client
 4. 后续 AppendEntries（含心跳）携带 leaderCommit，Follower 跟进提交
```

**一致性检查与冲突修复**：
- Follower 检查 `log[prevLogIndex].term == prevLogTerm`？否则拒绝。
- Leader 被拒 → `nextIndex--` 重试。
- **优化**：Follower 拒绝时返回 `(conflictTerm, firstIndexOfConflictTerm)`，Leader 直接跳到该 term 的第一条，避免逐条回退（一条条退在大规模冲突时是 O(n) 次 RPC）。
- 匹配后：**删除冲突位置之后的全部日志，再追加新日志**（保证 Leader 的日志权威）。

**提交规则（易错点）**：

> Leader 只能**通过计数副本的方式提交当前 term 的日志**；前任 term 的日志只能在当前 term 有新日志提交时被"间接"提交。（Raft 论文 Figure 8 问题）

绕过方案：新 Leader 上任后**先提交一条 no-op 空日志**，一旦 no-op 被多数派确认，`commitIndex` 推进，前面所有前任日志自然被间接提交，状态机追上。这条 no-op 也让 Leader 快速确认自己的 leadership。

### 7.5 安全性五条核心规则

| 规则 | 内容 |
| --- | --- |
| 选举安全 | 一个 term 内最多一个 Leader |
| Leader 只追加 | Leader 从不删除或覆盖日志 |
| 日志匹配 | 若两节点某 index 的 term 相同，则该 index 之前的日志完全相同 |
| Leader 完备性 | 已提交的日志一定出现在所有后续 Leader 中（靠 §7.3 投票限制） |
| 状态机安全 | 若某节点在 index i 应用了日志，则其他节点不会在 i 应用不同日志 |

### 7.6 线性一致读（ReadIndex）

**朴素做法的问题**：直接读 Leader 的状态机 —— 若该 Leader 其实已被隔离（新 Leader 已产生），会返回**陈旧数据**，违反线性一致。

**ReadIndex 流程**：

```
1. Leader 记录 readIndex = 当前 commitIndex
2. 向全体 Follower 发一轮心跳，确认自己仍是合法 Leader（多数派响应 + term 未变）
3. 等待本地 lastApplied >= readIndex
4. 读状态机返回
```

代价：一次 RTT（心跳轮）。
**优化 Lease Read**：Leader 在上次心跳多数派成功后的一段时间（lease，如 心跳间隔×0.9）内，假定没有新 Leader 产生 → 省去心跳轮，读性能接近本地读（P99 <2ms）。
风险：依赖**时钟**，NTP 大幅跳变可能破坏 lease → 生产做法：租约时长远小于选举超时、监控时钟漂移、或用 etcd 的 `ReadIndex` 保守模式。

另外提供 **Follower Read**（读从副本分担压力），但同样要向 Leader 要 ReadIndex + 等本地 apply 追上，保证线性一致。

### 7.7 日志压缩与快照

日志不能无限增长：

```
触发：日志数 > 128MB 或 条数 > 100w
 1. 对当前状态机做快照（RocksDB Checkpoint，硬链接 O(1)）
 2. 记录 lastIncludedIndex / lastIncludedTerm
 3. 删除快照点之前的日志
 4. 落后太多的 Follower → 发 InstallSnapshot RPC（分块流式传输，每块 1MB）

RocksDB Checkpoint 用硬链接共享 SST 文件 → 快照几乎零额外空间、零阻塞。
```

### 7.8 成员变更

用 **单节点变更（Single-node Change）**：一次只增/删一个节点。

**为什么安全**：旧配置 quorum = ⌈N/2⌉+1，新配置（±1 节点）quorum 必然与旧 quorum 有交集 → 不可能同时选出两个 Leader。
对比 **Joint Consensus**（两阶段，新旧配置共存）正确但复杂得多；工程上单节点变更更简单，只要保证"**同一时刻只有一个变更在进行，且新 Leader 必须先提交 no-op 再发起下一个变更**"。

### 7.9 Multi-Raft：分片内 Raft，分片间并行

```
Store Node A:  R1(Leader) R3(Follower) R7(Leader)  R9(Follower)
Store Node B:  R1(Follower) R3(Leader) R7(Follower) R9(Leader)
Store Node C:  R1(Follower) R3(Follower) R7(Follower) R9(Follower)
```

- 每个 Region 一个**独立 Raft Group**，独立 term、独立选举、独立日志 → 一个 Region 选主不影响其他；
- **不同 Region 的 Leader 分散在不同节点** → 写入压力天然打散（这是 Multi-Raft 相比"整个节点一个 Raft"最大的优势）；
- **心跳合并**：同节点上若有 1000 个 Region，不能发 1000 次心跳 → 同一个 target 节点的所有 Region 心跳**合并成一个 RPC**（batch），RPC 量从 O(Region) 降到 O(Node)；
- **分裂与 Raft 的配合**：见 §6.6，分裂本身就是一条 Raft 日志。

### 7.10 工程性能优化

| 优化 | 说明 |
| --- | --- |
| **批量写（Batch）** | Leader 把多个请求合并成一条 Raft 日志提交，摊薄 fsync 成本；QPS 提升 3~5x，代价是延迟略增 |
| **流水线（Pipeline）** | 不等上一条日志 ACK 就发下一条，把 RTT 与 fsync 重叠 |
| **组提交（Group Commit）** | 多个 follower 的 append 合并为一次磁盘 fsync |
| **并行 Apply** | 不同 Region 的状态机 apply 在不同线程并行执行（同 Region 内严格串行） |
| **异步 fsync + 半同步** | 日志写 PageCache 即返回 + 定时/定量 fsync；或"多数派 PageCache + Leader fsync"折中 |
| **Follower 只读本地 RocksDB** | 避免跨网络读 |

---

## 8. 核心流程时序

### 8.1 写入（Put）

```
SDK                PD              Leader(R1)          Follower          Follower
 │                  │                  │                   │                 │
 │─ 路由查询(缓存) ─▶│(miss 时)          │                   │                 │
 │◀─ region+leader ─┤                  │                   │                 │
 │─ AppendEntries(put) ───────────────▶│                   │                 │
 │                  │           本地 append log              │                 │
 │                  │─ AppendEntries ──┼──────────────────▶│                 │
 │                  │─ AppendEntries ──┼───────────────────────────────────▶│
 │                  │            fsync  │                   │                 │
 │                  │◀──── ACK ────────┤                   │                 │
 │                  │◀────────────── ACK ──────────────────┤                 │
 │                  │  多数派达成 → commit → apply RocksDB    │                 │
 │◀──── OK ─────────┼──────────────────┤                   │                 │
 │                  │  下个心跳携带 commitIndex → follower apply
```

### 8.2 线性一致读（Get）

```
SDK → Leader: ReadIndex 请求
Leader: readIndex = commitIndex → 心跳确认 leadership（多数派）→ 等 lastApplied ≥ readIndex
      → 读 RocksDB → 返回
（Lease Read 命中则跳过心跳轮，直接读）
```

### 8.3 Leader 故障与恢复

```
t0  Leader 宕机
t1  Follower 选举超时(150~300ms 随机，最快者先超时)
t2  PreVote → 正式选举 → 多数派 → 新 Leader（~300ms）
t3  提交 no-op → 前任日志被间接提交 → 可服务（~500ms）
t4  新 Leader 上报 PD → 路由更新 → 客户端收到 NotLeader 重拉路由（~1s）
t5  PD 检测原节点失联 > 30min → 标记 Tombstone → 调度补副本到健康节点
```

**客户端视角不可用窗口 ≈ 1~3s**（期间请求失败重试）。

### 8.4 扩容（加节点）

```
1. 新节点启动 → Nacos 注册 → PD 发现新节点（心跳上报）
2. PD 计算：从副本数最多的节点挑 Region → AddPeer（Learner）
3. Learner 追日志（先 InstallSnapshot 再追增量）
4. 追平 → 变 Follower（confVer++）→ 若需均衡则删掉源节点旧副本
5. Leader 均衡调度：TransferLeader 一部分 Region 给新节点
6. 完成。key→Region 映射全程不变，客户端缓存零失效（只更新 Region→Node）
```

---

## 9. 一致性与可用性分析

### 9.1 CAP 结论

| | 说明 |
| --- | --- |
| **C** | 线性一致：Raft 日志复制 + ReadIndex 读，已提交数据不丢、读不返旧值 |
| **A** | 部分可用：多数派分区可用，少数派分区**拒绝服务**（返回错误而非脏数据） |
| **P** | 分区容忍：分区期间多数派继续工作，恢复后少数派通过日志回滚/补齐自动收敛 |
| **结论** | **CP** |

### 9.2 PACELC 补充

> **P**artition → 在 A/C 间选 **C**；**E**lse（正常情况）→ 在 **L**atency/**C**onsistency 间权衡。

正常无分区时，我们仍然为一致性付出延迟：每次写要等多数派 fsync（~10ms）。优化手段（Batch / Pipeline / Lease Read）本质都是在 **E 段**用可控的正确性风险换延迟。

### 9.3 故障矩阵

| 故障 | 影响 | 恢复 |
| --- | --- | --- |
| 单 Follower 宕机 | 无影响（quorum 仍为 2/3） | 重启后追日志；超 30min 触发补副本 |
| 单 Leader 宕机 | 该 Region 不可用 1~3s | 自动选举 |
| 多节点/整机宕机（≤1/3） | 其上 Region 不可用，数据不丢 | 自动选举 + PD 补副本 |
| 机架断电 | 若 3 副本跨 rack 则安全 | PD 调度补副本 |
| 网络分区 2:1 | 2 侧可用，1 侧拒绝服务 | 分区恢复后 1 侧回滚未提交日志并补齐 |
| PD 全部宕机 | **已有读写不受影响**（客户端有路由缓存），仅扩缩容/调度停止 | PD 本身用 Raft 保证元数据高可用 |
| 磁盘损坏 | 副本丢失 | PD 检测后从其他副本重新拉起 |

---

## 10. 性能与压测

### 10.1 环境

- 3 台 8C16G 云主机，千兆内网，SSD（约 300MB/s），CentOS 7
- JDK 17，堆 8G，G1；RocksDB 7.x；Netty 4.1.x
- 压测工具：自研 Netty 压测客户端（避免 HTTP 客户端本身成为瓶颈）+ `wrk` 测管理面

### 10.2 结果

| 场景 | QPS | P50 | P99 | 瓶颈 |
| --- | --- | --- | --- | --- |
| 单 Region 写（3 副本 fsync） | ~9k | 6ms | 16ms | 多数派 fsync + 一次 RTT |
| 单 Region 写（Batch 512） | ~3.5w | 15ms | 40ms | 吞吐换延迟 |
| 32 Region 并行写 | ~11w | 8ms | 25ms | 网络带宽 / CPU |
| 读（Lease Read 命中） | ~30w | 0.4ms | 1.8ms | RocksDB Block Cache 命中率 |
| 读（ReadIndex） | ~12w | 1.2ms | 5ms | 心跳轮 RTT |
| 单节点长连接 | 5w | - | - | 内存 + fd |
| **对比 minFS（HTTP）** | 2.3k | 40ms | 120ms | 短连接 + 同步阻塞 |

### 10.3 方法论（防止被质疑）

1. 先**单变量基线**：关闭 Raft（单副本写 RocksDB）测出存储引擎上限，再开 Raft 看一致性代价；
2. 压测客户端与服务端**分离部署**，否则客户端先被打满（`CLOSE_WAIT`、端口耗尽、GC）；
3. 关注 **P99/P999 而非均值**，均值会掩盖 fsync 毛刺与 GC 停顿；
4. 压测前预热 5 分钟（RocksDB Compaction、JIT、连接池）；
5. 同时观测：CPU、磁盘 util（`iostat -x`）、网络（是否打满千兆）、GC 次数、RocksDB `pending compaction bytes`。

### 10.4 优化前后对比（可讲的故事）

| 优化 | 提升 |
| --- | --- |
| HTTP → Netty 长连接 + 二进制协议 | QPS 2.3k → 1.2w（同单机单 Region） |
| 逐条 fsync → Batch + Pipeline + 组提交 | 1.2w → 3.5w |
| 无分片 → Multi-Raft 32 Region | 3.5w → 11w |
| 直接读 Leader → Lease Read | 读 P99 5ms → 1.8ms |

---

## 11. 已知缺陷与后续演进

| # | 缺陷 | 现状 | 演进方向 |
| --- | --- | --- | --- |
| 1 | 无跨分片事务 | 单 key 原子 | 借鉴 Percolator 实现 2PC（Prewrite/Commit + 全局 TSO） |
| 2 | PD 自身高可用 | 单实例 + MySQL 主备 | PD 也组 Raft（或直接用 etcd 存元数据） |
| 3 | Lease Read 依赖时钟 | 有漂移风险 | 监控 NTP 偏移，超阈值自动降级为 ReadIndex |
| 4 | 热点 key 无自动治理 | 人工拆分 | 自动热点识别 + key 打散 + Follower Read |
| 5 | 无跨机房多活 | 单机房 3 副本 | 引入 Learner + 异地异步复制（Raft Learner 天然适合做灾备） |
| 6 | RocksDB Compaction 抖动 | 写停顿 | 限速 + 独立线程池 + 业务低峰触发 |
| 7 | 无多租户配额/限流 | 无 | 接入层令牌桶 + 按 namespace 配额 |
| 8 | 大文件（>64MB） | 需客户端分块 | 服务端自动分块 + 元数据索引 |
| 9 | 无备份/时间点恢复 | 靠副本 | 定期快照上传对象存储 + binlog（Raft log）回放 |

---

## 12. 附录

### 12.1 Redis 在项目中的位置（次要，非重点）

- **路由表缓存**：`HASH region:route`，PD 更新时 `DEL`，降低 MySQL 读压力（PD 路由查询 QPS 高但数据变更极少）；
- **调度互斥锁**：PD 多实例时对同一 Region 的调度互斥（`SET NX PX`）；
- **热点 Key 统计**：滑动窗口计数，供调度器做 Leader 打散。

> 简历里的「分布式限流 + 多级缓存」是另一个（订单）场景，本文不展开。

### 12.2 模块与技术点对照（简历逐条落实）

| 简历表述 | 本文对应 |
| --- | --- |
| SpringCloud + Nacos 服务注册发现、健康检查、自动故障转移 | §4 全章（metadata 带 netty 端口、三层健康检查、优雅下线 leadership transfer） |
| Netty 高性能异步通信模块 | §5 全章（主从 Reactor、私有协议、RequestFuture、零拷贝、背压） |
| 一致性哈希实现数据分片与负载均衡 | §6 全章（哈希环 + 虚拟节点、**环上放 Region**、epoch、分裂、PD 调度） |
| CAP 定理权衡 | §2.3 分层 CAP、§9.1/9.2（CP + PACELC） |
| Raft 领导者选举 + 日志同步，强一致 CP | §7 全章（选举/PreVote/日志复制/no-op/ReadIndex/快照/成员变更/Multi-Raft） |

### 12.3 关键类索引

```
nexus-common/protocol   NexusProtocol / NexusFrameDecoder / RequestFuture / ConnectionManager
nexus-pd               RegionTable / ConsistentHashRouter / ScheduleService / StoreNodeManager
nexus-store/raft       RaftNode / RaftLog / RaftStateMachine / ElectionTimer / ReplicationWorker / Snapshotter
nexus-store/netty      NettyServer / RequestDispatcherHandler / RaftAppendHandler
nexus-store/engine     RocksDBEngine / ColumnFamilyManager
nexus-client           NexusClient / RouteCache / RetryPolicy
```
