# 面试准备入口

从 [面试准备-简明主文档.md](面试准备-简明主文档.md) 开始：项目介绍、Raft/CAP、Multi-Raft、日志快照，以及 Go 并发内存和 Redis 常见问答，按当前基础准备。

需要查实现细节时再看 [nexusstore/docs/interview.md](nexusstore/docs/interview.md)，运行与故障演示见 [nexusstore/README.md](nexusstore/README.md)。

原版 QA 已归档到 [original-interview.md](nexusstore/docs/archive/original-interview.md)。旧稿中的 PD、RocksDB、ReadIndex、Lease Read、自动分片、零拷贝和性能数字不属于当前实现，不应作为已完成工作复述。

本 demo 聚焦：固定分片、Netty 异步 RPC、手写 Raft、强一致文件操作、故障切主、重启恢复，以及可选 Nacos 服务发现。
