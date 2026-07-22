# MiniKV TODO

## Current Baseline

- [x] 单节点内存 Store 内核
- [x] 1024 个逻辑分片与 XXH3 分片算法
- [x] 64 GiB 默认节点容量
- [x] 128 MiB 单对象上限
- [x] TTL、SLRU 淘汰和容量控制
- [x] gRPC Protobuf 协议与生成链
- [x] gRPC 帧校验、流式 Reader 和错误映射
- [ ] Gateway 到单节点的完整数据链路

## P0: gRPC Data Plane

目标：客户端可以通过 Gateway 完成 Put、Get 和 Delete。

- [x] 完成 StaticRouter
- [x] 完成 BackendPool 连接复用与关闭逻辑
- [x] 实现 NodeService 流式 Put
- [ ] 实现 NodeService 流式 Get
- [ ] 实现 NodeService Delete
- [ ] 实现 Gateway Put 全代理
- [ ] 实现 Gateway Get 全代理
- [ ] 实现 Gateway Delete 全代理
- [ ] 注册标准 gRPC Health Service
- [ ] 支持 `minikv node` 与 `minikv gateway` 两种角色
- [ ] 实现信号处理和优雅停机

当前暂停点：NodeService 流式 Get 与 Delete 已有本地未提交代码，恢复开发后从这里继续。

### Acceptance

- [ ] Gateway 与 Node 不缓存完整大对象
- [ ] Put/Get 使用 256 KiB 数据块
- [ ] 所有 gRPC 消息不超过 1 MiB
- [ ] 取消和 deadline 能传播到 Store
- [x] 异常 Put 不会发布部分对象
- [x] 后端连接不会按 RPC 重建
- [ ] `go test -race ./...` 通过

## P1: Integration and Benchmark

目标：验证两跳架构的正确性与性能成本。

- [ ] 使用 bufconn 构建 Client -> Gateway -> Node -> Store 测试
- [ ] 覆盖 0 B、1 B、跨 chunk、1 MiB 对象
- [ ] 覆盖二进制 key、TTL、Delete 和 NotFound
- [ ] 覆盖短流、长流、错误序号和取消
- [ ] 增加 1 KiB、1 MiB、32 MiB benchmark
- [ ] 增加可选的 128 MiB 边界测试
- [ ] 建立 Python benchmark 客户端
- [ ] 输出吞吐、延迟、分配次数和内存占用

## P2: Distributed Control Plane

目标：用外部 etcd 管理节点、分片和路由。

- [ ] 定义 etcd keyspace
- [ ] 实现节点注册与 lease 续约
- [ ] 实现节点失效检测
- [ ] 实现 1024 分片 placement
- [ ] 实现 etcd-backed Router
- [ ] 实现路由 watch 与本地快照
- [ ] 引入 shard epoch
- [ ] NodeService 强制校验 shard ownership 和 epoch
- [ ] 实现路由切换期间的 fencing
- [ ] 定义 Gateway 无可用路由时的降级行为

## P3: Replication and Rebalancing

目标：节点故障后数据仍可服务，并支持集群扩缩容。

- [ ] 每分片一个 primary 和一个 replica
- [ ] 设计异步复制协议
- [ ] 增加复制进度和 lag 指标
- [ ] 实现副本提升
- [ ] 实现分片迁移
- [ ] 实现增量 rebalance
- [ ] 限制迁移带宽和并发数
- [ ] 验证扩容、缩容和节点故障场景

## P4: Production Readiness

- [ ] Prometheus 指标
- [ ] OpenTelemetry tracing
- [ ] 结构化日志和 request ID
- [ ] TLS 与 mTLS
- [ ] 身份认证和授权
- [ ] 多租户配额
- [ ] Gateway 限流
- [ ] 压力测试和故障注入
- [ ] 部署文档与运维手册
- [ ] Docker 和 Kubernetes 部署清单

## Deferred

- WAL 和重启恢复
- 磁盘分层缓存
- 压缩与应用层 checksum
- CAS、批量操作和事务
- 智能客户端直连 Node
- 跨地域复制

## Definition of Done

一个 TODO 只有同时满足以下条件才能勾选：

- [ ] 实现已经提交
- [ ] 单元测试覆盖正常和错误路径
- [ ] 并发代码通过 race detector
- [ ] 相关设计文档已经同步
- [ ] 没有未处理的 Critical 或 Important review 问题
