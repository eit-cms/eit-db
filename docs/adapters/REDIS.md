# Redis Adapter

## 概述

Redis 适配器在 EIT-DB 中承担两类职责：

1. 常规 KV / PubSub 能力。
2. 协作层能力（消息面 + 管理控制面）。

- 适配器标识：`redis`
- 驱动包：`github.com/redis/go-redis/v9`
- 典型场景：协作消息总线、节点健康管理、轻量实时事件

关系语义支持等级：不适用（键值/流模型，不作为关系语义主执行面）。

> vNext 方向：对外 API 将逐步收敛为后端无关的统一 `Query/Exec` 入口；Redis 仍主要承担缓存、流式消息与协作控制面，不建议承载关系语义查询。

## 快速开始

```go
cfg := &db.Config{
    Adapter: "redis",
    Redis: &db.RedisConnectionConfig{
        Host: "127.0.0.1",
        Port: 56379,
        DB:   0,
    },
}

repo, err := db.NewRepository(cfg)
if err != nil {
    panic(err)
}
defer repo.Close()

if err := repo.Connect(context.Background()); err != nil {
    panic(err)
}
```

## 连接配置

```yaml
database:
    adapter: redis
    redis:
        host: 127.0.0.1
        port: 56379
        db: 0
        username: ""
        password: ""
        tls_enabled: false
        cluster_mode: false
```

关键字段：

1. `uri`：可直接使用 Redis URI。
2. `host`/`port`/`db`：常规单实例配置。
3. `cluster_mode` + `cluster_addrs`：集群模式。
4. `dial_timeout`/`read_timeout`/`write_timeout`：连接与 IO 超时。

## 普通应用场景能力矩阵

Redis 在 EIT-DB 中不是关系型查询后端，建议把它定位为缓存、会话、实时事件与协作消息基础设施。

### 1) 数据库能力矩阵（DatabaseFeatures）

| 能力 | 状态 | 说明 |
|---|---|---|
| 复合键 / 外键 / JOIN 约束 | ❌ | 非关系型模型，不提供 SQL 约束语义 |
| 原生 JSON | ✅ | 依赖 RedisJSON 模块 |
| JSONPath | ✅ | 依赖 RedisJSON 模块 |
| JSON 索引 | ⚠️ | 需 RediSearch 配合 |
| 全文搜索 | ⚠️ | 需 RediSearch 配合 |
| Functions / Lua | ✅ | 支持 Lua 与 Redis Functions |
| Upsert 语义 | ✅ | `SET` / `HSET` 等天然覆盖写入 |
| Listen/Notify 风格 | ✅ | Pub/Sub 与 Streams |

### 2) 查询能力矩阵（QueryFeatures）

| 能力 | 状态 | 说明 |
|---|---|---|
| IN / BETWEEN / LIKE / GROUP BY | ❌ | 不走 SQL 查询构造路径 |
| JOIN / CTE / 窗口函数 | ❌ | 不适用 |
| 子查询 | ❌ | 不适用 |
| UNION / EXCEPT / INTERSECT | ❌ | 不适用 |

### 3) 业务场景适配矩阵

| 场景 | 适配度 | 推荐方式 |
|---|---|---|
| 缓存（对象缓存、热点键） | 高 | 直接使用 Redis Adapter 命令路径 |
| 会话与令牌存储 | 高 | KV + TTL 模型 |
| 发布订阅通知 | 高 | `GetRedisSubscriberFeatures` |
| 消息队列/事件流 | 高 | `GetRedisStreamFeatures` |
| 协作层节点管理 | 高 | `GetRedisManagementFeatures` |
| 复杂关系查询报表 | 低 | 交给 SQL/图数据库后端 |
| 跨表 JOIN 分析 | 低 | 不建议在 Redis 承担 |

## 协作层能力视图

### 1) Subscriber 视图

```go
sub, ok := repo.GetRedisSubscriberFeatures()
if !ok {
    panic("redis subscriber features unavailable")
}

pubsub := sub.Subscribe(ctx, "collab.events")
defer pubsub.Close()
_, _ = sub.Publish(ctx, "collab.events", "hello")
```

核心 API：

1. `Publish`
2. `Subscribe`
3. `PSubscribe`

### 2) Stream 视图（协作消息面）

```go
stream, ok := repo.GetRedisStreamFeatures()
if !ok {
    panic("redis stream features unavailable")
}

_ = stream.EnsureConsumerGroup(ctx, "collab:demo:request", "adapter-postgres")
_, _ = stream.PublishEnvelope(ctx, "collab:demo:request", &db.CollaborationMessageEnvelope{
    MessageID:      "msg-1",
    RequestID:      "req-1",
    EventType:      "query.requested",
    IdempotencyKey: "idem-1",
})
```

核心 API：

1. `PublishEnvelope`
2. `ReadGroupEnvelopes`
3. `Ack`
4. `ListPendingMessages`
5. `ClaimPendingEnvelopes`
6. `RetryPendingEnvelopes`
7. `SnapshotLag`
8. `StartAutoRecovery`

### 3) Management 视图（协作控制面）

```go
mgmt, ok := repo.GetRedisManagementFeatures()
if !ok {
    panic("redis management features unavailable")
}

_ = mgmt.RegisterAdapterNode(ctx, &db.CollaborationAdapterNodePresence{
    NodeID:      "adapter-postgres-1",
    AdapterType: "postgres",
    AdapterID:   "managed-postgres",
    Group:       "adapter-postgres",
    Namespace:   "collab_demo",
}, 30*time.Second)

_ = mgmt.HeartbeatAdapterNode(ctx, "collab_demo", "adapter-postgres", "adapter-postgres-1", 30*time.Second)
```

核心 API：

1. `RegisterAdapterNode`
2. `HeartbeatAdapterNode`
3. `MarkAdapterOffline`
4. `ListGroupNodes`
5. `PublishGroupEvent`
6. `SubscribeGroupEvents`

## 协作键空间约定

管理控制面默认使用以下前缀：

1. `collab:mgmt:<namespace>:node:<node_id>`
2. `collab:mgmt:<namespace>:group:<group>:members`
3. `collab:mgmt:<namespace>:group:<group>:events`

建议：

1. 生产中务必设置独立 `namespace`。
2. 不要将业务普通缓存键与 `collab:mgmt:` 前缀混用。

## 运行与诊断建议

1. 对消息消费端实现幂等逻辑（`idempotency_key` + 业务去重键）。
2. 定期观测 `SnapshotLag` 与 pending 数量，避免积压放大。
3. 将 `request_id`、`trace_id` 注入日志，联动 Arango 账本排障。
4. 在压测环境先验证 `RetryPendingEnvelopes` 和 DLQ 处理策略。

## 测试建议

集成测试可参考：

1. `adapter-application-tests/collaboration_integration_test.go`
2. `adapter-application-tests/collaboration_arango_integration_test.go`
3. `adapter-application-tests/collaboration_monitor_tree_integration_test.go`

快速运行（示例）：

```bash
cd adapter-application-tests
REDIS_PORT=56379 POSTGRES_PORT=55432 ARANGO_PORT=58529 go test ./... -run 'Redis|Collaboration' -count=1
```
