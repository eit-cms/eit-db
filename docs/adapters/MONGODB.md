# MongoDB Adapter

## 概述

MongoDB 适配器提供文档型数据库接入，支持原生 BSON 操作、TTL 索引、应用层 JOIN 预加载、虚拟物化视图等特色能力。EIT-DB 为 MongoDB 设计了一套独立于 SQL 的特色功能体系。

- **适配器标识**：`"mongodb"`
- **驱动包**：`go.mongodb.org/mongo-driver`
- **特性等级**：文档型 / 非 SQL / 特色功能优先

## 快速开始

```go
cfg := &db.Config{
    Adapter: "mongodb",
    MongoDB: &db.MongoConnectionConfig{
        URI:      "mongodb://localhost:27017",
        Database: "myapp",
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

也可通过 `Options` 字段传递 URI（向后兼容）：

```go
cfg := &db.Config{
    Adapter:  "mongodb",
    Database: "myapp",
    Options:  map[string]interface{}{"uri": "mongodb://localhost:27017"},
}
```

## 支持的能力

### 数据库特性（DatabaseFeatures）

| 能力 | 状态 | 备注 |
|---|---|---|
| 外键约束 | ❌ | 降级：应用层或 $lookup aggregation |
| 复合外键 | ❌ | 降级：应用层 |
| 复合索引 | ✅ | |
| 部分索引 | ✅ | `partialFilterExpression` |
| 原生 JSON | ✅ | 文档即 BSON |
| JSON 路径 | ✅ | `$` 表达式 / dot notation |
| JSON 索引 | ✅ | 任意字段可建索引 |
| 全文搜索 | ✅ | text index / Atlas Search |
| UPSERT | ✅ | `upsert: true` 选项 |
| 聚合函数 | ✅ | Aggregation Pipeline |
| TTL 索引 | ✅ | **特色：自动文档过期** |
| 本地缓存 JOIN | ✅ | **特色：替代 $lookup** |
| 虚拟物化视图 | ✅ | **特色：替代 SQL 物化视图** |
| 事务 | ✅ | MongoDB 4.0+ 多文档事务（原生 driver） |
| 定时任务 | ❌ | 降级：应用层调度器 |

### SQL 接口兼容性

MongoDB 不走 SQL，以下标准接口均通过明确错误信息引导至正确路径：

| 接口 | 行为 | 替代方案 |
|---|---|---|
| `Query(ctx, sql, ...)` | 返回 `"mongodb: sql query not supported"` | 使用原生 driver 或自定义特性 |
| `Exec(ctx, sql, ...)` | 返回 `"mongodb: sql exec not supported"` | 使用原生 driver 或自定义特性 |
| `Begin(ctx, ...)` | 返回 `"mongodb: transactions not supported in SQL interface"` | 使用 `client.StartSession()` |
| `RegisterScheduledTask` | 返回 `"mongodb: scheduled task not supported"` | 使用 cron 库或 APOC |

### 统一 QueryConstructor 入口

MongoDB 也支持通过统一 `QueryConstructor` 入口构建并执行查询：

```go
schema := db.NewBaseSchema("users")
qc, err := repo.NewQueryConstructor(schema)
if err != nil {
    panic(err)
}

qc.Select("name", "age").Where(db.Eq("name", "alice")).OrderBy("age", "DESC").Limit(10)

result, err := repo.ExecuteQueryConstructorAuto(ctx, qc)
if err != nil {
    panic(err)
}
// result.Mode == "query"
// result.Rows 为 Mongo Find 查询结果
```

说明：
- `MongoQueryConstructor` 会把统一条件表达式编译为 `MONGO_FIND::`（查询）和 `MONGO_WRITE::`（写入）计划，再由 Repository 自动路由到 Mongo Adapter 执行。

写入示例（Insert/Update/Delete）：

```go
schema := db.NewBaseSchema("users")
qc, _ := repo.NewQueryConstructor(schema)

mongoQC, ok := qc.GetNativeBuilder().(*db.MongoQueryConstructor)
if !ok {
    panic("not mongo query constructor")
}

// InsertOne
insertResult, err := repo.ExecuteQueryConstructorAuto(ctx,
    mongoQC.InsertOne(map[string]interface{}{"name": "alice", "age": 18}),
)
if err != nil {
    panic(err)
}

// UpdateMany (filter 由 Where 条件生成)
qc2, _ := repo.NewQueryConstructor(schema)
mongoQC2 := qc2.GetNativeBuilder().(*db.MongoQueryConstructor)
updateResult, err := repo.ExecuteQueryConstructorAuto(ctx,
    func() db.QueryConstructor {
        mongoQC2.Where(db.Eq("status", "active"))
        return mongoQC2.UpdateMany(map[string]interface{}{"status": "inactive"}, false)
    }(),
)
if err != nil {
    panic(err)
}

// DeleteMany
qc3, _ := repo.NewQueryConstructor(schema)
mongoQC3 := qc3.GetNativeBuilder().(*db.MongoQueryConstructor)
deleteResult, err := repo.ExecuteQueryConstructorAuto(ctx,
    func() db.QueryConstructor {
        mongoQC3.Where(db.Eq("status", "deleted"))
        return mongoQC3.DeleteMany()
    }(),
)
if err != nil {
    panic(err)
}

_ = insertResult.Exec.RowsAffected
_ = updateResult.Exec.RowsAffected
_ = deleteResult.Exec.RowsAffected
```

可选返回增强：

```go
qc4, _ := repo.NewQueryConstructor(schema)
mongoQC4 := qc4.GetNativeBuilder().(*db.MongoQueryConstructor)

result, err := repo.ExecuteQueryConstructorAuto(ctx,
    mongoQC4.
        InsertOne(map[string]interface{}{"name": "carol"}).
        ReturnInsertedID().
        ReturnWriteDetail(),
)
if err != nil {
    panic(err)
}

// result.Exec.Details 可能包含：inserted_id / inserted_ids / matched_count / modified_count / deleted_count ...
_ = result.Exec.Details
```

自动执行返回字段对照见：[docs/AUTO_EXEC_RESULT_MATRIX.md](docs/AUTO_EXEC_RESULT_MATRIX.md)

## 特色功能

### 1. TTL 索引与自动过期（`MongoTTLFeatures`）

MongoDB 原生支持 TTL 索引：在 `Date` 类型字段上建立 `expireAfterSeconds` 索引后，后台进程每 60 秒轮询一次，自动删除过期文档。

**适用场景**：Session token、短信验证码、速率限制计数器、日志保留策略。

```go
ttlFeat, ok := db.GetMongoTTLFeatures(repo.GetAdapter())
if !ok {
    // adapter 不是 MongoDB
}

// 1. 在 expires_at 字段建 TTL 索引（幂等，重复调用不报错）
err := ttlFeat.EnsureTTLIndex(ctx, "sessions", "expires_at", 0)

// 2. 插入 30 分钟后过期的 Session
doc := bson.M{"token": "abc123", "user_id": "u1"}
err = ttlFeat.InsertWithExpiry(ctx, "sessions", doc, "expires_at",
    time.Now().Add(30*time.Minute))

// 3. 续期（更新过期时间）
err = ttlFeat.ExtendExpiry(ctx, "sessions",
    bson.D{{Key: "user_id", Value: "u1"}},
    "expires_at", time.Now().Add(2*time.Hour))

// 4. 列出所有 TTL 索引
indexes, _ := ttlFeat.ListTTLIndexes(ctx, "sessions")
for _, idx := range indexes {
    fmt.Printf("field=%s expireAfter=%v\n", idx.Field, idx.ExpireAfter)
}
```

### 2. 本地预加载缓存 + 应用层 JOIN（`MongoLocalCache`）

把体积小、读多写少的集合整体拉入进程内存，替代代价高昂的 `$lookup`，在应用层做高效等值 JOIN。

**适用场景**：字典表、角色权限表、配置表、分类树等写入频率低的集合。

```go
cache, ok := db.GetMongoLocalCache(repo.GetAdapter())

// 1. 预加载 roles 集合（10 分钟 TTL，未过期直接返回缓存，不发请求）
err := cache.Preload(ctx, "roles", "roles", nil, nil, 10*time.Minute)

// 2. N:1 JOIN：每个 user 关联一个 role
users := []bson.M{
    {"name": "Alice", "role_id": "admin"},
    {"name": "Bob",   "role_id": "user"},
}
enriched := cache.JoinWith(users, "role_id", "roles", "_id", "role", false)
// enriched[0]["role"] == bson.M{"_id": "admin", "label": "Administrator"}

// 3. 1:N JOIN：每个 user 关联多条 comments
enrichedWithComments := cache.JoinWith(users, "id", "comments", "author_id", "comments", true)

// 4. 手动失效（触发下次 Preload 重新拉取）
cache.Invalidate("roles")

// 5. 状态检查
stats := cache.Stats()
fmt.Printf("total=%d fresh=%d stale=%d\n", stats.Total, stats.Fresh, stats.Stale)
```

### 3. 虚拟物化视图（`MongoVirtualView`）

将 Aggregation Pipeline 执行结果按 TTL 缓存在进程内存，模拟 SQL 物化视图的语义，免 DDL、免数据库存储、免手动 REFRESH。

**适用场景**：报表汇总快照、权限角色预计算、低频变化但高频读取的聚合结果。

```go
vv, ok := db.GetMongoVirtualView(repo.GetAdapter())

// 1. 注册视图定义（不立即执行）
vv.Define("active_order_summary", "orders",
    bson.A{
        bson.D{{Key: "$match",  Value: bson.D{{Key: "status", Value: "active"}}}},
        bson.D{{Key: "$group",  Value: bson.D{
            {Key: "_id",   Value: "$region"},
            {Key: "total", Value: bson.D{{Key: "$sum", Value: "$amount"}}},
        }}},
        bson.D{{Key: "$sort", Value: bson.D{{Key: "total", Value: -1}}}},
    },
    5*time.Minute,
)

// 2. 首次 Execute：触发 Aggregate，结果缓存 5 分钟
docs, err := vv.Execute(ctx, "active_order_summary")

// 3. 5 分钟内再次调用：直接返回缓存，不发请求
docs, err = vv.Execute(ctx, "active_order_summary")

// 4. 强制刷新
docs, err = vv.Refresh(ctx, "active_order_summary")

// 5. 检查是否有效缓存
if vv.IsCached("active_order_summary") {
    // 缓存命中
}

// 6. 所有已注册视图
names := vv.DefinedViews()
```

## 自定义特性（ExecuteCustomFeature）

MongoDB Adapter 通过 `HasCustomFeatureImplementation` / `ExecuteCustomFeature` 接口暴露以下特色能力：

| 特性名 | 功能描述 |
|---|---|
| `document_join` / `custom_joiner` | 生成 `$lookup` Aggregation Pipeline 模板，支持多字段 |
| `full_text_search` | 生成基于 `$text` 索引的全文搜索 pipeline |
| `tokenized_full_text_search` | 生成应用层分词 + `$regex` 组合查询 pipeline |

```go
result, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "document_join", map[string]interface{}{
    "local_collection":   "orders",
    "foreign_collection": "products",
    "local_fields":       []string{"product_id"},
    "foreign_fields":     []string{"_id"},
    "as":                 "product",
})
// result["pipeline"] 为可直接传入 collection.Aggregate() 的 pipeline
```

## 全文搜索运行时探测

```go
rt, err := repo.GetAdapter().InspectFullTextRuntime(ctx)
// rt.NativeSupported == true
// rt.TokenizationMode == "application"（应用层分词回退）
```

## 获取原生 Driver

如需直接使用 MongoDB 原生 `*mongo.Client`：

```go
raw := repo.GetAdapter().GetRawConn()
client, ok := raw.(*mongo.Client)
if ok {
    // 使用原生 client 执行任意操作
    collection := client.Database("myapp").Collection("users")
}
```

## 降级策略总览

| MongoDB 不支持 | 推荐替代方案 |
|---|---|
| SQL JOIN | `$lookup` aggregation 或 `MongoLocalCache.JoinWith`（预加载集合） |
| SQL 物化视图 | `MongoVirtualView`（进程内缓存 pipeline 结果） |
| SQL LIKE | `$regex` 操作符 |
| CASE WHEN | `$cond` / `$switch` 聚合操作符 |
| 定时任务 | 应用层调度器（如 `robfig/cron`）或 APOC |
| 外键约束 | 应用层校验或 Schema 层验证 |

## 限制与注意事项

- **SQL 接口**：`Query` / `Exec` / `Begin` 均不支持，调用会返回明确错误，请使用 `GetRawConn()` 获取原生 driver。
- **事务**：多文档事务需要 MongoDB 4.0+ 副本集或分片集群；使用原生 `client.StartSession()` 实现。
- **TTL 精度**：MongoDB 后台 TTL 扫描线程每 ~60 秒运行一次，过期不会在毫秒级精确删除。
- **本地缓存 JOIN**：适合体积 ≤ 数万条文档的集合；大集合仍应使用 `$lookup`。

## 推荐场景

- 非结构化/半结构化文档存储（内容、日志、配置）
- Session / 验证码等需要 TTL 过期的临时数据
- 搜索索引（结合 Atlas Search 或 text index）
- 读多写少的字典/权限类数据（配合 `MongoLocalCache`）
- 报表汇总快照（配合 `MongoVirtualView`）
