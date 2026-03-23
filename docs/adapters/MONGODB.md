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
| `log_special_tokenization` | 日志规则分词（ip/url/error_code/trace_id/hashtag） |
| `log_hot_words` | 全量日志热词统计 |
| `log_hot_words_by_level` | 按日志级别（ERROR/WARN/INFO）统计热词 |
| `log_hot_words_by_time_window` | 按小时/天时间窗口统计热词 |
| `article_draft_management` | 文章草稿状态流转（create/update/publish/archive/restore/info/query_plan） |
| `article_draft_query_plan` | 输出草稿/待发布/已发布/归档的查询计划（filter/projection/sort/limit/skip） |
| `article_template_preset_library` | 内置模板库（blog/news/knowledge_base）列表与读取 |
| `article_template_rendering` | Go 标准模板渲染，支持模板白名单函数与安全策略 |

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

## 文档编辑与模板渲染能力（Why + How）

### 为什么提供这组能力

1. 文档类业务（CMS、知识库、公告系统）通常要同时处理“编辑中草稿”和“对外渲染内容”，仅靠基础 CRUD 容易在业务层重复造轮子。
2. 模板渲染若没有统一策略，常见问题是模板风格不一致、字段缺失时静默失败、函数滥用带来维护风险。
3. 查询计划输出可将“状态筛选 + 分页排序”标准化，便于 API 层和后台任务复用同一过滤语义。

### 用法一：草稿状态管理 + 查询计划

```go
// 1) 创建草稿
draftOut, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "article_draft_management", map[string]interface{}{
    "operation": "create",
    "article": map[string]interface{}{
        "title":   "Mongo Article",
        "content": "Draft content",
        "author_id": "u1001",
    },
    "tags":     []string{"db", "cms"},
    "category": "tech",
    "priority": 3,
})
if err != nil {
    panic(err)
}

// 2) 构建“已发布文章”查询计划（可直接给集合查询层使用）
planOut, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "article_draft_query_plan", map[string]interface{}{
    "status":    "published",   // draft | pending_publish | published | archived | all
    "author_id": "u1001",
    "category":  "tech",
    "sort_by":   "updated_at",
    "sort_order": -1,
    "limit":     20,
    "skip":      0,
})
if err != nil {
    panic(err)
}

_ = draftOut
_ = planOut // planOut["query_plan"] 包含 collection/filter/projection/sort/limit/skip
```

### 用法二：模板库 + 安全渲染

```go
// 1) 列出内置模板
presets, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "article_template_preset_library", map[string]interface{}{
    "operation": "list",
})
if err != nil {
    panic(err)
}

// 2) 使用 news 预设模板渲染
rendered, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "article_template_rendering", map[string]interface{}{
    "template_preset": "news",
    "template_name":   "news_card",
    "data": map[string]interface{}{
        "title":    "Weekly Digest",
        "lead":     "This week in engineering",
        "content":  "...",
        "reporter": "Team A",
        "source":   "Internal",
    },
    "enable_functions": true,
    "strict_variables": true,
    "max_template_size": 4096,
    "allowed_functions": []string{"trim", "upper", "lower", "join"},
})
if err != nil {
    panic(err)
}

_ = presets
_ = rendered // rendered["rendered_output"] 为最终文本
```

### 安全策略建议

1. 对外部可编辑模板，建议始终设置 `max_template_size` 和 `strict_variables=true`。
2. 开启 `enable_functions` 时，建议使用 `allowed_functions` 白名单，避免模板层过度复杂化。
3. 若仅需要固定风格页面，建议优先使用 `article_template_preset_library`，减少自定义模板维护成本。

### REST API 对接建议

如果你的项目是前后端分离（CMS、知识库后台、运营平台），建议用下表直接映射：

| 路由 | Method | 特性调用 | 说明 |
|---|---|---|---|
| /api/articles/draft | POST | article_draft_management | operation=create/update |
| /api/articles/publish | POST | article_draft_management | operation=publish |
| /api/articles/archive | POST | article_draft_management | operation=archive |
| /api/articles/restore | POST | article_draft_management | operation=restore |
| /api/articles/query-plan | POST | article_draft_query_plan | 产出筛选计划用于列表与批处理 |
| /api/articles/templates/presets | GET | article_template_preset_library | operation=list |
| /api/articles/render | POST | article_template_rendering | 支持 template 或 template_preset |

推荐请求体约定：

1. 草稿接口统一使用 operation 字段驱动状态迁移。
2. 渲染接口优先 template_preset，只有需要高度自定义时再传 template。
3. 安全策略字段统一放在根级，便于 API 网关做参数校验：
     - strict_variables
     - max_template_size
     - allowed_functions

推荐响应体约定：

```json
{
    "strategy": "article_template_rendering",
    "template_name": "news_card",
    "template_preset": "news",
    "rendered_output": "...",
    "security_policy": {
        "max_template_size": 4096,
        "strict_variables": true,
        "allowed_functions": ["trim", "upper", "join"]
    }
}
```

错误码映射建议：

1. 参数缺失（例如未传 template/template_preset）返回 400。
2. 模板解析失败返回 422。
3. 非法模板函数或超出安全策略返回 422。
4. 不支持的 operation/status 返回 400。

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
