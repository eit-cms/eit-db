# Neo4j Adapter

## 概述

Neo4j 适配器提供图数据库接入，支持 Cypher 查询语言、原生图遍历、全文搜索、关系语义以及 Cypher Query Builder（通过 QueryConstructor 体系接入）。

- **适配器标识**：`"neo4j"`
- **驱动包**：`github.com/neo4j/neo4j-go-driver/v5`
- **特性等级**：图数据库 / Cypher / 非 SQL

关系语义支持等级：强支持（图关系为 first-class）。

> vNext 方向：对外 API 将逐步收敛为后端无关的统一 `Query/Exec` 入口，由适配器自动映射到 Cypher；本文中的 `QueryCypher/ExecCypher` 可视为当前阶段的显式能力入口与兼容路径。

## 快速开始

```go
cfg := &db.Config{
    Adapter: "neo4j",
    Neo4j: &db.Neo4jConnectionConfig{
        URI:      "neo4j://localhost:7687",
        Username: "neo4j",
        Password: "secret",
        Database: "neo4j", // 默认库
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

## 支持的能力

### 数据库特性（DatabaseFeatures）

| 能力 | 状态 | 备注 |
|---|---|---|
| 外键 / 关联约束 | ✅ | 原生关系边替代 SQL FK |
| 复合关联键 | ✅ | 多属性关系模式替代复合 FK |
| 复合索引 | ✅ | 属性+标签组合索引 |
| 部分索引 | ❌ | Neo4j 无 WHERE 子句部分索引 |
| 原生 JSON | ✅ | 节点/关系属性支持 map/list |
| JSON 路径 | ✅ | 属性 dot notation |
| 全文搜索 | ✅ | 需创建 fulltext index |
| UPSERT | ✅ | `MERGE` 语句 |
| 生成列 | ❌ | |
| 存储过程 | ❌ | 通过 APOC 库扩展 |
| 窗口函数 | ❌ | Cypher 不支持 |
| 递归 CTE | ✅ | 可变长度路径匹配替代 SQL 递归 CTE |
| LISTEN/NOTIFY | ❌ | |

### SQL 接口兼容性

Neo4j 不走 SQL，以下标准接口均通过明确错误信息引导至 Cypher 路径：

| 接口 | 行为 | 替代方案 |
|---|---|---|
| `Query(ctx, sql, ...)` | `"neo4j: sql query not supported; use custom feature or native driver"` | `QueryCypher` |
| `Exec(ctx, sql, ...)` | `"neo4j: sql exec not supported; use custom feature or native driver"` | `ExecCypher` |
| `Begin(ctx, ...)` | `"neo4j: SQL transaction interface is not supported"` | 原生 session |
| `RegisterScheduledTask` | `"neo4j: scheduled task not supported"` | 应用层调度器 |

## Cypher 执行 API

### 直接执行 Cypher

```go
// 读查询（ExecuteRead session）
rows, err := repo.QueryCypher(ctx,
    "MATCH (u:User {active: true}) RETURN u.id, u.name LIMIT $limit",
    map[string]interface{}{"limit": 20},
)
// rows: []map[string]interface{}

// 写查询（ExecuteWrite session，返回变更摘要）
summary, err := repo.ExecCypher(ctx,
    "CREATE (u:User {id: $id, name: $name, created_at: datetime()})",
    map[string]interface{}{"id": "u1", "name": "Alice"},
)
fmt.Printf("nodes created: %d\n", summary.NodesCreated)
```

`CypherWriteSummary` 包含：`NodesCreated`、`NodesDeleted`、`RelationshipsCreated`、`RelationshipsDeleted`、`PropertiesSet`、`LabelsAdded`、`LabelsRemoved`。

### 通过 QueryConstructor 构建 Cypher

EIT-DB 的 QueryConstructor 体系支持生成 Cypher，查询语义通过 IR 层编译：

```go
provider := repo.GetAdapter().GetQueryBuilderProvider()
qc := provider.NewQueryConstructor(userSchema)

cypher, args, err := qc.
    Select("u.id", "u.name", "u.email").
    Where(db.Eq("u.active", true)).
    Where(db.Gt("u.age", 18)).
    OrderBy("u.name", "ASC").
    Limit(20).
    Offset(0).
    Build(ctx)

// cypher: MATCH (u:User) WHERE u.active = $p1 AND u.age > $p2 RETURN u.id, u.name, u.email ORDER BY u.name ASC SKIP 0 LIMIT 20
```

### 关系语义映射（图语义优先）

| SQL JOIN 类型 | Cypher 等价 | 降级策略 |
|---|---|---|
| INNER JOIN | `MATCH (a)-[r:REL]->(b)` | 原生支持 |
| LEFT JOIN | `OPTIONAL MATCH (a)-[r:REL]->(b)` | 原生支持 |
| CROSS JOIN | 多个独立 `MATCH` 子句 | 原生支持 |
| RIGHT JOIN | ❌ 不支持 | 返回错误（无降级） |

说明：该表仅用于迁移对照。Neo4j 的主表达方式是“关系边 + 路径遍历”，不建议以 SQL JOIN 覆盖率评估图语义能力。

```go
qc.Join("Company", "WORKS_AT", "c")          // → MATCH (n)-[r1:WORKS_AT]->(c:Company)
qc.LeftJoin("Department", "BELONGS_TO", "d") // → OPTIONAL MATCH (n)-[r2:BELONGS_TO]->(d:Department)
```

### 统一执行接口（`ExecuteQueryConstructor`）

```go
result, err := repo.ExecuteQueryConstructor(ctx, qc)
// result.Statement: 编译后的 Cypher
// result.Args: 参数列表
// result.Rows: []map[string]interface{}（QueryCypher 已自动执行）
```

带缓存版本（缓存编译结果，命中后跳过 Build）：

```go
result, cacheHit, err := repo.ExecuteQueryConstructorWithCache(ctx, "user:list", qc)
fmt.Printf("cache hit: %v\n", cacheHit)
```

### 统一 Query/Exec 自动路由（`ExecuteQueryConstructorAuto`）

当你希望框架自动识别这是读查询还是写查询时，可使用自动路由入口：

```go
result, err := repo.ExecuteQueryConstructorAuto(ctx, qc)
if err != nil {
    panic(err)
}

switch result.Mode {
case "query":
    fmt.Println("rows:", len(result.Rows))
case "exec":
    fmt.Println("affected:", result.Exec.RowsAffected)
    fmt.Println("counters:", result.Exec.Counters)
}
```

在 Neo4j 下：
- `MATCH/RETURN/CALL...YIELD` 会自动走 `QueryCypher`。
- 写语句（如 `CREATE/MERGE/DELETE/SET`）会自动走 `ExecCypher`，并返回 `CypherWriteSummary` 映射后的执行摘要。

自动执行返回字段对照见：[docs/AUTO_EXEC_RESULT_MATRIX.md](docs/AUTO_EXEC_RESULT_MATRIX.md)

## 自定义特性（ExecuteCustomFeature）

Neo4j Adapter 通过 `ExecuteCustomFeature` 暴露以下图特色能力（返回 Cypher 模板和参数）：

| 特性名 | 功能描述 |
|---|---|
| `graph_traversal` / `recursive_query` | 可变长度路径遍历（`*1..n`） |
| `document_join` | 通过关系边 MATCH 两端节点 |
| `full_text_search` | 使用 fulltext index 的 Cypher 搜索模板 |
| `relationship_association_query` | 方向可控的关系查询（in/out/both） |
| `relationship_with_payload` | 创建/更新带属性的关系边 |
| `bidirectional_relationship_semantics` | 单向申请 → 双向关系的语义转换（如好友请求） |
| `social_model_one_to_one_chat` | 一对一聊天模型：仅双向 FRIEND 或双向 FOLLOWS 可发送消息 |
| `social_model_group_chat_room` | 多人聊天室模型：ChatRoom 单元 + 双向 IN 发言权限 + 单向 IN 入群申请 |
| `social_model_chat_receipt` | 聊天已读回执模型：使用 `READ_BY` 关系表达消息读取状态 |
| `social_model_chat_moderation` | 聊天治理模型：使用 `MUTED_IN` / `BANNED_IN` 关系控制发言权限 |
| `social_model_message_emoji` | 消息 Emoji 嵌入模型：`INCLUDED_BY(index)` 关系绑定 `{{0}}` 占位符 |
| `social_network_preset_model` | 统一社交预设入口（含 one_to_one_chat / group_chat_room） |

```go
// 图遍历：从某类节点出发，沿某类关系最多遍历 3 层
result, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "graph_traversal", map[string]interface{}{
    "start_label":  "Department",
    "relationship": "MANAGES",
    "max_depth":    3,
})
// result["cypher"] = "MATCH p=(n:Department)-[:MANAGES*1..3]->(m) RETURN p"

// 好友申请转双向好友关系
result, err = repo.GetAdapter().ExecuteCustomFeature(ctx, "bidirectional_relationship_semantics", map[string]interface{}{
    "user_label":            "User",
    "request_relationship":  "KNOWS_REQUEST",
    "friend_relationship":   "KNOWS",
    "from_id":               "u1",
    "to_id":                 "u2",
})
```

## 社交聊天预设模型（新增）

这组模型是专门为社交场景准备的预设能力，核心目标是把“可聊天关系判断”“发言权限判断”“消息引用关系”在图模型层一次定义好，避免上层业务重复拼接复杂 Cypher。

### 1) 一对一聊天模型（`one_to_one_chat`）

规则：

1. 只有双向 `FRIEND` 或双向 `FOLLOWS` 的用户对，才允许发送私聊消息。
2. 消息是中间节点 `ChatMessage`，通过 `(:User)-[:SENT]->(:ChatMessage)-[:TO]->(:User)` 表达发送链路。
3. 支持 `REF` 关系引用历史消息。
4. 默认包含 `chat_message_fulltext` 全文索引（`ChatMessage.content`），可直接做消息检索。
5. 提供高级检索模板：支持时间窗口、软删除过滤和 @ 命中加权。

```go
out, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "social_model_one_to_one_chat", map[string]interface{}{})
if err != nil {
    panic(err)
}
queries := out.(map[string]interface{})["queries"].(map[string]string)

// queries["send_direct_message"]
// queries["can_chat_check"]
// queries["list_direct_messages"]
// queries["search_direct_messages"]
// queries["search_direct_messages_advanced"]
```

### 2) 多人聊天室模型（`group_chat_room`）

规则：

1. 聊天单元为 `ChatRoom`，可由创建者关系 `(:User)-[:CREATED]->(:ChatRoom)` 建立。
2. 房间名上层可传入，未传时默认名为 `room`。
3. 单向 `(:User)-[:IN]->(:ChatRoom)` 表示入群申请，双向 `IN` 表示正式成员并具备发言权限。
4. 在房间消息中可使用 `AT` 关联被提及用户，使用 `REF` 引用其他消息。
5. 默认包含 `chat_message_fulltext` 全文索引，并提供房间内消息检索模板。
6. 提供高级检索模板：支持时间窗口、软删除过滤和 @ 命中加权。

```go
out, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "social_network_preset_model", map[string]interface{}{
    "preset": "group_chat_room",
})
if err != nil {
    panic(err)
}
queries := out.(map[string]interface{})["queries"].(map[string]string)

// queries["create_room"]      // 默认 room 名称逻辑
// queries["request_join_room"]
// queries["approve_join_room"]
// queries["send_room_message"] // 双向 IN 才可发言
// queries["at_user"]
// queries["ref_message"]
// queries["search_room_messages"]
// queries["search_room_messages_advanced"]
```

### 3) 统一预设入口

你也可以用统一入口按名称拿到预设：

```go
out, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "social_network_preset_model", map[string]interface{}{
    "preset": "one_to_one_chat", // 或 group_chat_room
})
```

### 4) 已读回执模型（`chat_receipt`）

核心语义：

1. 已读是一个关系事实，不是消息属性，使用 `(:User)-[:READ_BY]->(:ChatMessage)` 表达。
2. 直接聊天和群聊都复用同一回执关系，避免上层维护两套已读逻辑。
3. 默认包含 `chat_message_fulltext` 索引，便于与“未读消息检索”组合使用。

### 6) 消息 Emoji 嵌入模型（`message_emoji`）

核心语义：

1. 消息文本中使用数字占位符（例如 `{{0}}`、`{{1}}`）表达嵌入点。
2. 通过关系 `(:Emoji)-[:INCLUDED_BY {index: n}]->(:ChatMessage)` 绑定占位符和 Emoji 节点。
3. Emoji 节点是静态可复用节点，可被多个消息复用引用。
4. 上层可先取 `render_message_emoji_payload`，再按 index 替换模板中的 `{{index}}`。

```go
out, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "social_model_message_emoji", map[string]interface{}{})
if err != nil {
    panic(err)
}
queries := out.(map[string]interface{})["queries"].(map[string]string)

// queries["attach_emoji_to_message"]
// queries["list_message_emojis"]
// queries["render_message_emoji_payload"]
```

```go
out, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "social_model_chat_receipt", map[string]interface{}{})
if err != nil {
    panic(err)
}
queries := out.(map[string]interface{})["queries"].(map[string]string)

// queries["mark_direct_message_read"]
// queries["mark_room_message_read"]
// queries["list_direct_unread"]
// queries["list_room_unread"]
```

### 5) 聊天治理模型（`chat_moderation`）

核心语义：

1. 禁言使用 `MUTED_IN` 关系，封禁使用 `BANNED_IN` 关系，治理规则沉淀在图关系层。
2. 提供 `can_send_room_message` 查询模板，API 层可在发消息前执行权限门禁，避免绕过上层校验。

```go
out, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "social_network_preset_model", map[string]interface{}{
    "preset": "chat_moderation",
})
if err != nil {
    panic(err)
}
queries := out.(map[string]interface{})["queries"].(map[string]string)

// queries["mute_member"]
// queries["ban_member"]
// queries["can_send_room_message"]
```

## 为什么图数据库更适合在数据层实现关系建模

对于社交/聊天类系统，图数据库的优势不只在查询速度，更在于“关系语义可被数据库直接约束和复用”：

1. 关系规则下沉到数据层：例如“必须双向关系才能私聊”“双向 IN 才能发群消息”，可以直接在 Cypher 条件中判定。
2. API 更安全：上层 API 不需要在多个服务重复实现权限判断，可复用 `can_chat_check`、`can_send_room_message` 这类模板。
3. 降低越权风险：把权限关系（READ_BY、MUTED_IN、BANNED_IN）建模为图关系后，越权路径更容易被统一拦截。
4. 复杂关系可持续演进：随着业务增长，可在模型上新增关系类型而无需重构大量中间表与联表逻辑。

## 全文搜索

Neo4j 通过 fulltext index 支持全文搜索。在使用前需要在 Neo4j 中建立全文索引：

```cypher
-- 先在 Neo4j 侧创建 fulltext 索引
CALL db.index.fulltext.createNodeIndex('userFullText', ['User'], ['name', 'bio'])
```

然后：

```go
// 获取全文搜索 Cypher 模板
result, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "full_text_search", map[string]interface{}{
    "index": "userFullText",
    "query": "Alice developer",
})
// result["cypher"] = "CALL db.index.fulltext.queryNodes($index, $query) YIELD node, score RETURN node, score"
// 再用 QueryCypher 执行即可：
rows, err := repo.QueryCypher(ctx, result["cypher"].(string), result["params"].(map[string]interface{}))
```

## 全文搜索能力探测

```go
rt, err := repo.GetAdapter().InspectFullTextRuntime(ctx)
// rt.NativeSupported == true；连接可用时 rt.PluginAvailable == true
```

## 获取原生 Driver

```go
raw := repo.GetAdapter().GetRawConn()
driver, ok := raw.(neo4j.DriverWithContext)
if ok {
    session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
    defer session.Close(ctx)
    // 执行任意 Cypher
}
```

## 降级策略总览

| Neo4j 不支持 / 差异 | 降级方案 |
|---|---|
| RIGHT JOIN | 返回错误（不支持，无降级） |
| 跨节点 CROSS JOIN | 编译为多个独立 MATCH 子句 |
| SQL 事务接口 | 使用原生 `session.RunImplicitTransaction` 或 `ExecuteWrite` |
| 定时任务 | 应用层调度器 |
| SQL ENUM / DOMAIN / UDT | 分类节点 + 关系边语义建模 |
| SQL CTE | Cypher 可变长度路径匹配替代 |

## 限制与注意事项

- **SQL 接口不支持**：`Query` / `Exec` / `Begin` 均返回错误，请使用 `QueryCypher` / `ExecCypher`。
- **图数据模型**：JOIN 语义类比为关系边匹配，需要设计合理的节点 + 关系类型结构。
- **全文搜索依赖索引**：必须先在 Neo4j 侧创建 fulltext index，否则 `CALL db.index.fulltext.queryNodes` 会报错。
- **读写分离**：`QueryCypher` 使用 `AccessModeRead`，`ExecCypher` 使用 `AccessModeWrite`，Bolt 协议会自动路由至主/副本节点。
- **数据库名**：多数据库支持需要 Neo4j Enterprise 或 5.x Community；默认使用 `Config.Neo4j.Database` 字段指定（空字符串使用默认库）。

## 推荐场景

- 社交网络（好友关系、关注、互动）
- 权限图（角色 → 权限 → 资源层级）
- 知识图谱 / 语义关联
- 推荐引擎（路径相似度）
- 高度关联且查询以"关系遍历"为主的业务数据
