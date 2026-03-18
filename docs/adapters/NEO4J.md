# Neo4j Adapter

## 概述

Neo4j 适配器提供图数据库接入，支持 Cypher 查询语言、原生图遍历、全文搜索、关系语义以及 Cypher Query Builder（通过 QueryConstructor 体系接入）。

- **适配器标识**：`"neo4j"`
- **驱动包**：`github.com/neo4j/neo4j-go-driver/v5`
- **特性等级**：图数据库 / Cypher / 非 SQL

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

### JOIN 语义映射

| SQL JOIN 类型 | Cypher 等价 | 降级策略 |
|---|---|---|
| INNER JOIN | `MATCH (a)-[r:REL]->(b)` | 原生支持 |
| LEFT JOIN | `OPTIONAL MATCH (a)-[r:REL]->(b)` | 原生支持 |
| CROSS JOIN | 多个独立 `MATCH` 子句 | 原生支持 |
| RIGHT JOIN | ❌ 不支持 | 返回错误（无降级） |

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
