# EIT-DB 关系语义支持说明（v1.1）

本文定义 EIT-DB 在关系语义层（HasMany / BelongsTo / HasOne / ManyToMany + Through）上的后端支持等级、边界与设计取舍。

为对齐后端无关化 API 演进（统一 `Query/Exec` 自动映射、统一事务语义入口），本文将“关系语义支持方式”作为主口径，不再把 SQL JOIN 覆盖率作为唯一主口径。

## 1. 支持等级矩阵

| 后端 | 支持等级 | 当前落地能力 | 备注 |
|---|---|---|---|
| Neo4j | 强支持 | 关系名映射边类型；ManyToMany + Through 编译为关系路径 | 图模型原生表达 |
| ArangoDB | 强支持 | AQL 图/文档关系语义；路径与关联能力可组合 | 图/文档关系为 first-class |
| PostgreSQL | 中支持 | 外键 + JOIN（关系型） | 关系表达稳定，但以关系型范式为主 |
| MySQL | 中支持 | 外键 + JOIN（关系型） | 关系表达稳定，但以关系型范式为主 |
| SQLite | 中支持 | 外键 + JOIN（关系型） | 关系表达可用，复杂语义常需补偿 |
| SQL Server | 中支持 | 外键 + JOIN / recursive_cte 策略 | 关系路径可策略化，但仍属关系型范式 |
| MongoDB | 弱支持 | `$lookup` / pipeline / 本地预加载 | 关系语义可模拟，但非一等关系模型 |
| Redis | 不适用 | 键值/流式协作模型 | 不承担关系语义主执行面 |

## 2. 分级判定标准

1. 强支持：关系语义是后端一等模型，路径遍历、关系类型与关联查询原生可表达。
2. 中支持：可通过外键与 JOIN（或策略化递归）稳定表达关系，但核心仍是关系型连接范式。
3. 弱支持：可通过聚合或应用层补偿实现关系查询，但语义表达与治理成本较高。
4. 不适用：后端定位不在关系语义主执行面（例如键值/流处理）。

## 3. 为什么关系型外键 + JOIN 归为“中支持”

中支持不代表“能力不足”，而是表示在当前版本中：

1. PostgreSQL / MySQL / SQL Server / SQLite 在关系型连接语义上具备稳定可用能力。
2. 其关系表达核心是表连接，不是图关系路径作为一等执行语义。
3. 面向跨后端统一语义时，仍建议把关系分析与路径遍历能力下沉到图后端。

## 4. PostgreSQL + AGE 说明

若启用 Apache AGE，PostgreSQL 的图能力可显著增强，理论上可提升到“强支持”层级。

但 v1 当前版本不将 AGE 作为基线依赖，原因是：

1. 插件前置条件会提高部署复杂度与跨环境一致性成本。
2. 关系语义路径会绑定到单一扩展生态，不利于多适配器可替换协同。
3. 当前阶段的路线目标优先“多后端并行可治理”，而非把图能力集中押注在单一关系库扩展。

## 5. 与配置策略的对应关系

### SQL Server

- many_to_many_strategy:
  - direct_join
  - recursive_cte
- recursive_cte_depth（默认 8）
- recursive_cte_max_recursion（默认 100）

### MongoDB

- relation_join_strategy:
  - lookup
  - pipeline
- hide_through_artifacts（默认 true）

## 5.1 配置示例（可直接复制）

### SQL Server：启用递归 CTE 路径策略

```yaml
database:
  adapter: sqlserver
  sqlserver:
    host: localhost
    port: 1433
    username: sa
    password: your_password
    database: app_db
    many_to_many_strategy: recursive_cte
    recursive_cte_depth: 12
    recursive_cte_max_recursion: 250
```

环境变量等价配置：

```bash
export SQLSERVER_MANY_TO_MANY_STRATEGY=recursive_cte
export SQLSERVER_RECURSIVE_CTE_DEPTH=12
export SQLSERVER_RECURSIVE_CTE_MAX_RECURSION=250
```

### MongoDB：启用 pipeline 策略并保留 through 临时字段

```yaml
database:
  adapter: mongodb
  mongodb:
    uri: mongodb://localhost:27017
    database: app_db
    relation_join_strategy: pipeline
    hide_through_artifacts: false
```

环境变量等价配置：

```bash
export MONGODB_RELATION_JOIN_STRATEGY=pipeline
export MONGODB_HIDE_THROUGH_ARTIFACTS=false
```

## 5.2 多适配器协同建议

推荐把关系语义分工为：

1. 主事务与结构化一致性：SQL（PostgreSQL / MySQL / SQL Server）。
2. 图关系遍历与关系语义查询：Neo4j。
3. 文档聚合与弹性查询：MongoDB。

该分工更符合 v1 当前目标：多后端并行可治理，而不是把所有关系语义压在单一后端扩展能力上。

## 5.3 端到端最小示例（Schema -> JoinWith -> 策略生效）

### 示例 A：SQL Server + recursive_cte（ManyToMany + Through）

```go
package main

import (
  "context"
  "fmt"

  db "github.com/eit-cms/eit-db"
)

func main() {
  // 1) 定义 Schema 与关系
  users := db.NewBaseSchema("users").
    AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true})
  roles := db.NewBaseSchema("roles").
    AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true})
  userRoles := db.NewBaseSchema("user_roles")

  users.ManyToMany(roles).
    Through(userRoles, "user_id", "role_id").
    Named("grants_role")

  // 2) 创建 SQL Server 方言（递归 CTE 策略）
  dialect := db.NewSQLServerDialectWithOptions("recursive_cte", 12, 250)

  // 3) 构造查询
  qc := db.NewSQLQueryConstructor(users, dialect).
    FromAlias("u").
    JoinWith(db.NewJoinWith(roles).As("r")).
    Where(db.Eq("u.id", 1001))

  sql, args, err := qc.Build(context.Background())
  if err != nil {
    panic(err)
  }

  fmt.Println(sql)
  fmt.Println(args)
}
```

说明：当关系为 ManyToMany + Through 且方言策略为 recursive_cte 时，编译器会生成递归 CTE 路径（含深度与 MAXRECURSION 控制）。

### 示例 B：Neo4j（命名关系类型）

```go
package main

import (
  "context"
  "fmt"

  db "github.com/eit-cms/eit-db"
)

func main() {
  users := db.NewBaseSchema("users")
  companies := db.NewBaseSchema("companies")

  users.BelongsTo(companies).
    Over("company_id", "id").
    Named("works_at")

  qc := db.NewNeo4jQueryConstructor(users).
    FromAlias("u").
    JoinWith(db.NewJoinWith(companies).As("c"))

  cypher, args, err := qc.Build(context.Background())
  if err != nil {
    panic(err)
  }

  fmt.Println(cypher) // 关系类型会使用 WORKS_AT
  fmt.Println(args)
}
```

### 示例 C：MongoDB（pipeline 策略 + through 字段折叠）

```go
cfg := &db.Config{
  Adapter: "mongodb",
  MongoDB: &db.MongoConnectionConfig{
    URI:                  "mongodb://localhost:27017",
    Database:             "app_db",
    RelationJoinStrategy: "pipeline",
    HideThroughArtifacts: ptrBool(true),
  },
}

func ptrBool(v bool) *bool { return &v }
```

说明：pipeline 模式下关系通过聚合管线执行；HideThroughArtifacts=true 时，会自动移除 through 中间临时字段。

## 5.4 策略选择决策表（建议起步值）

| 场景特征 | 推荐后端/策略 | 说明 |
|---|---|---|
| SQL Server，关系层级浅（通常 <= 3 层），追求可读 SQL 与稳定执行计划 | SQL Server + direct_join | 直接 Join 成本更可控，调试与 DBA 诊断更直接。 |
| SQL Server，层级较深或路径长度不固定，需要遍历式关系扩展 | SQL Server + recursive_cte | 用递归 CTE 表达路径扩展；按业务设置 recursive_cte_depth 与 recursive_cte_max_recursion。 |
| MongoDB，关系路径较短，优先简单可维护 | MongoDB + lookup | 单/双段 lookup 更直观，便于快速落地。 |
| MongoDB，需要复杂关联过滤、投影重排或多阶段聚合 | MongoDB + pipeline | 聚合管线可承载更复杂的数据重组逻辑。 |
| ManyToMany 且业务不希望暴露 through 中间集合细节 | MongoDB + hide_through_artifacts=true | 对外结果更贴近业务语义，减少中间字段泄漏。 |
| 关系语义是一等能力（边类型、路径查询、关系分析） | Neo4j + Named 关系 | 优先将关系语义下沉到图后端，Named 可稳定映射边类型。 |

落地建议（默认顺序）：

1. 先选“语义主后端”：关系分析优先 Neo4j；事务一致性优先 SQL。
2. 再定“执行策略”：SQL Server 从 direct_join 起步，确有深层路径需求再切 recursive_cte。
3. Mongo 先用 lookup，出现复杂重排与过滤需求再升到 pipeline。
4. 将策略写入配置并用同一组查询回归，确保跨环境行为一致。

## 6. 版本边界

本文描述的是 v1.1 基线行为。

后续若将 AGE 纳入基线或增加其他图语义后端能力，需同步更新：

- docs/RELATION_SEMANTICS.md（本文）
- docs/CAPABILITY_MATRIX.md（索引与矩阵）
- docs/ARCHITECTURE.md（架构取舍）
- README.md（用户导览）
