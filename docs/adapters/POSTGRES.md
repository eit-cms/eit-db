# PostgreSQL Adapter

## 概述

PostgreSQL 是 EIT-DB 的首选生产关系型数据库，提供最完整的 SQL 特性支持，包括 JSONB、物化视图、窗口函数、递归 CTE 等高级能力。

- **适配器标识**：`"postgres"`
- **驱动包**：`github.com/lib/pq`
- **特性等级**：SQL 关系型 / 服务端 / 最完整

## 快速开始

```go
cfg := &db.Config{
    Adapter: "postgres",
    Postgres: &db.PostgresConnectionConfig{
        Host:     "localhost",
        Port:     5432,
        Database: "myapp",
        Username: "postgres",
        Password: "secret",
        SSLMode:  "disable",
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
| 复合主键 | ✅ | |
| 外键约束 | ✅ | |
| 复合外键 | ✅ | |
| 复合索引 | ✅ | B-Tree、GiST、GIN 多类型 |
| 部分索引 | ✅ | `CREATE INDEX ... WHERE` |
| 延迟约束 | ✅ | `DEFERRABLE INITIALLY DEFERRED` |
| 原生 JSON | ✅ | `json` / `jsonb`（推荐 jsonb） |
| JSON 路径 | ✅ | `->` / `->>` / `jsonb_path_query` |
| JSON 索引 | ✅ | GIN 索引直接作用于 jsonb |
| 全文搜索 | ✅ | tsvector / tsquery 原生支持 |
| RETURNING | ✅ | `INSERT/UPDATE/DELETE ... RETURNING` |
| UPSERT | ✅ | `ON CONFLICT ... DO UPDATE` |
| 生成列 | ✅ | |
| 数组类型 | ✅ | |
| 枚举类型 | ✅ | `CREATE TYPE ... AS ENUM` |
| 复合类型 | ✅ | |
| DOMAIN 类型 | ✅ | |
| 存储过程 | ✅ | PL/pgSQL |
| 窗口函数 | ✅ | |
| CTE / 物化 CTE | ✅ | |
| 递归 CTE | ✅ | |
| LISTEN / NOTIFY | ✅ | |

### 查询特性（QueryFeatures）

| 特性 | 状态 | 备注 |
|---|---|---|
| IN / NOT IN / BETWEEN | ✅ | — |
| LIKE / ILIKE | ✅ | ILIKE 大小写不敏感 |
| INNER / LEFT / RIGHT / CROSS JOIN | ✅ | — |
| FULL OUTER JOIN | ✅ | — |
| 自连接 | ✅ | — |
| CTE / 递归 CTE | ✅ | — |
| 窗口函数 | ✅ | — |
| UNION / EXCEPT / INTERSECT | ✅ | — |
| JSON 路径 / 运算符 | ✅ | — |
| 物化视图 | ✅ | `CREATE MATERIALIZED VIEW` |
| 全文搜索 | ✅ | — |
| COALESCE / CAST | ✅ | — |

## 高级特性

### LISTEN / NOTIFY 实时推送

PostgreSQL 原生支持 `LISTEN/NOTIFY`，可以做轻量级的数据库事件总线：

```go
// adapter 层拿到连接后直接发布通知（通过 GetRawConn 获取 *sql.DB）
// 注意：完整 LISTEN/NOTIFY 接口在 EIT-DB repository 层暂为透传，
// 依赖 lib/pq 的 `pq.NewListener` 实现订阅端。
```

### 物化视图

```go
vf, ok := db.GetViewFeatures(repo.GetAdapter())
// 使用 .Materialized() 切换为物化视图（不支持 CREATE OR REPLACE）
err := vf.View("order_stats").
    Materialized().
    As("SELECT DATE(created_at) AS day, COUNT(*) AS cnt FROM orders GROUP BY 1").
    ExecuteCreate(ctx)
```

### JSON 操作

PostgreSQL JSONB 支持完整的路径提取和 GIN 索引，可以在 `QueryConstructor` 的条件中直接使用字段路径：

```go
// 通过 Eq/Like 等条件构造器设置 JSONB 字段路径（字段名使用 -> / ->> 表达式）
qc.Where(db.Eq("meta->>'status'", "active"))
```

## 视图支持

PostgreSQL 支持 `CREATE OR REPLACE VIEW` 和 `CREATE MATERIALIZED VIEW`：

```go
vf, ok := db.GetViewFeatures(repo.GetAdapter())

// 普通视图（支持 OR REPLACE）
err := vf.View("active_sessions").
    As("SELECT * FROM sessions WHERE expires_at > NOW()").
    ExecuteCreate(ctx)

// 物化视图
err = vf.View("daily_revenue").
    Materialized().
    As("SELECT DATE(paid_at) AS day, SUM(amount) AS total FROM orders GROUP BY 1").
    ExecuteCreate(ctx)
```

## 版本门槛

| 功能 | 最低版本 |
|---|---|
| json | 9.2 |
| jsonb | 9.4（推荐默认） |
| 物化视图 | 9.3 |
| LATERAL JOIN | 9.3 |
| UPSERT (ON CONFLICT) | 9.5 |
| 生成列 | 12 |
| 存储过程 | 11 |

## 限制与注意事项

- **json 与 jsonb 的选择**：默认映射为 `jsonb`；如需 `json`，设置 `Config.Options.postgres_json_type=json`。
- **物化视图不支持 OR REPLACE**：需要先 DROP 再 CREATE。
- **事务隔离**：默认 READ COMMITTED；分析型工作流建议 REPEATABLE READ 或 SERIALIZABLE。

## 推荐场景

- 生产级 Web/SaaS 应用
- 需要 JSONB + GIN 索引的半结构化数据存储
- 需要递归 CTE、窗口函数的复杂报表
- 需要 LISTEN/NOTIFY 的轻量事件通知
- 重视 SQL 标准合规和数据完整性的场景
