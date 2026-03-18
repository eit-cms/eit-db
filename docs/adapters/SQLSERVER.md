# SQL Server Adapter

## 概述

SQL Server 适配器支持 Microsoft SQL Server 和 Azure SQL Database，适合企业级 Windows 环境或已使用 Microsoft 技术栈的系统。

- **适配器标识**：`"sqlserver"`
- **驱动包**：`github.com/microsoft/go-mssqldb`
- **特性等级**：SQL 关系型 / 企业级

## 快速开始

```go
cfg := &db.Config{
    Adapter: "sqlserver",
    SQLServer: &db.SQLServerConnectionConfig{
        Host:     "localhost",
        Port:     1433,
        Database: "MyApp",
        Username: "sa",
        Password: "StrongP@ssw0rd",
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
| 复合索引 | ✅ | |
| 部分索引 | ✅ | 过滤索引（Filtered Index） |
| 延迟约束 | ❌ | SQL Server 不支持 DEFERRABLE |
| 原生 JSON | ❌ | JSON 以 NVARCHAR 存储，通过内置函数操作 |
| JSON 路径 | ✅ | `JSON_VALUE()` / `JSON_QUERY()` |
| JSON 索引 | ✅ | 通过计算列间接实现 |
| 全文搜索 | ✅ | Full-Text Search Service |
| RETURNING | ✅ | `OUTPUT INSERTED.*` 子句 |
| UPSERT | ✅ | `MERGE ... WHEN MATCHED / WHEN NOT MATCHED` |
| 存储过程 | ✅ | T-SQL |
| 窗口函数 | ✅ | |
| CTE / 递归 CTE | ✅ | |
| 物化 CTE | ❌ | 无 MATERIALIZED 关键字 |

### 查询特性（QueryFeatures）

| 特性 | 状态 | 备注 |
|---|---|---|
| IN / NOT IN / BETWEEN | ✅ | — |
| LIKE | ✅ | — |
| INNER / LEFT / RIGHT / CROSS JOIN | ✅ | — |
| FULL OUTER JOIN | ✅ | — |
| CTE / 递归 CTE | ✅ | — |
| 窗口函数 | ✅ | — |
| UNION / EXCEPT / INTERSECT | ✅ | — |
| JSON 路径 | ✅ | — |
| 全文搜索 | ✅ | 需 FTS Service 已安装 |
| INSERT IGNORE | ❌ | 使用 MERGE 替代 |
| LIMIT / OFFSET | ✅ | `OFFSET n ROWS FETCH NEXT m ROWS ONLY` |

## 高级特性

### MERGE（UPSERT）

SQL Server 的 `MERGE` 语句支持复杂的 UPSERT 逻辑：

```go
// 通过 ExecuteCustomFeature 获取 SQL Server 特有的 MERGE 模板
result, err := repo.GetAdapter().ExecuteCustomFeature(ctx, "upsert_merge", map[string]interface{}{
    "target": "employees",
    "source": "employee_updates",
})
```

### OUTPUT（RETURNING 等价）

SQL Server 通过 `OUTPUT` 子句返回受影响的行：

```sql
INSERT INTO orders (customer_id, total)
OUTPUT INSERTED.id, INSERTED.created_at
VALUES (@p1, @p2)
```

### 视图支持

SQL Server 支持 `CREATE OR ALTER VIEW`，通过 `ViewFeatures` 统一管理：

```go
vf, ok := db.GetViewFeatures(repo.GetAdapter())
err := vf.View("active_accounts").
    As("SELECT id, name FROM accounts WHERE status = 'active'").
    ExecuteCreate(ctx)
```

### 动态建表

SQL Server Adapter 支持 `DynamicTable` 接口：

```go
dynTable := db.NewDynamicTable(repo.GetAdapter(), "audit_log_2026_Q1")
err := dynTable.CreateTable(ctx, auditSchema)
```

## SQL Server 特有语法说明

| SQL 标准 | SQL Server 等价 |
|---|---|
| `LIMIT n OFFSET m` | `OFFSET m ROWS FETCH NEXT n ROWS ONLY` |
| `RETURNING` | `OUTPUT INSERTED.*` |
| `UPSERT` | `MERGE ... WHEN MATCHED THEN UPDATE ...` |
| `CREATE OR REPLACE VIEW` | `CREATE OR ALTER VIEW` |

Query Constructor 和 Migration 工具已对上述差异做了方言适配。

## 限制与注意事项

- **JSON 无原生类型**：JSON 数据以 `NVARCHAR(MAX)` 存储，需通过 `JSON_VALUE` / `JSON_QUERY` 函数操作。
- **DEFERRABLE 不支持**：外键约束验证不能延迟到事务结束。
- **区分大小写**：默认 collation 根据数据库配置而定，建议统一使用 `CI_AS` (Case-Insensitive, Accent-Sensitive)。
- **全文搜索服务**：需要 SQL Server Full-Text Search 服务组件已安装。

## 推荐场景

- Microsoft / Windows 技术栈的企业系统
- Azure SQL Database 云数据库
- 需要 T-SQL 存储过程和企业级安全特性的场景
- ERP / 金融系统等依赖 SQL Server 特有功能的迁移路径
