# MySQL Adapter

## 概述

MySQL 适配器提供完整的 MySQL / MariaDB 关系型数据库支持，适合绝大多数 Web 应用和业务系统场景。

- **适配器标识**：`"mysql"`
- **驱动包**：`github.com/go-sql-driver/mysql`
- **特性等级**：SQL 关系型 / 服务端

## 快速开始

```go
cfg := &db.Config{
    Adapter: "mysql",
    MySQL: &db.MySQLConnectionConfig{
        Host:     "localhost",
        Port:     3306,
        Database: "myapp",
        Username: "root",
        Password: "secret",
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
| 外键约束 | ✅ | InnoDB 引擎 |
| 复合外键 | ✅ | |
| 复合索引 | ✅ | |
| 部分索引 | ❌ | MySQL 不支持 WHERE 子句部分索引 |
| 延迟约束 | ❌ | MySQL 不支持 DEFERRABLE |
| 原生 JSON | ✅ | MySQL 5.7+ |
| JSON 路径 | ✅ | `JSON_EXTRACT()` |
| JSON 索引 | ✅ | MySQL 8.0.13+（生成列 + 索引） |
| 全文搜索 | ✅ | InnoDB FTS（MySQL 5.6+） |
| RETURNING | ❌ | 通过 `LAST_INSERT_ID()` 取回主键 |
| UPSERT | ✅ | `ON DUPLICATE KEY UPDATE` |
| 窗口函数 | ✅ | MySQL 8.0+ |
| CTE / 递归 CTE | ✅ | MySQL 8.0+ |

### 版本门槛

| 功能 | 最低版本 | 降级策略 |
|---|---|---|
| window_functions | 8.0 | 应用层模拟 |
| cte / recursive_cte | 8.0 | 应用层模拟 |
| native_json / json_path | 5.7 | 应用层 |
| json_index | 8.0.13 | 应用层 |
| generated（生成列） | 5.7 | 应用层 |

### 查询特性（QueryFeatures）

| 特性 | 状态 | 降级方案 |
|---|---|---|
| IN / NOT IN / BETWEEN | ✅ | — |
| LIKE / REGEXP | ✅ | — |
| INNER / LEFT / RIGHT JOIN | ✅ | — |
| FULL OUTER JOIN | ❌ | LEFT + RIGHT UNION 模拟 |
| CTE / 递归 CTE | ✅(8.0+) | 应用层 |
| 窗口函数 | ✅(8.0+) | 应用层 |
| UNION / EXCEPT / INTERSECT | ✅ | INTERSECT/EXCEPT 在 8.0+ |
| 全文搜索 | ✅ | — |
| JSON 路径 | ✅ | — |
| INSERT IGNORE | ✅ | MySQL 特有扩展 |

## 动态建表

MySQL Adapter 实现了 `DynamicTable` 接口，支持运行时动态创建表：

```go
dynTable := db.NewDynamicTable(repo.GetAdapter(), "logs_2026_03")
err := dynTable.CreateTable(ctx, logSchema)
```

## 视图支持

支持通过 `ViewFeatures` 创建普通视图（不支持物化视图）：

```go
vf, ok := db.GetViewFeatures(repo.GetAdapter())
err := vf.View("active_users").
    As("SELECT id, name FROM users WHERE active = 1").
    ExecuteCreate(ctx)
```

## 全文搜索

MySQL InnoDB FTS 索引支持 `MATCH ... AGAINST` 语法，通过 `InspectFullTextRuntime` 可以动态检查是否可用：

```go
rt, err := repo.GetAdapter().InspectFullTextRuntime(ctx)
if rt.NativeSupported {
    // 使用原生全文搜索
}
```

## 限制与注意事项

- **RETURNING 不支持**：插入后通过 `LAST_INSERT_ID()` 获取自增主键。
- **部分索引不支持**：需要升级到 MySQL 8.0.13+ 或改用生成列模拟。
- **事务隔离**：默认 REPEATABLE READ，可通过连接参数调整。
- **charset**：推荐使用 `utf8mb4` 支持完整 Unicode（含 Emoji）。

## 推荐场景

- 主流 Web 应用和 SaaS 系统
- 已有 MySQL 基础设施的迁移路径
- InnoDB 事务 + 外键约束重度使用场景
