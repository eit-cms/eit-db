# SQLite Adapter

## 概述

SQLite 是 EIT-DB 默认的嵌入式关系型数据库适配器，无需独立服务进程，适合本地开发、测试、桌面应用以及轻量部署场景。

- **适配器标识**：`"sqlite"`
- **驱动包**：`github.com/mattn/go-sqlite3` (cgo)
- **特性等级**：SQL 关系型 / 本地嵌入

## 快速开始

```go
cfg := &db.Config{
    Adapter: "sqlite",
    SQLite:  &db.SQLiteConnectionConfig{Path: "./myapp.db"},
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

内存数据库（测试专用）：

```go
cfg := &db.Config{
    Adapter: "sqlite",
    SQLite:  &db.SQLiteConnectionConfig{Path: ":memory:"},
}
```

## 支持的能力

### 数据库特性（DatabaseFeatures）

| 能力 | 状态 | 备注 |
|---|---|---|
| 复合主键 | ✅ | |
| 外键约束 | ✅ | 需要 `PRAGMA foreign_keys = ON` |
| 复合外键 | ✅ | |
| 复合索引 | ✅ | |
| 部分索引 | ✅ | WHERE 子句索引 |
| 延迟约束 | ✅ | DEFERRABLE INITIALLY DEFERRED |
| 原生 JSON | ❌ | JSON 存为 TEXT；可通过自定义函数扩展 |
| JSON 路径 | ✅ | `json_extract()` 内置函数 |
| JSON 索引 | ❌ | 无法在 JSON 字段值上建索引 |
| 全文搜索 | ✅ | 依赖 FTS5 扩展（通常内置） |
| RETURNING | ✅ | SQLite 3.35+ |
| UPSERT | ✅ | `ON CONFLICT ... DO UPDATE` |
| 存储过程 | ❌ | |
| 窗口函数 | ✅ | SQLite 3.25+ |
| CTE / 递归 CTE | ✅ | SQLite 3.8.4+ |

### 查询特性（QueryFeatures）

| 特性 | 状态 | 降级方案 |
|---|---|---|
| IN / NOT IN / BETWEEN | ✅ | — |
| LIKE | ✅ | — |
| INNER / LEFT JOIN | ✅ | — |
| FULL OUTER JOIN | ❌ | 应用层模拟 |
| CTE / 递归 CTE | ✅(3.8.4+) | 应用层 |
| 窗口函数 | ✅(3.25+) | 应用层 |
| UNION / EXCEPT / INTERSECT | ✅ | — |
| JSON 路径 | ✅ | — |
| 全文搜索 | ✅ | — |

版本门槛由 `FeatureSupport` 字段运行时声明，启动体检会核查可用性。

## 自定义 SQLite 函数

SQLite Adapter 支持注册自定义函数（通过 cgo sqlite3 扩展机制），可以在查询中使用 Go 实现的函数：

```go
err := db.RegisterCustomSQLiteDriver("sqlite3_custom", map[string]interface{}{
    "REGEXP": func(pattern, value string) bool {
        matched, _ := regexp.MatchString(pattern, value)
        return matched
    },
    "FLOOR_DIV": func(a, b int) int {
        return a / b
    },
})
```

需要在 Config 中指定 driver name：

```go
cfg := &db.Config{
    Adapter: "sqlite",
    SQLite: &db.SQLiteConnectionConfig{
        Path:       "./myapp.db",
        DriverName: "sqlite3_custom",
    },
}
```

## 动态建表

SQLite Adapter 支持 `DynamicTable` 接口，可在运行时动态创建表：

```go
dynTable := db.NewDynamicTable(repo.GetAdapter(), "events_2026_03")
err := dynTable.CreateTable(ctx, mySchema)
```

## 限制与注意事项

- **并发写**：SQLite 默认写操作是串行的；高并发写场景需设置 WAL 模式（`PRAGMA journal_mode=WAL`）。
- **外键默认关闭**：每次连接需要 `PRAGMA foreign_keys = ON`，框架层会在连接时自动启用。
- **cgo 依赖**：`go-sqlite3` 需要 CGO，交叉编译时需要配置对应工具链。
- **全文搜索**：依赖 FTS5 扩展；大多数构建场景已内置，如遇文本搜索失败请验证 FTS5 是否可用。

## 推荐场景

- 本地开发与测试（`:memory:`）
- 轻量桌面或嵌入式应用
- 单机部署的小型 Web 应用
- CI/CD 中代替真实数据库做集成测试
