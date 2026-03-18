# Adapter Provider Guide

本文面向 Adapter 提供者（扩展方言/后端实现者），目标是让你可以按统一契约接入 EIT-DB，并与现有 Query/Features/降级机制兼容。

## 1. 适用范围

- 你要新增一个数据库 Adapter（SQL 或非 SQL）。
- 你要把已有 Adapter 以更规范方式接入 `RegisterAdapterConstructor`。
- 你要为 Adapter 提供 QueryBuilder、Feature 声明和运行时能力边界。

## 2. 最小实现契约

核心接口定义在 [adapter.go](adapter.go#L32)。

你至少需要实现：

- 连接生命周期：`Connect` / `Close` / `Ping`
- 事务入口：`Begin`（若后端不支持，返回明确错误）
- SQL 形态入口：`Query` / `QueryRow` / `Exec`（非 SQL 后端可返回不支持错误）
- `GetRawConn`（返回底层连接对象，避免返回 ORM 对象）
- 定时任务接口（不支持时统一返回错误）
- `GetQueryBuilderProvider`
- `GetDatabaseFeatures`
- `GetQueryFeatures`

## 3. 注册方式（推荐）

推荐使用构造函数注册（避免硬编码工厂）：

```go
func init() {
    _ = RegisterAdapterConstructor("your_adapter", NewYourAdapter)
}
```

构造函数签名要求：

```go
func NewYourAdapter(cfg *Config) (*YourAdapter, error)
```

注册 API 定义见 [adapter.go](adapter.go#L284)。

## 4. 配置与连接建议

推荐模式：

1. 在构造函数中做轻量校验（`config.Validate()`）并保存解析后的配置。
2. 在 `Connect` 中真正建立连接，并做一次短超时健康检查。
3. 保持 `Connect` 幂等（已连接则直接返回）。

## 5. QueryBuilder Provider 接入

`QueryConstructorProvider` 定义见 [adapter.go](adapter.go#L961)。

- SQL 后端：通常返回 SQL 构造器 Provider。
- 非 SQL 后端：返回该后端自己的 Provider（例如 BSON/Cypher）。

同时建议配套完善 `QueryBuilderCapabilities`，让上层能按能力路由。

## 6. Feature 声明要求

需要实现两类声明：

- `GetDatabaseFeatures()`：数据库级能力（索引、FK、JSON、全文、事务等）
- `GetQueryFeatures()`：查询构造能力（JOIN、CTE、窗口、JSON 操作等）

实践建议：

- 对“不支持”的能力给出明确 fallback/notes，而不是静默忽略。
- 这两份声明会影响运行时路由、降级策略与文档一致性。

## 7. 非 SQL Adapter 参考源码（节选）

### 7.1 MongoDB 参考（节选）

来源： [mongo_adapter.go](mongo_adapter.go#L20)

```go
func NewMongoAdapter(config *Config) (*MongoAdapter, error) {
    if config == nil {
        return nil, fmt.Errorf("config cannot be nil")
    }
    if err := config.Validate(); err != nil {
        return nil, err
    }
    resolved := config.ResolvedMongoConfig()
    return &MongoAdapter{database: resolved.Database, uri: resolved.URI}, nil
}

func (a *MongoAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
    return nil, fmt.Errorf("mongodb: sql query not supported")
}

func (a *MongoAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
    return NewMongoQueryConstructorProvider()
}
```

要点：

- 保留统一接口，但对 SQL 入口返回明确不支持错误。
- 通过独立 Provider 接入 BSON 查询构造路径。

### 7.2 Neo4j 参考（节选）

来源： [neo4j_adapter.go](neo4j_adapter.go#L29)

```go
func NewNeo4jAdapter(config *Config) (*Neo4jAdapter, error) {
    if config == nil {
        return nil, fmt.Errorf("config cannot be nil")
    }
    if err := config.Validate(); err != nil {
        return nil, err
    }
    resolved := config.ResolvedNeo4jConfig()
    return &Neo4jAdapter{
        uri: resolved.URI, username: resolved.Username,
        password: resolved.Password, database: resolved.Database,
    }, nil
}

func (a *Neo4jAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
    return nil, fmt.Errorf("neo4j: sql query not supported; use custom feature or native driver")
}

func (a *Neo4jAdapter) QueryCypher(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
    // 使用 Neo4j session.ExecuteRead 执行 Cypher
    // 并把 record.AsMap() 返回给上层
}
```

要点：

- SQL 接口保持边界清晰，Cypher 走专用执行通道。
- 通过 Repository 扩展方法暴露后端原生能力（例如 `QueryCypher` / `ExecCypher`）。

## 8. 分层建议

- 应用层语义（跨库统一）放在通用 API（如 Condition / Presets）。
- Adapter 细节（BSON pipeline、Cypher 特性、方言 Hint）留在 Adapter 侧。

分层规范参考： [docs/PRESETS_CONVENTIONS.md](docs/PRESETS_CONVENTIONS.md)

## 9. 上线前检查清单

- 接口实现完整（Adapter + Provider + Features）
- `Connect` 可重入且有健康检查
- 不支持能力有明确错误或 fallback 声明
- 至少有初始化/健康检查/核心查询路径测试
- 文档补齐（快速开始 + 能力边界 + 降级策略）
