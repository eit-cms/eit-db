# EIT-DB v1.1 升级指南（Schema 与 Query Builder）

> 本文面向从 v1.0.x 升级到 v1.1.0 的项目，重点说明 Schema 与 Query Builder API 变化。

## 1. 升级范围

v1.1 的重点不是替换整套使用方式，而是在现有 v1.0 基础上补齐关系语义与查询构造能力：

1. Schema 新增关系注册表（HasMany/HasOne/BelongsTo/ManyToMany + Through）。
2. Query Builder 新增 Schema 感知连接接口（JoinWith）。
3. QueryConstructor 接口扩展了 WhereWith/Count/CountWith/CustomMode/SelectCount/Upsert。
4. 默认语义推断优先级调整为：关系注册表 > FK 约束 > 默认 required。
5. MongoDB 与 SQL Server 增加关系策略配置项。

## 2. 关键 API 变化

### 2.1 Schema: 从纯 FK 约束升级到显式关系注册

v1.0 常见写法（仅 FK）：

```go
users := db.NewBaseSchema("users")
orders := db.NewBaseSchema("orders")
orders.AddForeignKey("fk_orders_users", []string{"user_id"}, "users", []string{"id"}, "", "")
```

v1.1 推荐写法（显式关系语义）：

```go
users := db.NewBaseSchema("users")
orders := db.NewBaseSchema("orders")

users.HasMany(orders).Over("user_id", "id")
orders.BelongsTo(users).Over("user_id", "id")
```

ManyToMany 场景建议明确 through：

```go
users := db.NewBaseSchema("users")
roles := db.NewBaseSchema("roles")
userRoles := db.NewBaseSchema("user_roles")

users.ManyToMany(roles).
  Through(userRoles, "user_id", "role_id").
  Named("grants_role")
```

说明：

1. FK 约束回退仍可用，但 v1.1 已不建议只靠 FK 隐式推断。
2. 关系注册后，跨后端语义更稳定（SQL/Neo4j/Mongo 一致性更高）。

### 2.2 Query Builder: Join/LeftJoin 仍兼容，新增 JoinWith

v1.0 常见写法：

```go
qc := db.NewSQLQueryConstructor(users, dialect).
  Join("orders", "users.id = orders.user_id")
```

v1.1 推荐写法：

```go
qc := db.NewSQLQueryConstructor(users, dialect).
  JoinWith(db.NewJoinWith(orders).As("o"))
```

可选语义覆盖：

```go
db.NewJoinWith(orders).Optional()
db.NewJoinWith(orders).Required()
```

说明：

1. JoinWith 优先使用关系注册表推断语义与连接路径。
2. On 子句可继续通过 .On(...) 显式覆盖。

### 2.3 QueryConstructor 接口新增能力

v1.1 新增方法（已由 SQL/Mongo/Neo4j 构造器实现）：

1. WhereWith(builder)
2. Count(fieldName...)
3. CountWith(builder)
4. JoinWith(builder)
5. CustomMode()
6. SelectCount(ctx, repo)
7. Upsert(ctx, repo, cs, conflictColumns...)

最小替换建议：

1. 需要复合条件复用时，用 WhereWith + NewWhereBuilder。
2. 需要计数时，优先 SelectCount 或 Count/CountWith。
3. 需要表达式字段（如 COUNT(*) as c）时，先调用 CustomMode 再 Select。

### 2.4 严格字段校验与 CustomMode

v1.1 在 SQLQueryConstructor 默认模式下会做字段校验。

可能出现的新报错：

1. field does not exist in schema
2. field looks like expression

处理方式：

1. 优先补齐 Schema 字段定义。
2. 确认确实是自定义表达式时，显式启用 CustomMode。

示例：

```go
qc.CustomMode().Select("COUNT(*) as c")
```

## 3. 配置项迁移

### 3.1 SQL Server 新增配置

```yaml
database:
  adapter: sqlserver
  sqlserver:
    many_to_many_strategy: direct_join # 或 recursive_cte
    recursive_cte_depth: 8
    recursive_cte_max_recursion: 100
```

环境变量：

```bash
SQLSERVER_MANY_TO_MANY_STRATEGY=recursive_cte
SQLSERVER_RECURSIVE_CTE_DEPTH=12
SQLSERVER_RECURSIVE_CTE_MAX_RECURSION=250
```

### 3.2 MongoDB 新增配置

```yaml
database:
  adapter: mongodb
  mongodb:
    relation_join_strategy: lookup # 或 pipeline
    hide_through_artifacts: true
```

环境变量：

```bash
MONGODB_RELATION_JOIN_STRATEGY=pipeline
MONGODB_HIDE_THROUGH_ARTIFACTS=false
```

## 4. 迁移执行清单

建议按下面顺序迁移：

1. 先为核心 Schema 补关系注册（HasMany/BelongsTo/ManyToMany+Through）。
2. 将新增查询逐步切到 JoinWith。
3. 复合条件构造切到 WhereWith（可选，非强制）。
4. 计数逻辑统一为 SelectCount 或 CountWith。
5. 如出现表达式字段校验报错，按需启用 CustomMode。
6. 根据后端选择关系策略配置（SQL Server/MongoDB）。

## 5. 回归验证建议

```bash
go test -count=1 ./...
```

重点关注测试类别：

1. schema_relations_test
2. query_builder_v2_test
3. mongo_joinwith_semantic_test
4. mongo_adapter_strategy_test
5. sqlserver_adapter_strategy_test
6. neo4j_adapter_test

## 6. 常见问题

1. 只保留 FK，不注册关系，可以吗？
- 可以。v1.1 仍有 FK 回退逻辑，但不建议作为长期主路径。

2. 旧 Join/LeftJoin 是否必须一次性替换？
- 不需要。旧 API 保持兼容，建议新功能优先用 JoinWith。

3. 为什么升级后同一条查询在不同后端行为更一致？
- 因为语义来源从方言细节转向 Schema 关系语义，后端只做策略化执行。

## 7. 关联文档

1. [关系语义支持说明](RELATION_SEMANTICS.md)
2. [架构总览](ARCHITECTURE.md)
3. [能力支持矩阵](CAPABILITY_MATRIX.md)
4. [v1.1 迁移工具说明](MIGRATION_TOOL_GUIDE_v1_1.md)
5. [v1.0 升级指南](MIGRATION_GUIDE_v1_0.md)
