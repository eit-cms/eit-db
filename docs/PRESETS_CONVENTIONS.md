# Presets 分层与命名规范

本文定义 `Presets` 的分层边界与命名约定，避免应用层语义与 Adapter 实现耦合。

## 1. 分层边界

- `Presets` 仅承载应用层语义预置（adapter-agnostic）。
- 应用层预置返回统一 `Condition`，可直接用于 Query Builder：
  - 例如：`Presets.Date.ActiveUsersInBusinessHours()`
- Adapter 细节预置不进入 `Presets`：
  - 例如 Mongo `pipeline`、`collection`、`bson`、Neo4j `cypher`、SQL 方言特定 hint。

## 2. 统一入口约定

- 新增应用层预置时，优先挂载到：`Presets.<Domain>.<UseCase>()`
- 允许保留顶层兼容函数，但新代码建议优先走 `Presets`。

示例：

```go
qc.Where(db.Presets.Date.OrdersInCurrentQuarter())
```

## 3. 应用层命名约定

- 命名模板：`<Subject><Intent><Scope>`
  - 例如：`ActiveUsersInBusinessHours`
- 默认版本与可配置版本成对出现：
  - 默认：`Xxx()`
  - 可配置：`XxxBy(...)`
- 时间窗口条件统一使用业务语义词：
  - `CurrentMonth`、`CurrentQuarter`、`BusinessDays`、`BusinessHours`

## 4. Adapter 层预置命名约定（不放入 Presets）

- 命名应显式带适配器域，避免误归类：
  - Mongo：`Mongo...` 前缀或放在 `mongo_*` 文件中
  - Neo4j：`Neo4j...` 前缀或放在 `neo4j_*` 文件中
  - SQL 方言：`SQL...` / `Postgres...` / `MySQL...` 等前缀
- Adapter 预置应返回 Adapter 本地结构，而不是通用 `Condition`。

## 5. 文件组织建议

- 应用层预置：
  - 入口：`presets.go`
  - 具体实现：`*_condition_presets.go`
  - 测试：`*_condition_presets_test.go`
- Adapter 层预置：
  - 放在对应 Adapter 文件域，不与 `presets.go` 同目录命名空间混用。

## 6. 审查清单

新增预置前，确认：

- 是否可表达为业务语义而非数据库语法？
- 是否不依赖 Adapter 驱动类型（mongo/bson/cypher 等）？
- 是否提供了默认版与可配置版？
- 是否补充了 Query Builder 集成测试与 README 示例？
