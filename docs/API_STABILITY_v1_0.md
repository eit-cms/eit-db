# EIT-DB v1.0 API 稳定性清单

本文定义 v1.0 的 API 承诺边界，按以下级别分组：

- Stable：v1.0 对外稳定承诺，后续仅做向后兼容演进。
- Compat：兼容层 API，短期保留；不建议新代码新增依赖。
- Internal：框架/路由内部能力，不作为 v1.0 对外稳定承诺。

## 1. Stable（v1.0 承诺）

### Repository 与业务默认写路径

- `NewRepository`
- `NewRepositoryFromAdapterConfig`
- `Repository.NewChangesetExecutor`
- `Repository.WithChangeset`
- `Repository.Query`
- `Repository.QueryRow`
- `Repository.Exec`

### Config 与初始化

- `LoadConfig`
- `LoadAdapterRegistry`
- `Config.Validate`
- `Config.Validation.DefaultLocale`
- `Config.Validation.EnabledLocales`

### Schema/Changeset 主路径

- `NewBaseSchema`
- `NewField`
- `NewChangeset`
- `Changeset.Cast`
- `Changeset.Validate`
- `Changeset.ValidateWithLocale`
- `Changeset.ValidateWithContext`

### Reflection 主路径

- `InferSchema`
- `GetStructFields`
- `GetStructValues`

### Validator 主路径

- `RegexValidator`
- `RangeValidator`
- `EmailValidator`
- `PhoneNumberValidator`
- `URLValidator`
- `PostalCodeValidator`
- `IDCardValidator`
- `SetValidationLocale`
- `GetValidationLocale`
- `RegisterValidationProfile`
- `ConfigureValidationLocales`
- `ValidationLocaleExists`
- `WithValidationLocale`
- `ValidationLocaleFromContext`

### Query v2 主路径

- `Repository.NewQueryConstructor`
- `QueryConstructor`（`Where`/`Select`/`OrderBy`/`Limit`/`Offset`/`Build`）
- 条件构造器：`Eq` `Ne` `Gt` `Lt` `Gte` `Lte` `In` `Like` `Between` `And` `Or` `Not`

## 2. Compat（兼容层）

### 事务与查询兼容入口

- `Repository.Begin`（Deprecated）
- `NewQueryBuilder`（v1，Deprecated）
- `QueryBuilder` / `QueryChain`（v1 兼容层）
- `SetLegacyQueryBuilderWarningEnabled`（迁移期开关）
- `SetLowLevelTransactionWarningEnabled`（迁移期开关）

### 历史 tag 兼容

- struct tag `db:"..."`（兼容，推荐迁移到 `eit_db:"..."`）
- struct tag `gorm:"..."` 兜底解析（仅兼容提取核心字段信息）

## 3. Internal（不纳入 v1.0 稳定承诺）

### 能力路由与自定义特性执行

- `FeatureExecutionMode`
- `FeatureExecutionDecision`
- `CustomFeatureProvider`
- `Repository.DecideFeatureExecution`
- `Repository.ExecuteFeature`

### 运行时全文能力探测与模糊策略路由

- `FullTextRuntimeCapability`
- `FullTextRuntimeInspector`
- `FuzzySearchPlan`
- `Repository.AnalyzeFuzzySearch`
- `Repository.BuildFuzzyConditionWithContext`

### 低层辅助与框架内部表构建路径

- `Repository.GetQueryBuilderCapabilities`
- `DefaultQueryBuilderCapabilities`
- `framework_tables.go` 中的 helper（框架内部）

## 4. 关键说明（v1.0 发布约束）

1. locale 配置是进程级全局行为：
   - 推荐在应用启动阶段统一初始化一次。
   - 多仓储并存时避免在运行中频繁切换全局 locale。

2. Compat API 不新增能力：
   - 兼容层仅保证迁移窗口可用，不承诺新增功能。

3. Internal API 可能在 1.x 内调整：
   - 若需对外开放，将先补文档、示例与回归测试，再升级为 Stable。

## 5. 评审使用方法

Go/No-Go 评审时按以下规则检查：

1. 新增公开 API 必须先归类到 Stable/Compat/Internal。
2. Stable API 变更必须证明向后兼容。
3. Compat API 只允许修复与迁移辅助，不允许扩展语义。
4. Internal API 变更必须更新实现说明与相关测试。
