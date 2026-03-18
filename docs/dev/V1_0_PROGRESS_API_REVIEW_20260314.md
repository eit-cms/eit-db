# v1.0 进度与新增 API 盘点（2026-03-14）

本文用于回答两个问题：

1. v1.0 已完成哪些收敛工作。
2. 最近追加的 API 中，哪些可能影响 v1.0 发布边界。

## 1. 已完成收敛项（M0 视角）

### M0-1 默认业务写路径收敛

- 已完成：业务默认写入入口收敛到 Changeset 执行器。
- 关键落地：
  - `Repository.NewChangesetExecutor`
  - `Repository.WithChangeset`
  - `Repository.Begin` 标注 Deprecated，增加一次性运行提示。

### M0-2 Adapter/ORM 解耦边界

- 已完成：不再对外暴露 ORM 连接。
- 关键落地：
  - `Repository.GetGormDB()` 固定返回 nil。
  - 各 SQL Adapter 的 `GetRawConn()` 返回标准驱动连接（`*sql.DB`）。

### M0-3 能力矩阵与派发体系

- 已基本完成：数据库能力、查询能力、版本门槛、降级策略已有实现与测试。
- 关键落地：
  - `DatabaseFeatures` 增强（版本支持、降级策略、复合外键能力）。
  - `QueryFeatures` 降级策略和能力路由测试补齐。
  - 能力矩阵文档已发布。

### M0-4 发布门禁

- 已完成：
  - `scripts/release_gate.sh`（quick/full）
  - CI workflow（release-gate）
  - 演练记录（2026-03-14）

### M0-5 DDL 路径统一

- 已完成：
  - 框架工具表（schema_migrations）改为 Schema Builder 路径。
  - MySQL/SQLite/PostgreSQL 动态建表改为动态配置 -> Schema -> 方言建表器。

## 2. 近期新增 API 盘点（按主题）

### A. 业务路径与兼容治理

- `SetLowLevelTransactionWarningEnabled`
- `SetLegacyQueryBuilderWarningEnabled`
- `Repository.NewQueryConstructor`
- `Repository.GetQueryBuilderCapabilities`

### B. locale 与校验体系

- `Config.Validation`（`default_locale`、`enabled_locales`）
- `ValidationLocaleExists`
- `ConfigureValidationLocales`
- `GetEnabledValidationLocales`
- `WithValidationLocale`
- `ValidationLocaleFromContext`
- `Changeset.ValidateWithLocale`
- `Changeset.ValidateWithContext`
- `Changeset.ValidateChangeWithLocale`
- `Changeset.ValidateChangeWithContext`

### C. 反射与 Schema 约束

- `BaseSchema.AddPrimaryKey`
- `BaseSchema.AddUniqueConstraint`
- `BaseSchema.AddForeignKey`
- `EqFields`
- `FullText`

### D. 能力路由与全文检索

- `FeatureExecutionMode`
- `FeatureExecutionDecision`
- `CustomFeatureProvider`
- `Repository.DecideFeatureExecution`
- `Repository.ExecuteFeature`
- `FullTextRuntimeInspector`
- `FullTextRuntimeCapability`
- `Repository.AnalyzeFuzzySearch`
- `Repository.BuildFuzzyConditionWithContext`

## 3. 新增 API 问题清单（发布前需要决策）

### P0-1 API 冻结边界未显式分层

问题：
- 近期公开 API 增加较多，但缺少统一的稳定级别标注（stable/compat/internal）。

影响：
- v1.0 发布后兼容承诺不清晰，后续收敛成本高。

建议：
- 在发布前形成 API 清单并分级：
  - Stable（v1.0 承诺）
  - Compat（兼容层，允许弱承诺）
  - Internal（不承诺，文档不对外推荐）

### P0-2 locale 配置是全局状态，存在多仓储互相影响风险

问题：
- `ConfigureValidationLocales` 与 `SetValidationLocale` 使用进程级全局状态。
- `NewRepository` 初始化时会应用 locale 配置，多个仓储并存时可能相互覆盖。

影响：
- CMS 多租户/多上下文并行场景可能出现非预期切换。

建议：
- v1.0 最小策略：
  - 在文档中明确 locale 配置是全局行为。
  - 在应用启动阶段统一初始化一次，不建议运行中频繁切换。
- v1.1 方向：
  - 逐步引入 repository-scoped validator runtime，降低全局副作用。

### P1-3 FeatureExecution 与 FuzzySearch API 仍偏框架内部能力

问题：
- `ExecuteFeature` / `AnalyzeFuzzySearch` 等 API 已公开，但业务使用模式尚未完全稳定。

影响：
- 若作为 stable API 承诺，后续调整空间受限。

建议：
- v1.0 先标为 compat/internal，补充示例后再升级为 stable。

### P1-4 gormAdapter 与“无 ORM 暴露”策略并存带来语义歧义

问题：
- `gormAdapter` 仍存在于代码中，但对外获取 ORM 连接均返回 nil。

影响：
- 用户可能误解为仍推荐 ORM 直连路径。

建议：
- 在文档补一段说明：gormAdapter 为内部桥接实现，不构成对外 ORM 暴露承诺。

## 4. 下一步执行建议（可直接进入）

1. 产出 API 分级清单（阻断项）
   - 目标：给所有新增公开 API 打上 stable/compat/internal 标记。
2. 发布说明补充“全局 locale 限制”
   - 目标：降低上线误用风险。
3. 执行一次基线 full gate 并附 API 变更审阅结论
   - 命令：
     - `EIT_GATE_ENABLE_LINT=auto EIT_GATE_RUN_OPTIONAL_DB=1 EIT_GATE_CHECK_SCHEMA_PATH=1 bash scripts/release_gate.sh full`
4. Go/No-Go 会议按本文件 P0/P1 项逐条过会。
