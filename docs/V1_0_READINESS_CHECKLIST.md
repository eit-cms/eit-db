# EIT-DB v1.0 Readiness Checklist

本文用于发布前收敛，不讨论“还能做什么”，只讨论“1.0 必须稳定什么”。

## 目标定义

v1.0 的目标不是增加功能数量，而是确保：

1. 默认路径正确（业务层不容易走错）。
2. 跨数据库行为可预期（能力差异有声明、有降级、有测试）。
3. 对外 API 语义稳定（弃用清晰，迁移成本可控）。

## M0 必须完成（发布阻断项）

### M0-1 默认业务写路径收敛到 Changeset

- 状态：已完成
- 当前进展：
  - 已提供 `Repository.NewChangesetExecutor` 和 `Repository.WithChangeset`。
  - `Repository.Begin` 已标注 Deprecated，且新增一次性运行时提示。
  - 主 README 已切换为 ChangesetExecutor/WithChangeset 默认示例。
  - 反射指南的事务示例已改为“业务层推荐 WithChangeset，Begin 仅底层使用”。
  - 已完成示例文案巡检：`examples/` 当前为空；`adapter-application-tests/README.md` 已补充“适配器测试范围 vs 业务默认写路径”说明。
- 剩余工作：
  - 无。

### M0-2 Adapter/ORM 解耦边界稳定

- 状态：已完成
- 当前进展：
  - `GetGormDB` 已弃用并固定返回 nil。
  - `GetRawConn` 约束为返回标准驱动连接（如 `*sql.DB`），不暴露 ORM 对象。
  - 示例已移除 `GetGormDB` 用法。
- 验收标准：
  - 业务侧不依赖 ORM API 即可完成常见写流程。

### M0-3 跨数据库能力矩阵冻结（声明 + 测试）

- 状态：基本完成（待发布前最终校对）
- 当前进展：
  - 已有 `DatabaseFeatures` / `QueryFeatures` + fallback + version support。
  - 复合键、复合外键、全文搜索降级、自定义能力路由已有测试。
  - 已新增对外矩阵文档：`docs/CAPABILITY_MATRIX.md`。
- 剩余工作：
  - 发布前逐条校对矩阵与测试命名的一致性（避免文案与测试漂移）。

### M0-4 发布级回归门禁

- 状态：已完成
- 当前进展：
  - 单测覆盖面较完整，`go test ./...` 常态通过。
  - 已新增发布门禁脚本：`scripts/release_gate.sh`（quick/full 两档）。
  - 已新增门禁说明：`docs/RELEASE_GATE.md`。
  - 已新增 CI 草案：`.github/workflows/release-gate.yml`（PR/push 到 main 触发）。
  - 已完成 full gate 候选演练并留档：`docs/dev/RELEASE_GATE_REHEARSAL_20260314.md`。
- 剩余工作：
  - 无。

### M0-5 框架工具表/动态表 DDL 路径统一

- 状态：已完成
- 当前进展：
  - migration v1/v2 的 `schema_migrations` 创建路径已统一为 Schema Builder。
  - MySQL/SQLite/PostgreSQL 动态建表路径已统一转为“动态配置 -> Schema -> 方言建表器”管线。
  - 已清理动态表 Adapter 中失活的并行字段映射实现，降低后续风格回退风险。
  - `README.md` 与 `docs/ARCHITECTURE.md` 已补充统一约束说明。
- 验收标准：
  - 框架内部表与动态表不存在并行手写 `CREATE TABLE` 主路径。
  - DDL 语义变更优先在共享 Schema/方言建表层生效，而非分散在各 Adapter。

## M1 建议完成（高价值非阻断）

1. 发布 API 稳定清单：
  - 已落地文档：`docs/API_STABILITY_v1_0.md`。
  - 发布评审按 Stable/Compat/Internal 边界执行。
2. 升级指南：
  - 从 v0.x 到 v1.0 的迁移清单（QueryBuilder v1、Begin、GetGormDB、validation locale 配置相关）。
  - 明确 `validation.default_locale` / `validation.enabled_locales` 的配置模板与回退行为。
  - 已落地文档：`docs/MIGRATION_GUIDE_v1_0.md`。
3. 可观测性：
   - 关键降级路径增加结构化日志字段（feature/mode/fallback/reason）。

## 当前 1.0 收尾建议（按执行顺序）

1. 文档冻结（D+1）：
  - README、架构文档、`docs/MIGRATION_GUIDE_v1_0.md` 已覆盖主要迁移点，进入最终审阅与冻结阶段。
  - 进度与新增 API 盘点：`docs/dev/V1_0_PROGRESS_API_REVIEW_20260314.md`。
2. 发布门禁冻结（D+1）：
  - 固定候选发布命令：`EIT_GATE_ENABLE_LINT=auto EIT_GATE_RUN_OPTIONAL_DB=1 EIT_GATE_CHECK_SCHEMA_PATH=1 bash scripts/release_gate.sh full`。
3. 候选发布演练（D+2）：
  - 执行一次全量演练并存档结果（命令、环境、失败重试、最终产物）。
  - 最新基线演练记录：`docs/dev/RELEASE_GATE_REHEARSAL_20260314_BASELINE.md`。
4. Go/No-Go 评审（D+3）：
  - 对照 M0 与门禁结果开评审会，形成发布结论与回滚预案。

## M2 可延后（1.1+）

1. 多适配器路由策略自动化（按查询类型和代价模型动态选择）。
2. 更细粒度性能基准（按适配器/查询类型/数据规模）。
3. 更强的插件自动探测缓存机制（PG/SQLite/SQL Server）。
4. 位置信息类型增强（`TypeLocation`）：PostGIS/空间索引/距离查询能力与降级策略统一。

## 当前里程碑建议

- 里程碑 A（默认路径收敛）：
  - 完成 README 与示例更新，默认写路径统一为 Changeset。
- 里程碑 B（能力边界公开）：
  - 发布能力矩阵文档并与测试项对齐。
- 里程碑 C（发布门禁）：
  - 完成 CI 门禁配置并跑通候选版本演练。

## 完成判定（v1.0 Go/No-Go）

满足以下条件可发布 v1.0：

1. M0-1 ~ M0-5 全部完成。
2. 所有 Deprecated API 在文档中有迁移路径。
3. `go test ./...` 与发布门禁流水线均稳定通过。
