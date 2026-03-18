# EIT-DB v1.0 升级指南

> 本文面向从 v0.x 升级到 v1.0 的项目，聚合所有必须关注的行为变化与迁移步骤。

## 1. 升级范围

v1.0 的升级重点不是新增功能，而是默认路径和边界收敛：

1. 业务写入路径统一到 Changeset。
2. Adapter/ORM 边界稳定，不暴露 ORM 对象。
3. 查询与验证能力的默认入口明确并可配置。

## 2. 关键行为变化

### 2.1 业务写路径：Changeset 优先

1. 推荐使用 `Repository.NewChangesetExecutor` 或 `Repository.WithChangeset`。
2. `Repository.Begin` 标记为 Deprecated，仅建议用于底层集成（迁移、框架内部、驱动桥接）。
3. 运行时会输出一次性提示，提醒业务层避免直接使用底层事务原语。

### 2.2 ORM 边界：不再对外暴露 GORM

1. `GetGormDB` 已弃用并固定返回 `nil`。
2. `GetRawConn` 约束为返回标准驱动连接（如 `*sql.DB` / `*sql.Tx`），避免上层绕过能力派发。

### 2.3 查询构造：v2 为默认入口

1. 推荐使用 `Repository.NewQueryConstructor(schema)`。
2. v1 `NewQueryBuilder` 仅保留兼容，不建议新代码继续扩展。

### 2.4 反射标签：`eit_db` 优先

1. 推荐统一使用 `eit_db:"..."`。
2. 兼容顺序：`eit_db` > `db` > `gorm`。
3. `db` 仅作为兼容层，不建议新增使用。

### 2.5 校验 locale：初始化配置驱动

1. 在 `Config.validation` 中配置 locale：
   - `default_locale`
   - `enabled_locales`
2. 规则：
   - `default_locale` 必须是已注册 locale。
   - `enabled_locales` 中每个 locale 都必须已注册。
   - 当两者同时设置时，`default_locale` 必须包含在 `enabled_locales` 中。
3. 运行时行为：
   - `SetValidationLocale(locale)` 只允许切到已启用 locale。
   - 上下文传入未启用 locale 时，自动回退到默认 locale。

## 3. 配置模板

### 3.1 单库配置（推荐）

```yaml
# config.yaml
database:
  adapter: sqlite
  database: ./data/app.db
  validation:
    default_locale: zh-CN
    enabled_locales:
      - zh-CN
      - en-US
```

### 3.2 多 Adapter 配置（示例）

```yaml
# adapters.yaml
adapters:
  primary:
    adapter: postgres
    host: localhost
    port: 5432
    username: postgres
    password: ""
    database: app
    ssl_mode: disable
    validation:
      default_locale: zh-CN
      enabled_locales:
        - zh-CN
        - en-US
```

## 4. 迁移执行清单

1. 替换默认写入口：
   - 将业务层 `Repository.Begin` 迁移到 `WithChangeset`/`ChangesetExecutor`。
2. 清理 ORM 依赖：
   - 删除业务代码中对 `GetGormDB` 的依赖。
3. 切换查询构造入口：
   - 新增查询走 `NewQueryConstructor`，v1 仅保留兼容。
4. 统一反射标签：
   - 新代码统一使用 `eit_db` 标签。
5. 落实 locale 配置：
   - 在配置文件中明确 `default_locale` + `enabled_locales`。
   - CMS 多区域场景一次性启用全部目标 locale。

## 5. 回归验证（发布前）

```bash
# 基础回归
go test -count=1 ./...

# 发布门禁（建议 full）
EIT_GATE_ENABLE_LINT=auto EIT_GATE_RUN_OPTIONAL_DB=1 EIT_GATE_CHECK_SCHEMA_PATH=1 bash scripts/release_gate.sh full
```

## 6. 常见问题

1. 为什么设置 locale 后仍报错？
   - 先确认 locale 已注册，再确认 locale 在 `enabled_locales` 中。
2. 为什么上下文 locale 没生效？
   - 未启用 locale 会回退默认 locale，这是预期行为。
3. 是否必须立刻删除 v1 QueryBuilder？
   - 不必须。v1 可短期兼容，但新开发应迁移到 v2。

## 7. 关联文档

1. [README.md](../README.md)
2. [docs/ARCHITECTURE.md](ARCHITECTURE.md)
3. [docs/RELEASE_GATE.md](RELEASE_GATE.md)
4. [docs/V1_0_READINESS_CHECKLIST.md](V1_0_READINESS_CHECKLIST.md)
5. [docs/MIGRATION_GUIDE_v0.4.md](MIGRATION_GUIDE_v0.4.md)
