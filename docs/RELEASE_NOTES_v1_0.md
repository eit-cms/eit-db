# EIT-DB v1.0.0 Release Notes

> 发布日期：2026-03-18  
> 里程碑：从实验性库到稳定可承诺 API 的第一个正式版本

---

## 概述

v1.0.0 不是一次功能爆发式版本，而是边界收敛版本：将过去多次迭代中积累的写路径、适配器层、查询构造、反射推断等分散实现，统一到一套可承诺的 API 界面，并通过发布门（Release Gate）验证稳定性。

---

## 主要变更（v0.4.x → v1.0.0）

### 1. 适配器 API 统一（`adapter-api`）

- 引入 **能力路由（capability routing）**：通过 `CanExecute` / `Dispatch` 机制，将 SQL / NoSQL / embedded 差异收敛在路由层而非业务层。
- 运行时能力检查（runtime checks）：适配器向上声明自身支持的特性（事务、batch、fuzzy search 等），上层按需降级而非崩溃。
- `Repository.Begin` 标记为 **Deprecated**，推荐统一使用 `WithChangeset` / `NewChangesetExecutor`。
- `GetGormDB` 固定返回 `nil`，彻底封闭 ORM 对象外泄路径。

### 2. 新增适配器与后端功能包（`adapters`）

- **Neo4j Adapter**：图数据库适配器，支持 Cypher 查询路由与基本事务。
- **Backend Feature Packs**：
  - MySQL / Postgres / SQLServer 各自独立的 "backend feature" 模块，包含数据库特有能力（全文检索、分区表、json 列操作等）。
- 动态表（Dynamic Table）支持：SQLite / MySQL / Postgres / SQLServer 均已对齐动态表接口。

### 3. 应用层能力扩展（`app-layer`）

- **Presets & Sharding Helper**：内置分库分表预设，基于 `Repository` 封装多 shard 路由。
- **Schema Builder 统一**：DDL 路径不再绕过 Schema 管道直接操作驱动，统一经 `NewBaseSchema` → `SchemaBuilder` 管道派发。
- Reflection 增强：`InferSchema` 支持嵌套结构体、标签兼容层（`eit_db` > `db` > `gorm`）。

### 4. 工具链与发布流程（`tooling`）

- **CLI 统一**：`eit-db-cli`（alias `eit-migrate`）合并所有子命令——`init` / `generate` / `adapter` / `up` / `version`，不再需要分散调用。
- **Release Gate**：`scripts/release_gate.sh` 提供 `quick` 和 `full` 两种门验证：
  - quick：Root 单测 + SQLite 集成 + Schema 路径审计
  - full：在 quick 基础上追加可选 DB 套件 + backend 测试
- **CI 工作流**：`.github/workflows/release-gate.yml` 在 PR / push main / 手动触发时自动执行 gate。

### 5. 文档刷新（`docs`）

- `docs/API_STABILITY_v1_0.md`：明确 Stable / Compat / Internal 三级 API 承诺边界。
- `docs/MIGRATION_GUIDE_v1_0.md`：v0.x → v1.0 行为变化与迁移步骤。
- `docs/ARCHITECTURE.md`：整体架构图更新，反映路由层与能力派发模型。
- `docs/CAPABILITY_MATRIX.md`：各适配器能力对照表。
- `docs/RELEASE_GATE.md`：Gate 运行手册与环境变量说明。

---

## API 稳定性承诺（v1.0 起）

从 v1.0.0 开始，以下 API 进入 **Stable** 承诺，后续版本仅做向后兼容演进：

| 类别 | 代表 API |
|------|---------|
| Repository 核心 | `NewRepository`, `WithChangeset`, `NewChangesetExecutor`, `Query`, `Exec` |
| Config 初始化 | `LoadConfig`, `LoadAdapterRegistry`, `Config.Validate` |
| Schema / Changeset | `NewBaseSchema`, `NewChangeset`, `Changeset.Cast`, `Changeset.Validate` |
| Reflection | `InferSchema`, `GetStructFields`, `GetStructValues` |
| Validators | `RegexValidator`, `RangeValidator`, `EmailValidator`, `PhoneNumberValidator` |
| 查询 v2 | `Repository.NewQueryConstructor`, 所有 v2 构造函数 |

**Compat（短期兼容，不建议新代码依赖）**：

- `Repository.Begin`（已 Deprecated）
- `NewQueryBuilder` v1 系列
- `db` struct tag（请迁移到 `eit_db`）

---

## 破坏性变化

| 变化 | 影响范围 | 迁移方式 |
|------|---------|---------|
| `GetGormDB` 固定返回 `nil` | 直接依赖 GORM 对象的代码 | 改用 `GetRawConn` 或通过 Repository 间接操作 |
| `Repository.Begin` Deprecated | 业务层手动事务 | 迁移到 `WithChangeset` |
| 校验 locale 需在 Config 中预先注册 | 动态切换 locale 的代码 | 在 `Config.Validation.EnabledLocales` 中声明 |

详见 [docs/MIGRATION_GUIDE_v1_0.md](MIGRATION_GUIDE_v1_0.md)。

---

## 发布验证

```
Release Gate (full mode) — 2026-03-18
Root unit/regression tests : PASS (1.266s)
SQLite application suite   : PASS (0.499s)
Schema-path audit          : PASS
Optional DB application    : PASS (1.094s)
Optional DB backend        : PASS (0.610s)
Lint                       : SKIPPED (golangci-lint not installed, auto mode)
─────────────────────────────────────────
Release gate               : PASSED
```

---

## 升级方式

```bash
go get github.com/eit-cms/eit-db@v1.0.0
```

---

## 后续计划

v1.0.0 稳定后：

- 补全 `golangci-lint` CI 集成（目前 auto 模式跳过）
- Neo4j Adapter 事务支持完整化
- Relationship 支持（已有架构设计草稿）
- 性能基准测试套件

---

---

# EIT-DB v1.0.1 Release Notes

> 发布日期：2026-03-19  
> 类型：Patch（Bug Fix）

---

## 概述

v1.0.1 是 v1.0.0 发布后的第一个补丁版本，专项修复 Schema 默认值在方言间的兼容性问题。

---

## Bug 修复

### fix(migration): SQL 默认值方言安全格式化（SQLSTATE 0A000）

**问题**：在 `migration_v2.go` 的 `applyColumnConstraints` 函数中，字符串类型的列默认值（`field.Default`）直接通过 `fmt.Sprint()` 拼接为 SQL，导致裸字符串（如 `active`）在 PostgreSQL DDL 中被解析为列引用（标识符）而非字符串字面量，触发 `SQLSTATE 0A000` 错误。

**修复内容**：
- 新增 `formatDefaultValueForDialect(value interface{}, dialectName string) string` 分发函数，按方言路由格式化逻辑
- 新增 `formatStringDefaultValue(raw string) string`：自动为裸字符串添加 `'` 引号，并转义内部单引号；对已有引号的字符串字面量和 SQL 表达式（`CURRENT_TIMESTAMP`、函数调用、`::` 类型转换、`INTERVAL` 等）原样保留
- SQL Server `BIT` 列的布尔默认值（`true`/`false`）映射为 `1`/`0`
- **向后兼容**：历史写法 `Default: "'guest'"` 不会被重复包裹

**影响范围**：PostgreSQL / MySQL / SQLite / SQL Server 均已更新，全部通过跨方言单元测试及 PostgreSQL 真实集成测试。

**相关 commit**：`c666c62`

---

## 测试覆盖

| 测试文件 | 类型 | 内容 |
|---|---|---|
| `migration_v2_default_value_test.go` | 单元测试 | 4 个跨方言默认值渲染测试 |
| `adapter-application-tests/postgres_integration_test.go` | 集成测试 | 真实 PostgreSQL 默认值写入验证 |

---

## 升级方式

```bash
go get github.com/eit-cms/eit-db@v1.0.1
```

无 API 变更，直接升级即可。

---

# EIT-DB v1.0.2 Release Notes

> 发布日期：2026-03-19  
> 类型：Patch（Critical Fix）

---

## 概述

v1.0.2 针对 v1.0.1 在 PostgreSQL 下暴露的新语法风险进行修复，并处理网络数据库不可达时测试长时间等待的问题。

---

## Bug 修复

### fix(migration): 修复字符串默认值误判为 SQL 表达式导致的 42601 风险

问题说明：v1.0.1 中默认值格式化逻辑会把带括号的字符串（例如 POINT(1,2)）误判为原生 SQL 表达式，生成未加引号的 DEFAULT 片段，在 PostgreSQL 中可能触发 ERROR: syntax error at or near "," (SQLSTATE 42601)。

修复内容：
- 默认值格式化新增字段类型维度，字符串字段默认值不再按“函数表达式”规则放行
- 字符串默认值统一进行 SQL 字面量转义和包裹
- 保持已有引号字面量向后兼容

### fix(mysql): 为连接 DSN 补充 timeout/readTimeout/writeTimeout

问题说明：MySQL 服务不可达时，测试场景下连接握手可能长时间等待，表现为“测试卡死”。

修复内容：
- MySQL 默认 DSN 注入 timeout/readTimeout/writeTimeout
- 自定义 DSN 若未显式声明上述参数，自动补齐

---

## 回归测试

- 新增测试：migration_v2_default_value_test.go
  - TestBuildCreateTableSQL_StringDefaultWithCommaAndParensIsQuoted
- 验证通过：
  - go test -run TestBuildCreateTableSQL_ -count=1 -v ./...
  - go test -run TestAllAdaptersAvailable -count=1 -timeout=40s -v ./...
  - go test -count=1 -timeout=90s ./...

---

## 升级方式

go get github.com/eit-cms/eit-db@v1.0.2

无 API 变更，建议所有 v1.0.1 用户直接升级至 v1.0.2。
