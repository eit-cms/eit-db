# EIT-DB v0.4 迁移指南

> 本文面向 v0.3.x 及更早版本用户，帮助你快速迁移到 v0.4.x（含 v0.4.3、v0.4.4-preview 等）。

---

## 1. 主要变更概览

- **Repository/Schema/Changeset 架构全面升级**，不再对外暴露 GORM
- **Schema 支持 Go 结构体反射自动生成**，也可手动声明
- **Adapter 能力声明与功能派发**，支持跨数据库特性降级
- **QueryFeatures/DatabaseFeatures 系统**，统一特性检测与降级策略
- **三层查询构造器**，支持多 SQL 方言与未来扩展
- **查询构造器使用策略**：v2 优先，v1 仅兼容
- **文档结构调整**，用户文档与开发文档分离

---

## 2. 迁移步骤

### 2.1 依赖升级

```bash
go get github.com/eit-cms/eit-db@v0.4.3
```

### 2.2 代码适配

#### 2.2.1 Repository 替换
- 原：直接操作 GORM
- 新：统一通过 `Repository` 进行所有数据库操作

```go
repo, err := db.NewRepository(config)
// ...
repo.Query(...)
repo.Exec(...)
```

#### 2.2.2 Schema 定义
- 原：GORM struct/tag 或自定义 struct
- 新：推荐用 `db.NewBaseSchema` 手动声明，或用 `db.InferSchema` 从 struct 自动生成

```go
// 手动
schema := db.NewBaseSchema("users")
schema.AddField(db.NewField("id", db.TypeInteger).PrimaryKey().Build())
// ...

// 自动
schema, err := db.InferSchema(User{})
```

#### 2.2.3 不再暴露 GORM
- `repo.GetGormDB()` 已废弃，不建议直接依赖 GORM
- 所有操作请用 Repository API

#### 2.2.4 Adapter 注册与多数据源
- 推荐用 YAML/代码注册多 Adapter
- 详见 [README.md](../README.md) 示例

#### 2.2.5 Changeset 验证
- 统一用 `db.NewChangeset(schema)` 进行数据校验

#### 2.2.6 查询构造器迁移（重要）
- 推荐：使用 v2 `Repository.NewQueryConstructor(schema)`
- 兼容：`NewQueryBuilder` (v1) 仅保留历史兼容，不建议新代码继续使用

```go
qc, err := repo.NewQueryConstructor(schema)
if err != nil {
	panic(err)
}

sql, args, err := qc.
	Select("id", "email").
	Where(db.Eq("status", "active")).
	OrderBy("created_at", "DESC").
	Limit(20).
	Build(context.Background())
```

如需短期继续使用 v1，可关闭兼容提示：

```go
db.SetLegacyQueryBuilderWarningEnabled(false)
```

---

## 3. 特性声明与降级

- 通过 `DatabaseFeatures`/`QueryFeatures` 检查数据库能力
- 统一降级策略（如 FULL OUTER JOIN、JSON、CTE 等）
- 详见 [docs/ARCHITECTURE.md](ARCHITECTURE.md) 和 [docs/REFLECTION_GUIDE.md](REFLECTION_GUIDE.md)

### 3.1 反射 Tag 兼容策略

- 推荐：统一使用 `eit_db:"..."`，避免与其他库 `db` tag 命名冲突。
- 兼容：`db:"..."` 在 `v1.x` 继续可用，但不建议新增。
- 规划：`v2` 将评估移除 `db` 兼容层（以正式迁移公告为准）。

---

## 4. 典型问题与解决

- **GORM 相关代码报错**：请全部迁移到 Repository API
- **Schema 反射失败**：优先使用 `eit_db:"字段名,约束"`（避免与其他库 `db` tag 冲突；`db` 仍兼容）
- **多数据库适配**：用 YAML/代码注册，避免硬编码
- **特性不支持**：用 `HasFeature`/`GetFallbackStrategy` 检查并降级

---

## 5. 参考文档

- [README.md](../README.md)
- [docs/ARCHITECTURE.md](ARCHITECTURE.md)
- [docs/REFLECTION_GUIDE.md](REFLECTION_GUIDE.md)
- [docs/QUERY_FEATURES.md](../.dev-docs/QUERY_FEATURES.md)
- [docs/ADAPTER_WORKFLOW.md](../.dev-docs/ADAPTER_WORKFLOW.md)

---

如有疑问，欢迎提交 Issue 或 PR。
