# EIT-DB v1.1 Migration Tool Guide

> 更新日期：2026-03-19  
> 适用版本：v1.0.2+（含后续 v1.1 演进）

---

## 1. 目标与策略

从本次策略调整开始，迁移工具收敛为两条明确路径：

1. **SchemaMigration**：默认和推荐路径。
2. **RawSQLMigration**：仅用于手写 SQL 的高级场景。

同时，**v1 `Migrator` 入口已禁用**，避免历史入口继续扩散不一致行为。

---

## 2. 当前状态（含 PostgreSQL 问题修复）

以下问题已在当前代码版本中修复并回归验证：

1. PostgreSQL 默认值字符串被误解析（`SQLSTATE 0A000`）。
2. 字符串默认值表达式误判导致语法错误（`SQLSTATE 42601`）。
3. 迁移日志占位符在 PostgreSQL 下使用 `?` 的兼容问题。

回归命令示例：

```bash
go test -run 'TestRawSQLMigration_|TestMigratorV1EntryDisabled|TestCompileSQLMigrationOperation_|TestMigrationLogPlaceholder_|TestBuildCreateTableSQL_' -count=1 ./...
```

---

## 3. 迁移入口变更

### 3.1 v1 Migrator 禁用

`Migrator` 的以下方法会返回显式错误：

1. `Register`
2. `Up`
3. `Down`
4. `Status`

错误文案会引导你使用：

1. `MigrationRunner`
2. `SchemaMigration` 或 `RawSQLMigration.ForAdapter(...)`

### 3.2 RawSQLMigration 强制绑定 Adapter

Raw SQL 迁移必须手动调用 `ForAdapter(...)`，并且必须与当前仓库 Adapter 一致。

未绑定或绑定不匹配都会直接失败，防止 SQL 在错误数据库执行。

---

## 4. 推荐实践

### 4.1 推荐：SchemaMigration

```go
migration := db.NewSchemaMigration("20260319190000", "create_users")

users := db.NewBaseSchema("users")
users.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true, Autoinc: true})
users.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
users.AddField(&db.Field{Name: "created_at", Type: db.TypeTime, Null: false, Default: "CURRENT_TIMESTAMP"})

migration.CreateTable(users)
```

适用场景：

1. 表/字段/约束的常规演进。
2. 需要跨 SQL 方言兼容。

### 4.2 高级：RawSQLMigration（必须绑定 adapter）

```go
migration := db.NewRawSQLMigration("20260319191000", "postgres_only_feature").
	ForAdapter("postgres").
	AddUpSQL(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_name ON users(name)`).
	AddDownSQL(`DROP INDEX IF EXISTS idx_users_name`)
```

适用场景：

1. 数据库特有能力（方言专属语法）。
2. SchemaMigration 暂未覆盖的高级 DDL。

---

## 5. 支持的 RawSQL adapter 绑定值

当前建议值：

1. `postgres`
2. `mysql`
3. `sqlite`
4. `sqlserver`

注意：

1. `postgresql` 会归一化为 `postgres`。
2. `mongo` 会归一化为 `mongodb`（用于未来扩展）。

---

## 6. CLI 生成策略

`eit-db-cli generate --type sql` 生成的模板已更新，默认提示你必须调用 `ForAdapter(...)`。

建议流程：

1. 生成文件后第一步先写 `ForAdapter("...")`。
2. 再填写 `AddUpSQL` / `AddDownSQL`。

---

## 7. PostgreSQL 用户注意事项

1. 字符串默认值请以字符串语义定义，不要用裸表达式混写。
2. 迁移日志占位符由方言自动编译，不要手动拼接 `?`。
3. 使用 RawSQLMigration 时，确保 `ForAdapter("postgres")` 与当前仓库配置一致。

---

## 8. NoSQL 迁移说明（阶段性）

MongoDB / Neo4j 迁移能力将作为下一阶段独立实现。当前先完成 SQL 迁移入口收敛，避免在语义尚未稳定前引入不一致行为。

后续方向：

1. 复用 SchemaMigration 统一入口。
2. 由各 Adapter 编译为各自迁移命令风格（而非复用 SQL 语法）。

---

## 9. 关联文档

1. [v1.1 路线图](V1_1_ROADMAP.md)
2. [v1.0 升级指南](MIGRATION_GUIDE_v1_0.md)
3. [v1.0 Release Notes](RELEASE_NOTES_v1_0.md)