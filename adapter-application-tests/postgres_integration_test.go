package adapter_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
)

func setupPostgresRepo(t *testing.T) (*db.Repository, func()) {
	config := postgresIntegrationConfig()
	if err := config.Validate(); err != nil {
		failIntegrationEnv(t, "PostgreSQL", err)
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		failIntegrationEnv(t, "PostgreSQL", err)
	}

	if err := repo.Ping(context.Background()); err != nil {
		failIntegrationEnv(t, "PostgreSQL", err)
	}

	cleanup := func() {
		_ = repo.Close()
	}

	return repo, cleanup
}

func buildPostgresUserSchema(tableName string) db.Schema {
	schema := db.NewBaseSchema(tableName)
	schema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true, Autoinc: true})
	schema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
	schema.AddField(&db.Field{Name: "email", Type: db.TypeString, Null: false, Unique: true})
	schema.AddField(&db.Field{Name: "age", Type: db.TypeInteger, Null: false})
	return schema
}

func TestPostgresChangesetValidation(t *testing.T) {
	schema := buildPostgresUserSchema("pg_users_validation")

	valid := db.NewChangeset(schema).
		Cast(map[string]interface{}{"name": "Alice", "email": "alice@example.com", "age": 25}).
		ValidateRequired([]string{"name", "email", "age"}).
		ValidateFormat("email", `.+@.+\..+`).
		ValidateNumber("age", map[string]interface{}{"min": 1})

	if !valid.IsValid() {
		t.Fatalf("期望 changeset 有效，得到错误: %v", valid.Errors())
	}

	invalid := db.NewChangeset(schema).
		Cast(map[string]interface{}{"name": "", "email": "invalid", "age": 0}).
		ValidateRequired([]string{"name", "email", "age"}).
		ValidateFormat("email", `.+@.+\..+`).
		ValidateNumber("age", map[string]interface{}{"min": 1})

	if invalid.IsValid() {
		t.Fatalf("期望 changeset 无效，但未返回错误")
	}
}

func TestPostgresSchemaMigration(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	defer cleanup()

	tableName := fmt.Sprintf("pg_users_%d", time.Now().UnixNano())
	schema := buildPostgresUserSchema(tableName)
	version := fmt.Sprintf("%d", time.Now().UnixNano())
	migration := db.NewSchemaMigration(version, "create_pg_users").CreateTable(schema)

	ctx := context.Background()
	if err := migration.Up(ctx, repo); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}

	qb := db.NewQueryBuilder(schema, repo)
	if _, err := qb.SelectCount(""); err != nil {
		t.Fatalf("查询表失败: %v", err)
	}

	if err := migration.Down(ctx, repo); err != nil {
		t.Fatalf("回滚失败: %v", err)
	}

	var exists bool
	err := repo.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)
	`, tableName).Scan(&exists)
	if err != nil {
		t.Fatalf("检查表存在性失败: %v", err)
	}
	if exists {
		t.Fatalf("表回滚后仍存在: %s", tableName)
	}
}

func TestPostgresSchemaMigration_DefaultStringLiteralApplied(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	defer cleanup()

	tableName := fmt.Sprintf("pg_default_literal_%d", time.Now().UnixNano())
	schema := db.NewBaseSchema(tableName)
	schema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true, Autoinc: true})
	schema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
	schema.AddField(&db.Field{Name: "status", Type: db.TypeString, Null: false, Default: "active"})

	migration := db.NewSchemaMigration("20260319000100", "create_default_literal_table").CreateTable(schema)
	ctx := context.Background()

	if err := migration.Up(ctx, repo); err != nil {
		t.Fatalf("迁移失败（默认值方言映射可能错误）: %v", err)
	}
	defer func() {
		_ = migration.Down(ctx, repo)
	}()

	if _, err := repo.Exec(ctx, fmt.Sprintf(`INSERT INTO "%s" ("name") VALUES ($1)`, tableName), "alice"); err != nil {
		t.Fatalf("插入失败（默认值应由数据库自动填充）: %v", err)
	}

	var status string
	if err := repo.QueryRow(ctx, fmt.Sprintf(`SELECT "status" FROM "%s" WHERE "name" = $1`, tableName), "alice").Scan(&status); err != nil {
		t.Fatalf("查询默认值失败: %v", err)
	}

	if status != "active" {
		t.Fatalf("默认值不正确: expected active, got %s", status)
	}
}
