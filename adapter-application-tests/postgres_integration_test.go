package adapter_tests

import (
	"context"
	"testing"

	db "github.com/eit-cms/eit-db"
)

func setupPostgresRepo(t *testing.T) (*db.Repository, func()) {
	config := &db.Config{
		Adapter:  "postgres",
		Host:     getEnv("POSTGRES_HOST", "localhost"),
		Port:     getEnvInt("POSTGRES_PORT", 5432),
		Username: getEnv("POSTGRES_USER", "testuser"),
		Password: getEnv("POSTGRES_PASSWORD", "testpass"),
		Database: getEnv("POSTGRES_DB", "testdb"),
		SSLMode:  getEnv("POSTGRES_SSLMODE", "disable"),
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		t.Skipf("PostgreSQL 不可用: %v", err)
		return nil, nil
	}

	if err := repo.Ping(context.Background()); err != nil {
		t.Skipf("PostgreSQL 连接失败: %v", err)
		return nil, nil
	}

	cleanup := func() {
		_ = repo.Close()
	}

	return repo, cleanup
}

func buildPostgresUserSchema() db.Schema {
	schema := db.NewBaseSchema("pg_users")
	schema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true, Autoinc: true})
	schema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
	schema.AddField(&db.Field{Name: "email", Type: db.TypeString, Null: false, Unique: true})
	schema.AddField(&db.Field{Name: "age", Type: db.TypeInteger, Null: false})
	return schema
}

func TestPostgresChangesetValidation(t *testing.T) {
	schema := buildPostgresUserSchema()

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
	if repo == nil {
		return
	}
	defer cleanup()

	schema := buildPostgresUserSchema()
	migration := db.NewSchemaMigration("20260204090000", "create_pg_users").CreateTable(schema)

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

	if _, err := qb.SelectCount(""); err == nil {
		t.Fatalf("表已删除，预期查询失败")
	}
}

