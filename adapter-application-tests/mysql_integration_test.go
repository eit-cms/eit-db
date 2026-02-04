package adapter_tests

import (
	"context"
	"testing"

	db "github.com/eit-cms/eit-db"
)

func setupMySQLRepo(t *testing.T) (*db.Repository, func()) {
	config := &db.Config{
		Adapter:  "mysql",
		Host:     getEnv("MYSQL_HOST", "localhost"),
		Port:     getEnvInt("MYSQL_PORT", 3306),
		Username: getEnv("MYSQL_USER", "testuser"),
		Password: getEnv("MYSQL_PASSWORD", "testpass"),
		Database: getEnv("MYSQL_DB", "testdb"),
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		t.Skipf("MySQL 不可用: %v", err)
		return nil, nil
	}

	if err := repo.Ping(context.Background()); err != nil {
		t.Skipf("MySQL 连接失败: %v", err)
		return nil, nil
	}

	cleanup := func() {
		_ = repo.Close()
	}

	return repo, cleanup
}

func buildMySQLUserSchema() db.Schema {
	schema := db.NewBaseSchema("mysql_users")
	schema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true, Autoinc: true})
	schema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
	schema.AddField(&db.Field{Name: "email", Type: db.TypeString, Null: false, Unique: true})
	schema.AddField(&db.Field{Name: "age", Type: db.TypeInteger, Null: false})
	return schema
}

func TestMySQLChangesetValidation(t *testing.T) {
	schema := buildMySQLUserSchema()

	valid := db.NewChangeset(schema).
		Cast(map[string]interface{}{"name": "Alice", "email": "alice@example.com", "age": 25}).
		ValidateRequired([]string{"name", "email", "age"}).
		ValidateFormat("email", `.+@.+\..+`).
		ValidateNumber("age", map[string]interface{}{"min": 1})

	if !valid.IsValid() {
		t.Fatalf("期望 changeset 有效，得到错误: %v", valid.Errors())
	}
}

func TestMySQLSchemaMigration(t *testing.T) {
	repo, cleanup := setupMySQLRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	schema := buildMySQLUserSchema()
	migration := db.NewSchemaMigration("20260204091000", "create_mysql_users").CreateTable(schema)

	ctx := context.Background()
	if err := migration.Up(ctx, repo); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}
	defer func() {
		_ = migration.Down(ctx, repo)
	}()

	qb := db.NewQueryBuilder(schema, repo)
	if _, err := qb.SelectCount(""); err != nil {
		t.Fatalf("查询表失败: %v", err)
	}
}

func TestMySQLQueryBuilderCRUD(t *testing.T) {
	repo, cleanup := setupMySQLRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	schema := buildMySQLUserSchema()
	migration := db.NewSchemaMigration("20260204092000", "create_mysql_users_crud").CreateTable(schema)

	ctx := context.Background()
	if err := migration.Up(ctx, repo); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}
	defer func() {
		_ = migration.Down(ctx, repo)
	}()

	qb := db.NewQueryBuilder(schema, repo)

	cs := db.NewChangeset(schema).
		Cast(map[string]interface{}{"name": "Alice", "email": "alice@example.com", "age": 25}).
		ValidateRequired([]string{"name", "email", "age"}).
		ValidateFormat("email", `.+@.+\..+`).
		ValidateNumber("age", map[string]interface{}{"min": 1})

	if !cs.IsValid() {
		t.Fatalf("changeset 无效: %v", cs.Errors())
	}

	if _, err := qb.Insert(cs); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	count, err := qb.SelectCount("")
	if err != nil {
		t.Fatalf("计数失败: %v", err)
	}
	if count != 1 {
		t.Fatalf("期望 1 条记录，得到 %d", count)
	}

	updateCS := db.NewChangeset(schema).
		Cast(map[string]interface{}{"age": 26}).
		ValidateNumber("age", map[string]interface{}{"min": 1})

	if _, err := qb.Update(updateCS, "email = ?", "alice@example.com"); err != nil {
		t.Fatalf("更新失败: %v", err)
	}

	if _, err := qb.Delete("email = ?", "alice@example.com"); err != nil {
		t.Fatalf("删除失败: %v", err)
	}

	count, err = qb.SelectCount("")
	if err != nil {
		t.Fatalf("计数失败: %v", err)
	}
	if count != 0 {
		t.Fatalf("期望 0 条记录，得到 %d", count)
	}
}

