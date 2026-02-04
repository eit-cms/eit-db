package adapter_tests

import (
	"context"
	"fmt"
	"testing"

	db "github.com/eit-cms/eit-db"
)

func setupSQLServerRepo(t *testing.T) (*db.Repository, func()) {
	config := &db.Config{
		Adapter:  "sqlserver",
		Host:     getEnv("SQLSERVER_HOST", "localhost"),
		Port:     getEnvInt("SQLSERVER_PORT", 1433),
		Username: getEnv("SQLSERVER_USER", "sa"),
		Password: getEnv("SQLSERVER_PASSWORD", "Test@1234"),
		Database: getEnv("SQLSERVER_DB", "testdb"),
	}

	if err := ensureSQLServerDatabase(config); err != nil {
		t.Skipf("SQL Server 初始化失败: %v", err)
		return nil, nil
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		t.Skipf("SQL Server 不可用: %v", err)
		return nil, nil
	}

	if err := repo.Ping(context.Background()); err != nil {
		t.Skipf("SQL Server 连接失败: %v", err)
		return nil, nil
	}

	cleanup := func() {
		_ = repo.Close()
	}

	return repo, cleanup
}

func ensureSQLServerDatabase(config *db.Config) error {
	masterConfig := *config
	masterConfig.Database = "master"

	repo, err := db.NewRepository(&masterConfig)
	if err != nil {
		return err
	}
	defer repo.Close()

	ctx := context.Background()
	if err := repo.Ping(ctx); err != nil {
		return err
	}

	createSQL := fmt.Sprintf("IF DB_ID('%s') IS NULL CREATE DATABASE [%s]", config.Database, config.Database)
	_, err = repo.Exec(ctx, createSQL)
	return err
}

func buildSQLServerUserSchema() db.Schema {
	schema := db.NewBaseSchema("sqlserver_users")
	schema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true, Autoinc: true})
	schema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
	schema.AddField(&db.Field{Name: "email", Type: db.TypeString, Null: false, Unique: true})
	schema.AddField(&db.Field{Name: "age", Type: db.TypeInteger, Null: false})
	return schema
}

func TestSQLServerChangesetValidation(t *testing.T) {
	schema := buildSQLServerUserSchema()

	valid := db.NewChangeset(schema).
		Cast(map[string]interface{}{"name": "Alice", "email": "alice@example.com", "age": 25}).
		ValidateRequired([]string{"name", "email", "age"}).
		ValidateFormat("email", `.+@.+\..+`).
		ValidateNumber("age", map[string]interface{}{"min": 1})

	if !valid.IsValid() {
		t.Fatalf("期望 changeset 有效，得到错误: %v", valid.Errors())
	}
}

func TestSQLServerSchemaMigration(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	schema := buildSQLServerUserSchema()
	migration := db.NewSchemaMigration("20260204093000", "create_sqlserver_users").CreateTable(schema)

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

