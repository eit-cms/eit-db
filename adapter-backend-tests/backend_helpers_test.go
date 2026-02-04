package adapter_backend_tests

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"testing"

	"github.com/eit-cms/eit-db"
)

// 获取环境变量，带默认值
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// 获取环境变量为整数，带默认值
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

// 创建 PostgreSQL Repository
func setupPostgresRepo(t *testing.T) (*db.Repository, func()) {
	config := &db.Config{
		Adapter:  "postgres",
		Host:     getEnv("POSTGRES_HOST", "localhost"),
		Port:     getEnvInt("POSTGRES_PORT", 5432),
		Username: getEnv("POSTGRES_USER", "testuser"),
		Password: getEnv("POSTGRES_PASSWORD", "testpass"),
		Database: getEnv("POSTGRES_DB", "testdb"),
		SSLMode:  "disable",
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

	return repo, func() { _ = repo.Close() }
}

// 创建 MySQL Repository
func setupMySQLRepo(t *testing.T) (*db.Repository, func()) {
	config := &db.Config{
		Adapter:  "mysql",
		Host:     getEnv("MYSQL_HOST", "localhost"),
		Port:     getEnvInt("MYSQL_PORT", 3306),
		Username: getEnv("MYSQL_USER", "testuser"),
		Password: getEnv("MYSQL_PASSWORD", "testpass"),
		Database: getEnv("MYSQL_DATABASE", "testdb"),
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

	return repo, func() { _ = repo.Close() }
}

// 创建 SQL Server Repository
func setupSQLServerRepo(t *testing.T) (*db.Repository, func()) {
	config := &db.Config{
		Adapter:  "sqlserver",
		Host:     getEnv("SQLSERVER_HOST", "localhost"),
		Port:     getEnvInt("SQLSERVER_PORT", 1433),
		Username: getEnv("SQLSERVER_USER", "SA"),
		Password: getEnv("SQLSERVER_PASSWORD", "YourPassword123"),
		Database: getEnv("SQLSERVER_DB", "testdb"),
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

	return repo, func() { _ = repo.Close() }
}

// execSQL 执行 SQL，用于建表、删表等不需要返回结果的操作
func execSQL(ctx context.Context, t *testing.T, repo *db.Repository, sql string) {
	if _, err := repo.Exec(ctx, sql); err != nil {
		t.Fatalf("执行 SQL 失败: %v\nSQL: %s", err, sql)
	}
}

// querySQL 执行查询 SQL，返回行集
func querySQL(ctx context.Context, t *testing.T, repo *db.Repository, sql string) *sql.Rows {
	rows, err := repo.Query(ctx, sql)
	if err != nil {
		t.Fatalf("查询 SQL 失败: %v\nSQL: %s", err, sql)
	}
	return rows
}

// querySingleRow 查询单行
func querySingleRow(ctx context.Context, t *testing.T, repo *db.Repository, sql string) *sql.Row {
	return repo.QueryRow(ctx, sql)
}

// assertFeatureSupported 验证某个功能确实可以使用
func assertFeatureSupported(ctx context.Context, t *testing.T, repo *db.Repository, featureName string, sql string) {
	if _, err := repo.Exec(ctx, sql); err != nil {
		t.Errorf("功能 %s 不被支持或出错: %v\nSQL: %s", featureName, err, sql)
	}
}

// assertQuerySupported 验证查询功能可以执行
func assertQuerySupported(ctx context.Context, t *testing.T, repo *db.Repository, featureName string, sql string) {
	rows, err := repo.Query(ctx, sql)
	if err != nil {
		t.Errorf("查询功能 %s 不被支持或出错: %v\nSQL: %s", featureName, err, sql)
		return
	}
	defer rows.Close()
}
