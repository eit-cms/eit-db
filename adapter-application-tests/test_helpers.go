package adapter_tests

import (
	"os"
	"strconv"
	"testing"

	db "github.com/eit-cms/eit-db"
)

const officialIntegrationEnvHint = "请先在仓库根目录执行 `docker compose up -d`（项目官方测试镜像），并等待容器健康检查通过。"

func failIntegrationEnv(t *testing.T, backend string, err error) {
	t.Helper()
	t.Fatalf("%s 集成测试环境不可用: %v\n%s", backend, err, officialIntegrationEnvHint)
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func getEnvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func postgresIntegrationConfig() *db.Config {
	return &db.Config{
		Adapter: "postgres",
		Postgres: &db.PostgresConnectionConfig{
			Host:     getEnv("POSTGRES_HOST", "localhost"),
			Port:     getEnvInt("POSTGRES_PORT", 55432),
			Username: getEnv("POSTGRES_USER", "testuser"),
			Password: getEnv("POSTGRES_PASSWORD", "testpass"),
			Database: getEnv("POSTGRES_DB", "testdb"),
			SSLMode:  getEnv("POSTGRES_SSLMODE", "disable"),
			DSN:      getEnv("POSTGRES_DSN", ""),
		},
	}
}

func mysqlIntegrationConfig() *db.Config {
	return &db.Config{
		Adapter: "mysql",
		MySQL: &db.MySQLConnectionConfig{
			Host:     getEnv("MYSQL_HOST", "localhost"),
			Port:     getEnvInt("MYSQL_PORT", 3306),
			Username: getEnv("MYSQL_USER", "testuser"),
			Password: getEnv("MYSQL_PASSWORD", "testpass"),
			Database: getEnv("MYSQL_DB", "testdb"),
			DSN:      getEnv("MYSQL_DSN", ""),
		},
	}
}

func sqlServerIntegrationConfig() *db.Config {
	return &db.Config{
		Adapter: "sqlserver",
		SQLServer: &db.SQLServerConnectionConfig{
			Host:     getEnv("SQLSERVER_HOST", "localhost"),
			Port:     getEnvInt("SQLSERVER_PORT", 1433),
			Username: getEnv("SQLSERVER_USER", "sa"),
			Password: getEnv("SQLSERVER_PASSWORD", "Test@1234"),
			Database: getEnv("SQLSERVER_DATABASE", "testdb"),
			DSN:      getEnv("SQLSERVER_DSN", ""),
		},
	}
}

func redisIntegrationConfig() *db.Config {
	return &db.Config{
		Adapter: "redis",
		Redis: &db.RedisConnectionConfig{
			URI:      getEnv("REDIS_URI", ""),
			Host:     getEnv("REDIS_HOST", "localhost"),
			Port:     getEnvInt("REDIS_PORT", 6379),
			Username: getEnv("REDIS_USERNAME", ""),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getEnvInt("REDIS_DB", 0),
		},
	}
}

func mongoIntegrationConfig() *db.Config {
	return &db.Config{
		Adapter: "mongodb",
		MongoDB: &db.MongoConnectionConfig{
			URI:      getEnv("MONGODB_URI", "mongodb://localhost:27017"),
			Database: getEnv("MONGODB_DATABASE", "testdb"),
		},
	}
}

func neo4jIntegrationConfig() *db.Config {
	return &db.Config{
		Adapter: "neo4j",
		Neo4j: &db.Neo4jConnectionConfig{
			URI:      getEnv("NEO4J_URI", "neo4j://localhost:7687"),
			Username: getEnv("NEO4J_USER", "neo4j"),
			Password: getEnv("NEO4J_PASSWORD", "neo4jtest"),
			Database: getEnv("NEO4J_DATABASE", "neo4j"),
		},
	}
}
