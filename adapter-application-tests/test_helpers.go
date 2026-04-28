package adapter_tests

import (
	"os"
	"strconv"

	db "github.com/eit-cms/eit-db"
)

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

func arangoIntegrationConfig() *db.Config {
	defaultURI := "http://" + getEnv("ARANGO_HOST", "localhost") + ":" + strconv.Itoa(getEnvInt("ARANGO_PORT", 58529))
	return &db.Config{
		Adapter: "arango",
		Arango: &db.ArangoConnectionConfig{
			URI:            getEnv("ARANGO_URI", defaultURI),
			Database:       getEnv("ARANGO_DB", "_system"),
			Username:       getEnv("ARANGO_USER", "root"),
			Password:       getEnv("ARANGO_PASSWORD", ""),
			Namespace:      getEnv("ARANGO_NAMESPACE", "collab_it"),
			TimeoutSeconds: getEnvInt("ARANGO_TIMEOUT_SECONDS", 10),
		},
	}
}
