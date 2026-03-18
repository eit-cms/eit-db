package db

import "testing"

func TestLoadConfigFromEnvWithDefaults_PostgresDSN(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://user:pass@db.local:55432/sample?sslmode=disable")
	cfg, err := LoadConfigFromEnvWithDefaults("postgres", &Config{Adapter: "postgres", Host: "localhost", Port: 5432, Username: "postgres", Database: "eit", SSLMode: "disable"})
	if err != nil {
		t.Fatalf("LoadConfigFromEnvWithDefaults() error = %v", err)
	}
	if cfg.Adapter != "postgres" {
		t.Fatalf("expected adapter postgres, got %s", cfg.Adapter)
	}
	resolved := cfg.ResolvedPostgresConfig()
	if resolved.DSN == "" {
		t.Fatal("expected POSTGRES_DSN to be loaded into postgres.dsn")
	}
	if resolved.Port != 5432 {
		t.Fatalf("expected defaults to remain when DSN is used, got port=%d", resolved.Port)
	}
}

func TestLoadConfigFromEnvWithDefaults_PostgresCustomPort(t *testing.T) {
	t.Setenv("POSTGRES_HOST", "db.internal")
	t.Setenv("POSTGRES_PORT", "55432")
	t.Setenv("POSTGRES_USER", "alice")
	t.Setenv("POSTGRES_PASSWORD", "secret")
	t.Setenv("POSTGRES_DB", "analytics")
	t.Setenv("POSTGRES_SSLMODE", "require")

	cfg, err := LoadConfigFromEnv("postgres")
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}
	resolved := cfg.ResolvedPostgresConfig()
	if resolved.Host != "db.internal" || resolved.Port != 55432 || resolved.Username != "alice" || resolved.Database != "analytics" || resolved.SSLMode != "require" {
		t.Fatalf("unexpected postgres config: %+v", cfg)
	}
}

func TestLoadConfigFromEnvWithDefaults_MySQLDSN(t *testing.T) {
	t.Setenv("MYSQL_DSN", "user:pass@tcp(db.local:3307)/sample?parseTime=true")
	cfg, err := LoadConfigFromEnv("mysql")
	if err != nil {
		t.Fatalf("LoadConfigFromEnv(mysql) error = %v", err)
	}
	if cfg.ResolvedMySQLConfig().DSN == "" {
		t.Fatal("expected MYSQL_DSN to be loaded into mysql.dsn")
	}
}

func TestLoadConfigFromEnvWithDefaults_SQLServerDetailed(t *testing.T) {
	t.Setenv("SQLSERVER_HOST", "sql.local")
	t.Setenv("SQLSERVER_PORT", "11433")
	t.Setenv("SQLSERVER_USER", "sa")
	t.Setenv("SQLSERVER_PASSWORD", "StrongPass!1")
	t.Setenv("SQLSERVER_DATABASE", "sample")

	cfg, err := LoadConfigFromEnv("sqlserver")
	if err != nil {
		t.Fatalf("LoadConfigFromEnv(sqlserver) error = %v", err)
	}
	resolved := cfg.ResolvedSQLServerConfig()
	if resolved.Host != "sql.local" || resolved.Port != 11433 || resolved.Database != "sample" {
		t.Fatalf("unexpected sqlserver config: %+v", cfg)
	}
}

func TestLoadConfigFromEnvWithDefaults_Mongo(t *testing.T) {
	t.Setenv("MONGODB_URI", "mongodb://localhost:27018")
	t.Setenv("MONGODB_DATABASE", "sample")

	cfg, err := LoadConfigFromEnv("mongodb")
	if err != nil {
		t.Fatalf("LoadConfigFromEnv(mongodb) error = %v", err)
	}
	resolved := cfg.ResolvedMongoConfig()
	if resolved.Database != "sample" {
		t.Fatalf("expected database sample, got %s", resolved.Database)
	}
	if resolved.URI != "mongodb://localhost:27018" {
		t.Fatalf("expected mongodb uri to be loaded, got %q", resolved.URI)
	}
}

func TestLoadConfigFromEnvWithDefaults_Neo4j(t *testing.T) {
	t.Setenv("NEO4J_URI", "neo4j://localhost:7687")
	t.Setenv("NEO4J_USER", "neo4j")
	t.Setenv("NEO4J_PASSWORD", "secret")
	t.Setenv("NEO4J_DATABASE", "graph")
	t.Setenv("EIT_QUERY_CACHE_MAX_ENTRIES", "64")
	t.Setenv("EIT_QUERY_CACHE_DEFAULT_TTL_SECONDS", "30")
	t.Setenv("EIT_QUERY_CACHE_ENABLE_METRICS", "false")

	cfg, err := LoadConfigFromEnv("neo4j")
	if err != nil {
		t.Fatalf("LoadConfigFromEnv(neo4j) error = %v", err)
	}
	resolved := cfg.ResolvedNeo4jConfig()
	if resolved.URI != "neo4j://localhost:7687" || resolved.Username != "neo4j" || resolved.Password != "secret" || resolved.Database != "graph" {
		t.Fatalf("unexpected neo4j config: %+v", resolved)
	}
	if cfg.QueryCache == nil || cfg.QueryCache.MaxEntries != 64 {
		t.Fatalf("expected query cache size from env, got %+v", cfg.QueryCache)
	}
	if cfg.QueryCache.DefaultTTLSeconds != 30 {
		t.Fatalf("expected query cache ttl from env, got %+v", cfg.QueryCache)
	}
	if cfg.QueryCache.EnableMetrics {
		t.Fatalf("expected metrics disabled from env, got %+v", cfg.QueryCache)
	}
}

func TestValidateAllowsDSNForRelationalAdapters(t *testing.T) {
	cases := []*Config{
		{Adapter: "postgres", Options: map[string]interface{}{"dsn": "postgres://u:p@h/db?sslmode=disable"}},
		{Adapter: "mysql", Options: map[string]interface{}{"dsn": "u:p@tcp(localhost:3306)/db"}},
		{Adapter: "sqlserver", Options: map[string]interface{}{"dsn": "sqlserver://sa:pass@localhost:1433?database=db&encrypt=disable"}},
	}

	for _, cfg := range cases {
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() should allow DSN for %s, got error: %v", cfg.Adapter, err)
		}
	}
}

func TestLoadConfigFromEnvWithDefaults_UnsupportedAdapter(t *testing.T) {
	if _, err := LoadConfigFromEnv("oracle"); err == nil {
		t.Fatal("expected unsupported adapter error")
	}
}
