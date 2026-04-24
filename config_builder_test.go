package db

import (
	"testing"
)

// ── NewConfig / MustConfig ─────────────────────────────────────────────────

func TestNewConfigPostgres(t *testing.T) {
	cfg, err := NewConfig("postgres",
		WithHost("db.example.com"),
		WithPort(5432),
		WithDatabase("myapp"),
		WithUsername("admin"),
		WithPassword("secret"),
		WithSSLMode("disable"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pg := cfg.Postgres
	if pg == nil {
		t.Fatal("expected Postgres sub-config to be populated")
	}
	if pg.Host != "db.example.com" {
		t.Errorf("Host: got %q, want %q", pg.Host, "db.example.com")
	}
	if pg.Port != 5432 {
		t.Errorf("Port: got %d, want %d", pg.Port, 5432)
	}
	if pg.Database != "myapp" {
		t.Errorf("Database: got %q, want %q", pg.Database, "myapp")
	}
	if pg.Username != "admin" {
		t.Errorf("Username: got %q, want %q", pg.Username, "admin")
	}
	if pg.SSLMode != "disable" {
		t.Errorf("SSLMode: got %q, want %q", pg.SSLMode, "disable")
	}
	// 选项写入的是子配置，不写入平铺字段
	if cfg.Host != "" {
		t.Errorf("expected flat Host to be empty, got %q", cfg.Host)
	}
}

func TestNewConfigPostgresDSN(t *testing.T) {
	cfg, err := NewConfig("postgres",
		WithDSN("postgres://admin:secret@localhost/myapp?sslmode=disable"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Postgres == nil || cfg.Postgres.DSN == "" {
		t.Fatal("expected Postgres DSN to be set")
	}
}

func TestNewConfigMySQL(t *testing.T) {
	cfg, err := NewConfig("mysql",
		WithHost("localhost"),
		WithPort(3306),
		WithDatabase("shop"),
		WithUsername("root"),
		WithPassword(""),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MySQL == nil {
		t.Fatal("expected MySQL sub-config to be populated")
	}
	if cfg.MySQL.Host != "localhost" {
		t.Errorf("Host: got %q, want %q", cfg.MySQL.Host, "localhost")
	}
}

func TestNewConfigMySQLDSN(t *testing.T) {
	cfg, err := NewConfig("mysql",
		WithDSN("root:@tcp(localhost:3306)/shop?charset=utf8mb4&parseTime=True"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MySQL == nil || cfg.MySQL.DSN == "" {
		t.Fatal("expected MySQL DSN to be set")
	}
}

func TestNewConfigSQLiteDefaults(t *testing.T) {
	cfg, err := NewConfig("sqlite")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolved := cfg.ResolvedSQLiteConfig()
	if resolved.Path == "" && resolved.DSN == "" {
		t.Error("expected default SQLite path to be set")
	}
}

func TestNewConfigSQLiteWithPath(t *testing.T) {
	cfg, err := NewConfig("sqlite",
		WithSQLitePath("/data/app.db"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SQLite == nil || cfg.SQLite.Path != "/data/app.db" {
		t.Errorf("expected SQLite path '/data/app.db', got %v", cfg.SQLite)
	}
}

func TestNewConfigSQLiteDSN(t *testing.T) {
	cfg, err := NewConfig("sqlite",
		WithDSN("file:/data/app.db?cache=shared"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SQLite == nil || cfg.SQLite.DSN == "" {
		t.Fatal("expected SQLite DSN to be set")
	}
}

func TestNewConfigSQLServer(t *testing.T) {
	cfg, err := NewConfig("sqlserver",
		WithHost("localhost"),
		WithDatabase("master"),
		WithUsername("sa"),
		WithPassword("Password123!"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SQLServer == nil {
		t.Fatal("expected SQLServer sub-config to be populated")
	}
	if cfg.SQLServer.Host != "localhost" {
		t.Errorf("Host: got %q, want %q", cfg.SQLServer.Host, "localhost")
	}
}

func TestNewConfigSQLServerManyToMany(t *testing.T) {
	cfg, err := NewConfig("sqlserver",
		WithHost("localhost"),
		WithDatabase("master"),
		WithUsername("sa"),
		WithPassword("Password123!"),
		WithSQLServerManyToMany("recursive_cte", 5, 50),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SQLServer.ManyToManyStrategy != "recursive_cte" {
		t.Errorf("strategy: got %q, want %q", cfg.SQLServer.ManyToManyStrategy, "recursive_cte")
	}
	if cfg.SQLServer.RecursiveCTEDepth != 5 {
		t.Errorf("depth: got %d, want %d", cfg.SQLServer.RecursiveCTEDepth, 5)
	}
	if cfg.SQLServer.RecursiveCTEMaxRecursion != 50 {
		t.Errorf("maxRecursion: got %d, want %d", cfg.SQLServer.RecursiveCTEMaxRecursion, 50)
	}
}

func TestNewConfigMongoDB(t *testing.T) {
	cfg, err := NewConfig("mongodb",
		WithURI("mongodb://localhost:27017"),
		WithDatabase("mydb"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MongoDB == nil {
		t.Fatal("expected MongoDB sub-config to be populated")
	}
	if cfg.MongoDB.URI != "mongodb://localhost:27017" {
		t.Errorf("URI: got %q, want %q", cfg.MongoDB.URI, "mongodb://localhost:27017")
	}
	if cfg.MongoDB.Database != "mydb" {
		t.Errorf("Database: got %q, want %q", cfg.MongoDB.Database, "mydb")
	}
}

func TestNewConfigMongoDBRelationStrategy(t *testing.T) {
	cfg, err := NewConfig("mongodb",
		WithURI("mongodb://localhost:27017"),
		WithDatabase("mydb"),
		WithMongoRelationJoinStrategy("pipeline"),
		WithMongoHideThroughArtifacts(false),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MongoDB.RelationJoinStrategy != "pipeline" {
		t.Errorf("strategy: got %q, want %q", cfg.MongoDB.RelationJoinStrategy, "pipeline")
	}
	if cfg.MongoDB.HideThroughArtifacts == nil || *cfg.MongoDB.HideThroughArtifacts != false {
		t.Error("expected HideThroughArtifacts to be false")
	}
}

func TestNewConfigNeo4jDefaults(t *testing.T) {
	cfg, err := NewConfig("neo4j")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolved := cfg.ResolvedNeo4jConfig()
	if resolved.URI != "neo4j://localhost:7687" {
		t.Errorf("default URI: got %q, want %q", resolved.URI, "neo4j://localhost:7687")
	}
	if resolved.Username != "neo4j" {
		t.Errorf("default username: got %q, want %q", resolved.Username, "neo4j")
	}
	if resolved.Database != "neo4j" {
		t.Errorf("default database: got %q, want %q", resolved.Database, "neo4j")
	}
}

func TestNewConfigNeo4jWithURI(t *testing.T) {
	cfg, err := NewConfig("neo4j",
		WithURI("neo4j://prod-server:7687"),
		WithUsername("admin"),
		WithPassword("secret"),
		WithDatabase("social"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Neo4j == nil {
		t.Fatal("expected Neo4j sub-config to be populated")
	}
	if cfg.Neo4j.URI != "neo4j://prod-server:7687" {
		t.Errorf("URI: got %q, want %q", cfg.Neo4j.URI, "neo4j://prod-server:7687")
	}
	if cfg.Neo4j.Database != "social" {
		t.Errorf("Database: got %q, want %q", cfg.Neo4j.Database, "social")
	}
	// 选项写入子配置，不写入平铺字段
	if cfg.Username != "" {
		t.Errorf("expected flat Username to be empty, got %q", cfg.Username)
	}
}

func TestNewConfigNeo4jSocialNetwork(t *testing.T) {
	social := &Neo4jSocialNetworkConfig{
		UserLabel:     "Account",
		ChatRoomLabel: "Room",
		PostLabel:     "Article",
	}
	cfg, err := NewConfig("neo4j",
		WithNeo4jSocialNetwork(social),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Neo4j == nil || cfg.Neo4j.SocialNetwork == nil {
		t.Fatal("expected Neo4j SocialNetwork to be set")
	}
	resolved := cfg.ResolvedNeo4jConfig()
	if resolved.SocialNetwork.UserLabel != "Account" {
		t.Errorf("UserLabel: got %q, want %q", resolved.SocialNetwork.UserLabel, "Account")
	}
	// 未指定的字段应回退到默认值
	if resolved.SocialNetwork.FollowsRelType != "FOLLOWS" {
		t.Errorf("FollowsRelType: got %q, want default %q", resolved.SocialNetwork.FollowsRelType, "FOLLOWS")
	}
}

// ── 通用框架配置选项 ───────────────────────────────────────────────────────

func TestNewConfigWithPool(t *testing.T) {
	pool := &PoolConfig{MaxConnections: 10, MinConnections: 2}
	cfg, err := NewConfig("postgres",
		WithHost("localhost"),
		WithDatabase("myapp"),
		WithUsername("admin"),
		WithPool(pool),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Pool == nil || cfg.Pool.MaxConnections != 10 {
		t.Error("expected pool config to be set correctly")
	}
}

func TestNewConfigWithQueryCache(t *testing.T) {
	cache := &QueryCacheConfig{MaxEntries: 500, DefaultTTLSeconds: 120}
	cfg, err := NewConfig("sqlite",
		WithQueryCache(cache),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.QueryCache == nil || cfg.QueryCache.MaxEntries != 500 {
		t.Error("expected query cache config to be set correctly")
	}
}

func TestNewConfigScheduledTaskFallbackDisabled(t *testing.T) {
	cfg, err := NewConfig("sqlite",
		WithScheduledTaskFallback(false),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ScheduledTaskFallbackEnabled() {
		t.Error("expected scheduled task fallback to be disabled")
	}
}

func TestNewConfigScheduledTaskFallbackEnabled(t *testing.T) {
	cfg, err := NewConfig("sqlite",
		WithScheduledTaskFallback(true),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.ScheduledTaskFallbackEnabled() {
		t.Error("expected scheduled task fallback to be enabled")
	}
}

func TestNewConfigWithValidation(t *testing.T) {
	validation := &ValidationConfig{
		DefaultLocale:  "zh-CN",
		EnabledLocales: []string{"zh-CN", "en-US"},
	}
	cfg, err := NewConfig("sqlite",
		WithValidation(validation),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Validation == nil || cfg.Validation.DefaultLocale != "zh-CN" {
		t.Error("expected validation config to be set")
	}
}

// ── MustConfig 异常路径 ────────────────────────────────────────────────────

func TestMustConfigPanicsOnInvalidAdapter(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid adapter")
		}
	}()
	MustConfig("not_a_real_adapter")
}

func TestMustConfigSucceeds(t *testing.T) {
	cfg := MustConfig("sqlite",
		WithSQLitePath(":memory:"),
	)
	if cfg == nil {
		t.Error("expected non-nil config")
	}
}

// ── 选项顺序：后者覆盖前者 ────────────────────────────────────────────────

func TestNewConfigOptionsLastWins(t *testing.T) {
	cfg, err := NewConfig("postgres",
		WithHost("first-host"),
		WithDatabase("myapp"),
		WithUsername("admin"),
		WithHost("second-host"), // 后者覆盖前者
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Postgres.Host != "second-host" {
		t.Errorf("expected last WithHost to win, got %q", cfg.Postgres.Host)
	}
}

// ── 选项不污染其他适配器子配置 ────────────────────────────────────────────

func TestNewConfigOptionsDoNotPolluteFlatFields(t *testing.T) {
	cfg, err := NewConfig("mysql",
		WithHost("db-host"),
		WithPort(3306),
		WithDatabase("shop"),
		WithUsername("root"),
		WithPassword(""),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 平铺字段应保持空，避免歧义
	if cfg.Host != "" {
		t.Errorf("expected cfg.Host to be empty for mysql, got %q", cfg.Host)
	}
	if cfg.Port != 0 {
		t.Errorf("expected cfg.Port to be 0 for mysql, got %d", cfg.Port)
	}
	// 子配置应正确填充
	if cfg.MySQL == nil || cfg.MySQL.Host != "db-host" {
		t.Errorf("expected MySQL.Host 'db-host'")
	}
}
