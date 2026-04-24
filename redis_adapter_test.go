package db

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ==================== Config 验证测试 ====================

func TestRedisConfigValidationDefaultValues(t *testing.T) {
	cfg, err := NewConfig("redis")
	if err != nil {
		t.Fatalf("expected no error with defaults, got: %v", err)
	}
	resolved := cfg.ResolvedRedisConfig()
	if resolved.Host != "localhost" {
		t.Errorf("expected Host=localhost, got %q", resolved.Host)
	}
	if resolved.Port != 6379 {
		t.Errorf("expected Port=6379, got %d", resolved.Port)
	}
	if resolved.DB != 0 {
		t.Errorf("expected DB=0, got %d", resolved.DB)
	}
}

func TestRedisConfigValidationWithHost(t *testing.T) {
	cfg, err := NewConfig("redis",
		WithHost("redis.example.com"),
		WithPort(6380),
		WithPassword("secret"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolved := cfg.ResolvedRedisConfig()
	if resolved.Host != "redis.example.com" {
		t.Errorf("expected Host=redis.example.com, got %q", resolved.Host)
	}
	if resolved.Port != 6380 {
		t.Errorf("expected Port=6380, got %d", resolved.Port)
	}
	if resolved.Password != "secret" {
		t.Errorf("expected Password=secret, got %q", resolved.Password)
	}
}

func TestRedisConfigValidationWithURI(t *testing.T) {
	cfg, err := NewConfig("redis",
		WithURI("redis://:mypassword@redis.internal:6379/1"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolved := cfg.ResolvedRedisConfig()
	if resolved.URI != "redis://:mypassword@redis.internal:6379/1" {
		t.Errorf("unexpected URI: %q", resolved.URI)
	}
}

func TestRedisConfigValidationClusterMode(t *testing.T) {
	cfg, err := NewConfig("redis",
		WithRedisCluster("node1:7000", "node2:7001", "node3:7002"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolved := cfg.ResolvedRedisConfig()
	if !resolved.ClusterMode {
		t.Error("expected ClusterMode=true")
	}
	if len(resolved.ClusterAddrs) != 3 {
		t.Errorf("expected 3 cluster addrs, got %d", len(resolved.ClusterAddrs))
	}
}

func TestRedisConfigValidationClusterModeRequiresAddrs(t *testing.T) {
	_, err := NewConfig("redis",
		func(cfg *Config) {
			if cfg.Redis == nil {
				cfg.Redis = &RedisConnectionConfig{}
			}
			cfg.Redis.ClusterMode = true
			// 不设置 ClusterAddrs
		},
	)
	if err == nil {
		t.Fatal("expected error for cluster mode without addrs")
	}
}

func TestRedisConfigValidationDBNumber(t *testing.T) {
	cfg, err := NewConfig("redis",
		WithRedisDB(5),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolved := cfg.ResolvedRedisConfig()
	if resolved.DB != 5 {
		t.Errorf("expected DB=5, got %d", resolved.DB)
	}
}

func TestRedisConfigValidationTLS(t *testing.T) {
	cfg, err := NewConfig("redis",
		WithHost("redis.tls.example.com"),
		WithRedisTLS(true),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolved := cfg.ResolvedRedisConfig()
	if !resolved.TLSEnabled {
		t.Error("expected TLSEnabled=true")
	}
}

func TestRedisConfigValidationTimeouts(t *testing.T) {
	cfg, err := NewConfig("redis",
		WithRedisTimeouts(5, 3, 3),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolved := cfg.ResolvedRedisConfig()
	if resolved.DialTimeout != 5 {
		t.Errorf("expected DialTimeout=5, got %d", resolved.DialTimeout)
	}
	if resolved.ReadTimeout != 3 {
		t.Errorf("expected ReadTimeout=3, got %d", resolved.ReadTimeout)
	}
	if resolved.WriteTimeout != 3 {
		t.Errorf("expected WriteTimeout=3, got %d", resolved.WriteTimeout)
	}
}

func TestRedisConfigValidationUsername(t *testing.T) {
	cfg, err := NewConfig("redis",
		WithUsername("alice"),
		WithPassword("pw123"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolved := cfg.ResolvedRedisConfig()
	if resolved.Username != "alice" {
		t.Errorf("expected Username=alice, got %q", resolved.Username)
	}
}

// ==================== DefaultConfig 测试 ====================

func TestRedisDefaultConfig(t *testing.T) {
	cfg := DefaultConfig("redis")
	if cfg.Adapter != "redis" {
		t.Errorf("expected Adapter=redis, got %q", cfg.Adapter)
	}
	if cfg.Redis == nil {
		t.Fatal("expected Redis config to be non-nil")
	}
	if cfg.Redis.Host != "localhost" {
		t.Errorf("expected Host=localhost, got %q", cfg.Redis.Host)
	}
	if cfg.Redis.Port != 6379 {
		t.Errorf("expected Port=6379, got %d", cfg.Redis.Port)
	}
}

// ==================== cloneConfig 测试 ====================

func TestRedisCloneConfig(t *testing.T) {
	src := &Config{
		Adapter: "redis",
		Redis: &RedisConnectionConfig{
			Host:         "redis-server",
			Port:         6380,
			Password:     "pass",
			DB:           2,
			TLSEnabled:   true,
			ClusterMode:  true,
			ClusterAddrs: []string{"node1:7000", "node2:7001"},
		},
	}
	clone := cloneConfig(src)
	if clone.Redis == nil {
		t.Fatal("cloned Redis config should not be nil")
	}
	if clone.Redis.Host != "redis-server" {
		t.Errorf("unexpected Host: %q", clone.Redis.Host)
	}
	// 修改克隆不影响原始
	clone.Redis.Host = "other"
	clone.Redis.ClusterAddrs[0] = "changed"
	if src.Redis.Host != "redis-server" {
		t.Error("cloneConfig mutated source Host")
	}
	if src.Redis.ClusterAddrs[0] != "node1:7000" {
		t.Error("cloneConfig mutated source ClusterAddrs")
	}
}

// ==================== 适配器构造测试 ====================

func TestNewRedisAdapterSuccess(t *testing.T) {
	cfg, err := NewConfig("redis",
		WithHost("localhost"),
		WithPort(6379),
	)
	if err != nil {
		t.Fatalf("config error: %v", err)
	}
	a, err := NewRedisAdapter(cfg)
	if err != nil {
		t.Fatalf("adapter construction failed: %v", err)
	}
	if a == nil {
		t.Fatal("expected non-nil adapter")
	}
}

func TestNewRedisAdapterNilConfig(t *testing.T) {
	_, err := NewRedisAdapter(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
}

// ==================== 不支持的 SQL 接口测试 ====================

func TestRedisAdapterSQLNotSupported(t *testing.T) {
	cfg := MustConfig("redis")
	a, _ := NewRedisAdapter(cfg)
	ctx := context.Background()

	if _, err := a.Query(ctx, "SELECT 1"); err == nil {
		t.Error("expected error from Query")
	}
	if row := a.QueryRow(ctx, "SELECT 1"); row != nil {
		t.Error("expected nil from QueryRow")
	}
	if _, err := a.Exec(ctx, "DELETE FROM t"); err == nil {
		t.Error("expected error from Exec")
	}
	if _, err := a.Begin(ctx); err == nil {
		t.Error("expected error from Begin")
	}
}

// ==================== 定时任务降级测试 ====================

func TestRedisAdapterScheduledTaskFallback(t *testing.T) {
	cfg := MustConfig("redis")
	a, _ := NewRedisAdapter(cfg)
	ctx := context.Background()

	task := &ScheduledTaskConfig{Name: "test_task", CronExpression: "*/5 * * * *"}
	if err := a.RegisterScheduledTask(ctx, task); !IsScheduledTaskFallbackError(err) {
		t.Errorf("expected fallback error, got: %v", err)
	}
	if err := a.UnregisterScheduledTask(ctx, "test_task"); !IsScheduledTaskFallbackError(err) {
		t.Errorf("expected fallback error, got: %v", err)
	}
	if _, err := a.ListScheduledTasks(ctx); !IsScheduledTaskFallbackError(err) {
		t.Errorf("expected fallback error, got: %v", err)
	}
}

// ==================== 特性声明测试 ====================

func TestRedisAdapterFeatures(t *testing.T) {
	cfg := MustConfig("redis")
	a, _ := NewRedisAdapter(cfg)

	dbFeatures := a.GetDatabaseFeatures()
	if dbFeatures == nil {
		t.Fatal("expected non-nil DatabaseFeatures")
	}
	if dbFeatures.DatabaseName != "Redis" {
		t.Errorf("unexpected DatabaseName: %q", dbFeatures.DatabaseName)
	}
	if !dbFeatures.SupportsUpsert {
		t.Error("expected SupportsUpsert=true")
	}
	if !dbFeatures.SupportsListenNotify {
		t.Error("expected SupportsListenNotify=true (pub/sub)")
	}
	if dbFeatures.SupportsForeignKeys {
		t.Error("expected SupportsForeignKeys=false")
	}

	qFeatures := a.GetQueryFeatures()
	if qFeatures == nil {
		t.Fatal("expected non-nil QueryFeatures")
	}
	if qFeatures.SupportsIN {
		t.Error("expected SupportsIN=false")
	}
	if qFeatures.SupportsInnerJoin {
		t.Error("expected SupportsInnerJoin=false")
	}

	provider := a.GetQueryBuilderProvider()
	if provider == nil {
		t.Fatal("expected non-nil QueryBuilderProvider for Redis")
	}
	if !provider.GetCapabilities().SupportsNativeQuery {
		t.Fatal("expected SupportsNativeQuery=true for Redis provider")
	}
	if provider.GetCapabilities().NativeQueryLang != "redis" {
		t.Fatalf("expected NativeQueryLang=redis, got %q", provider.GetCapabilities().NativeQueryLang)
	}
}

func TestRedisQueryConstructorBuildCommandPrefix(t *testing.T) {
	provider := NewRedisQueryConstructorProvider()
	constructor := provider.NewQueryConstructor(nil)
	native, ok := constructor.GetNativeBuilder().(*RedisQueryConstructor)
	if !ok {
		t.Fatal("expected RedisQueryConstructor native builder")
	}
	query, args, err := native.Command("GET", "user:1").Build(context.Background())
	if err != nil {
		t.Fatalf("unexpected build error: %v", err)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %d", len(args))
	}
	if !strings.HasPrefix(query, redisCompiledCommandPrefix) {
		t.Fatalf("expected query prefix %q, got %q", redisCompiledCommandPrefix, query)
	}
	plan, err := parseRedisCompiledCommandPlan(query)
	if err != nil {
		t.Fatalf("expected parse success: %v", err)
	}
	if plan.Command != "GET" || !plan.ReadOnly {
		t.Fatalf("unexpected command plan: %+v", plan)
	}
}

func TestRedisQueryConstructorBuildPipelinePrefix(t *testing.T) {
	constructor := NewRedisQueryConstructor(nil)
	query, _, err := constructor.Pipeline(
		RedisCompiledCommandPlan{Command: "GET", Args: []interface{}{"k1"}},
		RedisCompiledCommandPlan{Command: "TTL", Args: []interface{}{"k1"}},
	).Build(context.Background())
	if err != nil {
		t.Fatalf("unexpected build error: %v", err)
	}
	if !strings.HasPrefix(query, redisCompiledPipelinePrefix) {
		t.Fatalf("expected pipeline prefix %q, got %q", redisCompiledPipelinePrefix, query)
	}
	plan, err := parseRedisCompiledPipelinePlan(query)
	if err != nil {
		t.Fatalf("expected parse success: %v", err)
	}
	if len(plan.Commands) != 2 || !plan.ReadOnly {
		t.Fatalf("unexpected pipeline plan: %+v", plan)
	}
}

func TestRedisFeatureSupportMatrix(t *testing.T) {
	a := &RedisAdapter{}
	matrix := a.BuildFeatureSupportMatrix(&RedisCapabilitySnapshot{
		Mode:     "standalone",
		Modules:  []RedisCapabilityModule{{Name: "RedisJSON"}, {Name: "search"}},
		Commands: []string{"get", "set", "del", "mget", "mset", "expire", "ttl", "json.get", "ft.search"},
	})
	if matrix.AdapterRequired["feature_declaration"].State != redisFeatureStateSupported {
		t.Fatalf("expected feature_declaration supported, got %+v", matrix.AdapterRequired["feature_declaration"])
	}
	if matrix.Core["kv_get_set"].State != redisFeatureStateSupported {
		t.Fatalf("expected kv_get_set supported, got %+v", matrix.Core["kv_get_set"])
	}
	if matrix.Stack["json_cache"].State != redisFeatureStateSupported {
		t.Fatalf("expected json_cache supported, got %+v", matrix.Stack["json_cache"])
	}
	if matrix.Stack["graph_join"].State != redisFeatureStateUnavailable {
		t.Fatalf("expected graph_join unavailable, got %+v", matrix.Stack["graph_join"])
	}
}

func TestRedisNamespacedKey(t *testing.T) {
	a := &RedisAdapter{}
	if got := a.NamespacedKey("cache", "user:1"); got != "cache:user:1" {
		t.Fatalf("unexpected namespaced key: %q", got)
	}
}

func TestRedisBeginRequiresConnection(t *testing.T) {
	a := &RedisAdapter{}
	if _, err := a.Begin(context.Background()); err == nil {
		t.Fatal("expected Begin to fail when client is not connected")
	}
}

func TestRedisTxExecRequiresCompiledPrefix(t *testing.T) {
	tx := &redisTx{adapter: &RedisAdapter{}, queued: make([]RedisCompiledCommandPlan, 0)}
	if _, err := tx.Exec(context.Background(), "SET key value"); err == nil {
		t.Fatal("expected transaction Exec to reject non-compiled statements")
	}
}

func TestRedisTxExecQueuesCompiledCommand(t *testing.T) {
	tx := &redisTx{adapter: &RedisAdapter{}, queued: make([]RedisCompiledCommandPlan, 0)}
	constructor := NewRedisQueryConstructor(nil).Command("SET", "key", "value")
	query, _, err := constructor.Build(context.Background())
	if err != nil {
		t.Fatalf("unexpected build error: %v", err)
	}
	res, err := tx.Exec(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected Exec error: %v", err)
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		t.Fatalf("expected queued rows=1, got %d", rows)
	}
	if len(tx.queued) != 1 || tx.queued[0].Command != "SET" {
		t.Fatalf("unexpected queued commands: %+v", tx.queued)
	}
}

// ==================== GetRawConn 测试（未连接时为 nil） ====================

func TestRedisAdapterGetRawConnBeforeConnect(t *testing.T) {
	cfg := MustConfig("redis")
	a, _ := NewRedisAdapter(cfg)
	if a.GetRawConn() != nil {
		t.Error("expected nil GetRawConn before Connect")
	}
	if a.Client() != nil {
		t.Error("expected nil Client() before Connect")
	}
}

// ==================== MustConfig + Redis 集成测试 ====================

func TestMustConfigRedisWithAllOptions(t *testing.T) {
	cfg := MustConfig("redis",
		WithHost("redis.prod.internal"),
		WithPort(6380),
		WithUsername("svcaccount"),
		WithPassword("strongpass"),
		WithRedisDB(3),
		WithRedisTLS(true),
		WithRedisTimeouts(10, 5, 5),
		WithPool(&PoolConfig{MaxConnections: 50}),
	)
	r := cfg.ResolvedRedisConfig()
	if r.Host != "redis.prod.internal" {
		t.Errorf("unexpected Host: %q", r.Host)
	}
	if r.Port != 6380 {
		t.Errorf("unexpected Port: %d", r.Port)
	}
	if r.Username != "svcaccount" {
		t.Errorf("unexpected Username: %q", r.Username)
	}
	if r.DB != 3 {
		t.Errorf("unexpected DB: %d", r.DB)
	}
	if !r.TLSEnabled {
		t.Error("expected TLSEnabled=true")
	}
	if r.DialTimeout != 10 {
		t.Errorf("unexpected DialTimeout: %d", r.DialTimeout)
	}
}

// ==================== Ping 未连接测试 ====================

func TestRedisAdapterPingBeforeConnect(t *testing.T) {
	cfg := MustConfig("redis")
	a, _ := NewRedisAdapter(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := a.Ping(ctx)
	if err == nil {
		t.Error("expected error from Ping when not connected")
	}
}

// ==================== Close 未连接测试 ====================

func TestRedisAdapterCloseBeforeConnect(t *testing.T) {
	cfg := MustConfig("redis")
	a, _ := NewRedisAdapter(cfg)
	if err := a.Close(); err != nil {
		t.Errorf("Close before Connect should be a no-op, got: %v", err)
	}
}
