package db

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestCompiledQueryCacheCapacityEviction(t *testing.T) {
	repo := &Repository{compiledQueryCache: NewCompiledQueryCacheWithSize(2)}

	if err := repo.StoreCompiledQuery("k1", "SELECT 1"); err != nil {
		t.Fatalf("store k1 failed: %v", err)
	}
	if err := repo.StoreCompiledQuery("k2", "SELECT 2"); err != nil {
		t.Fatalf("store k2 failed: %v", err)
	}
	if _, ok := repo.GetCompiledQuery("k1"); !ok {
		t.Fatalf("expected k1 to exist before eviction")
	}
	if err := repo.StoreCompiledQuery("k3", "SELECT 3"); err != nil {
		t.Fatalf("store k3 failed: %v", err)
	}

	hits := 0
	if _, ok := repo.GetCompiledQuery("k1"); ok {
		hits++
	}
	if _, ok := repo.GetCompiledQuery("k2"); ok {
		hits++
	}
	if _, ok := repo.GetCompiledQuery("k3"); ok {
		hits++
	}
	if hits > 2 {
		t.Fatalf("expected cache to keep at most 2 entries, got %d", hits)
	}
}

func TestCompiledQueryCacheStoreAndGet(t *testing.T) {
	repo := &Repository{compiledQueryCache: NewCompiledQueryCache()}

	if err := repo.StoreCompiledQuery("k1", "SELECT 1", 1, "a"); err != nil {
		t.Fatalf("StoreCompiledQuery failed: %v", err)
	}

	q, ok := repo.GetCompiledQuery("k1")
	if !ok {
		t.Fatalf("expected cache hit")
	}
	if q.Query != "SELECT 1" {
		t.Fatalf("unexpected query: %s", q.Query)
	}
	if len(q.Args) != 2 || q.Args[0] != 1 || q.Args[1] != "a" {
		t.Fatalf("unexpected args: %+v", q.Args)
	}

	// 确认返回切片是副本，避免外部修改污染缓存
	q.Args[0] = 999
	q2, ok := repo.GetCompiledQuery("k1")
	if !ok {
		t.Fatalf("expected cache hit on second read")
	}
	if q2.Args[0] != 1 {
		t.Fatalf("expected cached args to be immutable copy, got %+v", q2.Args)
	}
}

func TestBuildAndCacheQuery(t *testing.T) {
	repo := &Repository{compiledQueryCache: NewCompiledQueryCache()}

	schema := NewBaseSchema("users")
	schema.AddField(NewField("id", TypeInteger).PrimaryKey().Build())
	schema.AddField(NewField("name", TypeString).Build())

	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Select("id", "name").Where(Eq("name", "alice")).OrderBy("id", "DESC").Limit(10)

	query1, args1, hit1, err := repo.BuildAndCacheQuery(context.Background(), "users:alice:top10", qc)
	if err != nil {
		t.Fatalf("BuildAndCacheQuery first call failed: %v", err)
	}
	if hit1 {
		t.Fatalf("expected first call cache miss")
	}

	query2, args2, hit2, err := repo.BuildAndCacheQuery(context.Background(), "users:alice:top10", qc)
	if err != nil {
		t.Fatalf("BuildAndCacheQuery second call failed: %v", err)
	}
	if !hit2 {
		t.Fatalf("expected second call cache hit")
	}
	if query1 != query2 {
		t.Fatalf("expected same query, got %q vs %q", query1, query2)
	}
	if len(args1) != len(args2) {
		t.Fatalf("expected same args length, got %d vs %d", len(args1), len(args2))
	}
}

func TestCompiledQueryCacheInvalidateAndClear(t *testing.T) {
	repo := &Repository{compiledQueryCache: NewCompiledQueryCache()}
	if err := repo.StoreCompiledQuery("k1", "SELECT 1"); err != nil {
		t.Fatalf("store k1 failed: %v", err)
	}
	if err := repo.StoreCompiledQuery("k2", "SELECT 2"); err != nil {
		t.Fatalf("store k2 failed: %v", err)
	}

	repo.InvalidateCompiledQuery("k1")
	if _, ok := repo.GetCompiledQuery("k1"); ok {
		t.Fatalf("expected k1 invalidated")
	}
	if _, ok := repo.GetCompiledQuery("k2"); !ok {
		t.Fatalf("expected k2 still exists")
	}

	repo.ClearCompiledQueryCache()
	if _, ok := repo.GetCompiledQuery("k2"); ok {
		t.Fatalf("expected cache cleared")
	}
}

func TestBuildAndCacheQueryTemplate(t *testing.T) {
	repo := &Repository{compiledQueryCache: NewCompiledQueryCache()}

	schema := NewBaseSchema("users")
	schema.AddField(NewField("id", TypeInteger).PrimaryKey().Build())
	schema.AddField(NewField("name", TypeString).Build())

	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Select("id", "name").Where(Eq("name", "alice")).OrderBy("id", "DESC").Limit(10)

	query1, argc1, hit1, err := repo.BuildAndCacheQueryTemplate(context.Background(), "tpl:users:by_name", qc)
	if err != nil {
		t.Fatalf("BuildAndCacheQueryTemplate first call failed: %v", err)
	}
	if hit1 {
		t.Fatalf("expected first call cache miss")
	}
	if argc1 != 1 {
		t.Fatalf("expected arg count=1, got %d", argc1)
	}

	query2, argc2, hit2, err := repo.BuildAndCacheQueryTemplate(context.Background(), "tpl:users:by_name", qc)
	if err != nil {
		t.Fatalf("BuildAndCacheQueryTemplate second call failed: %v", err)
	}
	if !hit2 {
		t.Fatalf("expected second call cache hit")
	}
	if query1 != query2 || argc1 != argc2 {
		t.Fatalf("expected same template result, got query(%q,%q) argc(%d,%d)", query1, query2, argc1, argc2)
	}
}

func TestQueryWithCachedTemplateArgCountValidation(t *testing.T) {
	repo := &Repository{compiledQueryCache: NewCompiledQueryCache()}
	if err := repo.StoreCompiledQueryTemplate("tpl:k", "SELECT 1 WHERE ? = ?", 2); err != nil {
		t.Fatalf("store template failed: %v", err)
	}
	if _, err := repo.QueryWithCachedTemplate(context.Background(), "tpl:k", 1); err == nil {
		t.Fatalf("expected arg count mismatch error")
	}
}

func TestExecAndQueryWithCachedTemplateSQLite(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "tpl-cache.db"),
		},
	}
	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create repository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	if _, err := repo.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	if err := repo.StoreCompiledQueryTemplate("tpl:insert_user", "INSERT INTO users (id, name) VALUES (?, ?)", 2); err != nil {
		t.Fatalf("store insert template failed: %v", err)
	}
	if _, err := repo.ExecWithCachedTemplate(ctx, "tpl:insert_user", 1, "alice"); err != nil {
		t.Fatalf("exec with template failed: %v", err)
	}

	if err := repo.StoreCompiledQueryTemplate("tpl:select_user", "SELECT name FROM users WHERE id = ?", 1); err != nil {
		t.Fatalf("store select template failed: %v", err)
	}
	row, err := repo.QueryRowWithCachedTemplate(ctx, "tpl:select_user", 1)
	if err != nil {
		t.Fatalf("query row with template failed: %v", err)
	}
	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if name != "alice" {
		t.Fatalf("expected alice, got %s", name)
	}
}

func TestNewRepositoryUsesConfiguredQueryCacheSize(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "cache-size.db"),
		},
		QueryCache: &QueryCacheConfig{MaxEntries: 1},
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create repository failed: %v", err)
	}
	defer repo.Close()

	if err := repo.StoreCompiledQuery("k1", "SELECT 1"); err != nil {
		t.Fatalf("store k1 failed: %v", err)
	}
	if err := repo.StoreCompiledQuery("k2", "SELECT 2"); err != nil {
		t.Fatalf("store k2 failed: %v", err)
	}

	if _, ok := repo.GetCompiledQuery("k1"); ok {
		t.Fatalf("expected k1 to be evicted when max entries is 1")
	}
	if _, ok := repo.GetCompiledQuery("k2"); !ok {
		t.Fatalf("expected k2 to remain in cache")
	}
}

func TestCompiledQueryCacheTTLExpiration(t *testing.T) {
	repo := &Repository{compiledQueryCache: NewCompiledQueryCacheWithOptions(16, 30*time.Millisecond, true)}

	if err := repo.StoreCompiledQuery("ttl:k1", "SELECT 1"); err != nil {
		t.Fatalf("store ttl:k1 failed: %v", err)
	}
	if _, ok := repo.GetCompiledQuery("ttl:k1"); !ok {
		t.Fatalf("expected cache hit immediately after store")
	}

	time.Sleep(80 * time.Millisecond)
	if _, ok := repo.GetCompiledQuery("ttl:k1"); ok {
		t.Fatalf("expected key to expire by ttl")
	}
}

func TestCompiledQueryCacheStats(t *testing.T) {
	repo := &Repository{compiledQueryCache: NewCompiledQueryCacheWithOptions(16, 0, true)}

	if err := repo.StoreCompiledQuery("stats:k1", "SELECT 1"); err != nil {
		t.Fatalf("store stats:k1 failed: %v", err)
	}
	if _, ok := repo.GetCompiledQuery("stats:k1"); !ok {
		t.Fatalf("expected hit for stats:k1")
	}
	if _, ok := repo.GetCompiledQuery("stats:not_found"); ok {
		t.Fatalf("expected miss for absent key")
	}

	stats := repo.GetCompiledQueryCacheStats()
	if !stats.Enabled {
		t.Fatalf("expected metrics enabled")
	}
	if stats.Hits == 0 {
		t.Fatalf("expected hits > 0, got %+v", stats)
	}
	if stats.Misses == 0 {
		t.Fatalf("expected misses > 0, got %+v", stats)
	}
}
