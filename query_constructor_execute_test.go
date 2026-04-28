package db

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

type routingHookTestAdapter struct{}

func (a *routingHookTestAdapter) Connect(ctx context.Context, config *Config) error { return nil }
func (a *routingHookTestAdapter) Close() error                                      { return nil }
func (a *routingHookTestAdapter) Ping(ctx context.Context) error                    { return nil }
func (a *routingHookTestAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, fmt.Errorf("not supported")
}
func (a *routingHookTestAdapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("not supported")
}
func (a *routingHookTestAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return &sql.Row{}
}
func (a *routingHookTestAdapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("not supported")
}
func (a *routingHookTestAdapter) GetRawConn() interface{} { return nil }
func (a *routingHookTestAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return fmt.Errorf("not supported")
}
func (a *routingHookTestAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return fmt.Errorf("not supported")
}
func (a *routingHookTestAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, nil
}
func (a *routingHookTestAdapter) GetQueryBuilderProvider() QueryConstructorProvider { return nil }
func (a *routingHookTestAdapter) GetDatabaseFeatures() *DatabaseFeatures            { return nil }
func (a *routingHookTestAdapter) GetQueryFeatures() *QueryFeatures                  { return nil }

func TestExecuteQueryConstructorSQLite(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "execute-qc.db"),
		},
	}
	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create repository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	if _, err := repo.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER NOT NULL)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if _, err := repo.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'alice', 18), (2, 'bob', 20)"); err != nil {
		t.Fatalf("insert seed data failed: %v", err)
	}

	schema := NewBaseSchema("users")
	qc, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}

	qc.Select("id", "name").Where(Eq("name", "alice"))
	result, err := repo.ExecuteQueryConstructor(ctx, qc)
	if err != nil {
		t.Fatalf("execute query constructor failed: %v", err)
	}

	if result == nil {
		t.Fatalf("expected execution result")
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0]["name"] != "alice" {
		t.Fatalf("expected row name=alice, got %+v", result.Rows[0])
	}
	if len(result.Args) != 1 || result.Args[0] != "alice" {
		t.Fatalf("unexpected execution args: %+v", result.Args)
	}
}

func TestExecuteQueryConstructorNeo4jRouting(t *testing.T) {
	cfg := &Config{
		Adapter: "neo4j",
		Neo4j: &Neo4jConnectionConfig{
			URI:      "neo4j://localhost:7687",
			Username: "neo4j",
			Password: "pass",
			Database: "neo4j",
		},
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create neo4j repository failed: %v", err)
	}
	defer repo.Close()

	schema := NewBaseSchema("User")
	qc, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}
	qc.FromAlias("u").Where(Eq("name", "alice")).Select("u")

	_, err = repo.ExecuteQueryConstructor(context.Background(), qc)
	if err == nil {
		t.Fatalf("expected not connected neo4j error")
	}
}

func TestExecuteQueryConstructorMongoRouting(t *testing.T) {
	cfg := &Config{
		Adapter: "mongodb",
		MongoDB: &MongoConnectionConfig{
			URI:      "mongodb://localhost:27017",
			Database: "test_db",
		},
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create mongodb repository failed: %v", err)
	}
	defer repo.Close()

	schema := NewBaseSchema("users")
	qc, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}
	qc.Where(Eq("name", "alice")).Select("name")

	_, err = repo.ExecuteQueryConstructor(context.Background(), qc)
	if err == nil {
		t.Fatalf("expected not connected mongodb error")
	}
}

func TestExecuteQueryConstructorNilConstructor(t *testing.T) {
	repo := &Repository{adapter: &SQLiteAdapter{}}
	_, err := repo.ExecuteQueryConstructor(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected nil constructor error")
	}
}

func TestExecuteQueryConstructorWithCacheSQLite(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "execute-qc-cache.db"),
		},
		QueryCache: &QueryCacheConfig{MaxEntries: 16, DefaultTTLSeconds: 60, EnableMetrics: true},
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
	if _, err := repo.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		t.Fatalf("insert seed data failed: %v", err)
	}

	schema := NewBaseSchema("users")
	qc1, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}
	qc1.Select("id", "name").Where(Eq("name", "alice"))

	result1, hit1, err := repo.ExecuteQueryConstructorWithCache(ctx, "users:by_name:alice", qc1)
	if err != nil {
		t.Fatalf("first execute with cache failed: %v", err)
	}
	if hit1 {
		t.Fatalf("expected first execution cache miss")
	}
	if len(result1.Rows) != 1 {
		t.Fatalf("expected first execution 1 row, got %d", len(result1.Rows))
	}

	qc2, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}
	qc2.Select("id", "name").Where(Eq("name", "alice"))

	result2, hit2, err := repo.ExecuteQueryConstructorWithCache(ctx, "users:by_name:alice", qc2)
	if err != nil {
		t.Fatalf("second execute with cache failed: %v", err)
	}
	if !hit2 {
		t.Fatalf("expected second execution cache hit")
	}
	if result2.Rows[0]["name"] != "alice" {
		t.Fatalf("expected cached execution row name=alice, got %+v", result2.Rows[0])
	}
}

func TestExecuteQueryConstructorWithCacheEmptyKey(t *testing.T) {
	repo := &Repository{adapter: &SQLiteAdapter{}, compiledQueryCache: NewCompiledQueryCache()}
	schema := NewBaseSchema("users")
	qc := NewSQLQueryConstructor(schema, NewSQLiteDialect())
	qc.Select("id")

	_, _, err := repo.ExecuteQueryConstructorWithCache(context.Background(), "", qc)
	if err == nil {
		t.Fatalf("expected empty cache key error")
	}
}

func TestExecuteQueryConstructorAutoSQLiteQuery(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "execute-qc-auto-query.db"),
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
	if _, err := repo.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert seed data failed: %v", err)
	}

	schema := NewBaseSchema("users")
	qc := NewSQLQueryConstructor(schema, NewSQLiteDialect())
	qc.Select("id", "name").Where(Eq("name", "alice"))

	result, err := repo.ExecuteQueryConstructorAuto(ctx, qc)
	if err != nil {
		t.Fatalf("execute auto failed: %v", err)
	}
	if result.Mode != "query" {
		t.Fatalf("expected query mode, got %s", result.Mode)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
}

func TestExecuteQueryConstructorAutoCustomDescriptorHook(t *testing.T) {
	adapterName := "custom_hook_exec"

	MustRegisterAdapterDescriptor(adapterName, AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) {
			return &routingHookTestAdapter{}, nil
		},
		ValidateConfig: func(cfg *Config) error { return nil },
		ExecuteQueryConstructorAuto: func(ctx context.Context, adapter Adapter, query string, args []interface{}) (*QueryConstructorAutoExecutionResult, bool, error) {
			return &QueryConstructorAutoExecutionResult{
				Mode:      "query",
				Statement: query,
				Args:      copyQueryArgs(args),
				Rows:      []map[string]interface{}{{"source": "descriptor_hook"}},
			}, true, nil
		},
	})

	repo, err := NewRepository(&Config{Adapter: adapterName})
	if err != nil {
		t.Fatalf("create custom repository failed: %v", err)
	}
	defer repo.Close()

	q := NewSQLQueryConstructor(NewBaseSchema("users"), NewSQLiteDialect())
	q.Select("id").Where(Eq("name", "alice"))

	result, err := repo.ExecuteQueryConstructorAuto(context.Background(), q)
	if err != nil {
		t.Fatalf("execute via descriptor hook failed: %v", err)
	}
	if result == nil || len(result.Rows) != 1 {
		t.Fatalf("expected one row routed by descriptor hook, got %+v", result)
	}
	if result.Rows[0]["source"] != "descriptor_hook" {
		t.Fatalf("unexpected hook payload: %+v", result.Rows[0])
	}
}

func TestExecuteQueryConstructorPagedWithCacheSQLite(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "execute-qc-paged-cache.db"),
		},
		QueryCache: &QueryCacheConfig{MaxEntries: 16, DefaultTTLSeconds: 60, EnableMetrics: true},
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
	if _, err := repo.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'cindy'), (4, 'david'), (5, 'ellen')"); err != nil {
		t.Fatalf("insert seed data failed: %v", err)
	}

	schema := NewBaseSchema("users")
	qc1, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}
	qc1.Select("id", "name").OrderBy("id", "ASC")

	result1, err := repo.ExecuteQueryConstructorPagedWithCache(ctx, "users:list", qc1, 2, 2)
	if err != nil {
		t.Fatalf("first paged execute failed: %v", err)
	}
	if result1.QueryCacheHit || result1.CountCacheHit {
		t.Fatalf("expected first paged execution to miss cache, got query=%v count=%v", result1.QueryCacheHit, result1.CountCacheHit)
	}
	if result1.Total != 5 || result1.TotalPages != 3 {
		t.Fatalf("unexpected total metadata: %+v", result1)
	}
	if len(result1.Rows) != 2 {
		t.Fatalf("expected 2 rows on page 2, got %d", len(result1.Rows))
	}
	if result1.Rows[0]["name"] != "cindy" || result1.Rows[1]["name"] != "david" {
		t.Fatalf("unexpected paged rows: %+v", result1.Rows)
	}
	if !result1.HasNext || !result1.HasPrevious {
		t.Fatalf("expected page 2 to have both previous and next pages, got %+v", result1)
	}

	qc2, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}
	qc2.Select("id", "name").OrderBy("id", "ASC")

	result2, err := repo.ExecuteQueryConstructorPagedWithCache(ctx, "users:list", qc2, 2, 2)
	if err != nil {
		t.Fatalf("second paged execute failed: %v", err)
	}
	if !result2.QueryCacheHit || !result2.CountCacheHit {
		t.Fatalf("expected second paged execution cache hits, got query=%v count=%v", result2.QueryCacheHit, result2.CountCacheHit)
	}
	if result2.Offset != 2 || result2.Page != 2 || result2.PageSize != 2 {
		t.Fatalf("unexpected pagination coordinates: %+v", result2)
	}
	if result2.Statement == "" {
		t.Fatalf("expected compiled statement to be returned")
	}
}

func TestExecuteQueryConstructorPagedSQLite(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "execute-qc-paged.db"),
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
	if _, err := repo.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'cindy')"); err != nil {
		t.Fatalf("insert seed data failed: %v", err)
	}

	schema := NewBaseSchema("users")
	qc, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}
	qc.Select("id", "name").OrderBy("id", "ASC")

	result, err := repo.ExecuteQueryConstructorPaged(ctx, qc, 1, 2)
	if err != nil {
		t.Fatalf("paged execute failed: %v", err)
	}
	if result.QueryCacheHit || result.CountCacheHit {
		t.Fatalf("expected no cache hits for non-cached pagination, got %+v", result)
	}
	if result.Total != 3 || result.TotalPages != 2 {
		t.Fatalf("unexpected pagination totals: %+v", result)
	}
	if result.HasPrevious || !result.HasNext {
		t.Fatalf("unexpected navigation flags: %+v", result)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected first page to contain 2 rows, got %d", len(result.Rows))
	}
}

func TestExecuteQueryConstructorPaginatedOffsetBuilderSQLite(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "execute-qc-paginated-builder-offset.db"),
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
	if _, err := repo.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'cindy'), (4, 'david')"); err != nil {
		t.Fatalf("insert seed data failed: %v", err)
	}

	schema := NewBaseSchema("users")
	qc, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}
	qc.Select("id", "name").OrderBy("id", "ASC")

	builder := NewPaginationBuilder(2, 2).OffsetOnly()
	result, err := repo.ExecuteQueryConstructorPaginated(ctx, qc, builder)
	if err != nil {
		t.Fatalf("paginated execute failed: %v", err)
	}
	if result.Page != 2 || result.PageSize != 2 || result.Offset != 2 {
		t.Fatalf("unexpected pagination metadata: %+v", result)
	}
	if result.Total != 4 || result.TotalPages != 2 {
		t.Fatalf("unexpected total metadata: %+v", result)
	}
	if len(result.Rows) != 2 || result.Rows[0]["name"] != "cindy" || result.Rows[1]["name"] != "david" {
		t.Fatalf("unexpected rows: %+v", result.Rows)
	}
}

func TestExecuteQueryConstructorPaginatedCursorBuilderSQLite(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "execute-qc-paginated-builder-cursor.db"),
		},
	}
	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create repository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	if _, err := repo.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, name TEXT NOT NULL)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if _, err := repo.Exec(ctx, "INSERT INTO users (id, created_at, name) VALUES (1, '2026-03-21T10:00:00Z', 'alice'), (2, '2026-03-21T10:00:00Z', 'bob'), (3, '2026-03-21T10:00:00Z', 'cindy'), (4, '2026-03-21T11:00:00Z', 'david')"); err != nil {
		t.Fatalf("insert seed data failed: %v", err)
	}

	schema := NewBaseSchema("users")
	schema.AddField(NewField("id", TypeInteger).PrimaryKey().Build())
	schema.AddField(NewField("created_at", TypeString).Build())
	schema.AddField(NewField("name", TypeString).Build())

	qc := NewSQLQueryConstructor(schema, NewSQLiteDialect())
	qc.Select("id", "name", "created_at")

	builder := NewCursorPaginationBuilder("created_at", "ASC", "2026-03-21T10:00:00Z", 1, 2)
	result, err := repo.ExecuteQueryConstructorPaginated(ctx, qc, builder)
	if err != nil {
		t.Fatalf("cursor paginated execute failed: %v", err)
	}
	if result.Page != 1 || result.Offset != 0 || result.PageSize != 2 {
		t.Fatalf("unexpected cursor pagination metadata: %+v", result)
	}
	if len(result.Rows) != 2 || result.Rows[0]["name"] != "bob" || result.Rows[1]["name"] != "cindy" {
		t.Fatalf("unexpected cursor rows: %+v", result.Rows)
	}
}

func TestSQLQueryConstructorCursorPageSQLite(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "execute-qc-cursor.db"),
		},
	}
	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create repository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	if _, err := repo.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, name TEXT NOT NULL)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if _, err := repo.Exec(ctx, "INSERT INTO users (id, created_at, name) VALUES (1, '2026-03-21T10:00:00Z', 'alice'), (2, '2026-03-21T10:00:00Z', 'bob'), (3, '2026-03-21T10:00:00Z', 'cindy'), (4, '2026-03-21T11:00:00Z', 'david')"); err != nil {
		t.Fatalf("insert seed data failed: %v", err)
	}

	schema := NewBaseSchema("users")
	schema.AddField(NewField("id", TypeInteger).PrimaryKey().Build())
	schema.AddField(NewField("created_at", TypeString).Build())
	schema.AddField(NewField("name", TypeString).Build())

	qc := NewSQLQueryConstructor(schema, NewSQLiteDialect())
	qc.Select("id", "name", "created_at")
	qc.CursorPage("created_at", "ASC", "2026-03-21T10:00:00Z", 1, 2)

	result, err := repo.ExecuteQueryConstructor(ctx, qc)
	if err != nil {
		t.Fatalf("execute cursor page failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 cursor rows, got %d", len(result.Rows))
	}
	if result.Rows[0]["name"] != "bob" || result.Rows[1]["name"] != "cindy" {
		t.Fatalf("unexpected cursor rows: %+v", result.Rows)
	}
}

func TestExecuteQueryConstructorAutoSQLiteExec(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "execute-qc-auto-exec.db"),
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

	schema := NewBaseSchema("users")
	qc := NewSQLQueryConstructor(schema, NewSQLiteDialect())
	qc.Select("id")
	builtSQL, args, err := qc.Build(ctx)
	if err != nil {
		t.Fatalf("build query failed: %v", err)
	}
	_ = args

	qcExec := &staticQueryConstructor{query: "INSERT INTO users (id, name) VALUES (?, ?)", args: []interface{}{2, "bob"}}
	result, err := repo.ExecuteQueryConstructorAuto(ctx, qcExec)
	if err != nil {
		t.Fatalf("execute auto exec failed: %v", err)
	}
	if result.Mode != "exec" {
		t.Fatalf("expected exec mode, got %s", result.Mode)
	}
	if result.Exec == nil {
		t.Fatalf("expected exec summary")
	}
	if result.Exec.RowsAffected != 1 {
		t.Fatalf("expected rows affected=1, got %d", result.Exec.RowsAffected)
	}

	_ = builtSQL
}

func TestExecuteQueryConstructorAutoMongoWriteRouting(t *testing.T) {
	cfg := &Config{
		Adapter: "mongodb",
		MongoDB: &MongoConnectionConfig{
			URI:      "mongodb://localhost:27017",
			Database: "test_db",
		},
	}
	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create mongodb repository failed: %v", err)
	}
	defer repo.Close()

	qc := &staticQueryConstructor{query: "MONGO_WRITE::{\"operation\":\"insert_one\",\"collection\":\"users\",\"document\":{\"name\":\"alice\"}}", args: nil}
	_, err = repo.ExecuteQueryConstructorAuto(context.Background(), qc)
	if err == nil {
		t.Fatalf("expected not connected mongodb error")
	}
}

func TestExecuteQueryConstructorMongoWritePlanRejectedByQueryOnlyAPI(t *testing.T) {
	repo := &Repository{adapter: &MongoAdapter{}}
	qc := &staticQueryConstructor{query: "MONGO_WRITE::{\"operation\":\"delete_many\",\"collection\":\"users\"}", args: nil}
	_, err := repo.ExecuteQueryConstructor(context.Background(), qc)
	if err == nil {
		t.Fatalf("expected query-only api to reject mongodb write plans")
	}
}

func TestExecuteQueryConstructorAutoNeo4jExecRouting(t *testing.T) {
	cfg := &Config{
		Adapter: "neo4j",
		Neo4j: &Neo4jConnectionConfig{
			URI:      "neo4j://localhost:7687",
			Username: "neo4j",
			Password: "pass",
			Database: "neo4j",
		},
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create neo4j repository failed: %v", err)
	}
	defer repo.Close()

	qc := &staticQueryConstructor{query: "CREATE (n:User {name: $p1})", args: []interface{}{"alice"}}
	_, err = repo.ExecuteQueryConstructorAuto(context.Background(), qc)
	if err == nil {
		t.Fatalf("expected not connected neo4j error")
	}
}

func TestExecuteAutoSQLiteQueryAndExec(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlite",
		SQLite: &SQLiteConnectionConfig{
			Path: filepath.Join(t.TempDir(), "execute-auto-sqlite.db"),
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

	execResult, err := repo.ExecuteAuto(ctx, "INSERT INTO users (id, name) VALUES (?, ?)", 1, "alice")
	if err != nil {
		t.Fatalf("execute auto insert failed: %v", err)
	}
	if execResult.Mode != "exec" {
		t.Fatalf("expected exec mode, got %s", execResult.Mode)
	}
	if execResult.Exec == nil || execResult.Exec.RowsAffected != 1 {
		t.Fatalf("expected rows affected=1, got %+v", execResult.Exec)
	}

	queryResult, err := repo.ExecuteAuto(ctx, "SELECT id, name FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("execute auto select failed: %v", err)
	}
	if queryResult.Mode != "query" {
		t.Fatalf("expected query mode, got %s", queryResult.Mode)
	}
	if len(queryResult.Rows) != 1 || queryResult.Rows[0]["name"] != "alice" {
		t.Fatalf("unexpected query rows: %+v", queryResult.Rows)
	}
}

func TestExecuteAutoArangoDescriptorRouting(t *testing.T) {
	repo := &Repository{adapter: &ArangoAdapter{}, adapterType: "arango"}
	_, err := repo.ExecuteAuto(context.Background(), "FOR d IN users RETURN d")
	if err == nil {
		t.Fatalf("expected arango runtime error when adapter is not connected")
	}
	if !strings.Contains(err.Error(), "arango client not connected") {
		t.Fatalf("unexpected arango error: %v", err)
	}
}

type staticQueryConstructor struct {
	query string
	args  []interface{}
}

func (s *staticQueryConstructor) Where(condition Condition) QueryConstructor { return s }
func (s *staticQueryConstructor) WhereWith(builder *WhereBuilder) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) WhereAll(conditions ...Condition) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) WhereAny(conditions ...Condition) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) Select(fields ...string) QueryConstructor   { return s }
func (s *staticQueryConstructor) Count(fieldName ...string) QueryConstructor { return s }
func (s *staticQueryConstructor) CountWith(builder *CountBuilder) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) OrderBy(field string, direction string) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) Limit(count int) QueryConstructor  { return s }
func (s *staticQueryConstructor) Offset(count int) QueryConstructor { return s }
func (s *staticQueryConstructor) Page(page int, pageSize int) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) Paginate(builder *PaginationBuilder) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) FromAlias(alias string) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) Join(table, onClause string, alias ...string) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) LeftJoin(table, onClause string, alias ...string) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) RightJoin(table, onClause string, alias ...string) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) CrossJoin(table string, alias ...string) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) CrossTableStrategy(strategy CrossTableStrategy) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) JoinWith(builder *JoinBuilder) QueryConstructor { return s }
func (s *staticQueryConstructor) CustomMode() QueryConstructor                   { return s }
func (s *staticQueryConstructor) Build(ctx context.Context) (string, []interface{}, error) {
	return s.query, copyQueryArgs(s.args), nil
}
func (s *staticQueryConstructor) SelectCount(ctx context.Context, repo *Repository) (int64, error) {
	return 0, fmt.Errorf("static query constructor does not implement SelectCount")
}
func (s *staticQueryConstructor) Upsert(ctx context.Context, repo *Repository, cs *Changeset, conflictColumns ...string) (sql.Result, error) {
	return nil, fmt.Errorf("static query constructor does not implement Upsert")
}
func (s *staticQueryConstructor) GetNativeBuilder() interface{} { return s }
