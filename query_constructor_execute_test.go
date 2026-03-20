package db

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

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
func (s *staticQueryConstructor) Select(fields ...string) QueryConstructor { return s }
func (s *staticQueryConstructor) Count(fieldName ...string) QueryConstructor { return s }
func (s *staticQueryConstructor) CountWith(builder *CountBuilder) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) OrderBy(field string, direction string) QueryConstructor {
	return s
}
func (s *staticQueryConstructor) Limit(count int) QueryConstructor  { return s }
func (s *staticQueryConstructor) Offset(count int) QueryConstructor { return s }
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
func (s *staticQueryConstructor) CustomMode() QueryConstructor { return s }
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
