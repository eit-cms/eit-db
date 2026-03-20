package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

type upsertFallbackAdapter struct {
	inner Adapter
}

func (a *upsertFallbackAdapter) Connect(ctx context.Context, config *Config) error {
	return a.inner.Connect(ctx, config)
}
func (a *upsertFallbackAdapter) Close() error { return a.inner.Close() }
func (a *upsertFallbackAdapter) Ping(ctx context.Context) error { return a.inner.Ping(ctx) }
func (a *upsertFallbackAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return a.inner.Begin(ctx, opts...)
}
func (a *upsertFallbackAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return a.inner.Query(ctx, sql, args...)
}
func (a *upsertFallbackAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return a.inner.QueryRow(ctx, sql, args...)
}
func (a *upsertFallbackAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return a.inner.Exec(ctx, sql, args...)
}
func (a *upsertFallbackAdapter) GetRawConn() interface{} { return a.inner.GetRawConn() }
func (a *upsertFallbackAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return a.inner.RegisterScheduledTask(ctx, task)
}
func (a *upsertFallbackAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return a.inner.UnregisterScheduledTask(ctx, taskName)
}
func (a *upsertFallbackAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return a.inner.ListScheduledTasks(ctx)
}
func (a *upsertFallbackAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return a.inner.GetQueryBuilderProvider()
}
func (a *upsertFallbackAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return a.inner.GetDatabaseFeatures()
}
func (a *upsertFallbackAdapter) GetQueryFeatures() *QueryFeatures {
	features := a.inner.GetQueryFeatures()
	if features == nil {
		features = &QueryFeatures{}
	}
	copied := *features
	copied.SupportsUpsert = false
	return &copied
}

func TestQueryConstructorV2_SelectCount_SQLite(t *testing.T) {
	cfg := &Config{Adapter: "sqlite", SQLite: &SQLiteConnectionConfig{Path: filepath.Join(t.TempDir(), "count-v2.db")}}
	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create repository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	if _, err := repo.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if _, err := repo.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'a', 18), (2, 'b', 25), (3, 'c', 31)"); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	schema := NewBaseSchema("users")
	qc, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}

	count, err := qc.Where(Gte("age", 20)).SelectCount(ctx, repo)
	if err != nil {
		t.Fatalf("select count failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}
}

func TestQueryConstructorV2_CountChainBuild_SQLite(t *testing.T) {
	qb := NewSQLQueryConstructor(NewBaseSchema("users"), NewSQLiteDialect())
	sqlText, _, err := qb.Where(Eq("id", 1)).Count("id").Build(context.Background())
	if err != nil {
		t.Fatalf("count chain build failed: %v", err)
	}
	if !strings.Contains(strings.ToUpper(sqlText), "COUNT(") {
		t.Fatalf("expected COUNT expression, got: %s", sqlText)
	}
}

func TestQueryConstructorV2_StrictSchemaValidation_DefaultMode(t *testing.T) {
	qb := NewSQLQueryConstructor(NewBaseSchema("users").AddField(&Field{Name: "id", Type: TypeInteger}), NewSQLiteDialect())
	_, _, err := qb.Select("unknown_field").Build(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not exist in schema") {
		t.Fatalf("expected strict schema validation error, got: %v", err)
	}
}

func TestQueryConstructorV2_CustomMode_AllowsExpression(t *testing.T) {
	qb := NewSQLQueryConstructor(NewBaseSchema("users").AddField(&Field{Name: "id", Type: TypeInteger}), NewSQLiteDialect())
	_, _, err := qb.CustomMode().Select("COUNT(*) as c").Build(context.Background())
	if err != nil {
		t.Fatalf("expected custom mode to allow expression, got: %v", err)
	}
}

func TestQueryConstructorV2_Upsert_SQLiteNative(t *testing.T) {
	cfg := &Config{Adapter: "sqlite", SQLite: &SQLiteConnectionConfig{Path: filepath.Join(t.TempDir(), "upsert-v2-native.db")}}
	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create repository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	if _, err := repo.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	schema.AddField(&Field{Name: "email", Type: TypeString, Unique: true})
	schema.AddField(&Field{Name: "name", Type: TypeString})

	qc, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}

	cs := NewChangeset(schema).Cast(map[string]interface{}{"id": 1, "email": "u@test.com", "name": "alice"}).Validate()
	if _, err := qc.Upsert(ctx, repo, cs, "email"); err != nil {
		t.Fatalf("first upsert failed: %v", err)
	}

	cs2 := NewChangeset(schema).Cast(map[string]interface{}{"id": 2, "email": "u@test.com", "name": "alice-updated"}).Validate()
	if _, err := qc.Upsert(ctx, repo, cs2, "email"); err != nil {
		t.Fatalf("second upsert failed: %v", err)
	}

	var id int
	var name string
	if err := repo.QueryRow(ctx, "SELECT id, name FROM users WHERE email = ?", "u@test.com").Scan(&id, &name); err != nil {
		t.Fatalf("query row failed: %v", err)
	}
	if id != 1 || name != "alice-updated" {
		t.Fatalf("unexpected row after upsert: id=%d name=%s", id, name)
	}
}

func TestQueryConstructorV2_Upsert_AutoInferConflictColumns(t *testing.T) {
	cfg := &Config{Adapter: "sqlite", SQLite: &SQLiteConnectionConfig{Path: filepath.Join(t.TempDir(), "upsert-v2-infer.db")}}
	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create repository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	if _, err := repo.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	schema.AddField(&Field{Name: "email", Type: TypeString, Unique: true})
	schema.AddField(&Field{Name: "name", Type: TypeString})

	qc, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}

	first := NewChangeset(schema).Cast(map[string]interface{}{"id": 1, "email": "infer@test.com", "name": "first"}).Validate()
	if _, err := qc.Upsert(ctx, repo, first); err != nil {
		t.Fatalf("first inferred upsert failed: %v", err)
	}
	second := NewChangeset(schema).Cast(map[string]interface{}{"id": 8, "email": "infer@test.com", "name": "second"}).Validate()
	if _, err := qc.Upsert(ctx, repo, second); err != nil {
		t.Fatalf("second inferred upsert failed: %v", err)
	}

	var id int
	var name string
	if err := repo.QueryRow(ctx, "SELECT id, name FROM users WHERE email = ?", "infer@test.com").Scan(&id, &name); err != nil {
		t.Fatalf("query row failed: %v", err)
	}
	if id != 1 || name != "second" {
		t.Fatalf("unexpected inferred upsert result: id=%d name=%s", id, name)
	}
}

func TestQueryConstructorV2_Upsert_FallbackTransaction(t *testing.T) {
	cfg := &Config{Adapter: "sqlite", SQLite: &SQLiteConnectionConfig{Path: filepath.Join(t.TempDir(), "upsert-v2-fallback.db")}}
	baseRepo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create base repository failed: %v", err)
	}
	defer baseRepo.Close()

	ctx := context.Background()
	if _, err := baseRepo.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	repo := &Repository{adapter: &upsertFallbackAdapter{inner: baseRepo.GetAdapter()}}

	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	schema.AddField(&Field{Name: "email", Type: TypeString, Unique: true})
	schema.AddField(&Field{Name: "name", Type: TypeString})

	qc, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}

	cs := NewChangeset(schema).Cast(map[string]interface{}{"id": 1, "email": "f@test.com", "name": "first"}).Validate()
	if _, err := qc.Upsert(ctx, repo, cs, "email"); err != nil {
		t.Fatalf("fallback first upsert failed: %v", err)
	}
	cs2 := NewChangeset(schema).Cast(map[string]interface{}{"id": 9, "email": "f@test.com", "name": "updated"}).Validate()
	if _, err := qc.Upsert(ctx, repo, cs2, "email"); err != nil {
		t.Fatalf("fallback second upsert failed: %v", err)
	}

	var id int
	var name string
	if err := baseRepo.QueryRow(ctx, "SELECT id, name FROM users WHERE email = ?", "f@test.com").Scan(&id, &name); err != nil {
		t.Fatalf("query row failed: %v", err)
	}
	if id != 1 || name != "updated" {
		t.Fatalf("unexpected row after fallback upsert: id=%d name=%s", id, name)
	}
}

func TestQueryConstructorV2_WhereWithBuilder_SQLite(t *testing.T) {
	cfg := &Config{Adapter: "sqlite", SQLite: &SQLiteConnectionConfig{Path: filepath.Join(t.TempDir(), "where-builder-v2.db")}}
	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("create repository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	if _, err := repo.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if _, err := repo.Exec(ctx, "INSERT INTO users (id, age) VALUES (1, 18), (2, 25), (3, 31)"); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	schema.AddField(&Field{Name: "age", Type: TypeInteger})
	qc, err := repo.NewQueryConstructor(schema)
	if err != nil {
		t.Fatalf("new query constructor failed: %v", err)
	}

	where := NewWhereBuilder(Gte("age", 20)).And(Lte("age", 30))
	count, err := qc.WhereWith(where).SelectCount(ctx, repo)
	if err != nil {
		t.Fatalf("select count with where builder failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
}

func TestQueryConstructorV2_CountWithBuilder_DistinctAs_SQLite(t *testing.T) {
	qb := NewSQLQueryConstructor(NewBaseSchema("users").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true}).
		AddField(&Field{Name: "email", Type: TypeString}), NewSQLiteDialect())

	sqlText, _, err := qb.CountWith(NewCountBuilder("email").Distinct().As("email_count")).Build(context.Background())
	if err != nil {
		t.Fatalf("count with builder build failed: %v", err)
	}
	upper := strings.ToUpper(sqlText)
	if !strings.Contains(upper, "COUNT(DISTINCT") {
		t.Fatalf("expected COUNT(DISTINCT ...) expression, got: %s", sqlText)
	}
	if !strings.Contains(upper, "EMAIL_COUNT") {
		t.Fatalf("expected COUNT alias, got: %s", sqlText)
	}
}
