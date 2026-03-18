package db

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestBuildFuzzyConditionMySQLUsesFullText(t *testing.T) {
	repo := &Repository{adapter: &MySQLAdapter{}}
	cond := repo.BuildFuzzyCondition("title", "golang", "8.0.36")

	simple, ok := cond.(*SimpleCondition)
	if !ok {
		t.Fatalf("expected *SimpleCondition, got %T", cond)
	}
	if simple.Operator != "full_text" {
		t.Fatalf("expected full_text operator, got %s", simple.Operator)
	}

	schema := NewBaseSchema("posts")
	schema.AddField(NewField("title", TypeString).Build())
	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(cond)

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if !strings.Contains(sql, "MATCH (`posts`.`title`) AGAINST (? IN NATURAL LANGUAGE MODE)") {
		t.Fatalf("expected mysql full text sql, got: %s", sql)
	}
	if len(args) != 1 || args[0] != "golang" {
		t.Fatalf("expected args [golang], got %v", args)
	}
}

func TestBuildFuzzyConditionSQLiteFallsBackToLike(t *testing.T) {
	repo := &Repository{adapter: &SQLiteAdapter{}}
	cond := repo.BuildFuzzyCondition("title", "golang", "3.45.0")

	simple, ok := cond.(*SimpleCondition)
	if !ok {
		t.Fatalf("expected *SimpleCondition, got %T", cond)
	}
	if simple.Operator != "like" {
		t.Fatalf("expected like operator fallback, got %s", simple.Operator)
	}
	if simple.Value != "%golang%" {
		t.Fatalf("expected like value %%golang%%, got %v", simple.Value)
	}
}

func TestFullTextConditionPostgresTranslation(t *testing.T) {
	schema := NewBaseSchema("articles")
	schema.AddField(NewField("content", TypeString).Build())

	qc := NewSQLQueryConstructor(schema, NewPostgreSQLDialect())
	qc.Where(FullText("content", "search words"))

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	if !strings.Contains(sql, "to_tsvector('simple', \"articles\".\"content\") @@ plainto_tsquery('simple', $1)") {
		t.Fatalf("expected postgres full text sql, got: %s", sql)
	}
	if len(args) != 1 || args[0] != "search words" {
		t.Fatalf("expected args [search words], got %v", args)
	}
}

func TestFullTextConditionSQLiteTranslatorFallbackLike(t *testing.T) {
	schema := NewBaseSchema("notes")
	schema.AddField(NewField("body", TypeString).Build())

	qc := NewSQLQueryConstructor(schema, NewSQLiteDialect())
	qc.Where(FullText("body", "abc"))

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	if !strings.Contains(sql, "`notes`.`body` LIKE ?") {
		t.Fatalf("expected sqlite fallback LIKE sql, got: %s", sql)
	}
	if len(args) != 1 || args[0] != "%abc%" {
		t.Fatalf("expected args [%%abc%%], got %v", args)
	}
}

type mockFuzzyRuntimeAdapter struct {
	features   *QueryFeatures
	runtime    *FullTextRuntimeCapability
	runtimeErr error
}

func (m *mockFuzzyRuntimeAdapter) Connect(ctx context.Context, config *Config) error { return nil }
func (m *mockFuzzyRuntimeAdapter) Close() error                                      { return nil }
func (m *mockFuzzyRuntimeAdapter) Ping(ctx context.Context) error                    { return nil }
func (m *mockFuzzyRuntimeAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, errors.New("not implemented")
}
func (m *mockFuzzyRuntimeAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("not implemented")
}
func (m *mockFuzzyRuntimeAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}
func (m *mockFuzzyRuntimeAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return nil, errors.New("not implemented")
}
func (m *mockFuzzyRuntimeAdapter) GetRawConn() interface{} { return nil }
func (m *mockFuzzyRuntimeAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return errors.New("not implemented")
}
func (m *mockFuzzyRuntimeAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return errors.New("not implemented")
}
func (m *mockFuzzyRuntimeAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, errors.New("not implemented")
}
func (m *mockFuzzyRuntimeAdapter) GetQueryBuilderProvider() QueryConstructorProvider { return nil }
func (m *mockFuzzyRuntimeAdapter) GetDatabaseFeatures() *DatabaseFeatures            { return &DatabaseFeatures{} }
func (m *mockFuzzyRuntimeAdapter) GetQueryFeatures() *QueryFeatures {
	if m.features == nil {
		return &QueryFeatures{}
	}
	return m.features
}
func (m *mockFuzzyRuntimeAdapter) InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error) {
	if m.runtimeErr != nil {
		return nil, m.runtimeErr
	}
	return m.runtime, nil
}

func TestAnalyzeFuzzySearchPostgresPluginMissingFallback(t *testing.T) {
	adapter := &mockFuzzyRuntimeAdapter{
		features: &QueryFeatures{SupportsFullTextSearch: true},
		runtime: &FullTextRuntimeCapability{
			NativeSupported:       true,
			PluginChecked:         true,
			PluginAvailable:       false,
			TokenizationSupported: true,
			TokenizationMode:      "plugin",
		},
	}

	repo := &Repository{adapter: adapter}
	plan, err := repo.AnalyzeFuzzySearch(context.Background(), "content", "golang search", "")
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if plan.Mode != "tokenized_like_fallback" {
		t.Fatalf("expected tokenized_like_fallback, got %s", plan.Mode)
	}
}

func TestAnalyzeFuzzySearchPostgresPluginAvailableUsesFullText(t *testing.T) {
	adapter := &mockFuzzyRuntimeAdapter{
		features: &QueryFeatures{SupportsFullTextSearch: true},
		runtime: &FullTextRuntimeCapability{
			NativeSupported:       true,
			PluginChecked:         true,
			PluginAvailable:       true,
			PluginName:            "zhparser",
			TokenizationSupported: true,
			TokenizationMode:      "plugin",
		},
	}

	repo := &Repository{adapter: adapter}
	plan, err := repo.AnalyzeFuzzySearch(context.Background(), "content", "golang", "")
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if plan.Mode != "full_text" {
		t.Fatalf("expected full_text mode, got %s", plan.Mode)
	}
	simple, ok := plan.Condition.(*SimpleCondition)
	if !ok || simple.Operator != "full_text" {
		t.Fatalf("expected full_text condition, got %T %+v", plan.Condition, plan.Condition)
	}
}

func TestMongoCustomTokenizedSearchPlan(t *testing.T) {
	adapter := &MongoAdapter{}
	result, err := adapter.ExecuteCustomFeature(context.Background(), "tokenized_full_text_search", map[string]interface{}{
		"collection": "docs",
		"query":      "go 数据库 搜索",
		"fields":     []string{"title", "body"},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	resultMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map result, got %T", result)
	}
	if resultMap["strategy"] != "tokenized_regex_pipeline" {
		t.Fatalf("expected tokenized_regex_pipeline strategy, got %v", resultMap["strategy"])
	}
}
