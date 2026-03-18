package db

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

type mockCompositeFallbackAdapter struct {
	features *DatabaseFeatures
	provider QueryConstructorProvider
}

func (a *mockCompositeFallbackAdapter) Connect(ctx context.Context, config *Config) error { return nil }
func (a *mockCompositeFallbackAdapter) Close() error                                      { return nil }
func (a *mockCompositeFallbackAdapter) Ping(ctx context.Context) error                    { return nil }
func (a *mockCompositeFallbackAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, nil
}
func (a *mockCompositeFallbackAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}
func (a *mockCompositeFallbackAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}
func (a *mockCompositeFallbackAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}
func (a *mockCompositeFallbackAdapter) GetRawConn() interface{} { return nil }
func (a *mockCompositeFallbackAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return nil
}
func (a *mockCompositeFallbackAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return nil
}
func (a *mockCompositeFallbackAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, nil
}
func (a *mockCompositeFallbackAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return a.provider
}
func (a *mockCompositeFallbackAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return a.features
}
func (a *mockCompositeFallbackAdapter) GetQueryFeatures() *QueryFeatures {
	return NewMySQLQueryFeatures()
}

func TestBaseSchemaCompositeConstraints(t *testing.T) {
	schema := NewBaseSchema("orders").
		AddField(NewField("tenant_id", TypeInteger).Build()).
		AddField(NewField("order_no", TypeString).Build()).
		AddPrimaryKey("tenant_id", "order_no").
		AddUniqueConstraint("uk_orders_tenant_ref", "tenant_id", "order_no")

	constraints := schema.Constraints()
	if len(constraints) != 2 {
		t.Fatalf("expected 2 constraints, got %d", len(constraints))
	}

	if constraints[0].Kind != ConstraintPrimaryKey {
		t.Fatalf("expected first constraint to be primary key, got %s", constraints[0].Kind)
	}

	if constraints[1].Kind != ConstraintUnique {
		t.Fatalf("expected second constraint to be unique, got %s", constraints[1].Kind)
	}
}

func TestBuildCreateTableSQLWithCompositeConstraints(t *testing.T) {
	repo := &Repository{adapter: &PostgreSQLAdapter{}}
	schema := NewBaseSchema("public.order_items").
		AddField(NewField("tenant_id", TypeInteger).Null(false).Build()).
		AddField(NewField("order_no", TypeString).Null(false).Build()).
		AddField(NewField("line_no", TypeInteger).Null(false).Build()).
		AddUniqueConstraint("uk_order_line", "tenant_id", "order_no", "line_no").
		AddPrimaryKey("tenant_id", "order_no")

	sql := buildCreateTableSQL(repo, schema)

	if !strings.Contains(sql, `CREATE TABLE IF NOT EXISTS "public"."order_items"`) {
		t.Fatalf("expected quoted schema.table in SQL, got: %s", sql)
	}

	if !strings.Contains(sql, `PRIMARY KEY ("tenant_id", "order_no")`) {
		t.Fatalf("expected composite primary key clause, got: %s", sql)
	}

	if !strings.Contains(sql, `CONSTRAINT "uk_order_line" UNIQUE ("tenant_id", "order_no", "line_no")`) {
		t.Fatalf("expected named composite unique constraint, got: %s", sql)
	}
}

func TestBuildCreateTableSQLFallbackWhenCompositeNotSupported(t *testing.T) {
	adapter := &mockCompositeFallbackAdapter{
		features: &DatabaseFeatures{
			SupportsCompositeKeys:    false,
			SupportsCompositeIndexes: false,
		},
		provider: NewDefaultSQLQueryConstructorProvider(NewMySQLDialect()),
	}

	repo := &Repository{adapter: adapter}
	schema := NewBaseSchema("orders").
		AddField(NewField("tenant_id", TypeInteger).Null(false).Build()).
		AddField(NewField("order_no", TypeString).Null(false).Build()).
		AddPrimaryKey("tenant_id", "order_no").
		AddUniqueConstraint("uk_tenant_order", "tenant_id", "order_no")

	sql := buildCreateTableSQL(repo, schema)

	if !strings.Contains(sql, "`tenant_id`") {
		t.Fatalf("expected tenant_id to be present in SQL, got: %s", sql)
	}

	if !strings.Contains(sql, "`tenant_id` TEXT NOT NULL PRIMARY KEY") {
		t.Fatalf("expected fallback to first-column primary key, got: %s", sql)
	}

	if strings.Contains(sql, "UNIQUE (`tenant_id`, `order_no`)") {
		t.Fatalf("expected composite unique to be skipped when unsupported, got: %s", sql)
	}
}
