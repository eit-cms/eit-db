package db

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

type policyMockAdapter struct {
	provider QueryConstructorProvider
	execCnt  int
}

func newPolicyMockAdapter(dialect SQLDialect) *policyMockAdapter {
	return &policyMockAdapter{provider: NewDefaultSQLQueryConstructorProvider(dialect)}
}

func (a *policyMockAdapter) Connect(ctx context.Context, config *Config) error { return nil }
func (a *policyMockAdapter) Close() error                                      { return nil }
func (a *policyMockAdapter) Ping(ctx context.Context) error                    { return nil }
func (a *policyMockAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, nil
}
func (a *policyMockAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}
func (a *policyMockAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}
func (a *policyMockAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	a.execCnt++
	return nil, nil
}
func (a *policyMockAdapter) GetRawConn() interface{} { return nil }
func (a *policyMockAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return nil
}
func (a *policyMockAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return nil
}
func (a *policyMockAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, nil
}
func (a *policyMockAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return a.provider
}
func (a *policyMockAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{}
}
func (a *policyMockAdapter) GetQueryFeatures() *QueryFeatures {
	return NewMySQLQueryFeatures()
}

func TestRawSQLMigration_RequiresForAdapter(t *testing.T) {
	repo := &Repository{adapter: newPolicyMockAdapter(NewPostgreSQLDialect())}
	migration := NewRawSQLMigration("20260319170000", "raw policy test").
		AddUpSQL("SELECT 1")

	err := migration.Up(context.Background(), repo)
	if err == nil || !strings.Contains(err.Error(), "must call ForAdapter") {
		t.Fatalf("expected ForAdapter required error, got: %v", err)
	}
}

func TestRawSQLMigration_AdapterMustMatchCurrentRepo(t *testing.T) {
	mock := newPolicyMockAdapter(NewPostgreSQLDialect())
	repo := &Repository{adapter: mock}

	migration := NewRawSQLMigration("20260319170001", "raw policy match test").
		ForAdapter("mysql").
		AddUpSQL("SELECT 1")

	err := migration.Up(context.Background(), repo)
	if err == nil || !strings.Contains(err.Error(), "targets adapter") {
		t.Fatalf("expected adapter mismatch error, got: %v", err)
	}

	migration.ForAdapter("postgres")
	err = migration.Up(context.Background(), repo)
	if err != nil {
		t.Fatalf("expected adapter matched execution success, got: %v", err)
	}
	if mock.execCnt != 1 {
		t.Fatalf("expected exactly one raw SQL execution, got %d", mock.execCnt)
	}
}

func TestMigratorV1EntryDisabled(t *testing.T) {
	m := NewMigrator(nil)

	if err := m.Register(&Migration{Version: "1", Description: "x", UpSQL: []string{"SELECT 1"}}); err == nil || !strings.Contains(err.Error(), "v1 Migrator is disabled") {
		t.Fatalf("expected v1 disabled error from Register, got: %v", err)
	}
	if err := m.Up(context.Background()); err == nil || !strings.Contains(err.Error(), "v1 Migrator is disabled") {
		t.Fatalf("expected v1 disabled error from Up, got: %v", err)
	}
	if err := m.Down(context.Background()); err == nil || !strings.Contains(err.Error(), "v1 Migrator is disabled") {
		t.Fatalf("expected v1 disabled error from Down, got: %v", err)
	}
	if _, err := m.Status(context.Background()); err == nil || !strings.Contains(err.Error(), "v1 Migrator is disabled") {
		t.Fatalf("expected v1 disabled error from Status, got: %v", err)
	}
}
