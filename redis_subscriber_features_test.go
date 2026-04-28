package db

import (
	"context"
	"database/sql"
	"testing"
)

type nonRedisAdapterForSubscriberTest struct{}

func (a *nonRedisAdapterForSubscriberTest) Connect(ctx context.Context, config *Config) error {
	return nil
}
func (a *nonRedisAdapterForSubscriberTest) Close() error { return nil }
func (a *nonRedisAdapterForSubscriberTest) Ping(ctx context.Context) error {
	return nil
}
func (a *nonRedisAdapterForSubscriberTest) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, nil
}
func (a *nonRedisAdapterForSubscriberTest) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}
func (a *nonRedisAdapterForSubscriberTest) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}
func (a *nonRedisAdapterForSubscriberTest) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}
func (a *nonRedisAdapterForSubscriberTest) GetRawConn() interface{} { return nil }
func (a *nonRedisAdapterForSubscriberTest) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return nil
}
func (a *nonRedisAdapterForSubscriberTest) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return nil
}
func (a *nonRedisAdapterForSubscriberTest) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, nil
}
func (a *nonRedisAdapterForSubscriberTest) GetQueryBuilderProvider() QueryConstructorProvider {
	return nil
}
func (a *nonRedisAdapterForSubscriberTest) GetDatabaseFeatures() *DatabaseFeatures { return nil }
func (a *nonRedisAdapterForSubscriberTest) GetQueryFeatures() *QueryFeatures       { return nil }

func TestGetRedisSubscriberFeatures(t *testing.T) {
	t.Run("non redis adapter", func(t *testing.T) {
		feat, ok := GetRedisSubscriberFeatures(&nonRedisAdapterForSubscriberTest{})
		if ok || feat != nil {
			t.Fatalf("expected (nil, false), got (%v, %v)", feat, ok)
		}
	})

	t.Run("redis adapter", func(t *testing.T) {
		feat, ok := GetRedisSubscriberFeatures(&RedisAdapter{})
		if !ok || feat == nil {
			t.Fatalf("expected redis subscriber feature view")
		}
	})
}

func TestRepositoryGetRedisSubscriberFeatures(t *testing.T) {
	repo := &Repository{adapter: &RedisAdapter{}}
	feat, ok := repo.GetRedisSubscriberFeatures()
	if !ok || feat == nil {
		t.Fatalf("expected repository to expose redis subscriber features")
	}

	nonRedisRepo := &Repository{adapter: &nonRedisAdapterForSubscriberTest{}}
	feat, ok = nonRedisRepo.GetRedisSubscriberFeatures()
	if ok || feat != nil {
		t.Fatalf("expected non redis repository to return (nil, false)")
	}
}
