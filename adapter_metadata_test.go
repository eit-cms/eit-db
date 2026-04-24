package db

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

type metadataProviderAdapter struct{}

func (m *metadataProviderAdapter) Connect(ctx context.Context, config *Config) error { return nil }
func (m *metadataProviderAdapter) Close() error                                      { return nil }
func (m *metadataProviderAdapter) Ping(ctx context.Context) error                    { return nil }
func (m *metadataProviderAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, errors.New("not implemented")
}
func (m *metadataProviderAdapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("not implemented")
}
func (m *metadataProviderAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return nil
}
func (m *metadataProviderAdapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return nil, errors.New("not implemented")
}
func (m *metadataProviderAdapter) GetRawConn() interface{} { return nil }
func (m *metadataProviderAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return errors.New("not implemented")
}
func (m *metadataProviderAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return errors.New("not implemented")
}
func (m *metadataProviderAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, errors.New("not implemented")
}
func (m *metadataProviderAdapter) GetQueryBuilderProvider() QueryConstructorProvider { return nil }
func (m *metadataProviderAdapter) GetDatabaseFeatures() *DatabaseFeatures            { return &DatabaseFeatures{} }
func (m *metadataProviderAdapter) GetQueryFeatures() *QueryFeatures                  { return &QueryFeatures{} }
func (m *metadataProviderAdapter) Metadata() AdapterMetadata {
	return AdapterMetadata{Name: "provider_db", DriverKind: "document", Vendor: "custom"}
}

type indexedTypeAdapter struct{}

func (m *indexedTypeAdapter) Connect(ctx context.Context, config *Config) error { return nil }
func (m *indexedTypeAdapter) Close() error                                      { return nil }
func (m *indexedTypeAdapter) Ping(ctx context.Context) error                    { return nil }
func (m *indexedTypeAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, errors.New("not implemented")
}
func (m *indexedTypeAdapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("not implemented")
}
func (m *indexedTypeAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return nil
}
func (m *indexedTypeAdapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return nil, errors.New("not implemented")
}
func (m *indexedTypeAdapter) GetRawConn() interface{} { return nil }
func (m *indexedTypeAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return errors.New("not implemented")
}
func (m *indexedTypeAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return errors.New("not implemented")
}
func (m *indexedTypeAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, errors.New("not implemented")
}
func (m *indexedTypeAdapter) GetQueryBuilderProvider() QueryConstructorProvider { return nil }
func (m *indexedTypeAdapter) GetDatabaseFeatures() *DatabaseFeatures            { return &DatabaseFeatures{} }
func (m *indexedTypeAdapter) GetQueryFeatures() *QueryFeatures                  { return &QueryFeatures{} }

func TestResolveAdapterMetadata_DescriptorMetadata(t *testing.T) {
	adapterName := "meta_desc_db"
	_ = RegisterAdapterDescriptor(adapterName, AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) { return &metadataProviderAdapter{}, nil },
		Metadata: func() AdapterMetadata {
			return AdapterMetadata{Name: adapterName, DriverKind: "graph", Vendor: "acme", Aliases: []string{"meta_desc"}}
		},
	})

	meta := resolveAdapterMetadata(adapterName, nil)
	if meta.Name != adapterName {
		t.Fatalf("expected adapter name %q, got %q", adapterName, meta.Name)
	}
	if meta.DriverKind != "graph" {
		t.Fatalf("expected driver kind graph, got %q", meta.DriverKind)
	}
}

func TestResolveAdapterMetadata_ProviderFallback(t *testing.T) {
	meta := resolveAdapterMetadata("", &metadataProviderAdapter{})
	if meta.Name != "provider_db" {
		t.Fatalf("expected provider metadata name provider_db, got %q", meta.Name)
	}
}

func TestResolveAdapterMetadata_PublicAPI(t *testing.T) {
	meta := ResolveAdapterMetadata("", &metadataProviderAdapter{})
	if meta.Name != "provider_db" {
		t.Fatalf("expected provider metadata name provider_db, got %q", meta.Name)
	}
}

func TestResolveAdapterMetadata_ConcreteTypeIndexFallback(t *testing.T) {
	rememberAdapterConcreteType(&indexedTypeAdapter{}, "indexed_db")
	meta := resolveAdapterMetadata("", &indexedTypeAdapter{})
	if meta.Name != "indexed_db" {
		t.Fatalf("expected indexed metadata name indexed_db, got %q", meta.Name)
	}
}

func TestRepositoryGetAdapterMetadata(t *testing.T) {
	repo := &Repository{adapterType: "", adapter: &metadataProviderAdapter{}}
	meta := repo.GetAdapterMetadata()
	if meta.Name != "provider_db" {
		t.Fatalf("expected provider metadata name provider_db, got %q", meta.Name)
	}
}
