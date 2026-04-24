package db

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type mockStartupCapabilityAdapter struct {
	jsonRuntime    *JSONRuntimeCapability
	jsonRuntimeErr error
	fullText       *FullTextRuntimeCapability
	fullTextErr    error
}

type metadataStartupCapabilityAdapter struct {
	mockStartupCapabilityAdapter
}

func (m *metadataStartupCapabilityAdapter) Metadata() AdapterMetadata {
	return AdapterMetadata{Name: "meta_startup_db", DriverKind: "document", Vendor: "custom"}
}

func (m *mockStartupCapabilityAdapter) Connect(ctx context.Context, config *Config) error { return nil }
func (m *mockStartupCapabilityAdapter) Close() error                                      { return nil }
func (m *mockStartupCapabilityAdapter) Ping(ctx context.Context) error                    { return nil }
func (m *mockStartupCapabilityAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, errors.New("not implemented")
}
func (m *mockStartupCapabilityAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("not implemented")
}
func (m *mockStartupCapabilityAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}
func (m *mockStartupCapabilityAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return nil, errors.New("not implemented")
}
func (m *mockStartupCapabilityAdapter) GetRawConn() interface{} { return nil }
func (m *mockStartupCapabilityAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return errors.New("not implemented")
}
func (m *mockStartupCapabilityAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return errors.New("not implemented")
}
func (m *mockStartupCapabilityAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, errors.New("not implemented")
}
func (m *mockStartupCapabilityAdapter) GetQueryBuilderProvider() QueryConstructorProvider { return nil }
func (m *mockStartupCapabilityAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{}
}
func (m *mockStartupCapabilityAdapter) GetQueryFeatures() *QueryFeatures { return &QueryFeatures{} }
func (m *mockStartupCapabilityAdapter) InspectJSONRuntime(ctx context.Context) (*JSONRuntimeCapability, error) {
	if m.jsonRuntimeErr != nil {
		return nil, m.jsonRuntimeErr
	}
	return m.jsonRuntime, nil
}
func (m *mockStartupCapabilityAdapter) InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error) {
	if m.fullTextErr != nil {
		return nil, m.fullTextErr
	}
	return m.fullText, nil
}

func TestRunStartupCapabilityCheckLenient(t *testing.T) {
	repo := &Repository{adapter: &mockStartupCapabilityAdapter{}}
	report, err := repo.RunStartupCapabilityCheck(context.Background(), &StartupCapabilityConfig{
		Mode:     "lenient",
		Inspect:  []string{StartupCapabilityJSONRuntime},
		Required: []string{StartupCapabilityJSONRuntime},
	})
	if err != nil {
		t.Fatalf("expected lenient mode not to fail, got error: %v", err)
	}
	if report == nil || len(report.Checks) != 1 {
		t.Fatalf("expected one check in report, got %+v", report)
	}
	if report.Checks[0].Status != "degraded" {
		t.Fatalf("expected degraded status, got %s", report.Checks[0].Status)
	}
}

func TestRunStartupCapabilityCheckStrictFails(t *testing.T) {
	repo := &Repository{adapter: &mockStartupCapabilityAdapter{}}
	_, err := repo.RunStartupCapabilityCheck(context.Background(), &StartupCapabilityConfig{
		Mode:     "strict",
		Inspect:  []string{StartupCapabilityJSONRuntime},
		Required: []string{StartupCapabilityJSONRuntime},
	})
	if err == nil {
		t.Fatal("expected strict mode failure when required capability unsupported")
	}
	if !strings.Contains(err.Error(), "strict startup capability check failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunStartupCapabilityCheckStrictPasses(t *testing.T) {
	repo := &Repository{adapter: &mockStartupCapabilityAdapter{
		jsonRuntime: &JSONRuntimeCapability{NativeSupported: true, Notes: "ok"},
	}}

	report, err := repo.RunStartupCapabilityCheck(context.Background(), &StartupCapabilityConfig{
		Mode:     "strict",
		Inspect:  []string{StartupCapabilityJSONRuntime},
		Required: []string{StartupCapabilityJSONRuntime},
	})
	if err != nil {
		t.Fatalf("expected strict mode pass, got error: %v", err)
	}
	if report.Checks[0].Status != "ok" {
		t.Fatalf("expected ok status, got %s", report.Checks[0].Status)
	}
}

func TestRunStartupCapabilityCheckReportUsesMetadataName(t *testing.T) {
	repo := &Repository{adapter: &metadataStartupCapabilityAdapter{}}
	report, err := repo.RunStartupCapabilityCheck(context.Background(), &StartupCapabilityConfig{
		Mode:    "lenient",
		Inspect: []string{StartupCapabilityJSONRuntime},
	})
	if err != nil {
		t.Fatalf("expected capability check success, got error: %v", err)
	}
	if report == nil {
		t.Fatal("expected non-nil startup capability report")
	}
	if report.Adapter != "meta_startup_db" {
		t.Fatalf("expected adapter name meta_startup_db, got %q", report.Adapter)
	}
}

func TestStartupCapabilityConfigValidation(t *testing.T) {
	cfg := &Config{
		Adapter:  "sqlite",
		Database: ":memory:",
		StartupCapabilities: &StartupCapabilityConfig{
			Mode:    "invalid",
			Inspect: []string{StartupCapabilityJSONRuntime},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid mode validation error")
	}
}

func TestStartupCapabilityConfigValidationInvalidCapabilityMode(t *testing.T) {
	cfg := &Config{
		Adapter:  "sqlite",
		Database: ":memory:",
		StartupCapabilities: &StartupCapabilityConfig{
			Mode: "lenient",
			CapabilityModes: map[string]string{
				StartupCapabilityJSONRuntime: "hard",
			},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid capability mode validation error")
	}
}

func TestRunStartupCapabilityCheckPerCapabilityStrictInLenientModeFails(t *testing.T) {
	repo := &Repository{adapter: &mockStartupCapabilityAdapter{}}
	_, err := repo.RunStartupCapabilityCheck(context.Background(), &StartupCapabilityConfig{
		Mode: "lenient",
		CapabilityModes: map[string]string{
			StartupCapabilityJSONRuntime: startupCapabilityModeStrict,
		},
		Inspect: []string{StartupCapabilityJSONRuntime},
	})
	if err == nil {
		t.Fatal("expected per-capability strict override to fail in lenient mode")
	}
}

func TestRunStartupCapabilityCheckPerCapabilityMixedPolicy(t *testing.T) {
	repo := &Repository{adapter: &mockStartupCapabilityAdapter{
		jsonRuntime: &JSONRuntimeCapability{NativeSupported: true, Notes: "ok"},
		fullText: &FullTextRuntimeCapability{
			NativeSupported: false,
			PluginAvailable: false,
			Notes:           "degraded",
		},
	}}

	report, err := repo.RunStartupCapabilityCheck(context.Background(), &StartupCapabilityConfig{
		Mode: "lenient",
		CapabilityModes: map[string]string{
			StartupCapabilityJSONRuntime:     startupCapabilityModeStrict,
			StartupCapabilityFullTextRuntime: startupCapabilityModeLenient,
		},
		Inspect: []string{StartupCapabilityJSONRuntime, StartupCapabilityFullTextRuntime},
	})
	if err != nil {
		t.Fatalf("expected mixed policy to pass, got: %v", err)
	}
	if report == nil || len(report.Checks) != 2 {
		t.Fatalf("expected 2 checks, got %+v", report)
	}

	for _, check := range report.Checks {
		switch check.Name {
		case StartupCapabilityJSONRuntime:
			if check.EffectiveMode != startupCapabilityModeStrict {
				t.Fatalf("expected json_runtime effective mode strict, got %s", check.EffectiveMode)
			}
			if check.Status != "ok" {
				t.Fatalf("expected json_runtime status ok, got %s", check.Status)
			}
		case StartupCapabilityFullTextRuntime:
			if check.EffectiveMode != startupCapabilityModeLenient {
				t.Fatalf("expected full_text_runtime effective mode lenient, got %s", check.EffectiveMode)
			}
			if check.Status != "degraded" {
				t.Fatalf("expected full_text_runtime degraded in lenient mode, got %s", check.Status)
			}
		}
	}
}

func TestNewRepositoryStrictStartupCapabilityFailsForSQLiteJSON(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "eit-db-startup-capability")
	_ = os.MkdirAll(tmpDir, 0o755)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "strict.db")
	cfg := &Config{
		Adapter:  "sqlite",
		Database: dbPath,
		StartupCapabilities: &StartupCapabilityConfig{
			Mode:     "strict",
			Inspect:  []string{StartupCapabilityJSONRuntime},
			Required: []string{StartupCapabilityJSONRuntime},
		},
	}

	repo, err := NewRepository(cfg)
	if err == nil {
		defer repo.Close()
		t.Fatal("expected strict startup capability check to fail for sqlite json_runtime")
	}
	if !strings.Contains(err.Error(), "startup capability check failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewRepositoryLenientStartupCapabilityPassesForSQLiteJSON(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "eit-db-startup-capability-lenient")
	_ = os.MkdirAll(tmpDir, 0o755)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "lenient.db")
	cfg := &Config{
		Adapter:  "sqlite",
		Database: dbPath,
		StartupCapabilities: &StartupCapabilityConfig{
			Mode:     "lenient",
			Inspect:  []string{StartupCapabilityJSONRuntime},
			Required: []string{StartupCapabilityJSONRuntime},
		},
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("expected lenient startup capability check to pass, got: %v", err)
	}
	defer repo.Close()

	report := repo.GetStartupCapabilityReport()
	if report == nil || len(report.Checks) == 0 {
		t.Fatalf("expected startup capability report, got %+v", report)
	}
	if report.Mode != "lenient" {
		t.Fatalf("expected lenient report mode, got %s", report.Mode)
	}
}
