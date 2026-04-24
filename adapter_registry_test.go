package db

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
)

// ==================== 自定义适配器桩，用于测试注册流程 ====================

type stubCustomAdapter struct{ name string }

type stubLegacyFactory struct{ name string }

func (a *stubCustomAdapter) Connect(_ context.Context, _ *Config) error  { return nil }
func (a *stubCustomAdapter) Close() error                                  { return nil }
func (a *stubCustomAdapter) Ping(_ context.Context) error                  { return nil }
func (a *stubCustomAdapter) Begin(_ context.Context, _ ...interface{}) (Tx, error) {
	return nil, fmt.Errorf("%s: transactions not supported", a.name)
}
func (a *stubCustomAdapter) Query(_ context.Context, _ string, _ ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("%s: SQL not supported", a.name)
}
func (a *stubCustomAdapter) QueryRow(_ context.Context, _ string, _ ...interface{}) *sql.Row {
	return nil
}
func (a *stubCustomAdapter) Exec(_ context.Context, _ string, _ ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("%s: SQL not supported", a.name)
}
func (a *stubCustomAdapter) GetRawConn() interface{}                          { return nil }
func (a *stubCustomAdapter) RegisterScheduledTask(_ context.Context, _ *ScheduledTaskConfig) error {
	return NewScheduledTaskFallbackErrorWithReason(a.name, ScheduledTaskFallbackReasonAdapterUnsupported, "not supported")
}
func (a *stubCustomAdapter) UnregisterScheduledTask(_ context.Context, _ string) error {
	return NewScheduledTaskFallbackErrorWithReason(a.name, ScheduledTaskFallbackReasonAdapterUnsupported, "not supported")
}
func (a *stubCustomAdapter) ListScheduledTasks(_ context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, NewScheduledTaskFallbackErrorWithReason(a.name, ScheduledTaskFallbackReasonAdapterUnsupported, "not supported")
}
func (a *stubCustomAdapter) GetQueryBuilderProvider() QueryConstructorProvider { return nil }
func (a *stubCustomAdapter) GetDatabaseFeatures() *DatabaseFeatures            { return &DatabaseFeatures{} }
func (a *stubCustomAdapter) GetQueryFeatures() *QueryFeatures                  { return &QueryFeatures{} }

func (f *stubLegacyFactory) Name() string { return f.name }

func (f *stubLegacyFactory) Create(_ *Config) (Adapter, error) {
	return &stubCustomAdapter{name: f.name}, nil
}

// ==================== 测试 ====================

func TestRegisterAdapterDescriptor_Success(t *testing.T) {
	name := "testdb_reg"
	err := RegisterAdapterDescriptor(name, AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) {
			return &stubCustomAdapter{name: name}, nil
		},
		ValidateConfig: func(cfg *Config) error {
			ep, _ := cfg.Options["endpoint"].(string)
			if ep == "" {
				return fmt.Errorf("%s: options.endpoint is required", name)
			}
			return nil
		},
		DefaultConfig: func() *Config {
			return &Config{
				Adapter: name,
				Options: map[string]interface{}{"endpoint": "localhost:9000"},
			}
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRegisterAdapterDescriptor_NilFactory(t *testing.T) {
	err := RegisterAdapterDescriptor("baddb", AdapterDescriptor{Factory: nil})
	if err == nil {
		t.Fatal("expected error for nil Factory, got nil")
	}
}

func TestRegisterAdapterDescriptor_EmptyName(t *testing.T) {
	err := RegisterAdapterDescriptor("", AdapterDescriptor{
		Factory: func(_ *Config) (Adapter, error) { return nil, nil },
	})
	if err == nil {
		t.Fatal("expected error for empty name, got nil")
	}
}

func TestLookupAdapterDescriptor_Found(t *testing.T) {
	name := "testdb_lookup"
	_ = RegisterAdapterDescriptor(name, AdapterDescriptor{
		Factory: func(_ *Config) (Adapter, error) { return &stubCustomAdapter{name: name}, nil },
	})

	desc, ok := LookupAdapterDescriptor(name)
	if !ok {
		t.Fatal("expected descriptor to be found")
	}
	if desc.Factory == nil {
		t.Fatal("expected Factory to be non-nil")
	}
}

func TestLookupAdapterDescriptor_NotFound(t *testing.T) {
	_, ok := LookupAdapterDescriptor("nonexistent_db_xyz")
	if ok {
		t.Fatal("expected descriptor not found")
	}
}

func TestListRegisteredAdapters_IsSorted(t *testing.T) {
	_ = RegisterAdapterDescriptor("zzz_sort_test", AdapterDescriptor{
		Factory: func(_ *Config) (Adapter, error) { return &stubCustomAdapter{name: "zzz_sort_test"}, nil },
	})
	_ = RegisterAdapterDescriptor("aaa_sort_test", AdapterDescriptor{
		Factory: func(_ *Config) (Adapter, error) { return &stubCustomAdapter{name: "aaa_sort_test"}, nil },
	})

	names := ListRegisteredAdapters()
	if !slices.IsSorted(names) {
		t.Fatalf("expected registered adapter names to be sorted, got %v", names)
	}
}

func TestValidate_CustomAdapter_UsesDescriptorValidateConfig(t *testing.T) {
	name := "testdb_validate"
	_ = RegisterAdapterDescriptor(name, AdapterDescriptor{
		Factory: func(_ *Config) (Adapter, error) { return &stubCustomAdapter{name: name}, nil },
		ValidateConfig: func(cfg *Config) error {
			ep, _ := cfg.Options["endpoint"].(string)
			if ep == "" {
				return fmt.Errorf("%s: endpoint required", name)
			}
			return nil
		},
	})

	// 缺少 endpoint -> 应失败
	cfg := &Config{Adapter: name}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for missing endpoint")
	}

	// 提供 endpoint -> 应通过
	cfg.Options = map[string]interface{}{"endpoint": "localhost:9000"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected validation to pass: %v", err)
	}
}

func TestValidate_CustomAdapter_NoValidateConfig_Passes(t *testing.T) {
	name := "testdb_novalidate"
	_ = RegisterAdapterDescriptor(name, AdapterDescriptor{
		Factory: func(_ *Config) (Adapter, error) { return &stubCustomAdapter{name: name}, nil },
		// ValidateConfig 未设置
	})

	cfg := &Config{Adapter: name}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected validation to pass without ValidateConfig: %v", err)
	}
}

func TestValidate_UnknownAdapter_ReturnsError(t *testing.T) {
	cfg := &Config{Adapter: "completely_unknown_xyz_456"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for unknown adapter")
	}
}

func TestDefaultConfig_CustomAdapter_UsesDescriptorDefaultConfig(t *testing.T) {
	name := "testdb_default"
	_ = RegisterAdapterDescriptor(name, AdapterDescriptor{
		Factory: func(_ *Config) (Adapter, error) { return &stubCustomAdapter{name: name}, nil },
		DefaultConfig: func() *Config {
			return &Config{
				Adapter: name,
				Options: map[string]interface{}{"endpoint": "default-host:1234"},
			}
		},
	})

	cfg := DefaultConfig(name)
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.Adapter != name {
		t.Errorf("expected adapter %q, got %q", name, cfg.Adapter)
	}
	ep, _ := cfg.Options["endpoint"].(string)
	if ep != "default-host:1234" {
		t.Errorf("expected endpoint from DefaultConfig, got %q", ep)
	}
}

func TestDefaultConfig_CustomAdapter_NoDefaultConfig_ReturnsBasic(t *testing.T) {
	name := "testdb_nodefault"
	_ = RegisterAdapterDescriptor(name, AdapterDescriptor{
		Factory: func(_ *Config) (Adapter, error) { return &stubCustomAdapter{name: name}, nil },
		// DefaultConfig 未设置
	})

	cfg := DefaultConfig(name)
	// 当 DefaultConfig 为 nil 时，DefaultConfig() 返回带通用连接池的最简配置
	if cfg.Adapter != name {
		t.Errorf("expected adapter %q, got %q", name, cfg.Adapter)
	}
}

func TestNewConfig_CustomAdapter_WithConfigFunc(t *testing.T) {
	name := "testdb_configfunc"
	_ = RegisterAdapterDescriptor(name, AdapterDescriptor{
		Factory: func(_ *Config) (Adapter, error) { return &stubCustomAdapter{name: name}, nil },
		ValidateConfig: func(cfg *Config) error {
			ep, _ := cfg.Options["endpoint"].(string)
			if ep == "" {
				return fmt.Errorf("%s: endpoint required", name)
			}
			return nil
		},
	})

	cfg, err := NewConfig(name,
		WithConfigFunc(func(cfg *Config) {
			if cfg.Options == nil {
				cfg.Options = make(map[string]interface{})
			}
			cfg.Options["endpoint"] = "myserver:9000"
		}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ep, _ := cfg.Options["endpoint"].(string)
	if ep != "myserver:9000" {
		t.Errorf("expected endpoint myserver:9000, got %q", ep)
	}
}

func TestMustRegisterAdapterDescriptor_Panics_OnNilFactory(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil Factory")
		}
	}()
	MustRegisterAdapterDescriptor("panicdb", AdapterDescriptor{Factory: nil})
}

func TestRegisterAdapter_LegacyCompatibilityRegistersDescriptorShim(t *testing.T) {
	name := "legacy_factory_db"
	RegisterAdapter(&stubLegacyFactory{name: name})

	desc, ok := LookupAdapterDescriptor(name)
	if !ok {
		t.Fatal("expected legacy RegisterAdapter to register a descriptor shim")
	}
	if desc.Factory == nil {
		t.Fatal("expected legacy descriptor shim to expose Factory")
	}

	cfg := &Config{Adapter: name}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected legacy shim adapter to pass validation with minimal config: %v", err)
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("expected legacy shim adapter to remain creatable via NewRepository: %v", err)
	}
	if repo == nil {
		t.Fatal("expected non-nil repository")
	}
}

func TestRegisterAdapterConstructor_LegacyCompatibilityRegistersDescriptorShim(t *testing.T) {
	name := "legacy_ctor_db"
	err := RegisterAdapterConstructor(name, func(_ *Config) (Adapter, error) {
		return &stubCustomAdapter{name: name}, nil
	})
	if err != nil {
		t.Fatalf("unexpected constructor registration error: %v", err)
	}

	desc, ok := LookupAdapterDescriptor(name)
	if !ok {
		t.Fatal("expected legacy constructor registration to create a descriptor shim")
	}
	if desc.Factory == nil {
		t.Fatal("expected legacy constructor shim to expose Factory")
	}

	cfg := DefaultConfig(name)
	if cfg.Adapter != name {
		t.Fatalf("expected minimal default config for legacy adapter %q, got %q", name, cfg.Adapter)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected legacy constructor shim config to validate: %v", err)
	}
}

func TestBuiltinDescriptors_RegisteredViaInit(t *testing.T) {
	builtinAdapters := []string{"sqlite", "postgres", "mysql", "sqlserver", "mongodb", "neo4j", "redis"}
	for _, name := range builtinAdapters {
		desc, ok := LookupAdapterDescriptor(name)
		if !ok {
			t.Fatalf("expected builtin adapter %q to be registered via descriptor", name)
		}
		if desc.Factory == nil {
			t.Fatalf("expected builtin adapter %q descriptor to expose Factory", name)
		}
		if desc.Metadata == nil {
			t.Fatalf("expected builtin adapter %q descriptor to expose Metadata", name)
		}
		meta := desc.Metadata()
		if meta.Name != name {
			t.Fatalf("expected builtin adapter metadata name %q, got %q", name, meta.Name)
		}
	}
}

// TestRedisDescriptor_RegisteredViaInit 验证 redis 已通过 init() 中的描述符注册
func TestRedisDescriptor_RegisteredViaInit(t *testing.T) {
	desc, ok := LookupAdapterDescriptor("redis")
	if !ok {
		t.Fatal("redis descriptor should be registered via init()")
	}
	if desc.Factory == nil {
		t.Fatal("redis descriptor Factory should not be nil")
	}
	if desc.ValidateConfig == nil {
		t.Fatal("redis descriptor ValidateConfig should not be nil")
	}
	if desc.DefaultConfig == nil {
		t.Fatal("redis descriptor DefaultConfig should not be nil")
	}
}

// TestRedisDescriptor_DefaultConfig 验证 redis 通过描述符返回默认配置
func TestRedisDescriptor_DefaultConfig(t *testing.T) {
	cfg := DefaultConfig("redis")
	if cfg.Adapter != "redis" {
		t.Errorf("expected adapter redis, got %q", cfg.Adapter)
	}
	if cfg.Redis == nil {
		t.Fatal("expected redis sub-config to be set")
	}
	if cfg.Redis.Host != "localhost" || cfg.Redis.Port != 6379 {
		t.Errorf("unexpected redis defaults: host=%q port=%d", cfg.Redis.Host, cfg.Redis.Port)
	}
}

// TestRedisDescriptor_ValidateConfig_SingleMode 验证单机模式校验
func TestRedisDescriptor_ValidateConfig_SingleMode(t *testing.T) {
	// 默认 host/port 应通过
	cfg := &Config{Adapter: "redis"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected redis default config to validate: %v", err)
	}

	// 集群模式无 addrs -> 应失败
	cfg2 := &Config{
		Adapter: "redis",
		Redis:   &RedisConnectionConfig{ClusterMode: true},
	}
	if err := cfg2.Validate(); err == nil {
		t.Fatal("expected error for cluster mode with no addrs")
	}
}
