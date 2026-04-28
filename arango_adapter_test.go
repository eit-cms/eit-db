package db

import "testing"

func TestArangoDescriptor_RegisteredViaInit(t *testing.T) {
	desc, ok := LookupAdapterDescriptor("arango")
	if !ok {
		t.Fatal("arango descriptor should be registered via init()")
	}
	if desc.Factory == nil {
		t.Fatal("arango descriptor Factory should not be nil")
	}
	if desc.ValidateConfig == nil {
		t.Fatal("arango descriptor ValidateConfig should not be nil")
	}
	if desc.DefaultConfig == nil {
		t.Fatal("arango descriptor DefaultConfig should not be nil")
	}
}

func TestArangoDescriptor_DefaultConfig(t *testing.T) {
	cfg := DefaultConfig("arango")
	if cfg.Adapter != "arango" {
		t.Fatalf("expected adapter arango, got %q", cfg.Adapter)
	}
	if cfg.Arango == nil {
		t.Fatal("expected arango sub-config")
	}
	if cfg.Arango.URI == "" || cfg.Arango.Database == "" {
		t.Fatalf("expected arango defaults to contain uri/database, got uri=%q db=%q", cfg.Arango.URI, cfg.Arango.Database)
	}
}

func TestArangoDescriptor_ValidateConfig(t *testing.T) {
	cfg := &Config{Adapter: "arango", Arango: &ArangoConnectionConfig{URI: "http://localhost:8529", Database: "_system"}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid arango config, got %v", err)
	}

	cfgInvalid := &Config{Adapter: "arango", Arango: &ArangoConnectionConfig{URI: "://bad", Database: "_system"}}
	if err := cfgInvalid.Validate(); err == nil {
		t.Fatal("expected invalid uri validation error")
	}
}
