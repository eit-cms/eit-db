package db

import (
	"context"
	"os"
	"testing"
)

func TestMongoConfigValidationRequiresURI(t *testing.T) {
	cfg := &Config{
		Adapter:  "mongodb",
		Database: "test_db",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for missing options.uri")
	}
}

func TestMongoAdapterFactory(t *testing.T) {
	cfg := &Config{
		Adapter:  "mongodb",
		Database: "test_db",
		Options: map[string]interface{}{
			"uri": "mongodb://localhost:27017",
		},
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	if repo == nil {
		t.Fatalf("expected repo, got nil")
	}
}

func TestMongoAdapterPing(t *testing.T) {
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		t.Skip("MONGODB_URI not set; skipping integration test")
	}

	cfg := &Config{
		Adapter:  "mongodb",
		Database: "test_db",
		Options: map[string]interface{}{
			"uri": uri,
		},
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	if err := repo.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if err := repo.Ping(context.Background()); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}
