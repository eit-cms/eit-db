package db

import (
	"context"
	"strings"
	"testing"
)

func TestDecideFeatureExecutionNative(t *testing.T) {
	repo := &Repository{adapter: &MySQLAdapter{}}

	decision, err := repo.DecideFeatureExecution("window_functions", "8.0.36")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decision.Mode != FeatureExecutionNative {
		t.Fatalf("expected native mode, got %s", decision.Mode)
	}
}

func TestDecideFeatureExecutionFallback(t *testing.T) {
	repo := &Repository{adapter: &MySQLAdapter{}}

	decision, err := repo.DecideFeatureExecution("window_functions", "5.7.44")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decision.Mode != FeatureExecutionFallback {
		t.Fatalf("expected fallback mode, got %s", decision.Mode)
	}

	if decision.Fallback != FallbackApplicationLayer {
		t.Fatalf("expected application_layer fallback, got %s", decision.Fallback)
	}
}

func TestDecideAndExecuteCustomMongoJoiner(t *testing.T) {
	repo := &Repository{adapter: &MongoAdapter{}}

	decision, err := repo.DecideFeatureExecution("composite_foreign_keys", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decision.Mode != FeatureExecutionCustom {
		t.Fatalf("expected custom mode, got %s", decision.Mode)
	}

	result, err := repo.ExecuteFeature(context.Background(), "composite_foreign_keys", "", map[string]interface{}{
		"local_collection":  "orders",
		"foreign_collection": "order_items",
		"local_fields":      []string{"tenant_id", "order_no"},
		"foreign_fields":    []string{"tenant_id", "order_no"},
		"as":                "items",
	})
	if err != nil {
		t.Fatalf("unexpected execute error: %v", err)
	}

	payload, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected result map payload, got %T", result)
	}

	if payload["strategy"] != "aggregation_lookup" {
		t.Fatalf("expected aggregation_lookup strategy, got %v", payload["strategy"])
	}
}

func TestExecuteFeatureUnsupported(t *testing.T) {
	repo := &Repository{adapter: &PostgreSQLAdapter{}}

	_, err := repo.ExecuteFeature(context.Background(), "imaginary_feature", "", nil)
	if err == nil {
		t.Fatal("expected unsupported feature error")
	}

	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported keyword in error, got: %v", err)
	}
}
