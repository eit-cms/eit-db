package db

import (
	"testing"
)

func TestBlueprintValidateSuccess(t *testing.T) {
	bp := &Blueprint{
		ID:      "chat-routing",
		Version: "0.1.0",
		Owner:   "collab",
		Datasets: []BlueprintDataset{
			{Name: "main-sql", Adapter: "postgres", Database: "app"},
			{Name: "managed-graph", Adapter: "arango", Database: "_system", Managed: true},
		},
		Entities: []BlueprintEntity{
			{
				Name: "user",
				Sources: []BlueprintEntitySource{
					{Dataset: "main-sql", Resource: "users", PrimaryKey: "id"},
					{Dataset: "managed-graph", Resource: "user_nodes", PrimaryKey: "_key"},
				},
			},
		},
		Relations: []BlueprintRelation{
			{Name: "user_follows", FromEntity: "user", ToEntity: "user", Type: "directed_edge", SourceDataset: "managed-graph", Confidence: 1},
		},
		Modules: []BlueprintModuleSlot{
			{Name: "user-feed", Entity: "user", DefaultDataset: "managed-graph", Purpose: "feed routing"},
		},
	}

	if err := bp.Validate(); err != nil {
		t.Fatalf("expected blueprint validate success, got: %v", err)
	}
}

func TestBlueprintValidateUnknownDatasetFails(t *testing.T) {
	bp := &Blueprint{
		ID:      "bad",
		Version: "0.1.0",
		Datasets: []BlueprintDataset{
			{Name: "main-sql", Adapter: "postgres"},
		},
		Entities: []BlueprintEntity{
			{Name: "user", Sources: []BlueprintEntitySource{{Dataset: "missing", Resource: "users"}}},
		},
	}

	if err := bp.Validate(); err == nil {
		t.Fatal("expected blueprint validation failure for unknown dataset")
	}
}

func TestBlueprintRegistryRegisterAndList(t *testing.T) {
	registry := NewBlueprintRegistry()
	bp := &Blueprint{
		ID:      "bp-a",
		Version: "0.1.0",
		Datasets: []BlueprintDataset{
			{Name: "main-sql", Adapter: "postgres"},
		},
		Entities: []BlueprintEntity{
			{Name: "user", Sources: []BlueprintEntitySource{{Dataset: "main-sql", Resource: "users"}}},
		},
	}

	if err := registry.Register(bp); err != nil {
		t.Fatalf("register blueprint failed: %v", err)
	}
	if _, ok := registry.Get("bp-a"); !ok {
		t.Fatal("expected blueprint to be retrievable")
	}
	ids := registry.ListIDs()
	if len(ids) != 1 || ids[0] != "bp-a" {
		t.Fatalf("unexpected blueprint ids: %+v", ids)
	}
}

func TestRepositoryResolveBlueprintRouteHint(t *testing.T) {
	repo := &Repository{adapterType: "arango"}
	bp := &Blueprint{
		ID:      "bp-route",
		Version: "0.1.0",
		Datasets: []BlueprintDataset{
			{Name: "main-sql", Adapter: "postgres"},
			{Name: "managed-graph", Adapter: "arango", Managed: true},
		},
		Entities: []BlueprintEntity{
			{
				Name: "user",
				Sources: []BlueprintEntitySource{
					{Dataset: "main-sql", Resource: "users"},
					{Dataset: "managed-graph", Resource: "user_nodes"},
				},
			},
		},
		Modules: []BlueprintModuleSlot{
			{Name: "feed", Entity: "user", DefaultDataset: "managed-graph"},
		},
	}

	hint, err := repo.ResolveBlueprintRouteHint(bp, "user", "feed")
	if err != nil {
		t.Fatalf("resolve route hint failed: %v", err)
	}
	if hint.Adapter != "arango" {
		t.Fatalf("expected adapter arango, got %s", hint.Adapter)
	}
	if hint.DefaultDataset != "managed-graph" {
		t.Fatalf("expected default dataset managed-graph, got %s", hint.DefaultDataset)
	}
	if len(hint.CandidateDatasets) != 1 || hint.CandidateDatasets[0] != "managed-graph" {
		t.Fatalf("unexpected candidate datasets: %+v", hint.CandidateDatasets)
	}
	if !hint.ManagedPreferred {
		t.Fatal("expected managed preferred to be true")
	}
}

func TestRepositoryResolveBlueprintRouteHintByID(t *testing.T) {
	repo := &Repository{adapterType: "arango"}
	registry := NewBlueprintRegistry()
	bp := &Blueprint{
		ID:      "bp-route-id",
		Version: "0.1.0",
		Datasets: []BlueprintDataset{
			{Name: "managed-graph", Adapter: "arango", Managed: true},
		},
		Entities: []BlueprintEntity{
			{Name: "user", Sources: []BlueprintEntitySource{{Dataset: "managed-graph", Resource: "user_nodes"}}},
		},
		Modules: []BlueprintModuleSlot{
			{Name: "feed", Entity: "user", DefaultDataset: "managed-graph"},
		},
	}
	if err := registry.Register(bp); err != nil {
		t.Fatalf("register blueprint failed: %v", err)
	}
	repo.SetBlueprintRegistry(registry)

	hint, err := repo.ResolveBlueprintRouteHintByID("bp-route-id", "user", "feed")
	if err != nil {
		t.Fatalf("resolve route hint by id failed: %v", err)
	}
	if hint.BlueprintID != "bp-route-id" {
		t.Fatalf("expected blueprint id bp-route-id, got %s", hint.BlueprintID)
	}
	if hint.DefaultDataset != "managed-graph" {
		t.Fatalf("expected default dataset managed-graph, got %s", hint.DefaultDataset)
	}
}
