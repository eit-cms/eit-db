package db

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestNormalizeMongoRelationJoinStrategy(t *testing.T) {
	if got := normalizeMongoRelationJoinStrategy(""); got != "lookup" {
		t.Fatalf("expected default lookup, got %s", got)
	}
	if got := normalizeMongoRelationJoinStrategy("PIPELINE"); got != "pipeline" {
		t.Fatalf("expected pipeline, got %s", got)
	}
	if got := normalizeMongoRelationJoinStrategy("unknown"); got != "lookup" {
		t.Fatalf("expected fallback lookup, got %s", got)
	}
}

func TestBuildMongoLookupStageLookup(t *testing.T) {
	stage, ok := buildMongoLookupStage(MongoLookupStage{
		From:         "roles",
		LocalField:   "role_id",
		ForeignField: "id",
		As:           "role",
	}, "lookup")
	if !ok {
		t.Fatalf("expected lookup stage")
	}
	lookup, _ := stage["$lookup"].(bson.M)
	if lookup["localField"] != "role_id" {
		t.Fatalf("unexpected localField: %+v", lookup)
	}
	if lookup["foreignField"] != "id" {
		t.Fatalf("unexpected foreignField: %+v", lookup)
	}
}

func TestBuildMongoLookupStagePipeline(t *testing.T) {
	stage, ok := buildMongoLookupStage(MongoLookupStage{
		From:         "roles",
		LocalField:   "roles_through.role_id",
		ForeignField: "id",
		As:           "roles",
	}, "pipeline")
	if !ok {
		t.Fatalf("expected pipeline stage")
	}
	lookup, _ := stage["$lookup"].(bson.M)
	if _, ok := lookup["let"]; !ok {
		t.Fatalf("expected let in pipeline lookup: %+v", lookup)
	}
	if _, ok := lookup["pipeline"]; !ok {
		t.Fatalf("expected pipeline in lookup stage: %+v", lookup)
	}
}

func TestCollectThroughArtifactFields(t *testing.T) {
	lookups := []MongoLookupStage{
		{As: "roles_through"},
		{As: "roles"},
		{As: "roles_through"},
		{As: "orders_through"},
	}
	fields := collectThroughArtifactFields(lookups)
	if len(fields) != 2 {
		t.Fatalf("expected 2 through artifact fields, got: %v", fields)
	}
	if fields[0] != "roles_through" && fields[1] != "roles_through" {
		t.Fatalf("expected roles_through in fields: %v", fields)
	}
	if fields[0] != "orders_through" && fields[1] != "orders_through" {
		t.Fatalf("expected orders_through in fields: %v", fields)
	}
}

func TestMongoHideThroughArtifactsEnabledDefaultTrue(t *testing.T) {
	if !mongoHideThroughArtifactsEnabled(nil) {
		t.Fatalf("expected default hide-through=true when config is nil")
	}
	cfg := &MongoConnectionConfig{}
	if !mongoHideThroughArtifactsEnabled(cfg) {
		t.Fatalf("expected default hide-through=true when option is nil")
	}
}

func TestCollectThroughArtifactFieldsByExplicitFlag(t *testing.T) {
	lookups := []MongoLookupStage{
		{As: "bridge_temp", ThroughArtifact: true},
		{As: "roles"},
	}
	fields := collectThroughArtifactFields(lookups)
	if len(fields) != 1 || fields[0] != "bridge_temp" {
		t.Fatalf("expected explicit through artifact field bridge_temp, got: %v", fields)
	}
}
