package db

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestMongoQueryConstructorBuildPlan(t *testing.T) {
	schema := NewBaseSchema("users")
	qb := NewMongoQueryConstructor(schema)
	qb.Select("name", "age").Where(Eq("name", "alice")).OrderBy("age", "DESC").Limit(10).Offset(5)

	statement, args, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("build mongo query failed: %v", err)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args for mongo compiled statement, got %+v", args)
	}
	if !strings.HasPrefix(statement, mongoCompiledQueryPrefix) {
		t.Fatalf("unexpected mongo compiled statement prefix: %s", statement)
	}
}

func TestMongoQueryConstructorProviderCapabilities(t *testing.T) {
	provider := NewMongoQueryConstructorProvider()
	caps := provider.GetCapabilities()
	if caps == nil {
		t.Fatalf("expected capabilities")
	}
	if !caps.SupportsNativeQuery {
		t.Fatalf("expected mongo provider supports native query")
	}
	if caps.NativeQueryLang != "bson" {
		t.Fatalf("expected native query lang bson, got %s", caps.NativeQueryLang)
	}
	if caps.SupportsJoin {
		t.Fatalf("expected base mongo constructor does not support SQL join")
	}
}

func TestMongoQueryConstructorBuildInsertOnePlan(t *testing.T) {
	schema := NewBaseSchema("users")
	qb := NewMongoQueryConstructor(schema)
	qb.InsertOne(map[string]interface{}{"name": "alice", "age": 18})

	statement, args, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("build mongo insert plan failed: %v", err)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args for mongo compiled statement, got %+v", args)
	}
	if !strings.HasPrefix(statement, mongoCompiledWritePrefix) {
		t.Fatalf("unexpected mongo compiled write prefix: %s", statement)
	}
}

func TestMongoQueryConstructorBuildUpdateManyPlan(t *testing.T) {
	schema := NewBaseSchema("users")
	qb := NewMongoQueryConstructor(schema)
	qb.Where(Eq("status", "active"))
	qb.UpdateMany(map[string]interface{}{"status": "inactive"}, false)

	statement, args, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("build mongo update plan failed: %v", err)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args for mongo compiled statement, got %+v", args)
	}
	if !strings.HasPrefix(statement, mongoCompiledWritePrefix) {
		t.Fatalf("unexpected mongo compiled write prefix: %s", statement)
	}
}

func TestMongoQueryConstructorWritePlanReturnFlags(t *testing.T) {
	schema := NewBaseSchema("users")
	qb := NewMongoQueryConstructor(schema)
	qb.InsertOne(map[string]interface{}{"name": "alice"}).ReturnInsertedID().ReturnWriteDetail()

	statement, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("build mongo write plan failed: %v", err)
	}
	if !strings.HasPrefix(statement, mongoCompiledWritePrefix) {
		t.Fatalf("unexpected mongo compiled write prefix: %s", statement)
	}

	payload := strings.TrimPrefix(statement, mongoCompiledWritePrefix)
	var plan MongoCompiledWritePlan
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		t.Fatalf("failed to unmarshal write plan: %v", err)
	}
	if !plan.ReturnInsertedID {
		t.Fatalf("expected return_inserted_id flag enabled")
	}
	if !plan.ReturnWriteDetail {
		t.Fatalf("expected return_write_detail flag enabled")
	}
}
