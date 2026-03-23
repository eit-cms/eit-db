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

func TestMongoQueryConstructorPaginateCursorMode(t *testing.T) {
	qb := NewMongoQueryConstructor(NewBaseSchema("users"))
	qb.Paginate(NewPaginationBuilder(1, 2).CursorBy("created_at", "ASC", "2026-03-21T10:00:00Z", nil))

	plan, err := qb.BuildFindPlan()
	if err != nil {
		t.Fatalf("BuildFindPlan failed: %v", err)
	}
	if plan.Limit == nil || *plan.Limit != 2 {
		t.Fatalf("expected limit=2, got %+v", plan.Limit)
	}
	if plan.Offset != nil {
		t.Fatalf("expected cursor pagination to avoid offset, got %+v", plan.Offset)
	}
	if len(plan.Sort) == 0 || plan.Sort[0].Field != "created_at" || plan.Sort[0].Direction != 1 {
		t.Fatalf("expected cursor sort on created_at ASC, got %+v", plan.Sort)
	}
	if got, ok := plan.Filter["created_at"].(map[string]interface{}); !ok || got["$gt"] != "2026-03-21T10:00:00Z" {
		t.Fatalf("expected cursor filter created_at > value, got %+v", plan.Filter)
	}
}

func TestMongoQueryConstructorPaginateCursorModeWithPrimaryTieBreaker(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("id", TypeInteger).PrimaryKey().Build())
	schema.AddField(NewField("created_at", TypeString).Build())

	qb := NewMongoQueryConstructor(schema)
	qb.Paginate(NewPaginationBuilder(1, 2).CursorBy("created_at", "ASC", "2026-03-21T10:00:00Z", 7))

	plan, err := qb.BuildFindPlan()
	if err != nil {
		t.Fatalf("BuildFindPlan failed: %v", err)
	}
	orItems, ok := plan.Filter["$or"].([]map[string]interface{})
	if !ok || len(orItems) != 2 {
		t.Fatalf("expected cursor filter to include OR tie-breaker, got %+v", plan.Filter)
	}
	if len(plan.Sort) < 2 || plan.Sort[0].Field != "created_at" || plan.Sort[1].Field != "id" {
		t.Fatalf("expected stable cursor sort by created_at then id, got %+v", plan.Sort)
	}
}
