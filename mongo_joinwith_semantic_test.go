package db

import (
	"testing"
)

func TestMongoJoinWithUsesHasManyRelation(t *testing.T) {
	users := NewBaseSchema("users")
	orders := NewBaseSchema("orders")
	users.HasMany(orders).Over("user_id", "id")

	qb := NewMongoQueryConstructor(users)
	qb.JoinWith(NewJoinWith(orders).As("orders"))

	plan, err := qb.BuildFindPlan()
	if err != nil {
		t.Fatalf("BuildFindPlan failed: %v", err)
	}
	if len(plan.Lookups) != 1 {
		t.Fatalf("expected 1 lookup, got: %d", len(plan.Lookups))
	}
	lk := plan.Lookups[0]
	if lk.From != "orders" {
		t.Fatalf("unexpected lookup from: %s", lk.From)
	}
	if lk.LocalField != "id" || lk.ForeignField != "user_id" {
		t.Fatalf("unexpected lookup keys: local=%s foreign=%s", lk.LocalField, lk.ForeignField)
	}
	if lk.Semantic != JoinSemanticOptional {
		t.Fatalf("expected optional semantic for has_many, got: %s", lk.Semantic)
	}
}

func TestMongoJoinWithUsesBelongsToRelation(t *testing.T) {
	users := NewBaseSchema("users")
	orders := NewBaseSchema("orders")
	orders.BelongsTo(users).Over("user_id", "id")

	qb := NewMongoQueryConstructor(orders)
	qb.JoinWith(NewJoinWith(users).As("user"))

	plan, err := qb.BuildFindPlan()
	if err != nil {
		t.Fatalf("BuildFindPlan failed: %v", err)
	}
	if len(plan.Lookups) != 1 {
		t.Fatalf("expected 1 lookup, got: %d", len(plan.Lookups))
	}
	lk := plan.Lookups[0]
	if lk.From != "users" {
		t.Fatalf("unexpected lookup from: %s", lk.From)
	}
	if lk.LocalField != "user_id" || lk.ForeignField != "id" {
		t.Fatalf("unexpected lookup keys: local=%s foreign=%s", lk.LocalField, lk.ForeignField)
	}
	if lk.Semantic != JoinSemanticRequired {
		t.Fatalf("expected required semantic for belongs_to, got: %s", lk.Semantic)
	}
}

func TestMongoJoinWithFallsBackToFKConstraints(t *testing.T) {
	users := NewBaseSchema("users").AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	orders := NewBaseSchema("orders").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true}).
		AddField(&Field{Name: "user_id", Type: TypeInteger, Null: true})
	orders.AddForeignKey("fk_orders_users", []string{"user_id"}, "users", []string{"id"}, "", "")

	qb := NewMongoQueryConstructor(users)
	qb.JoinWith(NewJoinWith(orders).As("orders"))

	plan, err := qb.BuildFindPlan()
	if err != nil {
		t.Fatalf("BuildFindPlan failed: %v", err)
	}
	if len(plan.Lookups) != 1 {
		t.Fatalf("expected 1 lookup, got: %d", len(plan.Lookups))
	}
	lk := plan.Lookups[0]
	if lk.LocalField != "id" || lk.ForeignField != "user_id" {
		t.Fatalf("unexpected fallback lookup keys: local=%s foreign=%s", lk.LocalField, lk.ForeignField)
	}
	if lk.Semantic != JoinSemanticOptional {
		t.Fatalf("expected optional semantic from nullable FK fallback, got: %s", lk.Semantic)
	}
}

func TestMongoJoinWithManyToManyThroughBuildsTwoLookups(t *testing.T) {
	users := NewBaseSchema("users").AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	roles := NewBaseSchema("roles").AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	userRoles := NewBaseSchema("user_roles")

	users.ManyToMany(roles).Through(userRoles, "user_id", "role_id")

	qb := NewMongoQueryConstructor(users)
	qb.JoinWith(NewJoinWith(roles).As("roles"))

	plan, err := qb.BuildFindPlan()
	if err != nil {
		t.Fatalf("BuildFindPlan failed: %v", err)
	}
	if len(plan.Lookups) != 2 {
		t.Fatalf("expected 2 lookup stages for many_to_many through, got: %d", len(plan.Lookups))
	}

	first := plan.Lookups[0]
	if first.From != "user_roles" || first.LocalField != "id" || first.ForeignField != "user_id" {
		t.Fatalf("unexpected first lookup: %+v", first)
	}
	if !first.ThroughArtifact {
		t.Fatalf("expected first lookup to be marked as through artifact")
	}

	second := plan.Lookups[1]
	if second.From != "roles" || second.LocalField != "roles_through.role_id" || second.ForeignField != "id" {
		t.Fatalf("unexpected second lookup: %+v", second)
	}
}
