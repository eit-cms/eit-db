package db

import "testing"

func TestBaseSchemaHasManyRegister(t *testing.T) {
	userSchema := NewBaseSchema("users")
	orderSchema := NewBaseSchema("orders")

	userSchema.HasMany(orderSchema).Over("user_id", "id")

	rel := userSchema.FindRelation("orders")
	if rel == nil {
		t.Fatalf("expected relation users -> orders")
	}
	if rel.Type != RelationHasMany {
		t.Fatalf("expected has_many, got: %s", rel.Type)
	}
	if rel.ForeignKey != "user_id" || rel.OriginKey != "id" {
		t.Fatalf("unexpected relation keys: fk=%s origin=%s", rel.ForeignKey, rel.OriginKey)
	}
	if rel.Semantic() != JoinSemanticOptional {
		t.Fatalf("expected has_many semantic optional, got: %s", rel.Semantic())
	}
}

func TestBaseSchemaBelongsToDefaultRequired(t *testing.T) {
	userSchema := NewBaseSchema("users")
	orderSchema := NewBaseSchema("orders")

	orderSchema.BelongsTo(userSchema).Over("user_id", "id")

	rel := orderSchema.FindRelation("users")
	if rel == nil {
		t.Fatalf("expected relation orders -> users")
	}
	if rel.Type != RelationBelongsTo {
		t.Fatalf("expected belongs_to, got: %s", rel.Type)
	}
	if rel.Semantic() != JoinSemanticRequired {
		t.Fatalf("expected belongs_to semantic required, got: %s", rel.Semantic())
	}
}

func TestBaseSchemaRelationSemanticOverride(t *testing.T) {
	userSchema := NewBaseSchema("users")
	profileSchema := NewBaseSchema("profiles")

	userSchema.HasOne(profileSchema).Over("user_id", "id").Required()

	rel := userSchema.FindRelation("profiles")
	if rel == nil {
		t.Fatalf("expected relation users -> profiles")
	}
	if rel.Semantic() != JoinSemanticRequired {
		t.Fatalf("expected required semantic after override, got: %s", rel.Semantic())
	}
}

func TestBaseSchemaManyToManyThroughRegister(t *testing.T) {
	userSchema := NewBaseSchema("users")
	roleSchema := NewBaseSchema("roles")
	userRoleSchema := NewBaseSchema("user_roles")

	userSchema.ManyToMany(roleSchema).Through(userRoleSchema, "user_id", "role_id")

	rel := userSchema.FindRelation("roles")
	if rel == nil {
		t.Fatalf("expected relation users -> roles")
	}
	if rel.Type != RelationManyToMany {
		t.Fatalf("expected many_to_many, got: %s", rel.Type)
	}
	if rel.Through == nil {
		t.Fatalf("expected through metadata")
	}
	if rel.Through.Schema == nil || rel.Through.Schema.TableName() != "user_roles" {
		t.Fatalf("expected through schema user_roles")
	}
	if rel.Through.SourceKey != "user_id" || rel.Through.TargetKey != "role_id" {
		t.Fatalf("unexpected through keys: source=%s target=%s", rel.Through.SourceKey, rel.Through.TargetKey)
	}
	if rel.Semantic() != JoinSemanticOptional {
		t.Fatalf("expected many_to_many semantic optional, got: %s", rel.Semantic())
	}
}

func TestBaseSchemaHasOneReversible(t *testing.T) {
	userSchema := NewBaseSchema("users")
	profileSchema := NewBaseSchema("profiles")

	userSchema.HasOne(profileSchema).Over("user_id", "id").Reversible(true)

	rel := userSchema.FindRelation("profiles")
	if rel == nil {
		t.Fatalf("expected relation users -> profiles")
	}
	if !rel.Reversible {
		t.Fatalf("expected reversible flag true")
	}
}

func TestBaseSchemaRelationNamed(t *testing.T) {
	userSchema := NewBaseSchema("users")
	companySchema := NewBaseSchema("companies")

	userSchema.BelongsTo(companySchema).Over("company_id", "id").Named("works_at")

	rel := userSchema.FindRelation("companies")
	if rel == nil {
		t.Fatalf("expected relation users -> companies")
	}
	if rel.Name != "works_at" {
		t.Fatalf("expected relation name works_at, got: %s", rel.Name)
	}
}
