package db

import "testing"

func TestBaseSchemaAddTimestamps(t *testing.T) {
	schema := NewBaseSchema("users").
		AddField(NewField("id", TypeInteger).PrimaryKey().Build()).
		AddTimestamps()

	createdAt := schema.GetField("created_at")
	if createdAt == nil {
		t.Fatal("expected created_at field to be added")
	}
	if createdAt.Type != TypeTime {
		t.Fatalf("expected created_at type %s, got %s", TypeTime, createdAt.Type)
	}
	if createdAt.Null {
		t.Fatal("expected created_at to be NOT NULL")
	}
	if createdAt.Default != "CURRENT_TIMESTAMP" {
		t.Fatalf("expected created_at default CURRENT_TIMESTAMP, got %v", createdAt.Default)
	}

	updatedAt := schema.GetField("updated_at")
	if updatedAt == nil {
		t.Fatal("expected updated_at field to be added")
	}
	if updatedAt.Type != TypeTime {
		t.Fatalf("expected updated_at type %s, got %s", TypeTime, updatedAt.Type)
	}
	if updatedAt.Null {
		t.Fatal("expected updated_at to be NOT NULL")
	}
	if updatedAt.Default != "CURRENT_TIMESTAMP" {
		t.Fatalf("expected updated_at default CURRENT_TIMESTAMP, got %v", updatedAt.Default)
	}
}

func TestBaseSchemaAddTimestampsNoDuplicate(t *testing.T) {
	schema := NewBaseSchema("users").
		AddTimestamps().
		AddTimestamps()

	createdCount := 0
	updatedCount := 0
	for _, field := range schema.Fields() {
		switch field.Name {
		case "created_at":
			createdCount++
		case "updated_at":
			updatedCount++
		}
	}

	if createdCount != 1 {
		t.Fatalf("expected created_at count 1, got %d", createdCount)
	}
	if updatedCount != 1 {
		t.Fatalf("expected updated_at count 1, got %d", updatedCount)
	}
}

func TestBaseSchemaAddSoftDelete(t *testing.T) {
	schema := NewBaseSchema("users").AddSoftDelete()

	deletedAt := schema.GetField("deleted_at")
	if deletedAt == nil {
		t.Fatal("expected deleted_at field to be added")
	}
	if deletedAt.Type != TypeTime {
		t.Fatalf("expected deleted_at type %s, got %s", TypeTime, deletedAt.Type)
	}
	if !deletedAt.Null {
		t.Fatal("expected deleted_at to be nullable")
	}
}
