package db

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func buildUserSchemaForChangesetExecutor() *BaseSchema {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("id", TypeInteger).Build())
	schema.AddField(NewField("name", TypeString).Null(false).Build())
	schema.AddField(NewField("email", TypeString).Null(false).Build())
	return schema
}

func createChangesetExecutorTestRepo(t *testing.T) *Repository {
	t.Helper()
	config := &Config{
		Adapter:  "sqlite",
		Database: filepath.Join(t.TempDir(), "changeset_executor.db"),
	}
	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	ctx := context.Background()
	_, err = repo.Exec(ctx, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	if err != nil {
		repo.Close()
		t.Fatalf("failed to create table: %v", err)
	}

	return repo
}

func TestNewChangesetExecutorInsert(t *testing.T) {
	repo := createChangesetExecutorTestRepo(t)
	defer repo.Close()

	executor, err := repo.NewChangesetExecutor(context.Background(), buildUserSchemaForChangesetExecutor())
	if err != nil {
		t.Fatalf("failed to create changeset executor: %v", err)
	}

	cs := NewChangeset(buildUserSchemaForChangesetExecutor())
	cs.Cast(map[string]interface{}{
		"id":    1,
		"name":  "Alice",
		"email": "alice@example.com",
	}).Validate()

	if _, err := executor.Insert(cs); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	var count int
	row := repo.QueryRow(context.Background(), "SELECT COUNT(*) FROM users")
	if err := row.Scan(&count); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
}

func TestWithChangesetRollbackOnError(t *testing.T) {
	repo := createChangesetExecutorTestRepo(t)
	defer repo.Close()

	schema := buildUserSchemaForChangesetExecutor()
	err := repo.WithChangeset(context.Background(), schema, func(executor *ChangesetExecutor) error {
		first := NewChangeset(schema)
		first.Cast(map[string]interface{}{
			"id":    1,
			"name":  "Alice",
			"email": "alice@example.com",
		}).Validate()
		if _, err := executor.Insert(first); err != nil {
			return err
		}

		second := NewChangeset(schema)
		second.Cast(map[string]interface{}{
			"id":   2,
			"name": "Bob",
			// email 缺失，触发验证错误
		}).Validate()
		_, err := executor.Insert(second)
		return err
	})
	if err == nil {
		t.Fatal("expected transaction to fail, got nil")
	}

	var count int
	row := repo.QueryRow(context.Background(), "SELECT COUNT(*) FROM users")
	if scanErr := row.Scan(&count); scanErr != nil {
		t.Fatalf("scan failed: %v", scanErr)
	}
	if count != 0 {
		t.Fatalf("expected rollback to leave 0 rows, got %d", count)
	}
}

func TestChangesetExecutorGuardRails(t *testing.T) {
	var executor *ChangesetExecutor
	cs := NewChangeset(buildUserSchemaForChangesetExecutor())

	if _, err := executor.Insert(cs); err == nil {
		t.Fatalf("expected nil executor insert to fail")
	}
	if _, err := executor.Update(cs, "id = ?", 1); err == nil {
		t.Fatalf("expected nil executor update to fail")
	}
	if _, err := executor.UpdateByID(1, cs); err == nil {
		t.Fatalf("expected nil executor update by id to fail")
	}
	if _, err := executor.Delete("id = ?", 1); err == nil {
		t.Fatalf("expected nil executor delete to fail")
	}
	if _, err := executor.DeleteByID(1); err == nil {
		t.Fatalf("expected nil executor delete by id to fail")
	}
	if _, err := executor.SoftDelete("id = ?", 1); err == nil {
		t.Fatalf("expected nil executor soft delete to fail")
	}
	if _, err := executor.SoftDeleteByID(1); err == nil {
		t.Fatalf("expected nil executor soft delete by id to fail")
	}
}

func TestNewChangesetExecutorAndWithChangesetValidation(t *testing.T) {
	schema := buildUserSchemaForChangesetExecutor()

	var nilRepo *Repository
	if _, err := nilRepo.NewChangesetExecutor(context.Background(), schema); err == nil {
		t.Fatalf("expected nil repository to fail")
	}

	repo := &Repository{}
	if _, err := repo.NewChangesetExecutor(context.Background(), nil); err == nil {
		t.Fatalf("expected nil schema to fail")
	}
	if _, err := repo.NewChangesetExecutor(context.Background(), schema); err == nil {
		t.Fatalf("expected missing adapter to fail")
	}

	if err := nilRepo.WithChangeset(context.Background(), schema, func(*ChangesetExecutor) error { return nil }); err == nil {
		t.Fatalf("expected nil repository WithChangeset to fail")
	}
	if err := repo.WithChangeset(context.Background(), nil, func(*ChangesetExecutor) error { return nil }); err == nil {
		t.Fatalf("expected nil schema WithChangeset to fail")
	}
	if err := repo.WithChangeset(context.Background(), schema, nil); err == nil {
		t.Fatalf("expected nil callback WithChangeset to fail")
	}
}

func TestWithChangesetCallbackError(t *testing.T) {
	repo := createChangesetExecutorTestRepo(t)
	defer repo.Close()

	wantErr := fmt.Errorf("stop")
	err := repo.WithChangeset(context.Background(), buildUserSchemaForChangesetExecutor(), func(executor *ChangesetExecutor) error {
		return wantErr
	})
	if err == nil || err.Error() != wantErr.Error() {
		t.Fatalf("expected callback error to bubble up, got %v", err)
	}
}
