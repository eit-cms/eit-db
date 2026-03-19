package db

import (
	"context"
	"strings"
	"testing"
)

func TestCompileSQLMigrationOperation_PostgresRecordApplied(t *testing.T) {
	repo := &Repository{adapter: &PostgreSQLAdapter{}}
	op := MigrationOperation{
		Kind:    MigrationOpRecordApplied,
		Version: "20260319160000",
	}

	cmd, err := compileSQLMigrationOperation(repo, op)
	if err != nil {
		t.Fatalf("expected compile success, got error: %v", err)
	}

	if !strings.Contains(cmd.Query, "VALUES ($1, $2)") {
		t.Fatalf("expected postgres placeholders in query, got: %s", cmd.Query)
	}
	if len(cmd.Args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(cmd.Args))
	}
}

func TestCompileSQLMigrationOperation_PostgresRemoveApplied(t *testing.T) {
	repo := &Repository{adapter: &PostgreSQLAdapter{}}
	op := MigrationOperation{
		Kind:    MigrationOpRemoveApplied,
		Version: "20260319160000",
	}

	cmd, err := compileSQLMigrationOperation(repo, op)
	if err != nil {
		t.Fatalf("expected compile success, got error: %v", err)
	}

	if !strings.Contains(cmd.Query, "WHERE version = $1") {
		t.Fatalf("expected postgres placeholder in delete query, got: %s", cmd.Query)
	}
	if len(cmd.Args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(cmd.Args))
	}
}

func TestExecuteMigrationOperation_NoSQLUnsupported(t *testing.T) {
	op := MigrationOperation{Kind: MigrationOpRecordApplied, Version: "20260319160000"}

	mongoRepo := &Repository{adapter: &MongoAdapter{}}
	if err := executeMigrationOperation(context.Background(), mongoRepo, op); err == nil || !strings.Contains(err.Error(), "mongodb") {
		t.Fatalf("expected mongodb unsupported error, got: %v", err)
	}

	neoRepo := &Repository{adapter: &Neo4jAdapter{}}
	if err := executeMigrationOperation(context.Background(), neoRepo, op); err == nil || !strings.Contains(err.Error(), "neo4j") {
		t.Fatalf("expected neo4j unsupported error, got: %v", err)
	}
}
