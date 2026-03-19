package db

import "testing"

func TestMigrationLogPlaceholder_Postgres(t *testing.T) {
	repo := &Repository{adapter: &PostgreSQLAdapter{}}

	if got := migrationLogPlaceholder(repo, 1); got != "$1" {
		t.Fatalf("expected $1, got %q", got)
	}
	if got := migrationLogPlaceholder(repo, 2); got != "$2" {
		t.Fatalf("expected $2, got %q", got)
	}
}

func TestMigrationLogPlaceholder_MySQL(t *testing.T) {
	repo := &Repository{adapter: &MySQLAdapter{}}

	if got := migrationLogPlaceholder(repo, 1); got != "?" {
		t.Fatalf("expected ?, got %q", got)
	}
}
