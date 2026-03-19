package db

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// MigrationOperationKind 表示迁移操作类型。
type MigrationOperationKind string

const (
	MigrationOpRecordApplied MigrationOperationKind = "record_applied"
	MigrationOpRemoveApplied MigrationOperationKind = "remove_applied"
)

// MigrationOperation 是迁移执行层的统一操作描述。
// v1.1 起，迁移运行器应只表达操作意图，不直接拼接方言 SQL。
type MigrationOperation struct {
	Kind        MigrationOperationKind
	Version     string
	Description string
	AppliedAt   time.Time
}

type compiledMigrationCommand struct {
	Query string
	Args  []interface{}
}

type migrationOperationExecutor interface {
	Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error)
}

type repositoryMigrationExecutor struct {
	repo *Repository
}

func (e repositoryMigrationExecutor) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	return e.repo.Exec(ctx, sql, args...)
}

type txMigrationExecutor struct {
	tx Tx
}

func (e txMigrationExecutor) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	return e.tx.Exec(ctx, sql, args...)
}

func executeMigrationOperation(ctx context.Context, repo *Repository, op MigrationOperation) error {
	return executeMigrationOperationWithExecutor(ctx, repo, repositoryMigrationExecutor{repo: repo}, op)
}

func executeMigrationOperationWithTx(ctx context.Context, repo *Repository, tx Tx, op MigrationOperation) error {
	if tx == nil {
		return fmt.Errorf("migration operation tx executor is nil")
	}
	return executeMigrationOperationWithExecutor(ctx, repo, txMigrationExecutor{tx: tx}, op)
}

func executeMigrationOperationWithExecutor(ctx context.Context, repo *Repository, execer migrationOperationExecutor, op MigrationOperation) error {
	if repo == nil || repo.GetAdapter() == nil {
		return fmt.Errorf("migration operation requires initialized repository")
	}
	if execer == nil {
		return fmt.Errorf("migration operation executor is nil")
	}

	switch repo.GetAdapter().(type) {
	case *MongoAdapter:
		return fmt.Errorf("migration operation %s is not supported for mongodb yet", op.Kind)
	case *Neo4jAdapter:
		return fmt.Errorf("migration operation %s is not supported for neo4j yet", op.Kind)
	}

	cmd, err := compileSQLMigrationOperation(repo, op)
	if err != nil {
		return err
	}

	_, err = execer.Exec(ctx, cmd.Query, cmd.Args...)
	return err
}

func compileSQLMigrationOperation(repo *Repository, op MigrationOperation) (*compiledMigrationCommand, error) {
	if strings.TrimSpace(op.Version) == "" {
		return nil, fmt.Errorf("migration operation version is required")
	}

	switch op.Kind {
	case MigrationOpRecordApplied:
		p1 := migrationLogPlaceholder(repo, 1)
		p2 := migrationLogPlaceholder(repo, 2)
		query := fmt.Sprintf("INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)", p1, p2)
		appliedAt := op.AppliedAt
		if appliedAt.IsZero() {
			appliedAt = time.Now()
		}
		return &compiledMigrationCommand{
			Query: query,
			Args:  []interface{}{op.Version, appliedAt},
		}, nil

	case MigrationOpRemoveApplied:
		p1 := migrationLogPlaceholder(repo, 1)
		query := fmt.Sprintf("DELETE FROM schema_migrations WHERE version = %s", p1)
		return &compiledMigrationCommand{
			Query: query,
			Args:  []interface{}{op.Version},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported migration operation kind: %s", op.Kind)
	}
}
