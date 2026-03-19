package db

import (
	"context"
	"fmt"
	"time"
)

// Migration 表示一个数据库迁移
type Migration struct {
	// 版本号 (时间戳格式): 20260131100000
	Version string

	// 描述
	Description string

	// 迁移的 SQL 语句
	UpSQL []string

	// 回滚的 SQL 语句
	DownSQL []string
}

// MigrationLog 迁移日志记录
type MigrationLog struct {
	ID         int64
	Version    string
	RunOn      time.Time
	ExecutedAt time.Time
}

// Migrator 迁移器
type Migrator struct {
	repo       *Repository
	migrations map[string]*Migration
}

// NewMigrator 创建新的迁移器
func NewMigrator(repo *Repository) *Migrator {
	return &Migrator{
		repo:       repo,
		migrations: make(map[string]*Migration),
	}
}

// Register 注册迁移
func (m *Migrator) Register(migration *Migration) error {
	if migration.Version == "" {
		return fmt.Errorf("migration version cannot be empty")
	}

	if migration.Description == "" {
		return fmt.Errorf("migration description cannot be empty")
	}

	if len(migration.UpSQL) == 0 {
		return fmt.Errorf("migration UpSQL cannot be empty")
	}

	m.migrations[migration.Version] = migration
	return nil
}

// Up 执行所有待执行的迁移
func (m *Migrator) Up(ctx context.Context) error {
	// 创建迁移日志表 (如果不存在)
	if err := m.createMigrationLogTable(ctx); err != nil {
		return fmt.Errorf("failed to create migration log table: %w", err)
	}

	// 获取已执行的迁移
	executed, err := m.getExecutedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get executed migrations: %w", err)
	}

	// 执行待执行的迁移
	for version := range m.migrations {
		if _, exists := executed[version]; !exists {
			migration := m.migrations[version]
			if err := m.runMigration(ctx, migration); err != nil {
				return fmt.Errorf("failed to execute migration %s: %w", version, err)
			}
		}
	}

	return nil
}

// Down 回滚最后一个迁移
func (m *Migrator) Down(ctx context.Context) error {
	// 获取最后执行的迁移
	lastMigration, err := m.getLastExecutedMigration(ctx)
	if err != nil {
		return fmt.Errorf("failed to get last migration: %w", err)
	}

	if lastMigration == nil {
		return fmt.Errorf("no migrations to rollback")
	}

	// 查找迁移对象
	migration, exists := m.migrations[lastMigration.Version]
	if !exists {
		return fmt.Errorf("migration %s not found", lastMigration.Version)
	}

	// 执行回滚
	if err := m.rollbackMigration(ctx, migration); err != nil {
		return fmt.Errorf("failed to rollback migration %s: %w", migration.Version, err)
	}

	return nil
}

// Status 显示迁移状态
func (m *Migrator) Status(ctx context.Context) ([]map[string]interface{}, error) {
	executed, err := m.getExecutedMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get executed migrations: %w", err)
	}

	var status []map[string]interface{}

	for version, migration := range m.migrations {
		item := map[string]interface{}{
			"version":     version,
			"description": migration.Description,
			"status":      "pending",
		}

		if _, exists := executed[version]; exists {
			item["status"] = "executed"
		}

		status = append(status, item)
	}

	return status, nil
}

// runMigration 执行单个迁移
func (m *Migrator) runMigration(ctx context.Context, migration *Migration) error {
	// 开始事务
	tx, err := m.repo.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 执行迁移 SQL
	for _, sql := range migration.UpSQL {
		if _, err := tx.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to execute SQL: %w", err)
		}
	}

	// 记录迁移日志
	if err := executeMigrationOperationWithTx(ctx, m.repo, tx, MigrationOperation{
		Kind:      MigrationOpRecordApplied,
		Version:   migration.Version,
		AppliedAt: time.Now(),
	}); err != nil {
		return fmt.Errorf("failed to record migration: %w", err)
	}

	// 提交事务
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// rollbackMigration 回滚单个迁移
func (m *Migrator) rollbackMigration(ctx context.Context, migration *Migration) error {
	if len(migration.DownSQL) == 0 {
		return fmt.Errorf("migration %s has no down SQL", migration.Version)
	}

	// 开始事务
	tx, err := m.repo.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 执行回滚 SQL
	for _, sql := range migration.DownSQL {
		if _, err := tx.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to execute SQL: %w", err)
		}
	}

	// 删除迁移日志
	if err := executeMigrationOperationWithTx(ctx, m.repo, tx, MigrationOperation{
		Kind:    MigrationOpRemoveApplied,
		Version: migration.Version,
	}); err != nil {
		return fmt.Errorf("failed to delete migration log: %w", err)
	}

	// 提交事务
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// createMigrationLogTable 创建迁移日志表
func (m *Migrator) createMigrationLogTable(ctx context.Context) error {
	return ensureFrameworkTableUsingSchema(ctx, m.repo, buildSchemaMigrationsSchemaV1())
}

// getExecutedMigrations 获取已执行的迁移
func (m *Migrator) getExecutedMigrations(ctx context.Context) (map[string]bool, error) {
	executed := make(map[string]bool)

	query := `SELECT version FROM schema_migrations ORDER BY version`
	rows, err := m.repo.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		executed[version] = true
	}

	return executed, rows.Err()
}

// getLastExecutedMigration 获取最后执行的迁移
func (m *Migrator) getLastExecutedMigration(ctx context.Context) (*MigrationLog, error) {
	query := `SELECT id, version, executed_at FROM schema_migrations ORDER BY executed_at DESC LIMIT 1`
	row := m.repo.QueryRow(ctx, query)

	var log MigrationLog
	err := row.Scan(&log.ID, &log.Version, &log.ExecutedAt)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, err
	}

	return &log, nil
}
