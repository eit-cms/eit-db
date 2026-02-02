package db

import (
	"context"
	"database/sql"
	"fmt"

	"gorm.io/gorm"
)

// GetGormDB 从 Repository 获取 GORM 实例
// 用于保持与现有 GORM 代码的兼容性
func (r *Repository) GetGormDB() *gorm.DB {
	if r.adapter == nil {
		return nil
	}

	// 尝试从不同类型的adapter中提取GORM实例
	switch a := r.adapter.(type) {
	case *MySQLAdapter:
		if a != nil {
			return a.db
		}
	case *PostgreSQLAdapter:
		if a != nil {
			return a.db
		}
	case *SQLiteAdapter:
		if a != nil {
			return a.db
		}
	case *gormAdapter:
		if a != nil {
			return a.db
		}
	}

	return nil
}

// gormAdapter 内部适配器，用于包装 GORM 实例
type gormAdapter struct {
	db *gorm.DB
}

// 实现 Adapter 接口
func (a *gormAdapter) Connect(ctx context.Context, config *Config) error {
	return nil // 已连接
}

func (a *gormAdapter) Close() error {
	sqlDB, err := a.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (a *gormAdapter) Ping(ctx context.Context) error {
	sqlDB, err := a.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

func (a *gormAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	rows, err := a.db.Raw(sql, args...).Rows()
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (a *gormAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return a.db.Raw(sql, args...).Row()
}

func (a *gormAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	result := a.db.Exec(sql, args...)
	if result.Error != nil {
		return nil, result.Error
	}
	return &gormResult{rows: result.RowsAffected}, nil
}

func (a *gormAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	tx := a.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &gormTx{tx: tx}, nil
}

func (a *gormAdapter) GetRawConn() interface{} {
	return a.db
}

func (a *gormAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return fmt.Errorf("gormAdapter: scheduled tasks not supported")
}

func (a *gormAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return fmt.Errorf("gormAdapter: scheduled tasks not supported")
}

func (a *gormAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, fmt.Errorf("gormAdapter: scheduled tasks not supported")
}

// gormTx 实现 Tx 接口
type gormTx struct {
	tx *gorm.DB
}

func (t *gormTx) Commit(ctx context.Context) error {
	return t.tx.Commit().Error
}

func (t *gormTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback().Error
}

func (t *gormTx) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	result := t.tx.Exec(sql, args...)
	if result.Error != nil {
		return nil, result.Error
	}
	return &gormResult{rows: result.RowsAffected}, nil
}

func (t *gormTx) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	rows, err := t.tx.Raw(sql, args...).Rows()
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (t *gormTx) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return t.tx.Raw(sql, args...).Row()
}

// gormResult 实现 sql.Result 接口
type gormResult struct {
	rows int64
}

func (r *gormResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *gormResult) RowsAffected() (int64, error) {
	return r.rows, nil
}

