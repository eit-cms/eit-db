package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// PostgreSQLAdapter PostgreSQL 数据库适配器
type PostgreSQLAdapter struct {
	config *Config
	db     *gorm.DB
	sqlDB  *sql.DB
}

// PostgreSQLFactory PostgreSQL 适配器工厂
type PostgreSQLFactory struct{}

// Name 返回适配器名称
func (f *PostgreSQLFactory) Name() string {
	return "postgres"
}

// Create 创建 PostgreSQL 适配器
func (f *PostgreSQLFactory) Create(config *Config) (Adapter, error) {
	adapter := &PostgreSQLAdapter{config: config}
	if err := adapter.Connect(context.Background(), config); err != nil {
		return nil, err
	}
	return adapter, nil
}

// Connect 连接到 PostgreSQL 数据库
func (a *PostgreSQLAdapter) Connect(ctx context.Context, config *Config) error {
	if config == nil {
		config = a.config
	}

	// 验证必需字段
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Port == 0 {
		config.Port = 5432
	}
	if config.Username == "" {
		return fmt.Errorf("PostgreSQL: username is required")
	}
	if config.Database == "" {
		return fmt.Errorf("PostgreSQL: database name is required")
	}
	if config.SSLMode == "" {
		config.SSLMode = "disable"
	}

	// 处理空密码（支持trust和ident认证）
	password := config.Password

	// 构建 DSN (Data Source Name)
	// lib/pq 格式: postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
	// 或使用键值格式: host=localhost port=5432 user=postgres password=secret dbname=mydb
	var dsn string
	if password != "" {
		dsn = fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			config.Host,
			config.Port,
			config.Username,
			password,
			config.Database,
			config.SSLMode,
		)
	} else {
		// 处理无密码的情况（信任认证）
		dsn = fmt.Sprintf(
			"host=%s port=%d user=%s dbname=%s sslmode=%s",
			config.Host,
			config.Port,
			config.Username,
			config.Database,
			config.SSLMode,
		)
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL (host=%s, port=%d, user=%s, db=%s, ssl=%s): %w", 
			config.Host, config.Port, config.Username, config.Database, config.SSLMode, err)
	}

	a.db = db

	// 获取底层 sql.DB 对象
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}
	a.sqlDB = sqlDB

	// 配置连接池（使用Config中的Pool设置）
	if config.Pool != nil {
		maxConns := config.Pool.MaxConnections
		if maxConns <= 0 {
			maxConns = 25
		}
		sqlDB.SetMaxOpenConns(maxConns)

		idleTimeout := config.Pool.IdleTimeout
		if idleTimeout <= 0 {
			idleTimeout = 300 // 5分钟
		}
		sqlDB.SetConnMaxIdleTime(time.Duration(idleTimeout) * time.Second)

		if config.Pool.MaxLifetime > 0 {
			sqlDB.SetConnMaxLifetime(time.Duration(config.Pool.MaxLifetime) * time.Second)
		}
	} else {
		// 默认连接池配置
		sqlDB.SetMaxOpenConns(25)
		sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	}

	return nil
}

// Close 关闭数据库连接
func (a *PostgreSQLAdapter) Close() error {
	if a.sqlDB != nil {
		return a.sqlDB.Close()
	}
	return nil
}

// Ping 测试数据库连接
func (a *PostgreSQLAdapter) Ping(ctx context.Context) error {
	if a.sqlDB == nil {
		return fmt.Errorf("database not connected")
	}
	return a.sqlDB.PingContext(ctx)
}

// Query 执行查询 (返回多行)
func (a *PostgreSQLAdapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return a.sqlDB.QueryContext(ctx, query, args...)
}

// QueryRow 执行查询 (返回单行)
func (a *PostgreSQLAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return a.sqlDB.QueryRowContext(ctx, query, args...)
}

// Exec 执行操作 (INSERT/UPDATE/DELETE)
func (a *PostgreSQLAdapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return a.sqlDB.ExecContext(ctx, query, args...)
}

// Begin 开始事务
func (a *PostgreSQLAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	txOpts := &sql.TxOptions{}
	for _, opt := range opts {
		if o, ok := opt.(*sql.TxOptions); ok {
			txOpts = o
		}
	}

	sqlTx, err := a.sqlDB.BeginTx(ctx, txOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &PostgreSQLTx{tx: sqlTx}, nil
}

// GetRawConn 获取底层连接 (返回 *gorm.DB)
func (a *PostgreSQLAdapter) GetRawConn() interface{} {
	return a.db
}

// GetGormDB 获取GORM实例（用于直接访问GORM）
func (a *PostgreSQLAdapter) GetGormDB() *gorm.DB {
	return a.db
}

// RegisterScheduledTask 在 PostgreSQL 中注册定时任务
// 使用 PostgreSQL 的触发器 + 函数来实现按月自动创建表的功能
func (a *PostgreSQLAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	if task == nil {
		return fmt.Errorf("task cannot be nil")
	}

	if err := task.Validate(); err != nil {
		return err
	}

	switch task.Type {
	case TaskTypeMonthlyTableCreation:
		return a.registerMonthlyTableCreation(ctx, task)
	default:
		return fmt.Errorf("unsupported task type for PostgreSQL: %s", task.Type)
	}
}

// registerMonthlyTableCreation 注册按月自动创建表的任务
func (a *PostgreSQLAdapter) registerMonthlyTableCreation(ctx context.Context, task *ScheduledTaskConfig) error {
	config := task.GetMonthlyTableConfig()

	tableName, _ := config["tableName"].(string)
	monthFormat, _ := config["monthFormat"].(string)
	fieldDefs, _ := config["fieldDefinitions"].(string)

	// 创建存储过程：create_monthly_log_table
	createProcSQL := fmt.Sprintf(`
CREATE OR REPLACE FUNCTION %s_create_table()
RETURNS void AS $$
DECLARE
	new_table_name TEXT;
	full_sql TEXT;
BEGIN
	new_table_name := '%s_' || TO_CHAR(CURRENT_DATE + INTERVAL '1 month', '%s');
	
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_name = new_table_name
	) THEN
		full_sql := 'CREATE TABLE ' || new_table_name || ' (%s)';
		EXECUTE full_sql;
		RAISE NOTICE 'Created table: %%', new_table_name;
	END IF;
END;
$$ LANGUAGE plpgsql;
	`, task.Name, tableName, monthFormat, fieldDefs)

	if err := a.db.WithContext(ctx).Exec(createProcSQL).Error; err != nil {
		return fmt.Errorf("failed to create function %s_create_table: %w", task.Name, err)
	}

	// 预热当前月份的表
	warmTableSQL := fmt.Sprintf(`
DO $$
DECLARE
	table_name TEXT;
	full_sql TEXT;
BEGIN
	FOR i IN 0..0 LOOP
		table_name := '%s_' || TO_CHAR(CURRENT_DATE + (i || ' months')::INTERVAL, '%s');
		
		IF NOT EXISTS (
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = table_name
		) THEN
			full_sql := 'CREATE TABLE ' || table_name || ' (%s)';
			EXECUTE full_sql;
			RAISE NOTICE 'Pre-warmed table: %%', table_name;
		END IF;
	END LOOP;
END $$;
	`, tableName, monthFormat, fieldDefs)

	if err := a.db.WithContext(ctx).Exec(warmTableSQL).Error; err != nil {
		return fmt.Errorf("failed to pre-warm tables: %w", err)
	}

	return nil
}

// UnregisterScheduledTask 注销定时任务
func (a *PostgreSQLAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	if taskName == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	// 删除存储过程
	dropFuncSQL := fmt.Sprintf(`
		DROP FUNCTION IF EXISTS %s_create_table() CASCADE;
	`, taskName)

	if err := a.db.WithContext(ctx).Exec(dropFuncSQL).Error; err != nil {
		return fmt.Errorf("failed to drop function %s_create_table: %w", taskName, err)
	}

	return nil
}

// ListScheduledTasks 列出所有已注册的定时任务
// PostgreSQL 版本返回空列表，因为存储过程的管理比较复杂
func (a *PostgreSQLAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	// PostgreSQL 适配器目前不支持列举动态注册的任务
	// 应用层应该维护已注册任务的列表
	return make([]*ScheduledTaskStatus, 0), nil
}

// PostgreSQLTx PostgreSQL 事务实现
type PostgreSQLTx struct {
	tx *sql.Tx
}

// Commit 提交事务
func (t *PostgreSQLTx) Commit(ctx context.Context) error {
	return t.tx.Commit()
}

// Rollback 回滚事务
func (t *PostgreSQLTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback()
}

// Exec 在事务中执行
func (t *PostgreSQLTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query 在事务中查询
func (t *PostgreSQLTx) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// QueryRow 在事务中查询单行
func (t *PostgreSQLTx) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

// init 自动注册 PostgreSQL 适配器
func init() {
	RegisterAdapter(&PostgreSQLFactory{})
}
