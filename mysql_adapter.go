package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// MySQLAdapter MySQL 数据库适配器
type MySQLAdapter struct {
	config *Config
	db     *gorm.DB
	sqlDB  *sql.DB
}

// MySQLFactory MySQL 适配器工厂
type MySQLFactory struct{}

// Name 返回适配器名称
func (f *MySQLFactory) Name() string {
	return "mysql"
}

// Create 创建 MySQL 适配器
func (f *MySQLFactory) Create(config *Config) (Adapter, error) {
	adapter := &MySQLAdapter{config: config}
	if err := adapter.Connect(context.Background(), config); err != nil {
		return nil, err
	}
	return adapter, nil
}

// Connect 连接到 MySQL 数据库
func (a *MySQLAdapter) Connect(ctx context.Context, config *Config) error {
	if config == nil {
		config = a.config
	}

	// 验证必需字段
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Port == 0 {
		config.Port = 3306
	}
	if config.Username == "" {
		return fmt.Errorf("MySQL: username is required")
	}
	if config.Database == "" {
		return fmt.Errorf("MySQL: database name is required")
	}

	// 处理空密码
	password := config.Password

	// 构建 DSN (Data Source Name)
	// 格式: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true",
		config.Username,
		password,
		config.Host,
		config.Port,
		config.Database,
	)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL (host=%s, port=%d, user=%s, db=%s): %w", 
			config.Host, config.Port, config.Username, config.Database, err)
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
func (a *MySQLAdapter) Close() error {
	if a.sqlDB != nil {
		return a.sqlDB.Close()
	}
	return nil
}

// Ping 测试数据库连接
func (a *MySQLAdapter) Ping(ctx context.Context) error {
	if a.sqlDB == nil {
		return fmt.Errorf("database not connected")
	}
	return a.sqlDB.PingContext(ctx)
}

// Query 执行查询 (返回多行)
func (a *MySQLAdapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return a.sqlDB.QueryContext(ctx, query, args...)
}

// QueryRow 执行查询 (返回单行)
func (a *MySQLAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return a.sqlDB.QueryRowContext(ctx, query, args...)
}

// Exec 执行操作 (INSERT/UPDATE/DELETE)
func (a *MySQLAdapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return a.sqlDB.ExecContext(ctx, query, args...)
}

// Begin 开始事务
func (a *MySQLAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
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

	return &MySQLTx{tx: sqlTx}, nil
}

// GetRawConn 获取底层连接 (返回 *gorm.DB)
func (a *MySQLAdapter) GetRawConn() interface{} {
	return a.db
}

// GetGormDB 获取GORM实例（用于直接访问GORM）
func (a *MySQLAdapter) GetGormDB() *gorm.DB {
	return a.db
}

// RegisterScheduledTask MySQL 适配器暂不支持通过 EVENT 方式实现定时任务
// 建议在应用层使用 cron 库处理定时任务
func (a *MySQLAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return fmt.Errorf("MySQL adapter: scheduled tasks not implemented. Please implement in application layer using cron scheduler")
}

// UnregisterScheduledTask MySQL 适配器暂不支持
func (a *MySQLAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return fmt.Errorf("MySQL adapter: scheduled tasks not implemented")
}

// ListScheduledTasks MySQL 适配器暂不支持
func (a *MySQLAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, fmt.Errorf("MySQL adapter: scheduled tasks not implemented")
}

// MySQLTx MySQL 事务实现
type MySQLTx struct {
	tx *sql.Tx
}

// Commit 提交事务
func (t *MySQLTx) Commit(ctx context.Context) error {
	return t.tx.Commit()
}

// Rollback 回滚事务
func (t *MySQLTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback()
}

// Exec 在事务中执行
func (t *MySQLTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query 在事务中查询
func (t *MySQLTx) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// QueryRow 在事务中查询单行
func (t *MySQLTx) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

// init 自动注册 MySQL 适配器
func init() {
	RegisterAdapter(&MySQLFactory{})
}
