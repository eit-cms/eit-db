package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// SQLiteAdapter SQLite 数据库适配器
type SQLiteAdapter struct {
	config *Config
	db     *gorm.DB
	sqlDB  *sql.DB
}

// NewSQLiteAdapter 创建 SQLite 适配器
func NewSQLiteAdapter(config *Config) (*SQLiteAdapter, error) {
	adapter := &SQLiteAdapter{config: config}
	if err := adapter.Connect(context.Background(), config); err != nil {
		return nil, err
	}
	return adapter, nil
}

// Connect 连接到 SQLite 数据库
func (a *SQLiteAdapter) Connect(ctx context.Context, config *Config) error {
	if config == nil {
		config = a.config
	}

	if config.Database == "" {
		config.Database = "./eit.db"
	}

	dsn := fmt.Sprintf("file:%s?cache=shared&mode=rwc", config.Database)

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to SQLite: %w", err)
	}

	a.db = db

	// 获取底层 sql.DB 对象
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}
	a.sqlDB = sqlDB

	// 配置连接池
	if config.Pool != nil {
		if config.Pool.MaxConnections > 0 {
			sqlDB.SetMaxOpenConns(config.Pool.MaxConnections)
		}
		if config.Pool.IdleTimeout > 0 {
			sqlDB.SetConnMaxIdleTime(time.Duration(config.Pool.IdleTimeout) * time.Second)
		}
	} else {
		sqlDB.SetMaxOpenConns(25)
		sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	}

	return nil
}

// Close 关闭数据库连接
func (a *SQLiteAdapter) Close() error {
	if a.sqlDB != nil {
		return a.sqlDB.Close()
	}
	return nil
}

// Ping 测试数据库连接
func (a *SQLiteAdapter) Ping(ctx context.Context) error {
	if a.sqlDB == nil {
		return fmt.Errorf("database not connected")
	}
	return a.sqlDB.PingContext(ctx)
}

// Query 执行查询 (返回多行)
func (a *SQLiteAdapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return a.sqlDB.QueryContext(ctx, query, args...)
}

// QueryRow 执行查询 (返回单行)
func (a *SQLiteAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return a.sqlDB.QueryRowContext(ctx, query, args...)
}

// Exec 执行操作 (INSERT/UPDATE/DELETE)
func (a *SQLiteAdapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return a.sqlDB.ExecContext(ctx, query, args...)
}

// Begin 开始事务
func (a *SQLiteAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
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

	return &SQLiteTx{tx: sqlTx}, nil
}

// GetRawConn 获取底层连接 (返回 *gorm.DB)
func (a *SQLiteAdapter) GetRawConn() interface{} {
	return a.db
}

// GetGormDB 获取GORM实例（用于直接访问GORM）
func (a *SQLiteAdapter) GetGormDB() *gorm.DB {
	return a.db
}

// RegisterScheduledTask SQLite 适配器暂不支持通过触发器方式实现定时任务
// 建议在应用层使用 cron 库处理定时任务
func (a *SQLiteAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return fmt.Errorf("SQLite adapter: scheduled tasks not implemented. Please implement in application layer using cron scheduler")
}

// UnregisterScheduledTask SQLite 适配器暂不支持
func (a *SQLiteAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return fmt.Errorf("SQLite adapter: scheduled tasks not implemented")
}

// ListScheduledTasks SQLite 适配器暂不支持
func (a *SQLiteAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, fmt.Errorf("SQLite adapter: scheduled tasks not implemented")
}

// SQLiteTx SQLite 事务实现
type SQLiteTx struct {
	tx *sql.Tx
}

// Commit 提交事务
func (t *SQLiteTx) Commit(ctx context.Context) error {
	return t.tx.Commit()
}

// Rollback 回滚事务
func (t *SQLiteTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback()
}

// Exec 在事务中执行
func (t *SQLiteTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query 在事务中查询
func (t *SQLiteTx) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// QueryRow 在事务中查询单行
func (t *SQLiteTx) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

// GetQueryBuilderProvider 返回查询构造器提供者
func (a *SQLiteAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return NewDefaultSQLQueryConstructorProvider(NewSQLiteDialect())
}

// GetDatabaseFeatures 返回 SQLite 数据库特性
func (a *SQLiteAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{
		// 索引和约束
		SupportsCompositeKeys:    true,
		SupportsCompositeIndexes: true,
		SupportsPartialIndexes:   true,
		SupportsDeferrable:       true,
		
		// 自定义类型
		SupportsEnumType:      false,
		SupportsCompositeType: false,
		SupportsDomainType:    false,
		SupportsUDT:           false,
		
		// 函数和过程
		SupportsStoredProcedures: false,
		SupportsFunctions:        true,  // ✅ 通过 Go 代码注册！
		SupportsAggregateFuncs:   true,  // ✅ 也可以通过 Go 注册
		FunctionLanguages:        []string{"go"}, // 使用 Go 语言注册
		
		// 高级查询
		SupportsWindowFunctions: true, // 3.25+
		SupportsCTE:             true, // 3.8+
		SupportsRecursiveCTE:    true,
		SupportsMaterializedCTE: false,
		
		// JSON 支持
		HasNativeJSON:     false,
		SupportsJSONPath:  true, // 3.38+ JSON functions
		SupportsJSONIndex: false,
		
		// 全文搜索
		SupportsFullTextSearch: true, // FTS5 extension
		FullTextLanguages:      []string{"english"},
		
		// 其他特性
		SupportsArrays:       false,
		SupportsGenerated:    true, // 3.31+
		SupportsReturning:    true, // 3.35+
		SupportsUpsert:       true, // ON CONFLICT
		SupportsListenNotify: false,
		
		// 元信息
		DatabaseName:    "SQLite",
		DatabaseVersion: "3.35+",
		Description:     "Lightweight embedded database with Go function registration support",
	}
}

// GetQueryFeatures 返回 SQLite 的查询特性
func (a *SQLiteAdapter) GetQueryFeatures() *QueryFeatures {
	return NewSQLiteQueryFeatures()
}

// init 自动注册 SQLite 适配器
func init() {
	_ = RegisterAdapterConstructor("sqlite", NewSQLiteAdapter)
}
