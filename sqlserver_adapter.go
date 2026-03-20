package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

// SQLServerAdapter SQL Server 数据库适配器
type SQLServerAdapter struct {
	config *Config
	db     *gorm.DB
	sqlDB  *sql.DB
}

// SQLServerFactory SQL Server 适配器工厂
type SQLServerFactory struct{}

// Name 返回适配器名称
func (f *SQLServerFactory) Name() string {
	return "sqlserver"
}

// Create 创建 SQL Server 适配器
func (f *SQLServerFactory) Create(config *Config) (Adapter, error) {
	adapter := &SQLServerAdapter{config: config}
	if err := adapter.Connect(context.Background(), config); err != nil {
		return nil, err
	}
	return adapter, nil
}

// Connect 连接到 SQL Server 数据库
func (a *SQLServerAdapter) Connect(ctx context.Context, config *Config) error {
	if config == nil {
		config = a.config
	}
	resolved := config.ResolvedSQLServerConfig()

	// 验证必需字段
	if resolved.Host == "" {
		resolved.Host = "localhost"
	}
	if resolved.Port == 0 {
		resolved.Port = 1433 // SQL Server 默认端口
	}
	if resolved.Username == "" && strings.TrimSpace(resolved.DSN) == "" {
		return fmt.Errorf("SQL Server: username is required")
	}
	if resolved.Database == "" && strings.TrimSpace(resolved.DSN) == "" {
		return fmt.Errorf("SQL Server: database name is required")
	}

	// 处理空密码
	password := resolved.Password

	// 构建 DSN (Data Source Name)
	// 格式: sqlserver://username:password@host:port?database=dbname
	var dsn string
	if strings.TrimSpace(resolved.DSN) != "" {
		dsn = resolved.DSN
	} else {
		dsn = fmt.Sprintf(
			"sqlserver://%s:%s@%s:%d?database=%s&connection+timeout=30&encrypt=disable",
			resolved.Username,
			password,
			resolved.Host,
			resolved.Port,
			resolved.Database,
		)
	}

	db, err := gorm.Open(sqlserver.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to SQL Server (host=%s, port=%d, user=%s, db=%s): %w",
			resolved.Host, resolved.Port, resolved.Username, resolved.Database, err)
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
func (a *SQLServerAdapter) Close() error {
	if a.sqlDB != nil {
		return a.sqlDB.Close()
	}
	return nil
}

// Ping 测试数据库连接
func (a *SQLServerAdapter) Ping(ctx context.Context) error {
	if a.sqlDB == nil {
		return fmt.Errorf("database not connected")
	}
	return a.sqlDB.PingContext(ctx)
}

// InspectFullTextRuntime 检查 SQL Server Full-Text Search 是否安装。
func (a *SQLServerAdapter) InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error) {
	if a.sqlDB == nil {
		return nil, fmt.Errorf("database not connected")
	}

	var installed sql.NullInt64
	err := a.sqlDB.QueryRowContext(ctx, "SELECT FULLTEXTSERVICEPROPERTY('IsFullTextInstalled')").Scan(&installed)
	if err != nil {
		return nil, err
	}

	available := installed.Valid && installed.Int64 == 1
	return &FullTextRuntimeCapability{
		NativeSupported:       available,
		PluginChecked:         true,
		PluginAvailable:       available,
		PluginName:            "sqlserver_full_text_service",
		TokenizationSupported: available,
		TokenizationMode:      "native",
		Notes:                 "SQL Server full-text requires Full-Text service/catlog/index",
	}, nil
}

// InspectJSONRuntime 检查 SQL Server JSON 运行时能力。
// 说明：SQL Server 的 JSON 函数能力为内建能力（2016+），并非独立插件生态。
func (a *SQLServerAdapter) InspectJSONRuntime(ctx context.Context) (*JSONRuntimeCapability, error) {
	if a.sqlDB == nil {
		return nil, fmt.Errorf("database not connected")
	}

	var version sql.NullString
	if err := a.sqlDB.QueryRowContext(ctx, "SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))").Scan(&version); err != nil {
		return nil, err
	}

	// JSON 函数能力（SQL Server 2016+）
	hasJSONFunctions := false
	var isJSON sql.NullInt64
	if err := a.sqlDB.QueryRowContext(ctx, "SELECT ISJSON('{\"k\":1}')").Scan(&isJSON); err == nil {
		hasJSONFunctions = isJSON.Valid && isJSON.Int64 == 1
	}

	// 原生 JSON 类型（SQL Server 2025 预览及后续版本）
	hasNativeJSONType := false
	var jsonTypeID sql.NullInt64
	if err := a.sqlDB.QueryRowContext(ctx, "SELECT TYPE_ID('json')").Scan(&jsonTypeID); err == nil {
		hasNativeJSONType = jsonTypeID.Valid
	}

	notes := "SQL Server JSON is built-in (2016+); no separate JSON plugin installer is required"
	if !hasJSONFunctions {
		notes = "JSON functions not detected; ensure SQL Server 2016+ engine/version compatibility"
	} else if !hasNativeJSONType {
		notes = "JSON functions are available; native json data type requires SQL Server 2025+ preview/newer"
	}

	return &JSONRuntimeCapability{
		NativeSupported:         hasJSONFunctions,
		NativeJSONTypeSupported: hasNativeJSONType,
		Version:                 strings.TrimSpace(version.String),
		PluginChecked:           true,
		PluginAvailable:         false,
		PluginName:              "not_applicable",
		Notes:                   notes,
	}, nil
}

// Query 执行查询 (返回多行)
func (a *SQLServerAdapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return a.sqlDB.QueryContext(ctx, query, args...)
}

// QueryRow 执行查询 (返回单行)
func (a *SQLServerAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return a.sqlDB.QueryRowContext(ctx, query, args...)
}

// Exec 执行操作 (INSERT/UPDATE/DELETE)
func (a *SQLServerAdapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return a.sqlDB.ExecContext(ctx, query, args...)
}

// Begin 开始事务
func (a *SQLServerAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
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

	return &SQLServerTx{tx: sqlTx}, nil
}

// GetRawConn 获取底层连接 (返回 *sql.DB)
func (a *SQLServerAdapter) GetRawConn() interface{} {
	return a.sqlDB
}

// GetGormDB 获取GORM实例（用于直接访问GORM）
// Deprecated: Adapter 层不再暴露 GORM 连接。
func (a *SQLServerAdapter) GetGormDB() *gorm.DB {
	return nil
}

// RegisterScheduledTask SQL Server 适配器支持 SQL Server Agent 方式的定时任务
// 注意：需要 SQL Server Agent 服务运行，且用户需要相应权限
func (a *SQLServerAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	// TODO: 实现 SQL Server Agent Job 创建
	return fmt.Errorf("SQL Server adapter: scheduled tasks via SQL Server Agent not yet implemented")
}

// UnregisterScheduledTask SQL Server 适配器暂不支持
func (a *SQLServerAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return fmt.Errorf("SQL Server adapter: scheduled tasks not yet implemented")
}

// ListScheduledTasks SQL Server 适配器暂不支持
func (a *SQLServerAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, fmt.Errorf("SQL Server adapter: scheduled tasks not yet implemented")
}

// SQLServerTx SQL Server 事务实现
type SQLServerTx struct {
	tx *sql.Tx
}

// Commit 提交事务
func (t *SQLServerTx) Commit(ctx context.Context) error {
	return t.tx.Commit()
}

// Rollback 回滚事务
func (t *SQLServerTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback()
}

// Exec 在事务中执行
func (t *SQLServerTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query 在事务中查询
func (t *SQLServerTx) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// QueryRow 在事务中查询单行
func (t *SQLServerTx) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

// GetQueryBuilderProvider 返回查询构造器提供者
func (a *SQLServerAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	strategy := "direct_join"
	if a != nil && a.config != nil {
		resolved := a.config.ResolvedSQLServerConfig()
		if strings.TrimSpace(resolved.ManyToManyStrategy) != "" {
			strategy = resolved.ManyToManyStrategy
		}
		return NewDefaultSQLQueryConstructorProvider(NewSQLServerDialectWithOptions(
			strategy,
			resolved.RecursiveCTEDepth,
			resolved.RecursiveCTEMaxRecursion,
		))
	}
	return NewDefaultSQLQueryConstructorProvider(NewSQLServerDialectWithManyToManyStrategy(strategy))
}

// GetDatabaseFeatures 返回 SQL Server 数据库特性
func (a *SQLServerAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{
		// 索引和约束
		SupportsCompositeKeys:        true,
		SupportsForeignKeys:          true,
		SupportsCompositeForeignKeys: true,
		SupportsCompositeIndexes:     true,
		SupportsPartialIndexes:       true, // Filtered indexes
		SupportsDeferrable:           false,

		// 自定义类型
		SupportsEnumType:      false,
		SupportsCompositeType: false,
		SupportsDomainType:    false,
		SupportsUDT:           true,

		// 函数和过程
		SupportsStoredProcedures: true,
		SupportsFunctions:        true,
		SupportsAggregateFuncs:   true,
		FunctionLanguages:        []string{"tsql", "clr"},

		// 高级查询
		SupportsWindowFunctions: true,
		SupportsCTE:             true,
		SupportsRecursiveCTE:    true,
		SupportsMaterializedCTE: false,

		// JSON 支持
		HasNativeJSON:     false, // Stored as NVARCHAR
		SupportsJSONPath:  true,  // JSON functions since 2016
		SupportsJSONIndex: true,  // Via computed columns

		// 全文搜索
		SupportsFullTextSearch: true,
		FullTextLanguages:      []string{"english", "chinese", "japanese"},

		// 其他特性
		SupportsArrays:       false,
		SupportsGenerated:    true,  // Computed columns
		SupportsReturning:    true,  // OUTPUT clause
		SupportsUpsert:       true,  // MERGE
		SupportsListenNotify: false, // Use Service Broker instead

		// 元信息
		DatabaseName:    "SQL Server",
		DatabaseVersion: "2016+",
		Description:     "Enterprise database with T-SQL and CLR integration",
	}
}

// GetQueryFeatures 返回 SQL Server 的查询特性
func (a *SQLServerAdapter) GetQueryFeatures() *QueryFeatures {
	return NewSQLServerQueryFeatures()
}

// init 自动注册 SQL Server 适配器
func init() {
	RegisterAdapter(&SQLServerFactory{})
}
