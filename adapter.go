package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var lowLevelTxWarningEnabled atomic.Bool
var lowLevelTxWarningOnce sync.Once

func init() {
	lowLevelTxWarningEnabled.Store(true)
}

// SetLowLevelTransactionWarningEnabled 设置底层事务 Begin 的提示开关。
func SetLowLevelTransactionWarningEnabled(enabled bool) {
	lowLevelTxWarningEnabled.Store(enabled)
}

func maybeWarnLowLevelTransactionBegin() {
	if !lowLevelTxWarningEnabled.Load() {
		return
	}

	lowLevelTxWarningOnce.Do(func() {
		log.Printf("[eit-db] Repository.Begin 是底层事务原语，业务层推荐使用 Repository.WithChangeset")
	})
}

// Adapter 定义通用的数据库适配器接口 (参考 Ecto 设计)
// 每个数据库实现都必须满足这个接口
type Adapter interface {
	// 连接管理
	Connect(ctx context.Context, config *Config) error
	Close() error
	Ping(ctx context.Context) error

	// 事务管理
	// Deprecated: 这是底层事务原语，主要供框架内部、迁移、驱动集成使用。
	// 业务层写操作应优先使用 Changeset 封装入口，而不是直接手动拼事务。
	Begin(ctx context.Context, opts ...interface{}) (Tx, error)

	// 查询接口 (用于 SELECT)
	Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row

	// 执行接口 (用于 INSERT/UPDATE/DELETE)
	Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error)

	// 获取底层连接（建议返回标准驱动连接，如 *sql.DB / *sql.Tx）
	// 不应返回 ORM 对象，避免上层绕过能力路由与降级策略。
	GetRawConn() interface{}

	// 定时任务管理 - 允许数据库通过自己的方式实现后台任务
	// 例如: PostgreSQL 使用触发器 + pg_cron, MySQL 使用 EVENT, 应用层使用 cron 库
	RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error
	UnregisterScheduledTask(ctx context.Context, taskName string) error
	ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error)

	// QueryBuilder 提供者接口 (v0.4.1) - 中层转义层
	// Adapter 通过此接口提供特定数据库的 QueryConstructor 实现
	GetQueryBuilderProvider() QueryConstructorProvider

	// DatabaseFeatures 声明 (v0.4.3) - 数据库特性声明
	// 返回此 Adapter 支持的数据库特性集合
	GetDatabaseFeatures() *DatabaseFeatures

	// QueryFeatures 声明 (v0.4.4) - 查询特性声明
	// 返回此数据库支持的查询构造特性（JOIN、CTE、窗口函数等）
	GetQueryFeatures() *QueryFeatures
}

// Tx 定义事务接口
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error

	// 事务中的查询和执行
	Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row
	Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error)
}

// Config 数据库配置结构 (参考 Ecto 的 Repo 配置)
type Config struct {
	// 适配器类型: "sqlite" | "postgres" | "mysql" | "sqlserver" | "mongodb" | "neo4j"
	Adapter string `json:"adapter" yaml:"adapter"`

	// QueryCache 控制 Repository 级查询编译缓存策略。
	QueryCache *QueryCacheConfig `json:"query_cache,omitempty" yaml:"query_cache,omitempty"`

	// Adapter 专属配置。
	// 推荐使用这一组嵌套配置来表达连接细节，而不是把所有字段平铺到顶层。
	SQLite    *SQLiteConnectionConfig    `json:"sqlite,omitempty" yaml:"sqlite,omitempty"`
	Postgres  *PostgresConnectionConfig  `json:"postgres,omitempty" yaml:"postgres,omitempty"`
	MySQL     *MySQLConnectionConfig     `json:"mysql,omitempty" yaml:"mysql,omitempty"`
	SQLServer *SQLServerConnectionConfig `json:"sqlserver,omitempty" yaml:"sqlserver,omitempty"`
	MongoDB   *MongoConnectionConfig     `json:"mongodb,omitempty" yaml:"mongodb,omitempty"`
	Neo4j     *Neo4jConnectionConfig     `json:"neo4j,omitempty" yaml:"neo4j,omitempty"`

	// 旧的平铺字段仍保留为内部 fallback，便于仓库内逐步迁移。
	// 新代码应优先写入上面的 adapter 专属子配置。

	// SQLite 特定配置（legacy fallback）
	Database string `json:"database" yaml:"database"` // 数据库文件路径或数据库名

	// PostgreSQL/MySQL/SQL Server 通用配置（legacy fallback）
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`

	// PostgreSQL 特定配置（legacy fallback）
	SSLMode string `json:"ssl_mode" yaml:"ssl_mode"`

	// 连接池配置
	Pool *PoolConfig `json:"pool" yaml:"pool"`

	// 其他参数 (可选的适配器特定参数)
	Options map[string]interface{} `json:"options" yaml:"options"`

	// 校验规则与 locale 配置
	Validation *ValidationConfig `json:"validation,omitempty" yaml:"validation,omitempty"`

	// 启动期能力体检配置（strict/lenient）
	StartupCapabilities *StartupCapabilityConfig `json:"startup_capabilities,omitempty" yaml:"startup_capabilities,omitempty"`
}

// SQLiteConnectionConfig SQLite 连接配置。
type SQLiteConnectionConfig struct {
	Path string `json:"path,omitempty" yaml:"path,omitempty"`
	DSN  string `json:"dsn,omitempty" yaml:"dsn,omitempty"`
}

// QueryCacheConfig Repository 查询编译缓存配置。
type QueryCacheConfig struct {
	MaxEntries        int  `json:"max_entries,omitempty" yaml:"max_entries,omitempty"`
	DefaultTTLSeconds int  `json:"default_ttl_seconds,omitempty" yaml:"default_ttl_seconds,omitempty"`
	EnableMetrics     bool `json:"enable_metrics,omitempty" yaml:"enable_metrics,omitempty"`
}

// PostgresConnectionConfig PostgreSQL 连接配置。
type PostgresConnectionConfig struct {
	Host     string `json:"host,omitempty" yaml:"host,omitempty"`
	Port     int    `json:"port,omitempty" yaml:"port,omitempty"`
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	Database string `json:"database,omitempty" yaml:"database,omitempty"`
	SSLMode  string `json:"ssl_mode,omitempty" yaml:"ssl_mode,omitempty"`
	DSN      string `json:"dsn,omitempty" yaml:"dsn,omitempty"`
}

// MySQLConnectionConfig MySQL 连接配置。
type MySQLConnectionConfig struct {
	Host     string `json:"host,omitempty" yaml:"host,omitempty"`
	Port     int    `json:"port,omitempty" yaml:"port,omitempty"`
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	Database string `json:"database,omitempty" yaml:"database,omitempty"`
	DSN      string `json:"dsn,omitempty" yaml:"dsn,omitempty"`
}

// SQLServerConnectionConfig SQL Server 连接配置。
type SQLServerConnectionConfig struct {
	Host     string `json:"host,omitempty" yaml:"host,omitempty"`
	Port     int    `json:"port,omitempty" yaml:"port,omitempty"`
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	Database string `json:"database,omitempty" yaml:"database,omitempty"`
	DSN      string `json:"dsn,omitempty" yaml:"dsn,omitempty"`
}

// MongoConnectionConfig MongoDB 连接配置。
type MongoConnectionConfig struct {
	URI      string `json:"uri,omitempty" yaml:"uri,omitempty"`
	Database string `json:"database,omitempty" yaml:"database,omitempty"`
}

// Neo4jConnectionConfig Neo4j 连接配置。
type Neo4jConnectionConfig struct {
	URI      string `json:"uri,omitempty" yaml:"uri,omitempty"`
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	Database string `json:"database,omitempty" yaml:"database,omitempty"`
}

// ValidationConfig 校验 locale 配置
type ValidationConfig struct {
	// 默认 locale（例如: zh-CN / en-US）
	DefaultLocale string `json:"default_locale" yaml:"default_locale"`

	// 启用的 locale 列表；支持同时启用多个 locale
	EnabledLocales []string `json:"enabled_locales" yaml:"enabled_locales"`
}

// PoolConfig 连接池配置 (参考 Ecto 的设计)
type PoolConfig struct {
	MaxConnections int `json:"max_connections" yaml:"max_connections"`
	MinConnections int `json:"min_connections" yaml:"min_connections"`
	ConnectTimeout int `json:"connect_timeout" yaml:"connect_timeout"` // 秒
	IdleTimeout    int `json:"idle_timeout" yaml:"idle_timeout"`       // 秒
	MaxLifetime    int `json:"max_lifetime" yaml:"max_lifetime"`       // 秒
}

// AdapterFactory 适配器工厂接口
type AdapterFactory interface {
	Name() string
	Create(config *Config) (Adapter, error)
}

// Repository 数据库仓储对象 (类似 Ecto.Repo)
type Repository struct {
	adapter                 Adapter
	startupCapabilityReport *StartupCapabilityReport
	compiledQueryCache      *CompiledQueryCache
	mu                      sync.RWMutex
}

// 全局适配器工厂注册表
var (
	adapterFactories      = make(map[string]AdapterFactory)
	factoriesMutex        sync.RWMutex
	adapterConfigRegistry = make(map[string]*Config)
	configRegistryMutex   sync.RWMutex
)

// RegisterAdapter 注册适配器工厂
func RegisterAdapter(factory AdapterFactory) {
	factoriesMutex.Lock()
	defer factoriesMutex.Unlock()
	adapterFactories[factory.Name()] = factory
}

// adapterConstructorFactory 通过反射调用构造函数创建 Adapter
type adapterConstructorFactory struct {
	name    string
	ctor    reflect.Value
	argType reflect.Type
}

func (f *adapterConstructorFactory) Name() string {
	return f.name
}

func (f *adapterConstructorFactory) Create(config *Config) (Adapter, error) {
	if !f.ctor.IsValid() {
		return nil, fmt.Errorf("adapter constructor is invalid")
	}
	args := []reflect.Value{reflect.ValueOf(config)}
	results := f.ctor.Call(args)
	if len(results) != 2 {
		return nil, fmt.Errorf("adapter constructor must return (Adapter, error)")
	}

	if errVal := results[1]; !errVal.IsNil() {
		if err, ok := errVal.Interface().(error); ok {
			return nil, err
		}
		return nil, fmt.Errorf("adapter constructor returned invalid error type")
	}

	adapterVal := results[0]
	if !adapterVal.IsValid() || adapterVal.IsNil() {
		return nil, fmt.Errorf("adapter constructor returned nil adapter")
	}

	adapter, ok := adapterVal.Interface().(Adapter)
	if !ok {
		return nil, fmt.Errorf("adapter constructor return type does not implement Adapter")
	}

	return adapter, nil
}

// RegisterAdapterConstructor 使用构造函数动态注册 Adapter
// 允许的构造函数签名：func(*Config) (Adapter, error) 或 func(*Config) (*T, error)
func RegisterAdapterConstructor(name string, ctor interface{}) error {
	if name == "" {
		return fmt.Errorf("adapter name cannot be empty")
	}
	if ctor == nil {
		return fmt.Errorf("adapter constructor cannot be nil")
	}

	ctorVal := reflect.ValueOf(ctor)
	ctorType := ctorVal.Type()
	if ctorType.Kind() != reflect.Func {
		return fmt.Errorf("adapter constructor must be a function")
	}
	if ctorType.NumIn() != 1 || ctorType.In(0) != reflect.TypeOf(&Config{}) {
		return fmt.Errorf("adapter constructor must accept *Config")
	}
	if ctorType.NumOut() != 2 {
		return fmt.Errorf("adapter constructor must return (Adapter, error)")
	}
	if !ctorType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return fmt.Errorf("adapter constructor must return error as second return value")
	}

	factory := &adapterConstructorFactory{
		name:    name,
		ctor:    ctorVal,
		argType: ctorType.In(0),
	}
	RegisterAdapter(factory)
	return nil
}

// RegisterAdapterConfig 注册 Adapter 配置（支持多 Adapter 注册）
func RegisterAdapterConfig(name string, config *Config) error {
	if name == "" {
		return fmt.Errorf("adapter name cannot be empty")
	}
	if config == nil {
		return fmt.Errorf("adapter config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid adapter config '%s': %w", name, err)
	}

	configRegistryMutex.Lock()
	defer configRegistryMutex.Unlock()
	adapterConfigRegistry[name] = config
	return nil
}

// RegisterAdapterConfigs 批量注册 Adapter 配置
func RegisterAdapterConfigs(configs map[string]*Config) error {
	for name, cfg := range configs {
		if err := RegisterAdapterConfig(name, cfg); err != nil {
			return err
		}
	}
	return nil
}

// GetAdapterConfig 获取已注册的 Adapter 配置
func GetAdapterConfig(name string) (*Config, bool) {
	configRegistryMutex.RLock()
	defer configRegistryMutex.RUnlock()
	cfg, ok := adapterConfigRegistry[name]
	return cfg, ok
}

// NewRepositoryFromAdapterConfig 通过已注册的 Adapter 配置创建 Repository
func NewRepositoryFromAdapterConfig(name string) (*Repository, error) {
	cfg, ok := GetAdapterConfig(name)
	if !ok {
		return nil, fmt.Errorf("adapter config not found: %s", name)
	}
	return NewRepository(cfg)
}

// NewRepository 创建新的仓储实例 (通过配置注入)
func NewRepository(config *Config) (*Repository, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if config.Adapter == "" {
		return nil, fmt.Errorf("adapter type must be specified")
	}

	if err := applyValidationConfig(config.Validation); err != nil {
		return nil, fmt.Errorf("failed to apply validation locale config: %w", err)
	}

	// 从工厂注册表中获取适配器工厂
	factoriesMutex.RLock()
	factory, ok := adapterFactories[config.Adapter]
	factoriesMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unsupported adapter: %s", config.Adapter)
	}

	// 使用工厂创建适配器
	adapter, err := factory.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create adapter: %w", err)
	}

	cacheSize := DefaultCompiledQueryCacheMaxEntries
	cacheTTLSeconds := DefaultCompiledQueryCacheDefaultTTLSeconds
	cacheEnableMetrics := DefaultCompiledQueryCacheEnableMetrics
	if config.QueryCache != nil {
		cacheSize = config.QueryCache.MaxEntries
		cacheTTLSeconds = config.QueryCache.DefaultTTLSeconds
		cacheEnableMetrics = config.QueryCache.EnableMetrics
	}

	repo := &Repository{adapter: adapter, compiledQueryCache: NewCompiledQueryCacheWithOptions(cacheSize, time.Duration(cacheTTLSeconds)*time.Second, cacheEnableMetrics)}
	if config.StartupCapabilities != nil {
		report, checkErr := repo.RunStartupCapabilityCheck(context.Background(), config.StartupCapabilities)
		repo.startupCapabilityReport = report
		if checkErr != nil {
			_ = adapter.Close()
			return nil, fmt.Errorf("startup capability check failed: %w", checkErr)
		}
	}

	return repo, nil
}

// GetStartupCapabilityReport 返回最近一次启动体检报告。
func (r *Repository) GetStartupCapabilityReport() *StartupCapabilityReport {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.startupCapabilityReport
}

func applyValidationConfig(cfg *ValidationConfig) error {
	if cfg == nil {
		return nil
	}

	enabled := make([]string, 0, len(cfg.EnabledLocales))
	enabled = append(enabled, cfg.EnabledLocales...)

	return ConfigureValidationLocales(cfg.DefaultLocale, enabled)
}

// Connect 连接数据库
func (r *Repository) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.adapter == nil {
		return fmt.Errorf("adapter is not initialized")
	}
	return r.adapter.Connect(ctx, nil)
}

// Close 关闭数据库连接
func (r *Repository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.compiledQueryCache != nil {
		r.compiledQueryCache.close()
		r.compiledQueryCache = nil
	}

	if r.adapter == nil {
		return nil
	}
	return r.adapter.Close()
}

// Ping 测试数据库连接
func (r *Repository) Ping(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return fmt.Errorf("adapter is not initialized")
	}
	return r.adapter.Ping(ctx)
}

// Query 执行查询
func (r *Repository) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}
	return r.adapter.Query(ctx, sql, args...)
}

// QueryRow 执行单行查询
func (r *Repository) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return nil
	}
	return r.adapter.QueryRow(ctx, sql, args...)
}

// Exec 执行操作
func (r *Repository) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}
	return r.adapter.Exec(ctx, sql, args...)
}

// Begin 开始事务
// Deprecated: 这是底层事务原语，主要供迁移/框架集成使用。
// 业务层应优先使用 Repository.WithChangeset，以避免绕过 Changeset 校验与写入封装。
func (r *Repository) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	maybeWarnLowLevelTransactionBegin()

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}
	return r.adapter.Begin(ctx, opts...)
}

// QueryStruct 查询单个结构体
// 自动将查询结果映射到结构体
func (r *Repository) QueryStruct(ctx context.Context, dest interface{}, sql string, args ...interface{}) error {
	row := r.QueryRow(ctx, sql, args...)
	return ScanStruct(row, dest)
}

// QueryStructs 查询多个结构体
// 自动将查询结果映射到结构体切片
func (r *Repository) QueryStructs(ctx context.Context, dest interface{}, sql string, args ...interface{}) error {
	rows, err := r.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return ScanStructs(rows, dest)
}

// GetAdapter 获取底层适配器 (用于高级操作)
func (r *Repository) GetAdapter() Adapter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.adapter
}

// NewQueryConstructor 创建 v2 查询构造器（推荐）
func (r *Repository) NewQueryConstructor(schema Schema) (QueryConstructor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}

	provider := r.adapter.GetQueryBuilderProvider()
	if provider == nil {
		return nil, fmt.Errorf("query constructor provider is not available")
	}

	return provider.NewQueryConstructor(schema), nil
}

// QueryConstructorExecutionResult 表示 QueryConstructor 统一执行结果。
type QueryConstructorExecutionResult struct {
	Statement string
	Args      []interface{}
	Rows      []map[string]interface{}
}

// QueryConstructorAutoExecutionResult 表示 QueryConstructor 自动路由执行结果。
// Mode=query 表示查询结果在 Rows；Mode=exec 表示写入摘要在 Exec 中。
type QueryConstructorAutoExecutionResult struct {
	Mode      string
	Statement string
	Args      []interface{}
	Rows      []map[string]interface{}
	Exec      *QueryConstructorExecSummary
}

// QueryConstructorExecSummary 表示写入执行摘要。
type QueryConstructorExecSummary struct {
	RowsAffected int64
	LastInsertID *int64
	Counters     map[string]int
	Details      map[string]interface{}
}

// ExecuteQueryConstructor 执行 QueryConstructor，自动路由 SQL/Neo4j/MongoDB。
// - SQL 适配器：执行 Query 并将结果扫描为 []map[string]interface{}
// - Neo4j 适配器：执行 QueryCypher 并返回记录
// - MongoDB 适配器：执行 BSON Find 计划并返回文档
func (r *Repository) ExecuteQueryConstructor(ctx context.Context, constructor QueryConstructor) (*QueryConstructorExecutionResult, error) {
	if constructor == nil {
		return nil, fmt.Errorf("query constructor cannot be nil")
	}

	query, args, err := constructor.Build(ctx)
	if err != nil {
		return nil, err
	}

	return r.executeCompiledQueryStatement(ctx, query, args)
}

// ExecuteQueryConstructorWithCache 执行 QueryConstructor，并在编译阶段接入缓存。
// 返回 cacheHit=true 表示复用了已缓存的编译结果。
func (r *Repository) ExecuteQueryConstructorWithCache(ctx context.Context, cacheKey string, constructor QueryConstructor) (*QueryConstructorExecutionResult, bool, error) {
	if constructor == nil {
		return nil, false, fmt.Errorf("query constructor cannot be nil")
	}
	if strings.TrimSpace(cacheKey) == "" {
		return nil, false, fmt.Errorf("cache key cannot be empty")
	}

	query, args, cacheHit, err := r.BuildAndCacheQuery(ctx, cacheKey, constructor)
	if err != nil {
		return nil, false, err
	}

	result, execErr := r.executeCompiledQueryStatement(ctx, query, args)
	if execErr != nil {
		return nil, cacheHit, execErr
	}
	return result, cacheHit, nil
}

// ExecuteQueryConstructorAuto 执行 QueryConstructor，自动识别读/写语义并路由到 adapter。
//
// 路由规则：
// - SQL：SELECT/WITH...SELECT 走 Query，其他语句走 Exec
// - Neo4j：MATCH/OPTIONAL MATCH/RETURN/CALL...YIELD 走 Query，其余走 ExecCypher
// - MongoDB：支持由 MongoQueryConstructor 生成的 MONGO_FIND:: 查询计划（query）与 MONGO_WRITE:: 写入计划（exec）
func (r *Repository) ExecuteQueryConstructorAuto(ctx context.Context, constructor QueryConstructor) (*QueryConstructorAutoExecutionResult, error) {
	if constructor == nil {
		return nil, fmt.Errorf("query constructor cannot be nil")
	}

	query, args, err := constructor.Build(ctx)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query statement cannot be empty")
	}

	r.mu.RLock()
	adapter := r.adapter
	r.mu.RUnlock()
	if adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}

	if neo, ok := adapter.(*Neo4jAdapter); ok {
		if looksLikeReadCypher(query) {
			rows, queryErr := neo.QueryCypher(ctx, query, buildCypherParamsFromArgs(args))
			if queryErr != nil {
				return nil, queryErr
			}
			return &QueryConstructorAutoExecutionResult{
				Mode:      "query",
				Statement: query,
				Args:      copyQueryArgs(args),
				Rows:      rows,
			}, nil
		}

		summary, execErr := neo.ExecCypher(ctx, query, buildCypherParamsFromArgs(args))
		if execErr != nil {
			return nil, execErr
		}
		rowsAffected := int64(summary.NodesCreated + summary.NodesDeleted + summary.RelationshipsCreated + summary.RelationshipsDeleted)
		return &QueryConstructorAutoExecutionResult{
			Mode:      "exec",
			Statement: query,
			Args:      copyQueryArgs(args),
			Exec: &QueryConstructorExecSummary{
				RowsAffected: rowsAffected,
				LastInsertID: nil,
				Counters: map[string]int{
					"nodes_created":         summary.NodesCreated,
					"nodes_deleted":         summary.NodesDeleted,
					"relationships_created": summary.RelationshipsCreated,
					"relationships_deleted": summary.RelationshipsDeleted,
					"properties_set":        summary.PropertiesSet,
					"labels_added":          summary.LabelsAdded,
					"labels_removed":        summary.LabelsRemoved,
				},
			},
		}, nil
	}

	if mongoAdapter, ok := adapter.(*MongoAdapter); ok {
		trimmed := strings.TrimSpace(query)
		if strings.HasPrefix(trimmed, mongoCompiledQueryPrefix) {
			rows, queryErr := mongoAdapter.ExecuteCompiledFindPlan(ctx, query)
			if queryErr != nil {
				return nil, queryErr
			}
			return &QueryConstructorAutoExecutionResult{
				Mode:      "query",
				Statement: query,
				Args:      copyQueryArgs(args),
				Rows:      rows,
			}, nil
		}
		if strings.HasPrefix(trimmed, mongoCompiledWritePrefix) {
			summary, execErr := mongoAdapter.ExecuteCompiledWritePlan(ctx, query)
			if execErr != nil {
				return nil, execErr
			}
			return &QueryConstructorAutoExecutionResult{
				Mode:      "exec",
				Statement: query,
				Args:      copyQueryArgs(args),
				Exec:      summary,
			}, nil
		}
		return nil, fmt.Errorf("mongodb query constructor requires compiled plan prefix %q or %q", mongoCompiledQueryPrefix, mongoCompiledWritePrefix)
	}

	if looksLikeReadSQL(query) {
		sqlRows, queryErr := r.Query(ctx, query, args...)
		if queryErr != nil {
			return nil, queryErr
		}
		defer sqlRows.Close()

		rows, scanErr := scanRowsToMapSlice(sqlRows)
		if scanErr != nil {
			return nil, scanErr
		}
		return &QueryConstructorAutoExecutionResult{
			Mode:      "query",
			Statement: query,
			Args:      copyQueryArgs(args),
			Rows:      rows,
		}, nil
	}

	res, execErr := r.Exec(ctx, query, args...)
	if execErr != nil {
		return nil, execErr
	}

	var rowsAffected int64
	if affected, err := res.RowsAffected(); err == nil {
		rowsAffected = affected
	}

	var lastInsertID *int64
	if insertID, err := res.LastInsertId(); err == nil {
		lastInsertID = &insertID
	}

	return &QueryConstructorAutoExecutionResult{
		Mode:      "exec",
		Statement: query,
		Args:      copyQueryArgs(args),
		Exec: &QueryConstructorExecSummary{
			RowsAffected: rowsAffected,
			LastInsertID: lastInsertID,
		},
	}, nil
}

func (r *Repository) executeCompiledQueryStatement(ctx context.Context, query string, args []interface{}) (*QueryConstructorExecutionResult, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query statement cannot be empty")
	}

	r.mu.RLock()
	adapter := r.adapter
	r.mu.RUnlock()
	if adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}

	if neo, ok := adapter.(*Neo4jAdapter); ok {
		rows, queryErr := neo.QueryCypher(ctx, query, buildCypherParamsFromArgs(args))
		if queryErr != nil {
			return nil, queryErr
		}
		return &QueryConstructorExecutionResult{Statement: query, Args: copyQueryArgs(args), Rows: rows}, nil
	}

	if mongoAdapter, ok := adapter.(*MongoAdapter); ok {
		trimmed := strings.TrimSpace(query)
		if strings.HasPrefix(trimmed, mongoCompiledQueryPrefix) {
			rows, queryErr := mongoAdapter.ExecuteCompiledFindPlan(ctx, query)
			if queryErr != nil {
				return nil, queryErr
			}
			return &QueryConstructorExecutionResult{Statement: query, Args: copyQueryArgs(args), Rows: rows}, nil
		}
		if strings.HasPrefix(trimmed, mongoCompiledWritePrefix) {
			return nil, fmt.Errorf("ExecuteQueryConstructor is query-only for mongodb write plans; use ExecuteQueryConstructorAuto")
		}
		return nil, fmt.Errorf("mongodb query constructor requires compiled plan prefix %q", mongoCompiledQueryPrefix)
	}

	sqlRows, queryErr := r.Query(ctx, query, args...)
	if queryErr != nil {
		return nil, queryErr
	}
	defer sqlRows.Close()

	rows, scanErr := scanRowsToMapSlice(sqlRows)
	if scanErr != nil {
		return nil, scanErr
	}

	return &QueryConstructorExecutionResult{Statement: query, Args: copyQueryArgs(args), Rows: rows}, nil
}

func buildCypherParamsFromArgs(args []interface{}) map[string]interface{} {
	params := make(map[string]interface{}, len(args))
	for i, arg := range args {
		params[fmt.Sprintf("p%d", i+1)] = arg
	}
	return params
}

func looksLikeReadSQL(query string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(query))
	if normalized == "" {
		return false
	}
	if strings.HasPrefix(normalized, "SELECT") {
		return true
	}
	if strings.HasPrefix(normalized, "WITH") {
		return strings.Contains(normalized, "SELECT")
	}
	if strings.HasPrefix(normalized, "SHOW") || strings.HasPrefix(normalized, "DESCRIBE") || strings.HasPrefix(normalized, "EXPLAIN") {
		return true
	}
	return false
}

func looksLikeReadCypher(query string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(query))
	if normalized == "" {
		return false
	}
	if strings.HasPrefix(normalized, "MATCH") || strings.HasPrefix(normalized, "OPTIONAL MATCH") {
		return true
	}
	if strings.HasPrefix(normalized, "RETURN") {
		return true
	}
	if strings.HasPrefix(normalized, "CALL") {
		return strings.Contains(normalized, "YIELD") || strings.Contains(normalized, "RETURN")
	}
	if strings.HasPrefix(normalized, "WITH") {
		return strings.Contains(normalized, "RETURN")
	}
	return false
}

func scanRowsToMapSlice(rows *sql.Rows) ([]map[string]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if scanErr := rows.Scan(ptrs...); scanErr != nil {
			return nil, scanErr
		}

		entry := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			v := vals[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			entry[strings.ToLower(col)] = v
		}
		result = append(result, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// GetQueryBuilderCapabilities 获取当前适配器的查询构造能力声明
func (r *Repository) GetQueryBuilderCapabilities() (*QueryBuilderCapabilities, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}

	provider := r.adapter.GetQueryBuilderProvider()
	if provider == nil {
		return nil, fmt.Errorf("query constructor provider is not available")
	}

	return provider.GetCapabilities(), nil
}

// RegisterScheduledTask 注册定时任务
// 支持按月自动创建表等后台任务，具体实现由各个适配器决定：
//   - PostgreSQL: 使用触发器和 pg_cron 扩展
//   - MySQL: 使用 MySQL EVENT
//   - SQLite/其他: 建议由应用层通过 cron 库处理
func (r *Repository) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return fmt.Errorf("adapter is not initialized")
	}

	if err := task.Validate(); err != nil {
		return fmt.Errorf("invalid task configuration: %w", err)
	}

	return r.adapter.RegisterScheduledTask(ctx, task)
}

// UnregisterScheduledTask 注销定时任务
func (r *Repository) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return fmt.Errorf("adapter is not initialized")
	}

	if taskName == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	return r.adapter.UnregisterScheduledTask(ctx, taskName)
}

// ListScheduledTasks 列出所有已注册的定时任务
func (r *Repository) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}

	return r.adapter.ListScheduledTasks(ctx)
}

// ==================== Query Builder Provider (v0.4.1) ====================

// QueryConstructorProvider 查询构造器提供者接口 - 中层转义层
// 每个 Adapter 实现此接口，提供数据库特定的 QueryConstructor
type QueryConstructorProvider interface {
	// 创建新的查询构造器
	NewQueryConstructor(schema Schema) QueryConstructor

	// 获取此 Adapter 的查询能力声明
	GetCapabilities() *QueryBuilderCapabilities
}

// QueryBuilderCapabilities 查询构造器能力声明
// 声明此 Adapter 的 QueryBuilder 支持哪些操作和优化
type QueryBuilderCapabilities struct {
	// 支持的条件操作
	SupportsEq      bool
	SupportsNe      bool
	SupportsGt      bool
	SupportsLt      bool
	SupportsGte     bool
	SupportsLte     bool
	SupportsIn      bool
	SupportsBetween bool
	SupportsLike    bool
	SupportsAnd     bool
	SupportsOr      bool
	SupportsNot     bool

	// 支持的查询特性
	SupportsSelect   bool // 字段选择
	SupportsOrderBy  bool // 排序
	SupportsLimit    bool // LIMIT
	SupportsOffset   bool // OFFSET
	SupportsJoin     bool // JOIN（关系查询）
	SupportsSubquery bool // 子查询

	// 优化特性
	SupportsQueryPlan bool // 查询计划分析
	SupportsIndex     bool // 索引提示

	// 原生查询支持
	SupportsNativeQuery bool   // 是否支持原生查询（如 Cypher）
	NativeQueryLang     string // 原生查询语言名称（如 "cypher"）

	// 其他标记
	Description string // 此 Adapter 的简要描述
}

// DefaultQueryBuilderCapabilities 返回默认的查询能力（SQL 兼容）
func DefaultQueryBuilderCapabilities() *QueryBuilderCapabilities {
	return &QueryBuilderCapabilities{
		SupportsEq:          true,
		SupportsNe:          true,
		SupportsGt:          true,
		SupportsLt:          true,
		SupportsGte:         true,
		SupportsLte:         true,
		SupportsIn:          true,
		SupportsBetween:     true,
		SupportsLike:        true,
		SupportsAnd:         true,
		SupportsOr:          true,
		SupportsNot:         true,
		SupportsSelect:      true,
		SupportsOrderBy:     true,
		SupportsLimit:       true,
		SupportsOffset:      true,
		SupportsJoin:        true,
		SupportsSubquery:    true,
		SupportsQueryPlan:   true,
		SupportsIndex:       true,
		SupportsNativeQuery: false,
		Description:         "Default SQL Query Builder",
	}
}
