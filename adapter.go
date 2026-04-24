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

	// EnableScheduledTaskFallback 控制定时任务在适配器不支持时是否自动回退到应用层调度器。
	// nil 表示使用默认值（true）。
	EnableScheduledTaskFallback *bool `json:"enable_scheduled_task_fallback,omitempty" yaml:"enable_scheduled_task_fallback,omitempty"`

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
	Redis     *RedisConnectionConfig     `json:"redis,omitempty" yaml:"redis,omitempty"`

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
	Host                     string `json:"host,omitempty" yaml:"host,omitempty"`
	Port                     int    `json:"port,omitempty" yaml:"port,omitempty"`
	Username                 string `json:"username,omitempty" yaml:"username,omitempty"`
	Password                 string `json:"password,omitempty" yaml:"password,omitempty"`
	Database                 string `json:"database,omitempty" yaml:"database,omitempty"`
	DSN                      string `json:"dsn,omitempty" yaml:"dsn,omitempty"`
	ManyToManyStrategy       string `json:"many_to_many_strategy,omitempty" yaml:"many_to_many_strategy,omitempty"`             // "direct_join" | "recursive_cte"
	RecursiveCTEDepth        int    `json:"recursive_cte_depth,omitempty" yaml:"recursive_cte_depth,omitempty"`                 // 默认 8
	RecursiveCTEMaxRecursion int    `json:"recursive_cte_max_recursion,omitempty" yaml:"recursive_cte_max_recursion,omitempty"` // 默认 100
}

// MongoConnectionConfig MongoDB 连接配置。
type MongoConnectionConfig struct {
	URI                  string                `json:"uri,omitempty" yaml:"uri,omitempty"`
	Database             string                `json:"database,omitempty" yaml:"database,omitempty"`
	RelationJoinStrategy string                `json:"relation_join_strategy,omitempty" yaml:"relation_join_strategy,omitempty"` // "lookup" | "pipeline"
	HideThroughArtifacts *bool                 `json:"hide_through_artifacts,omitempty" yaml:"hide_through_artifacts,omitempty"` // 默认 true
	LogSystem            *MongoLogSystemConfig `json:"log_system,omitempty" yaml:"log_system,omitempty"`
}

// MongoLogSystemConfig MongoDB 日志系统特性配置。
type MongoLogSystemConfig struct {
	// 热词提取默认参数
	DefaultTopK        int `json:"default_top_k,omitempty" yaml:"default_top_k,omitempty"`                 // 默认 20
	DefaultMinTokenLen int `json:"default_min_token_len,omitempty" yaml:"default_min_token_len,omitempty"` // 默认 2

	// 停用词配置
	ExtraStopWords          []string `json:"extra_stop_words,omitempty" yaml:"extra_stop_words,omitempty"`
	DisableBuiltinStopWords bool     `json:"disable_builtin_stop_words,omitempty" yaml:"disable_builtin_stop_words,omitempty"`

	// 分词规则：内置规则 "ip" | "url" | "error_code" | "trace_id" | "hashtag"；可追加自定义规则名
	DefaultTokenizationRules []string `json:"default_tokenization_rules,omitempty" yaml:"default_tokenization_rules,omitempty"`

	// 自定义分词规则正则表达式（规则名 -> 正则字符串），优先级高于内置规则
	CustomTokenizationPatterns map[string]string `json:"custom_tokenization_patterns,omitempty" yaml:"custom_tokenization_patterns,omitempty"`

	// 日志字段名称
	DefaultLevelField string `json:"default_level_field,omitempty" yaml:"default_level_field,omitempty"` // 默认 "level"
	DefaultTimeField  string `json:"default_time_field,omitempty" yaml:"default_time_field,omitempty"`   // 默认 "timestamp"

	// 热词持久化集合名（用于 log_hot_words 持久化场景）
	HotWordCollection string `json:"hot_word_collection,omitempty" yaml:"hot_word_collection,omitempty"` // 默认 "eit_log_hot_words"

	// 自定义领域热词词库（词 -> 计数加成），可预置业务关键词
	CustomHotWords map[string]int `json:"custom_hot_words,omitempty" yaml:"custom_hot_words,omitempty"`
}

// RedisConnectionConfig Redis 连接配置。
type RedisConnectionConfig struct {
	// URI 格式: redis://[:password@]host[:port][/db-number]
	// 集群模式: redis://host1:port1,host2:port2,...
	URI      string `json:"uri,omitempty" yaml:"uri,omitempty"`
	Host     string `json:"host,omitempty" yaml:"host,omitempty"`
	Port     int    `json:"port,omitempty" yaml:"port,omitempty"`
	Username string `json:"username,omitempty" yaml:"username,omitempty"` // Redis ACL 用户名 (v6+)
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	DB       int    `json:"db,omitempty" yaml:"db,omitempty"` // Redis 数据库编号 0-15，默认 0

	// TLS
	TLSEnabled bool `json:"tls_enabled,omitempty" yaml:"tls_enabled,omitempty"`

	// 集群模式
	ClusterMode  bool     `json:"cluster_mode,omitempty" yaml:"cluster_mode,omitempty"`
	ClusterAddrs []string `json:"cluster_addrs,omitempty" yaml:"cluster_addrs,omitempty"`

	// 超时（秒），0 使用驱动默认值
	DialTimeout  int `json:"dial_timeout,omitempty" yaml:"dial_timeout,omitempty"`
	ReadTimeout  int `json:"read_timeout,omitempty" yaml:"read_timeout,omitempty"`
	WriteTimeout int `json:"write_timeout,omitempty" yaml:"write_timeout,omitempty"`
}

// Neo4jConnectionConfig Neo4j 连接配置。
type Neo4jConnectionConfig struct {
	URI           string                    `json:"uri,omitempty" yaml:"uri,omitempty"`
	Username      string                    `json:"username,omitempty" yaml:"username,omitempty"`
	Password      string                    `json:"password,omitempty" yaml:"password,omitempty"`
	Database      string                    `json:"database,omitempty" yaml:"database,omitempty"`
	SocialNetwork *Neo4jSocialNetworkConfig `json:"social_network,omitempty" yaml:"social_network,omitempty"`
}

// Neo4jSocialNetworkConfig Neo4j 社交网络特性配置。
type Neo4jSocialNetworkConfig struct {
	// 节点标签（留空则使用默认值）
	UserLabel        string `json:"user_label,omitempty" yaml:"user_label,omitempty"`                 // 默认 "User"
	ChatRoomLabel    string `json:"chat_room_label,omitempty" yaml:"chat_room_label,omitempty"`       // 默认 "ChatRoom"
	ChatMessageLabel string `json:"chat_message_label,omitempty" yaml:"chat_message_label,omitempty"` // 默认 "ChatMessage"
	PostLabel        string `json:"post_label,omitempty" yaml:"post_label,omitempty"`                 // 默认 "Post"
	CommentLabel     string `json:"comment_label,omitempty" yaml:"comment_label,omitempty"`           // 默认 "Comment"
	ForumLabel       string `json:"forum_label,omitempty" yaml:"forum_label,omitempty"`               // 默认 "Forum"
	EmojiLabel       string `json:"emoji_label,omitempty" yaml:"emoji_label,omitempty"`               // 默认 "Emoji"

	// 关系类型（留空则使用默认值）
	FollowsRelType       string `json:"follows_rel_type,omitempty" yaml:"follows_rel_type,omitempty"`               // 默认 "FOLLOWS"
	FriendRelType        string `json:"friend_rel_type,omitempty" yaml:"friend_rel_type,omitempty"`                 // 默认 "FRIEND"
	FriendRequestRelType string `json:"friend_request_rel_type,omitempty" yaml:"friend_request_rel_type,omitempty"` // 默认 "FRIEND_REQUEST"
	SentRelType          string `json:"sent_rel_type,omitempty" yaml:"sent_rel_type,omitempty"`                     // 默认 "SENT"
	MemberOfRelType      string `json:"member_of_rel_type,omitempty" yaml:"member_of_rel_type,omitempty"`           // 默认 "MEMBER_OF"
	InRoomRelType        string `json:"in_room_rel_type,omitempty" yaml:"in_room_rel_type,omitempty"`               // 默认 "IN"
	InRoomMsgRelType     string `json:"in_room_msg_rel_type,omitempty" yaml:"in_room_msg_rel_type,omitempty"`       // 默认 "IN_ROOM"（消息→聊天室）
	MutedInRelType       string `json:"muted_in_rel_type,omitempty" yaml:"muted_in_rel_type,omitempty"`             // 默认 "MUTED_IN"
	BannedInRelType      string `json:"banned_in_rel_type,omitempty" yaml:"banned_in_rel_type,omitempty"`           // 默认 "BANNED_IN"
	ReadByRelType        string `json:"read_by_rel_type,omitempty" yaml:"read_by_rel_type,omitempty"`               // 默认 "READ_BY"
	AuthoredRelType      string `json:"authored_rel_type,omitempty" yaml:"authored_rel_type,omitempty"`             // 默认 "AUTHORED"
	CreatedRelType       string `json:"created_rel_type,omitempty" yaml:"created_rel_type,omitempty"`               // 默认 "CREATED"（聊天室创建者）

	// 全文索引名称
	ChatMessageFulltextIndex string `json:"chat_message_fulltext_index,omitempty" yaml:"chat_message_fulltext_index,omitempty"` // 默认 "chat_message_fulltext"

	// 加入聊天室策略: "request_approval"（需审批，默认）| "open"（直接加入）
	JoinRoomStrategy string `json:"join_room_strategy,omitempty" yaml:"join_room_strategy,omitempty"`

	// 私信权限策略: "mutual_follow_or_friend"（默认）| "friends_only" | "mutual_follow_only" | "open"
	DirectChatPermission string `json:"direct_chat_permission,omitempty" yaml:"direct_chat_permission,omitempty"`

	// 具备 mute/ban 权限的关系类型列表（默认 ["CREATED"]，即仅房间创建者可管理）
	ModerationRelTypes []string `json:"moderation_rel_types,omitempty" yaml:"moderation_rel_types,omitempty"`

	// 成员权限级别定义（从低到高，供业务层查询使用）
	PermissionLevels []string `json:"permission_levels,omitempty" yaml:"permission_levels,omitempty"`
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
	adapterType             string
	startupCapabilityReport *StartupCapabilityReport
	compiledQueryCache      *CompiledQueryCache
	resultCacheBackend      CacheBackend
	scheduledTaskFallbackOn bool
	fallbackTaskManager     *inProcessScheduledTaskManager
	mu                      sync.RWMutex
}

// 全局适配器工厂注册表
var (
	adapterFactories      = make(map[string]AdapterFactory)
	factoriesMutex        sync.RWMutex
	adapterConfigRegistry = make(map[string]*Config)
	configRegistryMutex   sync.RWMutex
)

func registerAdapterFactory(factory AdapterFactory) {
	if factory == nil {
		return
	}
	factoriesMutex.Lock()
	defer factoriesMutex.Unlock()
	adapterFactories[normalizeAdapterName(factory.Name())] = factory
}

// RegisterAdapter 注册适配器工厂。
//
// Deprecated: 这是旧版兼容层，优先使用 RegisterAdapterDescriptor 或
// MustRegisterAdapterDescriptor。该兼容层计划在下一个 minor 版本移除。
func RegisterAdapter(factory AdapterFactory) {
	registerAdapterFactory(factory)
	registerLegacyDescriptorShim(factory)
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

// RegisterAdapterConstructor 使用构造函数动态注册 Adapter。
// 允许的构造函数签名：func(*Config) (Adapter, error) 或 func(*Config) (*T, error)
//
// Deprecated: 这是旧版兼容层，优先使用 RegisterAdapterDescriptor 或
// MustRegisterAdapterDescriptor。该兼容层计划在下一个 minor 版本移除。
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

	repo := &Repository{
		adapter:                 adapter,
		adapterType:             normalizeAdapterName(config.Adapter),
		compiledQueryCache:      NewCompiledQueryCacheWithOptions(cacheSize, time.Duration(cacheTTLSeconds)*time.Second, cacheEnableMetrics),
		scheduledTaskFallbackOn: config.ScheduledTaskFallbackEnabled(),
	}
	rememberAdapterConcreteType(adapter, repo.adapterType)
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

	if r.fallbackTaskManager != nil {
		r.fallbackTaskManager.stop()
		r.fallbackTaskManager = nil
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

// PagedQueryConstructorExecutionResult 表示带分页元信息的统一执行结果。
type PagedQueryConstructorExecutionResult struct {
	Statement     string
	Args          []interface{}
	Rows          []map[string]interface{}
	Total         int64
	Page          int
	PageSize      int
	Offset        int
	TotalPages    int
	HasNext       bool
	HasPrevious   bool
	QueryCacheHit bool
	CountCacheHit bool
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
// 返回 cacheHit=true 表示复用了已缓存的编译结果（SQL 文本），每次执行仍会访问数据库。
//
// Deprecated: 此方法仅缓存编译后的 SQL 文本（L1 Ristretto）而非查询结果。
// 如需缓存实际行数据（命中时完全跳过数据库），请使用 ExecuteQueryConstructorCached。
// 本方法及对应的分页变体（ExecuteQueryConstructorPagedWithCache 等）将在下一个主版本中移除。
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

// ExecuteQueryConstructorPaged 执行分页查询，并返回总数与分页元信息。
func (r *Repository) ExecuteQueryConstructorPaged(ctx context.Context, constructor QueryConstructor, page int, pageSize int) (*PagedQueryConstructorExecutionResult, error) {
	builder := NewPaginationBuilder(page, pageSize).OffsetOnly()
	return r.executeQueryConstructorPaginated(ctx, "", constructor, builder)
}

// ExecuteQueryConstructorPagedWithCache 执行分页查询，并为分页查询与 count 查询接入编译缓存。
//
// Deprecated: 见 ExecuteQueryConstructorWithCache 的说明。将在下一个主版本中移除。
// 替代方案：使用 ExecuteQueryConstructorPagedCached。
func (r *Repository) ExecuteQueryConstructorPagedWithCache(ctx context.Context, cacheKeyPrefix string, constructor QueryConstructor, page int, pageSize int) (*PagedQueryConstructorExecutionResult, error) {
	if strings.TrimSpace(cacheKeyPrefix) == "" {
		return nil, fmt.Errorf("cache key prefix cannot be empty")
	}
	builder := NewPaginationBuilder(page, pageSize).OffsetOnly()
	return r.executeQueryConstructorPaginated(ctx, cacheKeyPrefix, constructor, builder)
}

// ExecuteQueryConstructorPaginated 使用统一分页语义执行查询（offset/cursor/auto）。
func (r *Repository) ExecuteQueryConstructorPaginated(ctx context.Context, constructor QueryConstructor, builder *PaginationBuilder) (*PagedQueryConstructorExecutionResult, error) {
	return r.executeQueryConstructorPaginated(ctx, "", constructor, builder)
}

// ExecuteQueryConstructorPaginatedWithCache 使用统一分页语义执行查询，并为分页查询与 count 查询接入编译缓存。
//
// Deprecated: 见 ExecuteQueryConstructorWithCache 的说明。将在下一个主版本中移除。
// 替代方案：使用 ExecuteQueryConstructorPaginatedCached。
func (r *Repository) ExecuteQueryConstructorPaginatedWithCache(ctx context.Context, cacheKeyPrefix string, constructor QueryConstructor, builder *PaginationBuilder) (*PagedQueryConstructorExecutionResult, error) {
	if strings.TrimSpace(cacheKeyPrefix) == "" {
		return nil, fmt.Errorf("cache key prefix cannot be empty")
	}
	return r.executeQueryConstructorPaginated(ctx, cacheKeyPrefix, constructor, builder)
}

func (r *Repository) executeQueryConstructorPaged(ctx context.Context, cacheKeyPrefix string, constructor QueryConstructor, page int, pageSize int) (*PagedQueryConstructorExecutionResult, error) {
	builder := NewPaginationBuilder(page, pageSize).OffsetOnly()
	return r.executeQueryConstructorPaginated(ctx, cacheKeyPrefix, constructor, builder)
}

func (r *Repository) executeQueryConstructorPaginated(ctx context.Context, cacheKeyPrefix string, constructor QueryConstructor, builder *PaginationBuilder) (*PagedQueryConstructorExecutionResult, error) {
	if constructor == nil {
		return nil, fmt.Errorf("query constructor cannot be nil")
	}

	normalizedBuilder, normalizedPage, normalizedPageSize, offset, cursorMode := normalizePaginationBuilder(builder)
	constructor.Paginate(normalizedBuilder)

	var (
		execResult    *QueryConstructorExecutionResult
		queryCacheHit bool
		err           error
	)

	if strings.TrimSpace(cacheKeyPrefix) != "" {
		modeKey := "offset"
		if cursorMode {
			modeKey = "cursor"
		}
		pageCacheKey := fmt.Sprintf("%s:mode:%s:page:%d:size:%d", cacheKeyPrefix, modeKey, normalizedPage, normalizedPageSize)
		execResult, queryCacheHit, err = r.ExecuteQueryConstructorWithCache(ctx, pageCacheKey, constructor)
	} else {
		execResult, err = r.ExecuteQueryConstructor(ctx, constructor)
	}
	if err != nil {
		return nil, err
	}

	total, countCacheHit, err := r.executeQueryConstructorCount(ctx, cacheKeyPrefix, constructor)
	if err != nil {
		return nil, err
	}

	totalPages := 0
	if normalizedPageSize > 0 && total > 0 {
		totalPages = int((total + int64(normalizedPageSize) - 1) / int64(normalizedPageSize))
	}

	return &PagedQueryConstructorExecutionResult{
		Statement:     execResult.Statement,
		Args:          execResult.Args,
		Rows:          execResult.Rows,
		Total:         total,
		Page:          normalizedPage,
		PageSize:      normalizedPageSize,
		Offset:        offset,
		TotalPages:    totalPages,
		HasNext:       totalPages > 0 && normalizedPage < totalPages,
		HasPrevious:   normalizedPage > 1,
		QueryCacheHit: queryCacheHit,
		CountCacheHit: countCacheHit,
	}, nil
}

func normalizePaginationBuilder(builder *PaginationBuilder) (*PaginationBuilder, int, int, int, bool) {
	if builder == nil {
		page, pageSize, offset := normalizePaginationParams(1, defaultQueryPageSize)
		return NewPaginationBuilder(page, pageSize), page, pageSize, offset, false
	}

	mode := builder.Mode
	if mode == "" {
		mode = PaginationModeAuto
	}

	cursorMode := mode == PaginationModeCursor || (mode == PaginationModeAuto && strings.TrimSpace(builder.CursorField) != "")
	if cursorMode {
		_, pageSize, _ := normalizePaginationParams(1, builder.PageSize)
		normalized := &PaginationBuilder{
			Mode:               PaginationModeCursor,
			Page:               1,
			PageSize:           pageSize,
			CursorField:        strings.TrimSpace(builder.CursorField),
			CursorDirection:    strings.TrimSpace(builder.CursorDirection),
			CursorValue:        builder.CursorValue,
			CursorPrimaryValue: builder.CursorPrimaryValue,
		}
		return normalized, 1, pageSize, 0, true
	}

	page, pageSize, offset := normalizePaginationParams(builder.Page, builder.PageSize)
	normalized := &PaginationBuilder{
		Mode:     PaginationModeOffset,
		Page:     page,
		PageSize: pageSize,
	}
	return normalized, page, pageSize, offset, false
}

type countCachingQueryConstructor interface {
	buildCountConstructor() QueryConstructor
}

func (r *Repository) executeQueryConstructorCount(ctx context.Context, cacheKeyPrefix string, constructor QueryConstructor) (int64, bool, error) {
	if strings.TrimSpace(cacheKeyPrefix) == "" {
		count, err := constructor.SelectCount(ctx, r)
		return count, false, err
	}

	cacheAware, ok := constructor.(countCachingQueryConstructor)
	if !ok {
		count, err := constructor.SelectCount(ctx, r)
		return count, false, err
	}

	countQuery, args, cacheHit, err := r.BuildAndCacheQuery(ctx, cacheKeyPrefix+":count", cacheAware.buildCountConstructor())
	if err != nil {
		return 0, false, err
	}

	count, err := r.executeCompiledCountStatement(ctx, countQuery, args)
	if err != nil {
		return 0, cacheHit, err
	}
	return count, cacheHit, nil
}

func (r *Repository) executeCompiledCountStatement(ctx context.Context, query string, args []interface{}) (int64, error) {
	if !looksLikeReadSQL(query) {
		return 0, fmt.Errorf("count query must compile to SQL SELECT")
	}

	row := r.QueryRow(ctx, query, args...)
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
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
	if descriptor, ok := r.resolveExecutionDescriptor(adapter); ok && descriptor.ExecuteQueryConstructorAuto != nil {
		if routed, handled, hookErr := descriptor.ExecuteQueryConstructorAuto(ctx, adapter, query, args); handled {
			return routed, hookErr
		}
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

	if redisAdapter, ok := adapter.(*RedisAdapter); ok {
		trimmed := strings.TrimSpace(query)
		if strings.HasPrefix(trimmed, redisCompiledCommandPrefix) {
			rows, execSummary, redisErr := redisAdapter.ExecuteCompiledCommandPlan(ctx, query)
			if redisErr != nil {
				return nil, redisErr
			}
			if rows != nil {
				return &QueryConstructorAutoExecutionResult{Mode: "query", Statement: query, Args: copyQueryArgs(args), Rows: rows}, nil
			}
			return &QueryConstructorAutoExecutionResult{Mode: "exec", Statement: query, Args: copyQueryArgs(args), Exec: execSummary}, nil
		}
		if strings.HasPrefix(trimmed, redisCompiledPipelinePrefix) {
			rows, execSummary, redisErr := redisAdapter.ExecuteCompiledPipelinePlan(ctx, query)
			if redisErr != nil {
				return nil, redisErr
			}
			if rows != nil {
				return &QueryConstructorAutoExecutionResult{Mode: "query", Statement: query, Args: copyQueryArgs(args), Rows: rows}, nil
			}
			return &QueryConstructorAutoExecutionResult{Mode: "exec", Statement: query, Args: copyQueryArgs(args), Exec: execSummary}, nil
		}
		return nil, fmt.Errorf("redis query constructor requires compiled plan prefix %q or %q", redisCompiledCommandPrefix, redisCompiledPipelinePrefix)
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
	if descriptor, ok := r.resolveExecutionDescriptor(adapter); ok && descriptor.ExecuteQueryConstructor != nil {
		if routed, handled, hookErr := descriptor.ExecuteQueryConstructor(ctx, adapter, query, args); handled {
			return routed, hookErr
		}
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

	if redisAdapter, ok := adapter.(*RedisAdapter); ok {
		trimmed := strings.TrimSpace(query)
		if strings.HasPrefix(trimmed, redisCompiledCommandPrefix) {
			rows, execSummary, redisErr := redisAdapter.ExecuteCompiledCommandPlan(ctx, query)
			if redisErr != nil {
				return nil, redisErr
			}
			if execSummary != nil {
				return nil, fmt.Errorf("ExecuteQueryConstructor is query-only for redis write plans; use ExecuteQueryConstructorAuto")
			}
			return &QueryConstructorExecutionResult{Statement: query, Args: copyQueryArgs(args), Rows: rows}, nil
		}
		if strings.HasPrefix(trimmed, redisCompiledPipelinePrefix) {
			rows, execSummary, redisErr := redisAdapter.ExecuteCompiledPipelinePlan(ctx, query)
			if redisErr != nil {
				return nil, redisErr
			}
			if execSummary != nil {
				return nil, fmt.Errorf("ExecuteQueryConstructor is query-only for redis write pipeline plans; use ExecuteQueryConstructorAuto")
			}
			return &QueryConstructorExecutionResult{Statement: query, Args: copyQueryArgs(args), Rows: rows}, nil
		}
		return nil, fmt.Errorf("redis query constructor requires compiled plan prefix %q", redisCompiledCommandPrefix)
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

func (r *Repository) resolveExecutionDescriptor(adapter Adapter) (AdapterDescriptor, bool) {
	if descriptor, ok := LookupAdapterDescriptor(r.adapterType); ok {
		return descriptor, true
	}
	metadata := r.resolveAdapterMetadata(adapter)
	return LookupAdapterDescriptor(metadata.Name)
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
	adapter := r.adapter
	r.mu.RUnlock()

	if adapter == nil {
		return fmt.Errorf("adapter is not initialized")
	}

	if err := task.Validate(); err != nil {
		return fmt.Errorf("invalid task configuration: %w", err)
	}

	err := adapter.RegisterScheduledTask(ctx, task)
	if err == nil {
		return nil
	}
	if !r.scheduledTaskFallbackOn {
		return err
	}
	if !shouldUseScheduledTaskFallback(err) {
		return err
	}

	manager := r.getOrCreateFallbackTaskManager()
	if manager == nil {
		return err
	}
	if fallbackErr := manager.register(ctx, task); fallbackErr != nil {
		return fallbackErr
	}
	return nil
}

// UnregisterScheduledTask 注销定时任务
func (r *Repository) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	r.mu.RLock()
	adapter := r.adapter
	r.mu.RUnlock()

	if adapter == nil {
		return fmt.Errorf("adapter is not initialized")
	}

	if taskName == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	err := adapter.UnregisterScheduledTask(ctx, taskName)
	if err == nil {
		return nil
	}
	if !r.scheduledTaskFallbackOn {
		return err
	}
	if !shouldUseScheduledTaskFallback(err) {
		return err
	}

	manager := r.getOrCreateFallbackTaskManager()
	if manager == nil {
		return err
	}
	return manager.unregister(ctx, taskName)
}

// ListScheduledTasks 列出所有已注册的定时任务
func (r *Repository) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	r.mu.RLock()
	adapter := r.adapter
	manager := r.fallbackTaskManager
	r.mu.RUnlock()

	if adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}

	tasks, err := adapter.ListScheduledTasks(ctx)
	if err == nil {
		if manager == nil {
			return tasks, nil
		}
		fallbackTasks, listErr := manager.list(ctx)
		if listErr != nil {
			return tasks, nil
		}
		return append(tasks, fallbackTasks...), nil
	}
	if !r.scheduledTaskFallbackOn {
		return nil, err
	}
	if !shouldUseScheduledTaskFallback(err) {
		return nil, err
	}
	manager = r.getOrCreateFallbackTaskManager()
	if manager == nil {
		return nil, err
	}
	return manager.list(ctx)
}

func (r *Repository) getAdapterUnsafe() Adapter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.adapter
}

func (r *Repository) getOrCreateFallbackTaskManager() *inProcessScheduledTaskManager {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fallbackTaskManager != nil {
		return r.fallbackTaskManager
	}
	manager := newInProcessScheduledTaskManager(r, &repositoryScheduledTaskExecutor{repo: r})
	manager.start()
	r.fallbackTaskManager = manager
	return manager
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
