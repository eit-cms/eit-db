package db

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// QueryConstructorExecuteHook 允许适配器通过注册机制自定义 QueryConstructor 查询路由。
// handled=true 表示该 hook 已消费本次请求；handled=false 表示交给框架默认路由。
type QueryConstructorExecuteHook func(ctx context.Context, adapter Adapter, query string, args []interface{}) (result *QueryConstructorExecutionResult, handled bool, err error)

// QueryConstructorAutoExecuteHook 允许适配器通过注册机制自定义 QueryConstructor 自动路由。
// handled=true 表示该 hook 已消费本次请求；handled=false 表示交给框架默认路由。
type QueryConstructorAutoExecuteHook func(ctx context.Context, adapter Adapter, query string, args []interface{}) (result *QueryConstructorAutoExecutionResult, handled bool, err error)

// AdapterDescriptor 完整描述一个可注册的数据库适配器。
//
// 通过 RegisterAdapterDescriptor 一次调用即可完成工厂创建、配置校验、默认配置
// 三项注册，无需分散修改框架内部代码。
//
// # 自定义适配器示例
//
//	func init() {
//	    db.MustRegisterAdapterDescriptor("mydb", db.AdapterDescriptor{
//	        Factory: func(cfg *db.Config) (db.Adapter, error) {
//	            a, err := NewMyDBAdapter(cfg)
//	            if err != nil {
//	                return nil, err
//	            }
//	            if err := a.Connect(context.Background(), cfg); err != nil {
//	                return nil, err
//	            }
//	            return a, nil
//	        },
//	        ValidateConfig: func(cfg *db.Config) error {
//	            endpoint, _ := cfg.Options["endpoint"].(string)
//	            if strings.TrimSpace(endpoint) == "" {
//	                return fmt.Errorf("mydb: options.endpoint is required")
//	            }
//	            return nil
//	        },
//	        DefaultConfig: func() *db.Config {
//	            return &db.Config{
//	                Adapter: "mydb",
//	                Options: map[string]interface{}{
//	                    "endpoint": "localhost:9999",
//	                },
//	            }
//	        },
//	    })
//	}
//
// 注册后，以下框架功能自动生效：
//   - db.NewRepository(cfg) 通过 Factory 创建实例
//   - (*Config).Validate() 通过 ValidateConfig 校验专属配置
//   - db.DefaultConfig("mydb") 通过 DefaultConfig 返回默认配置
type AdapterDescriptor struct {
	// Factory 创建并返回已连接的适配器实例（必须）。
	// 接收已通过 ValidateConfig 验证的 *Config。
	// 实现者应在此函数内完成 Connect，并在失败时关闭资源。
	Factory func(config *Config) (Adapter, error)

	// ValidateConfig 验证该适配器的专属配置（可选）。
	// 在 (*Config).Validate() 中被调用，仅检查适配器特有字段。
	// 通用字段（Pool、QueryCache、Validation）由框架统一验证，无需在此重复。
	// 返回 nil 表示配置合法；返回 error 将中止 Validate()。
	ValidateConfig func(cfg *Config) error

	// DefaultConfig 返回该适配器的开箱即用默认配置（可选）。
	// 在 db.DefaultConfig(adapterType) 中被调用。
	// 若未设置，DefaultConfig 将返回仅含 Adapter 字段和标准连接池的最简配置。
	DefaultConfig func() *Config

	// Metadata 返回适配器的元信息（可选）。
	// 建议所有官方/第三方适配器设置该字段，避免运行期类型推断。
	Metadata func() AdapterMetadata

	// ExecuteQueryConstructor 允许适配器覆写 QueryConstructor 查询执行逻辑（可选）。
	// 常用于非 SQL 适配器（如 Mongo/Neo4j/Redis）接入自定义编译前缀与结果映射。
	ExecuteQueryConstructor QueryConstructorExecuteHook

	// ExecuteQueryConstructorAuto 允许适配器覆写 QueryConstructor 自动路由执行逻辑（可选）。
	// 返回 handled=false 时，框架将回退到默认 SQL/内建适配器路由。
	ExecuteQueryConstructorAuto QueryConstructorAutoExecuteHook
}

// 描述符注册表
var (
	descriptorRegistry = make(map[string]AdapterDescriptor)
	descriptorMu       sync.RWMutex
)

// RegisterAdapterDescriptor 注册一个完整的适配器描述符。
//
// 一次调用即可完成工厂、配置校验、默认配置的全量注册，同时兼容旧版
// RegisterAdapter / NewRepository 调用路径（内部自动包装为 AdapterFactory）。
//
// name 不区分大小写，内部统一转为小写存储。
// 若 Factory 为 nil，返回 error。
func RegisterAdapterDescriptor(name string, desc AdapterDescriptor) error {
	name = normalizeAdapterName(name)
	if name == "" {
		return fmt.Errorf("adapter name cannot be empty")
	}
	if desc.Factory == nil {
		return fmt.Errorf("adapter %q: Factory must not be nil", name)
	}

	storeAdapterDescriptor(name, desc, true)

	// 同时注册到旧版工厂注册表，保持 NewRepository 的向后兼容
	registerAdapterFactory(&descriptorAdapterFactory{name: name, desc: desc})

	return nil
}

// MustRegisterAdapterDescriptor 是 RegisterAdapterDescriptor 的 panic 版本。
// 适合在 init() 函数中使用——配置错误应在启动期暴露而非运行期。
func MustRegisterAdapterDescriptor(name string, desc AdapterDescriptor) {
	if err := RegisterAdapterDescriptor(name, desc); err != nil {
		panic(fmt.Sprintf("eit-db: failed to register adapter descriptor %q: %v", name, err))
	}
}

// LookupAdapterDescriptor 返回已注册的适配器描述符。
// 第二个返回值表示是否找到。
func LookupAdapterDescriptor(name string) (AdapterDescriptor, bool) {
	descriptorMu.RLock()
	defer descriptorMu.RUnlock()
	desc, ok := descriptorRegistry[normalizeAdapterName(name)]
	return desc, ok
}

// ListRegisteredAdapters 返回所有已通过描述符注册的适配器名称列表（有序）。
func ListRegisteredAdapters() []string {
	descriptorMu.RLock()
	defer descriptorMu.RUnlock()
	names := make([]string, 0, len(descriptorRegistry))
	for name := range descriptorRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func normalizeAdapterName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func storeAdapterDescriptor(name string, desc AdapterDescriptor, overwrite bool) {
	descriptorMu.Lock()
	defer descriptorMu.Unlock()
	if !overwrite {
		if _, exists := descriptorRegistry[name]; exists {
			return
		}
	}
	descriptorRegistry[name] = desc
}

func registerLegacyDescriptorShim(factory AdapterFactory) {
	if factory == nil {
		return
	}
	name := normalizeAdapterName(factory.Name())
	if name == "" {
		return
	}
	storeAdapterDescriptor(name, AdapterDescriptor{
		Factory: func(config *Config) (Adapter, error) {
			return factory.Create(config)
		},
	}, false)
}

func newDefaultAdapterConfig(adapter string) *Config {
	return &Config{
		Adapter: adapter,
		Pool: &PoolConfig{
			MaxConnections: 25,
			MinConnections: 0,
			ConnectTimeout: 30,
			IdleTimeout:    300,
		},
	}
}

func validateBasicSQLConnection(adapter, host, username, database, dsn string) error {
	if strings.TrimSpace(dsn) != "" {
		return nil
	}
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("%s: host must be specified", adapter)
	}
	if strings.TrimSpace(username) == "" {
		return fmt.Errorf("%s: username must be specified", adapter)
	}
	if strings.TrimSpace(database) == "" {
		return fmt.Errorf("%s: database name must be specified", adapter)
	}
	return nil
}

func builtinAdapterMetadata(name, driverKind, vendor string, aliases ...string) AdapterMetadata {
	return AdapterMetadata{
		Name:       name,
		DriverKind: driverKind,
		Vendor:     vendor,
		Aliases:    aliases,
	}
}

func newSQLiteAdapterDescriptor() AdapterDescriptor {
	return AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) {
			return NewSQLiteAdapter(cfg)
		},
		ValidateConfig: func(cfg *Config) error {
			sqliteCfg := cfg.ResolvedSQLiteConfig()
			cfg.SQLite = sqliteCfg
			if strings.TrimSpace(sqliteCfg.Path) == "" && strings.TrimSpace(sqliteCfg.DSN) == "" {
				return fmt.Errorf("sqlite: database path must be specified")
			}
			return nil
		},
		DefaultConfig: func() *Config {
			cfg := newDefaultAdapterConfig("sqlite")
			cfg.SQLite = &SQLiteConnectionConfig{Path: "./eit.db"}
			return cfg
		},
		Metadata: func() AdapterMetadata {
			return builtinAdapterMetadata("sqlite", "sql", "sqlite")
		},
	}
}

func newPostgresAdapterDescriptor() AdapterDescriptor {
	return AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) {
			return (&PostgreSQLFactory{}).Create(cfg)
		},
		ValidateConfig: func(cfg *Config) error {
			postgresCfg := cfg.ResolvedPostgresConfig()
			cfg.Postgres = postgresCfg
			return validateBasicSQLConnection("postgres", postgresCfg.Host, postgresCfg.Username, postgresCfg.Database, postgresCfg.DSN)
		},
		DefaultConfig: func() *Config {
			cfg := newDefaultAdapterConfig("postgres")
			cfg.Postgres = &PostgresConnectionConfig{Host: "localhost", Port: 5432, Database: "eit", Username: "postgres", Password: "postgres", SSLMode: "disable"}
			return cfg
		},
		Metadata: func() AdapterMetadata {
			return builtinAdapterMetadata("postgres", "sql", "postgresql", "postgresql")
		},
	}
}

func newMySQLAdapterDescriptor() AdapterDescriptor {
	return AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) {
			return (&MySQLFactory{}).Create(cfg)
		},
		ValidateConfig: func(cfg *Config) error {
			mysqlCfg := cfg.ResolvedMySQLConfig()
			cfg.MySQL = mysqlCfg
			return validateBasicSQLConnection("mysql", mysqlCfg.Host, mysqlCfg.Username, mysqlCfg.Database, mysqlCfg.DSN)
		},
		DefaultConfig: func() *Config {
			cfg := newDefaultAdapterConfig("mysql")
			cfg.MySQL = &MySQLConnectionConfig{Host: "localhost", Port: 3306, Database: "eit", Username: "root", Password: "root"}
			return cfg
		},
		Metadata: func() AdapterMetadata {
			return builtinAdapterMetadata("mysql", "sql", "mysql")
		},
	}
}

func newSQLServerAdapterDescriptor() AdapterDescriptor {
	return AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) {
			return (&SQLServerFactory{}).Create(cfg)
		},
		ValidateConfig: func(cfg *Config) error {
			sqlServerCfg := cfg.ResolvedSQLServerConfig()
			cfg.SQLServer = sqlServerCfg
			if err := validateBasicSQLConnection("sqlserver", sqlServerCfg.Host, sqlServerCfg.Username, sqlServerCfg.Database, sqlServerCfg.DSN); err != nil {
				return err
			}
			if sqlServerCfg.ManyToManyStrategy != "direct_join" && sqlServerCfg.ManyToManyStrategy != "recursive_cte" {
				return fmt.Errorf("sqlserver: many_to_many_strategy must be direct_join or recursive_cte")
			}
			if sqlServerCfg.RecursiveCTEDepth <= 0 {
				return fmt.Errorf("sqlserver: recursive_cte_depth must be greater than 0")
			}
			if sqlServerCfg.RecursiveCTEMaxRecursion <= 0 {
				return fmt.Errorf("sqlserver: recursive_cte_max_recursion must be greater than 0")
			}
			return nil
		},
		DefaultConfig: func() *Config {
			cfg := newDefaultAdapterConfig("sqlserver")
			cfg.SQLServer = &SQLServerConnectionConfig{Host: "localhost", Port: 1433, Database: "master", Username: "sa", Password: "YourStrong!Passw0rd"}
			return cfg
		},
		Metadata: func() AdapterMetadata {
			return builtinAdapterMetadata("sqlserver", "sql", "microsoft", "mssql")
		},
	}
}

func newMongoAdapterDescriptor() AdapterDescriptor {
	return AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) {
			return NewMongoAdapter(cfg)
		},
		ValidateConfig: func(cfg *Config) error {
			mongoCfg := cfg.ResolvedMongoConfig()
			cfg.MongoDB = mongoCfg
			if strings.TrimSpace(mongoCfg.Database) == "" {
				return fmt.Errorf("mongodb: database name must be specified")
			}
			if strings.TrimSpace(mongoCfg.URI) == "" {
				return fmt.Errorf("mongodb: uri must be specified")
			}
			if mongoCfg.RelationJoinStrategy != "lookup" && mongoCfg.RelationJoinStrategy != "pipeline" {
				return fmt.Errorf("mongodb: relation_join_strategy must be lookup or pipeline")
			}
			return nil
		},
		DefaultConfig: func() *Config {
			cfg := newDefaultAdapterConfig("mongodb")
			cfg.MongoDB = &MongoConnectionConfig{URI: "mongodb://localhost:27017", Database: "eit"}
			return cfg
		},
		Metadata: func() AdapterMetadata {
			return builtinAdapterMetadata("mongodb", "document", "mongodb", "mongo")
		},
		ExecuteQueryConstructor: func(ctx context.Context, adapter Adapter, query string, args []interface{}) (*QueryConstructorExecutionResult, bool, error) {
			mongoAdapter, ok := adapter.(*MongoAdapter)
			if !ok {
				return nil, false, nil
			}
			trimmed := strings.TrimSpace(query)
			if strings.HasPrefix(trimmed, mongoCompiledQueryPrefix) {
				rows, queryErr := mongoAdapter.ExecuteCompiledFindPlan(ctx, query)
				if queryErr != nil {
					return nil, true, queryErr
				}
				return &QueryConstructorExecutionResult{Statement: query, Args: copyQueryArgs(args), Rows: rows}, true, nil
			}
			if strings.HasPrefix(trimmed, mongoCompiledWritePrefix) {
				return nil, true, fmt.Errorf("ExecuteQueryConstructor is query-only for mongodb write plans; use ExecuteQueryConstructorAuto")
			}
			return nil, true, fmt.Errorf("mongodb query constructor requires compiled plan prefix %q", mongoCompiledQueryPrefix)
		},
		ExecuteQueryConstructorAuto: func(ctx context.Context, adapter Adapter, query string, args []interface{}) (*QueryConstructorAutoExecutionResult, bool, error) {
			mongoAdapter, ok := adapter.(*MongoAdapter)
			if !ok {
				return nil, false, nil
			}
			trimmed := strings.TrimSpace(query)
			if strings.HasPrefix(trimmed, mongoCompiledQueryPrefix) {
				rows, queryErr := mongoAdapter.ExecuteCompiledFindPlan(ctx, query)
				if queryErr != nil {
					return nil, true, queryErr
				}
				return &QueryConstructorAutoExecutionResult{Mode: "query", Statement: query, Args: copyQueryArgs(args), Rows: rows}, true, nil
			}
			if strings.HasPrefix(trimmed, mongoCompiledWritePrefix) {
				summary, execErr := mongoAdapter.ExecuteCompiledWritePlan(ctx, query)
				if execErr != nil {
					return nil, true, execErr
				}
				return &QueryConstructorAutoExecutionResult{Mode: "exec", Statement: query, Args: copyQueryArgs(args), Exec: summary}, true, nil
			}
			return nil, true, fmt.Errorf("mongodb query constructor requires compiled plan prefix %q or %q", mongoCompiledQueryPrefix, mongoCompiledWritePrefix)
		},
	}
}

func newNeo4jAdapterDescriptor() AdapterDescriptor {
	return AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) {
			return NewNeo4jAdapter(cfg)
		},
		ValidateConfig: func(cfg *Config) error {
			neo4jCfg := cfg.ResolvedNeo4jConfig()
			cfg.Neo4j = neo4jCfg
			if strings.TrimSpace(neo4jCfg.URI) == "" {
				return fmt.Errorf("neo4j: uri must be specified")
			}
			if strings.TrimSpace(neo4jCfg.Username) == "" {
				return fmt.Errorf("neo4j: username must be specified")
			}
			return nil
		},
		DefaultConfig: func() *Config {
			cfg := newDefaultAdapterConfig("neo4j")
			cfg.Neo4j = &Neo4jConnectionConfig{URI: "neo4j://localhost:7687", Username: "neo4j", Password: "neo4j", Database: "neo4j"}
			return cfg
		},
		Metadata: func() AdapterMetadata {
			return builtinAdapterMetadata("neo4j", "graph", "neo4j")
		},
		ExecuteQueryConstructor: func(ctx context.Context, adapter Adapter, query string, args []interface{}) (*QueryConstructorExecutionResult, bool, error) {
			neo, ok := adapter.(*Neo4jAdapter)
			if !ok {
				return nil, false, nil
			}
			rows, queryErr := neo.QueryCypher(ctx, query, buildCypherParamsFromArgs(args))
			if queryErr != nil {
				return nil, true, queryErr
			}
			return &QueryConstructorExecutionResult{Statement: query, Args: copyQueryArgs(args), Rows: rows}, true, nil
		},
		ExecuteQueryConstructorAuto: func(ctx context.Context, adapter Adapter, query string, args []interface{}) (*QueryConstructorAutoExecutionResult, bool, error) {
			neo, ok := adapter.(*Neo4jAdapter)
			if !ok {
				return nil, false, nil
			}
			if looksLikeReadCypher(query) {
				rows, queryErr := neo.QueryCypher(ctx, query, buildCypherParamsFromArgs(args))
				if queryErr != nil {
					return nil, true, queryErr
				}
				return &QueryConstructorAutoExecutionResult{Mode: "query", Statement: query, Args: copyQueryArgs(args), Rows: rows}, true, nil
			}

			summary, execErr := neo.ExecCypher(ctx, query, buildCypherParamsFromArgs(args))
			if execErr != nil {
				return nil, true, execErr
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
			}, true, nil
		},
	}
}

// descriptorAdapterFactory 将 AdapterDescriptor 包装为 AdapterFactory 接口，
// 保持与旧版 RegisterAdapter / NewRepository 路径的向后兼容。
type descriptorAdapterFactory struct {
	name string
	desc AdapterDescriptor
}

func (f *descriptorAdapterFactory) Name() string { return f.name }

func (f *descriptorAdapterFactory) Create(config *Config) (Adapter, error) {
	return f.desc.Factory(config)
}
