package db

import (
	"fmt"
	"strings"
)

// ConfigOption 是构建 Config 的函数式选项。
// 通过 NewConfig 或 MustConfig 应用，各选项按注册顺序依次执行。
type ConfigOption func(*Config)

// NewConfig 通过函数式选项创建并验证 Config。
//
// adapter 可选值: "postgres" | "mysql" | "sqlserver" | "sqlite" | "mongodb" | "neo4j"
//
// 每个选项直接写入对应适配器的专属子配置（如 cfg.Postgres.Host），无需关心
// 平铺字段或 Options map。Validate() 在内部调用，返回 error 时配置无效。
//
// 示例:
//
//	cfg, err := db.NewConfig("postgres",
//	    db.WithHost("localhost"),
//	    db.WithDatabase("myapp"),
//	    db.WithUsername("admin"),
//	    db.WithPassword("secret"),
//	)
//
//	cfg, err := db.NewConfig("neo4j",
//	    db.WithURI("neo4j://localhost:7687"),
//	    db.WithPassword("neo4j"),
//	)
//
//	cfg, err := db.NewConfig("sqlite",
//	    db.WithSQLitePath("/data/app.db"),
//	)
func NewConfig(adapter string, opts ...ConfigOption) (*Config, error) {
	cfg := &Config{Adapter: strings.ToLower(strings.TrimSpace(adapter))}
	for _, opt := range opts {
		opt(cfg)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// MustConfig 是 NewConfig 的 panic 版本，适用于静态初始化、测试或
// 已知配置有效的场合。配置无效时触发 panic。
func MustConfig(adapter string, opts ...ConfigOption) *Config {
	cfg, err := NewConfig(adapter, opts...)
	if err != nil {
		panic(fmt.Sprintf("eit-db: invalid config for adapter %q: %v", adapter, err))
	}
	return cfg
}

// ── 通用连接参数 ────────────────────────────────────────────────────────────

// WithHost 设置数据库主机地址，适用于 postgres、mysql、sqlserver。
func WithHost(host string) ConfigOption {
	return func(cfg *Config) {
		switch cfg.Adapter {
		case "postgres":
			if cfg.Postgres == nil {
				cfg.Postgres = &PostgresConnectionConfig{}
			}
			cfg.Postgres.Host = host
		case "mysql":
			if cfg.MySQL == nil {
				cfg.MySQL = &MySQLConnectionConfig{}
			}
			cfg.MySQL.Host = host
		case "sqlserver":
			if cfg.SQLServer == nil {
				cfg.SQLServer = &SQLServerConnectionConfig{}
			}
			cfg.SQLServer.Host = host
		default:
			cfg.Host = host
		}
	}
}

// WithPort 设置数据库端口，适用于 postgres、mysql、sqlserver。
func WithPort(port int) ConfigOption {
	return func(cfg *Config) {
		switch cfg.Adapter {
		case "postgres":
			if cfg.Postgres == nil {
				cfg.Postgres = &PostgresConnectionConfig{}
			}
			cfg.Postgres.Port = port
		case "mysql":
			if cfg.MySQL == nil {
				cfg.MySQL = &MySQLConnectionConfig{}
			}
			cfg.MySQL.Port = port
		case "sqlserver":
			if cfg.SQLServer == nil {
				cfg.SQLServer = &SQLServerConnectionConfig{}
			}
			cfg.SQLServer.Port = port
		default:
			cfg.Port = port
		}
	}
}

// WithDatabase 设置数据库名称，适用于所有适配器。
// 对于 SQLite 请使用 WithSQLitePath 设置文件路径。
func WithDatabase(database string) ConfigOption {
	return func(cfg *Config) {
		switch cfg.Adapter {
		case "postgres":
			if cfg.Postgres == nil {
				cfg.Postgres = &PostgresConnectionConfig{}
			}
			cfg.Postgres.Database = database
		case "mysql":
			if cfg.MySQL == nil {
				cfg.MySQL = &MySQLConnectionConfig{}
			}
			cfg.MySQL.Database = database
		case "sqlserver":
			if cfg.SQLServer == nil {
				cfg.SQLServer = &SQLServerConnectionConfig{}
			}
			cfg.SQLServer.Database = database
		case "mongodb":
			if cfg.MongoDB == nil {
				cfg.MongoDB = &MongoConnectionConfig{}
			}
			cfg.MongoDB.Database = database
		case "neo4j":
			if cfg.Neo4j == nil {
				cfg.Neo4j = &Neo4jConnectionConfig{}
			}
			cfg.Neo4j.Database = database
		default:
			cfg.Database = database
		}
	}
}

// WithUsername 设置数据库连接用户名，适用于 postgres、mysql、sqlserver、neo4j。
func WithUsername(username string) ConfigOption {
	return func(cfg *Config) {
		switch cfg.Adapter {
		case "postgres":
			if cfg.Postgres == nil {
				cfg.Postgres = &PostgresConnectionConfig{}
			}
			cfg.Postgres.Username = username
		case "mysql":
			if cfg.MySQL == nil {
				cfg.MySQL = &MySQLConnectionConfig{}
			}
			cfg.MySQL.Username = username
		case "sqlserver":
			if cfg.SQLServer == nil {
				cfg.SQLServer = &SQLServerConnectionConfig{}
			}
			cfg.SQLServer.Username = username
		case "neo4j":
			if cfg.Neo4j == nil {
				cfg.Neo4j = &Neo4jConnectionConfig{}
			}
			cfg.Neo4j.Username = username
		default:
			cfg.Username = username
		}
	}
}

// WithPassword 设置数据库连接密码，适用于 postgres、mysql、sqlserver、neo4j。
func WithPassword(password string) ConfigOption {
	return func(cfg *Config) {
		switch cfg.Adapter {
		case "postgres":
			if cfg.Postgres == nil {
				cfg.Postgres = &PostgresConnectionConfig{}
			}
			cfg.Postgres.Password = password
		case "mysql":
			if cfg.MySQL == nil {
				cfg.MySQL = &MySQLConnectionConfig{}
			}
			cfg.MySQL.Password = password
		case "sqlserver":
			if cfg.SQLServer == nil {
				cfg.SQLServer = &SQLServerConnectionConfig{}
			}
			cfg.SQLServer.Password = password
		case "neo4j":
			if cfg.Neo4j == nil {
				cfg.Neo4j = &Neo4jConnectionConfig{}
			}
			cfg.Neo4j.Password = password
		default:
			cfg.Password = password
		}
	}
}

// WithDSN 通过 DSN 字符串直接配置 SQL 类适配器（postgres/mysql/sqlserver/sqlite）。
// 使用 DSN 时，WithHost / WithPort / WithUsername / WithPassword / WithDatabase 均可省略。
// 对于 MongoDB 和 Neo4j，请使用 WithURI。
func WithDSN(dsn string) ConfigOption {
	return func(cfg *Config) {
		switch cfg.Adapter {
		case "postgres":
			if cfg.Postgres == nil {
				cfg.Postgres = &PostgresConnectionConfig{}
			}
			cfg.Postgres.DSN = dsn
		case "mysql":
			if cfg.MySQL == nil {
				cfg.MySQL = &MySQLConnectionConfig{}
			}
			cfg.MySQL.DSN = dsn
		case "sqlserver":
			if cfg.SQLServer == nil {
				cfg.SQLServer = &SQLServerConnectionConfig{}
			}
			cfg.SQLServer.DSN = dsn
		case "sqlite":
			if cfg.SQLite == nil {
				cfg.SQLite = &SQLiteConnectionConfig{}
			}
			cfg.SQLite.DSN = dsn
		}
	}
}

// WithURI 通过 URI 字符串配置 MongoDB 或 Neo4j 适配器。
// 使用 URI 时，WithHost / WithPort / WithUsername / WithPassword 均可省略。
//
// MongoDB 示例: "mongodb://localhost:27017" 或 "mongodb+srv://..."
// Neo4j 示例: "neo4j://localhost:7687" 或 "neo4j+ssc://..."
func WithURI(uri string) ConfigOption {
	return func(cfg *Config) {
		switch cfg.Adapter {
		case "mongodb":
			if cfg.MongoDB == nil {
				cfg.MongoDB = &MongoConnectionConfig{}
			}
			cfg.MongoDB.URI = uri
		case "neo4j":
			if cfg.Neo4j == nil {
				cfg.Neo4j = &Neo4jConnectionConfig{}
			}
			cfg.Neo4j.URI = uri
		}
	}
}

// ── 适配器专属配置 ──────────────────────────────────────────────────────────

// WithSSLMode 设置 PostgreSQL 的 SSL 连接模式。
// 可选值: "disable"（默认）| "require" | "verify-ca" | "verify-full"
func WithSSLMode(mode string) ConfigOption {
	return func(cfg *Config) {
		if cfg.Postgres == nil {
			cfg.Postgres = &PostgresConnectionConfig{}
		}
		cfg.Postgres.SSLMode = mode
	}
}

// WithSQLitePath 设置 SQLite 数据库文件路径。默认值: "./eit.db"
func WithSQLitePath(path string) ConfigOption {
	return func(cfg *Config) {
		if cfg.SQLite == nil {
			cfg.SQLite = &SQLiteConnectionConfig{}
		}
		cfg.SQLite.Path = path
	}
}

// WithSQLServerManyToMany 设置 SQL Server 的多对多关系查询策略。
//   - strategy: "direct_join"（默认）| "recursive_cte"
//   - recursiveCTEDepth: recursive_cte 模式下的递归深度，传 0 使用默认值 8
//   - maxRecursion: 最大递归次数，传 0 使用默认值 100
func WithSQLServerManyToMany(strategy string, recursiveCTEDepth, maxRecursion int) ConfigOption {
	return func(cfg *Config) {
		if cfg.SQLServer == nil {
			cfg.SQLServer = &SQLServerConnectionConfig{}
		}
		cfg.SQLServer.ManyToManyStrategy = strategy
		if recursiveCTEDepth > 0 {
			cfg.SQLServer.RecursiveCTEDepth = recursiveCTEDepth
		}
		if maxRecursion > 0 {
			cfg.SQLServer.RecursiveCTEMaxRecursion = maxRecursion
		}
	}
}

// WithMongoRelationJoinStrategy 设置 MongoDB 关联查询策略。
// 可选值: "lookup"（默认）| "pipeline"
func WithMongoRelationJoinStrategy(strategy string) ConfigOption {
	return func(cfg *Config) {
		if cfg.MongoDB == nil {
			cfg.MongoDB = &MongoConnectionConfig{}
		}
		cfg.MongoDB.RelationJoinStrategy = strategy
	}
}

// WithMongoHideThroughArtifacts 控制 MongoDB 多对多查询是否隐藏中间关联字段。
// 默认 true。
func WithMongoHideThroughArtifacts(hide bool) ConfigOption {
	return func(cfg *Config) {
		if cfg.MongoDB == nil {
			cfg.MongoDB = &MongoConnectionConfig{}
		}
		cfg.MongoDB.HideThroughArtifacts = &hide
	}
}

// WithNeo4jSocialNetwork 设置 Neo4j 社交网络特性配置。
// 所有字段均有合理默认值，仅在需要自定义节点标签或关系类型时使用。
func WithNeo4jSocialNetwork(social *Neo4jSocialNetworkConfig) ConfigOption {
	return func(cfg *Config) {
		if cfg.Neo4j == nil {
			cfg.Neo4j = &Neo4jConnectionConfig{}
		}
		cfg.Neo4j.SocialNetwork = social
	}
}

// ── 通用框架配置 ────────────────────────────────────────────────────────────

// WithPool 设置数据库连接池配置。
func WithPool(pool *PoolConfig) ConfigOption {
	return func(cfg *Config) {
		cfg.Pool = pool
	}
}

// WithQueryCache 设置 Repository 级查询编译缓存配置。
func WithQueryCache(cache *QueryCacheConfig) ConfigOption {
	return func(cfg *Config) {
		cfg.QueryCache = cache
	}
}

// WithScheduledTaskFallback 控制定时任务在适配器不支持时是否自动回退到应用层调度器。
// 默认 true；传 false 可完全禁用 fallback。
func WithScheduledTaskFallback(enabled bool) ConfigOption {
	return func(cfg *Config) {
		cfg.EnableScheduledTaskFallback = &enabled
	}
}

// WithValidation 设置字段验证规则的 locale 配置。
func WithValidation(validation *ValidationConfig) ConfigOption {
	return func(cfg *Config) {
		cfg.Validation = validation
	}
}
