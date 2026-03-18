package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConfigFile 配置文件结构 (从 YAML 或 JSON 加载)
type ConfigFile struct {
	Database *Config `yaml:"database" json:"database"`
}

// AdapterRegistryFile 多 Adapter 配置文件结构
type AdapterRegistryFile struct {
	Adapters map[string]*Config `yaml:"adapters" json:"adapters"`
}

// ResolvedSQLiteConfig 返回 SQLite 的有效配置。
func (c *Config) ResolvedSQLiteConfig() *SQLiteConnectionConfig {
	resolved := &SQLiteConnectionConfig{}
	if c != nil && c.SQLite != nil {
		*resolved = *c.SQLite
	}
	if resolved.Path == "" {
		resolved.Path = c.Database
	}
	if resolved.DSN == "" {
		if dsn, _ := c.Options["dsn"].(string); strings.TrimSpace(dsn) != "" {
			resolved.DSN = strings.TrimSpace(dsn)
		}
	}
	if resolved.Path == "" && resolved.DSN == "" {
		resolved.Path = "./eit.db"
	}
	return resolved
}

// ResolvedPostgresConfig 返回 PostgreSQL 的有效配置。
func (c *Config) ResolvedPostgresConfig() *PostgresConnectionConfig {
	resolved := &PostgresConnectionConfig{}
	if c != nil && c.Postgres != nil {
		*resolved = *c.Postgres
	}
	if resolved.Host == "" {
		resolved.Host = c.Host
	}
	if resolved.Port == 0 {
		resolved.Port = c.Port
	}
	if resolved.Username == "" {
		resolved.Username = c.Username
	}
	if resolved.Password == "" {
		resolved.Password = c.Password
	}
	if resolved.Database == "" {
		resolved.Database = c.Database
	}
	if resolved.SSLMode == "" {
		resolved.SSLMode = c.SSLMode
	}
	if resolved.DSN == "" {
		if dsn, _ := c.Options["dsn"].(string); strings.TrimSpace(dsn) != "" {
			resolved.DSN = strings.TrimSpace(dsn)
		}
	}
	if resolved.Host == "" {
		resolved.Host = "localhost"
	}
	if resolved.Port == 0 {
		resolved.Port = 5432
	}
	if resolved.SSLMode == "" {
		resolved.SSLMode = "disable"
	}
	return resolved
}

// ResolvedMySQLConfig 返回 MySQL 的有效配置。
func (c *Config) ResolvedMySQLConfig() *MySQLConnectionConfig {
	resolved := &MySQLConnectionConfig{}
	if c != nil && c.MySQL != nil {
		*resolved = *c.MySQL
	}
	if resolved.Host == "" {
		resolved.Host = c.Host
	}
	if resolved.Port == 0 {
		resolved.Port = c.Port
	}
	if resolved.Username == "" {
		resolved.Username = c.Username
	}
	if resolved.Password == "" {
		resolved.Password = c.Password
	}
	if resolved.Database == "" {
		resolved.Database = c.Database
	}
	if resolved.DSN == "" {
		if dsn, _ := c.Options["dsn"].(string); strings.TrimSpace(dsn) != "" {
			resolved.DSN = strings.TrimSpace(dsn)
		}
	}
	if resolved.Host == "" {
		resolved.Host = "localhost"
	}
	if resolved.Port == 0 {
		resolved.Port = 3306
	}
	return resolved
}

// ResolvedSQLServerConfig 返回 SQL Server 的有效配置。
func (c *Config) ResolvedSQLServerConfig() *SQLServerConnectionConfig {
	resolved := &SQLServerConnectionConfig{}
	if c != nil && c.SQLServer != nil {
		*resolved = *c.SQLServer
	}
	if resolved.Host == "" {
		resolved.Host = c.Host
	}
	if resolved.Port == 0 {
		resolved.Port = c.Port
	}
	if resolved.Username == "" {
		resolved.Username = c.Username
	}
	if resolved.Password == "" {
		resolved.Password = c.Password
	}
	if resolved.Database == "" {
		resolved.Database = c.Database
	}
	if resolved.DSN == "" {
		if dsn, _ := c.Options["dsn"].(string); strings.TrimSpace(dsn) != "" {
			resolved.DSN = strings.TrimSpace(dsn)
		}
	}
	if resolved.Host == "" {
		resolved.Host = "localhost"
	}
	if resolved.Port == 0 {
		resolved.Port = 1433
	}
	return resolved
}

// ResolvedMongoConfig 返回 MongoDB 的有效配置。
func (c *Config) ResolvedMongoConfig() *MongoConnectionConfig {
	resolved := &MongoConnectionConfig{}
	if c != nil && c.MongoDB != nil {
		*resolved = *c.MongoDB
	}
	if resolved.Database == "" {
		resolved.Database = c.Database
	}
	if resolved.URI == "" {
		if uri, _ := c.Options["uri"].(string); strings.TrimSpace(uri) != "" {
			resolved.URI = strings.TrimSpace(uri)
		}
	}
	if resolved.Database == "" {
		resolved.Database = "eit"
	}
	return resolved
}

// ResolvedNeo4jConfig 返回 Neo4j 的有效配置。
func (c *Config) ResolvedNeo4jConfig() *Neo4jConnectionConfig {
	resolved := &Neo4jConnectionConfig{}
	if c != nil && c.Neo4j != nil {
		*resolved = *c.Neo4j
	}
	if resolved.Database == "" {
		resolved.Database = c.Database
	}
	if resolved.Username == "" {
		resolved.Username = c.Username
	}
	if resolved.Password == "" {
		resolved.Password = c.Password
	}
	if resolved.URI == "" {
		if uri, _ := c.Options["uri"].(string); strings.TrimSpace(uri) != "" {
			resolved.URI = strings.TrimSpace(uri)
		}
	}
	if resolved.Database == "" {
		resolved.Database = "neo4j"
	}
	return resolved
}

// LoadConfigFromEnv 按 adapter 从环境变量加载数据库配置。
//
// 支持的环境变量：
// - PostgreSQL: POSTGRES_DSN 或 POSTGRES_HOST/PORT/USER/PASSWORD/DB/SSLMODE
// - MySQL: MYSQL_DSN 或 MYSQL_HOST/PORT/USER/PASSWORD/DB
// - SQL Server: SQLSERVER_DSN 或 SQLSERVER_HOST/PORT/USER/PASSWORD/DB
// - SQLite: SQLITE_DATABASE 或 SQLITE_PATH
// - MongoDB: MONGODB_URI + MONGODB_DATABASE/MONGODB_DB
func LoadConfigFromEnv(adapter string) (*Config, error) {
	return LoadConfigFromEnvWithDefaults(adapter, nil)
}

// LoadConfigFromEnvWithDefaults 按 adapter 从环境变量加载数据库配置，并允许通过 defaults 指定默认值。
// 环境变量优先级高于 defaults；若设置了 *_DSN，则直接使用 DSN 连接细节。
func LoadConfigFromEnvWithDefaults(adapter string, defaults *Config) (*Config, error) {
	normalized := strings.ToLower(strings.TrimSpace(adapter))
	if normalized == "" {
		return nil, fmt.Errorf("adapter must be specified")
	}

	config := cloneConfig(defaults)
	if config == nil {
		config = &Config{}
	}
	config.Adapter = normalized

	switch normalized {
	case "postgres":
		resolved := config.ResolvedPostgresConfig()
		resolved.Host = preferEnvString(firstNonEmptyEnv("POSTGRES_HOST"), resolved.Host, "localhost")
		resolved.Port = preferEnvInt(firstNonEmptyEnv("POSTGRES_PORT"), resolved.Port, 5432)
		resolved.Username = preferEnvString(firstNonEmptyEnv("POSTGRES_USER", "PGUSER"), resolved.Username, "postgres")
		resolved.Password = preferEnvString(firstNonEmptyEnv("POSTGRES_PASSWORD", "PGPASSWORD"), resolved.Password, "")
		resolved.Database = preferEnvString(firstNonEmptyEnv("POSTGRES_DB", "POSTGRES_DATABASE", "PGDATABASE"), resolved.Database, "eit")
		resolved.SSLMode = preferEnvString(firstNonEmptyEnv("POSTGRES_SSLMODE", "PGSSLMODE"), resolved.SSLMode, "disable")
		resolved.DSN = preferEnvString(firstNonEmptyEnv("POSTGRES_DSN"), resolved.DSN, "")
		config.Postgres = resolved

	case "mysql":
		resolved := config.ResolvedMySQLConfig()
		resolved.Host = preferEnvString(firstNonEmptyEnv("MYSQL_HOST"), resolved.Host, "localhost")
		resolved.Port = preferEnvInt(firstNonEmptyEnv("MYSQL_PORT"), resolved.Port, 3306)
		resolved.Username = preferEnvString(firstNonEmptyEnv("MYSQL_USER"), resolved.Username, "root")
		resolved.Password = preferEnvString(firstNonEmptyEnv("MYSQL_PASSWORD"), resolved.Password, "")
		resolved.Database = preferEnvString(firstNonEmptyEnv("MYSQL_DB", "MYSQL_DATABASE"), resolved.Database, "eit")
		resolved.DSN = preferEnvString(firstNonEmptyEnv("MYSQL_DSN"), resolved.DSN, "")
		config.MySQL = resolved

	case "sqlserver":
		resolved := config.ResolvedSQLServerConfig()
		resolved.Host = preferEnvString(firstNonEmptyEnv("SQLSERVER_HOST"), resolved.Host, "localhost")
		resolved.Port = preferEnvInt(firstNonEmptyEnv("SQLSERVER_PORT"), resolved.Port, 1433)
		resolved.Username = preferEnvString(firstNonEmptyEnv("SQLSERVER_USER"), resolved.Username, "sa")
		resolved.Password = preferEnvString(firstNonEmptyEnv("SQLSERVER_PASSWORD"), resolved.Password, "")
		resolved.Database = preferEnvString(firstNonEmptyEnv("SQLSERVER_DB", "SQLSERVER_DATABASE"), resolved.Database, "master")
		resolved.DSN = preferEnvString(firstNonEmptyEnv("SQLSERVER_DSN"), resolved.DSN, "")
		config.SQLServer = resolved

	case "sqlite":
		resolved := config.ResolvedSQLiteConfig()
		resolved.Path = preferEnvString(firstNonEmptyEnv("SQLITE_DATABASE", "SQLITE_PATH"), resolved.Path, "./eit.db")
		resolved.DSN = preferEnvString(firstNonEmptyEnv("SQLITE_DSN"), resolved.DSN, "")
		config.SQLite = resolved

	case "mongodb":
		resolved := config.ResolvedMongoConfig()
		resolved.Database = preferEnvString(firstNonEmptyEnv("MONGODB_DB", "MONGODB_DATABASE"), resolved.Database, "eit")
		resolved.URI = preferEnvString(firstNonEmptyEnv("MONGODB_URI", "MONGO_URI"), resolved.URI, "")
		config.MongoDB = resolved

	case "neo4j":
		resolved := config.ResolvedNeo4jConfig()
		resolved.URI = preferEnvString(firstNonEmptyEnv("NEO4J_URI"), resolved.URI, "")
		resolved.Username = preferEnvString(firstNonEmptyEnv("NEO4J_USER", "NEO4J_USERNAME"), resolved.Username, "neo4j")
		resolved.Password = preferEnvString(firstNonEmptyEnv("NEO4J_PASSWORD"), resolved.Password, "")
		resolved.Database = preferEnvString(firstNonEmptyEnv("NEO4J_DATABASE", "NEO4J_DB"), resolved.Database, "neo4j")
		config.Neo4j = resolved

	default:
		return nil, fmt.Errorf("unsupported adapter: %s", normalized)
	}

	if config.QueryCache == nil {
		config.QueryCache = &QueryCacheConfig{}
	}
	config.QueryCache.MaxEntries = preferEnvInt(firstNonEmptyEnv("EIT_QUERY_CACHE_MAX_ENTRIES"), config.QueryCache.MaxEntries, DefaultCompiledQueryCacheMaxEntries)
	config.QueryCache.DefaultTTLSeconds = preferEnvInt(firstNonEmptyEnv("EIT_QUERY_CACHE_DEFAULT_TTL_SECONDS"), config.QueryCache.DefaultTTLSeconds, DefaultCompiledQueryCacheDefaultTTLSeconds)
	config.QueryCache.EnableMetrics = preferEnvBool(firstNonEmptyEnv("EIT_QUERY_CACHE_ENABLE_METRICS"), config.QueryCache.EnableMetrics, DefaultCompiledQueryCacheEnableMetrics)

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

func cloneConfig(src *Config) *Config {
	if src == nil {
		return nil
	}
	clone := *src
	if src.QueryCache != nil {
		queryCache := *src.QueryCache
		clone.QueryCache = &queryCache
	}
	if src.Pool != nil {
		pool := *src.Pool
		clone.Pool = &pool
	}
	if src.Options != nil {
		clone.Options = make(map[string]interface{}, len(src.Options))
		for key, value := range src.Options {
			clone.Options[key] = value
		}
	}
	if src.Validation != nil {
		validation := *src.Validation
		if src.Validation.EnabledLocales != nil {
			validation.EnabledLocales = append([]string(nil), src.Validation.EnabledLocales...)
		}
		clone.Validation = &validation
	}
	if src.SQLite != nil {
		sqliteCfg := *src.SQLite
		clone.SQLite = &sqliteCfg
	}
	if src.Postgres != nil {
		pgCfg := *src.Postgres
		clone.Postgres = &pgCfg
	}
	if src.MySQL != nil {
		myCfg := *src.MySQL
		clone.MySQL = &myCfg
	}
	if src.SQLServer != nil {
		ssCfg := *src.SQLServer
		clone.SQLServer = &ssCfg
	}
	if src.MongoDB != nil {
		mongoCfg := *src.MongoDB
		clone.MongoDB = &mongoCfg
	}
	if src.Neo4j != nil {
		neo4jCfg := *src.Neo4j
		clone.Neo4j = &neo4jCfg
	}
	return &clone
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

func preferEnvString(envValue, currentValue, fallback string) string {
	if strings.TrimSpace(envValue) != "" {
		return strings.TrimSpace(envValue)
	}
	if strings.TrimSpace(currentValue) != "" {
		return currentValue
	}
	return fallback
}

func preferEnvInt(envValue string, currentValue, fallback int) int {
	if strings.TrimSpace(envValue) != "" {
		if parsed, err := strconv.Atoi(strings.TrimSpace(envValue)); err == nil {
			return parsed
		}
	}
	if currentValue > 0 {
		return currentValue
	}
	return fallback
}

func preferEnvBool(envValue string, currentValue, fallback bool) bool {
	if strings.TrimSpace(envValue) != "" {
		if parsed, err := strconv.ParseBool(strings.TrimSpace(envValue)); err == nil {
			return parsed
		}
	}
	if currentValue {
		return true
	}
	return fallback
}

func hasDSNOption(config *Config) bool {
	if config == nil || config.Options == nil {
		return false
	}
	dsn, _ := config.Options["dsn"].(string)
	return strings.TrimSpace(dsn) != ""
}

// LoadConfig 从文件加载数据库配置（支持 JSON 和 YAML 格式）
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(filename))
	var config *Config

	switch ext {
	case ".json":
		// 先尝试解析为直接的 Config 对象
		if err := json.Unmarshal(data, &config); err == nil && config != nil && config.Adapter != "" {
			// 成功解析为直接的 Config 对象
		} else {
			// 尝试解析为 ConfigFile 包装结构
			var cf ConfigFile
			if err := json.Unmarshal(data, &cf); err != nil {
				return nil, fmt.Errorf("failed to parse JSON config: %w", err)
			}
			if cf.Database == nil {
				return nil, fmt.Errorf("database configuration not found in JSON config file")
			}
			config = cf.Database
		}

	case ".yaml", ".yml":
		// 使用 YAML 解析器
		var cf ConfigFile
		if err := yaml.Unmarshal(data, &cf); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
		if cf.Database == nil {
			return nil, fmt.Errorf("database configuration not found in YAML config file")
		}
		config = cf.Database

	default:
		// 默认使用 YAML 解析器
		var cf ConfigFile
		if err := yaml.Unmarshal(data, &cf); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
		if cf.Database == nil {
			return nil, fmt.Errorf("database configuration not found in config file")
		}
		config = cf.Database
	}

	// 验证配置
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid database configuration: %w", err)
	}

	return config, nil
}

// LoadAdapterRegistry 从文件加载多 Adapter 配置（支持 JSON 和 YAML）
func LoadAdapterRegistry(filename string) (map[string]*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(filename))
	var registry AdapterRegistryFile

	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &registry); err != nil {
			return nil, fmt.Errorf("failed to parse JSON adapter registry: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &registry); err != nil {
			return nil, fmt.Errorf("failed to parse YAML adapter registry: %w", err)
		}
	default:
		if err := yaml.Unmarshal(data, &registry); err != nil {
			return nil, fmt.Errorf("failed to parse adapter registry: %w", err)
		}
	}

	if len(registry.Adapters) == 0 {
		return nil, fmt.Errorf("no adapters found in config file")
	}

	// 验证每个 adapter 配置
	for name, cfg := range registry.Adapters {
		if name == "" {
			return nil, fmt.Errorf("adapter name cannot be empty")
		}
		if cfg == nil {
			return nil, fmt.Errorf("adapter '%s' config cannot be nil", name)
		}
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid adapter '%s' config: %w", name, err)
		}
	}

	return registry.Adapters, nil
}

// LoadConfigWithDefaults 从文件加载配置并应用默认值
func LoadConfigWithDefaults(filename string, defaults *Config) (*Config, error) {
	config, err := LoadConfig(filename)
	if err != nil {
		return nil, err
	}

	// 应用默认值
	if defaults != nil {
		if config.Adapter == "" {
			config.Adapter = defaults.Adapter
		}
		if config.SQLite == nil && defaults.SQLite != nil {
			sqliteCfg := *defaults.SQLite
			config.SQLite = &sqliteCfg
		}
		if config.Postgres == nil && defaults.Postgres != nil {
			pgCfg := *defaults.Postgres
			config.Postgres = &pgCfg
		}
		if config.MySQL == nil && defaults.MySQL != nil {
			myCfg := *defaults.MySQL
			config.MySQL = &myCfg
		}
		if config.SQLServer == nil && defaults.SQLServer != nil {
			ssCfg := *defaults.SQLServer
			config.SQLServer = &ssCfg
		}
		if config.MongoDB == nil && defaults.MongoDB != nil {
			mongoCfg := *defaults.MongoDB
			config.MongoDB = &mongoCfg
		}
		if config.Neo4j == nil && defaults.Neo4j != nil {
			neo4jCfg := *defaults.Neo4j
			config.Neo4j = &neo4jCfg
		}
		if config.Adapter == "" {
			config.Adapter = defaults.Adapter
		}
		if config.Port == 0 && defaults.Port != 0 {
			config.Port = defaults.Port
		}
		if config.Pool == nil && defaults.Pool != nil {
			config.Pool = defaults.Pool
		}
		if config.SSLMode == "" && defaults.SSLMode != "" {
			config.SSLMode = defaults.SSLMode
		}
	}

	return config, nil
}

// SaveConfig 保存配置到文件
func SaveConfig(filename string, config *Config) error {
	cf := ConfigFile{Database: config}

	data, err := yaml.Marshal(cf)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(filename, data, 0o644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Validate 验证配置有效性
func (c *Config) Validate() error {
	if c.Adapter == "" {
		return fmt.Errorf("adapter must be specified")
	}

	switch c.Adapter {
	case "sqlite":
		sqliteCfg := c.ResolvedSQLiteConfig()
		if strings.TrimSpace(sqliteCfg.Path) == "" && strings.TrimSpace(sqliteCfg.DSN) == "" {
			return fmt.Errorf("sqlite: database path must be specified")
		}

	case "postgres", "mysql", "sqlserver":
		var hasDSN bool
		var host, username, database string
		if c.Adapter == "postgres" {
			cfg := c.ResolvedPostgresConfig()
			hasDSN = strings.TrimSpace(cfg.DSN) != ""
			host, username, database = cfg.Host, cfg.Username, cfg.Database
			c.Postgres = cfg
		} else if c.Adapter == "mysql" {
			cfg := c.ResolvedMySQLConfig()
			hasDSN = strings.TrimSpace(cfg.DSN) != ""
			host, username, database = cfg.Host, cfg.Username, cfg.Database
			c.MySQL = cfg
		} else {
			cfg := c.ResolvedSQLServerConfig()
			hasDSN = strings.TrimSpace(cfg.DSN) != ""
			host, username, database = cfg.Host, cfg.Username, cfg.Database
			c.SQLServer = cfg
		}
		if hasDSN {
			break
		}
		if host == "" {
			return fmt.Errorf("%s: host must be specified", c.Adapter)
		}
		if username == "" {
			return fmt.Errorf("%s: username must be specified", c.Adapter)
		}
		if database == "" {
			return fmt.Errorf("%s: database name must be specified", c.Adapter)
		}

	case "mongodb":
		mongoCfg := c.ResolvedMongoConfig()
		c.MongoDB = mongoCfg
		if mongoCfg.Database == "" {
			return fmt.Errorf("mongodb: database name must be specified")
		}
		if strings.TrimSpace(mongoCfg.URI) == "" {
			return fmt.Errorf("mongodb: uri must be specified")
		}

	case "neo4j":
		neo4jCfg := c.ResolvedNeo4jConfig()
		c.Neo4j = neo4jCfg
		if strings.TrimSpace(neo4jCfg.URI) == "" {
			return fmt.Errorf("neo4j: uri must be specified")
		}
		if strings.TrimSpace(neo4jCfg.Username) == "" {
			return fmt.Errorf("neo4j: username must be specified")
		}

	default:
		return fmt.Errorf("unsupported adapter: %s", c.Adapter)
	}

	// 验证连接池配置
	if c.Pool != nil {
		if c.Pool.MaxConnections <= 0 {
			c.Pool.MaxConnections = 25
		}
		if c.Pool.MinConnections < 0 {
			c.Pool.MinConnections = 0
		}
		if c.Pool.ConnectTimeout <= 0 {
			c.Pool.ConnectTimeout = 30
		}
		if c.Pool.IdleTimeout <= 0 {
			c.Pool.IdleTimeout = 300
		}
	} else {
		// 使用默认连接池配置
		c.Pool = &PoolConfig{
			MaxConnections: 25,
			MinConnections: 0,
			ConnectTimeout: 30,
			IdleTimeout:    300,
		}
	}

	// PostgreSQL 特定验证
	if c.Adapter == "postgres" {
		c.Postgres = c.ResolvedPostgresConfig()
	}

	if c.QueryCache == nil {
		c.QueryCache = &QueryCacheConfig{}
	}
	if c.QueryCache.MaxEntries <= 0 {
		c.QueryCache.MaxEntries = DefaultCompiledQueryCacheMaxEntries
	}
	if c.QueryCache.DefaultTTLSeconds < 0 {
		return fmt.Errorf("query_cache.default_ttl_seconds must be >= 0")
	}

	// 校验 locale 配置
	if c.Validation != nil {
		if c.Validation.DefaultLocale != "" && !ValidationLocaleExists(c.Validation.DefaultLocale) {
			return fmt.Errorf("validation.default_locale is not supported: %s", c.Validation.DefaultLocale)
		}

		for _, locale := range c.Validation.EnabledLocales {
			if !ValidationLocaleExists(locale) {
				return fmt.Errorf("validation.enabled_locales contains unsupported locale: %s", locale)
			}
		}

		if c.Validation.DefaultLocale != "" && len(c.Validation.EnabledLocales) > 0 && !slices.Contains(c.Validation.EnabledLocales, c.Validation.DefaultLocale) {
			return fmt.Errorf("validation.default_locale (%s) must be included in validation.enabled_locales", c.Validation.DefaultLocale)
		}
	}

	if err := validateStartupCapabilityConfig(c.StartupCapabilities); err != nil {
		return err
	}

	return nil
}

// DefaultConfig 返回默认配置
func DefaultConfig(adapterType string) *Config {
	config := &Config{
		Adapter: adapterType,
		Pool: &PoolConfig{
			MaxConnections: 25,
			MinConnections: 0,
			ConnectTimeout: 30,
			IdleTimeout:    300,
		},
	}

	switch adapterType {
	case "sqlite":
		config.SQLite = &SQLiteConnectionConfig{Path: "./eit.db"}

	case "postgres":
		config.Postgres = &PostgresConnectionConfig{Host: "localhost", Port: 5432, Database: "eit", Username: "postgres", Password: "postgres", SSLMode: "disable"}

	case "mysql":
		config.MySQL = &MySQLConnectionConfig{Host: "localhost", Port: 3306, Database: "eit", Username: "root", Password: "root"}

	case "sqlserver":
		config.SQLServer = &SQLServerConnectionConfig{Host: "localhost", Port: 1433, Database: "master", Username: "sa", Password: "YourStrong!Passw0rd"}

	case "mongodb":
		config.MongoDB = &MongoConnectionConfig{URI: "mongodb://localhost:27017", Database: "eit"}

	case "neo4j":
		config.Neo4j = &Neo4jConnectionConfig{URI: "neo4j://localhost:7687", Username: "neo4j", Password: "neo4j", Database: "neo4j"}
	}

	return config
}
