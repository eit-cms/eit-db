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
	if strings.TrimSpace(resolved.ManyToManyStrategy) == "" {
		if v, _ := c.Options["many_to_many_strategy"].(string); strings.TrimSpace(v) != "" {
			resolved.ManyToManyStrategy = strings.TrimSpace(v)
		}
	}
	if resolved.Host == "" {
		resolved.Host = "localhost"
	}
	if resolved.Port == 0 {
		resolved.Port = 1433
	}
	if strings.TrimSpace(resolved.ManyToManyStrategy) == "" {
		resolved.ManyToManyStrategy = "direct_join"
	}
	resolved.ManyToManyStrategy = strings.ToLower(strings.TrimSpace(resolved.ManyToManyStrategy))
	if resolved.RecursiveCTEDepth <= 0 {
		if v, ok := c.Options["recursive_cte_depth"].(int); ok && v > 0 {
			resolved.RecursiveCTEDepth = v
		}
	}
	if resolved.RecursiveCTEDepth <= 0 {
		resolved.RecursiveCTEDepth = 8
	}
	if resolved.RecursiveCTEMaxRecursion <= 0 {
		if v, ok := c.Options["recursive_cte_max_recursion"].(int); ok && v > 0 {
			resolved.RecursiveCTEMaxRecursion = v
		}
	}
	if resolved.RecursiveCTEMaxRecursion <= 0 {
		resolved.RecursiveCTEMaxRecursion = 100
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
	if strings.TrimSpace(resolved.RelationJoinStrategy) == "" {
		if v, _ := c.Options["relation_join_strategy"].(string); strings.TrimSpace(v) != "" {
			resolved.RelationJoinStrategy = strings.TrimSpace(v)
		}
	}
	if strings.TrimSpace(resolved.RelationJoinStrategy) == "" {
		resolved.RelationJoinStrategy = "lookup"
	}
	resolved.RelationJoinStrategy = strings.ToLower(strings.TrimSpace(resolved.RelationJoinStrategy))
	if resolved.HideThroughArtifacts == nil {
		if v, ok := c.Options["hide_through_artifacts"].(bool); ok {
			resolved.HideThroughArtifacts = &v
		}
	}
	if resolved.HideThroughArtifacts == nil {
		defaultHide := true
		resolved.HideThroughArtifacts = &defaultHide
	}
	resolved.LogSystem = resolvedMongoLogSystemConfig(resolved.LogSystem)
	return resolved
}

// resolvedMongoLogSystemConfig 返回补全默认值后的日志系统配置。
func resolvedMongoLogSystemConfig(src *MongoLogSystemConfig) *MongoLogSystemConfig {
	cfg := &MongoLogSystemConfig{}
	if src != nil {
		*cfg = *src
		if src.ExtraStopWords != nil {
			cfg.ExtraStopWords = append([]string(nil), src.ExtraStopWords...)
		}
		if src.DefaultTokenizationRules != nil {
			cfg.DefaultTokenizationRules = append([]string(nil), src.DefaultTokenizationRules...)
		}
		if src.CustomTokenizationPatterns != nil {
			cfg.CustomTokenizationPatterns = make(map[string]string, len(src.CustomTokenizationPatterns))
			for k, v := range src.CustomTokenizationPatterns {
				cfg.CustomTokenizationPatterns[k] = v
			}
		}
		if src.CustomHotWords != nil {
			cfg.CustomHotWords = make(map[string]int, len(src.CustomHotWords))
			for k, v := range src.CustomHotWords {
				cfg.CustomHotWords[k] = v
			}
		}
	}
	if cfg.DefaultTopK <= 0 {
		cfg.DefaultTopK = 20
	}
	if cfg.DefaultMinTokenLen <= 0 {
		cfg.DefaultMinTokenLen = 2
	}
	if len(cfg.DefaultTokenizationRules) == 0 {
		cfg.DefaultTokenizationRules = []string{"ip", "url", "error_code", "trace_id", "hashtag"}
	}
	if strings.TrimSpace(cfg.DefaultLevelField) == "" {
		cfg.DefaultLevelField = "level"
	}
	if strings.TrimSpace(cfg.DefaultTimeField) == "" {
		cfg.DefaultTimeField = "timestamp"
	}
	if strings.TrimSpace(cfg.HotWordCollection) == "" {
		cfg.HotWordCollection = "eit_log_hot_words"
	}
	return cfg
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
	// 设置默认值
	if resolved.Database == "" {
		resolved.Database = "neo4j"
	}
	if resolved.Username == "" {
		resolved.Username = "neo4j"
	}
	if resolved.URI == "" {
		resolved.URI = "neo4j://localhost:7687"
	}
	resolved.SocialNetwork = resolvedNeo4jSocialNetworkConfig(resolved.SocialNetwork)
	return resolved
}

// ResolvedRedisConfig 返回 Redis 的有效配置。
func (c *Config) ResolvedRedisConfig() *RedisConnectionConfig {
	resolved := &RedisConnectionConfig{}
	if c != nil && c.Redis != nil {
		*resolved = *c.Redis
		if c.Redis.ClusterAddrs != nil {
			resolved.ClusterAddrs = append([]string(nil), c.Redis.ClusterAddrs...)
		}
	}
	if resolved.URI == "" {
		if uri, _ := c.Options["uri"].(string); strings.TrimSpace(uri) != "" {
			resolved.URI = strings.TrimSpace(uri)
		}
	}
	if resolved.Host == "" {
		resolved.Host = c.Host
	}
	if resolved.Port == 0 {
		resolved.Port = c.Port
	}
	if resolved.Password == "" {
		resolved.Password = c.Password
	}
	// 默认值：集群模式不设置 host/port 默认值
	if !resolved.ClusterMode {
		if resolved.Host == "" {
			resolved.Host = "localhost"
		}
		if resolved.Port == 0 {
			resolved.Port = 6379
		}
	}
	return resolved
}

// ResolvedArangoConfig 返回 ArangoDB 的有效配置。
func (c *Config) ResolvedArangoConfig() *ArangoConnectionConfig {
	resolved := &ArangoConnectionConfig{}
	if c != nil && c.Arango != nil {
		*resolved = *c.Arango
	}
	if resolved.URI == "" {
		if uri, _ := c.Options["uri"].(string); strings.TrimSpace(uri) != "" {
			resolved.URI = strings.TrimSpace(uri)
		}
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
	if resolved.Namespace == "" {
		if ns, _ := c.Options["namespace"].(string); strings.TrimSpace(ns) != "" {
			resolved.Namespace = strings.TrimSpace(ns)
		}
	}
	if resolved.URI == "" {
		resolved.URI = "http://localhost:8529"
	}
	if resolved.Database == "" {
		resolved.Database = "_system"
	}
	if resolved.Username == "" {
		resolved.Username = "root"
	}
	if resolved.TimeoutSeconds <= 0 {
		resolved.TimeoutSeconds = 10
	}
	return resolved
}

// resolvedNeo4jSocialNetworkConfig 返回补全默认值后的社交网络配置。
func resolvedNeo4jSocialNetworkConfig(src *Neo4jSocialNetworkConfig) *Neo4jSocialNetworkConfig {
	cfg := &Neo4jSocialNetworkConfig{}
	if src != nil {
		*cfg = *src
		if src.ModerationRelTypes != nil {
			cfg.ModerationRelTypes = append([]string(nil), src.ModerationRelTypes...)
		}
		if src.PermissionLevels != nil {
			cfg.PermissionLevels = append([]string(nil), src.PermissionLevels...)
		}
	}
	// 节点标签默认值
	if strings.TrimSpace(cfg.UserLabel) == "" {
		cfg.UserLabel = "User"
	}
	if strings.TrimSpace(cfg.ChatRoomLabel) == "" {
		cfg.ChatRoomLabel = "ChatRoom"
	}
	if strings.TrimSpace(cfg.ChatMessageLabel) == "" {
		cfg.ChatMessageLabel = "ChatMessage"
	}
	if strings.TrimSpace(cfg.PostLabel) == "" {
		cfg.PostLabel = "Post"
	}
	if strings.TrimSpace(cfg.CommentLabel) == "" {
		cfg.CommentLabel = "Comment"
	}
	if strings.TrimSpace(cfg.ForumLabel) == "" {
		cfg.ForumLabel = "Forum"
	}
	if strings.TrimSpace(cfg.EmojiLabel) == "" {
		cfg.EmojiLabel = "Emoji"
	}
	// 关系类型默认值
	if strings.TrimSpace(cfg.FollowsRelType) == "" {
		cfg.FollowsRelType = "FOLLOWS"
	}
	if strings.TrimSpace(cfg.FriendRelType) == "" {
		cfg.FriendRelType = "FRIEND"
	}
	if strings.TrimSpace(cfg.FriendRequestRelType) == "" {
		cfg.FriendRequestRelType = "FRIEND_REQUEST"
	}
	if strings.TrimSpace(cfg.SentRelType) == "" {
		cfg.SentRelType = "SENT"
	}
	if strings.TrimSpace(cfg.MemberOfRelType) == "" {
		cfg.MemberOfRelType = "MEMBER_OF"
	}
	if strings.TrimSpace(cfg.InRoomRelType) == "" {
		cfg.InRoomRelType = "IN"
	}
	if strings.TrimSpace(cfg.InRoomMsgRelType) == "" {
		cfg.InRoomMsgRelType = "IN_ROOM"
	}
	if strings.TrimSpace(cfg.MutedInRelType) == "" {
		cfg.MutedInRelType = "MUTED_IN"
	}
	if strings.TrimSpace(cfg.BannedInRelType) == "" {
		cfg.BannedInRelType = "BANNED_IN"
	}
	if strings.TrimSpace(cfg.ReadByRelType) == "" {
		cfg.ReadByRelType = "READ_BY"
	}
	if strings.TrimSpace(cfg.AuthoredRelType) == "" {
		cfg.AuthoredRelType = "AUTHORED"
	}
	if strings.TrimSpace(cfg.CreatedRelType) == "" {
		cfg.CreatedRelType = "CREATED"
	}
	// 其他默认值
	if strings.TrimSpace(cfg.ChatMessageFulltextIndex) == "" {
		cfg.ChatMessageFulltextIndex = "chat_message_fulltext"
	}
	if strings.TrimSpace(cfg.JoinRoomStrategy) == "" {
		cfg.JoinRoomStrategy = "request_approval"
	}
	if strings.TrimSpace(cfg.DirectChatPermission) == "" {
		cfg.DirectChatPermission = "mutual_follow_or_friend"
	}
	if len(cfg.ModerationRelTypes) == 0 {
		cfg.ModerationRelTypes = []string{"CREATED"}
	}
	if len(cfg.PermissionLevels) == 0 {
		cfg.PermissionLevels = []string{"member", "moderator", "admin", "creator"}
	}
	return cfg
}

// LoadConfigFromEnv 按 adapter 从环境变量加载数据库配置。
//
// Deprecated: 统一配置入口应通过 Config / NewConfig / LoadConfig 完成，环境变量加载仅作为旧版兼容层保留。
// 该兼容层不再扩展新能力，计划在下一个 minor 版本移除。
//
// 现存支持的环境变量：
// - PostgreSQL: POSTGRES_DSN 或 POSTGRES_HOST/PORT/USER/PASSWORD/DB/SSLMODE
// - MySQL: MYSQL_DSN 或 MYSQL_HOST/PORT/USER/PASSWORD/DB
// - SQL Server: SQLSERVER_DSN 或 SQLSERVER_HOST/PORT/USER/PASSWORD/DB
// - SQLite: SQLITE_DATABASE 或 SQLITE_PATH
// - MongoDB: MONGODB_URI + MONGODB_DATABASE/MONGODB_DB
// - Redis: REDIS_URI 或 REDIS_HOST/PORT/PASSWORD/DB
func LoadConfigFromEnv(adapter string) (*Config, error) {
	return LoadConfigFromEnvWithDefaults(adapter, nil)
}

// LoadConfigFromEnvWithDefaults 按 adapter 从环境变量加载数据库配置，并允许通过 defaults 指定默认值。
// 环境变量优先级高于 defaults；若设置了 *_DSN，则直接使用 DSN 连接细节。
//
// Deprecated: 统一配置入口应通过 Config / NewConfig / LoadConfig 完成，环境变量加载仅作为旧版兼容层保留。
// 该兼容层不再扩展新能力，计划在下一个 minor 版本移除。
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
		resolved.ManyToManyStrategy = strings.ToLower(strings.TrimSpace(preferEnvString(
			firstNonEmptyEnv("SQLSERVER_MANY_TO_MANY_STRATEGY"),
			resolved.ManyToManyStrategy,
			"direct_join",
		)))
		resolved.RecursiveCTEDepth = preferEnvInt(firstNonEmptyEnv("SQLSERVER_RECURSIVE_CTE_DEPTH"), resolved.RecursiveCTEDepth, 8)
		resolved.RecursiveCTEMaxRecursion = preferEnvInt(firstNonEmptyEnv("SQLSERVER_RECURSIVE_CTE_MAX_RECURSION"), resolved.RecursiveCTEMaxRecursion, 100)
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
		resolved.RelationJoinStrategy = strings.ToLower(strings.TrimSpace(preferEnvString(
			firstNonEmptyEnv("MONGODB_RELATION_JOIN_STRATEGY"),
			resolved.RelationJoinStrategy,
			"lookup",
		)))
		if v := strings.TrimSpace(firstNonEmptyEnv("MONGODB_HIDE_THROUGH_ARTIFACTS")); v != "" {
			parsed, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("invalid MONGODB_HIDE_THROUGH_ARTIFACTS value: %q", v)
			}
			resolved.HideThroughArtifacts = &parsed
		}
		// 日志系统配置环境变量
		if resolved.LogSystem == nil {
			resolved.LogSystem = resolvedMongoLogSystemConfig(nil)
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("MONGODB_LOG_DEFAULT_TOP_K")); v != "" {
			if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
				resolved.LogSystem.DefaultTopK = parsed
			}
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("MONGODB_LOG_DEFAULT_MIN_TOKEN_LEN")); v != "" {
			if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
				resolved.LogSystem.DefaultMinTokenLen = parsed
			}
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("MONGODB_LOG_DEFAULT_LEVEL_FIELD")); v != "" {
			resolved.LogSystem.DefaultLevelField = v
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("MONGODB_LOG_DEFAULT_TIME_FIELD")); v != "" {
			resolved.LogSystem.DefaultTimeField = v
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("MONGODB_LOG_HOT_WORD_COLLECTION")); v != "" {
			resolved.LogSystem.HotWordCollection = v
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("MONGODB_LOG_TOKENIZATION_RULES")); v != "" {
			rules := strings.Split(v, ",")
			cleaned := make([]string, 0, len(rules))
			for _, r := range rules {
				r = strings.TrimSpace(r)
				if r != "" {
					cleaned = append(cleaned, r)
				}
			}
			if len(cleaned) > 0 {
				resolved.LogSystem.DefaultTokenizationRules = cleaned
			}
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("MONGODB_LOG_DISABLE_BUILTIN_STOP_WORDS")); v != "" {
			if parsed, err := strconv.ParseBool(v); err == nil {
				resolved.LogSystem.DisableBuiltinStopWords = parsed
			}
		}
		config.MongoDB = resolved

	case "neo4j":
		resolved := config.ResolvedNeo4jConfig()
		resolved.URI = preferEnvString(firstNonEmptyEnv("NEO4J_URI"), resolved.URI, "")
		resolved.Username = preferEnvString(firstNonEmptyEnv("NEO4J_USER", "NEO4J_USERNAME"), resolved.Username, "neo4j")
		resolved.Password = preferEnvString(firstNonEmptyEnv("NEO4J_PASSWORD"), resolved.Password, "")
		resolved.Database = preferEnvString(firstNonEmptyEnv("NEO4J_DATABASE", "NEO4J_DB"), resolved.Database, "neo4j")
		// 社交网络配置环境变量
		if resolved.SocialNetwork == nil {
			resolved.SocialNetwork = resolvedNeo4jSocialNetworkConfig(nil)
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("NEO4J_SOCIAL_USER_LABEL")); v != "" {
			resolved.SocialNetwork.UserLabel = v
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("NEO4J_SOCIAL_CHAT_ROOM_LABEL")); v != "" {
			resolved.SocialNetwork.ChatRoomLabel = v
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("NEO4J_SOCIAL_CHAT_MESSAGE_LABEL")); v != "" {
			resolved.SocialNetwork.ChatMessageLabel = v
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("NEO4J_SOCIAL_FOLLOWS_REL")); v != "" {
			resolved.SocialNetwork.FollowsRelType = v
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("NEO4J_SOCIAL_FRIEND_REL")); v != "" {
			resolved.SocialNetwork.FriendRelType = v
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("NEO4J_SOCIAL_JOIN_ROOM_STRATEGY")); v != "" {
			resolved.SocialNetwork.JoinRoomStrategy = strings.ToLower(v)
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("NEO4J_SOCIAL_DIRECT_CHAT_PERMISSION")); v != "" {
			resolved.SocialNetwork.DirectChatPermission = strings.ToLower(v)
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("NEO4J_SOCIAL_CHAT_MESSAGE_FULLTEXT_INDEX")); v != "" {
			resolved.SocialNetwork.ChatMessageFulltextIndex = v
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("NEO4J_SOCIAL_MODERATION_REL_TYPES")); v != "" {
			relTypes := strings.Split(v, ",")
			cleaned := make([]string, 0, len(relTypes))
			for _, r := range relTypes {
				r = strings.TrimSpace(r)
				if r != "" {
					cleaned = append(cleaned, r)
				}
			}
			if len(cleaned) > 0 {
				resolved.SocialNetwork.ModerationRelTypes = cleaned
			}
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("NEO4J_SOCIAL_PERMISSION_LEVELS")); v != "" {
			levels := strings.Split(v, ",")
			cleaned := make([]string, 0, len(levels))
			for _, l := range levels {
				l = strings.TrimSpace(l)
				if l != "" {
					cleaned = append(cleaned, l)
				}
			}
			if len(cleaned) > 0 {
				resolved.SocialNetwork.PermissionLevels = cleaned
			}
		}
		config.Neo4j = resolved

	case "redis":
		resolved := config.ResolvedRedisConfig()
		resolved.URI = preferEnvString(firstNonEmptyEnv("REDIS_URI"), resolved.URI, "")
		resolved.Host = preferEnvString(firstNonEmptyEnv("REDIS_HOST"), resolved.Host, "localhost")
		resolved.Port = preferEnvInt(firstNonEmptyEnv("REDIS_PORT"), resolved.Port, 6379)
		resolved.Password = preferEnvString(firstNonEmptyEnv("REDIS_PASSWORD"), resolved.Password, "")
		resolved.Username = preferEnvString(firstNonEmptyEnv("REDIS_USERNAME", "REDIS_USER"), resolved.Username, "")
		if v := strings.TrimSpace(firstNonEmptyEnv("REDIS_DB")); v != "" {
			if parsed, err := strconv.Atoi(v); err == nil {
				resolved.DB = parsed
			}
		}
		if v := strings.TrimSpace(firstNonEmptyEnv("REDIS_TLS_ENABLED")); v != "" {
			if parsed, err := strconv.ParseBool(v); err == nil {
				resolved.TLSEnabled = parsed
			}
		}
		config.Redis = resolved

	case "arango":
		resolved := config.ResolvedArangoConfig()
		resolved.URI = preferEnvString(firstNonEmptyEnv("ARANGO_URI", "ARANGODB_URI"), resolved.URI, "http://localhost:8529")
		resolved.Database = preferEnvString(firstNonEmptyEnv("ARANGO_DB", "ARANGO_DATABASE", "ARANGODB_DATABASE"), resolved.Database, "_system")
		resolved.Username = preferEnvString(firstNonEmptyEnv("ARANGO_USER", "ARANGO_USERNAME", "ARANGODB_USER", "ARANGODB_USERNAME"), resolved.Username, "root")
		resolved.Password = preferEnvString(firstNonEmptyEnv("ARANGO_PASSWORD", "ARANGODB_PASSWORD"), resolved.Password, "")
		resolved.Namespace = preferEnvString(firstNonEmptyEnv("ARANGO_NAMESPACE", "ARANGODB_NAMESPACE"), resolved.Namespace, "")
		resolved.TimeoutSeconds = preferEnvInt(firstNonEmptyEnv("ARANGO_TIMEOUT_SECONDS", "ARANGODB_TIMEOUT_SECONDS"), resolved.TimeoutSeconds, 10)
		config.Arango = resolved

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
	if src.EnableScheduledTaskFallback != nil {
		v := *src.EnableScheduledTaskFallback
		clone.EnableScheduledTaskFallback = &v
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
		if src.MongoDB.LogSystem != nil {
			mongoCfg.LogSystem = resolvedMongoLogSystemConfig(src.MongoDB.LogSystem)
		}
		clone.MongoDB = &mongoCfg
	}
	if src.Neo4j != nil {
		neo4jCfg := *src.Neo4j
		if src.Neo4j.SocialNetwork != nil {
			neo4jCfg.SocialNetwork = resolvedNeo4jSocialNetworkConfig(src.Neo4j.SocialNetwork)
		}
		clone.Neo4j = &neo4jCfg
	}
	if src.Redis != nil {
		redisCfg := *src.Redis
		if src.Redis.ClusterAddrs != nil {
			redisCfg.ClusterAddrs = append([]string(nil), src.Redis.ClusterAddrs...)
		}
		clone.Redis = &redisCfg
	}
	if src.Arango != nil {
		arangoCfg := *src.Arango
		clone.Arango = &arangoCfg
	}
	return &clone
}

// ScheduledTaskFallbackEnabled 返回定时任务回退开关的有效值。
// 默认 true；当显式配置为 false 时禁用回退。
func (c *Config) ScheduledTaskFallbackEnabled() bool {
	if c == nil {
		return true
	}
	if c.EnableScheduledTaskFallback != nil {
		return *c.EnableScheduledTaskFallback
	}
	if c.Options != nil {
		if v, ok := c.Options["scheduled_task_fallback"].(bool); ok {
			return v
		}
		if raw, ok := c.Options["scheduled_task_fallback"].(string); ok {
			if parsed, err := strconv.ParseBool(strings.TrimSpace(raw)); err == nil {
				return parsed
			}
		}
	}
	return true
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

	if desc, ok := LookupAdapterDescriptor(c.Adapter); ok {
		if desc.ValidateConfig != nil {
			if err := desc.ValidateConfig(c); err != nil {
				return err
			}
		}
	} else {
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

	if c.EnableScheduledTaskFallback == nil && c.Options != nil {
		if v, ok := c.Options["scheduled_task_fallback"].(bool); ok {
			c.EnableScheduledTaskFallback = &v
		} else if raw, ok := c.Options["scheduled_task_fallback"].(string); ok {
			if parsed, err := strconv.ParseBool(strings.TrimSpace(raw)); err == nil {
				c.EnableScheduledTaskFallback = &parsed
			}
		}
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
	config := newDefaultAdapterConfig(adapterType)
	if desc, ok := LookupAdapterDescriptor(adapterType); ok && desc.DefaultConfig != nil {
		return desc.DefaultConfig()
	}

	return config
}
