package db

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// init 通过 AdapterDescriptor 一次性完成 Redis 适配器的全量注册。
// 这是内建适配器迁移到新注册模式的规范示例。
func init() {
	MustRegisterAdapterDescriptor("redis", AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) {
			a, err := NewRedisAdapter(cfg)
			if err != nil {
				return nil, err
			}
			if err := a.Connect(context.Background(), cfg); err != nil {
				return nil, err
			}
			return a, nil
		},
		ValidateConfig: func(cfg *Config) error {
			redisCfg := cfg.ResolvedRedisConfig()
			cfg.Redis = redisCfg
			if redisCfg.ClusterMode {
				if len(redisCfg.ClusterAddrs) == 0 {
					return fmt.Errorf("redis: cluster_addrs must not be empty when cluster_mode is true")
				}
				return nil
			}
			if strings.TrimSpace(redisCfg.URI) == "" && strings.TrimSpace(redisCfg.Host) == "" {
				return fmt.Errorf("redis: host or uri must be specified")
			}
			return nil
		},
		DefaultConfig: func() *Config {
			return &Config{
				Adapter: "redis",
				Redis:   &RedisConnectionConfig{Host: "localhost", Port: 6379},
				Pool: &PoolConfig{
					MaxConnections: 25,
					MinConnections: 0,
					ConnectTimeout: 30,
					IdleTimeout:    300,
				},
			}
		},
		Metadata: func() AdapterMetadata {
			return AdapterMetadata{
				Name:       "redis",
				DriverKind: "kv",
				Vendor:     "redis",
			}
		},
		ExecuteQueryConstructor: func(ctx context.Context, adapter Adapter, query string, args []interface{}) (*QueryConstructorExecutionResult, bool, error) {
			redisAdapter, ok := adapter.(*RedisAdapter)
			if !ok {
				return nil, false, nil
			}
			trimmed := strings.TrimSpace(query)
			if strings.HasPrefix(trimmed, redisCompiledCommandPrefix) {
				rows, execSummary, redisErr := redisAdapter.ExecuteCompiledCommandPlan(ctx, query)
				if redisErr != nil {
					return nil, true, redisErr
				}
				if execSummary != nil {
					return nil, true, fmt.Errorf("ExecuteQueryConstructor is query-only for redis write plans; use ExecuteQueryConstructorAuto")
				}
				return &QueryConstructorExecutionResult{Statement: query, Args: copyQueryArgs(args), Rows: rows}, true, nil
			}
			if strings.HasPrefix(trimmed, redisCompiledPipelinePrefix) {
				rows, execSummary, redisErr := redisAdapter.ExecuteCompiledPipelinePlan(ctx, query)
				if redisErr != nil {
					return nil, true, redisErr
				}
				if execSummary != nil {
					return nil, true, fmt.Errorf("ExecuteQueryConstructor is query-only for redis write pipeline plans; use ExecuteQueryConstructorAuto")
				}
				return &QueryConstructorExecutionResult{Statement: query, Args: copyQueryArgs(args), Rows: rows}, true, nil
			}
			return nil, true, fmt.Errorf("redis query constructor requires compiled plan prefix %q", redisCompiledCommandPrefix)
		},
		ExecuteQueryConstructorAuto: func(ctx context.Context, adapter Adapter, query string, args []interface{}) (*QueryConstructorAutoExecutionResult, bool, error) {
			redisAdapter, ok := adapter.(*RedisAdapter)
			if !ok {
				return nil, false, nil
			}
			trimmed := strings.TrimSpace(query)
			if strings.HasPrefix(trimmed, redisCompiledCommandPrefix) {
				rows, execSummary, redisErr := redisAdapter.ExecuteCompiledCommandPlan(ctx, query)
				if redisErr != nil {
					return nil, true, redisErr
				}
				if rows != nil {
					return &QueryConstructorAutoExecutionResult{Mode: "query", Statement: query, Args: copyQueryArgs(args), Rows: rows}, true, nil
				}
				return &QueryConstructorAutoExecutionResult{Mode: "exec", Statement: query, Args: copyQueryArgs(args), Exec: execSummary}, true, nil
			}
			if strings.HasPrefix(trimmed, redisCompiledPipelinePrefix) {
				rows, execSummary, redisErr := redisAdapter.ExecuteCompiledPipelinePlan(ctx, query)
				if redisErr != nil {
					return nil, true, redisErr
				}
				if rows != nil {
					return &QueryConstructorAutoExecutionResult{Mode: "query", Statement: query, Args: copyQueryArgs(args), Rows: rows}, true, nil
				}
				return &QueryConstructorAutoExecutionResult{Mode: "exec", Statement: query, Args: copyQueryArgs(args), Exec: execSummary}, true, nil
			}
			return nil, true, fmt.Errorf("redis query constructor requires compiled plan prefix %q or %q", redisCompiledCommandPrefix, redisCompiledPipelinePrefix)
		},
	})
}

// RedisAdapter Redis 适配器。
//
// Redis 是内存型键值存储，不支持 SQL 接口（Query/Exec/Begin 均返回不支持错误）。
// 业务层应通过 GetRawConn().(redis.UniversalClient) 或类型断言到 *RedisAdapter
// 来使用 Redis 的原生能力（键值操作、哈希、列表、集合、有序集合、发布订阅、管道等）。
type RedisAdapter struct {
	client      redis.UniversalClient
	config      *RedisConnectionConfig
	clusterMode bool
}

// NewRedisAdapter 创建 RedisAdapter（不建立连接）。
func NewRedisAdapter(config *Config) (*RedisAdapter, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	resolved := config.ResolvedRedisConfig()
	return &RedisAdapter{
		config:      resolved,
		clusterMode: resolved.ClusterMode,
	}, nil
}

// Connect 建立 Redis 连接。
func (a *RedisAdapter) Connect(ctx context.Context, config *Config) error {
	if a.client != nil {
		return nil
	}

	cfg := a.config
	if config != nil {
		if err := config.Validate(); err != nil {
			return err
		}
		cfg = config.ResolvedRedisConfig()
		a.config = cfg
		a.clusterMode = cfg.ClusterMode
	}

	client, err := buildRedisClient(cfg)
	if err != nil {
		return err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return fmt.Errorf("redis ping failed: %w", err)
	}

	a.client = client
	return nil
}

// buildRedisClient 根据配置构建 redis.UniversalClient。
func buildRedisClient(cfg *RedisConnectionConfig) (redis.UniversalClient, error) {
	var tlsCfg *tls.Config
	if cfg.TLSEnabled {
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	dialTimeout := time.Duration(cfg.DialTimeout) * time.Second
	readTimeout := time.Duration(cfg.ReadTimeout) * time.Second
	writeTimeout := time.Duration(cfg.WriteTimeout) * time.Second

	if cfg.ClusterMode {
		addrs := cfg.ClusterAddrs
		opts := &redis.ClusterOptions{
			Addrs:        addrs,
			Password:     cfg.Password,
			TLSConfig:    tlsCfg,
			DialTimeout:  dialTimeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
		}
		if cfg.Username != "" {
			opts.Username = cfg.Username
		}
		return redis.NewClusterClient(opts), nil
	}

	// 单机 / Sentinel 模式
	addr := cfg.Host + ":" + fmt.Sprintf("%d", cfg.Port)
	if strings.TrimSpace(cfg.URI) != "" {
		opt, err := redis.ParseURL(cfg.URI)
		if err != nil {
			return nil, fmt.Errorf("redis: invalid uri: %w", err)
		}
		if cfg.TLSEnabled && opt.TLSConfig == nil {
			opt.TLSConfig = tlsCfg
		}
		if cfg.DialTimeout > 0 {
			opt.DialTimeout = dialTimeout
		}
		if cfg.ReadTimeout > 0 {
			opt.ReadTimeout = readTimeout
		}
		if cfg.WriteTimeout > 0 {
			opt.WriteTimeout = writeTimeout
		}
		return redis.NewClient(opt), nil
	}

	opts := &redis.Options{
		Addr:         addr,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		TLSConfig:    tlsCfg,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
	return redis.NewClient(opts), nil
}

// Close 关闭连接。
func (a *RedisAdapter) Close() error {
	if a.client == nil {
		return nil
	}
	err := a.client.Close()
	a.client = nil
	return err
}

// Ping 测试连接健康状态。
func (a *RedisAdapter) Ping(ctx context.Context) error {
	if a.client == nil {
		return fmt.Errorf("redis client not connected")
	}
	return a.client.Ping(ctx).Err()
}

// GetRawConn 返回底层 redis.UniversalClient。
// 业务层可类型断言为 *redis.Client 或 *redis.ClusterClient 使用完整 API。
func (a *RedisAdapter) GetRawConn() interface{} {
	return a.client
}

// Client 返回强类型的 redis.UniversalClient，方便直接操作 Redis 命令。
func (a *RedisAdapter) Client() redis.UniversalClient {
	return a.client
}

// ==================== 不支持的 SQL 接口 ====================

// Query Redis 不支持 SQL 查询。
func (a *RedisAdapter) Query(_ context.Context, _ string, _ ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("redis adapter does not support SQL Query")
}

// QueryRow Redis 不支持 SQL 查询。
func (a *RedisAdapter) QueryRow(_ context.Context, _ string, _ ...interface{}) *sql.Row {
	return nil
}

// Exec Redis 不支持 SQL 执行。
func (a *RedisAdapter) Exec(_ context.Context, _ string, _ ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("redis adapter does not support SQL Exec directly; use REDIS_CMD:: or REDIS_PIPE:: via ExecuteQueryConstructorAuto or transaction wrapper")
}

// Begin 返回基于 MULTI/EXEC 的受控事务包装。
func (a *RedisAdapter) Begin(_ context.Context, _ ...interface{}) (Tx, error) {
	if a.client == nil {
		return nil, fmt.Errorf("redis client not connected")
	}
	return &redisTx{adapter: a, queued: make([]RedisCompiledCommandPlan, 0)}, nil
}

// ==================== 定时任务（降级到应用层） ====================

func (a *RedisAdapter) RegisterScheduledTask(_ context.Context, _ *ScheduledTaskConfig) error {
	return NewScheduledTaskFallbackErrorWithReason("redis", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

func (a *RedisAdapter) UnregisterScheduledTask(_ context.Context, _ string) error {
	return NewScheduledTaskFallbackErrorWithReason("redis", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

func (a *RedisAdapter) ListScheduledTasks(_ context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, NewScheduledTaskFallbackErrorWithReason("redis", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

// ==================== 特性声明 ====================

// GetQueryBuilderProvider Redis 暂不提供 SQL 式查询构造器。
func (a *RedisAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return NewRedisQueryConstructorProvider()
}

// GetDatabaseFeatures 返回 Redis 数据库特性声明。
func (a *RedisAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return NewRedisDatabaseFeatures()
}

// GetQueryFeatures 返回 Redis 查询特性声明。
func (a *RedisAdapter) GetQueryFeatures() *QueryFeatures {
	return NewRedisQueryFeatures()
}

// ==================== 键值操作 ====================

// Get 获取键对应的字符串值。
func (a *RedisAdapter) Get(ctx context.Context, key string) (string, error) {
	return a.client.Get(ctx, key).Result()
}

// Set 设置键值，ttl=0 表示永不过期。
func (a *RedisAdapter) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return a.client.Set(ctx, key, value, ttl).Err()
}

// SetNX 仅当键不存在时设置值（Set if Not eXists）。
func (a *RedisAdapter) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) (bool, error) {
	return a.client.SetNX(ctx, key, value, ttl).Result()
}

// Del 删除一个或多个键。
func (a *RedisAdapter) Del(ctx context.Context, keys ...string) (int64, error) {
	return a.client.Del(ctx, keys...).Result()
}

// Exists 检查键是否存在，返回存在的键的数量。
func (a *RedisAdapter) Exists(ctx context.Context, keys ...string) (int64, error) {
	return a.client.Exists(ctx, keys...).Result()
}

// Expire 为键设置过期时间。
func (a *RedisAdapter) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return a.client.Expire(ctx, key, ttl).Result()
}

// TTL 返回键的剩余过期时间。
func (a *RedisAdapter) TTL(ctx context.Context, key string) (time.Duration, error) {
	return a.client.TTL(ctx, key).Result()
}

// Incr 将键的整数值加 1。
func (a *RedisAdapter) Incr(ctx context.Context, key string) (int64, error) {
	return a.client.Incr(ctx, key).Result()
}

// IncrBy 将键的整数值加上增量 delta。
func (a *RedisAdapter) IncrBy(ctx context.Context, key string, delta int64) (int64, error) {
	return a.client.IncrBy(ctx, key, delta).Result()
}

// Decr 将键的整数值减 1。
func (a *RedisAdapter) Decr(ctx context.Context, key string) (int64, error) {
	return a.client.Decr(ctx, key).Result()
}

// MGet 批量获取多个键值。
func (a *RedisAdapter) MGet(ctx context.Context, keys ...string) ([]interface{}, error) {
	return a.client.MGet(ctx, keys...).Result()
}

// MSet 批量设置多个键值。
func (a *RedisAdapter) MSet(ctx context.Context, pairs ...interface{}) error {
	return a.client.MSet(ctx, pairs...).Err()
}

// NamespacedKey 构建带命名空间的键。
func (a *RedisAdapter) NamespacedKey(prefix, key string) string {
	prefix = strings.TrimSpace(prefix)
	key = strings.TrimSpace(key)
	if prefix == "" {
		return key
	}
	if key == "" {
		return prefix
	}
	return prefix + ":" + key
}

// RegisterTagKeys 维护 tag -> keys 的最小失效集合。
func (a *RedisAdapter) RegisterTagKeys(ctx context.Context, tag string, keys ...string) error {
	if strings.TrimSpace(tag) == "" || len(keys) == 0 {
		return nil
	}
	members := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		if trimmed := strings.TrimSpace(key); trimmed != "" {
			members = append(members, trimmed)
		}
	}
	if len(members) == 0 {
		return nil
	}
	return a.client.SAdd(ctx, a.NamespacedKey("tag", tag), members...).Err()
}

// InvalidateTag 删除 tag 关联的所有键，并清理 tag 集合。
func (a *RedisAdapter) InvalidateTag(ctx context.Context, tag string) (int64, error) {
	setKey := a.NamespacedKey("tag", tag)
	keys, err := a.client.SMembers(ctx, setKey).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	deleted := int64(0)
	if len(keys) > 0 {
		rows, delErr := a.client.Del(ctx, keys...).Result()
		if delErr != nil {
			return deleted, delErr
		}
		deleted += rows
	}
	rows, err := a.client.Del(ctx, setKey).Result()
	if err != nil {
		return deleted, err
	}
	deleted += rows
	return deleted, nil
}

// ==================== Hash 操作 ====================

// HGet 获取哈希表中指定字段的值。
func (a *RedisAdapter) HGet(ctx context.Context, key, field string) (string, error) {
	return a.client.HGet(ctx, key, field).Result()
}

// HSet 设置哈希表中一个或多个字段。values 格式为 field, value, field, value ...
func (a *RedisAdapter) HSet(ctx context.Context, key string, values ...interface{}) error {
	return a.client.HSet(ctx, key, values...).Err()
}

// HGetAll 获取哈希表的所有字段和值。
func (a *RedisAdapter) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return a.client.HGetAll(ctx, key).Result()
}

// HDel 删除哈希表中一个或多个字段。
func (a *RedisAdapter) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	return a.client.HDel(ctx, key, fields...).Result()
}

// HExists 判断哈希表中指定字段是否存在。
func (a *RedisAdapter) HExists(ctx context.Context, key, field string) (bool, error) {
	return a.client.HExists(ctx, key, field).Result()
}

// ==================== List 操作 ====================

// LPush 在列表头部插入一个或多个值。
func (a *RedisAdapter) LPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return a.client.LPush(ctx, key, values...).Result()
}

// RPush 在列表尾部插入一个或多个值。
func (a *RedisAdapter) RPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return a.client.RPush(ctx, key, values...).Result()
}

// LPop 移除并返回列表头部的元素。
func (a *RedisAdapter) LPop(ctx context.Context, key string) (string, error) {
	return a.client.LPop(ctx, key).Result()
}

// RPop 移除并返回列表尾部的元素。
func (a *RedisAdapter) RPop(ctx context.Context, key string) (string, error) {
	return a.client.RPop(ctx, key).Result()
}

// LRange 获取列表中指定范围的元素，start/stop 为 0-based 索引，-1 表示最后一个元素。
func (a *RedisAdapter) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return a.client.LRange(ctx, key, start, stop).Result()
}

// LLen 获取列表长度。
func (a *RedisAdapter) LLen(ctx context.Context, key string) (int64, error) {
	return a.client.LLen(ctx, key).Result()
}

// ==================== Set 操作 ====================

// SAdd 向集合添加一个或多个成员。
func (a *RedisAdapter) SAdd(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return a.client.SAdd(ctx, key, members...).Result()
}

// SRem 从集合中移除一个或多个成员。
func (a *RedisAdapter) SRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return a.client.SRem(ctx, key, members...).Result()
}

// SMembers 返回集合中的所有成员。
func (a *RedisAdapter) SMembers(ctx context.Context, key string) ([]string, error) {
	return a.client.SMembers(ctx, key).Result()
}

// SIsMember 判断 member 是否为集合 key 的成员。
func (a *RedisAdapter) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	return a.client.SIsMember(ctx, key, member).Result()
}

// SCard 返回集合的元素数量。
func (a *RedisAdapter) SCard(ctx context.Context, key string) (int64, error) {
	return a.client.SCard(ctx, key).Result()
}

// ==================== Sorted Set 操作 ====================

// ZAdd 向有序集合添加一个或多个成员。
func (a *RedisAdapter) ZAdd(ctx context.Context, key string, members ...redis.Z) (int64, error) {
	return a.client.ZAdd(ctx, key, members...).Result()
}

// ZRange 返回有序集合中按分数从低到高的指定范围成员。
func (a *RedisAdapter) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return a.client.ZRange(ctx, key, start, stop).Result()
}

// ZRangeByScore 返回有序集合中分数在 min 到 max 之间的成员。
func (a *RedisAdapter) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	return a.client.ZRangeByScore(ctx, key, opt).Result()
}

// ZRem 移除有序集合中一个或多个成员。
func (a *RedisAdapter) ZRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return a.client.ZRem(ctx, key, members...).Result()
}

// ZScore 返回有序集合中指定成员的分数。
func (a *RedisAdapter) ZScore(ctx context.Context, key string, member string) (float64, error) {
	return a.client.ZScore(ctx, key, member).Result()
}

// ZCard 返回有序集合的元素数量。
func (a *RedisAdapter) ZCard(ctx context.Context, key string) (int64, error) {
	return a.client.ZCard(ctx, key).Result()
}

// ==================== 发布订阅 ====================

// Publish 向频道发布消息，返回接收到消息的订阅者数量。
func (a *RedisAdapter) Publish(ctx context.Context, channel string, message interface{}) (int64, error) {
	return a.client.Publish(ctx, channel, message).Result()
}

// Subscribe 订阅一个或多个频道，返回 *redis.PubSub。
// 调用方负责在使用完毕后调用 pubSub.Close()。
func (a *RedisAdapter) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return a.client.Subscribe(ctx, channels...)
}

// PSubscribe 按模式订阅一个或多个频道。
// 调用方负责在使用完毕后调用 pubSub.Close()。
func (a *RedisAdapter) PSubscribe(ctx context.Context, patterns ...string) *redis.PubSub {
	return a.client.PSubscribe(ctx, patterns...)
}

// ==================== 管道 / 事务 ====================

// Pipelined 在管道中批量执行命令，减少网络往返。
func (a *RedisAdapter) Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	return a.client.Pipelined(ctx, fn)
}

// TxPipelined 在 MULTI/EXEC 事务管道中批量执行命令。
func (a *RedisAdapter) TxPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	return a.client.TxPipelined(ctx, fn)
}

// ExecuteCompiledCommandPlan 执行 REDIS_CMD:: 原生命令计划。
func (a *RedisAdapter) ExecuteCompiledCommandPlan(ctx context.Context, query string) ([]map[string]interface{}, *QueryConstructorExecSummary, error) {
	plan, err := parseRedisCompiledCommandPlan(query)
	if err != nil {
		return nil, nil, err
	}
	result, err := a.client.Do(ctx, append([]interface{}{plan.Command}, plan.Args...)...).Result()
	if err != nil {
		return nil, nil, err
	}
	if plan.ReadOnly {
		return []map[string]interface{}{{"command": plan.Command, "result": normalizeRedisResult(result)}}, nil, nil
	}
	return nil, &QueryConstructorExecSummary{RowsAffected: estimateRedisRowsAffected(result), Counters: map[string]int{"commands_executed": 1}}, nil
}

// ExecuteCompiledPipelinePlan 执行 REDIS_PIPE:: 原生命令计划。
func (a *RedisAdapter) ExecuteCompiledPipelinePlan(ctx context.Context, query string) ([]map[string]interface{}, *QueryConstructorExecSummary, error) {
	plan, err := parseRedisCompiledPipelinePlan(query)
	if err != nil {
		return nil, nil, err
	}
	rows := make([]map[string]interface{}, 0, len(plan.Commands))
	cmders, err := a.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, cmd := range plan.Commands {
			pipe.Do(ctx, append([]interface{}{cmd.Command}, cmd.Args...)...)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if plan.ReadOnly {
		for i, cmd := range cmders {
			rows = append(rows, map[string]interface{}{"index": i, "command": plan.Commands[i].Command, "result": normalizeRedisResult(cmd.(*redis.Cmd).Val())})
		}
		return rows, nil, nil
	}
	return nil, &QueryConstructorExecSummary{RowsAffected: int64(len(cmders)), Counters: map[string]int{"commands_executed": len(cmders)}}, nil
}

type redisExecResult struct {
	rows int64
}

func (r *redisExecResult) LastInsertId() (int64, error) { return 0, nil }
func (r *redisExecResult) RowsAffected() (int64, error) { return r.rows, nil }

type redisTx struct {
	adapter *RedisAdapter
	queued  []RedisCompiledCommandPlan
	closed  bool
}

func (tx *redisTx) Commit(ctx context.Context) error {
	if tx.closed {
		return nil
	}
	defer func() { tx.closed = true }()
	if len(tx.queued) == 0 {
		return nil
	}
	_, err := tx.adapter.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, cmd := range tx.queued {
			pipe.Do(ctx, append([]interface{}{cmd.Command}, cmd.Args...)...)
		}
		return nil
	})
	return err
}

func (tx *redisTx) Rollback(ctx context.Context) error {
	tx.queued = nil
	tx.closed = true
	return nil
}

func (tx *redisTx) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("redis transaction Query is not supported; use native read plans outside Tx")
}

func (tx *redisTx) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}

func (tx *redisTx) Exec(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
	if tx.closed {
		return nil, fmt.Errorf("redis transaction already closed")
	}
	if strings.HasPrefix(strings.TrimSpace(statement), redisCompiledCommandPrefix) {
		plan, err := parseRedisCompiledCommandPlan(statement)
		if err != nil {
			return nil, err
		}
		tx.queued = append(tx.queued, *plan)
		return &redisExecResult{rows: 1}, nil
	}
	if strings.HasPrefix(strings.TrimSpace(statement), redisCompiledPipelinePrefix) {
		plan, err := parseRedisCompiledPipelinePlan(statement)
		if err != nil {
			return nil, err
		}
		tx.queued = append(tx.queued, plan.Commands...)
		return &redisExecResult{rows: int64(len(plan.Commands))}, nil
	}
	return nil, fmt.Errorf("redis transaction Exec requires %s or %s compiled plans", redisCompiledCommandPrefix, redisCompiledPipelinePrefix)
}

func parseRedisCompiledCommandPlan(query string) (*RedisCompiledCommandPlan, error) {
	payload := strings.TrimPrefix(strings.TrimSpace(query), redisCompiledCommandPrefix)
	if payload == strings.TrimSpace(query) {
		return nil, fmt.Errorf("redis compiled command requires prefix %s", redisCompiledCommandPrefix)
	}
	var plan RedisCompiledCommandPlan
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		return nil, err
	}
	plan.Command = strings.ToUpper(strings.TrimSpace(plan.Command))
	if plan.Command == "" {
		return nil, fmt.Errorf("redis compiled command plan command cannot be empty")
	}
	if !plan.ReadOnly {
		plan.ReadOnly = redisCommandIsReadOnly(plan.Command)
	}
	return &plan, nil
}

func parseRedisCompiledPipelinePlan(query string) (*RedisCompiledPipelinePlan, error) {
	payload := strings.TrimPrefix(strings.TrimSpace(query), redisCompiledPipelinePrefix)
	if payload == strings.TrimSpace(query) {
		return nil, fmt.Errorf("redis compiled pipeline requires prefix %s", redisCompiledPipelinePrefix)
	}
	var plan RedisCompiledPipelinePlan
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		return nil, err
	}
	if len(plan.Commands) == 0 {
		return nil, fmt.Errorf("redis compiled pipeline plan requires at least one command")
	}
	readOnly := true
	for i := range plan.Commands {
		plan.Commands[i].Command = strings.ToUpper(strings.TrimSpace(plan.Commands[i].Command))
		if plan.Commands[i].Command == "" {
			return nil, fmt.Errorf("redis compiled pipeline command cannot be empty")
		}
		if !plan.Commands[i].ReadOnly {
			plan.Commands[i].ReadOnly = redisCommandIsReadOnly(plan.Commands[i].Command)
		}
		if !plan.Commands[i].ReadOnly {
			readOnly = false
		}
	}
	plan.ReadOnly = readOnly
	return &plan, nil
}

func normalizeRedisResult(result interface{}) interface{} {
	switch v := result.(type) {
	case []interface{}:
		out := make([]interface{}, 0, len(v))
		for _, item := range v {
			out = append(out, normalizeRedisResult(item))
		}
		return out
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(v))
		for key, value := range v {
			out[fmt.Sprint(key)] = normalizeRedisResult(value)
		}
		return out
	default:
		return v
	}
}

func estimateRedisRowsAffected(result interface{}) int64 {
	switch v := result.(type) {
	case int64:
		return v
	case []interface{}:
		return int64(len(v))
	default:
		return 1
	}
}
