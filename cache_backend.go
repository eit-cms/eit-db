package db

import (
	"context"
	"time"
)

// CacheLevel 表示缓存所在层级。
type CacheLevel int

const (
	// CacheLevelL1 进程内缓存（如 Ristretto），速度最快，无网络开销，进程重启后失效。
	CacheLevelL1 CacheLevel = 1
	// CacheLevelL2 共享分布式缓存（如 Redis），多实例共享，重启后保留。
	CacheLevelL2 CacheLevel = 2
	// CacheLevelL3 适配器原生持久化缓存（如物化视图、预聚合表），最重，一致性最强。
	CacheLevelL3 CacheLevel = 3
)

// CacheHitSource 标记缓存命中来源层级。空字符串表示未命中（来自实际查询）。
type CacheHitSource string

const (
	// CacheHitNone 未命中任何缓存层，结果来自实际查询。
	CacheHitNone CacheHitSource = ""
	// CacheHitL1 命中进程内缓存（L1）。
	CacheHitL1 CacheHitSource = "L1"
	// CacheHitL2 命中共享分布式缓存（L2）。
	CacheHitL2 CacheHitSource = "L2"
	// CacheHitL3 命中适配器原生持久化缓存（L3）。
	CacheHitL3 CacheHitSource = "L3"
)

// CacheGetResult 单个 key 的读取结果。
type CacheGetResult struct {
	// Key 是查询的缓存键。
	Key string
	// Value 是缓存的序列化值（[]byte）。Hit=false 时为 nil。
	Value []byte
	// Hit 表示该 key 是否命中缓存。
	Hit bool
	// Source 标记命中来源层级，Hit=false 时为 CacheHitNone。
	Source CacheHitSource
}

// CacheOptions 控制缓存读写行为的选项。
type CacheOptions struct {
	// TTL 写入 TTL，0 表示不过期（永久保留，直到手动失效或被驱逐）。
	TTL time.Duration

	// Tags 写入时关联的失效标签，用于 InvalidateByTag 批量失效。
	Tags []string

	// Levels 限定只使用指定层级，nil 或空表示使用所有已配置的层级。
	Levels []CacheLevel

	// SkipBackfill 命中低层时禁止向高层回填（默认允许回填）。
	SkipBackfill bool
}

// CacheBackendStats 缓存后端统计快照。
type CacheBackendStats struct {
	// Level 该后端所处的缓存层级。
	Level CacheLevel
	// Hits 成功命中次数。
	Hits uint64
	// Misses 未命中次数。
	Misses uint64
	// Entries 当前缓存条目数（-1 表示不支持）。
	Entries int64
	// Errors 操作出错次数。
	Errors uint64
	// Extra 后端自定义扩展字段（如 ristretto 的 CostEvicted 等）。
	Extra map[string]interface{}
}

// CacheBackend 统一缓存后端接口，支持 L1/L2/L3 多层体系。
//
// 使用约定：
//   - Get/Set/MGet 的 value 均为 []byte，序列化与反序列化由调用方负责。
//   - InvalidateByTag 需尽力失效；不保证强一致，失败时返回 error 而非静默丢弃。
//   - Stats 可返回快照数据，不要求实时精确。
//   - 多个 CacheBackend 可通过 LayeredCacheBackend 组合为多层链路。
type CacheBackend interface {
	// Get 读取单个 key，返回 (value, hit, error)。
	// key 不存在时返回 nil, false, nil。
	Get(ctx context.Context, key string) ([]byte, bool, error)

	// Set 写入单个 key，ttl=0 表示不过期；tags 用于后续批量失效。
	Set(ctx context.Context, key string, value []byte, ttl time.Duration, tags ...string) error

	// MGet 批量读取，返回结果切片与 keys 顺序对应。
	MGet(ctx context.Context, keys ...string) ([]CacheGetResult, error)

	// Del 删除指定 keys。
	Del(ctx context.Context, keys ...string) error

	// InvalidateByTag 按标签批量失效关联的所有 keys。
	InvalidateByTag(ctx context.Context, tags ...string) error

	// Stats 返回当前后端统计快照。
	Stats() CacheBackendStats

	// Level 返回该后端所处的缓存层级。
	Level() CacheLevel
}
