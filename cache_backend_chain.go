package db

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	ristretto "github.com/dgraph-io/ristretto/v2"
)

// ==================== RistrettoCacheBackend (L1) ====================

const (
	defaultRistrettoResultCacheMaxEntries = 1024
)

// RistrettoCacheBackend 基于 ristretto 实现 L1 进程内缓存后端。
// 存储原始 []byte 值，与编译查询缓存（CompiledQueryCache）互相独立。
type RistrettoCacheBackend struct {
	entries *ristretto.Cache[string, []byte]
	ttl     time.Duration
	hits    atomic.Uint64
	misses  atomic.Uint64
	errors  atomic.Uint64
}

// NewRistrettoCacheBackend 创建默认容量（1024 条）的 L1 缓存后端。
func NewRistrettoCacheBackend() *RistrettoCacheBackend {
	return NewRistrettoCacheBackendWithOptions(defaultRistrettoResultCacheMaxEntries, 0)
}

// NewRistrettoCacheBackendWithOptions 创建指定最大条目数与默认 TTL 的 L1 缓存后端。
// maxEntries<=0 时使用默认值；defaultTTL=0 表示不过期。
func NewRistrettoCacheBackendWithOptions(maxEntries int, defaultTTL time.Duration) *RistrettoCacheBackend {
	if maxEntries <= 0 {
		maxEntries = defaultRistrettoResultCacheMaxEntries
	}
	numCounters := int64(maxEntries * 10)
	if numCounters < 1000 {
		numCounters = 1000
	}
	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters:        numCounters,
		MaxCost:            int64(maxEntries),
		BufferItems:        64,
		Metrics:            true,
		IgnoreInternalCost: true,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to initialize ristretto result cache: %v", err))
	}
	return &RistrettoCacheBackend{entries: cache, ttl: defaultTTL}
}

// Level 返回 CacheLevelL1。
func (b *RistrettoCacheBackend) Level() CacheLevel { return CacheLevelL1 }

// Get 读取单个 key。
func (b *RistrettoCacheBackend) Get(_ context.Context, key string) ([]byte, bool, error) {
	val, ok := b.entries.Get(key)
	if !ok {
		b.misses.Add(1)
		return nil, false, nil
	}
	b.hits.Add(1)
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, true, nil
}

// Set 写入单个 key。ristretto 不原生支持 tag，tags 在 L1 层会被忽略（标签失效通过 Del 配合上层实现）。
func (b *RistrettoCacheBackend) Set(_ context.Context, key string, value []byte, ttl time.Duration, _ ...string) error {
	cp := make([]byte, len(value))
	copy(cp, value)
	effective := ttl
	if effective == 0 {
		effective = b.ttl
	}
	if effective > 0 {
		b.entries.SetWithTTL(key, cp, 1, effective)
	} else {
		b.entries.Set(key, cp, 1)
	}
	b.entries.Wait()
	return nil
}

// MGet 批量读取。
func (b *RistrettoCacheBackend) MGet(_ context.Context, keys ...string) ([]CacheGetResult, error) {
	results := make([]CacheGetResult, len(keys))
	for i, key := range keys {
		results[i].Key = key
		val, ok := b.entries.Get(key)
		if ok {
			cp := make([]byte, len(val))
			copy(cp, val)
			results[i].Value = cp
			results[i].Hit = true
			results[i].Source = CacheHitL1
			b.hits.Add(1)
		} else {
			results[i].Source = CacheHitNone
			b.misses.Add(1)
		}
	}
	return results, nil
}

// Del 删除指定 keys。
func (b *RistrettoCacheBackend) Del(_ context.Context, keys ...string) error {
	for _, key := range keys {
		b.entries.Del(key)
	}
	return nil
}

// InvalidateByTag L1 不维护 tag 索引，调用此方法为空操作并返回 nil。
// 标签失效需通过 LayeredCacheBackend 统一由支持标签的层（如 L2）执行。
func (b *RistrettoCacheBackend) InvalidateByTag(_ context.Context, _ ...string) error {
	return nil
}

// Stats 返回当前 L1 统计快照。
func (b *RistrettoCacheBackend) Stats() CacheBackendStats {
	s := CacheBackendStats{
		Level:  CacheLevelL1,
		Hits:   b.hits.Load(),
		Misses: b.misses.Load(),
		Errors: b.errors.Load(),
	}
	if b.entries.Metrics != nil {
		s.Extra = map[string]interface{}{
			"hit_ratio":    b.entries.Metrics.Ratio(),
			"cost_added":   b.entries.Metrics.CostAdded(),
			"cost_evicted": b.entries.Metrics.CostEvicted(),
			"keys_evicted": b.entries.Metrics.KeysEvicted(),
			"remaining":    b.entries.RemainingCost(),
			"max_cost":     b.entries.MaxCost(),
		}
	}
	return s
}

// Close 释放底层 ristretto 缓存资源。
func (b *RistrettoCacheBackend) Close() {
	b.entries.Close()
}

// ==================== LayeredCacheBackend ====================

// LayeredCacheBackend 将多个 CacheBackend 组合为多层链路（L1 → L2 → L3）。
//
// 读取策略（Read-through with backfill）：
//   - 按 backends 顺序依次查找，命中时将值回填到所有更高层级（index 更小的后端）。
//   - SkipBackfill=true 时禁止回填。
//
// 写入策略（Write-through）：
//   - Set/Del/InvalidateByTag 默认写入所有层；CacheOptions.Levels 可限定范围。
type LayeredCacheBackend struct {
	backends []CacheBackend // 按层级从低到高排列（index 0 = L1）
}

// NewLayeredCacheBackend 创建多层缓存链路。backends 应按层级从低到高传入（L1 first）。
// 至少需要一个 backend；传入 nil 的 backend 会被静默过滤。
func NewLayeredCacheBackend(backends ...CacheBackend) *LayeredCacheBackend {
	filtered := make([]CacheBackend, 0, len(backends))
	for _, b := range backends {
		if b != nil {
			filtered = append(filtered, b)
		}
	}
	if len(filtered) == 0 {
		panic("LayeredCacheBackend: at least one non-nil backend is required")
	}
	return &LayeredCacheBackend{backends: filtered}
}

// Level 返回最低层级（第一个 backend 的层级）。
func (l *LayeredCacheBackend) Level() CacheLevel {
	return l.backends[0].Level()
}

// Get 按 L1→L2→L3 顺序读取，命中时回填更高层。
func (l *LayeredCacheBackend) Get(ctx context.Context, key string) ([]byte, bool, error) {
	for i, b := range l.backends {
		val, hit, err := b.Get(ctx, key)
		if err != nil {
			return nil, false, fmt.Errorf("cache[L%d].Get: %w", int(b.Level()), err)
		}
		if hit {
			// 回填：将值写回比命中层更高的层（index 更小的层）
			l.backfill(ctx, key, val, 0, i)
			return val, true, nil
		}
	}
	return nil, false, nil
}

// GetWithSource 与 Get 相同，但额外返回命中来源层级。
func (l *LayeredCacheBackend) GetWithSource(ctx context.Context, key string) ([]byte, bool, CacheHitSource, error) {
	for i, b := range l.backends {
		val, hit, err := b.Get(ctx, key)
		if err != nil {
			return nil, false, CacheHitNone, fmt.Errorf("cache[L%d].Get: %w", int(b.Level()), err)
		}
		if hit {
			l.backfill(ctx, key, val, 0, i)
			return val, true, cacheLevelToSource(b.Level()), nil
		}
	}
	return nil, false, CacheHitNone, nil
}

// backfill 将 value 回填到 backends[startIdx..endIdx-1]（不含 endIdx）。
func (l *LayeredCacheBackend) backfill(ctx context.Context, key string, value []byte, startIdx, endIdx int) {
	for i := startIdx; i < endIdx && i < len(l.backends); i++ {
		// 回填不带 TTL（由各层自身默认 TTL 控制），忽略错误（尽力回填）
		_ = l.backends[i].Set(ctx, key, value, 0)
	}
}

// Set 写入所有层（write-through）。
func (l *LayeredCacheBackend) Set(ctx context.Context, key string, value []byte, ttl time.Duration, tags ...string) error {
	var lastErr error
	for _, b := range l.backends {
		if err := b.Set(ctx, key, value, ttl, tags...); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// MGet 批量读取，尽量从低层（L1）命中，未命中的 key 继续向下查询并回填。
func (l *LayeredCacheBackend) MGet(ctx context.Context, keys ...string) ([]CacheGetResult, error) {
	results := make([]CacheGetResult, len(keys))
	for i, k := range keys {
		results[i].Key = k
	}

	remaining := make([]int, len(keys)) // original index
	for i := range keys {
		remaining[i] = i
	}

	for _, b := range l.backends {
		if len(remaining) == 0 {
			break
		}
		queryKeys := make([]string, len(remaining))
		for i, idx := range remaining {
			queryKeys[i] = keys[idx]
		}
		batchResults, err := b.MGet(ctx, queryKeys...)
		if err != nil {
			return nil, fmt.Errorf("cache[L%d].MGet: %w", int(b.Level()), err)
		}
		var stillMissing []int
		for i, br := range batchResults {
			origIdx := remaining[i]
			if br.Hit {
				results[origIdx] = CacheGetResult{
					Key:    keys[origIdx],
					Value:  br.Value,
					Hit:    true,
					Source: cacheLevelToSource(b.Level()),
				}
			} else {
				stillMissing = append(stillMissing, origIdx)
			}
		}
		remaining = stillMissing
	}
	return results, nil
}

// Del 从所有层删除指定 keys。
func (l *LayeredCacheBackend) Del(ctx context.Context, keys ...string) error {
	var lastErr error
	for _, b := range l.backends {
		if err := b.Del(ctx, keys...); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// InvalidateByTag 向所有层执行标签失效。
// 不支持标签的层（如 RistrettoCacheBackend）会返回 nil，不影响其他层。
func (l *LayeredCacheBackend) InvalidateByTag(ctx context.Context, tags ...string) error {
	var lastErr error
	for _, b := range l.backends {
		if err := b.InvalidateByTag(ctx, tags...); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Stats 返回所有层统计的聚合切片（按 backend 顺序）。
// 注意：Stats() 签名遵循 CacheBackend 接口，仅返回第一层统计；
// 如需全层统计，使用 AllStats()。
func (l *LayeredCacheBackend) Stats() CacheBackendStats {
	return l.backends[0].Stats()
}

// AllStats 返回所有层的统计快照切片。
func (l *LayeredCacheBackend) AllStats() []CacheBackendStats {
	stats := make([]CacheBackendStats, len(l.backends))
	for i, b := range l.backends {
		stats[i] = b.Stats()
	}
	return stats
}

// cacheLevelToSource 将 CacheLevel 转换为 CacheHitSource 字符串。
func cacheLevelToSource(level CacheLevel) CacheHitSource {
	switch level {
	case CacheLevelL1:
		return CacheHitL1
	case CacheLevelL2:
		return CacheHitL2
	case CacheLevelL3:
		return CacheHitL3
	default:
		return CacheHitNone
	}
}
