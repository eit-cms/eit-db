package db

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrCacheBackendNotConfigured 表示缓存后端未初始化或未配置，操作无法执行。
var ErrCacheBackendNotConfigured = errors.New("cache backend: adapter not configured")

// RedisCacheBackend 基于 RedisAdapter 实现 L2 分布式缓存后端。
//
// 使用 RegisterTagKeys + InvalidateTag 实现标签失效；
// 使用 RedisAdapter.MGet 实现批量读取。
type RedisCacheBackend struct {
	adapter *RedisAdapter
	hits    atomic.Uint64
	misses  atomic.Uint64
	errors  atomic.Uint64
}

// NewRedisCacheBackend 创建 Redis L2 缓存后端。
// adapter 为 nil 时，所有操作返回 ErrCacheBackendNotConfigured。
func NewRedisCacheBackend(adapter *RedisAdapter) *RedisCacheBackend {
	return &RedisCacheBackend{adapter: adapter}
}

// Level 返回 CacheLevelL2。
func (b *RedisCacheBackend) Level() CacheLevel { return CacheLevelL2 }

// Get 读取单个 key。key 不存在（redis.Nil）时返回 nil, false, nil。
func (b *RedisCacheBackend) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if b.adapter == nil {
		return nil, false, ErrCacheBackendNotConfigured
	}
	val, err := b.adapter.Get(ctx, key)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			b.misses.Add(1)
			return nil, false, nil
		}
		b.errors.Add(1)
		return nil, false, err
	}
	b.hits.Add(1)
	return []byte(val), true, nil
}

// Set 写入单个 key，ttl=0 表示不过期；tags 用于批量失效。
func (b *RedisCacheBackend) Set(ctx context.Context, key string, value []byte, ttl time.Duration, tags ...string) error {
	if b.adapter == nil {
		return ErrCacheBackendNotConfigured
	}
	if err := b.adapter.Set(ctx, key, value, ttl); err != nil {
		b.errors.Add(1)
		return err
	}
	if len(tags) > 0 {
		if err := b.adapter.RegisterTagKeys(ctx, tags[0], key); err != nil {
			b.errors.Add(1)
			return fmt.Errorf("cache backend: register tag keys failed: %w", err)
		}
		// 多 tag 逐一注册
		for _, tag := range tags[1:] {
			if err := b.adapter.RegisterTagKeys(ctx, tag, key); err != nil {
				b.errors.Add(1)
				return fmt.Errorf("cache backend: register tag keys failed: %w", err)
			}
		}
	}
	return nil
}

// MGet 批量读取，返回结果切片与 keys 顺序对应。
func (b *RedisCacheBackend) MGet(ctx context.Context, keys ...string) ([]CacheGetResult, error) {
	if b.adapter == nil {
		return nil, ErrCacheBackendNotConfigured
	}
	raw, err := b.adapter.MGet(ctx, keys...)
	if err != nil {
		b.errors.Add(1)
		return nil, err
	}
	results := make([]CacheGetResult, len(keys))
	for i, v := range raw {
		results[i].Key = keys[i]
		if v != nil {
			var data []byte
			switch tv := v.(type) {
			case string:
				data = []byte(tv)
			case []byte:
				data = tv
			default:
				data = []byte(fmt.Sprintf("%v", tv))
			}
			results[i].Value = data
			results[i].Hit = true
			results[i].Source = CacheHitL2
			b.hits.Add(1)
		} else {
			results[i].Source = CacheHitNone
			b.misses.Add(1)
		}
	}
	return results, nil
}

// Del 删除指定 keys。
func (b *RedisCacheBackend) Del(ctx context.Context, keys ...string) error {
	if b.adapter == nil {
		return ErrCacheBackendNotConfigured
	}
	if _, err := b.adapter.Del(ctx, keys...); err != nil {
		b.errors.Add(1)
		return err
	}
	return nil
}

// InvalidateByTag 按标签批量失效关联的所有 keys。
// 多个 tag 逐一失效；任一失败时返回最后一个错误，其余 tag 仍会继续尝试。
func (b *RedisCacheBackend) InvalidateByTag(ctx context.Context, tags ...string) error {
	if b.adapter == nil {
		return ErrCacheBackendNotConfigured
	}
	var lastErr error
	for _, tag := range tags {
		if _, err := b.adapter.InvalidateTag(ctx, tag); err != nil {
			b.errors.Add(1)
			lastErr = err
		}
	}
	return lastErr
}

// Stats 返回当前 Redis 后端统计快照。
func (b *RedisCacheBackend) Stats() CacheBackendStats {
	return CacheBackendStats{
		Level:   CacheLevelL2,
		Hits:    b.hits.Load(),
		Misses:  b.misses.Load(),
		Entries: -1, // Redis 不提供进程内条目计数
		Errors:  b.errors.Load(),
	}
}
