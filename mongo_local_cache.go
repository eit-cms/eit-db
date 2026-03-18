package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoLocalCache 提供"预加载集合到内存"的本地缓存能力，
// 用于替代 MongoDB 代价高昂的 $lookup 连表操作和全文搜索预热。
//
// 典型场景：
//   - 字典表、角色权限表等体积小且读多写少的集合：整体预加载后做应用层 JOIN，
//     避免每次请求都触发 $lookup aggregation。
//   - 应用层全文搜索预热：把需要搜索的文档拉进内存，配合 fuzzy 搜索库使用。
//
// 并发安全：内部使用 sync.RWMutex，多读单写，读路径无锁争用。
//
// 示例：
//
//	cache, ok := db.GetMongoLocalCache(repo.GetAdapter())
//	_ = cache.Preload(ctx, "roles", "roles", nil, nil, 10*time.Minute)
//	roles, _ := cache.Get("roles")
//
//	// 应用层 JOIN：把用户列表的 role_id 字段和已缓存的 roles 集合关联
//	enriched := cache.JoinWith(users, "role_id", "roles", "_id", "role", false)
type MongoLocalCache struct {
	adapter *MongoAdapter
	mu      sync.RWMutex
	entries map[string]*mongoLocalCacheEntry
}

type mongoLocalCacheEntry struct {
	docs      []bson.M
	fetchedAt time.Time
	ttl       time.Duration
}

func (e *mongoLocalCacheEntry) isStale() bool {
	if e.ttl <= 0 {
		return false // ttl <= 0 表示永不自动过期
	}
	return time.Since(e.fetchedAt) > e.ttl
}

// GetMongoLocalCache 从 Adapter 获取 MongoLocalCache。
// 若 adapter 不是 *MongoAdapter，返回 (nil, false)。
func GetMongoLocalCache(adapter Adapter) (*MongoLocalCache, bool) {
	m, ok := adapter.(*MongoAdapter)
	if !ok {
		return nil, false
	}
	return &MongoLocalCache{
		adapter: m,
		entries: make(map[string]*mongoLocalCacheEntry),
	}, true
}

// Preload 按需预加载集合数据到本地内存（若缓存未过期则直接复用，不发起网络请求）。
//
//   - name: 本地缓存标识符（可与集合名不同，用于 JoinWith 等 API 的 cacheName 参数）
//   - collection: MongoDB 集合名
//   - filter: BSON 过滤条件（nil 表示加载集合全量文档）
//   - projection: BSON 字段投影（nil 表示返回全部字段）
//   - ttl: 缓存有效期（<= 0 表示永不自动过期）
func (c *MongoLocalCache) Preload(ctx context.Context, name, collection string, filter, projection bson.D, ttl time.Duration) error {
	c.mu.RLock()
	entry, exists := c.entries[name]
	c.mu.RUnlock()

	if exists && !entry.isStale() {
		return nil // 缓存命中且未过期，直接返回
	}
	return c.fetchAndStore(ctx, name, collection, filter, projection, ttl)
}

// ForcePreload 无视缓存状态，强制重新从 MongoDB 拉取数据并刷新本地缓存。
func (c *MongoLocalCache) ForcePreload(ctx context.Context, name, collection string, filter, projection bson.D, ttl time.Duration) error {
	return c.fetchAndStore(ctx, name, collection, filter, projection, ttl)
}

func (c *MongoLocalCache) fetchAndStore(ctx context.Context, name, collection string, filter, projection bson.D, ttl time.Duration) error {
	if c.adapter.client == nil {
		return fmt.Errorf("mongodb: not connected")
	}
	if filter == nil {
		filter = bson.D{}
	}

	findOpts := options.Find()
	if len(projection) > 0 {
		findOpts.SetProjection(projection)
	}

	coll := c.adapter.client.Database(c.adapter.database).Collection(collection)
	cursor, err := coll.Find(ctx, filter, findOpts)
	if err != nil {
		return fmt.Errorf("mongodb: preload %q from collection %q: %w", name, collection, err)
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		return fmt.Errorf("mongodb: decode preload %q: %w", name, err)
	}

	c.mu.Lock()
	c.entries[name] = &mongoLocalCacheEntry{
		docs:      docs,
		fetchedAt: time.Now(),
		ttl:       ttl,
	}
	c.mu.Unlock()
	return nil
}

// Get 获取已预加载的文档列表。
// 若缓存不存在或已过期返回 (nil, false)。
func (c *MongoLocalCache) Get(name string) ([]bson.M, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[name]
	if !ok || entry.isStale() {
		return nil, false
	}
	return entry.docs, true
}

// Invalidate 删除指定名称的本地缓存条目（下次 Preload 会重新从 MongoDB 拉取）。
func (c *MongoLocalCache) Invalidate(name string) {
	c.mu.Lock()
	delete(c.entries, name)
	c.mu.Unlock()
}

// InvalidateAll 清空所有本地缓存条目。
func (c *MongoLocalCache) InvalidateAll() {
	c.mu.Lock()
	c.entries = make(map[string]*mongoLocalCacheEntry)
	c.mu.Unlock()
}

// LocalCacheStats 返回当前缓存状态统计（总条目数、已过期、有效）。
type LocalCacheStats struct {
	Total int
	Stale int
	Fresh int
}

// Stats 返回缓存条目的当前统计信息。
func (c *MongoLocalCache) Stats() LocalCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := LocalCacheStats{Total: len(c.entries)}
	for _, e := range c.entries {
		if e.isStale() {
			s.Stale++
		} else {
			s.Fresh++
		}
	}
	return s
}

// JoinWith 对 localDocs 做应用层等值 JOIN，替代代价高昂的 MongoDB $lookup。
//
//   - localDocs: 主集合已查询出的文档列表（不会被修改）
//   - localKey: 主文档中的连接字段名（如 "role_id"）
//   - cacheName: 维表在本地缓存中的标识符（通过 Preload 加载）
//   - cacheKey: 维表文档中的匹配字段名（如 "_id"）
//   - asField: JOIN 结果写入主文档的字段名（如 "role"）
//   - many: true = 1:N（结果为 []bson.M 切片），false = N:1（结果为首个匹配文档或 nil）
//
// 返回注入了 asField 字段的新文档列表。若缓存不存在或已过期，原样返回 localDocs。
//
// 示例（N:1，每个 user 对应一个 role）：
//
//	enriched := cache.JoinWith(users, "role_id", "roles", "_id", "role", false)
func (c *MongoLocalCache) JoinWith(localDocs []bson.M, localKey, cacheName, cacheKey, asField string, many bool) []bson.M {
	cached, ok := c.Get(cacheName)
	if !ok {
		return localDocs
	}

	// 构建索引 cacheKey 值 → []bson.M（支持 1:N）
	idx := make(map[interface{}][]bson.M, len(cached))
	for _, doc := range cached {
		k := doc[cacheKey]
		idx[k] = append(idx[k], doc)
	}

	result := make([]bson.M, 0, len(localDocs))
	for _, local := range localDocs {
		// 浅拷贝 + 注入关联字段，不修改原始 localDocs
		out := make(bson.M, len(local)+1)
		for k, v := range local {
			out[k] = v
		}
		matched := idx[local[localKey]]
		if many {
			out[asField] = matched
		} else {
			if len(matched) > 0 {
				out[asField] = matched[0]
			} else {
				out[asField] = nil
			}
		}
		result = append(result, out)
	}
	return result
}
