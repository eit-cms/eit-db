package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoVirtualView 用进程内存缓存模拟 MongoDB "物化视图"。
//
// 思路：将一条 Aggregation Pipeline 的执行结果按 TTL 缓存在进程内，
// 避免每次查询都触发完整的 aggregation，同时无需在数据库层维护物化视图 DDL。
//
// 与 SQL 物化视图的对比：
//   - SQL 物化视图：结果存储在数据库磁盘，需手动 REFRESH 或定时刷新。
//   - MongoVirtualView：结果缓存在应用进程内存，按 TTL 自动过期后重拉，免 DDL。
//
// 适用场景：报表汇总快照、权限角色预计算、低频变化但高频读取的聚合结果。
//
// 示例：
//
//	vv, ok := db.GetMongoVirtualView(repo.GetAdapter())
//	vv.Define("active_user_count", "users",
//	    bson.A{
//	        bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
//	        bson.D{{Key: "$count", Value: "count"}},
//	    },
//	    5*time.Minute,
//	)
//	docs, err := vv.Execute(ctx, "active_user_count")
type MongoVirtualView struct {
	adapter *MongoAdapter
	mu      sync.RWMutex
	defs    map[string]*virtualViewDef
	cache   map[string]*virtualViewCacheEntry
}

type virtualViewDef struct {
	collection string
	pipeline   bson.A
	ttl        time.Duration
}

type virtualViewCacheEntry struct {
	docs     []bson.M
	cachedAt time.Time
	ttl      time.Duration
}

func (v *virtualViewCacheEntry) isStale() bool {
	if v.ttl <= 0 {
		return false // ttl <= 0 表示永不自动过期
	}
	return time.Since(v.cachedAt) > v.ttl
}

// GetMongoVirtualView 从 Adapter 获取 MongoVirtualView。
// 若 adapter 不是 *MongoAdapter，返回 (nil, false)。
func GetMongoVirtualView(adapter Adapter) (*MongoVirtualView, bool) {
	m, ok := adapter.(*MongoAdapter)
	if !ok {
		return nil, false
	}
	return &MongoVirtualView{
		adapter: m,
		defs:    make(map[string]*virtualViewDef),
		cache:   make(map[string]*virtualViewCacheEntry),
	}, true
}

// Define 注册一个虚拟视图定义（仅注册，不执行聚合）。
//
//   - name: 视图标识符，Execute/Refresh/InvalidateView 时使用
//   - collection: 聚合操作的源集合名
//   - pipeline: MongoDB Aggregation Pipeline（bson.A，即 []interface{}）
//   - ttl: 结果缓存时长（<= 0 表示永不自动过期，直到 Refresh 或 InvalidateView）
//
// 重复调用 Define 同名视图会覆盖原定义（同时清除旧缓存）。
func (v *MongoVirtualView) Define(name, collection string, pipeline bson.A, ttl time.Duration) {
	v.mu.Lock()
	v.defs[name] = &virtualViewDef{
		collection: collection,
		pipeline:   pipeline,
		ttl:        ttl,
	}
	delete(v.cache, name) // 定义变更时清除旧缓存
	v.mu.Unlock()
}

// Execute 返回虚拟视图的文档列表。
//
//   - 首次调用（或缓存已过期）：执行 Aggregation Pipeline 并缓存结果。
//   - TTL 内再次调用：直接返回缓存，不发起 MongoDB 请求。
func (v *MongoVirtualView) Execute(ctx context.Context, name string) ([]bson.M, error) {
	v.mu.RLock()
	cached, hasCached := v.cache[name]
	v.mu.RUnlock()

	if hasCached && !cached.isStale() {
		return cached.docs, nil
	}
	return v.run(ctx, name)
}

// Refresh 强制重新执行 Aggregation Pipeline，忽略现有缓存，刷新后返回最新结果。
func (v *MongoVirtualView) Refresh(ctx context.Context, name string) ([]bson.M, error) {
	return v.run(ctx, name)
}

// InvalidateView 清除指定视图的内存缓存（下次 Execute 会重新执行 Pipeline）。
func (v *MongoVirtualView) InvalidateView(name string) {
	v.mu.Lock()
	delete(v.cache, name)
	v.mu.Unlock()
}

// InvalidateAll 清除所有视图缓存。
func (v *MongoVirtualView) InvalidateAll() {
	v.mu.Lock()
	v.cache = make(map[string]*virtualViewCacheEntry)
	v.mu.Unlock()
}

// IsCached 检查指定视图是否有未过期的内存缓存。
func (v *MongoVirtualView) IsCached(name string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	c, ok := v.cache[name]
	return ok && !c.isStale()
}

// IsDefined 检查指定名称的视图是否已通过 Define 注册。
func (v *MongoVirtualView) IsDefined(name string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	_, ok := v.defs[name]
	return ok
}

// DefinedViews 返回所有已注册的视图名称列表。
func (v *MongoVirtualView) DefinedViews() []string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	names := make([]string, 0, len(v.defs))
	for name := range v.defs {
		names = append(names, name)
	}
	return names
}

// run 执行聚合并更新缓存（内部方法）。
func (v *MongoVirtualView) run(ctx context.Context, name string) ([]bson.M, error) {
	v.mu.RLock()
	def, ok := v.defs[name]
	v.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("mongodb virtual view %q not defined", name)
	}
	if v.adapter.client == nil {
		return nil, fmt.Errorf("mongodb: not connected")
	}

	coll := v.adapter.client.Database(v.adapter.database).Collection(def.collection)
	cursor, err := coll.Aggregate(ctx, def.pipeline, options.Aggregate())
	if err != nil {
		return nil, fmt.Errorf("mongodb: virtual view %q aggregate: %w", name, err)
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("mongodb: virtual view %q decode: %w", name, err)
	}

	v.mu.Lock()
	v.cache[name] = &virtualViewCacheEntry{
		docs:     docs,
		cachedAt: time.Now(),
		ttl:      def.ttl,
	}
	v.mu.Unlock()
	return docs, nil
}
