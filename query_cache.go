package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	ristretto "github.com/dgraph-io/ristretto/v2"
)

const DefaultCompiledQueryCacheMaxEntries = 256
const DefaultCompiledQueryCacheDefaultTTLSeconds = 0
const DefaultCompiledQueryCacheEnableMetrics = true

type compiledQueryCacheEntryKind uint8

const (
	compiledQueryCacheEntryQuery compiledQueryCacheEntryKind = iota + 1
	compiledQueryCacheEntryTemplate
)

type compiledQueryCacheEntry struct {
	kind     compiledQueryCacheEntryKind
	query    CompiledQuery
	template CompiledQueryTemplate
}

// CompiledQuery 表示预编译后的查询结果。
type CompiledQuery struct {
	Query string
	Args  []interface{}
}

// CompiledQueryTemplate 表示可复用的查询模板（仅缓存查询文本，不缓存具体参数值）。
type CompiledQueryTemplate struct {
	Query    string
	ArgCount int
}

// CompiledQueryCache Repository 级别的编译结果缓存。
type CompiledQueryCache struct {
	entries       *ristretto.Cache[string, compiledQueryCacheEntry]
	defaultTTL    time.Duration
	enableMetrics bool
}

// CompiledQueryCacheStats 表示查询缓存命中统计。
type CompiledQueryCacheStats struct {
	Enabled       bool
	Hits          uint64
	Misses        uint64
	HitRatio      float64
	KeysAdded     uint64
	KeysUpdated   uint64
	KeysEvicted   uint64
	SetsDropped   uint64
	SetsRejected  uint64
	GetsDropped   uint64
	GetsKept      uint64
	CostAdded     uint64
	CostEvicted   uint64
	RemainingCost int64
	MaxCost       int64
}

// NewCompiledQueryCache 创建编译查询缓存。
func NewCompiledQueryCache() *CompiledQueryCache {
	return NewCompiledQueryCacheWithOptions(DefaultCompiledQueryCacheMaxEntries, time.Duration(DefaultCompiledQueryCacheDefaultTTLSeconds)*time.Second, DefaultCompiledQueryCacheEnableMetrics)
}

// NewCompiledQueryCacheWithSize 创建指定容量的编译查询缓存。
func NewCompiledQueryCacheWithSize(maxEntries int) *CompiledQueryCache {
	return NewCompiledQueryCacheWithOptions(maxEntries, time.Duration(DefaultCompiledQueryCacheDefaultTTLSeconds)*time.Second, DefaultCompiledQueryCacheEnableMetrics)
}

// NewCompiledQueryCacheWithOptions 创建指定容量、TTL 和 metrics 策略的编译查询缓存。
func NewCompiledQueryCacheWithOptions(maxEntries int, defaultTTL time.Duration, enableMetrics bool) *CompiledQueryCache {
	normalizedMaxEntries := normalizeCompiledQueryCacheSize(maxEntries)
	numCounters := int64(normalizedMaxEntries * 10)
	if numCounters < 1000 {
		numCounters = 1000
	}

	entries, err := ristretto.NewCache(&ristretto.Config[string, compiledQueryCacheEntry]{
		NumCounters:        numCounters,
		MaxCost:            int64(normalizedMaxEntries),
		BufferItems:        64,
		Metrics:            enableMetrics,
		IgnoreInternalCost: true,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to initialize compiled query cache: %v", err))
	}
	return &CompiledQueryCache{entries: entries, defaultTTL: defaultTTL, enableMetrics: enableMetrics}
}

func normalizeCompiledQueryCacheSize(maxEntries int) int {
	if maxEntries <= 0 {
		return DefaultCompiledQueryCacheMaxEntries
	}
	return maxEntries
}

func compiledQueryItemKey(key string) string {
	return "query:" + key
}

func compiledQueryTemplateKey(key string) string {
	return "template:" + key
}

func copyQueryArgs(args []interface{}) []interface{} {
	if len(args) == 0 {
		return nil
	}
	out := make([]interface{}, len(args))
	copy(out, args)
	return out
}

func (c *CompiledQueryCache) get(key string) (CompiledQuery, bool) {
	entry, ok := c.entries.Get(compiledQueryItemKey(key))
	if !ok || entry.kind != compiledQueryCacheEntryQuery {
		return CompiledQuery{}, false
	}
	q := entry.query
	q.Args = copyQueryArgs(q.Args)
	return q, true
}

func (c *CompiledQueryCache) set(key string, query string, args []interface{}) {
	entry := compiledQueryCacheEntry{
		kind:  compiledQueryCacheEntryQuery,
		query: CompiledQuery{Query: query, Args: copyQueryArgs(args)},
	}
	if c.defaultTTL > 0 {
		if c.entries.SetWithTTL(compiledQueryItemKey(key), entry, 1, c.defaultTTL) {
			c.entries.Wait()
		}
		return
	}
	if c.entries.Set(compiledQueryItemKey(key), entry, 1) {
		c.entries.Wait()
	}
}

func (c *CompiledQueryCache) delete(key string) {
	c.entries.Del(compiledQueryItemKey(key))
}

func (c *CompiledQueryCache) clear() {
	c.entries.Clear()
}

func (c *CompiledQueryCache) getTemplate(key string) (CompiledQueryTemplate, bool) {
	entry, ok := c.entries.Get(compiledQueryTemplateKey(key))
	if !ok || entry.kind != compiledQueryCacheEntryTemplate {
		return CompiledQueryTemplate{}, false
	}
	return entry.template, true
}

func (c *CompiledQueryCache) setTemplate(key string, query string, argCount int) {
	entry := compiledQueryCacheEntry{
		kind:     compiledQueryCacheEntryTemplate,
		template: CompiledQueryTemplate{Query: query, ArgCount: argCount},
	}
	if c.defaultTTL > 0 {
		if c.entries.SetWithTTL(compiledQueryTemplateKey(key), entry, 1, c.defaultTTL) {
			c.entries.Wait()
		}
		return
	}
	if c.entries.Set(compiledQueryTemplateKey(key), entry, 1) {
		c.entries.Wait()
	}
}

func (c *CompiledQueryCache) deleteTemplate(key string) {
	c.entries.Del(compiledQueryTemplateKey(key))
}

func (c *CompiledQueryCache) close() {
	c.entries.Close()
}

func (c *CompiledQueryCache) stats() CompiledQueryCacheStats {
	stats := CompiledQueryCacheStats{
		Enabled:       c.enableMetrics,
		RemainingCost: c.entries.RemainingCost(),
		MaxCost:       c.entries.MaxCost(),
	}
	if !c.enableMetrics || c.entries.Metrics == nil {
		return stats
	}

	m := c.entries.Metrics
	stats.Hits = m.Hits()
	stats.Misses = m.Misses()
	stats.HitRatio = m.Ratio()
	stats.KeysAdded = m.KeysAdded()
	stats.KeysUpdated = m.KeysUpdated()
	stats.KeysEvicted = m.KeysEvicted()
	stats.SetsDropped = m.SetsDropped()
	stats.SetsRejected = m.SetsRejected()
	stats.GetsDropped = m.GetsDropped()
	stats.GetsKept = m.GetsKept()
	stats.CostAdded = m.CostAdded()
	stats.CostEvicted = m.CostEvicted()
	return stats
}

// GetCompiledQuery 从缓存读取已编译查询。
//
// Deprecated: 编译缓存 API 仅缓存 SQL 文本，每次执行仍会访问数据库。
// 如需缓存查询结果（完全跳过数据库），请使用 ExecuteQueryConstructorCached。
// 编译缓存公共 API（GetCompiledQuery、StoreCompiledQuery、BuildAndCacheQuery 等）
// 将在下一个主版本中移除，届时相关兼容层也将一并删除。
func (r *Repository) GetCompiledQuery(cacheKey string) (*CompiledQuery, bool) {
	r.mu.RLock()
	cache := r.compiledQueryCache
	r.mu.RUnlock()

	if cache == nil || cacheKey == "" {
		return nil, false
	}
	q, ok := cache.get(cacheKey)
	if !ok {
		return nil, false
	}
	return &q, true
}

// StoreCompiledQuery 手动写入一条编译查询到缓存。
//
// Deprecated: 见 GetCompiledQuery 的说明。将在下一个主版本中移除。
func (r *Repository) StoreCompiledQuery(cacheKey string, query string, args ...interface{}) error {
	if cacheKey == "" {
		return fmt.Errorf("cache key cannot be empty")
	}
	if query == "" {
		return fmt.Errorf("query cannot be empty")
	}

	r.mu.RLock()
	cache := r.compiledQueryCache
	r.mu.RUnlock()

	if cache == nil {
		return fmt.Errorf("compiled query cache is not initialized")
	}

	cache.set(cacheKey, query, args)
	return nil
}

// InvalidateCompiledQuery 删除指定缓存键。
func (r *Repository) InvalidateCompiledQuery(cacheKey string) {
	if cacheKey == "" {
		return
	}
	r.mu.RLock()
	cache := r.compiledQueryCache
	r.mu.RUnlock()
	if cache == nil {
		return
	}
	cache.delete(cacheKey)
	cache.deleteTemplate(cacheKey)
}

// ClearCompiledQueryCache 清空 Repository 的查询编译缓存。
func (r *Repository) ClearCompiledQueryCache() {
	r.mu.RLock()
	cache := r.compiledQueryCache
	r.mu.RUnlock()
	if cache == nil {
		return
	}
	cache.clear()
}

// GetCompiledQueryCacheStats 返回查询缓存命中统计。
func (r *Repository) GetCompiledQueryCacheStats() CompiledQueryCacheStats {
	r.mu.RLock()
	cache := r.compiledQueryCache
	r.mu.RUnlock()
	if cache == nil {
		return CompiledQueryCacheStats{}
	}
	return cache.stats()
}

// BuildAndCacheQuery 编译查询并缓存；命中缓存时直接返回。
// 返回值 cacheHit=true 表示直接复用缓存，无需再次 Build。
//
// Deprecated: 见 GetCompiledQuery 的说明。将在下一个主版本中移除。
// 替代方案：使用 ExecuteQueryConstructorCached，可同时缓存编译结果与查询行数据。
func (r *Repository) BuildAndCacheQuery(ctx context.Context, cacheKey string, constructor QueryConstructor) (query string, args []interface{}, cacheHit bool, err error) {
	if cacheKey == "" {
		return "", nil, false, fmt.Errorf("cache key cannot be empty")
	}
	if constructor == nil {
		return "", nil, false, fmt.Errorf("query constructor cannot be nil")
	}

	if q, ok := r.GetCompiledQuery(cacheKey); ok {
		return q.Query, q.Args, true, nil
	}

	query, args, err = constructor.Build(ctx)
	if err != nil {
		return "", nil, false, err
	}
	if err := r.StoreCompiledQuery(cacheKey, query, args...); err != nil {
		return "", nil, false, err
	}
	return query, args, false, nil
}

// GetCompiledQueryTemplate 从缓存读取查询模板。
//
// Deprecated: 见 GetCompiledQuery 的说明。将在下一个主版本中移除。
func (r *Repository) GetCompiledQueryTemplate(cacheKey string) (*CompiledQueryTemplate, bool) {
	r.mu.RLock()
	cache := r.compiledQueryCache
	r.mu.RUnlock()

	if cache == nil || cacheKey == "" {
		return nil, false
	}
	t, ok := cache.getTemplate(cacheKey)
	if !ok {
		return nil, false
	}
	return &t, true
}

// StoreCompiledQueryTemplate 手动写入一条查询模板到缓存。
//
// Deprecated: 见 GetCompiledQuery 的说明。将在下一个主版本中移除。
func (r *Repository) StoreCompiledQueryTemplate(cacheKey string, query string, argCount int) error {
	if cacheKey == "" {
		return fmt.Errorf("cache key cannot be empty")
	}
	if query == "" {
		return fmt.Errorf("query cannot be empty")
	}
	if argCount < 0 {
		return fmt.Errorf("arg count cannot be negative")
	}

	r.mu.RLock()
	cache := r.compiledQueryCache
	r.mu.RUnlock()

	if cache == nil {
		return fmt.Errorf("compiled query cache is not initialized")
	}

	cache.setTemplate(cacheKey, query, argCount)
	return nil
}

// BuildAndCacheQueryTemplate 编译查询模板并缓存；命中缓存时直接返回模板。
// 与 BuildAndCacheQuery 的区别是该 API 不缓存具体参数值，仅缓存 query 文本和参数位个数。
//
// Deprecated: 见 GetCompiledQuery 的说明。将在下一个主版本中移除。
func (r *Repository) BuildAndCacheQueryTemplate(ctx context.Context, cacheKey string, constructor QueryConstructor) (query string, argCount int, cacheHit bool, err error) {
	if cacheKey == "" {
		return "", 0, false, fmt.Errorf("cache key cannot be empty")
	}
	if constructor == nil {
		return "", 0, false, fmt.Errorf("query constructor cannot be nil")
	}

	if tpl, ok := r.GetCompiledQueryTemplate(cacheKey); ok {
		return tpl.Query, tpl.ArgCount, true, nil
	}

	query, args, err := constructor.Build(ctx)
	if err != nil {
		return "", 0, false, err
	}
	argCount = len(args)
	if err := r.StoreCompiledQueryTemplate(cacheKey, query, argCount); err != nil {
		return "", 0, false, err
	}
	return query, argCount, false, nil
}

// QueryWithCachedTemplate 使用缓存模板执行查询。
//
// Deprecated: 见 GetCompiledQuery 的说明。将在下一个主版本中移除。
func (r *Repository) QueryWithCachedTemplate(ctx context.Context, cacheKey string, args ...interface{}) (*sql.Rows, error) {
	tpl, ok := r.GetCompiledQueryTemplate(cacheKey)
	if !ok {
		return nil, fmt.Errorf("compiled query template not found: %s", cacheKey)
	}
	if len(args) != tpl.ArgCount {
		return nil, fmt.Errorf("cached template arg count mismatch: expected %d, got %d", tpl.ArgCount, len(args))
	}
	return r.Query(ctx, tpl.Query, args...)
}

// QueryRowWithCachedTemplate 使用缓存模板执行单行查询。
//
// Deprecated: 见 GetCompiledQuery 的说明。将在下一个主版本中移除。
func (r *Repository) QueryRowWithCachedTemplate(ctx context.Context, cacheKey string, args ...interface{}) (*sql.Row, error) {
	tpl, ok := r.GetCompiledQueryTemplate(cacheKey)
	if !ok {
		return nil, fmt.Errorf("compiled query template not found: %s", cacheKey)
	}
	if len(args) != tpl.ArgCount {
		return nil, fmt.Errorf("cached template arg count mismatch: expected %d, got %d", tpl.ArgCount, len(args))
	}
	return r.QueryRow(ctx, tpl.Query, args...), nil
}

// ExecWithCachedTemplate 使用缓存模板执行写操作。
func (r *Repository) ExecWithCachedTemplate(ctx context.Context, cacheKey string, args ...interface{}) (sql.Result, error) {
	tpl, ok := r.GetCompiledQueryTemplate(cacheKey)
	if !ok {
		return nil, fmt.Errorf("compiled query template not found: %s", cacheKey)
	}
	if len(args) != tpl.ArgCount {
		return nil, fmt.Errorf("cached template arg count mismatch: expected %d, got %d", tpl.ArgCount, len(args))
	}
	return r.Exec(ctx, tpl.Query, args...)
}
