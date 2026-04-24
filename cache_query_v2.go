package db

// cache_query_v2.go
//
// 结果级多层缓存 API（V2）。
//
// 与旧版编译缓存 API 的区别：
//   - 旧版（BuildAndCacheQuery / ExecuteQueryConstructorWithCache 等）：
//     仅在 L1 Ristretto 中缓存编译后的 SQL 文本，每次查询仍会访问数据库。
//   - V2（ExecuteQueryConstructorCached / ExecuteQueryConstructorPagedCached 等）：
//     在 L1/L2/L3 多层中缓存实际查询结果（行数据），命中时完全不访问数据库。
//
// 使用方式：
//   1. 创建后端：backend := db.NewLayeredCacheBackend(db.NewRistrettoCacheBackend(), db.NewRedisCacheBackend(redisAdapter))
//   2. 绑定到 Repository：repo.SetResultCacheBackend(backend)
//   3. 执行带结果缓存的查询：result, err := repo.ExecuteQueryConstructorCached(ctx, "mykey", constructor, db.CacheOptions{TTL: time.Minute})

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// ==================== CacheBackend 绑定 ====================

// SetResultCacheBackend 设置 Repository 的结果级多层缓存后端。
// 调用后所有 V2 Cached 方法将使用该后端读写缓存。
// 传入 nil 表示禁用结果缓存（V2 方法将退化为直接查询）。
func (r *Repository) SetResultCacheBackend(backend CacheBackend) {
	r.mu.Lock()
	r.resultCacheBackend = backend
	r.mu.Unlock()
}

// ResultCacheBackend 返回当前绑定的结果级缓存后端，未设置时返回 nil。
func (r *Repository) ResultCacheBackend() CacheBackend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.resultCacheBackend
}

// ==================== 结果类型 ====================

// CachedQueryResult 带缓存元信息的查询结果。
type CachedQueryResult struct {
	// Result 是实际查询结果。
	Result *QueryConstructorExecutionResult
	// CacheSource 标记结果来自哪个缓存层级；CacheHitNone 表示来自实际数据库查询。
	CacheSource CacheHitSource
	// FromCache 是 CacheSource != CacheHitNone 的快捷判断。
	FromCache bool
}

// CachedPagedQueryResult 带缓存元信息的分页查询结果。
type CachedPagedQueryResult struct {
	// Result 是实际分页查询结果。
	Result *PagedQueryConstructorExecutionResult
	// RowsCacheSource 标记行数据来自哪个缓存层级。
	RowsCacheSource CacheHitSource
	// CountCacheSource 标记 count 来自哪个缓存层级。
	CountCacheSource CacheHitSource
	// FromCache 是 RowsCacheSource != CacheHitNone 的快捷判断。
	FromCache bool
}

// ==================== 序列化辅助 ====================

type cachedRowsPayload struct {
	Statement string                   `json:"s"`
	Args      []interface{}            `json:"a,omitempty"`
	Rows      []map[string]interface{} `json:"r"`
}

type cachedCountPayload struct {
	Total int64 `json:"t"`
}

func marshalRows(result *QueryConstructorExecutionResult) ([]byte, error) {
	return json.Marshal(cachedRowsPayload{
		Statement: result.Statement,
		Args:      result.Args,
		Rows:      result.Rows,
	})
}

func unmarshalRows(data []byte) (*QueryConstructorExecutionResult, error) {
	var p cachedRowsPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("cache: unmarshal rows payload: %w", err)
	}
	return &QueryConstructorExecutionResult{
		Statement: p.Statement,
		Args:      p.Args,
		Rows:      p.Rows,
	}, nil
}

func marshalCount(total int64) ([]byte, error) {
	return json.Marshal(cachedCountPayload{Total: total})
}

func unmarshalCount(data []byte) (int64, error) {
	var p cachedCountPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return 0, fmt.Errorf("cache: unmarshal count payload: %w", err)
	}
	return p.Total, nil
}

// ==================== V2 单次查询 ====================

// ExecuteQueryConstructorCached 执行 QueryConstructor 并将整行结果缓存至多层后端。
// 命中缓存时完全跳过数据库访问。
//
// cacheKey 须对当前查询结果唯一标识（包含表名、过滤条件、排序等所有决定结果的参数）。
// opts.TTL=0 表示不过期（由后端自身策略控制）。
//
// 若 SetResultCacheBackend 未调用，本方法退化为 ExecuteQueryConstructor。
func (r *Repository) ExecuteQueryConstructorCached(
	ctx context.Context,
	cacheKey string,
	constructor QueryConstructor,
	opts CacheOptions,
) (*CachedQueryResult, error) {
	if constructor == nil {
		return nil, fmt.Errorf("query constructor cannot be nil")
	}
	if strings.TrimSpace(cacheKey) == "" {
		return nil, fmt.Errorf("cache key cannot be empty")
	}

	r.mu.RLock()
	backend := r.resultCacheBackend
	r.mu.RUnlock()

	// --- 缓存读取 ---
	if backend != nil {
		var (
			data   []byte
			hit    bool
			source CacheHitSource
			err    error
		)
		if layered, ok := backend.(*LayeredCacheBackend); ok {
			data, hit, source, err = layered.GetWithSource(ctx, cacheKey)
		} else {
			data, hit, err = backend.Get(ctx, cacheKey)
			if hit {
				source = cacheLevelToSource(backend.Level())
			}
		}
		if err != nil {
			return nil, fmt.Errorf("result cache get: %w", err)
		}
		if hit {
			result, unmarshalErr := unmarshalRows(data)
			if unmarshalErr == nil {
				return &CachedQueryResult{Result: result, CacheSource: source, FromCache: true}, nil
			}
			// 反序列化失败视为 miss，继续查询数据库
		}
	}

	// --- 数据库查询 ---
	result, err := r.ExecuteQueryConstructor(ctx, constructor)
	if err != nil {
		return nil, err
	}

	// --- 缓存写入 ---
	if backend != nil {
		if data, marshalErr := marshalRows(result); marshalErr == nil {
			_ = backend.Set(ctx, cacheKey, data, opts.TTL, opts.Tags...)
		}
	}

	return &CachedQueryResult{Result: result, CacheSource: CacheHitNone, FromCache: false}, nil
}

// ==================== V2 分页查询 ====================

// ExecuteQueryConstructorPagedCached 执行分页查询（offset 模式）并将行与 count 分别缓存至多层后端。
//
// cacheKeyPrefix 为缓存键前缀；行缓存键为 "prefix:rows:page:N:size:M"，count 缓存键为 "prefix:count"。
// 若 SetResultCacheBackend 未调用，退化为 ExecuteQueryConstructorPaged。
func (r *Repository) ExecuteQueryConstructorPagedCached(
	ctx context.Context,
	cacheKeyPrefix string,
	constructor QueryConstructor,
	page, pageSize int,
	opts CacheOptions,
) (*CachedPagedQueryResult, error) {
	if strings.TrimSpace(cacheKeyPrefix) == "" {
		return nil, fmt.Errorf("cache key prefix cannot be empty")
	}
	builder := NewPaginationBuilder(page, pageSize).OffsetOnly()
	return r.executeQueryConstructorPaginatedCached(ctx, cacheKeyPrefix, constructor, builder, opts)
}

// ExecuteQueryConstructorPaginatedCached 使用统一分页语义执行查询，并将行与 count 分别缓存至多层后端。
//
// 若 SetResultCacheBackend 未调用，退化为 ExecuteQueryConstructorPaginated。
func (r *Repository) ExecuteQueryConstructorPaginatedCached(
	ctx context.Context,
	cacheKeyPrefix string,
	constructor QueryConstructor,
	builder *PaginationBuilder,
	opts CacheOptions,
) (*CachedPagedQueryResult, error) {
	if strings.TrimSpace(cacheKeyPrefix) == "" {
		return nil, fmt.Errorf("cache key prefix cannot be empty")
	}
	return r.executeQueryConstructorPaginatedCached(ctx, cacheKeyPrefix, constructor, builder, opts)
}

func (r *Repository) executeQueryConstructorPaginatedCached(
	ctx context.Context,
	cacheKeyPrefix string,
	constructor QueryConstructor,
	builder *PaginationBuilder,
	opts CacheOptions,
) (*CachedPagedQueryResult, error) {
	if constructor == nil {
		return nil, fmt.Errorf("query constructor cannot be nil")
	}

	r.mu.RLock()
	backend := r.resultCacheBackend
	r.mu.RUnlock()

	normalizedBuilder, normalizedPage, normalizedPageSize, offset, cursorMode := normalizePaginationBuilder(builder)

	modeKey := "offset"
	if cursorMode {
		modeKey = "cursor"
	}
	rowsCacheKey := fmt.Sprintf("%s:rows:%s:page:%d:size:%d", cacheKeyPrefix, modeKey, normalizedPage, normalizedPageSize)
	countCacheKey := fmt.Sprintf("%s:count", cacheKeyPrefix)

	var (
		rowsSource  CacheHitSource
		countSource CacheHitSource
		execResult  *QueryConstructorExecutionResult
		total       int64
	)

	// --- 行缓存读取 ---
	rowsHit := false
	if backend != nil {
		data, hit, src, err := cacheBackendGetWithSource(ctx, backend, rowsCacheKey)
		if err != nil {
			return nil, fmt.Errorf("result cache get rows: %w", err)
		}
		if hit {
			if res, unmarshalErr := unmarshalRows(data); unmarshalErr == nil {
				execResult = res
				rowsSource = src
				rowsHit = true
			}
		}
	}

	// --- 行数据库查询 ---
	if !rowsHit {
		constructor.Paginate(normalizedBuilder)
		var err error
		execResult, err = r.ExecuteQueryConstructor(ctx, constructor)
		if err != nil {
			return nil, err
		}
		if backend != nil {
			if data, marshalErr := marshalRows(execResult); marshalErr == nil {
				_ = backend.Set(ctx, rowsCacheKey, data, opts.TTL, opts.Tags...)
			}
		}
	}

	// --- count 缓存读取 ---
	countHit := false
	if backend != nil {
		data, hit, src, err := cacheBackendGetWithSource(ctx, backend, countCacheKey)
		if err != nil {
			return nil, fmt.Errorf("result cache get count: %w", err)
		}
		if hit {
			if n, unmarshalErr := unmarshalCount(data); unmarshalErr == nil {
				total = n
				countSource = src
				countHit = true
			}
		}
	}

	// --- count 数据库查询 ---
	if !countHit {
		var err error
		total, _, err = r.executeQueryConstructorCount(ctx, "", constructor)
		if err != nil {
			return nil, err
		}
		if backend != nil {
			if data, marshalErr := marshalCount(total); marshalErr == nil {
				_ = backend.Set(ctx, countCacheKey, data, opts.TTL, opts.Tags...)
			}
		}
	}

	totalPages := 0
	if normalizedPageSize > 0 && total > 0 {
		totalPages = int((total + int64(normalizedPageSize) - 1) / int64(normalizedPageSize))
	}

	pagedResult := &PagedQueryConstructorExecutionResult{
		Statement:     execResult.Statement,
		Args:          execResult.Args,
		Rows:          execResult.Rows,
		Total:         total,
		Page:          normalizedPage,
		PageSize:      normalizedPageSize,
		Offset:        offset,
		TotalPages:    totalPages,
		HasNext:       totalPages > 0 && normalizedPage < totalPages,
		HasPrevious:   normalizedPage > 1,
		QueryCacheHit: rowsHit,
		CountCacheHit: countHit,
	}

	return &CachedPagedQueryResult{
		Result:           pagedResult,
		RowsCacheSource:  rowsSource,
		CountCacheSource: countSource,
		FromCache:        rowsHit,
	}, nil
}

// cacheBackendGetWithSource 统一处理 LayeredCacheBackend 与普通 CacheBackend 的带 source 读取。
func cacheBackendGetWithSource(ctx context.Context, backend CacheBackend, key string) ([]byte, bool, CacheHitSource, error) {
	if layered, ok := backend.(*LayeredCacheBackend); ok {
		data, hit, src, err := layered.GetWithSource(ctx, key)
		return data, hit, src, err
	}
	data, hit, err := backend.Get(ctx, key)
	if hit {
		return data, true, cacheLevelToSource(backend.Level()), err
	}
	return nil, false, CacheHitNone, err
}
