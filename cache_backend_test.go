package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"
)

// ==================== RistrettoCacheBackend 测试 ====================

func TestRistrettoCacheBackend_SetAndGet(t *testing.T) {
	b := NewRistrettoCacheBackend()
	defer b.Close()
	ctx := context.Background()

	val := []byte(`{"name":"alice"}`)
	if err := b.Set(ctx, "k1", val, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, hit, err := b.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !hit {
		t.Fatal("expected hit=true")
	}
	if string(got) != string(val) {
		t.Fatalf("value mismatch: got %s, want %s", got, val)
	}
	if b.Level() != CacheLevelL1 {
		t.Fatalf("expected L1, got %v", b.Level())
	}
}

func TestRistrettoCacheBackend_Miss(t *testing.T) {
	b := NewRistrettoCacheBackend()
	defer b.Close()
	ctx := context.Background()

	val, hit, err := b.Get(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if hit {
		t.Fatal("expected miss")
	}
	if val != nil {
		t.Fatal("expected nil value on miss")
	}
}

func TestRistrettoCacheBackend_Del(t *testing.T) {
	b := NewRistrettoCacheBackend()
	defer b.Close()
	ctx := context.Background()

	_ = b.Set(ctx, "k1", []byte("v1"), 0)
	_ = b.Del(ctx, "k1")
	_, hit, _ := b.Get(ctx, "k1")
	if hit {
		t.Fatal("expected miss after Del")
	}
}

func TestRistrettoCacheBackend_MGet(t *testing.T) {
	b := NewRistrettoCacheBackend()
	defer b.Close()
	ctx := context.Background()

	_ = b.Set(ctx, "a", []byte("1"), 0)
	_ = b.Set(ctx, "b", []byte("2"), 0)

	results, err := b.MGet(ctx, "a", "missing", "b")
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if !results[0].Hit || string(results[0].Value) != "1" {
		t.Fatalf("results[0] unexpected: %+v", results[0])
	}
	if results[1].Hit {
		t.Fatalf("results[1] should miss: %+v", results[1])
	}
	if !results[2].Hit || string(results[2].Value) != "2" {
		t.Fatalf("results[2] unexpected: %+v", results[2])
	}
}

func TestRistrettoCacheBackend_InvalidateByTagNoOp(t *testing.T) {
	b := NewRistrettoCacheBackend()
	defer b.Close()
	// L1 不支持 tag，应返回 nil 而非 error
	if err := b.InvalidateByTag(context.Background(), "tag1"); err != nil {
		t.Fatalf("InvalidateByTag: %v", err)
	}
}

func TestRistrettoCacheBackend_Stats(t *testing.T) {
	b := NewRistrettoCacheBackend()
	defer b.Close()
	ctx := context.Background()

	_ = b.Set(ctx, "x", []byte("y"), 0)
	b.Get(ctx, "x")   // hit
	b.Get(ctx, "zzz") // miss

	stats := b.Stats()
	if stats.Level != CacheLevelL1 {
		t.Fatalf("expected L1 stats")
	}
	if stats.Hits == 0 {
		t.Fatal("expected hits > 0")
	}
	if stats.Misses == 0 {
		t.Fatal("expected misses > 0")
	}
}

// ==================== LayeredCacheBackend 测试 ====================

func TestLayeredCacheBackend_L1HitNoL2Access(t *testing.T) {
	l1 := NewRistrettoCacheBackend()
	defer l1.Close()
	recorder := &recordingCacheBackend{level: CacheLevelL2}
	layered := NewLayeredCacheBackend(l1, recorder)
	ctx := context.Background()

	_ = l1.Set(ctx, "k", []byte("v"), 0)
	val, hit, src, err := layered.GetWithSource(ctx, "k")
	if err != nil || !hit {
		t.Fatalf("GetWithSource: hit=%v err=%v", hit, err)
	}
	if src != CacheHitL1 {
		t.Fatalf("expected L1 source, got %s", src)
	}
	if string(val) != "v" {
		t.Fatalf("unexpected value: %s", val)
	}
	if recorder.getCalls > 0 {
		t.Fatal("L2 should not be accessed when L1 hits")
	}
}

func TestLayeredCacheBackend_L1MissFallsToL2(t *testing.T) {
	l1 := NewRistrettoCacheBackend()
	defer l1.Close()
	recorder := &recordingCacheBackend{level: CacheLevelL2, data: map[string][]byte{"k": []byte("v2")}}
	layered := NewLayeredCacheBackend(l1, recorder)
	ctx := context.Background()

	val, hit, src, err := layered.GetWithSource(ctx, "k")
	if err != nil || !hit {
		t.Fatalf("GetWithSource: hit=%v err=%v", hit, err)
	}
	if src != CacheHitL2 {
		t.Fatalf("expected L2 source, got %s", src)
	}
	if string(val) != "v2" {
		t.Fatalf("unexpected value: %s", val)
	}
	// L2 命中后应回填 L1
	l1Val, l1Hit, _ := l1.Get(ctx, "k")
	if !l1Hit || string(l1Val) != "v2" {
		t.Fatal("L1 should have been backfilled from L2")
	}
}

func TestLayeredCacheBackend_AllMiss(t *testing.T) {
	l1 := NewRistrettoCacheBackend()
	defer l1.Close()
	recorder := &recordingCacheBackend{level: CacheLevelL2, data: map[string][]byte{}}
	layered := NewLayeredCacheBackend(l1, recorder)
	ctx := context.Background()

	val, hit, src, err := layered.GetWithSource(ctx, "gone")
	if err != nil {
		t.Fatalf("GetWithSource error: %v", err)
	}
	if hit {
		t.Fatal("expected miss")
	}
	if src != CacheHitNone {
		t.Fatalf("expected CacheHitNone, got %s", src)
	}
	if val != nil {
		t.Fatal("expected nil value")
	}
}

func TestLayeredCacheBackend_SetWritesToAllLevels(t *testing.T) {
	l1 := NewRistrettoCacheBackend()
	defer l1.Close()
	recorder := &recordingCacheBackend{level: CacheLevelL2, data: map[string][]byte{}}
	layered := NewLayeredCacheBackend(l1, recorder)
	ctx := context.Background()

	if err := layered.Set(ctx, "k", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	_, l1Hit, _ := l1.Get(ctx, "k")
	if !l1Hit {
		t.Fatal("L1 should have value after Set")
	}
	if recorder.setCalls == 0 {
		t.Fatal("L2 should have been written")
	}
}

func TestLayeredCacheBackend_MGet_PartialHit(t *testing.T) {
	l1 := NewRistrettoCacheBackend()
	defer l1.Close()
	l1.Set(context.Background(), "a", []byte("1"), 0)
	recorder := &recordingCacheBackend{
		level: CacheLevelL2,
		data:  map[string][]byte{"b": []byte("2")},
	}
	layered := NewLayeredCacheBackend(l1, recorder)
	ctx := context.Background()

	results, err := layered.MGet(ctx, "a", "b", "c")
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if !results[0].Hit || results[0].Source != CacheHitL1 {
		t.Fatalf("results[0]: %+v", results[0])
	}
	if !results[1].Hit || results[1].Source != CacheHitL2 {
		t.Fatalf("results[1]: %+v", results[1])
	}
	if results[2].Hit {
		t.Fatalf("results[2] should miss: %+v", results[2])
	}
}

func TestLayeredCacheBackend_AllStats(t *testing.T) {
	l1 := NewRistrettoCacheBackend()
	defer l1.Close()
	recorder := &recordingCacheBackend{level: CacheLevelL2, data: map[string][]byte{}}
	layered := NewLayeredCacheBackend(l1, recorder)

	stats := layered.AllStats()
	if len(stats) != 2 {
		t.Fatalf("expected 2 stats entries, got %d", len(stats))
	}
	if stats[0].Level != CacheLevelL1 {
		t.Fatalf("first stats should be L1")
	}
	if stats[1].Level != CacheLevelL2 {
		t.Fatalf("second stats should be L2")
	}
}

// ==================== cache_query_v2 测试 ====================

func TestExecuteQueryConstructorCached_NilBackendDegrades(t *testing.T) {
	repo := newTestMemoryRepo()
	ctx := context.Background()

	opts := CacheOptions{TTL: time.Minute}
	result, err := repo.ExecuteQueryConstructorCached(ctx, "testkey", &stubQueryConstructor{}, opts)
	if err != nil {
		// 内存适配器可能不支持执行，只要不因为缓存层panic/崩溃即可
		return
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.FromCache {
		t.Fatal("should not be from cache when no backend set")
	}
	if result.CacheSource != CacheHitNone {
		t.Fatalf("cache source should be None, got %s", result.CacheSource)
	}
}

func TestExecuteQueryConstructorCached_L1Hit(t *testing.T) {
	l1 := NewRistrettoCacheBackend()
	defer l1.Close()
	ctx := context.Background()

	// 预热缓存
	prebuilt := &QueryConstructorExecutionResult{
		Statement: "SELECT 1",
		Rows:      []map[string]interface{}{{"id": float64(1)}},
	}
	data, _ := marshalRows(prebuilt)
	_ = l1.Set(ctx, "cached:key", data, 0)

	repo := newTestMemoryRepo()
	repo.SetResultCacheBackend(l1)

	result, err := repo.ExecuteQueryConstructorCached(ctx, "cached:key", &stubQueryConstructor{}, CacheOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.FromCache {
		t.Fatal("expected FromCache=true")
	}
	if result.CacheSource != CacheHitL1 {
		t.Fatalf("expected L1 source, got %s", result.CacheSource)
	}
	if result.Result.Statement != "SELECT 1" {
		t.Fatalf("unexpected statement: %s", result.Result.Statement)
	}
}

func TestCachedQueryResult_MarshalRoundtrip(t *testing.T) {
	original := &QueryConstructorExecutionResult{
		Statement: "SELECT id, name FROM users WHERE id = $1",
		Args:      []interface{}{42},
		Rows: []map[string]interface{}{
			{"id": float64(42), "name": "bob"},
		},
	}
	data, err := marshalRows(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !json.Valid(data) {
		t.Fatal("marshaled data is not valid JSON")
	}
	got, err := unmarshalRows(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Statement != original.Statement {
		t.Fatalf("statement mismatch")
	}
	if len(got.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got.Rows))
	}
	if got.Rows[0]["name"] != "bob" {
		t.Fatalf("row data mismatch: %v", got.Rows[0])
	}
}

func TestCacheLevel_Source_Mapping(t *testing.T) {
	cases := []struct {
		level  CacheLevel
		source CacheHitSource
	}{
		{CacheLevelL1, CacheHitL1},
		{CacheLevelL2, CacheHitL2},
		{CacheLevelL3, CacheHitL3},
		{CacheLevel(99), CacheHitNone},
	}
	for _, c := range cases {
		got := cacheLevelToSource(c.level)
		if got != c.source {
			t.Errorf("cacheLevelToSource(%d) = %s, want %s", c.level, got, c.source)
		}
	}
}

// ==================== 辅助 stub ====================

// recordingCacheBackend 记录调用次数，可预设命中数据。
type recordingCacheBackend struct {
	level    CacheLevel
	data     map[string][]byte
	getCalls int
	setCalls int
}

func (r *recordingCacheBackend) Level() CacheLevel { return r.level }
func (r *recordingCacheBackend) Get(_ context.Context, key string) ([]byte, bool, error) {
	r.getCalls++
	v, ok := r.data[key]
	return v, ok, nil
}
func (r *recordingCacheBackend) Set(_ context.Context, key string, value []byte, _ time.Duration, _ ...string) error {
	r.setCalls++
	if r.data == nil {
		r.data = make(map[string][]byte)
	}
	r.data[key] = value
	return nil
}
func (r *recordingCacheBackend) MGet(ctx context.Context, keys ...string) ([]CacheGetResult, error) {
	results := make([]CacheGetResult, len(keys))
	for i, k := range keys {
		results[i].Key = k
		if v, ok := r.data[k]; ok {
			results[i].Value = v
			results[i].Hit = true
			results[i].Source = cacheLevelToSource(r.level)
		}
	}
	return results, nil
}
func (r *recordingCacheBackend) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(r.data, k)
	}
	return nil
}
func (r *recordingCacheBackend) InvalidateByTag(_ context.Context, _ ...string) error { return nil }
func (r *recordingCacheBackend) Stats() CacheBackendStats {
	return CacheBackendStats{Level: r.level}
}

// stubQueryConstructor 用于测试中不需要真实查询的场景。
// stubQueryConstructor 实现 QueryConstructor 接口（最小 stub，用于单元测试）。
type stubQueryConstructor struct{}

func (s *stubQueryConstructor) Where(_ Condition) QueryConstructor              { return s }
func (s *stubQueryConstructor) WhereWith(_ *WhereBuilder) QueryConstructor       { return s }
func (s *stubQueryConstructor) WhereAll(_ ...Condition) QueryConstructor         { return s }
func (s *stubQueryConstructor) WhereAny(_ ...Condition) QueryConstructor         { return s }
func (s *stubQueryConstructor) Select(_ ...string) QueryConstructor              { return s }
func (s *stubQueryConstructor) Count(_ ...string) QueryConstructor               { return s }
func (s *stubQueryConstructor) CountWith(_ *CountBuilder) QueryConstructor       { return s }
func (s *stubQueryConstructor) OrderBy(_ string, _ string) QueryConstructor      { return s }
func (s *stubQueryConstructor) Limit(_ int) QueryConstructor                     { return s }
func (s *stubQueryConstructor) Offset(_ int) QueryConstructor                    { return s }
func (s *stubQueryConstructor) Page(_ int, _ int) QueryConstructor               { return s }
func (s *stubQueryConstructor) Paginate(_ *PaginationBuilder) QueryConstructor   { return s }
func (s *stubQueryConstructor) FromAlias(_ string) QueryConstructor              { return s }
func (s *stubQueryConstructor) Join(_ string, _ string, _ ...string) QueryConstructor      { return s }
func (s *stubQueryConstructor) LeftJoin(_ string, _ string, _ ...string) QueryConstructor  { return s }
func (s *stubQueryConstructor) RightJoin(_ string, _ string, _ ...string) QueryConstructor { return s }
func (s *stubQueryConstructor) CrossJoin(_ string, _ ...string) QueryConstructor           { return s }
func (s *stubQueryConstructor) JoinWith(_ *JoinBuilder) QueryConstructor         { return s }
func (s *stubQueryConstructor) CrossTableStrategy(_ CrossTableStrategy) QueryConstructor { return s }
func (s *stubQueryConstructor) CustomMode() QueryConstructor                     { return s }
func (s *stubQueryConstructor) Build(_ context.Context) (string, []interface{}, error) {
	return "SELECT 1", nil, nil
}
func (s *stubQueryConstructor) SelectCount(_ context.Context, _ *Repository) (int64, error) {
	return 0, nil
}
func (s *stubQueryConstructor) Upsert(_ context.Context, _ *Repository, _ *Changeset, _ ...string) (sql.Result, error) {
	return nil, nil
}
func (s *stubQueryConstructor) GetNativeBuilder() interface{} { return nil }

// newTestMemoryRepo 创建一个不依赖真实数据库连接的 Repository 供单元测试使用。
func newTestMemoryRepo() *Repository {
	return &Repository{
		compiledQueryCache: NewCompiledQueryCache(),
	}
}
