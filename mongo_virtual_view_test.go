package db

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// ─── GetMongoVirtualView ──────────────────────────────────────────────────────

func TestGetMongoVirtualViewRejectsNonMongo(t *testing.T) {
	_, ok := GetMongoVirtualView(&SQLiteAdapter{})
	if ok {
		t.Error("expected GetMongoVirtualView to return false for non-Mongo adapter")
	}
}

func TestGetMongoVirtualViewAcceptsMongo(t *testing.T) {
	adapter := newDisconnectedMongoAdapter(t)
	vv, ok := GetMongoVirtualView(adapter)
	if !ok {
		t.Fatal("expected GetMongoVirtualView to return true for MongoAdapter")
	}
	if vv == nil {
		t.Error("expected non-nil MongoVirtualView")
	}
}

// ─── Define / IsDefined / DefinedViews ────────────────────────────────────────

func TestVirtualViewDefineAndIsDefined(t *testing.T) {
	vv := newDisconnectedVirtualView(t)

	if vv.IsDefined("active_users") {
		t.Error("should not be defined before Define call")
	}

	pipeline := bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
	}
	vv.Define("active_users", "users", pipeline, 5*time.Minute)

	if !vv.IsDefined("active_users") {
		t.Error("expected view to be defined after Define")
	}
}

func TestVirtualViewDefinedViews(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	vv.Define("view_a", "col_a", bson.A{}, time.Minute)
	vv.Define("view_b", "col_b", bson.A{}, time.Minute)

	names := vv.DefinedViews()
	if len(names) != 2 {
		t.Errorf("expected 2 defined views, got %d", len(names))
	}
}

// ─── Execute / Refresh 无连接错误路径 ────────────────────────────────────────

func TestVirtualViewExecuteUndefinedReturnsError(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	_, err := vv.Execute(context.Background(), "ghost_view")
	if err == nil {
		t.Error("expected error for undefined view")
	}
}

func TestVirtualViewExecuteRequiresConnection(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	vv.Define("my_view", "users", bson.A{}, time.Minute)

	_, err := vv.Execute(context.Background(), "my_view")
	if err == nil {
		t.Error("expected error when not connected")
	}
}

func TestVirtualViewRefreshRequiresConnection(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	vv.Define("my_view", "users", bson.A{}, time.Minute)

	_, err := vv.Refresh(context.Background(), "my_view")
	if err == nil {
		t.Error("expected error when not connected")
	}
}

// ─── IsCached / InvalidateView / InvalidateAll（纯内存操作）─────────────────

func TestVirtualViewIsCachedFalseAfterDefine(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	vv.Define("report", "orders", bson.A{}, time.Minute)

	if vv.IsCached("report") {
		t.Error("view should not be cached immediately after Define")
	}
}

func TestVirtualViewIsCachedTrueAfterManualLoad(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	vv.Define("report", "orders", bson.A{}, time.Minute)

	// 手动注入缓存（模拟执行完成）
	vv.cache["report"] = &virtualViewCacheEntry{
		docs:     []bson.M{{"total": 42}},
		cachedAt: time.Now(),
		ttl:      time.Minute,
	}

	if !vv.IsCached("report") {
		t.Error("expected view to be cached")
	}
}

func TestVirtualViewInvalidateView(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	vv.Define("report", "orders", bson.A{}, time.Minute)
	vv.cache["report"] = &virtualViewCacheEntry{
		docs:     []bson.M{{"total": 42}},
		cachedAt: time.Now(),
		ttl:      time.Minute,
	}

	vv.InvalidateView("report")
	if vv.IsCached("report") {
		t.Error("expected cache to be cleared after InvalidateView")
	}
	// 视图定义仍然存在
	if !vv.IsDefined("report") {
		t.Error("view definition should persist after InvalidateView")
	}
}

func TestVirtualViewInvalidateAll(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	vv.cache["v1"] = &virtualViewCacheEntry{docs: []bson.M{}, cachedAt: time.Now(), ttl: time.Minute}
	vv.cache["v2"] = &virtualViewCacheEntry{docs: []bson.M{}, cachedAt: time.Now(), ttl: time.Minute}

	vv.InvalidateAll()
	if vv.IsCached("v1") || vv.IsCached("v2") {
		t.Error("expected all caches to be cleared after InvalidateAll")
	}
}

// ─── Stale cache detection ────────────────────────────────────────────────────

func TestVirtualViewStaleEntryShouldNotBeCached(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	vv.Define("stale_report", "orders", bson.A{}, time.Minute)
	vv.cache["stale_report"] = &virtualViewCacheEntry{
		docs:     []bson.M{{"total": 42}},
		cachedAt: time.Now().Add(-2 * time.Minute), // 2 分钟前缓存，TTL 1 分钟
		ttl:      time.Minute,
	}

	if vv.IsCached("stale_report") {
		t.Error("stale entry should not count as cached")
	}
}

func TestVirtualViewZeroTTLNeverExpires(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	vv.cache["permanent"] = &virtualViewCacheEntry{
		docs:     []bson.M{{"k": "v"}},
		cachedAt: time.Now().Add(-24 * time.Hour), // 1天前
		ttl:      0,                               // 永不过期
	}

	if !vv.IsCached("permanent") {
		t.Error("ttl=0 entry should never expire")
	}
}

// ─── Redefine clears old cache ────────────────────────────────────────────────

func TestVirtualViewRedefineClresOldCache(t *testing.T) {
	vv := newDisconnectedVirtualView(t)
	vv.Define("rep", "col", bson.A{}, time.Minute)
	vv.cache["rep"] = &virtualViewCacheEntry{
		docs:     []bson.M{{"old": "data"}},
		cachedAt: time.Now(),
		ttl:      time.Minute,
	}

	// 重新 Define 同名视图：应清除旧缓存
	vv.Define("rep", "col", bson.A{bson.D{{Key: "$limit", Value: 10}}}, time.Minute)

	if vv.IsCached("rep") {
		t.Error("redefining a view should clear its cache")
	}
}

// ─── Integration：Execute 完整流程（需要 MONGO_URI）──────────────────────────

func TestMongoVirtualViewExecuteAndCache(t *testing.T) {
	mongoAdapter := requireMongoConnection(t)
	ctx := context.Background()

	// 准备数据
	col := mongoAdapter.client.Database(mongoAdapter.database).Collection("test_vv_orders")
	_, _ = col.DeleteMany(ctx, bson.D{})
	_, err := col.InsertMany(ctx, []interface{}{
		bson.M{"status": "active", "amount": 100},
		bson.M{"status": "active", "amount": 200},
		bson.M{"status": "closed", "amount": 50},
	})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	vv, _ := GetMongoVirtualView(mongoAdapter)
	vv.Define("active_orders", "test_vv_orders",
		bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "status", Value: "active"}}}},
		},
		5*time.Minute,
	)

	// 第一次执行（应走 aggregation）
	docs, err := vv.Execute(ctx, "active_orders")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("expected 2 active orders, got %d", len(docs))
	}
	if !vv.IsCached("active_orders") {
		t.Error("expected view to be cached after first Execute")
	}

	// 第二次执行（应从缓存返回）
	docs2, err := vv.Execute(ctx, "active_orders")
	if err != nil {
		t.Fatalf("Execute (cache hit): %v", err)
	}
	if len(docs2) != 2 {
		t.Errorf("expected 2 from cache, got %d", len(docs2))
	}

	// Refresh 应重新拉取
	vv.InvalidateView("active_orders")
	docs3, err := vv.Refresh(ctx, "active_orders")
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if len(docs3) != 2 {
		t.Errorf("expected 2 after refresh, got %d", len(docs3))
	}
}

// ─── 辅助 ─────────────────────────────────────────────────────────────────────

func newDisconnectedVirtualView(t *testing.T) *MongoVirtualView {
	t.Helper()
	adapter := newDisconnectedMongoAdapter(t)
	vv, ok := GetMongoVirtualView(adapter)
	if !ok {
		t.Fatal("GetMongoVirtualView failed")
	}
	return vv
}
