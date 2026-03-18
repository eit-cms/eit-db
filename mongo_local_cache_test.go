package db

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// ─── GetMongoLocalCache ────────────────────────────────────────────────────────

func TestGetMongoLocalCacheRejectsNonMongo(t *testing.T) {
	_, ok := GetMongoLocalCache(&SQLiteAdapter{})
	if ok {
		t.Error("expected GetMongoLocalCache to return false for non-Mongo adapter")
	}
}

func TestGetMongoLocalCacheAcceptsMongo(t *testing.T) {
	adapter := newDisconnectedMongoAdapter(t)
	cache, ok := GetMongoLocalCache(adapter)
	if !ok {
		t.Fatal("expected GetMongoLocalCache to return true for MongoAdapter")
	}
	if cache == nil {
		t.Error("expected non-nil MongoLocalCache")
	}
}

// ─── Preload 无连接错误路径 ───────────────────────────────────────────────────

func TestLocalCachePreloadRequiresConnection(t *testing.T) {
	adapter := newDisconnectedMongoAdapter(t)
	cache, _ := GetMongoLocalCache(adapter)

	err := cache.Preload(context.Background(), "users", "users", nil, nil, 5*time.Minute)
	if err == nil {
		t.Error("expected error when not connected")
	}
}

func TestLocalCacheForcePreloadRequiresConnection(t *testing.T) {
	adapter := newDisconnectedMongoAdapter(t)
	cache, _ := GetMongoLocalCache(adapter)

	err := cache.ForcePreload(context.Background(), "users", "users", nil, nil, 5*time.Minute)
	if err == nil {
		t.Error("expected error when not connected")
	}
}

// ─── Get / Invalidate / InvalidateAll（纯内存操作，无需连接）────────────────────

func TestLocalCacheGetReturnsNothingWhenEmpty(t *testing.T) {
	adapter := newDisconnectedMongoAdapter(t)
	cache, _ := GetMongoLocalCache(adapter)

	docs, ok := cache.Get("nonexistent")
	if ok || docs != nil {
		t.Errorf("expected (nil, false) for nonexistent key, got (%v, %v)", docs, ok)
	}
}

func TestLocalCacheInvalidate(t *testing.T) {
	cache := newLocalCacheWithEntry("roles", []bson.M{{"_id": "admin", "name": "Administrator"}}, 0)

	if _, ok := cache.Get("roles"); !ok {
		t.Fatal("expected entry to exist before invalidation")
	}
	cache.Invalidate("roles")
	if _, ok := cache.Get("roles"); ok {
		t.Error("expected entry to be gone after Invalidate")
	}
}

func TestLocalCacheInvalidateAll(t *testing.T) {
	cache := newLocalCacheWithEntry("roles", []bson.M{{"_id": "admin"}}, 0)
	cache.entries["perms"] = &mongoLocalCacheEntry{
		docs:      []bson.M{{"code": "read"}},
		fetchedAt: time.Now(),
		ttl:       0,
	}

	cache.InvalidateAll()
	s := cache.Stats()
	if s.Total != 0 {
		t.Errorf("expected 0 entries after InvalidateAll, got %d", s.Total)
	}
}

// ─── TTL-based stale detection ────────────────────────────────────────────────

func TestLocalCacheEntryNotStaleWhenTTLZero(t *testing.T) {
	cache := newLocalCacheWithEntry("cfg", []bson.M{{"key": "val"}}, 0)
	if _, ok := cache.Get("cfg"); !ok {
		t.Error("entry with ttl=0 should never be stale")
	}
}

func TestLocalCacheEntryStaleAfterTTL(t *testing.T) {
	// 手动创建一个已过期的 entry
	adapter := newDisconnectedMongoAdapter(t)
	cache, _ := GetMongoLocalCache(adapter)
	cache.entries["old"] = &mongoLocalCacheEntry{
		docs:      []bson.M{{"k": "v"}},
		fetchedAt: time.Now().Add(-10 * time.Minute), // 10 分钟前
		ttl:       5 * time.Minute,                   // TTL 只有 5 分钟
	}

	if _, ok := cache.Get("old"); ok {
		t.Error("expected stale entry to be treated as miss")
	}
}

// ─── Stats ────────────────────────────────────────────────────────────────────

func TestLocalCacheStats(t *testing.T) {
	cache := newLocalCacheWithEntry("fresh", []bson.M{{"k": 1}}, 0)
	cache.entries["stale"] = &mongoLocalCacheEntry{
		docs:      []bson.M{{"k": 2}},
		fetchedAt: time.Now().Add(-time.Hour),
		ttl:       time.Minute,
	}

	s := cache.Stats()
	if s.Total != 2 {
		t.Errorf("expected Total=2, got %d", s.Total)
	}
	if s.Fresh != 1 {
		t.Errorf("expected Fresh=1, got %d", s.Fresh)
	}
	if s.Stale != 1 {
		t.Errorf("expected Stale=1, got %d", s.Stale)
	}
}

// ─── JoinWith（纯内存 JOIN，无需连接）────────────────────────────────────────

func TestJoinWithN1(t *testing.T) {
	// 维表：roles
	cache := newLocalCacheWithEntry("roles", []bson.M{
		{"_id": "admin", "label": "Administrator"},
		{"_id": "user", "label": "Regular User"},
	}, 0)

	// 主表：users
	users := []bson.M{
		{"name": "Alice", "role_id": "admin"},
		{"name": "Bob", "role_id": "user"},
		{"name": "Carol", "role_id": "unknown"}, // 无匹配
	}

	result := cache.JoinWith(users, "role_id", "roles", "_id", "role", false)

	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}

	// Alice 获得 admin role
	aliceRole, ok := result[0]["role"].(bson.M)
	if !ok || aliceRole == nil {
		t.Error("Alice should have a role")
	} else if aliceRole["label"] != "Administrator" {
		t.Errorf("expected Administrator, got %v", aliceRole["label"])
	}

	// Bob 获得 user role
	bobRole, ok := result[1]["role"].(bson.M)
	if !ok || bobRole["label"] != "Regular User" {
		t.Errorf("Bob should have Regular User role, got %v", result[1]["role"])
	}

	// Carol 无匹配，role 为 nil
	if result[2]["role"] != nil {
		t.Errorf("Carol should have nil role, got %v", result[2]["role"])
	}

	// 原始 localDocs 未被修改
	if _, exists := users[0]["role"]; exists {
		t.Error("original localDocs should not be modified")
	}
}

func TestJoinWithMany(t *testing.T) {
	// 维表：orders（一个用户有多个订单）
	cache := newLocalCacheWithEntry("orders", []bson.M{
		{"order_id": "o1", "user_id": "u1", "amount": 100},
		{"order_id": "o2", "user_id": "u1", "amount": 200},
		{"order_id": "o3", "user_id": "u2", "amount": 50},
	}, 0)

	// 主表：users
	users := []bson.M{
		{"id": "u1", "name": "Alice"},
		{"id": "u2", "name": "Bob"},
	}

	result := cache.JoinWith(users, "id", "orders", "user_id", "orders", true)

	alice, ok := result[0]["orders"].([]bson.M)
	if !ok {
		t.Fatal("expected []bson.M for Alice's orders")
	}
	if len(alice) != 2 {
		t.Errorf("expected 2 orders for Alice, got %d", len(alice))
	}

	bob, ok := result[1]["orders"].([]bson.M)
	if !ok {
		t.Fatal("expected []bson.M for Bob's orders")
	}
	if len(bob) != 1 {
		t.Errorf("expected 1 order for Bob, got %d", len(bob))
	}
}

func TestJoinWithReturnsOriginalWhenCacheMiss(t *testing.T) {
	adapter := newDisconnectedMongoAdapter(t)
	cache, _ := GetMongoLocalCache(adapter)

	docs := []bson.M{{"id": "1"}}
	result := cache.JoinWith(docs, "id", "nonexistent", "_id", "ref", false)

	if len(result) != 1 || result[0]["id"] != "1" {
		t.Error("should return original docs when cache misses")
	}
}

// ─── 辅助：创建预填充 entry 的 MongoLocalCache ──────────────────────────────

func newLocalCacheWithEntry(name string, docs []bson.M, ttl time.Duration) *MongoLocalCache {
	adapter := &MongoAdapter{database: "test"}
	cache := &MongoLocalCache{
		adapter: adapter,
		entries: make(map[string]*mongoLocalCacheEntry),
	}
	cache.entries[name] = &mongoLocalCacheEntry{
		docs:      docs,
		fetchedAt: time.Now(),
		ttl:       ttl,
	}
	return cache
}

// ─── Integration：Preload + JoinWith（需要 MONGO_URI）────────────────────────

func TestMongoLocalCachePreloadAndJoin(t *testing.T) {
	mongoAdapter := requireMongoConnection(t)
	ctx := context.Background()

	// 先插入一些角色文档
	rolesCol := mongoAdapter.client.Database(mongoAdapter.database).Collection("test_cache_roles")
	_, _ = rolesCol.DeleteMany(ctx, bson.D{})
	_, err := rolesCol.InsertMany(ctx, []interface{}{
		bson.M{"_id": "admin", "label": "Administrator"},
		bson.M{"_id": "user", "label": "Regular User"},
	})
	if err != nil {
		t.Fatalf("InsertMany roles: %v", err)
	}

	cache, _ := GetMongoLocalCache(mongoAdapter)
	err = cache.Preload(ctx, "roles", "test_cache_roles", nil, nil, 5*time.Minute)
	if err != nil {
		t.Fatalf("Preload: %v", err)
	}

	docs, ok := cache.Get("roles")
	if !ok || len(docs) != 2 {
		t.Fatalf("expected 2 cached roles, got %d (ok=%v)", len(docs), ok)
	}

	users := []bson.M{
		{"name": "Alice", "role_id": "admin"},
		{"name": "Bob", "role_id": "user"},
	}
	enriched := cache.JoinWith(users, "role_id", "roles", "_id", "role", false)
	for _, u := range enriched {
		if u["role"] == nil {
			t.Errorf("user %v should have a role", u["name"])
		}
	}
}
