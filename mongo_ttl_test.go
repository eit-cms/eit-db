package db

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// ─── 辅助：跳过需要真实 MongoDB 连接的 Integration 测试 ────────────────────────

func requireMongoConnection(t *testing.T) *MongoAdapter {
	t.Helper()
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		t.Skip("MONGO_URI not set, skipping MongoDB integration test")
	}
	cfg := &Config{
		Adapter:  "mongodb",
		Database: "eit_test",
		MongoDB:  &MongoConnectionConfig{URI: uri, Database: "eit_test"},
	}
	adapter, err := NewMongoAdapter(cfg)
	if err != nil {
		t.Fatalf("NewMongoAdapter: %v", err)
	}
	if err := adapter.Connect(context.Background(), nil); err != nil {
		t.Fatalf("MongoDB connect: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	return adapter
}

// ─── GetMongoTTLFeatures ──────────────────────────────────────────────────────

func TestGetMongoTTLFeaturesRejectsNonMongo(t *testing.T) {
	_, ok := GetMongoTTLFeatures(&SQLiteAdapter{})
	if ok {
		t.Error("expected GetMongoTTLFeatures to return false for non-Mongo adapter")
	}
}

func TestGetMongoTTLFeaturesAcceptsMongo(t *testing.T) {
	adapter := newDisconnectedMongoAdapter(t)
	feat, ok := GetMongoTTLFeatures(adapter)
	if !ok {
		t.Fatal("expected GetMongoTTLFeatures to return true for MongoAdapter")
	}
	if feat == nil {
		t.Error("expected non-nil MongoTTLFeatures")
	}
}

// ─── EnsureTTLIndex / DropTTLIndex / ListTTLIndexes（无连接错误路径）────────────

func TestTTLFeaturesRequireConnection(t *testing.T) {
	adapter := newDisconnectedMongoAdapter(t)
	feat, _ := GetMongoTTLFeatures(adapter)
	ctx := context.Background()

	if err := feat.EnsureTTLIndex(ctx, "col", "expires_at", time.Hour); err == nil {
		t.Error("expected error when not connected")
	}
	if err := feat.DropTTLIndex(ctx, "col", "idx"); err == nil {
		t.Error("expected error when not connected")
	}
	if _, err := feat.ListTTLIndexes(ctx, "col"); err == nil {
		t.Error("expected error when not connected")
	}
	if err := feat.InsertWithExpiry(ctx, "col", bson.M{"k": "v"}, "expires_at", time.Now().Add(time.Hour)); err == nil {
		t.Error("expected error when not connected")
	}
	if err := feat.ExtendExpiry(ctx, "col", bson.D{}, "expires_at", time.Now().Add(2*time.Hour)); err == nil {
		t.Error("expected error when not connected")
	}
}

// ─── TTL 字段默认值 ────────────────────────────────────────────────────────────

func TestInsertWithExpiryUsesDefaultField(t *testing.T) {
	// 只能通过无连接路径测试：确保传入空 ttlField 仍会报"not connected"而不是 panic
	adapter := newDisconnectedMongoAdapter(t)
	feat, _ := GetMongoTTLFeatures(adapter)
	err := feat.InsertWithExpiry(context.Background(), "col", bson.M{}, "", time.Now())
	if err == nil {
		t.Error("expected not-connected error")
	}
}

// ─── toMongoInt64 辅助函数 ─────────────────────────────────────────────────────

func TestToMongoInt64Variants(t *testing.T) {
	cases := []struct {
		input interface{}
		want  int64
		ok    bool
	}{
		{int32(60), 60, true},
		{int64(3600), 3600, true},
		{float64(300.9), 300, true},
		{"60", 0, false},
		{nil, 0, false},
	}
	for _, tc := range cases {
		got, ok := toMongoInt64(tc.input)
		if ok != tc.ok || got != tc.want {
			t.Errorf("toMongoInt64(%v): got (%v,%v), want (%v,%v)", tc.input, got, ok, tc.want, tc.ok)
		}
	}
}

// ─── isMongoIndexConflict ──────────────────────────────────────────────────────

func TestIsMongoIndexConflict(t *testing.T) {
	if isMongoIndexConflict(nil) {
		t.Error("nil error should not be a conflict")
	}
	// 非 CommandError 类型
	nonCmd := context.DeadlineExceeded
	if isMongoIndexConflict(nonCmd) {
		t.Error("DeadlineExceeded is not an index conflict")
	}
}

// ─── Integration：TTL 索引生命周期（需要 MONGO_URI）─────────────────────────────

func TestMongoTTLIndexLifecycle(t *testing.T) {
	mongoAdapter := requireMongoConnection(t)
	feat, _ := GetMongoTTLFeatures(mongoAdapter)
	ctx := context.Background()

	collection := "test_ttl_lifecycle"

	// 创建 TTL 索引（幂等）
	if err := feat.EnsureTTLIndex(ctx, collection, "expires_at", 0); err != nil {
		t.Fatalf("EnsureTTLIndex: %v", err)
	}
	// 再次调用应幂等
	if err := feat.EnsureTTLIndex(ctx, collection, "expires_at", 0); err != nil {
		t.Fatalf("EnsureTTLIndex idempotent: %v", err)
	}

	// 列出 TTL 索引
	indexes, err := feat.ListTTLIndexes(ctx, collection)
	if err != nil {
		t.Fatalf("ListTTLIndexes: %v", err)
	}
	found := false
	for _, idx := range indexes {
		if idx.Field == "expires_at" {
			found = true
		}
	}
	if !found {
		t.Error("expected to find TTL index on expires_at")
	}

	// 插入带过期的文档
	doc := bson.M{"content": "temporary data"}
	if err := feat.InsertWithExpiry(ctx, collection, doc, "expires_at", time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("InsertWithExpiry: %v", err)
	}
}

// ─── 辅助：创建未连接的 MongoAdapter ──────────────────────────────────────────

func newDisconnectedMongoAdapter(t *testing.T) *MongoAdapter {
	t.Helper()
	cfg := &Config{
		Adapter:  "mongodb",
		Database: "test",
		MongoDB:  &MongoConnectionConfig{URI: "mongodb://localhost:27017", Database: "test"},
	}
	a, err := NewMongoAdapter(cfg)
	if err != nil {
		t.Fatalf("NewMongoAdapter: %v", err)
	}
	return a
}
