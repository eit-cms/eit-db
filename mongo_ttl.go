package db

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoTTLFeatures 提供 MongoDB TTL 索引管理和文档过期时间设置的特色能力。
//
// MongoDB 原生支持 TTL 索引：在 Date 类型字段上建立 expireAfterSeconds 索引后，
// MongoDB 后台线程每 60 秒轮询一次，自动删除已过期的文档。
//
// 适用场景：Session token、临时验证码、速率限制计数器、日志保留策略等。
//
// 示例：
//
//	ttlFeat, ok := db.GetMongoTTLFeatures(repo.GetAdapter())
//	if !ok { ... }
//	_ = ttlFeat.EnsureTTLIndex(ctx, "sessions", "expires_at", 0)  // 精准到期
//	_ = ttlFeat.InsertWithExpiry(ctx, "sessions", bson.M{"token": "abc"}, "expires_at",
//	        time.Now().Add(30*time.Minute))
type MongoTTLFeatures struct {
	adapter *MongoAdapter
}

// GetMongoTTLFeatures 从 Adapter 获取 MongoDB TTL 特性视图。
// 若 adapter 不是 *MongoAdapter，返回 (nil, false)。
func GetMongoTTLFeatures(adapter Adapter) (*MongoTTLFeatures, bool) {
	m, ok := adapter.(*MongoAdapter)
	if !ok {
		return nil, false
	}
	return &MongoTTLFeatures{adapter: m}, true
}

// TTLIndexInfo 描述一个 TTL 索引的元信息。
type TTLIndexInfo struct {
	// Name 索引名称（MongoDB 自动生成或指定）
	Name string
	// Field 建立 TTL 索引的字段名
	Field string
	// ExpireAfter 文档字段时间到达后再延迟多久删除（0 表示精准到期即删）
	ExpireAfter time.Duration
}

// EnsureTTLIndex 在集合的指定字段上幂等地创建 TTL 索引。
//
//   - collection: MongoDB 集合名
//   - field: 必须是 BSON Date 类型字段（如 "expires_at"、"created_at"）
//   - expireAfter: 文档超过字段记录时间后延迟多久被删除（0 = 精准到期）
//
// 若同名/同字段索引已存在（IndexOptionsConflict code 85/86），视为幂等成功。
func (f *MongoTTLFeatures) EnsureTTLIndex(ctx context.Context, collection, field string, expireAfter time.Duration) error {
	if f.adapter.client == nil {
		return fmt.Errorf("mongodb: not connected")
	}
	expireSecs := int32(expireAfter.Seconds())
	coll := f.adapter.client.Database(f.adapter.database).Collection(collection)
	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: field, Value: 1}},
		Options: options.Index().
			SetExpireAfterSeconds(expireSecs).
			SetSparse(true),
	}
	_, err := coll.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		if isMongoIndexConflict(err) {
			return nil
		}
		return fmt.Errorf("mongodb: ensure ttl index on %s.%s: %w", collection, field, err)
	}
	return nil
}

// DropTTLIndex 删除指定名称的 TTL 索引。
func (f *MongoTTLFeatures) DropTTLIndex(ctx context.Context, collection, indexName string) error {
	if f.adapter.client == nil {
		return fmt.Errorf("mongodb: not connected")
	}
	coll := f.adapter.client.Database(f.adapter.database).Collection(collection)
	_, err := coll.Indexes().DropOne(ctx, indexName)
	if err != nil {
		return fmt.Errorf("mongodb: drop ttl index %s on %s: %w", indexName, collection, err)
	}
	return nil
}

// ListTTLIndexes 列出指定集合上所有带 expireAfterSeconds 的 TTL 索引。
func (f *MongoTTLFeatures) ListTTLIndexes(ctx context.Context, collection string) ([]*TTLIndexInfo, error) {
	if f.adapter.client == nil {
		return nil, fmt.Errorf("mongodb: not connected")
	}
	coll := f.adapter.client.Database(f.adapter.database).Collection(collection)
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("mongodb: list indexes on %s: %w", collection, err)
	}
	defer cursor.Close(ctx)

	var results []*TTLIndexInfo
	for cursor.Next(ctx) {
		var raw bson.M
		if err := cursor.Decode(&raw); err != nil {
			continue
		}
		eas, ok := raw["expireAfterSeconds"]
		if !ok {
			continue
		}
		expireSecs, ok := toMongoInt64(eas)
		if !ok {
			continue
		}
		info := &TTLIndexInfo{
			ExpireAfter: time.Duration(expireSecs) * time.Second,
		}
		if name, ok := raw["name"].(string); ok {
			info.Name = name
		}
		// key doc: {"fieldName": 1}
		if keyDoc, ok := raw["key"].(bson.M); ok {
			for k := range keyDoc {
				info.Field = k
				break
			}
		}
		results = append(results, info)
	}
	return results, cursor.Err()
}

// InsertWithExpiry 插入一个文档并自动设置过期时间字段。
//
//   - collection: MongoDB 集合名
//   - doc: 要插入的文档（会被写入 ttlField 字段，调用方 doc 也会被修改）
//   - ttlField: 文档中表示过期时间点的字段名（须与对应 TTL 索引字段一致）
//   - expiresAt: 文档应在此时间点被 MongoDB 自动删除
func (f *MongoTTLFeatures) InsertWithExpiry(ctx context.Context, collection string, doc bson.M, ttlField string, expiresAt time.Time) error {
	if f.adapter.client == nil {
		return fmt.Errorf("mongodb: not connected")
	}
	if ttlField == "" {
		ttlField = "expires_at"
	}
	doc[ttlField] = expiresAt.UTC()
	coll := f.adapter.client.Database(f.adapter.database).Collection(collection)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("mongodb: insert with expiry into %s: %w", collection, err)
	}
	return nil
}

// ExtendExpiry 更新符合 filter 的文档的过期时间字段，实现 TTL 续期。
//
//   - filter: 用于定位目标文档的 BSON 过滤条件
//   - ttlField: 文档中表示过期时间点的字段名
//   - newExpiry: 新的过期时间点
func (f *MongoTTLFeatures) ExtendExpiry(ctx context.Context, collection string, filter bson.D, ttlField string, newExpiry time.Time) error {
	if f.adapter.client == nil {
		return fmt.Errorf("mongodb: not connected")
	}
	if ttlField == "" {
		ttlField = "expires_at"
	}
	coll := f.adapter.client.Database(f.adapter.database).Collection(collection)
	update := bson.D{{Key: "$set", Value: bson.D{{Key: ttlField, Value: newExpiry.UTC()}}}}
	_, err := coll.UpdateMany(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("mongodb: extend expiry on %s: %w", collection, err)
	}
	return nil
}

// isMongoIndexConflict 检测是否为 MongoDB 索引选项冲突错误（幂等可忽略）。
// code 85 = IndexOptionsConflict，code 86 = IndexKeySpecsConflict。
func isMongoIndexConflict(err error) bool {
	if err == nil {
		return false
	}
	cmdErr, ok := err.(mongo.CommandError)
	if !ok {
		return false
	}
	return cmdErr.Code == 85 || cmdErr.Code == 86
}

// toMongoInt64 兼容 bson.M 中 expireAfterSeconds 的各种数值类型。
func toMongoInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	}
	return 0, false
}
