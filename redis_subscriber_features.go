package db

import (
	"context"

	redis "github.com/redis/go-redis/v9"
)

// RedisSubscriberFeatures 提供 Redis 发布订阅能力视图。
//
// 该能力既可用于 Collaboration Mode 的协作消息层，也可供用户自有 Redis 业务直接复用。
// 特性视图不会改变 Adapter 主契约，按需通过类型能力获取。
//
// 示例：
//
//	feat, ok := db.GetRedisSubscriberFeatures(repo.GetAdapter())
//	if !ok { ... }
//	pubSub := feat.Subscribe(ctx, "collab.events")
//	defer pubSub.Close()
//	_ = feat.Publish(ctx, "collab.events", "hello")
type RedisSubscriberFeatures struct {
	adapter *RedisAdapter
}

// GetRedisSubscriberFeatures 从 Adapter 获取 Redis subscriber 能力。
// 若 adapter 不是 *RedisAdapter，返回 (nil, false)。
func GetRedisSubscriberFeatures(adapter Adapter) (*RedisSubscriberFeatures, bool) {
	r, ok := adapter.(*RedisAdapter)
	if !ok {
		return nil, false
	}
	return &RedisSubscriberFeatures{adapter: r}, true
}

// GetRedisSubscriberFeatures 返回当前 Repository 绑定适配器的 Redis subscriber 能力视图。
func (r *Repository) GetRedisSubscriberFeatures() (*RedisSubscriberFeatures, bool) {
	if r == nil {
		return nil, false
	}
	return GetRedisSubscriberFeatures(r.GetAdapter())
}

// Publish 向频道发布消息。
func (f *RedisSubscriberFeatures) Publish(ctx context.Context, channel string, message interface{}) (int64, error) {
	return f.adapter.Publish(ctx, channel, message)
}

// Subscribe 订阅一个或多个频道。
func (f *RedisSubscriberFeatures) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return f.adapter.Subscribe(ctx, channels...)
}

// PSubscribe 按模式订阅一个或多个频道。
func (f *RedisSubscriberFeatures) PSubscribe(ctx context.Context, patterns ...string) *redis.PubSub {
	return f.adapter.PSubscribe(ctx, patterns...)
}
