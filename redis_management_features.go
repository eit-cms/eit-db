package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// CollaborationAdapterNodePresence 描述协作适配器在线状态。
type CollaborationAdapterNodePresence struct {
	NodeID              string
	AdapterType         string
	AdapterID           string
	Group               string
	Namespace           string
	Status              string
	LastHeartbeatUnixMs int64
	Capabilities        []string
}

// CollaborationAdapterGroupEvent 描述 Redis 管理频道事件。
type CollaborationAdapterGroupEvent struct {
	Namespace   string                 `json:"namespace"`
	Group       string                 `json:"group"`
	NodeID      string                 `json:"node_id,omitempty"`
	EventType   string                 `json:"event_type"`
	Status      string                 `json:"status,omitempty"`
	TimestampMs int64                  `json:"timestamp_ms"`
	Payload     map[string]interface{} `json:"payload,omitempty"`
}

// RedisManagementFeatures 提供协作适配器分组与健康管理能力。
type RedisManagementFeatures struct {
	adapter *RedisAdapter
}

// GetRedisManagementFeatures 从 Adapter 获取 Redis 管理能力。
func GetRedisManagementFeatures(adapter Adapter) (*RedisManagementFeatures, bool) {
	r, ok := adapter.(*RedisAdapter)
	if !ok {
		return nil, false
	}
	return &RedisManagementFeatures{adapter: r}, true
}

// GetRedisManagementFeatures 返回当前 Repository 绑定适配器的 Redis 管理能力视图。
func (r *Repository) GetRedisManagementFeatures() (*RedisManagementFeatures, bool) {
	if r == nil {
		return nil, false
	}
	return GetRedisManagementFeatures(r.GetAdapter())
}

func normalizeManagementNamespace(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return "default"
	}
	return namespace
}

func normalizeManagementStatus(status string) string {
	status = strings.ToLower(strings.TrimSpace(status))
	if status == "" {
		return "online"
	}
	return status
}

func validatePresenceNode(node *CollaborationAdapterNodePresence) error {
	if node == nil {
		return fmt.Errorf("collaboration adapter node presence cannot be nil")
	}
	if strings.TrimSpace(node.NodeID) == "" {
		return fmt.Errorf("collaboration adapter node id must not be empty")
	}
	if strings.TrimSpace(node.Group) == "" {
		return fmt.Errorf("collaboration adapter group must not be empty")
	}
	return nil
}

func (f *RedisManagementFeatures) keyNode(namespace string, nodeID string) string {
	return fmt.Sprintf("collab:mgmt:%s:node:%s", normalizeManagementNamespace(namespace), strings.TrimSpace(nodeID))
}

func (f *RedisManagementFeatures) keyNodeGroups(namespace string, nodeID string) string {
	return fmt.Sprintf("collab:mgmt:%s:node:%s:groups", normalizeManagementNamespace(namespace), strings.TrimSpace(nodeID))
}

func (f *RedisManagementFeatures) keyGroupMembers(namespace string, group string) string {
	return fmt.Sprintf("collab:mgmt:%s:group:%s:members", normalizeManagementNamespace(namespace), strings.TrimSpace(group))
}

func (f *RedisManagementFeatures) channelGroupEvents(namespace string, group string) string {
	return fmt.Sprintf("collab:mgmt:%s:group:%s:events", normalizeManagementNamespace(namespace), strings.TrimSpace(group))
}

// RegisterAdapterNode 注册在线适配器并写入分组与健康状态。
func (f *RedisManagementFeatures) RegisterAdapterNode(ctx context.Context, node *CollaborationAdapterNodePresence, ttl time.Duration) error {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return fmt.Errorf("redis management features unavailable")
	}
	if err := validatePresenceNode(node); err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = 45 * time.Second
	}
	if node.LastHeartbeatUnixMs <= 0 {
		node.LastHeartbeatUnixMs = time.Now().UnixMilli()
	}
	node.Status = normalizeManagementStatus(node.Status)
	node.Namespace = normalizeManagementNamespace(node.Namespace)

	nodeKey := f.keyNode(node.Namespace, node.NodeID)
	nodeGroupsKey := f.keyNodeGroups(node.Namespace, node.NodeID)
	groupMembersKey := f.keyGroupMembers(node.Namespace, node.Group)

	_, err := f.adapter.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, nodeKey,
			"node_id", node.NodeID,
			"adapter_type", strings.TrimSpace(node.AdapterType),
			"adapter_id", strings.TrimSpace(node.AdapterID),
			"group", strings.TrimSpace(node.Group),
			"namespace", node.Namespace,
			"status", node.Status,
			"last_heartbeat_unix_ms", node.LastHeartbeatUnixMs,
		)
		pipe.Expire(ctx, nodeKey, ttl)
		pipe.SAdd(ctx, groupMembersKey, node.NodeID)
		pipe.SAdd(ctx, nodeGroupsKey, node.Group)
		pipe.Expire(ctx, nodeGroupsKey, ttl)
		return nil
	})
	if err != nil {
		return err
	}

	_, err = f.PublishGroupEvent(ctx, node.Namespace, node.Group, CollaborationAdapterGroupEvent{
		Namespace:   node.Namespace,
		Group:       node.Group,
		NodeID:      node.NodeID,
		EventType:   "adapter.registered",
		Status:      node.Status,
		TimestampMs: node.LastHeartbeatUnixMs,
		Payload: map[string]interface{}{
			"adapter_type": node.AdapterType,
			"adapter_id":   node.AdapterID,
		},
	})
	return err
}

// HeartbeatAdapterNode 更新适配器心跳。
func (f *RedisManagementFeatures) HeartbeatAdapterNode(ctx context.Context, namespace string, group string, nodeID string, ttl time.Duration) error {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return fmt.Errorf("redis management features unavailable")
	}
	namespace = normalizeManagementNamespace(namespace)
	group = strings.TrimSpace(group)
	nodeID = strings.TrimSpace(nodeID)
	if group == "" || nodeID == "" {
		return fmt.Errorf("group and node id must not be empty")
	}
	if ttl <= 0 {
		ttl = 45 * time.Second
	}
	now := time.Now().UnixMilli()
	nodeKey := f.keyNode(namespace, nodeID)
	nodeGroupsKey := f.keyNodeGroups(namespace, nodeID)
	groupMembersKey := f.keyGroupMembers(namespace, group)

	_, err := f.adapter.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, nodeKey,
			"node_id", nodeID,
			"group", group,
			"namespace", namespace,
			"status", "online",
			"last_heartbeat_unix_ms", now,
		)
		pipe.Expire(ctx, nodeKey, ttl)
		pipe.SAdd(ctx, groupMembersKey, nodeID)
		pipe.SAdd(ctx, nodeGroupsKey, group)
		pipe.Expire(ctx, nodeGroupsKey, ttl)
		return nil
	})
	if err != nil {
		return err
	}
	_, err = f.PublishGroupEvent(ctx, namespace, group, CollaborationAdapterGroupEvent{
		Namespace:   namespace,
		Group:       group,
		NodeID:      nodeID,
		EventType:   "adapter.heartbeat",
		Status:      "online",
		TimestampMs: now,
	})
	return err
}

// MarkAdapterOffline 标记节点离线并从指定分组移除。
func (f *RedisManagementFeatures) MarkAdapterOffline(ctx context.Context, namespace string, group string, nodeID string) error {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return fmt.Errorf("redis management features unavailable")
	}
	namespace = normalizeManagementNamespace(namespace)
	group = strings.TrimSpace(group)
	nodeID = strings.TrimSpace(nodeID)
	if group == "" || nodeID == "" {
		return fmt.Errorf("group and node id must not be empty")
	}
	groupMembersKey := f.keyGroupMembers(namespace, group)
	nodeKey := f.keyNode(namespace, nodeID)
	now := time.Now().UnixMilli()

	_, err := f.adapter.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.SRem(ctx, groupMembersKey, nodeID)
		pipe.HSet(ctx, nodeKey,
			"status", "offline",
			"last_heartbeat_unix_ms", now,
		)
		pipe.Expire(ctx, nodeKey, 5*time.Minute)
		return nil
	})
	if err != nil {
		return err
	}
	_, err = f.PublishGroupEvent(ctx, namespace, group, CollaborationAdapterGroupEvent{
		Namespace:   namespace,
		Group:       group,
		NodeID:      nodeID,
		EventType:   "adapter.offline",
		Status:      "offline",
		TimestampMs: now,
	})
	return err
}

// ListGroupNodes 返回指定分组的在线/离线节点快照（自动清理过期成员）。
func (f *RedisManagementFeatures) ListGroupNodes(ctx context.Context, namespace string, group string) ([]CollaborationAdapterNodePresence, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return nil, fmt.Errorf("redis management features unavailable")
	}
	namespace = normalizeManagementNamespace(namespace)
	group = strings.TrimSpace(group)
	if group == "" {
		return nil, fmt.Errorf("group must not be empty")
	}
	groupMembersKey := f.keyGroupMembers(namespace, group)
	members, err := f.adapter.SMembers(ctx, groupMembersKey)
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	out := make([]CollaborationAdapterNodePresence, 0, len(members))
	for _, nodeID := range members {
		nodeKey := f.keyNode(namespace, nodeID)
		fields, getErr := f.adapter.HGetAll(ctx, nodeKey)
		if getErr != nil && getErr != redis.Nil {
			return nil, getErr
		}
		if len(fields) == 0 {
			_, _ = f.adapter.SRem(ctx, groupMembersKey, nodeID)
			continue
		}
		lastHeartbeat := int64(0)
		if raw := strings.TrimSpace(fields["last_heartbeat_unix_ms"]); raw != "" {
			if parsed, parseErr := strconv.ParseInt(raw, 10, 64); parseErr == nil {
				lastHeartbeat = parsed
			}
		}
		out = append(out, CollaborationAdapterNodePresence{
			NodeID:              firstNonEmpty(fields["node_id"], nodeID),
			AdapterType:         fields["adapter_type"],
			AdapterID:           fields["adapter_id"],
			Group:               firstNonEmpty(fields["group"], group),
			Namespace:           firstNonEmpty(fields["namespace"], namespace),
			Status:              normalizeManagementStatus(fields["status"]),
			LastHeartbeatUnixMs: lastHeartbeat,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].NodeID < out[j].NodeID
	})
	return out, nil
}

// PublishGroupEvent 向分组管理频道发布事件。
func (f *RedisManagementFeatures) PublishGroupEvent(ctx context.Context, namespace string, group string, event CollaborationAdapterGroupEvent) (int64, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return 0, fmt.Errorf("redis management features unavailable")
	}
	namespace = normalizeManagementNamespace(namespace)
	group = strings.TrimSpace(group)
	if group == "" {
		return 0, fmt.Errorf("group must not be empty")
	}
	if strings.TrimSpace(event.EventType) == "" {
		return 0, fmt.Errorf("event_type must not be empty")
	}
	if event.TimestampMs <= 0 {
		event.TimestampMs = time.Now().UnixMilli()
	}
	if strings.TrimSpace(event.Namespace) == "" {
		event.Namespace = namespace
	}
	if strings.TrimSpace(event.Group) == "" {
		event.Group = group
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	return f.adapter.Publish(ctx, f.channelGroupEvents(namespace, group), string(raw))
}

// SubscribeGroupEvents 订阅指定分组事件频道。
func (f *RedisManagementFeatures) SubscribeGroupEvents(ctx context.Context, namespace string, group string) *redis.PubSub {
	namespace = normalizeManagementNamespace(namespace)
	group = strings.TrimSpace(group)
	return f.adapter.Subscribe(ctx, f.channelGroupEvents(namespace, group))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
