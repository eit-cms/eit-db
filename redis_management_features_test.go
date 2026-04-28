package db

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
)

func TestRedisManagementFeaturesRegisterHeartbeatOfflineAndList(t *testing.T) {
	features, cleanup := newTestRedisManagementFeatures(t)
	defer cleanup()

	ctx := context.Background()
	namespace := "ns-a"
	group := "group-a"
	managed := &CollaborationAdapterNodePresence{
		NodeID:      "managed-node",
		AdapterType: "redis",
		AdapterID:   "managed",
		Group:       group,
		Namespace:   namespace,
	}
	explicit := &CollaborationAdapterNodePresence{
		NodeID:      "explicit-node",
		AdapterType: "redis",
		AdapterID:   "explicit",
		Group:       group,
		Namespace:   namespace,
	}
	if err := features.RegisterAdapterNode(ctx, managed, 30*time.Second); err != nil {
		t.Fatalf("register managed node failed: %v", err)
	}
	if err := features.RegisterAdapterNode(ctx, explicit, 30*time.Second); err != nil {
		t.Fatalf("register explicit node failed: %v", err)
	}

	nodes, err := features.ListGroupNodes(ctx, namespace, group)
	if err != nil {
		t.Fatalf("list group nodes failed: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	if err := features.MarkAdapterOffline(ctx, namespace, group, "managed-node"); err != nil {
		t.Fatalf("mark offline failed: %v", err)
	}
	nodes, err = features.ListGroupNodes(ctx, namespace, group)
	if err != nil {
		t.Fatalf("list group nodes after offline failed: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after removing offline node from group, got %d", len(nodes))
	}
	if nodes[0].NodeID != "explicit-node" {
		t.Fatalf("expected explicit-node remaining in group, got %s", nodes[0].NodeID)
	}

	if err := features.HeartbeatAdapterNode(ctx, namespace, group, "managed-node", 30*time.Second); err != nil {
		t.Fatalf("heartbeat managed node failed: %v", err)
	}
	nodes, err = features.ListGroupNodes(ctx, namespace, group)
	if err != nil {
		t.Fatalf("list group nodes after heartbeat failed: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes after heartbeat rejoin, got %d", len(nodes))
	}
}

func TestRedisManagementFeaturesPublishAndSubscribeGroupEvents(t *testing.T) {
	features, cleanup := newTestRedisManagementFeatures(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	namespace := "ns-events"
	group := "group-events"
	pubSub := features.SubscribeGroupEvents(ctx, namespace, group)
	defer pubSub.Close()

	if _, err := pubSub.Receive(ctx); err != nil {
		t.Fatalf("subscribe handshake failed: %v", err)
	}

	event := CollaborationAdapterGroupEvent{
		Namespace: namespace,
		Group:     group,
		NodeID:    "node-1",
		EventType: "adapter.heartbeat",
		Status:    "online",
		Payload: map[string]interface{}{
			"source": "unit-test",
		},
	}
	if _, err := features.PublishGroupEvent(ctx, namespace, group, event); err != nil {
		t.Fatalf("publish group event failed: %v", err)
	}

	msg, err := pubSub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("receive group event failed: %v", err)
	}
	var received CollaborationAdapterGroupEvent
	if err := json.Unmarshal([]byte(msg.Payload), &received); err != nil {
		t.Fatalf("decode event payload failed: %v", err)
	}
	if received.EventType != event.EventType || received.NodeID != event.NodeID || received.Group != group {
		t.Fatalf("unexpected event payload: %+v", received)
	}
}

func newTestRedisManagementFeatures(t *testing.T) (*RedisManagementFeatures, func()) {
	t.Helper()
	server := miniredis.RunT(t)
	host, portText, err := net.SplitHostPort(server.Addr())
	if err != nil {
		t.Fatalf("split miniredis addr failed: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse miniredis port failed: %v", err)
	}
	config := &Config{Adapter: "redis", Redis: &RedisConnectionConfig{Host: host, Port: port}}
	adapter, err := NewRedisAdapter(config)
	if err != nil {
		t.Fatalf("new redis adapter failed: %v", err)
	}
	if err := adapter.Connect(context.Background(), config); err != nil {
		t.Fatalf("connect redis adapter failed: %v", err)
	}
	features, ok := GetRedisManagementFeatures(adapter)
	if !ok || features == nil {
		t.Fatal("expected redis management features")
	}
	return features, func() {
		_ = adapter.Close()
		server.Close()
	}
}
