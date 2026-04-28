package adapter_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
)

// ---------------------------------------------------------------------------
// A1/A2: 协作运行时生命周期集成测试
// 验收：无需业务侧手写注册/心跳代码即可获得在线节点视图；
//       Stop 后离线状态可见。
// ---------------------------------------------------------------------------

func TestCollaborationRuntimeLifecycle_StartStop(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()

	mgmt, ok := repo.GetRedisManagementFeatures()
	if !ok || mgmt == nil {
		t.Fatalf("expected redis management features")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	now := time.Now().UnixNano()
	namespace := "lifecycle_it"
	group := fmt.Sprintf("grp-%d", now)
	nodeID := fmt.Sprintf("rt-node-%d", now)

	rt := db.NewCollaborationRuntime(mgmt, db.CollaborationAdapterNodePresence{
		NodeID:      nodeID,
		AdapterType: "redis",
		AdapterID:   "redis-default",
		Group:       group,
		Namespace:   namespace,
		Capabilities: []string{"stream", "pubsub"},
	}, db.CollaborationRuntimeConfig{
		HeartbeatInterval: 2 * time.Second,
		NodeTTL:           10 * time.Second,
		StopTimeout:       3 * time.Second,
	})

	// Start: should register and begin heartbeat.
	report, err := rt.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if report == nil {
		t.Fatal("expected non-nil report")
	}
	if report.NodeID != nodeID {
		t.Errorf("report.NodeID: want=%q got=%q", nodeID, report.NodeID)
	}
	if report.Group != group {
		t.Errorf("report.Group: want=%q got=%q", group, report.Group)
	}
	if !rt.IsRunning() {
		t.Fatal("expected IsRunning=true after Start")
	}

	// Node must be visible in online list.
	nodes, err := mgmt.ListGroupNodes(ctx, namespace, group)
	if err != nil {
		t.Fatalf("ListGroupNodes after Start failed: %v", err)
	}
	foundOnline := false
	for _, n := range nodes {
		if n.NodeID == nodeID && (n.Status == "online" || n.Status == "") {
			foundOnline = true
			break
		}
	}
	if !foundOnline {
		t.Errorf("expected node %q to be online after Start, got nodes: %+v", nodeID, nodes)
	}

	// Idempotent Start must return same report.
	report2, err := rt.Start(ctx)
	if err != nil {
		t.Fatalf("second Start failed: %v", err)
	}
	if report2.StartedAt != report.StartedAt {
		t.Errorf("idempotent Start must return same report: %q != %q", report.StartedAt, report2.StartedAt)
	}

	// Stop: must mark node offline.
	if err := rt.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if rt.IsRunning() {
		t.Fatal("expected IsRunning=false after Stop")
	}

	// Verify offline status.
	nodesAfter, err := mgmt.ListGroupNodes(ctx, namespace, group)
	if err != nil {
		t.Fatalf("ListGroupNodes after Stop failed: %v", err)
	}
	for _, n := range nodesAfter {
		if n.NodeID == nodeID && n.Status == "online" {
			t.Errorf("node %q should be offline after Stop, but status=%q", nodeID, n.Status)
		}
	}

	// Idempotent Stop must not return error.
	if err := rt.Stop(ctx); err != nil {
		t.Errorf("second Stop returned error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// C1: HRW 稳定选路集成测试（Redis 在线节点列表 + HRW 路由）
// 验收：同 request_key + route_tick 下路由稳定，节点下线后正确降级。
// ---------------------------------------------------------------------------

func TestCollaborationHRWStableRouting_Integration(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()

	mgmt, ok := repo.GetRedisManagementFeatures()
	if !ok || mgmt == nil {
		t.Fatalf("expected redis management features")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	now := time.Now().UnixNano()
	namespace := "hrw_routing_it"
	group := fmt.Sprintf("grp-hrw-%d", now)

	// Register 3 nodes.
	nodeIDs := []string{
		fmt.Sprintf("hrw-node-1-%d", now),
		fmt.Sprintf("hrw-node-2-%d", now),
		fmt.Sprintf("hrw-node-3-%d", now),
	}
	for _, id := range nodeIDs {
		if err := mgmt.RegisterAdapterNode(ctx, &db.CollaborationAdapterNodePresence{
			NodeID:      id,
			AdapterType: "postgres",
			AdapterID:   "pg-instance",
			Group:       group,
			Namespace:   namespace,
		}, 30*time.Second); err != nil {
			t.Fatalf("register node %q failed: %v", id, err)
		}
	}

	nodes, err := mgmt.ListGroupNodes(ctx, namespace, group)
	if err != nil {
		t.Fatalf("ListGroupNodes failed: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	requestKey := "req-hrw-stable"
	blueprintTick := "bp-v1"
	routeTick := "rt-snap-1"

	// Same route_tick must produce stable result across multiple calls.
	first, _, err := db.HRWRouteToOnlineNodeWithFallback(requestKey, blueprintTick, routeTick, nodes)
	if err != nil {
		t.Fatalf("initial route failed: %v", err)
	}
	for i := 0; i < 20; i++ {
		got, _, err := db.HRWRouteToOnlineNodeWithFallback(requestKey, blueprintTick, routeTick, nodes)
		if err != nil {
			t.Fatalf("route call %d failed: %v", i, err)
		}
		if got != first {
			t.Fatalf("unstable route at call %d: want=%q got=%q", i, first, got)
		}
	}

	// Simulate primary node going offline.
	primaryNodeID := first
	if err := mgmt.MarkAdapterOffline(ctx, namespace, group, primaryNodeID); err != nil {
		t.Fatalf("MarkAdapterOffline failed: %v", err)
	}

	// Refresh node list after offline event.
	nodesAfter, err := mgmt.ListGroupNodes(ctx, namespace, group)
	if err != nil {
		t.Fatalf("ListGroupNodes after offline failed: %v", err)
	}

	// Fallback must route to an online node.
	fallback, _, err := db.HRWRouteToOnlineNodeWithFallback(requestKey, blueprintTick, routeTick, nodesAfter)
	if err != nil {
		t.Fatalf("fallback route failed: %v", err)
	}
	if fallback == primaryNodeID {
		t.Errorf("fallback must not route to offline primary %q", primaryNodeID)
	}
}

// ---------------------------------------------------------------------------
// B1: DLQ 回放集成测试
// 验收：指定 request_id 的 DLQ 消息可回放并被消费确认。
// ---------------------------------------------------------------------------

func TestCollaborationDLQReplay_Integration(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()

	streamFeatures, ok := repo.GetRedisStreamFeatures()
	if !ok || streamFeatures == nil {
		t.Fatalf("expected redis stream features")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	now := time.Now().UnixNano()
	mainStream := fmt.Sprintf("eitdb:it:dlq:main:%d", now)
	dlqStream := fmt.Sprintf("eitdb:it:dlq:dead:%d", now)
	mainGroup := "main-consumers"
	dlqGroup := "dlq-consumers"
	consumer := "test-consumer"

	// Ensure consumer groups on both streams.
	for _, s := range []string{mainStream, dlqStream} {
		grp := mainGroup
		if s == dlqStream {
			grp = dlqGroup
		}
		if err := streamFeatures.EnsureConsumerGroup(ctx, s, grp); err != nil {
			t.Fatalf("EnsureConsumerGroup %s/%s failed: %v", s, grp, err)
		}
	}

	targetRequestID := fmt.Sprintf("req-dlq-target-%d", now)
	otherRequestID := fmt.Sprintf("req-dlq-other-%d", now)

	// Publish 3 messages directly to DLQ stream (2 with target request_id, 1 with other).
	// This simulates messages that have already been dead-lettered by the retry mechanism.
	for i, reqID := range []string{targetRequestID, otherRequestID, targetRequestID} {
		_, err := streamFeatures.PublishEnvelope(ctx, dlqStream, &db.CollaborationMessageEnvelope{
			MessageID:  fmt.Sprintf("dlq-msg-%d-%d", now, i),
			RequestID:  reqID,
			EventType:  "test.dlq",
			RetryCount: 3,
			MaxRetry:   3,
		})
		if err != nil {
			t.Fatalf("publish DLQ msg %d failed: %v", i, err)
		}
	}

	// Consume from DLQ to establish pending state in the DLQ consumer group.
	dlqMsgs, err := streamFeatures.ReadGroupEnvelopes(ctx, dlqStream, dlqGroup, consumer, 10, 0)
	if err != nil {
		t.Fatalf("ReadGroupEnvelopes from DLQ failed: %v", err)
	}
	if len(dlqMsgs) != 3 {
		t.Fatalf("expected 3 messages in DLQ, got %d", len(dlqMsgs))
	}

	// Ensure main stream group exists before replay writes into it.
	if err := streamFeatures.EnsureConsumerGroup(ctx, mainStream, mainGroup); err != nil {
		t.Fatalf("EnsureConsumerGroup main stream failed: %v", err)
	}

	// Replay only messages with targetRequestID.
	replayResult, err := streamFeatures.ReplayFromDLQ(ctx, dlqStream, mainStream, dlqGroup, consumer, targetRequestID, 50)
	if err != nil {
		t.Fatalf("ReplayFromDLQ failed: %v", err)
	}
	if replayResult.Replayed != 2 {
		t.Errorf("expected 2 replayed (matching targetRequestID), got %d", replayResult.Replayed)
	}
	if replayResult.Skipped != 1 {
		t.Errorf("expected 1 skipped (non-matching requestID), got %d", replayResult.Skipped)
	}

	// Verify replayed messages are back on main stream with RetryCount=0.
	replayed, err := streamFeatures.ReadGroupEnvelopes(ctx, mainStream, mainGroup, consumer, 10, 0)
	if err != nil {
		t.Fatalf("ReadGroupEnvelopes after replay failed: %v", err)
	}
	for _, m := range replayed {
		if m.Envelope == nil {
			continue
		}
		if m.Envelope.RetryCount != 0 {
			t.Errorf("replayed message %q: expected RetryCount=0, got %d", m.Envelope.MessageID, m.Envelope.RetryCount)
		}
		if m.Envelope.RequestID != targetRequestID {
			t.Errorf("replayed message request_id: want=%q got=%q", targetRequestID, m.Envelope.RequestID)
		}
	}
}
