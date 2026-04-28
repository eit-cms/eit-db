package db

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// TestReplayStatus_Values 验证 ReplayStatus 常量值正确。
func TestReplayStatus_Values(t *testing.T) {
	cases := []struct {
		status ReplayStatus
		want   string
	}{
		{ReplayStatusCandidate, "candidate"},
		{ReplayStatusPlanned, "planned"},
		{ReplayStatusReplayed, "replayed"},
		{ReplayStatusFailed, "failed"},
		{ReplayStatusSkipped, "skipped"},
		{ReplayStatusAudited, "audited"},
	}
	for _, c := range cases {
		if string(c.status) != c.want {
			t.Errorf("ReplayStatus %q: got %q, want %q", c.status, string(c.status), c.want)
		}
	}
}

// TestRedisOnlyReplayPlanner_PlanReplay 验证 redis_only 规划器行为。
func TestRedisOnlyReplayPlanner_PlanReplay(t *testing.T) {
	p := &RedisOnlyReplayPlanner{}
	plan, err := p.PlanReplay(context.Background(), "req-001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan == nil {
		t.Fatal("plan must not be nil")
	}
	if plan.RequestID != "req-001" {
		t.Errorf("RequestID: got %q, want %q", plan.RequestID, "req-001")
	}
	if plan.PlannedBy != "redis_only" {
		t.Errorf("PlannedBy: got %q, want %q", plan.PlannedBy, "redis_only")
	}
	if len(plan.MessageIDsToReplay) != 0 {
		t.Errorf("MessageIDsToReplay should be empty for redis_only, got %v", plan.MessageIDsToReplay)
	}
	if plan.PlannedAt.IsZero() {
		t.Error("PlannedAt must not be zero")
	}
}

// TestRedisOnlyReplayPlanner_EmptyRequestID 验证空 requestID 时 redis_only 规划器也正常返回。
func TestRedisOnlyReplayPlanner_EmptyRequestID(t *testing.T) {
	p := &RedisOnlyReplayPlanner{}
	plan, err := p.PlanReplay(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.PlannedBy != "redis_only" {
		t.Errorf("PlannedBy: got %q, want %q", plan.PlannedBy, "redis_only")
	}
}

// TestArangoReplayPlanner_NilAdapter 验证 nil adapter 时返回明确错误。
func TestArangoReplayPlanner_NilAdapter(t *testing.T) {
	p := &ArangoReplayPlanner{Adapter: nil}
	_, err := p.PlanReplay(context.Background(), "req-001")
	if err == nil {
		t.Fatal("expected error for nil adapter, got nil")
	}
}

// TestNewDefaultReplayPlanner_WithArango 验证传入非 nil Arango adapter 时返回 ArangoReplayPlanner。
func TestNewDefaultReplayPlanner_WithArango(t *testing.T) {
	planner := NewDefaultReplayPlanner(&ArangoAdapter{})
	if _, ok := planner.(*ArangoReplayPlanner); !ok {
		t.Errorf("expected *ArangoReplayPlanner, got %T", planner)
	}
}

// TestNewDefaultReplayPlanner_NilArango 验证传入 nil 时返回 RedisOnlyReplayPlanner。
func TestNewDefaultReplayPlanner_NilArango(t *testing.T) {
	planner := NewDefaultReplayPlanner(nil)
	if _, ok := planner.(*RedisOnlyReplayPlanner); !ok {
		t.Errorf("expected *RedisOnlyReplayPlanner, got %T", planner)
	}
}

// TestDLQReplayPlan_Fields 验证 DLQReplayPlan 字段赋值正确。
func TestDLQReplayPlan_Fields(t *testing.T) {
	now := time.Now()
	plan := &DLQReplayPlan{
		RequestID:          "req-abc",
		MessageIDsToReplay: []string{"msg-1", "msg-2"},
		PlannedBy:          "arango",
		PlannedAt:          now,
	}
	if plan.RequestID != "req-abc" {
		t.Errorf("RequestID: got %q", plan.RequestID)
	}
	if len(plan.MessageIDsToReplay) != 2 {
		t.Errorf("MessageIDsToReplay len: got %d, want 2", len(plan.MessageIDsToReplay))
	}
	if plan.PlannedBy != "arango" {
		t.Errorf("PlannedBy: got %q", plan.PlannedBy)
	}
}

// TestBuildMessageIDSet_Normal 验证 buildMessageIDSet 构建正确的集合。
func TestBuildMessageIDSet_Normal(t *testing.T) {
	ids := []string{"msg-1", "msg-2", "msg-3"}
	set := buildMessageIDSet(ids)
	if set == nil {
		t.Fatal("set must not be nil")
	}
	for _, id := range ids {
		if !set[id] {
			t.Errorf("expected %q to be in set", id)
		}
	}
	if set["msg-999"] {
		t.Error("msg-999 should not be in set")
	}
}

// TestBuildMessageIDSet_Empty 验证空切片返回 nil。
func TestBuildMessageIDSet_Empty(t *testing.T) {
	set := buildMessageIDSet(nil)
	if set != nil {
		t.Error("empty input should return nil set")
	}
	set2 := buildMessageIDSet([]string{})
	if set2 != nil {
		t.Error("empty slice should return nil set")
	}
}

// TestRedisStreamDLQReplayResult_PlannedByField 验证 PlannedBy 字段存在且可赋值。
func TestRedisStreamDLQReplayResult_PlannedByField(t *testing.T) {
	r := &RedisStreamDLQReplayResult{
		Read:                       3,
		Replayed:                   2,
		Skipped:                    1,
		ReplayedMessageIDs:         []string{"new-1", "new-2"},
		ReplayedOriginalMessageIDs: []string{"orig-1", "orig-2"},
		PlannedBy:                  "arango",
	}
	if r.PlannedBy != "arango" {
		t.Errorf("PlannedBy: got %q, want %q", r.PlannedBy, "arango")
	}
	if len(r.ReplayedOriginalMessageIDs) != 2 {
		t.Errorf("ReplayedOriginalMessageIDs len: got %d, want 2", len(r.ReplayedOriginalMessageIDs))
	}
}

// TestRecordReplayResultToLedger_NilArango 验证 arango 为 nil 时安全跳过。
func TestRecordReplayResultToLedger_NilArango(t *testing.T) {
	result := &RedisStreamDLQReplayResult{
		ReplayedOriginalMessageIDs: []string{"msg-1"},
	}
	err := RecordReplayResultToLedger(context.Background(), nil, result, "target-stream")
	if err != nil {
		t.Errorf("expected nil error for nil arango, got: %v", err)
	}
}

// TestRecordReplayResultToLedger_NilResult 验证 result 为 nil 时安全跳过。
func TestRecordReplayResultToLedger_NilResult(t *testing.T) {
	err := RecordReplayResultToLedger(context.Background(), &ArangoAdapter{}, nil, "target-stream")
	if err != nil {
		t.Errorf("expected nil error for nil result, got: %v", err)
	}
}

// TestRecordReplayResultToLedger_EmptyIDs 验证 ReplayedOriginalMessageIDs 为空时安全跳过。
func TestRecordReplayResultToLedger_EmptyIDs(t *testing.T) {
	result := &RedisStreamDLQReplayResult{
		ReplayedOriginalMessageIDs: nil,
	}
	err := RecordReplayResultToLedger(context.Background(), &ArangoAdapter{}, result, "target-stream")
	if err != nil {
		t.Errorf("expected nil error for empty IDs, got: %v", err)
	}
}

// TestPublishReplayResultToManagementChannel_PublishesEvent 验证回放结果会发布到管理事件通道。
func TestPublishReplayResultToManagementChannel_PublishesEvent(t *testing.T) {
	features, cleanup := newTestRedisManagementFeatures(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	namespace := "ns-replay"
	group := "group-replay"
	requestID := "req-123"
	dlqStream := "stream-dlq"
	targetStream := "stream-main"

	pubSub := features.SubscribeGroupEvents(ctx, namespace, group)
	defer pubSub.Close()
	if _, err := pubSub.Receive(ctx); err != nil {
		t.Fatalf("subscribe handshake failed: %v", err)
	}

	result := &RedisStreamDLQReplayResult{
		Read:                       3,
		Replayed:                   2,
		Skipped:                    1,
		PlannedBy:                  "arango",
		ReplayedMessageIDs:         []string{"new-1", "new-2"},
		ReplayedOriginalMessageIDs: []string{"orig-1", "orig-2"},
	}

	if err := PublishReplayResultToManagementChannel(ctx, features, namespace, group, requestID, dlqStream, targetStream, result); err != nil {
		t.Fatalf("PublishReplayResultToManagementChannel failed: %v", err)
	}

	msg, err := pubSub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("receive message failed: %v", err)
	}

	var event CollaborationAdapterGroupEvent
	if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
		t.Fatalf("unmarshal event failed: %v", err)
	}
	if event.EventType != "dlq.replay.published" {
		t.Fatalf("unexpected event type: %s", event.EventType)
	}
	if event.Status != string(ReplayStatusReplayed) {
		t.Fatalf("unexpected event status: %s", event.Status)
	}
	if got, _ := event.Payload["request_id"].(string); got != requestID {
		t.Fatalf("payload request_id mismatch: got=%q want=%q", got, requestID)
	}
	if got, _ := event.Payload["planned_by"].(string); got != "arango" {
		t.Fatalf("payload planned_by mismatch: got=%q want=%q", got, "arango")
	}
}
