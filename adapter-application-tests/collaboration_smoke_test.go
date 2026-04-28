package adapter_tests

// ---------------------------------------------------------------------------
// 协作层冒烟测试 (Smoke Tests)
//
// 目标：快速验证协作层关键路径"可启动、可通信、可追踪、可回放、可降级"，
//       作为发布门禁的最小可运行保障。
//
// 用例清单：
//   SmokeTest_RedisConnectivity              — Redis 连通性
//   SmokeTest_ArangoConnectivity             — Arango 连通性
//   SmokeTest_CollaborationRuntimeStartStop  — 运行时生命周期最小验证
//   SmokeTest_NodeRegisterAndList            — 节点注册 + 在线列表可见
//   SmokeTest_ManagementEventPublishReceive  — 管理事件发布/订阅
//   SmokeTest_StreamPublishConsume           — Stream 消息发布消费
//   SmokeTest_DLQReplayBasic                 — DLQ 基础回放（redis_only）
//   SmokeTest_ArangoLedgerWrite              — Arango 账本写入
//   SmokeTest_ArangoReplayPlannerSelectsCorrectMessages — Arango planner 精确过滤
//   SmokeTest_CheckpointResumeNoOp           — checkpoint 续跑（锚点后无消息）不报错
//   SmokeTest_FallbackReplayWhenArangoNil    — Arango nil 时 redis_only 降级不报错
// ---------------------------------------------------------------------------

import (
	"context"
	"fmt"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
)

// smokeCtx returns a short-lived context for smoke checks.
func smokeCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 15*time.Second)
}

func TestSmokeTest_RedisConnectivity(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()
	ctx, cancel := smokeCtx()
	defer cancel()
	if err := repo.Ping(ctx); err != nil {
		t.Fatalf("Redis Ping failed: %v", err)
	}
}

func TestSmokeTest_ArangoConnectivity(t *testing.T) {
	repo, cleanup := setupArangoCollabRepo(t)
	defer cleanup()
	ctx, cancel := smokeCtx()
	defer cancel()
	if err := repo.Ping(ctx); err != nil {
		t.Fatalf("Arango Ping failed: %v", err)
	}
}

func TestSmokeTest_CollaborationRuntimeStartStop(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()
	mgmt, ok := repo.GetRedisManagementFeatures()
	if !ok {
		t.Fatal("expected management features")
	}
	ctx, cancel := smokeCtx()
	defer cancel()

	now := time.Now().UnixNano()
	rt := db.NewCollaborationRuntime(mgmt, db.CollaborationAdapterNodePresence{
		NodeID: fmt.Sprintf("smoke-node-%d", now), AdapterType: "redis",
		AdapterID: "smoke", Group: fmt.Sprintf("sg-%d", now), Namespace: "smoke",
	}, db.CollaborationRuntimeConfig{HeartbeatInterval: 2 * time.Second, NodeTTL: 10 * time.Second, StopTimeout: 2 * time.Second})

	if _, err := rt.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if !rt.IsRunning() {
		t.Fatal("expected IsRunning=true")
	}
	if err := rt.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if rt.IsRunning() {
		t.Fatal("expected IsRunning=false after Stop")
	}
}

func TestSmokeTest_NodeRegisterAndList(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()
	mgmt, ok := repo.GetRedisManagementFeatures()
	if !ok {
		t.Fatal("expected management features")
	}
	ctx, cancel := smokeCtx()
	defer cancel()

	now := time.Now().UnixNano()
	ns := "smoke"
	grp := fmt.Sprintf("sg-list-%d", now)
	node := &db.CollaborationAdapterNodePresence{
		NodeID: fmt.Sprintf("smoke-reg-%d", now), AdapterType: "redis",
		AdapterID: "smoke", Group: grp, Namespace: ns,
	}
	if err := mgmt.RegisterAdapterNode(ctx, node, 30*time.Second); err != nil {
		t.Fatalf("RegisterAdapterNode: %v", err)
	}
	nodes, err := mgmt.ListGroupNodes(ctx, ns, grp)
	if err != nil {
		t.Fatalf("ListGroupNodes: %v", err)
	}
	found := false
	for _, n := range nodes {
		if n.NodeID == node.NodeID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("registered node %q not found in group %q", node.NodeID, grp)
	}
}

func TestSmokeTest_ManagementEventPublishReceive(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()
	mgmt, ok := repo.GetRedisManagementFeatures()
	if !ok {
		t.Fatal("expected management features")
	}
	ctx, cancel := smokeCtx()
	defer cancel()

	now := time.Now().UnixNano()
	ns := "smoke"
	grp := fmt.Sprintf("sg-evt-%d", now)

	pubSub := mgmt.SubscribeGroupEvents(ctx, ns, grp)
	defer pubSub.Close()
	if _, err := pubSub.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if _, err := mgmt.PublishGroupEvent(ctx, ns, grp, db.CollaborationAdapterGroupEvent{
		Namespace: ns, Group: grp, EventType: "smoke.ping",
		Status: "ok", TimestampMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("PublishGroupEvent: %v", err)
	}

	msgCh := make(chan struct{}, 1)
	go func() {
		if _, err := pubSub.ReceiveMessage(ctx); err == nil {
			msgCh <- struct{}{}
		}
	}()
	select {
	case <-msgCh:
	case <-time.After(5 * time.Second):
		t.Error("management event not received within 5s")
	}
}

func TestSmokeTest_StreamPublishConsume(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()
	sf, ok := repo.GetRedisStreamFeatures()
	if !ok {
		t.Fatal("expected stream features")
	}
	ctx, cancel := smokeCtx()
	defer cancel()

	now := time.Now().UnixNano()
	stream := fmt.Sprintf("eitdb:smoke:stream:%d", now)
	grp := "smoke-group"
	consumer := "smoke-consumer"

	if err := sf.EnsureConsumerGroup(ctx, stream, grp); err != nil {
		t.Fatalf("EnsureConsumerGroup: %v", err)
	}
	env := &db.CollaborationMessageEnvelope{
		MessageID: fmt.Sprintf("smoke-msg-%d", now), RequestID: "smoke-req",
		EventType: "smoke.test", RetryCount: 0, MaxRetry: 3,
	}
	if _, err := sf.PublishEnvelope(ctx, stream, env); err != nil {
		t.Fatalf("PublishEnvelope: %v", err)
	}
	msgs, err := sf.ReadGroupEnvelopes(ctx, stream, grp, consumer, 5, 0)
	if err != nil {
		t.Fatalf("ReadGroupEnvelopes: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Envelope.MessageID != env.MessageID {
		t.Errorf("MessageID mismatch: want=%q got=%q", env.MessageID, msgs[0].Envelope.MessageID)
	}
}

func TestSmokeTest_DLQReplayBasic(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()
	sf, ok := repo.GetRedisStreamFeatures()
	if !ok {
		t.Fatal("expected stream features")
	}
	ctx, cancel := smokeCtx()
	defer cancel()

	now := time.Now().UnixNano()
	target := fmt.Sprintf("eitdb:smoke:target:%d", now)
	dlq := fmt.Sprintf("eitdb:smoke:dlq:%d", now)
	grp := "smoke-dlq-group"
	consumer := "smoke-consumer"
	reqID := fmt.Sprintf("smoke-req-%d", now)

	for _, s := range []string{target, dlq} {
		if err := sf.EnsureConsumerGroup(ctx, s, grp); err != nil {
			t.Fatalf("EnsureConsumerGroup %s: %v", s, err)
		}
	}
	if _, err := sf.PublishEnvelope(ctx, dlq, &db.CollaborationMessageEnvelope{
		MessageID: fmt.Sprintf("smoke-dlq-msg-%d", now), RequestID: reqID,
		EventType: "smoke.dlq", RetryCount: 3, MaxRetry: 3,
	}); err != nil {
		t.Fatalf("PublishEnvelope DLQ: %v", err)
	}
	if _, err := sf.ReadGroupEnvelopes(ctx, dlq, grp, consumer, 5, 0); err != nil {
		t.Fatalf("ReadGroupEnvelopes DLQ: %v", err)
	}

	result, err := sf.ReplayFromDLQWithPlannerAndTracking(
		ctx, dlq, target, grp, consumer, reqID, 10,
		db.NewDefaultReplayPlanner(nil), nil, nil, "smoke", "sg",
	)
	if err != nil {
		t.Fatalf("ReplayFromDLQWithPlannerAndTracking: %v", err)
	}
	if result.Replayed != 1 {
		t.Errorf("expected 1 replayed, got %d", result.Replayed)
	}
	if result.PlannedBy != "redis_only" {
		t.Errorf("expected PlannedBy=redis_only, got %q", result.PlannedBy)
	}
}

func TestSmokeTest_ArangoLedgerWrite(t *testing.T) {
	repo, cleanup := setupArangoCollabRepo(t)
	defer cleanup()
	arango, ok := repo.GetAdapter().(*db.ArangoAdapter)
	if !ok {
		t.Fatalf("expected arango adapter, got %T", repo.GetAdapter())
	}
	ctx, cancel := smokeCtx()
	defer cancel()

	if err := arango.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("EnsureCollaborationLedgerCollections: %v", err)
	}
	now := time.Now().UnixNano()
	env := &db.CollaborationMessageEnvelope{
		MessageID: fmt.Sprintf("smoke-ledger-msg-%d", now),
		RequestID: fmt.Sprintf("smoke-ledger-req-%d", now),
		EventType: "smoke.ledger", RetryCount: 0, MaxRetry: 3,
	}
	if err := arango.RecordCollaborationEnvelopeToLedger(ctx, env); err != nil {
		t.Fatalf("RecordCollaborationEnvelopeToLedger: %v", err)
	}
}

func TestSmokeTest_ArangoReplayPlannerSelectsCorrectMessages(t *testing.T) {
	arangoRepo, arangoCleanup := setupArangoCollabRepo(t)
	defer arangoCleanup()
	redisRepo, redisCleanup := setupRedisCollabRepo(t)
	defer redisCleanup()

	arango, ok := arangoRepo.GetAdapter().(*db.ArangoAdapter)
	if !ok {
		t.Fatalf("expected arango adapter")
	}
	sf, ok := redisRepo.GetRedisStreamFeatures()
	if !ok {
		t.Fatal("expected stream features")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := arango.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("EnsureCollaborationLedgerCollections: %v", err)
	}

	now := time.Now().UnixNano()
	target := fmt.Sprintf("eitdb:smoke:planner:target:%d", now)
	dlq := fmt.Sprintf("eitdb:smoke:planner:dlq:%d", now)
	grp := "smoke-planner-group"
	consumer := "smoke-planner-consumer"
	reqID := fmt.Sprintf("smoke-planner-req-%d", now)
	noiseReqID := fmt.Sprintf("smoke-planner-noise-%d", now)

	// Write 1 target message to Arango ledger; noise is NOT recorded in ledger.
	targetMsgID := fmt.Sprintf("smoke-planner-target-%d", now)
	if err := arango.RecordCollaborationEnvelopeToLedger(ctx, &db.CollaborationMessageEnvelope{
		MessageID: targetMsgID, RequestID: reqID, EventType: "smoke.planner", RetryCount: 3, MaxRetry: 3,
	}); err != nil {
		t.Fatalf("RecordCollaborationEnvelopeToLedger: %v", err)
	}

	for _, s := range []string{target, dlq} {
		if err := sf.EnsureConsumerGroup(ctx, s, grp); err != nil {
			t.Fatalf("EnsureConsumerGroup %s: %v", s, err)
		}
	}

	// Push 1 target + 1 noise to DLQ.
	for _, env := range []*db.CollaborationMessageEnvelope{
		{MessageID: targetMsgID, RequestID: reqID, EventType: "smoke.planner", RetryCount: 3, MaxRetry: 3},
		{MessageID: fmt.Sprintf("smoke-planner-noise-%d", now), RequestID: noiseReqID, EventType: "smoke.noise", RetryCount: 1, MaxRetry: 3},
	} {
		if _, err := sf.PublishEnvelope(ctx, dlq, env); err != nil {
			t.Fatalf("PublishEnvelope DLQ: %v", err)
		}
	}
	if _, err := sf.ReadGroupEnvelopes(ctx, dlq, grp, consumer, 10, 0); err != nil {
		t.Fatalf("ReadGroupEnvelopes DLQ: %v", err)
	}

	result, err := sf.ReplayFromDLQWithPlanner(ctx, dlq, target, grp, consumer, reqID, 10, db.NewDefaultReplayPlanner(arango))
	if err != nil {
		t.Fatalf("ReplayFromDLQWithPlanner: %v", err)
	}
	if result.PlannedBy != "arango" {
		t.Errorf("expected PlannedBy=arango, got %q", result.PlannedBy)
	}
	if result.Replayed != 1 {
		t.Errorf("expected 1 replayed, got %d (skipped=%d)", result.Replayed, result.Skipped)
	}
}

func TestSmokeTest_CheckpointResumeNoOp(t *testing.T) {
	arangoRepo, arangoCleanup := setupArangoCollabRepo(t)
	defer arangoCleanup()
	redisRepo, redisCleanup := setupRedisCollabRepo(t)
	defer redisCleanup()

	arango, ok := arangoRepo.GetAdapter().(*db.ArangoAdapter)
	if !ok {
		t.Fatalf("expected arango adapter")
	}
	sf, ok := redisRepo.GetRedisStreamFeatures()
	if !ok {
		t.Fatal("expected stream features")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := arango.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("EnsureCollaborationLedgerCollections: %v", err)
	}

	now := time.Now().UnixNano()
	nowMs := time.Now().UnixMilli()
	target := fmt.Sprintf("eitdb:smoke:cp-noop:target:%d", now)
	dlq := fmt.Sprintf("eitdb:smoke:cp-noop:dlq:%d", now)
	grp := "smoke-cp-noop-group"
	consumer := "smoke-cp-consumer"
	reqID := fmt.Sprintf("smoke-cp-req-%d", now)
	sessionID := fmt.Sprintf("smoke-cp-sess-%d", now)
	msgID := fmt.Sprintf("smoke-cp-msg-%d", now)

	// Write 1 session + 1 message + checkpoint.
	if err := arango.UpsertLedgerReplaySessionNode(ctx, &db.ArangoReplaySessionNode{
		SessionID: sessionID, RequestID: reqID, Mode: "arango_preferred",
		PlannedBy: "arango", Status: "replayed", StartedAtUnixMs: nowMs, EndedAtUnixMs: nowMs,
	}); err != nil {
		t.Fatalf("UpsertLedgerReplaySessionNode: %v", err)
	}
	if err := arango.LinkLedgerSessionReplaysMessage(ctx, sessionID, msgID, map[string]interface{}{
		"seq": 1, "replayed_at": nowMs,
	}); err != nil {
		t.Fatalf("LinkLedgerSessionReplaysMessage: %v", err)
	}
	checkpointID := fmt.Sprintf("smoke-cp-%d", now)
	if err := arango.UpsertLedgerReplayCheckpointNode(ctx, &db.ArangoReplayCheckpointNode{
		CheckpointID: checkpointID, SessionID: sessionID,
		AnchorType: "message_id", AnchorValue: msgID,
		Tick: "1", Cursor: msgID, Status: "set", CreatedAtUnixMs: nowMs,
	}); err != nil {
		t.Fatalf("UpsertLedgerReplayCheckpointNode: %v", err)
	}

	for _, s := range []string{target, dlq} {
		if err := sf.EnsureConsumerGroup(ctx, s, grp); err != nil {
			t.Fatalf("EnsureConsumerGroup %s: %v", s, err)
		}
	}
	// DLQ is empty — nothing to replay from after the anchor.
	result, err := db.ResumeReplayFromCheckpoint(ctx, sf, arango, nil, &db.ResumeReplayFromCheckpointParams{
		CheckpointID: checkpointID, IncludeAnchor: false,
		RequestID: reqID, DLQStream: dlq, TargetStream: target,
		DLQGroup: grp, Consumer: consumer, Limit: 10,
		Namespace: "smoke", Group: "sg-cp",
	})
	if err != nil {
		t.Fatalf("ResumeReplayFromCheckpoint (no-op): %v", err)
	}
	// Empty DLQ → 0 replayed, no error.
	if result.Replayed != 0 {
		t.Errorf("expected 0 replayed on empty DLQ, got %d", result.Replayed)
	}
}

func TestSmokeTest_FallbackReplayWhenArangoNil(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()
	sf, ok := repo.GetRedisStreamFeatures()
	if !ok {
		t.Fatal("expected stream features")
	}
	ctx, cancel := smokeCtx()
	defer cancel()

	now := time.Now().UnixNano()
	target := fmt.Sprintf("eitdb:smoke:fallback:target:%d", now)
	dlq := fmt.Sprintf("eitdb:smoke:fallback:dlq:%d", now)
	grp := "smoke-fallback-group"
	consumer := "smoke-fallback-consumer"
	reqID := fmt.Sprintf("smoke-fallback-req-%d", now)

	for _, s := range []string{target, dlq} {
		if err := sf.EnsureConsumerGroup(ctx, s, grp); err != nil {
			t.Fatalf("EnsureConsumerGroup %s: %v", s, err)
		}
	}
	if _, err := sf.PublishEnvelope(ctx, dlq, &db.CollaborationMessageEnvelope{
		MessageID: fmt.Sprintf("smoke-fallback-msg-%d", now), RequestID: reqID,
		EventType: "smoke.fallback", RetryCount: 3, MaxRetry: 3,
	}); err != nil {
		t.Fatalf("PublishEnvelope: %v", err)
	}
	if _, err := sf.ReadGroupEnvelopes(ctx, dlq, grp, consumer, 5, 0); err != nil {
		t.Fatalf("ReadGroupEnvelopes: %v", err)
	}

	// arango=nil must not cause error; must use redis_only.
	result, err := sf.ReplayFromDLQWithPlannerAndTracking(
		ctx, dlq, target, grp, consumer, reqID, 10,
		db.NewDefaultReplayPlanner(nil), nil, nil, "smoke", "sg",
	)
	if err != nil {
		t.Fatalf("fallback replay error: %v", err)
	}
	if result.PlannedBy != "redis_only" {
		t.Errorf("expected PlannedBy=redis_only, got %q", result.PlannedBy)
	}
	if result.Replayed != 1 {
		t.Errorf("expected 1 replayed, got %d", result.Replayed)
	}
}
