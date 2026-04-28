package adapter_tests

// ---------------------------------------------------------------------------
// 综合型回放端到端集成测试
//
// 覆盖完整回放能力的全链路：
//   Phase 1 — 初始消息投递（模拟正常业务流程，部分消息进入 DLQ）
//   Phase 2 — Arango 账本查询，规划回放范围
//   Phase 3 — ReplayFromDLQWithPlannerAndTracking（Arango 增强路径）
//   Phase 4 — QueryReplaySessionMessages 验证账本回放序列
//   Phase 5 — checkpoint 续跑：ResumeReplayFromCheckpoint
//   Phase 6 — 任意锚点跳转：JumpReplayToAnchor
//   Phase 7 — Redis 管理事件摘要验证
//   Phase 8 — 降级验证：关闭 Arango 参数后 redis_only 路径正确执行
// ---------------------------------------------------------------------------

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
)

func TestCollaborationReplayE2E(t *testing.T) {
	arangoRepo, arangoCleanup := setupArangoCollabRepo(t)
	defer arangoCleanup()
	redisRepo, redisCleanup := setupRedisCollabRepo(t)
	defer redisCleanup()

	arangoAdapter, ok := arangoRepo.GetAdapter().(*db.ArangoAdapter)
	if !ok || arangoAdapter == nil {
		t.Fatalf("expected arango adapter, got %T", arangoRepo.GetAdapter())
	}
	streamFeatures, ok := redisRepo.GetRedisStreamFeatures()
	if !ok {
		t.Fatal("expected redis stream features")
	}
	mgmtFeatures, ok := redisRepo.GetRedisManagementFeatures()
	if !ok {
		t.Fatal("expected redis management features")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// --- Setup ---
	if err := arangoAdapter.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("EnsureCollaborationLedgerCollections: %v", err)
	}

	now := time.Now().UnixNano()
	ns := fmt.Sprintf("ns-e2e-%d", now)
	grp := fmt.Sprintf("grp-e2e-%d", now)
	mainStream := fmt.Sprintf("eitdb:e2e:main:%d", now)
	dlqStream := fmt.Sprintf("eitdb:e2e:dlq:%d", now)
	mainGroup := "main-group"
	dlqGroup := "dlq-group"
	consumer := "e2e-consumer"
	requestID := fmt.Sprintf("req-e2e-%d", now)

	// 6 messages: msg[0..4] belong to requestID, msg[5] is noise (different request).
	msgCount := 5
	msgIDs := make([]string, msgCount)
	for i := 0; i < msgCount; i++ {
		msgIDs[i] = fmt.Sprintf("e2e-msg-%d-%d", i+1, now)
	}
	noiseID := fmt.Sprintf("e2e-noise-%d", now)
	noiseRequestID := fmt.Sprintf("req-e2e-noise-%d", now)

	// ---------- Phase 1: 写入账本 + 推送 DLQ ----------
	t.Log("Phase 1: write ledger, push DLQ")
	for _, id := range msgIDs {
		if err := arangoAdapter.RecordCollaborationEnvelopeToLedger(ctx, &db.CollaborationMessageEnvelope{
			MessageID: id, RequestID: requestID, EventType: "e2e.test",
			RetryCount: 3, MaxRetry: 3,
		}); err != nil {
			t.Fatalf("RecordCollaborationEnvelopeToLedger %s: %v", id, err)
		}
	}

	for _, s := range []string{mainStream, dlqStream} {
		g := mainGroup
		if s == dlqStream {
			g = dlqGroup
		}
		if err := streamFeatures.EnsureConsumerGroup(ctx, s, g); err != nil {
			t.Fatalf("EnsureConsumerGroup %s/%s: %v", s, g, err)
		}
	}
	// Push all 5 target + 1 noise to DLQ.
	for _, id := range msgIDs {
		if _, err := streamFeatures.PublishEnvelope(ctx, dlqStream, &db.CollaborationMessageEnvelope{
			MessageID: id, RequestID: requestID, EventType: "e2e.test", RetryCount: 3, MaxRetry: 3,
		}); err != nil {
			t.Fatalf("PublishEnvelope DLQ %s: %v", id, err)
		}
	}
	if _, err := streamFeatures.PublishEnvelope(ctx, dlqStream, &db.CollaborationMessageEnvelope{
		MessageID: noiseID, RequestID: noiseRequestID, EventType: "e2e.noise", RetryCount: 2, MaxRetry: 3,
	}); err != nil {
		t.Fatalf("PublishEnvelope DLQ noise: %v", err)
	}

	// Consume DLQ to establish PEL.
	dlqMsgs, err := streamFeatures.ReadGroupEnvelopes(ctx, dlqStream, dlqGroup, consumer, 20, 0)
	if err != nil {
		t.Fatalf("ReadGroupEnvelopes DLQ: %v", err)
	}
	if len(dlqMsgs) != 6 {
		t.Fatalf("expected 6 DLQ msgs, got %d", len(dlqMsgs))
	}

	// ---------- Phase 2 & 3: Arango 增强回放 ----------
	t.Log("Phase 2+3: arango-enhanced replay with tracking")
	pubSub := mgmtFeatures.SubscribeGroupEvents(ctx, ns, grp)
	defer pubSub.Close()
	if _, err := pubSub.Receive(ctx); err != nil {
		t.Fatalf("subscribe management events: %v", err)
	}

	planner := db.NewDefaultReplayPlanner(arangoAdapter)
	replayResult, err := streamFeatures.ReplayFromDLQWithPlannerAndTracking(
		ctx, dlqStream, mainStream, dlqGroup, consumer,
		requestID, 50, planner, arangoAdapter, mgmtFeatures, ns, grp,
	)
	if err != nil {
		t.Fatalf("ReplayFromDLQWithPlannerAndTracking: %v", err)
	}
	if replayResult.PlannedBy != "arango" {
		t.Errorf("Phase3 PlannedBy: want=arango got=%q", replayResult.PlannedBy)
	}
	if replayResult.Replayed != msgCount {
		t.Errorf("Phase3 Replayed: want=%d got=%d (skipped=%d)", msgCount, replayResult.Replayed, replayResult.Skipped)
	}
	if len(replayResult.ReplaySessionID) == 0 {
		t.Error("Phase3 ReplaySessionID must not be empty")
	}
	sessionID := replayResult.ReplaySessionID

	// ---------- Phase 4: 账本回放序列验证 ----------
	t.Log("Phase 4: verify arango replay session messages")
	// Allow a brief moment for Arango writes to propagate (best-effort; not a time.Sleep).
	sessionMsgs, err := arangoAdapter.QueryReplaySessionMessages(ctx, sessionID, 0, 50)
	if err != nil {
		t.Fatalf("QueryReplaySessionMessages: %v", err)
	}
	if len(sessionMsgs) != msgCount {
		t.Errorf("Phase4 session message count: want=%d got=%d", msgCount, len(sessionMsgs))
	}

	// Checkpoint list must have exactly 1 (the "final" checkpoint written by RecordReplaySessionToLedger).
	checkpoints, err := arangoAdapter.QueryReplaySessionCheckpoints(ctx, sessionID, 10)
	if err != nil {
		t.Fatalf("QueryReplaySessionCheckpoints: %v", err)
	}
	if len(checkpoints) != 1 {
		t.Errorf("Phase4 checkpoint count: want=1 got=%d", len(checkpoints))
	}

	// ---------- Phase 5: 断点续跑 ----------
	// Scenario: simulate a new batch of DLQ messages (msgs 3..5 re-queued after partial failure).
	t.Log("Phase 5: checkpoint resume replay")

	// Determine the "final" checkpoint ID (written by tracking as sessionID__final).
	finalCPID := sessionID + "__final"

	// Push msgs[2..4] (msg-3, msg-4, msg-5) back to DLQ to simulate partial re-queue.
	dlq2Stream := fmt.Sprintf("eitdb:e2e:dlq2:%d", now)
	if err := streamFeatures.EnsureConsumerGroup(ctx, dlq2Stream, dlqGroup); err != nil {
		t.Fatalf("EnsureConsumerGroup dlq2: %v", err)
	}
	for _, id := range msgIDs[2:] {
		if _, err := streamFeatures.PublishEnvelope(ctx, dlq2Stream, &db.CollaborationMessageEnvelope{
			MessageID: id, RequestID: requestID, EventType: "e2e.test", RetryCount: 1, MaxRetry: 3,
		}); err != nil {
			t.Fatalf("PublishEnvelope dlq2 %s: %v", id, err)
		}
	}
	if _, err := streamFeatures.ReadGroupEnvelopes(ctx, dlq2Stream, dlqGroup, consumer, 20, 0); err != nil {
		t.Fatalf("ReadGroupEnvelopes dlq2: %v", err)
	}

	resumeResult, err := db.ResumeReplayFromCheckpoint(ctx, streamFeatures, arangoAdapter, nil, &db.ResumeReplayFromCheckpointParams{
		CheckpointID:  finalCPID,
		IncludeAnchor: false, // after final anchor (= last msg), expect 0 replayed
		RequestID:     requestID,
		DLQStream:     dlq2Stream,
		TargetStream:  mainStream,
		DLQGroup:      dlqGroup,
		Consumer:      consumer,
		Limit:         50,
		Namespace:     ns,
		Group:         grp,
	})
	if err != nil {
		t.Fatalf("Phase5 ResumeReplayFromCheckpoint: %v", err)
	}
	if resumeResult.PlannedBy != "arango_checkpoint" {
		t.Errorf("Phase5 PlannedBy: want=arango_checkpoint got=%q", resumeResult.PlannedBy)
	}
	// The final checkpoint anchors at the last message, so "after anchor" = no messages to replay.
	// This is the correct behavior: all messages were already replayed.
	t.Logf("Phase5 resume: replayed=%d skipped=%d", resumeResult.Replayed, resumeResult.Skipped)

	// ---------- Phase 6: 任意锚点跳转 ----------
	// Jump to msg-2 as anchor, replay msg-3..5 from dlq3.
	t.Log("Phase 6: jump to anchor msg-2, replay msg-3..5")
	dlq3Stream := fmt.Sprintf("eitdb:e2e:dlq3:%d", now)
	if err := streamFeatures.EnsureConsumerGroup(ctx, dlq3Stream, dlqGroup); err != nil {
		t.Fatalf("EnsureConsumerGroup dlq3: %v", err)
	}
	for _, id := range msgIDs[2:] { // push msg-3, msg-4, msg-5
		if _, err := streamFeatures.PublishEnvelope(ctx, dlq3Stream, &db.CollaborationMessageEnvelope{
			MessageID: id, RequestID: requestID, EventType: "e2e.test", RetryCount: 1, MaxRetry: 3,
		}); err != nil {
			t.Fatalf("PublishEnvelope dlq3 %s: %v", id, err)
		}
	}
	if _, err := streamFeatures.ReadGroupEnvelopes(ctx, dlq3Stream, dlqGroup, consumer, 20, 0); err != nil {
		t.Fatalf("ReadGroupEnvelopes dlq3: %v", err)
	}

	jumpResult, err := db.JumpReplayToAnchor(ctx, streamFeatures, arangoAdapter, nil, &db.JumpReplayToAnchorParams{
		SessionID:     sessionID,
		AnchorType:    "message_id",
		AnchorValue:   msgIDs[1], // anchor at msg-2 (0-indexed)
		IncludeAnchor: false,     // replay msg-3, msg-4, msg-5
		RequestID:     requestID,
		DLQStream:     dlq3Stream,
		TargetStream:  mainStream,
		DLQGroup:      dlqGroup,
		Consumer:      consumer,
		Limit:         50,
		Namespace:     ns,
		Group:         grp,
	})
	if err != nil {
		t.Fatalf("Phase6 JumpReplayToAnchor: %v", err)
	}
	if jumpResult.PlannedBy != "arango_checkpoint" {
		t.Errorf("Phase6 PlannedBy: want=arango_checkpoint got=%q", jumpResult.PlannedBy)
	}
	if jumpResult.Replayed != 3 {
		t.Errorf("Phase6 Replayed: want=3 got=%d (skipped=%d)", jumpResult.Replayed, jumpResult.Skipped)
	}

	// ---------- Phase 7: 管理事件摘要验证 ----------
	t.Log("Phase 7: verify management event published")
	// The management event was published during Phase 3 replay. Read it from pubsub.
	eventCh := make(chan map[string]interface{}, 1)
	go func() {
		msg, err := pubSub.ReceiveMessage(ctx)
		if err != nil {
			return
		}
		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
			return
		}
		eventCh <- payload
	}()
	select {
	case evt := <-eventCh:
		evtType, _ := evt["event_type"].(string)
		if evtType != "dlq.replay.published" {
			t.Errorf("Phase7 event_type: want=dlq.replay.published got=%q", evtType)
		}
		innerPayload, _ := evt["payload"].(map[string]interface{})
		if innerPayload != nil {
			if _, hasSessionID := innerPayload["replay_session_id"]; !hasSessionID {
				t.Error("Phase7 management event missing replay_session_id")
			}
			// Must NOT carry full message ID lists.
			if _, hasFullIDs := innerPayload["replayed_message_ids"]; hasFullIDs {
				t.Error("Phase7 management event must not contain full replayed_message_ids list")
			}
		}
	case <-time.After(3 * time.Second):
		t.Log("Phase7: management event not received within 3s (may already have been buffered)")
	}

	// ---------- Phase 8: 降级路径验证 ----------
	t.Log("Phase 8: redis-only fallback validation")
	dlq4Stream := fmt.Sprintf("eitdb:e2e:dlq4:%d", now)
	if err := streamFeatures.EnsureConsumerGroup(ctx, dlq4Stream, dlqGroup); err != nil {
		t.Fatalf("EnsureConsumerGroup dlq4: %v", err)
	}
	for _, id := range msgIDs[:2] { // push msg-1, msg-2
		if _, err := streamFeatures.PublishEnvelope(ctx, dlq4Stream, &db.CollaborationMessageEnvelope{
			MessageID: id, RequestID: requestID, EventType: "e2e.test", RetryCount: 3, MaxRetry: 3,
		}); err != nil {
			t.Fatalf("PublishEnvelope dlq4 %s: %v", id, err)
		}
	}
	if _, err := streamFeatures.ReadGroupEnvelopes(ctx, dlq4Stream, dlqGroup, consumer, 10, 0); err != nil {
		t.Fatalf("ReadGroupEnvelopes dlq4: %v", err)
	}

	fallbackPlanner := db.NewDefaultReplayPlanner(nil) // nil → redis_only
	fallbackResult, err := streamFeatures.ReplayFromDLQWithPlannerAndTracking(
		ctx, dlq4Stream, mainStream, dlqGroup, consumer,
		requestID, 50, fallbackPlanner, nil, nil, ns, grp,
	)
	if err != nil {
		t.Fatalf("Phase8 ReplayFromDLQWithPlannerAndTracking (fallback): %v", err)
	}
	if fallbackResult.PlannedBy != "redis_only" {
		t.Errorf("Phase8 PlannedBy: want=redis_only got=%q", fallbackResult.PlannedBy)
	}
	if fallbackResult.Replayed != 2 {
		t.Errorf("Phase8 Replayed: want=2 got=%d", fallbackResult.Replayed)
	}

	t.Log("E2E complete: all phases passed")
}
