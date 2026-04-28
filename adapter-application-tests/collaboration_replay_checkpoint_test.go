package adapter_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
)

// ---------------------------------------------------------------------------
// B1 扩展：CheckpointReplayPlanner / ResumeReplayFromCheckpoint / JumpReplayToAnchor
//
// 验收：
//   1. 写入 4 条 DLQ 消息（均属于同一 request）并建立 Arango 会话账本；
//   2. ResumeReplayFromCheckpoint：从 checkpoint（包含第 2 条消息锚点）恢复回放，
//      验证仅回放第 3、4 条（excludeAnchor）；
//   3. JumpReplayToAnchor：直接跳转到第 1 条消息作为锚点，
//      验证仅回放第 2、3、4 条（excludeAnchor）。
// ---------------------------------------------------------------------------

// TestResumeReplayFromCheckpoint_Integration 验证断点续跑从指定 checkpoint 之后恢复回放。
func TestResumeReplayFromCheckpoint_Integration(t *testing.T) {
	arangoRepo, arangoCleanup := setupArangoCollabRepo(t)
	defer arangoCleanup()
	redisRepo, redisCleanup := setupRedisCollabRepo(t)
	defer redisCleanup()

	arangoAdapter, ok := arangoRepo.GetAdapter().(*db.ArangoAdapter)
	if !ok || arangoAdapter == nil {
		t.Fatalf("expected arango adapter, got %T", arangoRepo.GetAdapter())
	}
	streamFeatures, ok := redisRepo.GetRedisStreamFeatures()
	if !ok || streamFeatures == nil {
		t.Fatalf("expected redis stream features")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	if err := arangoAdapter.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("EnsureCollaborationLedgerCollections failed: %v", err)
	}

	now := time.Now().UnixNano()
	mainStream := fmt.Sprintf("eitdb:it:resume_cp:main:%d", now)
	dlqStream := fmt.Sprintf("eitdb:it:resume_cp:dlq:%d", now)
	dlqGroup := "dlq-group"
	consumer := "cp-consumer"
	requestID := fmt.Sprintf("req-cp-%d", now)
	sessionID := fmt.Sprintf("sess-cp-%d", now)
	nowMs := time.Now().UnixMilli()

	msgIDs := []string{
		fmt.Sprintf("cp-msg-1-%d", now),
		fmt.Sprintf("cp-msg-2-%d", now),
		fmt.Sprintf("cp-msg-3-%d", now),
		fmt.Sprintf("cp-msg-4-%d", now),
	}

	// 1. Write all 4 messages to Arango ledger.
	for _, id := range msgIDs {
		if err := arangoAdapter.RecordCollaborationEnvelopeToLedger(ctx, &db.CollaborationMessageEnvelope{
			MessageID: id, RequestID: requestID, EventType: "test.cp",
			RetryCount: 3, MaxRetry: 3,
		}); err != nil {
			t.Fatalf("RecordCollaborationEnvelopeToLedger %s: %v", id, err)
		}
	}

	// 2. Write replay session and link all 4 messages with sequential seq.
	if err := arangoAdapter.UpsertLedgerReplaySessionNode(ctx, &db.ArangoReplaySessionNode{
		SessionID: sessionID, RequestID: requestID, Mode: "arango_preferred",
		PlannedBy: "arango", Status: "replayed", StartedAtUnixMs: nowMs, EndedAtUnixMs: nowMs,
	}); err != nil {
		t.Fatalf("UpsertLedgerReplaySessionNode: %v", err)
	}
	for i, id := range msgIDs {
		if err := arangoAdapter.LinkLedgerSessionReplaysMessage(ctx, sessionID, id, map[string]interface{}{
			"seq": i + 1, "replayed_at": nowMs,
		}); err != nil {
			t.Fatalf("LinkLedgerSessionReplaysMessage seq=%d: %v", i+1, err)
		}
	}

	// 3. Create checkpoint anchored at message 2 (seq=2).
	checkpointID := fmt.Sprintf("cp-anchor-msg2-%d", now)
	if err := arangoAdapter.UpsertLedgerReplayCheckpointNode(ctx, &db.ArangoReplayCheckpointNode{
		CheckpointID: checkpointID, SessionID: sessionID,
		AnchorType: "message_id", AnchorValue: msgIDs[1], // msg-2 (0-indexed)
		Tick: "2", Cursor: msgIDs[1], Status: "set", CreatedAtUnixMs: nowMs,
	}); err != nil {
		t.Fatalf("UpsertLedgerReplayCheckpointNode: %v", err)
	}
	if err := arangoAdapter.LinkLedgerSessionHasCheckpoint(ctx, sessionID, checkpointID, map[string]interface{}{"kind": "manual"}); err != nil {
		t.Fatalf("LinkLedgerSessionHasCheckpoint: %v", err)
	}

	// 4. Set up DLQ consumer group and publish all 4 msgs to DLQ.
	if err := streamFeatures.EnsureConsumerGroup(ctx, dlqStream, dlqGroup); err != nil {
		t.Fatalf("EnsureConsumerGroup dlq: %v", err)
	}
	if err := streamFeatures.EnsureConsumerGroup(ctx, mainStream, "main-group"); err != nil {
		t.Fatalf("EnsureConsumerGroup main: %v", err)
	}
	for _, id := range msgIDs {
		if _, err := streamFeatures.PublishEnvelope(ctx, dlqStream, &db.CollaborationMessageEnvelope{
			MessageID: id, RequestID: requestID, EventType: "test.cp", RetryCount: 3, MaxRetry: 3,
		}); err != nil {
			t.Fatalf("PublishEnvelope DLQ %s: %v", id, err)
		}
	}

	// 5. Consume DLQ to establish PEL.
	dlqMsgs, err := streamFeatures.ReadGroupEnvelopes(ctx, dlqStream, dlqGroup, consumer, 10, 0)
	if err != nil {
		t.Fatalf("ReadGroupEnvelopes DLQ failed: %v", err)
	}
	if len(dlqMsgs) != 4 {
		t.Fatalf("expected 4 DLQ msgs, got %d", len(dlqMsgs))
	}

	// 6. Resume replay from checkpoint (excludeAnchor=false means: exclude msg-2 itself, replay msg-3 and msg-4).
	result, err := db.ResumeReplayFromCheckpoint(ctx, streamFeatures, arangoAdapter, nil, &db.ResumeReplayFromCheckpointParams{
		CheckpointID:  checkpointID,
		IncludeAnchor: false, // replay only after anchor (msg-3, msg-4)
		RequestID:     requestID,
		DLQStream:     dlqStream,
		TargetStream:  mainStream,
		DLQGroup:      dlqGroup,
		Consumer:      consumer,
		Limit:         50,
		Namespace:     fmt.Sprintf("ns-cp-%d", now),
		Group:         fmt.Sprintf("grp-cp-%d", now),
	})
	if err != nil {
		t.Fatalf("ResumeReplayFromCheckpoint failed: %v", err)
	}

	if result.PlannedBy != "arango_checkpoint" {
		t.Errorf("expected PlannedBy=arango_checkpoint, got %q", result.PlannedBy)
	}
	// Expect exactly 2 replayed (msg-3, msg-4); msg-1 and msg-2 are skipped because
	// QueryReplayMessagesFromCheckpoint excludes anchor and prior messages.
	if result.Replayed != 2 {
		t.Errorf("expected 2 replayed, got %d (skipped=%d, read=%d)", result.Replayed, result.Skipped, result.Read)
	}
}

// TestJumpReplayToAnchor_Integration 验证任意锚点跳转回放从指定消息 ID 之后开始。
func TestJumpReplayToAnchor_Integration(t *testing.T) {
	arangoRepo, arangoCleanup := setupArangoCollabRepo(t)
	defer arangoCleanup()
	redisRepo, redisCleanup := setupRedisCollabRepo(t)
	defer redisCleanup()

	arangoAdapter, ok := arangoRepo.GetAdapter().(*db.ArangoAdapter)
	if !ok || arangoAdapter == nil {
		t.Fatalf("expected arango adapter, got %T", arangoRepo.GetAdapter())
	}
	streamFeatures, ok := redisRepo.GetRedisStreamFeatures()
	if !ok || streamFeatures == nil {
		t.Fatalf("expected redis stream features")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	if err := arangoAdapter.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("EnsureCollaborationLedgerCollections failed: %v", err)
	}

	now := time.Now().UnixNano()
	mainStream := fmt.Sprintf("eitdb:it:jump_anchor:main:%d", now)
	dlqStream := fmt.Sprintf("eitdb:it:jump_anchor:dlq:%d", now)
	dlqGroup := "dlq-group"
	consumer := "jump-consumer"
	requestID := fmt.Sprintf("req-jump-%d", now)
	sessionID := fmt.Sprintf("sess-jump-%d", now)
	nowMs := time.Now().UnixMilli()

	msgIDs := []string{
		fmt.Sprintf("jump-msg-1-%d", now),
		fmt.Sprintf("jump-msg-2-%d", now),
		fmt.Sprintf("jump-msg-3-%d", now),
	}

	// 1. Write all 3 messages to Arango ledger and session.
	for _, id := range msgIDs {
		if err := arangoAdapter.RecordCollaborationEnvelopeToLedger(ctx, &db.CollaborationMessageEnvelope{
			MessageID: id, RequestID: requestID, EventType: "test.jump",
			RetryCount: 3, MaxRetry: 3,
		}); err != nil {
			t.Fatalf("RecordCollaborationEnvelopeToLedger %s: %v", id, err)
		}
	}
	if err := arangoAdapter.UpsertLedgerReplaySessionNode(ctx, &db.ArangoReplaySessionNode{
		SessionID: sessionID, RequestID: requestID, Mode: "arango_preferred",
		PlannedBy: "arango", Status: "replayed", StartedAtUnixMs: nowMs, EndedAtUnixMs: nowMs,
	}); err != nil {
		t.Fatalf("UpsertLedgerReplaySessionNode: %v", err)
	}
	for i, id := range msgIDs {
		if err := arangoAdapter.LinkLedgerSessionReplaysMessage(ctx, sessionID, id, map[string]interface{}{
			"seq": i + 1, "replayed_at": nowMs,
		}); err != nil {
			t.Fatalf("LinkLedgerSessionReplaysMessage: %v", err)
		}
	}

	// 2. Set up streams and publish to DLQ.
	for _, s := range []string{mainStream, dlqStream} {
		grp := "main-group"
		if s == dlqStream {
			grp = dlqGroup
		}
		if err := streamFeatures.EnsureConsumerGroup(ctx, s, grp); err != nil {
			t.Fatalf("EnsureConsumerGroup %s: %v", s, err)
		}
	}
	for _, id := range msgIDs {
		if _, err := streamFeatures.PublishEnvelope(ctx, dlqStream, &db.CollaborationMessageEnvelope{
			MessageID: id, RequestID: requestID, EventType: "test.jump", RetryCount: 3, MaxRetry: 3,
		}); err != nil {
			t.Fatalf("PublishEnvelope DLQ %s: %v", id, err)
		}
	}

	// 3. Consume DLQ to establish PEL.
	dlqMsgs, err := streamFeatures.ReadGroupEnvelopes(ctx, dlqStream, dlqGroup, consumer, 10, 0)
	if err != nil {
		t.Fatalf("ReadGroupEnvelopes DLQ failed: %v", err)
	}
	if len(dlqMsgs) != 3 {
		t.Fatalf("expected 3 DLQ msgs, got %d", len(dlqMsgs))
	}

	// 4. Jump to anchor = msg-1 (excludeAnchor=false → replay msg-2, msg-3 only).
	result, err := db.JumpReplayToAnchor(ctx, streamFeatures, arangoAdapter, nil, &db.JumpReplayToAnchorParams{
		SessionID:     sessionID,
		AnchorType:    "message_id",
		AnchorValue:   msgIDs[0], // jump anchor at msg-1
		IncludeAnchor: false,
		RequestID:     requestID,
		DLQStream:     dlqStream,
		TargetStream:  mainStream,
		DLQGroup:      dlqGroup,
		Consumer:      consumer,
		Limit:         50,
		Namespace:     fmt.Sprintf("ns-jump-%d", now),
		Group:         fmt.Sprintf("grp-jump-%d", now),
	})
	if err != nil {
		t.Fatalf("JumpReplayToAnchor failed: %v", err)
	}

	if result.PlannedBy != "arango_checkpoint" {
		t.Errorf("expected PlannedBy=arango_checkpoint, got %q", result.PlannedBy)
	}
	// msg-1 is excluded (anchor), only msg-2 and msg-3 should be replayed.
	if result.Replayed != 2 {
		t.Errorf("expected 2 replayed, got %d (skipped=%d, read=%d)", result.Replayed, result.Skipped, result.Read)
	}
}
