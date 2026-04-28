package db

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
)

func TestCollaborationMessageEnvelopeRoundTrip(t *testing.T) {
	envelope := &CollaborationMessageEnvelope{
		MessageID:      "msg-1",
		RequestID:      "req-1",
		EventType:      "query.requested",
		IdempotencyKey: "idem-1",
		Payload: map[string]interface{}{
			"entity": "user",
		},
	}
	envelope.NormalizeForStream("collab:test:stream")

	encoded, err := EncodeCollaborationMessageEnvelope(envelope)
	if err != nil {
		t.Fatalf("encode collaboration message envelope failed: %v", err)
	}
	decoded, err := DecodeCollaborationMessageEnvelope(encoded)
	if err != nil {
		t.Fatalf("decode collaboration message envelope failed: %v", err)
	}
	if decoded.Stream != "collab:test:stream" {
		t.Fatalf("expected stream collab:test:stream, got %s", decoded.Stream)
	}
	if decoded.EventType != envelope.EventType {
		t.Fatalf("expected event_type %s, got %s", envelope.EventType, decoded.EventType)
	}
}

func TestGetRedisStreamFeatures(t *testing.T) {
	if feat, ok := GetRedisStreamFeatures(&nonRedisAdapterForSubscriberTest{}); ok || feat != nil {
		t.Fatalf("expected non-redis adapter to return nil stream features, got (%v, %v)", feat, ok)
	}

	feat, ok := GetRedisStreamFeatures(&RedisAdapter{})
	if !ok || feat == nil {
		t.Fatal("expected redis adapter stream features")
	}

	repo := &Repository{adapter: &RedisAdapter{}}
	feat, ok = repo.GetRedisStreamFeatures()
	if !ok || feat == nil {
		t.Fatal("expected repository to expose redis stream features")
	}
}

func TestRedisStreamFeaturesClaimPendingEnvelopes(t *testing.T) {
	features, cleanup := newTestRedisStreamFeatures(t)
	defer cleanup()

	ctx := context.Background()
	stream := "collab:test:claim"
	group := "adapter-a"
	if err := features.EnsureConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("ensure consumer group failed: %v", err)
	}

	_, err := features.PublishEnvelope(ctx, stream, &CollaborationMessageEnvelope{
		MessageID:      "msg-claim-1",
		RequestID:      "req-claim-1",
		EventType:      "query.requested",
		IdempotencyKey: "idem-claim-1",
		Payload:        map[string]interface{}{"email": "a@example.com"},
	})
	if err != nil {
		t.Fatalf("publish envelope failed: %v", err)
	}

	messages, err := features.ReadGroupEnvelopes(ctx, stream, group, "consumer-a", 1, 0)
	if err != nil {
		t.Fatalf("read group envelopes failed: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one message, got %d", len(messages))
	}

	claimed, err := features.ClaimPendingEnvelopes(ctx, stream, group, "consumer-b", 0, 10)
	if err != nil {
		t.Fatalf("claim pending envelopes failed: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimed message, got %d", len(claimed))
	}
	if claimed[0].Envelope.MessageID != "msg-claim-1" {
		t.Fatalf("unexpected claimed message id: %s", claimed[0].Envelope.MessageID)
	}

	pending, err := features.ListPendingMessages(ctx, stream, group, 10)
	if err != nil {
		t.Fatalf("list pending messages failed: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected one pending summary after claim, got %d", len(pending))
	}
	if pending[0].Consumer != "consumer-b" {
		t.Fatalf("expected pending owner consumer-b, got %s", pending[0].Consumer)
	}
}

func TestRedisStreamFeaturesRetryPendingEnvelopesDeadLetter(t *testing.T) {
	features, cleanup := newTestRedisStreamFeatures(t)
	defer cleanup()

	ctx := context.Background()
	stream := "collab:test:retry"
	group := "adapter-a"
	dlqStream := "collab:test:dlq"
	dlqGroup := "adapter-dlq"
	if err := features.EnsureConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("ensure consumer group failed: %v", err)
	}
	if err := features.EnsureConsumerGroup(ctx, dlqStream, dlqGroup); err != nil {
		t.Fatalf("ensure dlq consumer group failed: %v", err)
	}

	_, err := features.PublishEnvelope(ctx, stream, &CollaborationMessageEnvelope{
		MessageID:      "msg-retry-1",
		RequestID:      "req-retry-1",
		EventType:      "query.requested",
		IdempotencyKey: "idem-retry-1",
		RetryCount:     1,
		MaxRetry:       1,
		Payload:        map[string]interface{}{"email": "retry@example.com"},
	})
	if err != nil {
		t.Fatalf("publish envelope failed: %v", err)
	}

	if _, err := features.ReadGroupEnvelopes(ctx, stream, group, "consumer-a", 1, 0); err != nil {
		t.Fatalf("initial read failed: %v", err)
	}

	result, err := features.RetryPendingEnvelopes(ctx, stream, group, "consumer-retry", dlqStream, 0, 10)
	if err != nil {
		t.Fatalf("retry pending envelopes failed: %v", err)
	}
	if result.Claimed != 1 || result.DeadLettered != 1 || result.Retried != 0 {
		t.Fatalf("unexpected retry result: %+v", result)
	}

	pending, err := features.ListPendingMessages(ctx, stream, group, 10)
	if err != nil {
		t.Fatalf("list pending after dlq failed: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected original stream pending to be empty after dlq, got %d", len(pending))
	}

	dlqMessages, err := features.ReadGroupEnvelopes(ctx, dlqStream, dlqGroup, "consumer-dlq", 1, time.Millisecond)
	if err != nil {
		t.Fatalf("read dlq messages failed: %v", err)
	}
	if len(dlqMessages) != 1 {
		t.Fatalf("expected one dlq message, got %d", len(dlqMessages))
	}
	if dlqMessages[0].Envelope.RetryCount != 1 {
		t.Fatalf("expected dlq retry count 1, got %d", dlqMessages[0].Envelope.RetryCount)
	}
	if dlqMessages[0].Envelope.Stream != dlqStream {
		t.Fatalf("expected dlq stream %s, got %s", dlqStream, dlqMessages[0].Envelope.Stream)
	}
}

func TestRedisStreamFeaturesRetryPendingEnvelopesRepublish(t *testing.T) {
	features, cleanup := newTestRedisStreamFeatures(t)
	defer cleanup()

	ctx := context.Background()
	stream := "collab:test:retry-republish"
	group := "adapter-a"
	if err := features.EnsureConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("ensure consumer group failed: %v", err)
	}

	_, err := features.PublishEnvelope(ctx, stream, &CollaborationMessageEnvelope{
		MessageID:      "msg-republish-1",
		RequestID:      "req-republish-1",
		EventType:      "query.requested",
		IdempotencyKey: "idem-republish-1",
		RetryCount:     0,
		MaxRetry:       2,
		Payload:        map[string]interface{}{"email": "republish@example.com"},
	})
	if err != nil {
		t.Fatalf("publish envelope failed: %v", err)
	}

	if _, err := features.ReadGroupEnvelopes(ctx, stream, group, "consumer-a", 1, 0); err != nil {
		t.Fatalf("initial read failed: %v", err)
	}

	result, err := features.RetryPendingEnvelopes(ctx, stream, group, "consumer-retry", "", 0, 10)
	if err != nil {
		t.Fatalf("retry pending envelopes failed: %v", err)
	}
	if result.Claimed != 1 || result.Retried != 1 || result.DeadLettered != 0 {
		t.Fatalf("unexpected retry result: %+v", result)
	}

	retriedMessages, err := features.ReadGroupEnvelopes(ctx, stream, group, "consumer-b", 1, time.Millisecond)
	if err != nil {
		t.Fatalf("read retried messages failed: %v", err)
	}
	if len(retriedMessages) != 1 {
		t.Fatalf("expected one retried message, got %d", len(retriedMessages))
	}
	if retriedMessages[0].Envelope.RetryCount != 1 {
		t.Fatalf("expected retried message retry count 1, got %d", retriedMessages[0].Envelope.RetryCount)
	}
}

func TestRedisStreamFeaturesSnapshotLag(t *testing.T) {
	features, cleanup := newTestRedisStreamFeatures(t)
	defer cleanup()

	ctx := context.Background()
	stream := "collab:test:lag"
	group := "adapter-a"
	if err := features.EnsureConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("ensure consumer group failed: %v", err)
	}

	if _, err := features.PublishEnvelope(ctx, stream, &CollaborationMessageEnvelope{
		MessageID:      "msg-lag-1",
		RequestID:      "req-lag-1",
		EventType:      "query.requested",
		IdempotencyKey: "idem-lag-1",
	}); err != nil {
		t.Fatalf("publish first envelope failed: %v", err)
	}
	if _, err := features.PublishEnvelope(ctx, stream, &CollaborationMessageEnvelope{
		MessageID:      "msg-lag-2",
		RequestID:      "req-lag-2",
		EventType:      "query.requested",
		IdempotencyKey: "idem-lag-2",
	}); err != nil {
		t.Fatalf("publish second envelope failed: %v", err)
	}

	if _, err := features.ReadGroupEnvelopes(ctx, stream, group, "consumer-a", 1, 0); err != nil {
		t.Fatalf("read one message failed: %v", err)
	}

	snapshot, err := features.SnapshotLag(ctx, stream, group)
	if err != nil {
		t.Fatalf("snapshot lag failed: %v", err)
	}
	if snapshot.StreamLength < 2 {
		t.Fatalf("expected stream length >= 2, got %d", snapshot.StreamLength)
	}
	if snapshot.PendingCount < 1 {
		t.Fatalf("expected pending count >= 1, got %d", snapshot.PendingCount)
	}
}

func TestRedisStreamFeaturesStartAutoRecovery(t *testing.T) {
	features, cleanup := newTestRedisStreamFeatures(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream := "collab:test:auto-recovery"
	group := "adapter-a"
	if err := features.EnsureConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("ensure consumer group failed: %v", err)
	}

	if _, err := features.PublishEnvelope(ctx, stream, &CollaborationMessageEnvelope{
		MessageID:      "msg-auto-1",
		RequestID:      "req-auto-1",
		EventType:      "query.requested",
		IdempotencyKey: "idem-auto-1",
		RetryCount:     0,
		MaxRetry:       2,
	}); err != nil {
		t.Fatalf("publish envelope failed: %v", err)
	}

	if _, err := features.ReadGroupEnvelopes(ctx, stream, group, "consumer-a", 1, 0); err != nil {
		t.Fatalf("initial read failed: %v", err)
	}

	var recovered atomic.Int64
	controller, err := features.StartAutoRecovery(ctx, stream, group, "consumer-auto", RedisStreamRecoveryPolicy{
		Interval:  20 * time.Millisecond,
		MinIdle:   0,
		BatchSize: 5,
		MaxRounds: 2,
		OnTickResult: func(result *RedisStreamRetryResult) {
			if result != nil {
				recovered.Add(int64(result.Retried + result.DeadLettered))
			}
		},
	})
	if err != nil {
		t.Fatalf("start auto recovery failed: %v", err)
	}
	defer controller.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if recovered.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if recovered.Load() == 0 {
		t.Fatal("expected auto recovery to recover at least one message")
	}
}

func newTestRedisStreamFeatures(t *testing.T) (*RedisStreamFeatures, func()) {
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
	config := &Config{
		Adapter: "redis",
		Redis: &RedisConnectionConfig{
			Host: host,
			Port: port,
		},
	}
	adapter, err := NewRedisAdapter(config)
	if err != nil {
		t.Fatalf("new redis adapter failed: %v", err)
	}
	if err := adapter.Connect(context.Background(), config); err != nil {
		t.Fatalf("connect redis adapter failed: %v", err)
	}
	features, ok := GetRedisStreamFeatures(adapter)
	if !ok || features == nil {
		t.Fatal("expected redis stream features from adapter")
	}
	return features, func() {
		_ = adapter.Close()
		server.Close()
	}
}
