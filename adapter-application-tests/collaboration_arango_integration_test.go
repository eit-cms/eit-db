package adapter_tests

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
)

func setupArangoCollabRepo(t *testing.T) (*db.Repository, func()) {
	t.Helper()

	config := arangoIntegrationConfig()
	if err := config.Validate(); err != nil {
		t.Fatalf("Arango 配置无效: %v", err)
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		t.Fatalf("Arango Repository 初始化失败: %v", err)
	}

	if err := repo.Connect(context.Background()); err != nil {
		_ = repo.Close()
		t.Fatalf("Arango Connect 失败: %v", err)
	}
	if err := repo.Ping(context.Background()); err != nil {
		_ = repo.Close()
		t.Fatalf("Arango Ping 失败: %v", err)
	}

	return repo, func() {
		_ = repo.Close()
	}
}

func setupArangoCollabRepoWithNamespace(t *testing.T, namespace string) (*db.Repository, func()) {
	t.Helper()

	config := arangoIntegrationConfig()
	if config.Arango == nil {
		t.Fatal("expected arango sub-config")
	}
	config.Arango.Namespace = namespace
	if err := config.Validate(); err != nil {
		t.Fatalf("Arango 配置无效: %v", err)
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		t.Fatalf("Arango Repository 初始化失败: %v", err)
	}

	if err := repo.Connect(context.Background()); err != nil {
		_ = repo.Close()
		t.Fatalf("Arango Connect 失败: %v", err)
	}
	if err := repo.Ping(context.Background()); err != nil {
		_ = repo.Close()
		t.Fatalf("Arango Ping 失败: %v", err)
	}

	return repo, func() {
		_ = repo.Close()
	}
}

func mustRunRedisPostgresRoundTripOnce(t *testing.T, streamFeatures *db.RedisStreamFeatures, postgresRepo *db.Repository, requestStream string, responseStream string, requestID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("collab_users_dual_%d", time.Now().UnixNano())
	createSQL := fmt.Sprintf(`CREATE TABLE "%s" (id BIGINT PRIMARY KEY, email TEXT NOT NULL UNIQUE, name TEXT NOT NULL)`, tableName)
	if _, err := postgresRepo.Exec(ctx, createSQL); err != nil {
		t.Fatalf("create postgres table failed: %v", err)
	}
	defer func() {
		_, _ = postgresRepo.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName))
	}()

	const userID int64 = 3001
	email := fmt.Sprintf("dual-%d@example.com", time.Now().UnixNano())
	name := "dual-alice"
	insertSQL := fmt.Sprintf(`INSERT INTO "%s" (id, email, name) VALUES ($1, $2, $3)`, tableName)
	if _, err := postgresRepo.Exec(ctx, insertSQL, userID, email, name); err != nil {
		t.Fatalf("seed postgres row failed: %v", err)
	}

	requestGroup := "adapter-postgres"
	responseGroup := "adapter-caller"
	if err := streamFeatures.EnsureConsumerGroup(ctx, requestStream, requestGroup); err != nil {
		t.Fatalf("ensure request group failed: %v", err)
	}
	if err := streamFeatures.EnsureConsumerGroup(ctx, responseStream, responseGroup); err != nil {
		t.Fatalf("ensure response group failed: %v", err)
	}

	now := time.Now().UnixNano()
	requestEnvelope := &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-req-dual-%d", now),
		RequestID:      requestID,
		TraceID:        fmt.Sprintf("trace-dual-%d", now),
		SenderNodeID:   "adapter-api",
		ReceiverNodeID: "adapter-postgres",
		Topic:          "query.requested",
		EventType:      "query.requested",
		IdempotencyKey: fmt.Sprintf("idem-req-dual-%d", now),
		TicksSent:      1,
		SentAtUnixMs:   time.Now().UnixMilli(),
		Payload: map[string]interface{}{
			"table": tableName,
			"email": email,
		},
	}
	if _, err := streamFeatures.PublishEnvelope(ctx, requestStream, requestEnvelope); err != nil {
		t.Fatalf("publish request envelope failed: %v", err)
	}

	requestMessages, err := streamFeatures.ReadGroupEnvelopes(ctx, requestStream, requestGroup, "consumer-postgres", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("read request message failed: %v", err)
	}
	if len(requestMessages) != 1 {
		t.Fatalf("expected one request message, got %d", len(requestMessages))
	}

	payloadEmail, _ := requestMessages[0].Envelope.Payload["email"].(string)
	if payloadEmail == "" {
		t.Fatalf("expected request payload email, got %#v", requestMessages[0].Envelope.Payload["email"])
	}

	querySQL := fmt.Sprintf(`SELECT id, name FROM "%s" WHERE email = $1`, tableName)
	var gotID int64
	var gotName string
	if err := postgresRepo.QueryRow(ctx, querySQL, payloadEmail).Scan(&gotID, &gotName); err != nil {
		t.Fatalf("postgres query failed: %v", err)
	}

	responseEnvelope := &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-res-dual-%d", now),
		RequestID:      requestID,
		TraceID:        requestEnvelope.TraceID,
		SenderNodeID:   "adapter-postgres",
		ReceiverNodeID: "adapter-api",
		Topic:          "query.result",
		EventType:      "query.result",
		IdempotencyKey: fmt.Sprintf("idem-res-dual-%d", now),
		TicksSent:      2,
		SentAtUnixMs:   time.Now().UnixMilli(),
		Payload: map[string]interface{}{
			"email":   payloadEmail,
			"user_id": strconv.FormatInt(gotID, 10),
			"name":    gotName,
		},
	}
	if _, err := streamFeatures.PublishEnvelope(ctx, responseStream, responseEnvelope); err != nil {
		t.Fatalf("publish response envelope failed: %v", err)
	}

	if acked, err := streamFeatures.Ack(ctx, requestStream, requestGroup, requestMessages[0].ID); err != nil {
		t.Fatalf("ack request message failed: %v", err)
	} else if acked != 1 {
		t.Fatalf("expected one acked request message, got %d", acked)
	}

	responseMessages, err := streamFeatures.ReadGroupEnvelopes(ctx, responseStream, responseGroup, "consumer-caller", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("read response message failed: %v", err)
	}
	if len(responseMessages) != 1 {
		t.Fatalf("expected one response message, got %d", len(responseMessages))
	}
	if responseMessages[0].Envelope.Payload["email"] != email {
		t.Fatalf("unexpected response email: want=%q got=%v", email, responseMessages[0].Envelope.Payload["email"])
	}

	if acked, err := streamFeatures.Ack(ctx, responseStream, responseGroup, responseMessages[0].ID); err != nil {
		t.Fatalf("ack response message failed: %v", err)
	} else if acked != 1 {
		t.Fatalf("expected one acked response message, got %d", acked)
	}
}

func TestRedisPostgresConsumeAndArangoLedgerEndToEnd(t *testing.T) {
	redisRepo, redisCleanup := setupRedisCollabRepo(t)
	defer redisCleanup()

	postgresRepo, postgresCleanup := setupPostgresRepoStrict(t)
	defer postgresCleanup()

	arangoRepo, arangoCleanup := setupArangoCollabRepo(t)
	defer arangoCleanup()

	streamFeatures, ok := redisRepo.GetRedisStreamFeatures()
	if !ok || streamFeatures == nil {
		t.Fatalf("expected redis stream features, got ok=%v", ok)
	}

	arangoAdapter, ok := arangoRepo.GetAdapter().(*db.ArangoAdapter)
	if !ok || arangoAdapter == nil {
		t.Fatalf("expected arango adapter, got %T", arangoRepo.GetAdapter())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := arangoAdapter.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("ensure arango ledger collections failed: %v", err)
	}

	tableName := fmt.Sprintf("collab_users_arango_%d", time.Now().UnixNano())
	createSQL := fmt.Sprintf(`CREATE TABLE "%s" (id BIGINT PRIMARY KEY, email TEXT NOT NULL UNIQUE, name TEXT NOT NULL)`, tableName)
	if _, err := postgresRepo.Exec(ctx, createSQL); err != nil {
		t.Fatalf("create postgres table failed: %v", err)
	}
	defer func() {
		_, _ = postgresRepo.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName))
	}()

	const userID int64 = 2001
	email := fmt.Sprintf("ledger-%d@example.com", time.Now().UnixNano())
	name := "ledger-alice"
	insertSQL := fmt.Sprintf(`INSERT INTO "%s" (id, email, name) VALUES ($1, $2, $3)`, tableName)
	if _, err := postgresRepo.Exec(ctx, insertSQL, userID, email, name); err != nil {
		t.Fatalf("seed postgres row failed: %v", err)
	}

	now := time.Now().UnixNano()
	requestID := fmt.Sprintf("req-arango-%d", now)
	requestStream := fmt.Sprintf("collab:test:arango:request:%d", now)
	responseStream := fmt.Sprintf("collab:test:arango:response:%d", now)
	requestGroup := "adapter-postgres"
	responseGroup := "adapter-caller"

	if err := streamFeatures.EnsureConsumerGroup(ctx, requestStream, requestGroup); err != nil {
		t.Fatalf("ensure request group failed: %v", err)
	}
	if err := streamFeatures.EnsureConsumerGroup(ctx, responseStream, responseGroup); err != nil {
		t.Fatalf("ensure response group failed: %v", err)
	}

	requestEnvelope := &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-req-arango-%d", now),
		RequestID:      requestID,
		TraceID:        fmt.Sprintf("trace-arango-%d", now),
		SenderNodeID:   "adapter-api",
		ReceiverNodeID: "adapter-postgres",
		Topic:          "query.requested",
		EventType:      "query.requested",
		IdempotencyKey: fmt.Sprintf("idem-req-arango-%d", now),
		TicksSent:      1,
		SentAtUnixMs:   time.Now().UnixMilli(),
		Payload: map[string]interface{}{
			"table": tableName,
			"email": email,
		},
	}
	if _, err := streamFeatures.PublishEnvelope(ctx, requestStream, requestEnvelope); err != nil {
		t.Fatalf("publish request envelope failed: %v", err)
	}
	if err := arangoAdapter.RecordCollaborationEnvelopeToLedger(ctx, requestEnvelope); err != nil {
		t.Fatalf("record request envelope to arango ledger failed: %v", err)
	}

	requestMessages, err := streamFeatures.ReadGroupEnvelopes(ctx, requestStream, requestGroup, "consumer-postgres", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("read request message failed: %v", err)
	}
	if len(requestMessages) != 1 {
		t.Fatalf("expected one request message, got %d", len(requestMessages))
	}

	payloadEmail, _ := requestMessages[0].Envelope.Payload["email"].(string)
	if payloadEmail == "" {
		t.Fatalf("expected request payload email, got %#v", requestMessages[0].Envelope.Payload["email"])
	}

	querySQL := fmt.Sprintf(`SELECT id, name FROM "%s" WHERE email = $1`, tableName)
	var gotID int64
	var gotName string
	if err := postgresRepo.QueryRow(ctx, querySQL, payloadEmail).Scan(&gotID, &gotName); err != nil {
		t.Fatalf("postgres query failed: %v", err)
	}

	responseEnvelope := &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-res-arango-%d", now),
		RequestID:      requestID,
		TraceID:        requestEnvelope.TraceID,
		SenderNodeID:   "adapter-postgres",
		ReceiverNodeID: "adapter-api",
		Topic:          "query.result",
		EventType:      "query.result",
		IdempotencyKey: fmt.Sprintf("idem-res-arango-%d", now),
		TicksSent:      2,
		SentAtUnixMs:   time.Now().UnixMilli(),
		Payload: map[string]interface{}{
			"email":   payloadEmail,
			"user_id": strconv.FormatInt(gotID, 10),
			"name":    gotName,
		},
	}
	if _, err := streamFeatures.PublishEnvelope(ctx, responseStream, responseEnvelope); err != nil {
		t.Fatalf("publish response envelope failed: %v", err)
	}
	if err := arangoAdapter.RecordCollaborationEnvelopeToLedger(ctx, responseEnvelope); err != nil {
		t.Fatalf("record response envelope to arango ledger failed: %v", err)
	}

	if acked, err := streamFeatures.Ack(ctx, requestStream, requestGroup, requestMessages[0].ID); err != nil {
		t.Fatalf("ack request message failed: %v", err)
	} else if acked != 1 {
		t.Fatalf("expected one acked request message, got %d", acked)
	}

	responseMessages, err := streamFeatures.ReadGroupEnvelopes(ctx, responseStream, responseGroup, "consumer-caller", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("read response message failed: %v", err)
	}
	if len(responseMessages) != 1 {
		t.Fatalf("expected one response message, got %d", len(responseMessages))
	}

	if responseMessages[0].Envelope.Payload["email"] != email {
		t.Fatalf("unexpected response email: want=%q got=%v", email, responseMessages[0].Envelope.Payload["email"])
	}
	if responseMessages[0].Envelope.Payload["name"] != name {
		t.Fatalf("unexpected response name: want=%q got=%v", name, responseMessages[0].Envelope.Payload["name"])
	}

	if acked, err := streamFeatures.Ack(ctx, responseStream, responseGroup, responseMessages[0].ID); err != nil {
		t.Fatalf("ack response message failed: %v", err)
	} else if acked != 1 {
		t.Fatalf("expected one acked response message, got %d", acked)
	}

	paths, err := arangoAdapter.QueryLedgerDeliveryPath(ctx, requestID, 10)
	if err != nil {
		t.Fatalf("query arango ledger delivery path failed: %v", err)
	}
	if len(paths) < 2 {
		t.Fatalf("expected at least two ledger path rows, got %d", len(paths))
	}

	seenReq := false
	seenRes := false
	for _, row := range paths {
		msg, _ := row["message"].(map[string]interface{})
		sender, _ := row["sender"].(map[string]interface{})
		receiver, _ := row["receiver"].(map[string]interface{})
		if msg == nil {
			continue
		}
		msgKey, _ := msg["_key"].(string)
		senderKey, _ := sender["_key"].(string)
		receiverKey, _ := receiver["_key"].(string)
		switch msgKey {
		case requestEnvelope.MessageID:
			if senderKey != requestEnvelope.SenderNodeID || receiverKey != requestEnvelope.ReceiverNodeID {
				t.Fatalf("unexpected request path sender/receiver: sender=%q receiver=%q", senderKey, receiverKey)
			}
			seenReq = true
		case responseEnvelope.MessageID:
			if senderKey != responseEnvelope.SenderNodeID || receiverKey != responseEnvelope.ReceiverNodeID {
				t.Fatalf("unexpected response path sender/receiver: sender=%q receiver=%q", senderKey, receiverKey)
			}
			seenRes = true
		}
	}
	if !seenReq || !seenRes {
		t.Fatalf("expected to observe both request and response message paths, seenReq=%v seenRes=%v", seenReq, seenRes)
	}
}

func TestDualRedisAdaptersSameBackendRoundTripIsolation(t *testing.T) {
	managedRedisRepo, managedRedisCleanup := setupRedisCollabRepo(t)
	defer managedRedisCleanup()

	explicitRedisRepo, explicitRedisCleanup := setupRedisCollabRepo(t)
	defer explicitRedisCleanup()

	postgresRepo, postgresCleanup := setupPostgresRepoStrict(t)
	defer postgresCleanup()

	managedFeatures, ok := managedRedisRepo.GetRedisStreamFeatures()
	if !ok || managedFeatures == nil {
		t.Fatalf("expected managed redis stream features, got ok=%v", ok)
	}
	explicitFeatures, ok := explicitRedisRepo.GetRedisStreamFeatures()
	if !ok || explicitFeatures == nil {
		t.Fatalf("expected explicit redis stream features, got ok=%v", ok)
	}

	now := time.Now().UnixNano()
	managedRequestStream := fmt.Sprintf("collab:managed:request:%d", now)
	managedResponseStream := fmt.Sprintf("collab:managed:response:%d", now)
	explicitRequestStream := fmt.Sprintf("collab:explicit:request:%d", now)
	explicitResponseStream := fmt.Sprintf("collab:explicit:response:%d", now)

	mustRunRedisPostgresRoundTripOnce(t, managedFeatures, postgresRepo, managedRequestStream, managedResponseStream, fmt.Sprintf("req-managed-%d", now))
	mustRunRedisPostgresRoundTripOnce(t, explicitFeatures, postgresRepo, explicitRequestStream, explicitResponseStream, fmt.Sprintf("req-explicit-%d", now))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if msgs, err := explicitFeatures.ReadGroupEnvelopes(ctx, managedResponseStream, "adapter-caller", "consumer-cross", 1, time.Millisecond); err != nil {
		t.Fatalf("cross-read check failed: %v", err)
	} else if len(msgs) != 0 {
		t.Fatalf("expected explicit adapter not to consume managed response stream, got %d", len(msgs))
	}
}

func TestDualArangoAdaptersSameBackendNamespaceIsolation(t *testing.T) {
	managedArangoRepo, managedArangoCleanup := setupArangoCollabRepoWithNamespace(t, "managed_default")
	defer managedArangoCleanup()

	explicitArangoRepo, explicitArangoCleanup := setupArangoCollabRepoWithNamespace(t, "user_explicit")
	defer explicitArangoCleanup()

	managedAdapter, ok := managedArangoRepo.GetAdapter().(*db.ArangoAdapter)
	if !ok || managedAdapter == nil {
		t.Fatalf("expected managed arango adapter, got %T", managedArangoRepo.GetAdapter())
	}
	explicitAdapter, ok := explicitArangoRepo.GetAdapter().(*db.ArangoAdapter)
	if !ok || explicitAdapter == nil {
		t.Fatalf("expected explicit arango adapter, got %T", explicitArangoRepo.GetAdapter())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := managedAdapter.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("ensure managed arango collections failed: %v", err)
	}
	if err := explicitAdapter.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("ensure explicit arango collections failed: %v", err)
	}

	sharedRequestID := fmt.Sprintf("req-shared-%d", time.Now().UnixNano())
	managedEnvelope := &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-managed-%d", time.Now().UnixNano()),
		RequestID:      sharedRequestID,
		TraceID:        fmt.Sprintf("trace-managed-%d", time.Now().UnixNano()),
		SenderNodeID:   "managed-sender",
		ReceiverNodeID: "managed-receiver",
		EventType:      "query.requested",
		IdempotencyKey: fmt.Sprintf("idem-managed-%d", time.Now().UnixNano()),
		Stream:         "collab:managed:stream",
		TicksSent:      1,
		SentAtUnixMs:   time.Now().UnixMilli(),
	}
	explicitEnvelope := &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-explicit-%d", time.Now().UnixNano()),
		RequestID:      sharedRequestID,
		TraceID:        fmt.Sprintf("trace-explicit-%d", time.Now().UnixNano()),
		SenderNodeID:   "explicit-sender",
		ReceiverNodeID: "explicit-receiver",
		EventType:      "query.requested",
		IdempotencyKey: fmt.Sprintf("idem-explicit-%d", time.Now().UnixNano()),
		Stream:         "collab:explicit:stream",
		TicksSent:      1,
		SentAtUnixMs:   time.Now().UnixMilli(),
	}

	if err := managedAdapter.RecordCollaborationEnvelopeToLedger(ctx, managedEnvelope); err != nil {
		t.Fatalf("record managed envelope failed: %v", err)
	}
	if err := explicitAdapter.RecordCollaborationEnvelopeToLedger(ctx, explicitEnvelope); err != nil {
		t.Fatalf("record explicit envelope failed: %v", err)
	}

	managedPaths, err := managedAdapter.QueryLedgerDeliveryPath(ctx, sharedRequestID, 10)
	if err != nil {
		t.Fatalf("query managed ledger failed: %v", err)
	}
	explicitPaths, err := explicitAdapter.QueryLedgerDeliveryPath(ctx, sharedRequestID, 10)
	if err != nil {
		t.Fatalf("query explicit ledger failed: %v", err)
	}

	if len(managedPaths) != 1 {
		t.Fatalf("expected managed ledger to return exactly 1 path, got %d", len(managedPaths))
	}
	if len(explicitPaths) != 1 {
		t.Fatalf("expected explicit ledger to return exactly 1 path, got %d", len(explicitPaths))
	}

	managedMsg, _ := managedPaths[0]["message"].(map[string]interface{})
	explicitMsg, _ := explicitPaths[0]["message"].(map[string]interface{})
	if managedMsg == nil || explicitMsg == nil {
		t.Fatalf("expected message payload in both ledgers")
	}
	if managedMsg["_key"] != managedEnvelope.MessageID {
		t.Fatalf("managed ledger returned unexpected message key: %v", managedMsg["_key"])
	}
	if explicitMsg["_key"] != explicitEnvelope.MessageID {
		t.Fatalf("explicit ledger returned unexpected message key: %v", explicitMsg["_key"])
	}
}

func TestDualRedisAdaptersSameBackendPendingClaimRetryIsolation(t *testing.T) {
	managedRedisRepo, managedRedisCleanup := setupRedisCollabRepo(t)
	defer managedRedisCleanup()

	explicitRedisRepo, explicitRedisCleanup := setupRedisCollabRepo(t)
	defer explicitRedisCleanup()

	managedFeatures, ok := managedRedisRepo.GetRedisStreamFeatures()
	if !ok || managedFeatures == nil {
		t.Fatalf("expected managed redis stream features, got ok=%v", ok)
	}
	explicitFeatures, ok := explicitRedisRepo.GetRedisStreamFeatures()
	if !ok || explicitFeatures == nil {
		t.Fatalf("expected explicit redis stream features, got ok=%v", ok)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	now := time.Now().UnixNano()
	sharedRequestID := fmt.Sprintf("req-recovery-shared-%d", now)
	managedStream := fmt.Sprintf("collab:managed:recovery:%d", now)
	explicitStream := fmt.Sprintf("collab:explicit:recovery:%d", now)
	group := "adapter-recovery"

	if err := managedFeatures.EnsureConsumerGroup(ctx, managedStream, group); err != nil {
		t.Fatalf("ensure managed group failed: %v", err)
	}
	if err := explicitFeatures.EnsureConsumerGroup(ctx, explicitStream, group); err != nil {
		t.Fatalf("ensure explicit group failed: %v", err)
	}

	if _, err := managedFeatures.PublishEnvelope(ctx, managedStream, &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-managed-recovery-%d", now),
		RequestID:      sharedRequestID,
		EventType:      "query.requested",
		IdempotencyKey: fmt.Sprintf("idem-managed-recovery-%d", now),
		RetryCount:     0,
		MaxRetry:       3,
		Payload:        map[string]interface{}{"owner": "managed"},
	}); err != nil {
		t.Fatalf("publish managed envelope failed: %v", err)
	}
	if _, err := explicitFeatures.PublishEnvelope(ctx, explicitStream, &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-explicit-recovery-%d", now),
		RequestID:      sharedRequestID,
		EventType:      "query.requested",
		IdempotencyKey: fmt.Sprintf("idem-explicit-recovery-%d", now),
		RetryCount:     0,
		MaxRetry:       3,
		Payload:        map[string]interface{}{"owner": "explicit"},
	}); err != nil {
		t.Fatalf("publish explicit envelope failed: %v", err)
	}

	if _, err := managedFeatures.ReadGroupEnvelopes(ctx, managedStream, group, "consumer-main", 1, time.Second); err != nil {
		t.Fatalf("read managed pending seed failed: %v", err)
	}
	if _, err := explicitFeatures.ReadGroupEnvelopes(ctx, explicitStream, group, "consumer-main", 1, time.Second); err != nil {
		t.Fatalf("read explicit pending seed failed: %v", err)
	}

	if pending, err := managedFeatures.ListPendingMessages(ctx, managedStream, group, 10); err != nil {
		t.Fatalf("list managed pending failed: %v", err)
	} else if len(pending) != 1 {
		t.Fatalf("expected 1 managed pending message, got %d", len(pending))
	}
	if pending, err := explicitFeatures.ListPendingMessages(ctx, explicitStream, group, 10); err != nil {
		t.Fatalf("list explicit pending failed: %v", err)
	} else if len(pending) != 1 {
		t.Fatalf("expected 1 explicit pending message, got %d", len(pending))
	}

	if claimed, err := managedFeatures.ClaimPendingEnvelopes(ctx, managedStream, group, "managed-claim", 0, 10); err != nil {
		t.Fatalf("claim managed pending failed: %v", err)
	} else if len(claimed) != 1 {
		t.Fatalf("expected 1 managed claimed message, got %d", len(claimed))
	}

	if pending, err := explicitFeatures.ListPendingMessages(ctx, explicitStream, group, 10); err != nil {
		t.Fatalf("list explicit pending after managed claim failed: %v", err)
	} else if len(pending) != 1 {
		t.Fatalf("expected explicit pending unchanged at 1, got %d", len(pending))
	}

	managedResult, err := managedFeatures.RetryPendingEnvelopes(ctx, managedStream, group, "managed-retry", "", 0, 10)
	if err != nil {
		t.Fatalf("retry managed pending failed: %v", err)
	}
	if managedResult.Claimed != 1 || managedResult.Retried != 1 || managedResult.DeadLettered != 0 {
		t.Fatalf("unexpected managed retry result: %+v", managedResult)
	}

	if pending, err := managedFeatures.ListPendingMessages(ctx, managedStream, group, 10); err != nil {
		t.Fatalf("list managed pending after retry failed: %v", err)
	} else if len(pending) != 0 {
		t.Fatalf("expected managed pending empty after retry, got %d", len(pending))
	}
	if pending, err := explicitFeatures.ListPendingMessages(ctx, explicitStream, group, 10); err != nil {
		t.Fatalf("list explicit pending before explicit retry failed: %v", err)
	} else if len(pending) != 1 {
		t.Fatalf("expected explicit pending still 1 before explicit retry, got %d", len(pending))
	}

	explicitResult, err := explicitFeatures.RetryPendingEnvelopes(ctx, explicitStream, group, "explicit-retry", "", 0, 10)
	if err != nil {
		t.Fatalf("retry explicit pending failed: %v", err)
	}
	if explicitResult.Claimed != 1 || explicitResult.Retried != 1 || explicitResult.DeadLettered != 0 {
		t.Fatalf("unexpected explicit retry result: %+v", explicitResult)
	}
}

func TestDualRedisAdaptersSameBackendDeadLetterIsolation(t *testing.T) {
	managedRedisRepo, managedRedisCleanup := setupRedisCollabRepo(t)
	defer managedRedisCleanup()

	explicitRedisRepo, explicitRedisCleanup := setupRedisCollabRepo(t)
	defer explicitRedisCleanup()

	managedFeatures, ok := managedRedisRepo.GetRedisStreamFeatures()
	if !ok || managedFeatures == nil {
		t.Fatalf("expected managed redis stream features, got ok=%v", ok)
	}
	explicitFeatures, ok := explicitRedisRepo.GetRedisStreamFeatures()
	if !ok || explicitFeatures == nil {
		t.Fatalf("expected explicit redis stream features, got ok=%v", ok)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	now := time.Now().UnixNano()
	sharedRequestID := fmt.Sprintf("req-dlq-shared-%d", now)
	managedStream := fmt.Sprintf("collab:managed:dlq:src:%d", now)
	explicitStream := fmt.Sprintf("collab:explicit:dlq:src:%d", now)
	managedDLQ := fmt.Sprintf("collab:managed:dlq:dst:%d", now)
	explicitDLQ := fmt.Sprintf("collab:explicit:dlq:dst:%d", now)
	group := "adapter-dlq"

	if err := managedFeatures.EnsureConsumerGroup(ctx, managedStream, group); err != nil {
		t.Fatalf("ensure managed source group failed: %v", err)
	}
	if err := explicitFeatures.EnsureConsumerGroup(ctx, explicitStream, group); err != nil {
		t.Fatalf("ensure explicit source group failed: %v", err)
	}
	if err := managedFeatures.EnsureConsumerGroup(ctx, managedDLQ, group); err != nil {
		t.Fatalf("ensure managed dlq group failed: %v", err)
	}
	if err := explicitFeatures.EnsureConsumerGroup(ctx, explicitDLQ, group); err != nil {
		t.Fatalf("ensure explicit dlq group failed: %v", err)
	}

	if _, err := managedFeatures.PublishEnvelope(ctx, managedStream, &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-managed-dlq-%d", now),
		RequestID:      sharedRequestID,
		EventType:      "query.requested",
		IdempotencyKey: fmt.Sprintf("idem-managed-dlq-%d", now),
		RetryCount:     1,
		MaxRetry:       1,
		Payload:        map[string]interface{}{"owner": "managed"},
	}); err != nil {
		t.Fatalf("publish managed dlq envelope failed: %v", err)
	}
	if _, err := explicitFeatures.PublishEnvelope(ctx, explicitStream, &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-explicit-dlq-%d", now),
		RequestID:      sharedRequestID,
		EventType:      "query.requested",
		IdempotencyKey: fmt.Sprintf("idem-explicit-dlq-%d", now),
		RetryCount:     1,
		MaxRetry:       1,
		Payload:        map[string]interface{}{"owner": "explicit"},
	}); err != nil {
		t.Fatalf("publish explicit dlq envelope failed: %v", err)
	}

	if _, err := managedFeatures.ReadGroupEnvelopes(ctx, managedStream, group, "consumer-main", 1, time.Second); err != nil {
		t.Fatalf("read managed source failed: %v", err)
	}
	if _, err := explicitFeatures.ReadGroupEnvelopes(ctx, explicitStream, group, "consumer-main", 1, time.Second); err != nil {
		t.Fatalf("read explicit source failed: %v", err)
	}

	managedResult, err := managedFeatures.RetryPendingEnvelopes(ctx, managedStream, group, "managed-retry", managedDLQ, 0, 10)
	if err != nil {
		t.Fatalf("managed retry to dlq failed: %v", err)
	}
	if managedResult.Claimed != 1 || managedResult.DeadLettered != 1 || managedResult.Retried != 0 {
		t.Fatalf("unexpected managed dlq result: %+v", managedResult)
	}

	if pending, err := explicitFeatures.ListPendingMessages(ctx, explicitStream, group, 10); err != nil {
		t.Fatalf("list explicit pending after managed dlq failed: %v", err)
	} else if len(pending) != 1 {
		t.Fatalf("expected explicit pending unchanged at 1, got %d", len(pending))
	}

	explicitResult, err := explicitFeatures.RetryPendingEnvelopes(ctx, explicitStream, group, "explicit-retry", explicitDLQ, 0, 10)
	if err != nil {
		t.Fatalf("explicit retry to dlq failed: %v", err)
	}
	if explicitResult.Claimed != 1 || explicitResult.DeadLettered != 1 || explicitResult.Retried != 0 {
		t.Fatalf("unexpected explicit dlq result: %+v", explicitResult)
	}

	managedDLQMessages, err := managedFeatures.ReadGroupEnvelopes(ctx, managedDLQ, group, "consumer-dlq-managed", 10, time.Second)
	if err != nil {
		t.Fatalf("read managed dlq failed: %v", err)
	}
	if len(managedDLQMessages) != 1 {
		t.Fatalf("expected 1 managed dlq message, got %d", len(managedDLQMessages))
	}
	if managedDLQMessages[0].Envelope.Stream != managedDLQ {
		t.Fatalf("expected managed dlq stream %s, got %s", managedDLQ, managedDLQMessages[0].Envelope.Stream)
	}

	explicitDLQMessages, err := explicitFeatures.ReadGroupEnvelopes(ctx, explicitDLQ, group, "consumer-dlq-explicit", 10, time.Second)
	if err != nil {
		t.Fatalf("read explicit dlq failed: %v", err)
	}
	if len(explicitDLQMessages) != 1 {
		t.Fatalf("expected 1 explicit dlq message, got %d", len(explicitDLQMessages))
	}
	if explicitDLQMessages[0].Envelope.Stream != explicitDLQ {
		t.Fatalf("expected explicit dlq stream %s, got %s", explicitDLQ, explicitDLQMessages[0].Envelope.Stream)
	}
}
