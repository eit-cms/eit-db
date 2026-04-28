package adapter_tests

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
)

func redisCollabIntegrationConfig() *db.Config {
	return &db.Config{
		Adapter: "redis",
		Redis: &db.RedisConnectionConfig{
			Host:     getEnv("REDIS_HOST", "localhost"),
			Port:     getEnvInt("REDIS_PORT", 56379),
			Username: getEnv("REDIS_USER", ""),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getEnvInt("REDIS_DB", 0),
			URI:      getEnv("REDIS_URI", ""),
		},
	}
}

func setupRedisCollabRepo(t *testing.T) (*db.Repository, func()) {
	t.Helper()

	config := redisCollabIntegrationConfig()
	if err := config.Validate(); err != nil {
		t.Fatalf("Redis 配置无效: %v", err)
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		t.Fatalf("Redis Repository 初始化失败: %v", err)
	}

	if err := repo.Connect(context.Background()); err != nil {
		_ = repo.Close()
		t.Fatalf("Redis Connect 失败: %v", err)
	}
	if err := repo.Ping(context.Background()); err != nil {
		_ = repo.Close()
		t.Fatalf("Redis Ping 失败: %v", err)
	}

	return repo, func() {
		_ = repo.Close()
	}
}

func setupPostgresRepoStrict(t *testing.T) (*db.Repository, func()) {
	t.Helper()

	config := postgresIntegrationConfig()
	if err := config.Validate(); err != nil {
		t.Fatalf("PostgreSQL 配置无效: %v", err)
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		t.Fatalf("PostgreSQL Repository 初始化失败: %v", err)
	}

	if err := repo.Connect(context.Background()); err != nil {
		_ = repo.Close()
		t.Fatalf("PostgreSQL Connect 失败: %v", err)
	}
	if err := repo.Ping(context.Background()); err != nil {
		_ = repo.Close()
		t.Fatalf("PostgreSQL Ping 失败: %v", err)
	}

	return repo, func() {
		_ = repo.Close()
	}
}

func TestRedisIntegrationPublishSubscribeRoundTrip(t *testing.T) {
	repo, cleanup := setupRedisCollabRepo(t)
	defer cleanup()

	features, ok := repo.GetRedisSubscriberFeatures()
	if !ok || features == nil {
		t.Fatalf("expected redis subscriber features, got ok=%v", ok)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	channel := fmt.Sprintf("eitdb:it:collab:pubsub:%d", time.Now().UnixNano())
	pubSub := features.Subscribe(ctx, channel)
	defer pubSub.Close()

	if _, err := pubSub.Receive(ctx); err != nil {
		t.Fatalf("redis subscribe handshake failed: %v", err)
	}

	body := fmt.Sprintf("hello-collab-%d", time.Now().UnixNano())
	received := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		msg, err := pubSub.ReceiveMessage(ctx)
		if err != nil {
			errCh <- err
			return
		}
		received <- msg.Payload
	}()

	count, err := features.Publish(ctx, channel, body)
	if err != nil {
		t.Fatalf("redis publish failed: %v", err)
	}
	if count < 1 {
		t.Fatalf("expected at least one subscriber, got %d", count)
	}

	select {
	case err := <-errCh:
		t.Fatalf("redis receive failed: %v", err)
	case payload := <-received:
		if payload != body {
			t.Fatalf("unexpected payload: want=%q got=%q", body, payload)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for pub/sub message: %v", ctx.Err())
	}
}

func TestRedisPostgresCollaborationStreamRoundTrip(t *testing.T) {
	redisRepo, redisCleanup := setupRedisCollabRepo(t)
	defer redisCleanup()

	postgresRepo, postgresCleanup := setupPostgresRepoStrict(t)
	defer postgresCleanup()

	streamFeatures, ok := redisRepo.GetRedisStreamFeatures()
	if !ok || streamFeatures == nil {
		t.Fatalf("expected redis stream features, got ok=%v", ok)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("collab_users_%d", time.Now().UnixNano())
	createSQL := fmt.Sprintf(`CREATE TABLE "%s" (id BIGINT PRIMARY KEY, email TEXT NOT NULL UNIQUE, name TEXT NOT NULL)`, tableName)
	if _, err := postgresRepo.Exec(ctx, createSQL); err != nil {
		t.Fatalf("create postgres collaboration table failed: %v", err)
	}
	defer func() {
		_, _ = postgresRepo.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName))
	}()

	const userID int64 = 1001
	email := fmt.Sprintf("collab-%d@example.com", time.Now().UnixNano())
	name := "alice-collab"
	insertSQL := fmt.Sprintf(`INSERT INTO "%s" (id, email, name) VALUES ($1, $2, $3)`, tableName)
	if _, err := postgresRepo.Exec(ctx, insertSQL, userID, email, name); err != nil {
		t.Fatalf("seed postgres collaboration row failed: %v", err)
	}

	requestStream := fmt.Sprintf("collab:test:stream:request:%d", time.Now().UnixNano())
	responseStream := fmt.Sprintf("collab:test:stream:response:%d", time.Now().UnixNano())
	requestGroup := "adapter-postgres"
	responseGroup := "adapter-caller"

	if err := streamFeatures.EnsureConsumerGroup(ctx, requestStream, requestGroup); err != nil {
		t.Fatalf("ensure request consumer group failed: %v", err)
	}
	if err := streamFeatures.EnsureConsumerGroup(ctx, responseStream, responseGroup); err != nil {
		t.Fatalf("ensure response consumer group failed: %v", err)
	}

	requestEnvelope := &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-req-%d", time.Now().UnixNano()),
		RequestID:      fmt.Sprintf("req-%d", time.Now().UnixNano()),
		Topic:          "query.requested",
		EventType:      "query.requested",
		IdempotencyKey: fmt.Sprintf("idem-req-%d", time.Now().UnixNano()),
		Payload: map[string]interface{}{
			"table": tableName,
			"email": email,
		},
	}
	if _, err := streamFeatures.PublishEnvelope(ctx, requestStream, requestEnvelope); err != nil {
		t.Fatalf("publish request envelope failed: %v", err)
	}

	requestMessages, err := streamFeatures.ReadGroupEnvelopes(ctx, requestStream, requestGroup, "consumer-postgres", 1, 3*time.Second)
	if err != nil {
		t.Fatalf("read request envelope failed: %v", err)
	}
	if len(requestMessages) != 1 {
		t.Fatalf("expected one request message, got %d", len(requestMessages))
	}

	requestMessage := requestMessages[0]
	payloadEmail, ok := requestMessage.Envelope.Payload["email"].(string)
	if !ok || payloadEmail == "" {
		t.Fatalf("expected request payload email, got %#v", requestMessage.Envelope.Payload["email"])
	}

	querySQL := fmt.Sprintf(`SELECT id, name FROM "%s" WHERE email = $1`, tableName)
	var gotID int64
	var gotName string
	if err := postgresRepo.QueryRow(ctx, querySQL, payloadEmail).Scan(&gotID, &gotName); err != nil {
		t.Fatalf("postgres collaboration query failed: %v", err)
	}

	responseEnvelope := &db.CollaborationMessageEnvelope{
		MessageID:      fmt.Sprintf("msg-res-%d", time.Now().UnixNano()),
		RequestID:      requestMessage.Envelope.RequestID,
		Topic:          "query.result",
		EventType:      "query.result",
		IdempotencyKey: fmt.Sprintf("idem-res-%d", time.Now().UnixNano()),
		Payload: map[string]interface{}{
			"email":   payloadEmail,
			"user_id": strconv.FormatInt(gotID, 10),
			"name":    gotName,
		},
	}
	if _, err := streamFeatures.PublishEnvelope(ctx, responseStream, responseEnvelope); err != nil {
		t.Fatalf("publish response envelope failed: %v", err)
	}

	acked, err := streamFeatures.Ack(ctx, requestStream, requestGroup, requestMessage.ID)
	if err != nil {
		t.Fatalf("ack request message failed: %v", err)
	}
	if acked != 1 {
		t.Fatalf("expected one acked request message, got %d", acked)
	}

	responseMessages, err := streamFeatures.ReadGroupEnvelopes(ctx, responseStream, responseGroup, "consumer-caller", 1, 3*time.Second)
	if err != nil {
		t.Fatalf("read response envelope failed: %v", err)
	}
	if len(responseMessages) != 1 {
		t.Fatalf("expected one response message, got %d", len(responseMessages))
	}

	responsePayload := responseMessages[0].Envelope.Payload
	if responsePayload["email"] != email {
		t.Fatalf("unexpected response email: want=%q got=%v", email, responsePayload["email"])
	}
	if responsePayload["name"] != name {
		t.Fatalf("unexpected response name: want=%q got=%v", name, responsePayload["name"])
	}
	if responsePayload["user_id"] != strconv.FormatInt(userID, 10) {
		t.Fatalf("unexpected response user_id: want=%q got=%v", strconv.FormatInt(userID, 10), responsePayload["user_id"])
	}

	acked, err = streamFeatures.Ack(ctx, responseStream, responseGroup, responseMessages[0].ID)
	if err != nil {
		t.Fatalf("ack response message failed: %v", err)
	}
	if acked != 1 {
		t.Fatalf("expected one acked response message, got %d", acked)
	}
}
