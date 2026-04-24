package adapter_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
)

func setupRedisRepo(t *testing.T) (*db.Repository, func()) {
	config := redisIntegrationConfig()
	if err := config.Validate(); err != nil {
		failIntegrationEnv(t, "Redis", err)
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		failIntegrationEnv(t, "Redis", err)
	}

	if err := repo.Connect(context.Background()); err != nil {
		failIntegrationEnv(t, "Redis", err)
	}

	if err := repo.Ping(context.Background()); err != nil {
		failIntegrationEnv(t, "Redis", err)
	}

	cleanup := func() {
		_ = repo.Close()
	}

	return repo, cleanup
}

func TestRedisIntegrationSetGetDelete(t *testing.T) {
	repo, cleanup := setupRedisRepo(t)
	defer cleanup()

	adapter, ok := repo.GetAdapter().(*db.RedisAdapter)
	if !ok {
		t.Fatalf("expected *db.RedisAdapter, got %T", repo.GetAdapter())
	}

	ctx := context.Background()
	key := fmt.Sprintf("eitdb:it:redis:%d", time.Now().UnixNano())
	value := "hello-collab"

	if err := adapter.Set(ctx, key, value, 30*time.Second); err != nil {
		t.Fatalf("redis set failed: %v", err)
	}

	got, err := adapter.Get(ctx, key)
	if err != nil {
		t.Fatalf("redis get failed: %v", err)
	}
	if got != value {
		t.Fatalf("unexpected redis value: want=%q got=%q", value, got)
	}

	if _, err := adapter.Del(ctx, key); err != nil {
		t.Fatalf("redis del failed: %v", err)
	}
}
