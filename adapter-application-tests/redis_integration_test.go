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
		t.Skipf("Redis 配置无效: %v", err)
		return nil, nil
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		t.Skipf("Redis 不可用: %v", err)
		return nil, nil
	}

	if err := repo.Ping(context.Background()); err != nil {
		t.Skipf("Redis 连接失败: %v", err)
		return nil, nil
	}

	cleanup := func() {
		_ = repo.Close()
	}

	return repo, cleanup
}

func TestRedisIntegrationSetGetDelete(t *testing.T) {
	repo, cleanup := setupRedisRepo(t)
	if repo == nil {
		return
	}
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
