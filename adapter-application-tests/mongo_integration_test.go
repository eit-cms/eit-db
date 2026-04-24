package adapter_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func setupMongoRepo(t *testing.T) (*db.Repository, *db.Config, func()) {
	config := mongoIntegrationConfig()
	if err := config.Validate(); err != nil {
		t.Skipf("MongoDB 配置无效: %v", err)
		return nil, nil, nil
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		t.Skipf("MongoDB 不可用: %v", err)
		return nil, nil, nil
	}

	if err := repo.Ping(context.Background()); err != nil {
		t.Skipf("MongoDB 连接失败: %v", err)
		return nil, nil, nil
	}

	cleanup := func() {
		_ = repo.Close()
	}

	return repo, config, cleanup
}

func TestMongoIntegrationInsertFindDelete(t *testing.T) {
	repo, config, cleanup := setupMongoRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter, ok := repo.GetAdapter().(*db.MongoAdapter)
	if !ok {
		t.Fatalf("expected *db.MongoAdapter, got %T", repo.GetAdapter())
	}

	raw := adapter.GetRawConn()
	client, ok := raw.(*mongo.Client)
	if !ok || client == nil {
		t.Fatalf("expected raw mongo client, got %T", raw)
	}

	ctx := context.Background()
	collName := "eit_it_mongo_records"
	coll := client.Database(config.ResolvedMongoConfig().Database).Collection(collName)
	key := fmt.Sprintf("eitdb:it:mongo:%d", time.Now().UnixNano())

	_, err := coll.InsertOne(ctx, bson.M{"_id": key, "name": "mongo-it", "ts": time.Now().Unix()})
	if err != nil {
		t.Fatalf("mongo insert failed: %v", err)
	}

	var out bson.M
	if err := coll.FindOne(ctx, bson.M{"_id": key}).Decode(&out); err != nil {
		t.Fatalf("mongo find failed: %v", err)
	}
	if out["name"] != "mongo-it" {
		t.Fatalf("unexpected mongo field value: %#v", out)
	}

	if _, err := coll.DeleteOne(ctx, bson.M{"_id": key}); err != nil {
		t.Fatalf("mongo delete failed: %v", err)
	}
}
