package adapter_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
)

func setupNeo4jRepo(t *testing.T) (*db.Repository, func()) {
	config := neo4jIntegrationConfig()
	if err := config.Validate(); err != nil {
		failIntegrationEnv(t, "Neo4j", err)
	}

	repo, err := db.NewRepository(config)
	if err != nil {
		failIntegrationEnv(t, "Neo4j", err)
	}

	if err := repo.Connect(context.Background()); err != nil {
		failIntegrationEnv(t, "Neo4j", err)
	}

	if err := repo.Ping(context.Background()); err != nil {
		failIntegrationEnv(t, "Neo4j", err)
	}

	cleanup := func() {
		_ = repo.Close()
	}

	return repo, cleanup
}

func TestNeo4jIntegrationWriteReadDelete(t *testing.T) {
	repo, cleanup := setupNeo4jRepo(t)
	defer cleanup()

	adapter, ok := repo.GetAdapter().(*db.Neo4jAdapter)
	if !ok {
		t.Fatalf("expected *db.Neo4jAdapter, got %T", repo.GetAdapter())
	}

	ctx := context.Background()
	uid := fmt.Sprintf("eitdb-it-%d", time.Now().UnixNano())

	_, err := adapter.ExecCypher(ctx,
		"CREATE (n:EitITNode {uid: $uid, name: $name, ts: $ts}) RETURN n",
		map[string]interface{}{"uid": uid, "name": "neo4j-it", "ts": time.Now().Unix()},
	)
	if err != nil {
		t.Fatalf("neo4j create failed: %v", err)
	}

	rows, err := adapter.QueryCypher(ctx,
		"MATCH (n:EitITNode {uid: $uid}) RETURN n.name AS name LIMIT 1",
		map[string]interface{}{"uid": uid},
	)
	if err != nil {
		t.Fatalf("neo4j query failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one neo4j row, got %d", len(rows))
	}
	if rows[0]["name"] != "neo4j-it" {
		t.Fatalf("unexpected neo4j row content: %#v", rows[0])
	}

	_, err = adapter.ExecCypher(ctx,
		"MATCH (n:EitITNode {uid: $uid}) DETACH DELETE n",
		map[string]interface{}{"uid": uid},
	)
	if err != nil {
		t.Fatalf("neo4j delete failed: %v", err)
	}
}
