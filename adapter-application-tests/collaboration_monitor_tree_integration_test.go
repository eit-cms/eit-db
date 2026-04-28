package adapter_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	db "github.com/eit-cms/eit-db"
)

func TestRedisArangoUnifiedManagementPath(t *testing.T) {
	redisRepo, redisCleanup := setupRedisCollabRepo(t)
	defer redisCleanup()

	arangoRepo, arangoCleanup := setupArangoCollabRepoWithNamespace(t, "monitor_tree_it")
	defer arangoCleanup()

	managementFeatures, ok := redisRepo.GetRedisManagementFeatures()
	if !ok || managementFeatures == nil {
		t.Fatalf("expected redis management features, got ok=%v", ok)
	}

	arangoAdapter, ok := arangoRepo.GetAdapter().(*db.ArangoAdapter)
	if !ok || arangoAdapter == nil {
		t.Fatalf("expected arango adapter, got %T", arangoRepo.GetAdapter())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := arangoAdapter.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("ensure arango collections failed: %v", err)
	}

	now := time.Now().UnixNano()
	namespace := "monitor_tree_it"
	group := fmt.Sprintf("group-%d", now)
	managedNode := db.CollaborationAdapterNodePresence{
		NodeID:       fmt.Sprintf("managed-%d", now),
		AdapterType:  "redis",
		AdapterID:    "managed-default",
		Group:        group,
		Namespace:    namespace,
		Capabilities: []string{"stream", "pubsub", "monitor"},
	}
	explicitNode := db.CollaborationAdapterNodePresence{
		NodeID:       fmt.Sprintf("explicit-%d", now),
		AdapterType:  "redis",
		AdapterID:    "user-explicit",
		Group:        group,
		Namespace:    namespace,
		Capabilities: []string{"stream", "pubsub"},
	}

	if err := managementFeatures.RegisterAdapterNode(ctx, &managedNode, 30*time.Second); err != nil {
		t.Fatalf("register managed node failed: %v", err)
	}
	if err := managementFeatures.RegisterAdapterNode(ctx, &explicitNode, 30*time.Second); err != nil {
		t.Fatalf("register explicit node failed: %v", err)
	}
	if err := managementFeatures.HeartbeatAdapterNode(ctx, namespace, group, managedNode.NodeID, 30*time.Second); err != nil {
		t.Fatalf("heartbeat managed node failed: %v", err)
	}

	nodes, err := managementFeatures.ListGroupNodes(ctx, namespace, group)
	if err != nil {
		t.Fatalf("list group nodes failed: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes from redis management path, got %d", len(nodes))
	}

	for _, node := range nodes {
		n := node
		if err := arangoAdapter.UpsertOnlineAdapterPresence(ctx, &n); err != nil {
			t.Fatalf("upsert online adapter presence failed: %v", err)
		}
	}

	if err := managementFeatures.MarkAdapterOffline(ctx, namespace, group, explicitNode.NodeID); err != nil {
		t.Fatalf("mark explicit node offline failed: %v", err)
	}
	if err := arangoAdapter.UpsertOnlineAdapterPresence(ctx, &db.CollaborationAdapterNodePresence{
		NodeID:              explicitNode.NodeID,
		AdapterType:         explicitNode.AdapterType,
		AdapterID:           explicitNode.AdapterID,
		Group:               group,
		Namespace:           namespace,
		Status:              "offline",
		LastHeartbeatUnixMs: time.Now().UnixMilli(),
		Capabilities:        explicitNode.Capabilities,
	}); err != nil {
		t.Fatalf("project offline node to arango failed: %v", err)
	}

	onlineRows, err := arangoAdapter.QueryOnlineAdapterNodes(ctx, "online", 20)
	if err != nil {
		t.Fatalf("query online adapter nodes failed: %v", err)
	}
	offlineRows, err := arangoAdapter.QueryOnlineAdapterNodes(ctx, "offline", 20)
	if err != nil {
		t.Fatalf("query offline adapter nodes failed: %v", err)
	}

	seenManagedOnline := false
	for _, row := range onlineRows {
		if key, _ := row["_key"].(string); key == managedNode.NodeID {
			seenManagedOnline = true
		}
	}
	if !seenManagedOnline {
		t.Fatalf("expected managed node %s in arango online snapshot", managedNode.NodeID)
	}

	seenExplicitOffline := false
	for _, row := range offlineRows {
		if key, _ := row["_key"].(string); key == explicitNode.NodeID {
			seenExplicitOffline = true
		}
	}
	if !seenExplicitOffline {
		t.Fatalf("expected explicit node %s in arango offline snapshot", explicitNode.NodeID)
	}
}

func TestManagedAndExplicitAdaptersBehaviorParityInSameScenario(t *testing.T) {
	redisRepo, redisCleanup := setupRedisCollabRepo(t)
	defer redisCleanup()

	postgresRepo, postgresCleanup := setupPostgresRepoStrict(t)
	defer postgresCleanup()

	arangoRepo, arangoCleanup := setupArangoCollabRepoWithNamespace(t, "monitor_tree_parity")
	defer arangoCleanup()

	managementFeatures, ok := redisRepo.GetRedisManagementFeatures()
	if !ok || managementFeatures == nil {
		t.Fatalf("expected redis management features, got ok=%v", ok)
	}
	streamFeatures, ok := redisRepo.GetRedisStreamFeatures()
	if !ok || streamFeatures == nil {
		t.Fatalf("expected redis stream features, got ok=%v", ok)
	}
	arangoAdapter, ok := arangoRepo.GetAdapter().(*db.ArangoAdapter)
	if !ok || arangoAdapter == nil {
		t.Fatalf("expected arango adapter, got %T", arangoRepo.GetAdapter())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := arangoAdapter.EnsureCollaborationLedgerCollections(ctx); err != nil {
		t.Fatalf("ensure arango collections failed: %v", err)
	}

	now := time.Now().UnixNano()
	namespace := "monitor_tree_parity"
	group := fmt.Sprintf("group-parity-%d", now)

	testCases := []db.CollaborationAdapterNodePresence{
		{
			NodeID:       fmt.Sprintf("managed-%d", now),
			AdapterType:  "redis",
			AdapterID:    "managed-default",
			Group:        group,
			Namespace:    namespace,
			Capabilities: []string{"stream", "pubsub", "monitor"},
		},
		{
			NodeID:       fmt.Sprintf("explicit-%d", now),
			AdapterType:  "redis",
			AdapterID:    "user-explicit",
			Group:        group,
			Namespace:    namespace,
			Capabilities: []string{"stream", "pubsub"},
		},
	}

	for i, node := range testCases {
		if err := managementFeatures.RegisterAdapterNode(ctx, &node, 30*time.Second); err != nil {
			t.Fatalf("register node failed (idx=%d): %v", i, err)
		}
		if err := managementFeatures.HeartbeatAdapterNode(ctx, namespace, group, node.NodeID, 30*time.Second); err != nil {
			t.Fatalf("heartbeat node failed (idx=%d): %v", i, err)
		}

		nodes, err := managementFeatures.ListGroupNodes(ctx, namespace, group)
		if err != nil {
			t.Fatalf("list group nodes failed (idx=%d): %v", i, err)
		}
		found := false
		for _, n := range nodes {
			if n.NodeID == node.NodeID {
				found = true
				p := n
				if p.AdapterType == "" {
					p.AdapterType = node.AdapterType
				}
				if p.AdapterID == "" {
					p.AdapterID = node.AdapterID
				}
				if len(p.Capabilities) == 0 {
					p.Capabilities = append([]string(nil), node.Capabilities...)
				}
				if err := arangoAdapter.UpsertOnlineAdapterPresence(ctx, &p); err != nil {
					t.Fatalf("upsert online presence failed (idx=%d): %v", i, err)
				}
				break
			}
		}
		if !found {
			t.Fatalf("expected node in group snapshot (idx=%d, node=%s)", i, node.NodeID)
		}

		requestStream := fmt.Sprintf("collab:parity:%d:request:%d", i, now)
		responseStream := fmt.Sprintf("collab:parity:%d:response:%d", i, now)
		requestID := fmt.Sprintf("req-parity-%d-%d", i, now)
		mustRunRedisPostgresRoundTripOnce(t, streamFeatures, postgresRepo, requestStream, responseStream, requestID)

		onlineRows, err := arangoAdapter.QueryOnlineAdapterNodes(ctx, "online", 100)
		if err != nil {
			t.Fatalf("query online nodes failed (idx=%d): %v", i, err)
		}
		if !containsNodeKey(onlineRows, node.NodeID) {
			t.Fatalf("expected node %s in online snapshot", node.NodeID)
		}

		if err := managementFeatures.MarkAdapterOffline(ctx, namespace, group, node.NodeID); err != nil {
			t.Fatalf("mark node offline failed (idx=%d): %v", i, err)
		}
		if err := arangoAdapter.UpsertOnlineAdapterPresence(ctx, &db.CollaborationAdapterNodePresence{
			NodeID:              node.NodeID,
			AdapterType:         node.AdapterType,
			AdapterID:           node.AdapterID,
			Group:               group,
			Namespace:           namespace,
			Status:              "offline",
			LastHeartbeatUnixMs: time.Now().UnixMilli(),
			Capabilities:        node.Capabilities,
		}); err != nil {
			t.Fatalf("project offline state failed (idx=%d): %v", i, err)
		}

		offlineRows, err := arangoAdapter.QueryOnlineAdapterNodes(ctx, "offline", 100)
		if err != nil {
			t.Fatalf("query offline nodes failed (idx=%d): %v", i, err)
		}
		if !containsNodeKey(offlineRows, node.NodeID) {
			t.Fatalf("expected node %s in offline snapshot", node.NodeID)
		}
	}
}

func TestManagedAndExplicitAdaptersTTLAutoCleanupParity(t *testing.T) {
	redisRepo, redisCleanup := setupRedisCollabRepo(t)
	defer redisCleanup()

	managementFeatures, ok := redisRepo.GetRedisManagementFeatures()
	if !ok || managementFeatures == nil {
		t.Fatalf("expected redis management features, got ok=%v", ok)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	now := time.Now().UnixNano()
	namespace := "monitor_tree_ttl"
	group := fmt.Sprintf("group-ttl-%d", now)

	managed := db.CollaborationAdapterNodePresence{
		NodeID:       fmt.Sprintf("managed-ttl-%d", now),
		AdapterType:  "redis",
		AdapterID:    "managed-default",
		Group:        group,
		Namespace:    namespace,
		Capabilities: []string{"stream", "monitor"},
	}
	explicit := db.CollaborationAdapterNodePresence{
		NodeID:       fmt.Sprintf("explicit-ttl-%d", now),
		AdapterType:  "redis",
		AdapterID:    "user-explicit",
		Group:        group,
		Namespace:    namespace,
		Capabilities: []string{"stream"},
	}

	if err := managementFeatures.RegisterAdapterNode(ctx, &managed, 1*time.Second); err != nil {
		t.Fatalf("register managed node failed: %v", err)
	}
	if err := managementFeatures.RegisterAdapterNode(ctx, &explicit, 1*time.Second); err != nil {
		t.Fatalf("register explicit node failed: %v", err)
	}

	nodes, err := managementFeatures.ListGroupNodes(ctx, namespace, group)
	if err != nil {
		t.Fatalf("list initial nodes failed: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes before ttl expiry, got %d", len(nodes))
	}

	time.Sleep(2200 * time.Millisecond)
	nodes, err = managementFeatures.ListGroupNodes(ctx, namespace, group)
	if err != nil {
		t.Fatalf("list nodes after ttl expiry failed: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected both managed and explicit nodes cleaned after ttl expiry, got %d", len(nodes))
	}
}

func containsNodeKey(rows []map[string]interface{}, key string) bool {
	for _, row := range rows {
		if rowKey, _ := row["_key"].(string); rowKey == key {
			return true
		}
	}
	return false
}
