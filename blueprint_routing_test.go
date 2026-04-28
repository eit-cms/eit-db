package db

import (
	"testing"
)

func TestHRWSelectNode_Deterministic(t *testing.T) {
	candidates := []string{"node-a", "node-b", "node-c", "node-d"}
	requestKey := "req-1001"
	blueprintTick := "bp-v3"
	routeTick := "rt-snapshot-7"

	first, err := HRWSelectNode(requestKey, blueprintTick, routeTick, SchedulerPolicyHRWV1, candidates)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first == "" {
		t.Fatal("expected non-empty node id")
	}

	// Same inputs must always produce same output.
	for i := 0; i < 100; i++ {
		got, err := HRWSelectNode(requestKey, blueprintTick, routeTick, SchedulerPolicyHRWV1, candidates)
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
		if got != first {
			t.Fatalf("iteration %d: want %q, got %q (non-deterministic)", i, first, got)
		}
	}
}

func TestHRWSelectNode_StabilityOnRouteTick(t *testing.T) {
	// route_tick 变化时结果可以不同，但 route_tick 固定时结果必须稳定。
	candidates := []string{"alpha", "beta", "gamma"}
	requestKey := "req-stable"
	blueprintTick := "bp-1"

	got1, _ := HRWSelectNode(requestKey, blueprintTick, "tick-A", SchedulerPolicyHRWV1, candidates)
	got2, _ := HRWSelectNode(requestKey, blueprintTick, "tick-A", SchedulerPolicyHRWV1, candidates)
	if got1 != got2 {
		t.Fatalf("same route_tick must yield stable result: got %q and %q", got1, got2)
	}

	// Different route_tick may (but is not required to) differ.
	got3, _ := HRWSelectNode(requestKey, blueprintTick, "tick-B", SchedulerPolicyHRWV1, candidates)
	_ = got3 // acceptable if equal or different
}

func TestHRWSelectNode_EmptyCandidates(t *testing.T) {
	_, err := HRWSelectNode("req", "bp", "rt", SchedulerPolicyHRWV1, nil)
	if err == nil {
		t.Fatal("expected error for empty candidates")
	}
	_, err = HRWSelectNode("req", "bp", "rt", SchedulerPolicyHRWV1, []string{})
	if err == nil {
		t.Fatal("expected error for empty candidates slice")
	}
	_, err = HRWSelectNode("req", "bp", "rt", SchedulerPolicyHRWV1, []string{"", "  "})
	if err == nil {
		t.Fatal("expected error for all-empty candidate ids")
	}
}

func TestHRWSelectNode_DefaultPolicy(t *testing.T) {
	// 空 schedulerPolicy 应等价于 hrw_v1。
	candidates := []string{"node-x", "node-y", "node-z"}
	withExplicit, err1 := HRWSelectNode("req", "bp", "rt", SchedulerPolicyHRWV1, candidates)
	withEmpty, err2 := HRWSelectNode("req", "bp", "rt", "", candidates)
	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected errors: %v / %v", err1, err2)
	}
	if withExplicit != withEmpty {
		t.Fatalf("empty policy should default to hrw_v1: explicit=%q empty=%q", withExplicit, withEmpty)
	}
}

func TestHRWRankedCandidates_Order(t *testing.T) {
	candidates := []string{"n1", "n2", "n3", "n4", "n5"}
	ranked, err := HRWRankedCandidates("req", "bp", "rt", SchedulerPolicyHRWV1, candidates)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ranked) != len(candidates) {
		t.Fatalf("expected %d ranked nodes, got %d", len(candidates), len(ranked))
	}

	// First element must match HRWSelectNode result.
	selected, _ := HRWSelectNode("req", "bp", "rt", SchedulerPolicyHRWV1, candidates)
	if ranked[0] != selected {
		t.Fatalf("ranked[0]=%q must equal HRWSelectNode result=%q", ranked[0], selected)
	}

	// No duplicates.
	seen := make(map[string]bool)
	for _, n := range ranked {
		if seen[n] {
			t.Fatalf("duplicate node in ranked list: %q", n)
		}
		seen[n] = true
	}
}

func TestHRWRouteToOnlineNode_SkipsOffline(t *testing.T) {
	nodes := []CollaborationAdapterNodePresence{
		{NodeID: "node-1", Status: "online"},
		{NodeID: "node-2", Status: "offline"},
		{NodeID: "node-3", Status: "online"},
	}
	selected, err := HRWRouteToOnlineNode("req-x", "bp-1", "rt-1", nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected == "node-2" {
		t.Fatal("offline node must not be selected")
	}
}

func TestHRWRouteToOnlineNode_NoOnlineNodes(t *testing.T) {
	nodes := []CollaborationAdapterNodePresence{
		{NodeID: "node-1", Status: "offline"},
		{NodeID: "node-2", Status: "offline"},
	}
	_, err := HRWRouteToOnlineNode("req-x", "bp-1", "rt-1", nodes)
	if err == nil {
		t.Fatal("expected error when no online nodes available")
	}
}

func TestHRWRouteToOnlineNode_EmptyStatusTreatedAsOnline(t *testing.T) {
	nodes := []CollaborationAdapterNodePresence{
		{NodeID: "node-1", Status: ""},
	}
	selected, err := HRWRouteToOnlineNode("req", "bp", "rt", nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected != "node-1" {
		t.Fatalf("expected node-1, got %q", selected)
	}
}

func TestHRWRouteToOnlineNodeWithFallback_FallsBackOnOffline(t *testing.T) {
	// Determine the HRW primary node first.
	candidates := []string{"node-a", "node-b", "node-c"}
	ranked, err := HRWRankedCandidates("req-fb", "bp", "rt", SchedulerPolicyHRWV1, candidates)
	if err != nil {
		t.Fatalf("ranked: %v", err)
	}
	primary := ranked[0]
	secondary := ranked[1]

	// Mark primary offline, others online.
	nodes := make([]CollaborationAdapterNodePresence, len(candidates))
	for i, id := range candidates {
		status := "online"
		if id == primary {
			status = "offline"
		}
		nodes[i] = CollaborationAdapterNodePresence{NodeID: id, Status: status}
	}

	selected, _, err := HRWRouteToOnlineNodeWithFallback("req-fb", "bp", "rt", nodes)
	if err != nil {
		t.Fatalf("fallback: %v", err)
	}
	if selected == primary {
		t.Fatalf("offline primary should not be selected; got %q", selected)
	}
	if selected != secondary {
		t.Fatalf("expected fallback to secondary=%q, got %q", secondary, selected)
	}
}

func TestHRWRouteToOnlineNodeWithFallback_AllOffline(t *testing.T) {
	nodes := []CollaborationAdapterNodePresence{
		{NodeID: "node-1", Status: "offline"},
		{NodeID: "node-2", Status: "offline"},
	}
	_, _, err := HRWRouteToOnlineNodeWithFallback("req", "bp", "rt", nodes)
	if err == nil {
		t.Fatal("expected error when all nodes offline")
	}
}

func TestHRWSelectNode_DistributionSanity(t *testing.T) {
	// With enough distinct request keys, all nodes should be selected at least once
	// (basic load distribution sanity check — not a strict uniformity test).
	candidates := []string{"n1", "n2", "n3", "n4"}
	counts := make(map[string]int)
	for i := 0; i < 200; i++ {
		key := "req-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26%10))
		node, err := HRWSelectNode(key, "bp", "rt", SchedulerPolicyHRWV1, candidates)
		if err != nil {
			t.Fatalf("unexpected error at i=%d: %v", i, err)
		}
		counts[node]++
	}
	for _, c := range candidates {
		if counts[c] == 0 {
			t.Errorf("node %q was never selected across 200 distinct request keys (poor distribution)", c)
		}
	}
}
