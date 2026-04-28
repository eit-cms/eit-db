package db

import (
	"context"
	"testing"
	"time"
)

func TestCollaborationRuntimeConfig_Defaults(t *testing.T) {
	cfg := CollaborationRuntimeConfig{}.withDefaults()
	if cfg.HeartbeatInterval != 15*time.Second {
		t.Errorf("expected HeartbeatInterval=15s, got %v", cfg.HeartbeatInterval)
	}
	if cfg.NodeTTL != 45*time.Second {
		t.Errorf("expected NodeTTL=45s, got %v", cfg.NodeTTL)
	}
	if cfg.StopTimeout != 5*time.Second {
		t.Errorf("expected StopTimeout=5s, got %v", cfg.StopTimeout)
	}
}

func TestCollaborationRuntimeConfig_CustomValues(t *testing.T) {
	cfg := CollaborationRuntimeConfig{
		HeartbeatInterval: 30 * time.Second,
		NodeTTL:           90 * time.Second,
		StopTimeout:       10 * time.Second,
	}.withDefaults()
	if cfg.HeartbeatInterval != 30*time.Second {
		t.Errorf("expected 30s, got %v", cfg.HeartbeatInterval)
	}
	if cfg.NodeTTL != 90*time.Second {
		t.Errorf("expected 90s, got %v", cfg.NodeTTL)
	}
	if cfg.StopTimeout != 10*time.Second {
		t.Errorf("expected 10s, got %v", cfg.StopTimeout)
	}
}

func TestCollaborationRuntime_NilManagement(t *testing.T) {
	rt := NewCollaborationRuntime(nil, CollaborationAdapterNodePresence{
		NodeID: "n1",
		Group:  "g1",
	}, CollaborationRuntimeConfig{})

	_, err := rt.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when management features are nil")
	}
}

func TestCollaborationRuntime_InvalidNode(t *testing.T) {
	// We don't have a real Redis adapter here, but validatePresenceNode fires before
	// any network call, so passing an invalid node should return early with an error.
	rt := &CollaborationRuntime{
		mgmt: &RedisManagementFeatures{adapter: nil}, // adapter nil → call will fail anyway
		node: CollaborationAdapterNodePresence{
			NodeID: "", // invalid
			Group:  "g1",
		},
		cfg: CollaborationRuntimeConfig{}.withDefaults(),
	}
	_, err := rt.Start(context.Background())
	if err == nil {
		t.Fatal("expected error for empty NodeID")
	}
}

func TestCollaborationRuntime_StopBeforeStart(t *testing.T) {
	rt := NewCollaborationRuntime(nil, CollaborationAdapterNodePresence{
		NodeID: "n1",
		Group:  "g1",
	}, CollaborationRuntimeConfig{})

	// Stop before Start must not panic.
	if err := rt.Stop(context.Background()); err != nil {
		t.Errorf("unexpected error on Stop before Start: %v", err)
	}
}

func TestCollaborationRuntime_IsRunning(t *testing.T) {
	rt := NewCollaborationRuntime(nil, CollaborationAdapterNodePresence{
		NodeID: "n1",
		Group:  "g1",
	}, CollaborationRuntimeConfig{})

	if rt.IsRunning() {
		t.Fatal("expected IsRunning=false before Start")
	}
	if rt.Report() != nil {
		t.Fatal("expected nil Report before Start")
	}
}

func TestCollaborationRuntime_IdempotentStop(t *testing.T) {
	rt := NewCollaborationRuntime(nil, CollaborationAdapterNodePresence{
		NodeID: "n1",
		Group:  "g1",
	}, CollaborationRuntimeConfig{})

	// Multiple Stop calls must not panic.
	_ = rt.Stop(context.Background())
	_ = rt.Stop(context.Background())
}
