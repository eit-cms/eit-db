package db

import "testing"

func TestEvaluateRedisL2IsolationClusterMode(t *testing.T) {
	report := EvaluateRedisL2Isolation(&RedisConnectionConfig{ClusterMode: true, ClusterAddrs: []string{"127.0.0.1:7000"}}, "")
	if !report.Allowed {
		t.Fatalf("expected isolation allowed for cluster mode, got %+v", report)
	}
	if report.Mode != "cluster" {
		t.Fatalf("expected cluster mode, got %s", report.Mode)
	}
}

func TestEvaluateRedisL2IsolationStandaloneNonDefaultDB(t *testing.T) {
	report := EvaluateRedisL2Isolation(&RedisConnectionConfig{Host: "localhost", Port: 6379, DB: 2}, "")
	if !report.Allowed {
		t.Fatalf("expected isolation allowed for non-default db, got %+v", report)
	}
	if report.Mode != "standalone_db" {
		t.Fatalf("expected standalone_db mode, got %s", report.Mode)
	}
}

func TestEvaluateRedisL2IsolationStandaloneNamespace(t *testing.T) {
	report := EvaluateRedisL2Isolation(&RedisConnectionConfig{Host: "localhost", Port: 6379, DB: 0}, "eit:page")
	if !report.Allowed {
		t.Fatalf("expected isolation allowed for namespace mode, got %+v", report)
	}
	if report.Mode != "standalone_namespace" {
		t.Fatalf("expected standalone_namespace mode, got %s", report.Mode)
	}
}

func TestEvaluateRedisL2IsolationRejectDefaultStandalone(t *testing.T) {
	report := EvaluateRedisL2Isolation(&RedisConnectionConfig{Host: "localhost", Port: 6379, DB: 0}, "")
	if report.Allowed {
		t.Fatalf("expected isolation rejected for default standalone, got %+v", report)
	}
}
