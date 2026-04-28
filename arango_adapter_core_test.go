package db

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newArangoTestConfig(uri string) *Config {
	return &Config{
		Adapter: "arango",
		Arango: &ArangoConnectionConfig{
			URI:      uri,
			Database: "_system",
			Username: "root",
			Password: "secret",
		},
	}
}

func TestArangoAdapterCoreConnectPingClose(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_api/version" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"version":"3.11.0"}`))
	}))
	defer ts.Close()

	cfg := newArangoTestConfig(ts.URL)
	adapter, err := NewArangoAdapter(cfg)
	if err != nil {
		t.Fatalf("NewArangoAdapter failed: %v", err)
	}

	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	if err := adapter.Ping(context.Background()); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	if adapter.GetRawConn() == nil {
		t.Fatalf("GetRawConn should return non-nil client after Connect")
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if adapter.GetRawConn() != nil {
		t.Fatalf("GetRawConn should return nil after Close")
	}
}

func TestArangoAdapterConnectFailsWhenPingFails(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("unauthorized"))
	}))
	defer ts.Close()

	cfg := newArangoTestConfig(ts.URL)
	adapter, err := NewArangoAdapter(cfg)
	if err != nil {
		t.Fatalf("NewArangoAdapter failed: %v", err)
	}

	err = adapter.Connect(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected Connect to fail when ping endpoint returns error")
	}
	if adapter.GetRawConn() != nil {
		t.Fatalf("client should be cleared after failed Connect")
	}
}

func TestArangoAdapterExecuteAQLSuccess(t *testing.T) {
	expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("root:secret"))
	versionCalled := false
	cursorCalled := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_api/version":
			versionCalled = true
			if got := r.Header.Get("Authorization"); got != expectedAuth {
				t.Fatalf("unexpected Authorization header: %q", got)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"version":"3.11.0"}`))
		case "/_db/_system/_api/cursor":
			cursorCalled = true
			if r.Method != http.MethodPost {
				t.Fatalf("expected POST method, got %s", r.Method)
			}
			if got := r.Header.Get("Authorization"); got != expectedAuth {
				t.Fatalf("unexpected Authorization header: %q", got)
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("failed to read body: %v", err)
			}
			var payload map[string]interface{}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("invalid JSON payload: %v", err)
			}
			if payload["query"] != "FOR d IN docs RETURN d" {
				t.Fatalf("unexpected query payload: %#v", payload)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"error":false,"hasMore":false,"result":[{"name":"alice"}]}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer ts.Close()

	cfg := newArangoTestConfig(ts.URL)
	adapter, err := NewArangoAdapter(cfg)
	if err != nil {
		t.Fatalf("NewArangoAdapter failed: %v", err)
	}
	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	rows, err := adapter.ExecuteAQL(context.Background(), "FOR d IN docs RETURN d", map[string]interface{}{"k": "v"})
	if err != nil {
		t.Fatalf("ExecuteAQL failed: %v", err)
	}
	if !versionCalled || !cursorCalled {
		t.Fatalf("expected both version and cursor endpoints to be called")
	}
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows))
	}
	if got, ok := rows[0]["name"].(string); !ok || got != "alice" {
		t.Fatalf("unexpected row content: %#v", rows[0])
	}
}

func TestArangoAdapterExecuteAQLHasMoreNotSupported(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_api/version":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"version":"3.11.0"}`))
		case "/_db/_system/_api/cursor":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"error":false,"hasMore":true,"result":[{"id":1}]}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer ts.Close()

	cfg := newArangoTestConfig(ts.URL)
	adapter, err := NewArangoAdapter(cfg)
	if err != nil {
		t.Fatalf("NewArangoAdapter failed: %v", err)
	}
	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	_, err = adapter.ExecuteAQL(context.Background(), "FOR d IN docs RETURN d", nil)
	if err == nil || !strings.Contains(err.Error(), "cursor pagination is not supported") {
		t.Fatalf("expected hasMore unsupported error, got: %v", err)
	}
}

func TestArangoAdapterCoreFallbackInterfaces(t *testing.T) {
	adapter := &ArangoAdapter{}

	if _, err := adapter.Query(context.Background(), "SELECT 1"); err == nil {
		t.Fatalf("Query should be unsupported")
	}
	if row := adapter.QueryRow(context.Background(), "SELECT 1"); row != nil {
		t.Fatalf("QueryRow should return nil")
	}
	if _, err := adapter.Exec(context.Background(), "UPDATE t SET v=1"); err == nil {
		t.Fatalf("Exec should be unsupported")
	}
	if _, err := adapter.Begin(context.Background()); err == nil {
		t.Fatalf("Begin should be unsupported in MVP")
	}
	if err := adapter.RegisterScheduledTask(context.Background(), &ScheduledTaskConfig{Name: "job"}); err == nil {
		t.Fatalf("RegisterScheduledTask should fallback with error")
	}
	if err := adapter.UnregisterScheduledTask(context.Background(), "job"); err == nil {
		t.Fatalf("UnregisterScheduledTask should fallback with error")
	}
	if _, err := adapter.ListScheduledTasks(context.Background()); err == nil {
		t.Fatalf("ListScheduledTasks should fallback with error")
	}
	if adapter.GetQueryBuilderProvider() != nil {
		t.Fatalf("GetQueryBuilderProvider should be nil in MVP")
	}
	if adapter.GetDatabaseFeatures() == nil {
		t.Fatalf("GetDatabaseFeatures should not be nil")
	}
	if adapter.GetQueryFeatures() == nil {
		t.Fatalf("GetQueryFeatures should not be nil")
	}
}
