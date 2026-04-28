package db

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDefaultArangoLedgerCollectionsWithNamespace(t *testing.T) {
	collections := DefaultArangoLedgerCollections("Collab-NS@A")
	if !strings.HasPrefix(collections.OnlineAdapterNode, "collab_ns_a__") {
		t.Fatalf("unexpected ledger collection prefix: %s", collections.OnlineAdapterNode)
	}
	if !strings.HasPrefix(collections.EmitsEdge, "collab_ns_a__") {
		t.Fatalf("unexpected ledger edge prefix: %s", collections.EmitsEdge)
	}
}

func TestArangoAdapterEnsureCollaborationLedgerCollections(t *testing.T) {
	collectionCreateCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/_api/version":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"version":"3.12.0"}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_api/collection"):
			collectionCreateCount++
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"error":false}`))
		default:
			t.Fatalf("unexpected request path: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	cfg := &Config{Adapter: "arango", Arango: &ArangoConnectionConfig{URI: ts.URL, Database: "_system", Username: "root"}}
	adapter, err := NewArangoAdapter(cfg)
	if err != nil {
		t.Fatalf("new arango adapter failed: %v", err)
	}
	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	if err := adapter.EnsureCollaborationLedgerCollections(context.Background()); err != nil {
		t.Fatalf("ensure collaboration ledger collections failed: %v", err)
	}
	if collectionCreateCount != 10 {
		t.Fatalf("expected 10 collection create calls, got %d", collectionCreateCount)
	}
}

func TestArangoAdapterRecordCollaborationEnvelopeToLedger(t *testing.T) {
	querySeen := make([]string, 0)
	collectionCreateCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/_api/version":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"version":"3.12.0"}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_api/collection"):
			collectionCreateCount++
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"error":false}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_api/cursor"):
			body, _ := io.ReadAll(r.Body)
			var payload map[string]interface{}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("invalid cursor payload: %v", err)
			}
			query, _ := payload["query"].(string)
			querySeen = append(querySeen, query)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"error":false,"hasMore":false,"result":[{}]}`))
		default:
			t.Fatalf("unexpected request path: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	cfg := &Config{Adapter: "arango", Arango: &ArangoConnectionConfig{URI: ts.URL, Database: "_system", Username: "root", Namespace: "collab"}}
	adapter, err := NewArangoAdapter(cfg)
	if err != nil {
		t.Fatalf("new arango adapter failed: %v", err)
	}
	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	envelope := &CollaborationMessageEnvelope{
		MessageID:      "msg-1",
		RequestID:      "req-1",
		TraceID:        "trace-1",
		SenderNodeID:   "sender-1",
		ReceiverNodeID: "receiver-1",
		EventType:      "query.requested",
		Stream:         "collab:stream:request",
		TicksSent:      1,
		SentAtUnixMs:   1700000000000,
	}
	if err := adapter.RecordCollaborationEnvelopeToLedger(context.Background(), envelope); err != nil {
		t.Fatalf("record collaboration envelope failed: %v", err)
	}
	if collectionCreateCount != 10 {
		t.Fatalf("expected 10 collection ensure calls, got %d", collectionCreateCount)
	}
	if len(querySeen) < 6 {
		t.Fatalf("expected at least 6 AQL writes, got %d", len(querySeen))
	}
}

func TestArangoAdapterQueryLedgerDeliveryPath(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/_api/version":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"version":"3.12.0"}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_api/cursor"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"error":false,"hasMore":false,"result":[{"message":{"_key":"msg-1"},"sender":{"_key":"sender-1"},"receiver":{"_key":"receiver-1"}}]}`))
		default:
			t.Fatalf("unexpected request path: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	cfg := &Config{Adapter: "arango", Arango: &ArangoConnectionConfig{URI: ts.URL, Database: "_system", Username: "root", Namespace: "collab"}}
	adapter, err := NewArangoAdapter(cfg)
	if err != nil {
		t.Fatalf("new arango adapter failed: %v", err)
	}
	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	rows, err := adapter.QueryLedgerDeliveryPath(context.Background(), "req-1", 10)
	if err != nil {
		t.Fatalf("query ledger delivery path failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one ledger path row, got %d", len(rows))
	}
}

func TestArangoAdapterQueryReplaySessionMessages(t *testing.T) {
	var seenQuery string
	var seenBind map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/_api/version":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"version":"3.12.0"}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_api/cursor"):
			body, _ := io.ReadAll(r.Body)
			var payload map[string]interface{}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("invalid cursor payload: %v", err)
			}
			seenQuery, _ = payload["query"].(string)
			seenBind, _ = payload["bindVars"].(map[string]interface{})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"error":false,"hasMore":false,"result":[{"session_id":"sess-1","seq":1}]}`))
		default:
			t.Fatalf("unexpected request path: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	cfg := &Config{Adapter: "arango", Arango: &ArangoConnectionConfig{URI: ts.URL, Database: "_system", Username: "root", Namespace: "collab"}}
	adapter, err := NewArangoAdapter(cfg)
	if err != nil {
		t.Fatalf("new arango adapter failed: %v", err)
	}
	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	rows, err := adapter.QueryReplaySessionMessages(context.Background(), "sess-1", 2, 20)
	if err != nil {
		t.Fatalf("query replay session messages failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows))
	}
	if !strings.Contains(seenQuery, "session_replays_message") {
		t.Fatalf("expected query to contain session_replays_message, got: %s", seenQuery)
	}
	if seenBind["sessionId"] != "sess-1" {
		t.Fatalf("unexpected bindVars sessionId: %#v", seenBind["sessionId"])
	}
}

func TestArangoAdapterQueryReplayMessagesFromCheckpoint(t *testing.T) {
	var seenBind map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/_api/version":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"version":"3.12.0"}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_api/cursor"):
			body, _ := io.ReadAll(r.Body)
			var payload map[string]interface{}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("invalid cursor payload: %v", err)
			}
			seenBind, _ = payload["bindVars"].(map[string]interface{})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"error":false,"hasMore":false,"result":[{"checkpoint_id":"cp-1","seq":3}]}`))
		default:
			t.Fatalf("unexpected request path: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	cfg := &Config{Adapter: "arango", Arango: &ArangoConnectionConfig{URI: ts.URL, Database: "_system", Username: "root", Namespace: "collab"}}
	adapter, err := NewArangoAdapter(cfg)
	if err != nil {
		t.Fatalf("new arango adapter failed: %v", err)
	}
	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	rows, err := adapter.QueryReplayMessagesFromCheckpoint(context.Background(), "cp-1", false, 10)
	if err != nil {
		t.Fatalf("query replay messages from checkpoint failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows))
	}
	if seenBind["checkpointCollection"] != "collab__replay_checkpoint_node" {
		t.Fatalf("unexpected bindVars checkpointCollection: %#v", seenBind["checkpointCollection"])
	}
	if seenBind["checkpointId"] != "cp-1" {
		t.Fatalf("unexpected bindVars checkpointId: %#v", seenBind["checkpointId"])
	}
}

func TestArangoAdapterQueryReplaySessionCheckpoints(t *testing.T) {
	var seenQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/_api/version":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"version":"3.12.0"}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_api/cursor"):
			body, _ := io.ReadAll(r.Body)
			var payload map[string]interface{}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("invalid cursor payload: %v", err)
			}
			seenQuery, _ = payload["query"].(string)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"error":false,"hasMore":false,"result":[{"session_id":"sess-1"}]}`))
		default:
			t.Fatalf("unexpected request path: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	cfg := &Config{Adapter: "arango", Arango: &ArangoConnectionConfig{URI: ts.URL, Database: "_system", Username: "root", Namespace: "collab"}}
	adapter, err := NewArangoAdapter(cfg)
	if err != nil {
		t.Fatalf("new arango adapter failed: %v", err)
	}
	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	rows, err := adapter.QueryReplaySessionCheckpoints(context.Background(), "sess-1", 10)
	if err != nil {
		t.Fatalf("query replay session checkpoints failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows))
	}
	if !strings.Contains(seenQuery, "session_has_checkpoint") {
		t.Fatalf("expected query to contain session_has_checkpoint, got: %s", seenQuery)
	}
}
