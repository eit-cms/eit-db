package db

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

var arangoLedgerNameCleaner = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

// ArangoLedgerCollections 协作账本图使用的集合命名。
type ArangoLedgerCollections struct {
	OnlineAdapterNode     string
	RequestNode           string
	MessageNode           string
	ReplaySessionNode     string
	ReplayCheckpointNode  string
	EmitsEdge             string
	DeliversToEdge        string
	BelongsToRequest      string
	SessionReplaysMessage string
	SessionHasCheckpoint  string
}

// ArangoOnlineAdapterNode 在线适配器节点。
type ArangoOnlineAdapterNode struct {
	NodeID        string
	AdapterType   string
	AdapterID     string
	Capabilities  []string
	Groups        []string
	Status        string
	HeartbeatUnix int64
	DatasetNS     string
}

// ArangoRequestNode 请求节点。
type ArangoRequestNode struct {
	RequestID     string
	OriginAdapter string
	Intent        string
	CreatedAtUnix int64
	TraceID       string
	Status        string
}

// ArangoMessageNode 消息节点。
type ArangoMessageNode struct {
	MessageID       string
	RequestID       string
	EventType       string
	Stream          string
	TicksSent       int64
	TicksConsumed   int64
	RetryCount      int32
	SentAtUnixMs    int64
	DeliveredAtUnix int64
	Status          string
}

// ArangoReplaySessionNode 回放会话节点。
type ArangoReplaySessionNode struct {
	SessionID       string
	RequestID       string
	Mode            string
	PlannedBy       string
	DLQStream       string
	TargetStream    string
	Namespace       string
	Group           string
	Status          string
	StartedAtUnixMs int64
	EndedAtUnixMs   int64
}

// ArangoReplayCheckpointNode 回放断点节点。
type ArangoReplayCheckpointNode struct {
	CheckpointID    string
	SessionID       string
	AnchorType      string
	AnchorValue     string
	Tick            string
	Cursor          string
	Status          string
	CreatedAtUnixMs int64
}

// DefaultArangoLedgerCollections 返回协作账本集合命名。
func DefaultArangoLedgerCollections(namespace string) ArangoLedgerCollections {
	prefix := sanitizeArangoLedgerPrefix(namespace)
	name := func(base string) string {
		if prefix == "" {
			return base
		}
		return prefix + "__" + base
	}
	return ArangoLedgerCollections{
		OnlineAdapterNode:     name("online_adapter_node"),
		RequestNode:           name("request_node"),
		MessageNode:           name("message_node"),
		ReplaySessionNode:     name("replay_session_node"),
		ReplayCheckpointNode:  name("replay_checkpoint_node"),
		EmitsEdge:             name("emits"),
		DeliversToEdge:        name("delivers_to"),
		BelongsToRequest:      name("belongs_to_request"),
		SessionReplaysMessage: name("session_replays_message"),
		SessionHasCheckpoint:  name("session_has_checkpoint"),
	}
}

func sanitizeArangoLedgerPrefix(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return ""
	}
	namespace = arangoLedgerNameCleaner.ReplaceAllString(namespace, "_")
	namespace = strings.Trim(namespace, "_")
	if namespace == "" {
		return ""
	}
	return strings.ToLower(namespace)
}

func sanitizeArangoCollectionName(name string) string {
	name = strings.TrimSpace(name)
	name = arangoLedgerNameCleaner.ReplaceAllString(name, "_")
	name = strings.Trim(name, "_")
	if name == "" {
		return "collab_unknown"
	}
	return strings.ToLower(name)
}

func validateArangoLedgerNodeKey(key string, kind string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("arango ledger %s key must not be empty", kind)
	}
	return nil
}

func (a *ArangoAdapter) ledgerCollections() ArangoLedgerCollections {
	if a == nil || a.config == nil {
		return DefaultArangoLedgerCollections("")
	}
	return DefaultArangoLedgerCollections(a.config.Namespace)
}

// EnsureCollaborationLedgerCollections 预创建账本节点/边集合。
//
// 说明：Arango 集合创建依赖 HTTP collection API；写入与查询仍使用 AQL。
func (a *ArangoAdapter) EnsureCollaborationLedgerCollections(ctx context.Context) error {
	if a == nil || a.client == nil {
		return fmt.Errorf("arango client not connected")
	}
	collections := a.ledgerCollections()
	if err := a.ensureCollection(ctx, collections.OnlineAdapterNode, 2); err != nil {
		return err
	}
	if err := a.ensureCollection(ctx, collections.RequestNode, 2); err != nil {
		return err
	}
	if err := a.ensureCollection(ctx, collections.MessageNode, 2); err != nil {
		return err
	}
	if err := a.ensureCollection(ctx, collections.ReplaySessionNode, 2); err != nil {
		return err
	}
	if err := a.ensureCollection(ctx, collections.ReplayCheckpointNode, 2); err != nil {
		return err
	}
	if err := a.ensureCollection(ctx, collections.EmitsEdge, 3); err != nil {
		return err
	}
	if err := a.ensureCollection(ctx, collections.DeliversToEdge, 3); err != nil {
		return err
	}
	if err := a.ensureCollection(ctx, collections.BelongsToRequest, 3); err != nil {
		return err
	}
	if err := a.ensureCollection(ctx, collections.SessionReplaysMessage, 3); err != nil {
		return err
	}
	if err := a.ensureCollection(ctx, collections.SessionHasCheckpoint, 3); err != nil {
		return err
	}
	return nil
}

func (a *ArangoAdapter) ensureCollection(ctx context.Context, name string, collectionType int) error {
	name = sanitizeArangoCollectionName(name)
	reqBody := map[string]interface{}{
		"name": name,
		"type": collectionType,
	}
	respBody, statusCode, err := a.requestArangoAPI(ctx, "POST", "/_db/"+a.config.Database+"/_api/collection", reqBody)
	if err != nil {
		return err
	}
	if statusCode == 200 || statusCode == 201 || statusCode == 202 {
		return nil
	}
	if statusCode == 409 {
		return nil
	}
	return fmt.Errorf("arango ensure collection %s failed with status %d: %s", name, statusCode, strings.TrimSpace(respBody))
}

// UpsertLedgerOnlineAdapterNode 写入/更新 online_adapter_node。
func (a *ArangoAdapter) UpsertLedgerOnlineAdapterNode(ctx context.Context, node *ArangoOnlineAdapterNode) error {
	if node == nil {
		return fmt.Errorf("arango ledger online adapter node cannot be nil")
	}
	if err := validateArangoLedgerNodeKey(node.NodeID, "online_adapter_node"); err != nil {
		return err
	}
	collections := a.ledgerCollections()
	_, err := a.ExecuteAQL(ctx, fmt.Sprintf(`
UPSERT { _key: @key }
INSERT MERGE(@doc, { _key: @key })
UPDATE MERGE(OLD, @doc)
IN %s
RETURN NEW
`, collections.OnlineAdapterNode), map[string]interface{}{
		"key": node.NodeID,
		"doc": map[string]interface{}{
			"adapter_type": node.AdapterType,
			"adapter_id":   node.AdapterID,
			"capabilities": node.Capabilities,
			"groups":       node.Groups,
			"status":       node.Status,
			"heartbeat_at": node.HeartbeatUnix,
			"dataset_ns":   node.DatasetNS,
		},
	})
	return err
}

// UpsertOnlineAdapterPresence 将控制面节点状态投影到 online_adapter_node。
func (a *ArangoAdapter) UpsertOnlineAdapterPresence(ctx context.Context, node *CollaborationAdapterNodePresence) error {
	if node == nil {
		return fmt.Errorf("collaboration adapter presence cannot be nil")
	}
	if err := validateArangoLedgerNodeKey(node.NodeID, "online_adapter_node"); err != nil {
		return err
	}
	status := strings.TrimSpace(node.Status)
	if status == "" {
		status = "online"
	}
	groups := make([]string, 0, 1)
	if strings.TrimSpace(node.Group) != "" {
		groups = append(groups, strings.TrimSpace(node.Group))
	}
	return a.UpsertLedgerOnlineAdapterNode(ctx, &ArangoOnlineAdapterNode{
		NodeID:        strings.TrimSpace(node.NodeID),
		AdapterType:   strings.TrimSpace(node.AdapterType),
		AdapterID:     strings.TrimSpace(node.AdapterID),
		Capabilities:  append([]string(nil), node.Capabilities...),
		Groups:        groups,
		Status:        status,
		HeartbeatUnix: node.LastHeartbeatUnixMs,
		DatasetNS:     strings.TrimSpace(node.Namespace),
	})
}

// QueryOnlineAdapterNodes 查询在线适配器节点快照。
func (a *ArangoAdapter) QueryOnlineAdapterNodes(ctx context.Context, status string, limit int) ([]map[string]interface{}, error) {
	if strings.TrimSpace(status) == "" {
		status = "online"
	}
	if limit <= 0 {
		limit = 100
	}
	collections := a.ledgerCollections()
	return a.ExecuteAQL(ctx, fmt.Sprintf(`
FOR node IN %s
	FILTER @status == "*" OR node.status == @status
	SORT node.heartbeat_at DESC
	LIMIT @limit
	RETURN node
`, collections.OnlineAdapterNode), map[string]interface{}{
		"status": status,
		"limit":  limit,
	})
}

// UpsertLedgerRequestNode 写入/更新 request_node。
func (a *ArangoAdapter) UpsertLedgerRequestNode(ctx context.Context, node *ArangoRequestNode) error {
	if node == nil {
		return fmt.Errorf("arango ledger request node cannot be nil")
	}
	if err := validateArangoLedgerNodeKey(node.RequestID, "request_node"); err != nil {
		return err
	}
	collections := a.ledgerCollections()
	_, err := a.ExecuteAQL(ctx, fmt.Sprintf(`
UPSERT { _key: @key }
INSERT MERGE(@doc, { _key: @key })
UPDATE MERGE(OLD, @doc)
IN %s
RETURN NEW
`, collections.RequestNode), map[string]interface{}{
		"key": node.RequestID,
		"doc": map[string]interface{}{
			"origin_adapter": node.OriginAdapter,
			"intent":         node.Intent,
			"created_at":     node.CreatedAtUnix,
			"trace_id":       node.TraceID,
			"status":         node.Status,
		},
	})
	return err
}

// UpsertLedgerMessageNode 写入/更新 message_node。
func (a *ArangoAdapter) UpsertLedgerMessageNode(ctx context.Context, node *ArangoMessageNode) error {
	if node == nil {
		return fmt.Errorf("arango ledger message node cannot be nil")
	}
	if err := validateArangoLedgerNodeKey(node.MessageID, "message_node"); err != nil {
		return err
	}
	collections := a.ledgerCollections()
	_, err := a.ExecuteAQL(ctx, fmt.Sprintf(`
UPSERT { _key: @key }
INSERT MERGE(@doc, { _key: @key })
UPDATE MERGE(OLD, @doc)
IN %s
RETURN NEW
`, collections.MessageNode), map[string]interface{}{
		"key": node.MessageID,
		"doc": map[string]interface{}{
			"request_id":     node.RequestID,
			"event_type":     node.EventType,
			"stream":         node.Stream,
			"ticks_sent":     node.TicksSent,
			"ticks_consumed": node.TicksConsumed,
			"retry_count":    node.RetryCount,
			"sent_at":        node.SentAtUnixMs,
			"delivered_at":   node.DeliveredAtUnix,
			"status":         node.Status,
		},
	})
	return err
}

// UpsertLedgerReplaySessionNode 写入/更新 replay_session_node。
func (a *ArangoAdapter) UpsertLedgerReplaySessionNode(ctx context.Context, node *ArangoReplaySessionNode) error {
	if node == nil {
		return fmt.Errorf("arango ledger replay session node cannot be nil")
	}
	if err := validateArangoLedgerNodeKey(node.SessionID, "replay_session_node"); err != nil {
		return err
	}
	collections := a.ledgerCollections()
	_, err := a.ExecuteAQL(ctx, fmt.Sprintf(`
UPSERT { _key: @key }
INSERT MERGE(@doc, { _key: @key })
UPDATE MERGE(OLD, @doc)
IN %s
RETURN NEW
`, collections.ReplaySessionNode), map[string]interface{}{
		"key": node.SessionID,
		"doc": map[string]interface{}{
			"request_id":    node.RequestID,
			"mode":          node.Mode,
			"planned_by":    node.PlannedBy,
			"dlq_stream":    node.DLQStream,
			"target_stream": node.TargetStream,
			"namespace":     node.Namespace,
			"group":         node.Group,
			"status":        node.Status,
			"started_at":    node.StartedAtUnixMs,
			"ended_at":      node.EndedAtUnixMs,
		},
	})
	return err
}

// UpsertLedgerReplayCheckpointNode 写入/更新 replay_checkpoint_node。
func (a *ArangoAdapter) UpsertLedgerReplayCheckpointNode(ctx context.Context, node *ArangoReplayCheckpointNode) error {
	if node == nil {
		return fmt.Errorf("arango ledger replay checkpoint node cannot be nil")
	}
	if err := validateArangoLedgerNodeKey(node.CheckpointID, "replay_checkpoint_node"); err != nil {
		return err
	}
	collections := a.ledgerCollections()
	_, err := a.ExecuteAQL(ctx, fmt.Sprintf(`
UPSERT { _key: @key }
INSERT MERGE(@doc, { _key: @key })
UPDATE MERGE(OLD, @doc)
IN %s
RETURN NEW
`, collections.ReplayCheckpointNode), map[string]interface{}{
		"key": node.CheckpointID,
		"doc": map[string]interface{}{
			"session_id":   node.SessionID,
			"anchor_type":  node.AnchorType,
			"anchor_value": node.AnchorValue,
			"tick":         node.Tick,
			"cursor":       node.Cursor,
			"status":       node.Status,
			"created_at":   node.CreatedAtUnixMs,
		},
	})
	return err
}

func (a *ArangoAdapter) upsertLedgerEdge(ctx context.Context, edgeCollection string, fromCollection string, fromKey string, toCollection string, toKey string, extra map[string]interface{}) error {
	if err := validateArangoLedgerNodeKey(fromKey, "edge_from"); err != nil {
		return err
	}
	if err := validateArangoLedgerNodeKey(toKey, "edge_to"); err != nil {
		return err
	}
	_, err := a.ExecuteAQL(ctx, fmt.Sprintf(`
LET fromId = CONCAT(@fromCollection, "/", @fromKey)
LET toId = CONCAT(@toCollection, "/", @toKey)
UPSERT { _from: fromId, _to: toId }
INSERT MERGE(@extra, { _from: fromId, _to: toId })
UPDATE MERGE(OLD, @extra)
IN %s
RETURN NEW
`, edgeCollection), map[string]interface{}{
		"fromCollection": sanitizeArangoCollectionName(fromCollection),
		"toCollection":   sanitizeArangoCollectionName(toCollection),
		"fromKey":        fromKey,
		"toKey":          toKey,
		"extra":          extra,
	})
	return err
}

// LinkLedgerEmits 建立 adapter -> message 的 emits 边。
func (a *ArangoAdapter) LinkLedgerEmits(ctx context.Context, senderNodeID string, messageID string, extra map[string]interface{}) error {
	collections := a.ledgerCollections()
	return a.upsertLedgerEdge(ctx, collections.EmitsEdge, collections.OnlineAdapterNode, senderNodeID, collections.MessageNode, messageID, extra)
}

// LinkLedgerDeliversTo 建立 message -> adapter 的 delivers_to 边。
func (a *ArangoAdapter) LinkLedgerDeliversTo(ctx context.Context, messageID string, receiverNodeID string, extra map[string]interface{}) error {
	collections := a.ledgerCollections()
	return a.upsertLedgerEdge(ctx, collections.DeliversToEdge, collections.MessageNode, messageID, collections.OnlineAdapterNode, receiverNodeID, extra)
}

// LinkLedgerBelongsToRequest 建立 message -> request 的 belongs_to_request 边。
func (a *ArangoAdapter) LinkLedgerBelongsToRequest(ctx context.Context, messageID string, requestID string, extra map[string]interface{}) error {
	collections := a.ledgerCollections()
	return a.upsertLedgerEdge(ctx, collections.BelongsToRequest, collections.MessageNode, messageID, collections.RequestNode, requestID, extra)
}

// LinkLedgerSessionReplaysMessage 建立 replay_session -> message 的关系。
func (a *ArangoAdapter) LinkLedgerSessionReplaysMessage(ctx context.Context, sessionID string, messageID string, extra map[string]interface{}) error {
	collections := a.ledgerCollections()
	return a.upsertLedgerEdge(ctx, collections.SessionReplaysMessage, collections.ReplaySessionNode, sessionID, collections.MessageNode, messageID, extra)
}

// LinkLedgerSessionHasCheckpoint 建立 replay_session -> replay_checkpoint 的关系。
func (a *ArangoAdapter) LinkLedgerSessionHasCheckpoint(ctx context.Context, sessionID string, checkpointID string, extra map[string]interface{}) error {
	collections := a.ledgerCollections()
	return a.upsertLedgerEdge(ctx, collections.SessionHasCheckpoint, collections.ReplaySessionNode, sessionID, collections.ReplayCheckpointNode, checkpointID, extra)
}

// RecordCollaborationEnvelopeToLedger 将协作消息信封写入账本图。
func (a *ArangoAdapter) RecordCollaborationEnvelopeToLedger(ctx context.Context, envelope *CollaborationMessageEnvelope) error {
	if envelope == nil {
		return fmt.Errorf("collaboration envelope cannot be nil")
	}
	if err := envelope.Validate(); err != nil {
		return err
	}
	if err := a.EnsureCollaborationLedgerCollections(ctx); err != nil {
		return err
	}
	if err := a.UpsertLedgerRequestNode(ctx, &ArangoRequestNode{
		RequestID:     envelope.RequestID,
		OriginAdapter: "",
		Intent:        envelope.EventType,
		CreatedAtUnix: envelope.SentAtUnixMs,
		TraceID:       envelope.TraceID,
		Status:        "received",
	}); err != nil {
		return err
	}
	if err := a.UpsertLedgerMessageNode(ctx, &ArangoMessageNode{
		MessageID:       envelope.MessageID,
		RequestID:       envelope.RequestID,
		EventType:       envelope.EventType,
		Stream:          envelope.Stream,
		TicksSent:       envelope.TicksSent,
		TicksConsumed:   envelope.TicksConsumed,
		RetryCount:      envelope.RetryCount,
		SentAtUnixMs:    envelope.SentAtUnixMs,
		DeliveredAtUnix: 0,
		Status:          "recorded",
	}); err != nil {
		return err
	}

	if strings.TrimSpace(envelope.SenderNodeID) != "" {
		if err := a.UpsertLedgerOnlineAdapterNode(ctx, &ArangoOnlineAdapterNode{NodeID: envelope.SenderNodeID, Status: "online"}); err != nil {
			return err
		}
		if err := a.LinkLedgerEmits(ctx, envelope.SenderNodeID, envelope.MessageID, map[string]interface{}{"trace_id": envelope.TraceID}); err != nil {
			return err
		}
	}
	if strings.TrimSpace(envelope.ReceiverNodeID) != "" {
		if err := a.UpsertLedgerOnlineAdapterNode(ctx, &ArangoOnlineAdapterNode{NodeID: envelope.ReceiverNodeID, Status: "online"}); err != nil {
			return err
		}
		if err := a.LinkLedgerDeliversTo(ctx, envelope.MessageID, envelope.ReceiverNodeID, map[string]interface{}{"trace_id": envelope.TraceID}); err != nil {
			return err
		}
	}
	if err := a.LinkLedgerBelongsToRequest(ctx, envelope.MessageID, envelope.RequestID, map[string]interface{}{"trace_id": envelope.TraceID}); err != nil {
		return err
	}
	return nil
}

// QueryLedgerDeliveryPath 按 request_id 查询 sender -> message -> receiver 链路。
func (a *ArangoAdapter) QueryLedgerDeliveryPath(ctx context.Context, requestID string, limit int) ([]map[string]interface{}, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("request id must not be empty")
	}
	if limit <= 0 {
		limit = 50
	}
	collections := a.ledgerCollections()
	return a.ExecuteAQL(ctx, fmt.Sprintf(`
FOR req IN %s
	FILTER req._key == @requestId
	FOR rel IN %s
		FILTER rel._to == req._id
		LET msg = DOCUMENT(rel._from)
		LET sender = FIRST(
			FOR emits IN %s
				FILTER emits._to == msg._id
				RETURN DOCUMENT(emits._from)
		)
		LET receiver = FIRST(
			FOR delivers IN %s
				FILTER delivers._from == msg._id
				RETURN DOCUMENT(delivers._to)
		)
		SORT msg.sent_at ASC
		LIMIT @limit
		RETURN {
			request: req,
			message: msg,
			sender: sender,
			receiver: receiver,
			edges: {
				belongs_to_request: rel
			}
		}
`, collections.RequestNode, collections.BelongsToRequest, collections.EmitsEdge, collections.DeliversToEdge), map[string]interface{}{
		"requestId": requestID,
		"limit":     limit,
	})
}

// QueryReplaySessionMessages 按 replay_session_id 查询会话内回放消息序列。
//
// fromSeq <= 0 时从第一条开始；否则从指定 seq 开始。
func (a *ArangoAdapter) QueryReplaySessionMessages(ctx context.Context, sessionID string, fromSeq int, limit int) ([]map[string]interface{}, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("replay session id must not be empty")
	}
	if limit <= 0 {
		limit = 100
	}
	collections := a.ledgerCollections()
	return a.ExecuteAQL(ctx, fmt.Sprintf(`
LET sessionDocId = CONCAT(@sessionCollection, "/", @sessionId)
FOR edge IN %s
	FILTER edge._from == sessionDocId
	FILTER @fromSeq <= 0 OR edge.seq >= @fromSeq
	LET msg = DOCUMENT(edge._to)
	SORT edge.seq ASC
	LIMIT @limit
	RETURN {
		session_id: @sessionId,
		seq: edge.seq,
		replayed_at: edge.replayed_at,
		new_message_id: edge.new_message_id,
		message: msg
	}
`, collections.SessionReplaysMessage), map[string]interface{}{
		"sessionCollection": sanitizeArangoCollectionName(collections.ReplaySessionNode),
		"sessionId":         sessionID,
		"fromSeq":           fromSeq,
		"limit":             limit,
	})
}

// QueryReplaySessionCheckpoints 按 replay_session_id 查询断点列表。
func (a *ArangoAdapter) QueryReplaySessionCheckpoints(ctx context.Context, sessionID string, limit int) ([]map[string]interface{}, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("replay session id must not be empty")
	}
	if limit <= 0 {
		limit = 50
	}
	collections := a.ledgerCollections()
	return a.ExecuteAQL(ctx, fmt.Sprintf(`
LET sessionDocId = CONCAT(@sessionCollection, "/", @sessionId)
FOR edge IN %s
	FILTER edge._from == sessionDocId
	LET checkpoint = DOCUMENT(edge._to)
	SORT checkpoint.created_at ASC
	LIMIT @limit
	RETURN {
		session_id: @sessionId,
		checkpoint: checkpoint,
		edge: edge
	}
`, collections.SessionHasCheckpoint), map[string]interface{}{
		"sessionCollection": sanitizeArangoCollectionName(collections.ReplaySessionNode),
		"sessionId":         sessionID,
		"limit":             limit,
	})
}

// QueryReplayMessagesFromCheckpoint 从断点锚点开始查询可回放消息序列（支持任意锚点跳转）。
//
// includeAnchor=true 时包含锚点消息；false 时从锚点之后开始。
func (a *ArangoAdapter) QueryReplayMessagesFromCheckpoint(ctx context.Context, checkpointID string, includeAnchor bool, limit int) ([]map[string]interface{}, error) {
	if strings.TrimSpace(checkpointID) == "" {
		return nil, fmt.Errorf("checkpoint id must not be empty")
	}
	if limit <= 0 {
		limit = 100
	}
	collections := a.ledgerCollections()
	return a.ExecuteAQL(ctx, fmt.Sprintf(`
LET checkpoint = DOCUMENT(CONCAT(@checkpointCollection, "/", @checkpointId))
FILTER checkpoint != null
LET sessionDocId = CONCAT(@sessionCollection, "/", checkpoint.session_id)
LET anchorSeq = FIRST(
	FOR e IN %s
		FILTER e._from == sessionDocId
		LET m = DOCUMENT(e._to)
		FILTER m._key == checkpoint.anchor_value
		RETURN e.seq
)
FOR edge IN %s
	FILTER edge._from == sessionDocId
	FILTER anchorSeq == null OR (@includeAnchor && edge.seq >= anchorSeq) OR (!@includeAnchor && edge.seq > anchorSeq)
	LET msg = DOCUMENT(edge._to)
	SORT edge.seq ASC
	LIMIT @limit
	RETURN {
		session_id: checkpoint.session_id,
		checkpoint_id: checkpoint._key,
		anchor_type: checkpoint.anchor_type,
		anchor_value: checkpoint.anchor_value,
		seq: edge.seq,
		replayed_at: edge.replayed_at,
		new_message_id: edge.new_message_id,
		message: msg
	}
`, collections.SessionReplaysMessage, collections.SessionReplaysMessage), map[string]interface{}{
		"checkpointCollection": sanitizeArangoCollectionName(collections.ReplayCheckpointNode),
		"sessionCollection":    sanitizeArangoCollectionName(collections.ReplaySessionNode),
		"checkpointId":         checkpointID,
		"includeAnchor":        includeAnchor,
		"limit":                limit,
	})
}

// MarkLedgerMessageReplayed 将账本中指定消息节点的状态更新为 replayed，并记录回放时间与目标 stream。
//
// 若消息节点不存在则跳过（INSERT 保持最小字段，避免覆盖现有数据）。
func (a *ArangoAdapter) MarkLedgerMessageReplayed(ctx context.Context, messageID string, replayedAtUnixMs int64, targetStream string) error {
	if strings.TrimSpace(messageID) == "" {
		return fmt.Errorf("arango ledger: message_id must not be empty for replay mark")
	}
	collections := a.ledgerCollections()
	_, err := a.ExecuteAQL(ctx, fmt.Sprintf(`
UPSERT { _key: @key }
INSERT { _key: @key, status: @status, replayed_at: @replayedAt, replay_target_stream: @targetStream }
UPDATE { status: @status, replayed_at: @replayedAt, replay_target_stream: @targetStream }
IN %s
RETURN NEW
`, collections.MessageNode), map[string]interface{}{
		"key":          messageID,
		"status":       string(ReplayStatusReplayed),
		"replayedAt":   replayedAtUnixMs,
		"targetStream": targetStream,
	})
	return err
}
