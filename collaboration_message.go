package db

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// CollaborationMessageEnvelope 定义协作消息最小信封。
type CollaborationMessageEnvelope struct {
	MessageID      string                 `json:"message_id"`
	RequestID      string                 `json:"request_id"`
	TraceID        string                 `json:"trace_id,omitempty"`
	SenderNodeID   string                 `json:"sender_node_id,omitempty"`
	ReceiverNodeID string                 `json:"receiver_node_id,omitempty"`
	Topic          string                 `json:"topic,omitempty"`
	Stream         string                 `json:"stream,omitempty"`
	TicksSent      int64                  `json:"ticks_sent,omitempty"`
	TicksConsumed  int64                  `json:"ticks_consumed,omitempty"`
	IdempotencyKey string                 `json:"idempotency_key,omitempty"`
	EventType      string                 `json:"event_type"`
	Payload        map[string]interface{} `json:"payload,omitempty"`
	PayloadRef     string                 `json:"payload_ref,omitempty"`
	SentAtUnixMs   int64                  `json:"sent_at_unix_ms,omitempty"`
	ExpireAtUnixMs int64                  `json:"expire_at_unix_ms,omitempty"`
	RetryCount     int32                  `json:"retry_count,omitempty"`
	MaxRetry       int32                  `json:"max_retry,omitempty"`
	BlueprintTick   string                 `json:"blueprint_tick,omitempty"`
	RouteTick       string                 `json:"route_tick,omitempty"`
	SelectedNodeID  string                 `json:"selected_node_id,omitempty"`
	SchedulerPolicy string                 `json:"scheduler_policy,omitempty"`
}

// Validate 检查最小协议字段是否齐备。
func (e *CollaborationMessageEnvelope) Validate() error {
	if e == nil {
		return fmt.Errorf("collaboration message envelope cannot be nil")
	}
	if strings.TrimSpace(e.MessageID) == "" {
		return fmt.Errorf("collaboration message envelope message_id must not be empty")
	}
	if strings.TrimSpace(e.RequestID) == "" {
		return fmt.Errorf("collaboration message envelope request_id must not be empty")
	}
	if strings.TrimSpace(e.EventType) == "" {
		return fmt.Errorf("collaboration message envelope event_type must not be empty")
	}
	if e.RetryCount < 0 {
		return fmt.Errorf("collaboration message envelope retry_count must not be negative")
	}
	if e.MaxRetry < 0 {
		return fmt.Errorf("collaboration message envelope max_retry must not be negative")
	}
	if e.MaxRetry > 0 && e.RetryCount > e.MaxRetry {
		return fmt.Errorf("collaboration message envelope retry_count must not exceed max_retry")
	}
	return nil
}

// NormalizeForStream 在写入 Redis stream 前补足最小默认值。
func (e *CollaborationMessageEnvelope) NormalizeForStream(stream string) {
	if e == nil {
		return
	}
	if strings.TrimSpace(e.Stream) == "" {
		e.Stream = strings.TrimSpace(stream)
	}
	if e.SentAtUnixMs <= 0 {
		e.SentAtUnixMs = time.Now().UnixMilli()
	}
	if e.Payload == nil {
		e.Payload = make(map[string]interface{})
	}
}

// EncodeCollaborationMessageEnvelope 将消息编码为 JSON。
func EncodeCollaborationMessageEnvelope(envelope *CollaborationMessageEnvelope) (string, error) {
	if err := envelope.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// DecodeCollaborationMessageEnvelope 从 JSON 解码消息。
func DecodeCollaborationMessageEnvelope(payload string) (*CollaborationMessageEnvelope, error) {
	if strings.TrimSpace(payload) == "" {
		return nil, fmt.Errorf("collaboration message envelope payload must not be empty")
	}
	var envelope CollaborationMessageEnvelope
	if err := json.Unmarshal([]byte(payload), &envelope); err != nil {
		return nil, err
	}
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	return &envelope, nil
}