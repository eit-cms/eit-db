package db

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const replayEventIDSampleLimit = 20

// ReplayStatus 描述 DLQ 消息回放判定状态。
// 与 docs/adapters/ARANGO.md 中的回放判定状态机保持一致。
type ReplayStatus string

const (
	// ReplayStatusCandidate 消息在 DLQ/PEL 中，待评估是否需要回放。
	ReplayStatusCandidate ReplayStatus = "candidate"
	// ReplayStatusPlanned Arango 账本确认该消息需要回放。
	ReplayStatusPlanned ReplayStatus = "planned"
	// ReplayStatusReplayed 已成功写入目标 stream。
	ReplayStatusReplayed ReplayStatus = "replayed"
	// ReplayStatusFailed 回放写入失败（可重试）。
	ReplayStatusFailed ReplayStatus = "failed"
	// ReplayStatusSkipped 不在回放计划中（被过滤）。
	ReplayStatusSkipped ReplayStatus = "skipped"
	// ReplayStatusAudited 已消费并完成审计，链路闭合。
	ReplayStatusAudited ReplayStatus = "audited"
)

// DLQReplayPlan 描述一次回放计划。
type DLQReplayPlan struct {
	// RequestID 本次回放关联的 request_id。
	RequestID string
	// MessageIDsToReplay 精确指定需要回放的 message_id 集合。
	// 空切片表示"不限制 ID，仅依赖 filterRequestID 过滤"（redis_only 降级行为）。
	MessageIDsToReplay []string
	// PlannedBy 规划来源，"arango" | "redis_only"。
	PlannedBy string
	// PlannedAt 规划时间。
	PlannedAt time.Time
}

// DLQReplayPlanner 负责为给定 requestID 规划需要回放的消息 ID 集合。
//
// 返回 DLQReplayPlan.MessageIDsToReplay 为空表示"不限制 ID，回放所有 PEL 中符合 filterRequestID 的消息"。
// 返回 error 时调用方应决定是降级还是中止。
type DLQReplayPlanner interface {
	PlanReplay(ctx context.Context, requestID string) (*DLQReplayPlan, error)
}

// ArangoReplayPlanner 使用 ArangoDB 账本查询消息链路，返回需回放的 message_id 集合。
//
// Arango 可用时为默认回放规划器；不可用时应 fallback 到 RedisOnlyReplayPlanner。
// 仅回放账本中 status 为 "recorded" 或 "failed" 的消息（已投递成功的不重放）。
type ArangoReplayPlanner struct {
	Adapter *ArangoAdapter
	// Limit 账本查询上限，<=0 时默认 100。
	Limit int
}

// PlanReplay 查询 Arango 账本，返回 requestID 下需要回放的 message_id 列表。
func (p *ArangoReplayPlanner) PlanReplay(ctx context.Context, requestID string) (*DLQReplayPlan, error) {
	if p == nil || p.Adapter == nil {
		return nil, fmt.Errorf("arango replay planner: adapter is nil")
	}
	limit := p.Limit
	if limit <= 0 {
		limit = 100
	}
	rows, err := p.Adapter.QueryLedgerDeliveryPath(ctx, requestID, limit)
	if err != nil {
		return nil, fmt.Errorf("arango replay planner: ledger query failed: %w", err)
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		msgRaw, ok := row["message"]
		if !ok {
			continue
		}
		msgMap, ok := msgRaw.(map[string]interface{})
		if !ok {
			continue
		}
		// 跳过已完成传递的消息，避免重复投递。
		if status, ok := msgMap["status"].(string); ok {
			if status == "delivered" || status == "audited" || status == "replayed" {
				continue
			}
		}
		// 取 _key 作为 message_id（account 为 key）。
		key, ok := msgMap["_key"].(string)
		if !ok || key == "" {
			continue
		}
		ids = append(ids, key)
	}
	return &DLQReplayPlan{
		RequestID:          requestID,
		MessageIDsToReplay: ids,
		PlannedBy:          "arango",
		PlannedAt:          time.Now(),
	}, nil
}

// RedisOnlyReplayPlanner 不依赖 Arango，返回空计划（回放所有符合 filterRequestID 的 PEL 消息）。
//
// 作为 Arango 不可用时的降级方案。
type RedisOnlyReplayPlanner struct{}

// PlanReplay 返回空消息 ID 列表，表示不限制 ID，仅依赖 filterRequestID 过滤。
func (p *RedisOnlyReplayPlanner) PlanReplay(ctx context.Context, requestID string) (*DLQReplayPlan, error) {
	return &DLQReplayPlan{
		RequestID:          requestID,
		MessageIDsToReplay: nil,
		PlannedBy:          "redis_only",
		PlannedAt:          time.Now(),
	}, nil
}

// NewDefaultReplayPlanner 根据 Arango adapter 是否可用，返回最合适的 DLQReplayPlanner。
// Arango 可用（非 nil）时返回 ArangoReplayPlanner；否则返回 RedisOnlyReplayPlanner。
func NewDefaultReplayPlanner(arango *ArangoAdapter) DLQReplayPlanner {
	if arango != nil {
		return &ArangoReplayPlanner{Adapter: arango}
	}
	return &RedisOnlyReplayPlanner{}
}

// RecordReplayResultToLedger 将 DLQ 回放结果批量写入 Arango 账本。
//
// 对 result.ReplayedOriginalMessageIDs 中每个 message_id，将对应的 message_node 状态
// 更新为 replayed，并记录回放时间与目标 stream。arango 为 nil 时静默跳过（降级安全）。
func RecordReplayResultToLedger(ctx context.Context, arango *ArangoAdapter, result *RedisStreamDLQReplayResult, targetStream string) error {
	if arango == nil || result == nil || len(result.ReplayedOriginalMessageIDs) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	for _, msgID := range result.ReplayedOriginalMessageIDs {
		if err := arango.MarkLedgerMessageReplayed(ctx, msgID, now, targetStream); err != nil {
			return fmt.Errorf("record replay to ledger: message %s: %w", msgID, err)
		}
	}
	return nil
}

// RecordReplaySessionToLedger 将回放执行摘要写入 Arango replay_session/replay_checkpoint。
//
// 该结构用于承载时间关系、断点回放和后续任意锚点跳转。
func RecordReplaySessionToLedger(ctx context.Context, arango *ArangoAdapter, result *RedisStreamDLQReplayResult, requestID string, dlqStream string, targetStream string, namespace string, group string) error {
	if arango == nil || result == nil {
		return nil
	}
	now := time.Now().UnixMilli()
	if strings.TrimSpace(result.ReplaySessionID) == "" {
		result.ReplaySessionID = fmt.Sprintf("replay_%s_%d", strings.ReplaceAll(strings.TrimSpace(requestID), " ", "_"), now)
	}
	if err := arango.UpsertLedgerReplaySessionNode(ctx, &ArangoReplaySessionNode{
		SessionID:       result.ReplaySessionID,
		RequestID:       requestID,
		Mode:            "arango_preferred",
		PlannedBy:       result.PlannedBy,
		DLQStream:       dlqStream,
		TargetStream:    targetStream,
		Namespace:       namespace,
		Group:           group,
		Status:          string(ReplayStatusReplayed),
		StartedAtUnixMs: now,
		EndedAtUnixMs:   now,
	}); err != nil {
		return fmt.Errorf("record replay session to ledger: %w", err)
	}

	for i, msgID := range result.ReplayedOriginalMessageIDs {
		extra := map[string]interface{}{"seq": i + 1, "replayed_at": now}
		if i < len(result.ReplayedMessageIDs) {
			extra["new_message_id"] = result.ReplayedMessageIDs[i]
		}
		if err := arango.LinkLedgerSessionReplaysMessage(ctx, result.ReplaySessionID, msgID, extra); err != nil {
			return fmt.Errorf("record replay session message edge: %w", err)
		}
	}

	anchorValue := requestID
	if len(result.ReplayedOriginalMessageIDs) > 0 {
		anchorValue = result.ReplayedOriginalMessageIDs[len(result.ReplayedOriginalMessageIDs)-1]
	}
	checkpointID := result.ReplaySessionID + "__final"
	if err := arango.UpsertLedgerReplayCheckpointNode(ctx, &ArangoReplayCheckpointNode{
		CheckpointID:    checkpointID,
		SessionID:       result.ReplaySessionID,
		AnchorType:      "message_id",
		AnchorValue:     anchorValue,
		Tick:            "final",
		Cursor:          "final",
		Status:          "completed",
		CreatedAtUnixMs: now,
	}); err != nil {
		return fmt.Errorf("record replay checkpoint to ledger: %w", err)
	}
	if err := arango.LinkLedgerSessionHasCheckpoint(ctx, result.ReplaySessionID, checkpointID, map[string]interface{}{"kind": "final"}); err != nil {
		return fmt.Errorf("record replay session checkpoint edge: %w", err)
	}
	return nil
}

// PublishReplayResultToManagementChannel 将回放结果发布到 Redis 管理事件通道。
//
// mgmt 为 nil 或 result 为 nil 时静默跳过，方便在降级场景下复用同一流程。
func PublishReplayResultToManagementChannel(ctx context.Context, mgmt *RedisManagementFeatures, namespace string, group string, requestID string, dlqStream string, targetStream string, result *RedisStreamDLQReplayResult) error {
	if mgmt == nil || result == nil {
		return nil
	}
	replayedIDsSample, replayedIDsTruncated := sampleIDs(result.ReplayedMessageIDs, replayEventIDSampleLimit)
	originalIDsSample, originalIDsTruncated := sampleIDs(result.ReplayedOriginalMessageIDs, replayEventIDSampleLimit)
	_, err := mgmt.PublishGroupEvent(ctx, namespace, group, CollaborationAdapterGroupEvent{
		Namespace:   namespace,
		Group:       group,
		EventType:   "dlq.replay.published",
		Status:      string(ReplayStatusReplayed),
		TimestampMs: time.Now().UnixMilli(),
		Payload: map[string]interface{}{
			"replay_session_id":            result.ReplaySessionID,
			"request_id":                   requestID,
			"dlq_stream":                   dlqStream,
			"target_stream":                targetStream,
			"planned_by":                   result.PlannedBy,
			"read":                         result.Read,
			"replayed":                     result.Replayed,
			"skipped":                      result.Skipped,
			"replayed_message_ids_total":   len(result.ReplayedMessageIDs),
			"replayed_original_ids_total":  len(result.ReplayedOriginalMessageIDs),
			"replayed_message_ids_sample":  replayedIDsSample,
			"replayed_original_ids_sample": originalIDsSample,
			"sample_truncated":             replayedIDsTruncated || originalIDsTruncated,
		},
	})
	if err != nil {
		return fmt.Errorf("publish replay result to management channel: %w", err)
	}
	return nil
}

func sampleIDs(ids []string, max int) ([]string, bool) {
	if max <= 0 || len(ids) <= max {
		return append([]string(nil), ids...), false
	}
	return append([]string(nil), ids[:max]...), true
}

// CheckpointReplayPlanner 使用 Arango 断点查询规划从指定 checkpoint 开始的回放范围。
//
// 底层调用 QueryReplayMessagesFromCheckpoint，将返回的原始 message_id 集合
// 转换为 DLQReplayPlan，交由 ReplayFromDLQWithPlanner 执行。
type CheckpointReplayPlanner struct {
	Adapter       *ArangoAdapter
	CheckpointID  string
	IncludeAnchor bool
	// Limit 查询上限，<=0 时默认 200。
	Limit int
}

// PlanReplay 从断点锚点查询后续可回放消息序列，返回精确 message_id 集合。
func (p *CheckpointReplayPlanner) PlanReplay(ctx context.Context, requestID string) (*DLQReplayPlan, error) {
	if p == nil || p.Adapter == nil {
		return nil, fmt.Errorf("checkpoint replay planner: adapter is nil")
	}
	if strings.TrimSpace(p.CheckpointID) == "" {
		return nil, fmt.Errorf("checkpoint replay planner: checkpoint_id must not be empty")
	}
	limit := p.Limit
	if limit <= 0 {
		limit = 200
	}
	rows, err := p.Adapter.QueryReplayMessagesFromCheckpoint(ctx, p.CheckpointID, p.IncludeAnchor, limit)
	if err != nil {
		return nil, fmt.Errorf("checkpoint replay planner: query failed: %w", err)
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		msgRaw, ok := row["message"]
		if !ok {
			continue
		}
		msgMap, ok := msgRaw.(map[string]interface{})
		if !ok {
			continue
		}
		key, ok := msgMap["_key"].(string)
		if !ok || key == "" {
			continue
		}
		ids = append(ids, key)
	}
	return &DLQReplayPlan{
		RequestID:          requestID,
		MessageIDsToReplay: ids,
		PlannedBy:          "arango_checkpoint",
		PlannedAt:          time.Now(),
	}, nil
}

// ResumeReplayFromCheckpointParams 断点续跑所需参数。
type ResumeReplayFromCheckpointParams struct {
	// CheckpointID Arango replay_checkpoint_node 的 _key。
	CheckpointID string
	// IncludeAnchor 是否包含锚点消息本身（默认 false，从锚点之后开始）。
	IncludeAnchor bool
	// RequestID 回放关联的 request_id（用于 Arango 审计）。
	RequestID string
	// DLQStream Dead-letter queue stream 名称。
	DLQStream string
	// TargetStream 回放目标 stream 名称。
	TargetStream string
	// DLQGroup DLQ consumer group 名称。
	DLQGroup string
	// Consumer consumer 名称。
	Consumer string
	// Limit 单次读取 PEL 上限，<=0 时默认 200。
	Limit int64
	// Namespace 协作 namespace（用于管理事件通道）。
	Namespace string
	// Group 协作 group（用于管理事件通道）。
	Group string
}

// ResumeReplayFromCheckpoint 从指定 Arango checkpoint 开始执行断点续跑。
//
// 流程：
//  1. 使用 CheckpointReplayPlanner 查询 checkpoint 之后的可回放消息序列；
//  2. 调用 ReplayFromDLQWithPlannerAndTracking 精确回放并落账；
//  3. 结果写入新的 replay_session 节点（通过 tracking 流程自动完成）。
//
// arango 为 nil 时返回错误（断点续跑必须依赖 Arango）。
func ResumeReplayFromCheckpoint(ctx context.Context, streamFeatures *RedisStreamFeatures, arango *ArangoAdapter, mgmt *RedisManagementFeatures, p *ResumeReplayFromCheckpointParams) (*RedisStreamDLQReplayResult, error) {
	if arango == nil {
		return nil, fmt.Errorf("resume replay from checkpoint: arango adapter required")
	}
	if streamFeatures == nil {
		return nil, fmt.Errorf("resume replay from checkpoint: redis stream features required")
	}
	if p == nil || strings.TrimSpace(p.CheckpointID) == "" {
		return nil, fmt.Errorf("resume replay from checkpoint: checkpoint_id must not be empty")
	}
	limit := p.Limit
	if limit <= 0 {
		limit = 200
	}
	planner := &CheckpointReplayPlanner{
		Adapter:       arango,
		CheckpointID:  p.CheckpointID,
		IncludeAnchor: p.IncludeAnchor,
		Limit:         int(limit),
	}
	return streamFeatures.ReplayFromDLQWithPlannerAndTracking(
		ctx,
		p.DLQStream, p.TargetStream,
		p.DLQGroup, p.Consumer,
		p.RequestID, limit,
		planner, arango, mgmt, p.Namespace, p.Group,
	)
}

// JumpReplayToAnchorParams 任意锚点跳转回放所需参数。
type JumpReplayToAnchorParams struct {
	// SessionID 来源会话 ID（用于挂载新断点）。
	SessionID string
	// AnchorType 锚点类型，"message_id" | "tick" | "time"。
	AnchorType string
	// AnchorValue 锚点值（message_id、tick 字符串或 unix_ms 字符串）。
	AnchorValue string
	// IncludeAnchor 是否包含锚点消息本身，默认 false。
	IncludeAnchor bool
	// RequestID 回放关联的 request_id。
	RequestID string
	// DLQStream Dead-letter queue stream 名称。
	DLQStream string
	// TargetStream 回放目标 stream 名称。
	TargetStream string
	// DLQGroup DLQ consumer group 名称。
	DLQGroup string
	// Consumer consumer 名称。
	Consumer string
	// Limit 单次读取 PEL 上限，<=0 时默认 200。
	Limit int64
	// Namespace 协作 namespace。
	Namespace string
	// Group 协作 group。
	Group string
}

// JumpReplayToAnchor 向 Arango 写入任意锚点断点，然后执行断点回放。
//
// 流程：
//  1. 在 Arango 中创建 replay_checkpoint_node，锚点类型与值由调用方指定；
//  2. 将断点挂载到对应 session（SessionID 非空时）；
//  3. 调用 ResumeReplayFromCheckpoint 执行后续回放。
//
// 支持锚点类型：
//   - "message_id"：从消息 ID 对应位置跳转；
//   - "tick"：从 tick 值跳转；
//   - "time"：从时间戳（unix_ms）跳转（对应账本中的 sent_at 字段）。
func JumpReplayToAnchor(ctx context.Context, streamFeatures *RedisStreamFeatures, arango *ArangoAdapter, mgmt *RedisManagementFeatures, p *JumpReplayToAnchorParams) (*RedisStreamDLQReplayResult, error) {
	if arango == nil {
		return nil, fmt.Errorf("jump replay to anchor: arango adapter required")
	}
	if p == nil {
		return nil, fmt.Errorf("jump replay to anchor: params must not be nil")
	}
	anchorType := strings.TrimSpace(p.AnchorType)
	if anchorType == "" {
		anchorType = "message_id"
	}
	if strings.TrimSpace(p.AnchorValue) == "" {
		return nil, fmt.Errorf("jump replay to anchor: anchor_value must not be empty")
	}
	now := time.Now().UnixMilli()
	sessionID := strings.TrimSpace(p.SessionID)
	checkpointID := fmt.Sprintf("jump_%s_%s_%d", strings.ReplaceAll(strings.TrimSpace(p.RequestID), " ", "_"), strings.ReplaceAll(p.AnchorValue, " ", "_"), now)
	if err := arango.UpsertLedgerReplayCheckpointNode(ctx, &ArangoReplayCheckpointNode{
		CheckpointID:    checkpointID,
		SessionID:       sessionID,
		AnchorType:      anchorType,
		AnchorValue:     p.AnchorValue,
		Tick:            anchorType,
		Cursor:          p.AnchorValue,
		Status:          "jump",
		CreatedAtUnixMs: now,
	}); err != nil {
		return nil, fmt.Errorf("jump replay to anchor: create checkpoint failed: %w", err)
	}
	if sessionID != "" {
		if err := arango.LinkLedgerSessionHasCheckpoint(ctx, sessionID, checkpointID, map[string]interface{}{"kind": "jump", "anchor_type": anchorType}); err != nil {
			return nil, fmt.Errorf("jump replay to anchor: link checkpoint to session failed: %w", err)
		}
	}
	return ResumeReplayFromCheckpoint(ctx, streamFeatures, arango, mgmt, &ResumeReplayFromCheckpointParams{
		CheckpointID:  checkpointID,
		IncludeAnchor: p.IncludeAnchor,
		RequestID:     p.RequestID,
		DLQStream:     p.DLQStream,
		TargetStream:  p.TargetStream,
		DLQGroup:      p.DLQGroup,
		Consumer:      p.Consumer,
		Limit:         p.Limit,
		Namespace:     p.Namespace,
		Group:         p.Group,
	})
}
