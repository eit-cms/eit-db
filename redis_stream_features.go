package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"
)

const redisCollaborationEnvelopeField = "envelope"

// RedisStreamEnvelopeMessage 表示一个已解码的 Redis stream 消息。
type RedisStreamEnvelopeMessage struct {
	ID       string
	Stream   string
	Envelope *CollaborationMessageEnvelope
	Values   map[string]interface{}
}

// RedisStreamPendingMessage 表示待处理的 pending 消息摘要。
type RedisStreamPendingMessage struct {
	ID            string
	Consumer      string
	Idle          time.Duration
	DeliveryCount int64
}

// RedisStreamRetryResult 描述一次 retry/dead-letter 扫描结果。
type RedisStreamRetryResult struct {
	Claimed              int
	Retried              int
	DeadLettered         int
	RetriedMessageIDs    []string
	DeadLetterMessageIDs []string
}

// Merge 将另一个扫描结果累加到当前结果中。
func (r *RedisStreamRetryResult) Merge(other *RedisStreamRetryResult) {
	if r == nil || other == nil {
		return
	}
	r.Claimed += other.Claimed
	r.Retried += other.Retried
	r.DeadLettered += other.DeadLettered
	r.RetriedMessageIDs = append(r.RetriedMessageIDs, other.RetriedMessageIDs...)
	r.DeadLetterMessageIDs = append(r.DeadLetterMessageIDs, other.DeadLetterMessageIDs...)
}

// RedisStreamLagSnapshot 描述指定 stream/group 当前 backlog 观测值。
type RedisStreamLagSnapshot struct {
	Stream        string
	Group         string
	StreamLength  int64
	PendingCount  int64
	BacklogApprox int64
}

// RedisStreamRecoveryPolicy 定义主动恢复策略。
type RedisStreamRecoveryPolicy struct {
	Interval     time.Duration
	MinIdle      time.Duration
	BatchSize    int64
	MaxRounds    int
	DLQStream    string
	OnTickResult func(*RedisStreamRetryResult)
	OnTickError  func(error)
}

// RedisStreamRecoveryController 控制主动恢复生命周期。
type RedisStreamRecoveryController struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Stop 停止主动恢复循环并等待退出。
func (c *RedisStreamRecoveryController) Stop() {
	if c == nil {
		return
	}
	if c.cancel != nil {
		c.cancel()
	}
	if c.done != nil {
		<-c.done
	}
}

// RedisStreamFeatures 提供 Collaboration Mode 需要的 Redis stream 能力。
type RedisStreamFeatures struct {
	adapter *RedisAdapter
}

// GetRedisStreamFeatures 从 Adapter 获取 Redis stream 能力。
func GetRedisStreamFeatures(adapter Adapter) (*RedisStreamFeatures, bool) {
	r, ok := adapter.(*RedisAdapter)
	if !ok {
		return nil, false
	}
	return &RedisStreamFeatures{adapter: r}, true
}

// GetRedisStreamFeatures 返回当前 Repository 绑定适配器的 Redis stream 能力视图。
func (r *Repository) GetRedisStreamFeatures() (*RedisStreamFeatures, bool) {
	if r == nil {
		return nil, false
	}
	return GetRedisStreamFeatures(r.GetAdapter())
}

// PublishEnvelope 将协作消息写入 Redis stream。
func (f *RedisStreamFeatures) PublishEnvelope(ctx context.Context, stream string, envelope *CollaborationMessageEnvelope) (string, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return "", fmt.Errorf("redis stream features unavailable")
	}
	stream = strings.TrimSpace(stream)
	if stream == "" {
		return "", fmt.Errorf("redis stream name must not be empty")
	}
	envelope.NormalizeForStream(stream)
	encoded, err := EncodeCollaborationMessageEnvelope(envelope)
	if err != nil {
		return "", err
	}
	return f.adapter.Client().XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			redisCollaborationEnvelopeField: encoded,
			"event_type":                    envelope.EventType,
			"request_id":                    envelope.RequestID,
			"message_id":                    envelope.MessageID,
		},
	}).Result()
}

// EnsureConsumerGroup 确保 stream 与 consumer group 存在。
func (f *RedisStreamFeatures) EnsureConsumerGroup(ctx context.Context, stream string, group string) error {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return fmt.Errorf("redis stream features unavailable")
	}
	stream = strings.TrimSpace(stream)
	group = strings.TrimSpace(group)
	if stream == "" || group == "" {
		return fmt.Errorf("redis stream and group must not be empty")
	}
	err := f.adapter.Client().XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

// ReadGroupEnvelopes 通过 consumer group 读取协作消息。
func (f *RedisStreamFeatures) ReadGroupEnvelopes(ctx context.Context, stream string, group string, consumer string, count int64, block time.Duration) ([]RedisStreamEnvelopeMessage, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return nil, fmt.Errorf("redis stream features unavailable")
	}
	stream = strings.TrimSpace(stream)
	group = strings.TrimSpace(group)
	consumer = strings.TrimSpace(consumer)
	if stream == "" || group == "" || consumer == "" {
		return nil, fmt.Errorf("redis stream, group, consumer must not be empty")
	}
	if count <= 0 {
		count = 1
	}

	streams, err := f.adapter.Client().XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	messages := make([]RedisStreamEnvelopeMessage, 0)
	for _, item := range streams {
		for _, msg := range item.Messages {
			envelope, decodeErr := decodeRedisStreamEnvelope(msg.Values)
			if decodeErr != nil {
				return nil, decodeErr
			}
			messages = append(messages, RedisStreamEnvelopeMessage{
				ID:       msg.ID,
				Stream:   item.Stream,
				Envelope: envelope,
				Values:   msg.Values,
			})
		}
	}
	return messages, nil
}

// Ack 确认消息已被消费。
func (f *RedisStreamFeatures) Ack(ctx context.Context, stream string, group string, ids ...string) (int64, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return 0, fmt.Errorf("redis stream features unavailable")
	}
	stream = strings.TrimSpace(stream)
	group = strings.TrimSpace(group)
	if stream == "" || group == "" {
		return 0, fmt.Errorf("redis stream and group must not be empty")
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return f.adapter.Client().XAck(ctx, stream, group, ids...).Result()
}

// ListPendingMessages 列出指定 consumer group 下的 pending 消息。
func (f *RedisStreamFeatures) ListPendingMessages(ctx context.Context, stream string, group string, count int64) ([]RedisStreamPendingMessage, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return nil, fmt.Errorf("redis stream features unavailable")
	}
	stream = strings.TrimSpace(stream)
	group = strings.TrimSpace(group)
	if stream == "" || group == "" {
		return nil, fmt.Errorf("redis stream and group must not be empty")
	}
	if count <= 0 {
		count = 10
	}

	items, err := f.adapter.Client().XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  count,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	out := make([]RedisStreamPendingMessage, 0, len(items))
	for _, item := range items {
		out = append(out, RedisStreamPendingMessage{
			ID:            item.ID,
			Consumer:      item.Consumer,
			Idle:          item.Idle,
			DeliveryCount: item.RetryCount,
		})
	}
	return out, nil
}

// ClaimPendingEnvelopes 认领超时未 ack 的 pending 消息。
func (f *RedisStreamFeatures) ClaimPendingEnvelopes(ctx context.Context, stream string, group string, consumer string, minIdle time.Duration, count int64) ([]RedisStreamEnvelopeMessage, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return nil, fmt.Errorf("redis stream features unavailable")
	}
	stream = strings.TrimSpace(stream)
	group = strings.TrimSpace(group)
	consumer = strings.TrimSpace(consumer)
	if stream == "" || group == "" || consumer == "" {
		return nil, fmt.Errorf("redis stream, group, consumer must not be empty")
	}
	if count <= 0 {
		count = 1
	}

	pending, err := f.adapter.Client().XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  count,
		Idle:   minIdle,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	if len(pending) == 0 {
		return nil, nil
	}

	ids := make([]string, 0, len(pending))
	for _, item := range pending {
		ids = append(ids, item.ID)
	}
	claimed, err := f.adapter.Client().XClaim(ctx, &redis.XClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	out := make([]RedisStreamEnvelopeMessage, 0, len(claimed))
	for _, msg := range claimed {
		envelope, decodeErr := decodeRedisStreamEnvelope(msg.Values)
		if decodeErr != nil {
			return nil, decodeErr
		}
		out = append(out, RedisStreamEnvelopeMessage{
			ID:       msg.ID,
			Stream:   stream,
			Envelope: envelope,
			Values:   msg.Values,
		})
	}
	return out, nil
}

// RetryPendingEnvelopes 对超时 pending 消息执行 retry 或 dead-letter 转移。
func (f *RedisStreamFeatures) RetryPendingEnvelopes(ctx context.Context, stream string, group string, consumer string, dlqStream string, minIdle time.Duration, count int64) (*RedisStreamRetryResult, error) {
	claimed, err := f.ClaimPendingEnvelopes(ctx, stream, group, consumer, minIdle, count)
	if err != nil {
		return nil, err
	}
	result := &RedisStreamRetryResult{
		Claimed: claimedCount(claimed),
	}
	for _, item := range claimed {
		if item.Envelope == nil {
			continue
		}
		retried := *item.Envelope
		nextRetryCount := retried.RetryCount + 1

		targetStream := stream
		if retried.MaxRetry > 0 && nextRetryCount > retried.MaxRetry {
			if strings.TrimSpace(dlqStream) == "" {
				return nil, fmt.Errorf("dlq stream must not be empty when max retry is exceeded")
			}
			targetStream = strings.TrimSpace(dlqStream)
			retried.RetryCount = retried.MaxRetry
		} else {
			retried.RetryCount = nextRetryCount
		}
		retried.Stream = targetStream

		newID, publishErr := f.PublishEnvelope(ctx, targetStream, &retried)
		if publishErr != nil {
			return nil, publishErr
		}
		if _, ackErr := f.Ack(ctx, stream, group, item.ID); ackErr != nil {
			return nil, ackErr
		}

		if targetStream == stream {
			result.Retried++
			result.RetriedMessageIDs = append(result.RetriedMessageIDs, newID)
		} else {
			result.DeadLettered++
			result.DeadLetterMessageIDs = append(result.DeadLetterMessageIDs, newID)
		}
	}
	return result, nil
}

// ScanAndRecoverBacklog 对 pending/backlog 执行多轮扫描恢复。
func (f *RedisStreamFeatures) ScanAndRecoverBacklog(ctx context.Context, stream string, group string, consumer string, dlqStream string, minIdle time.Duration, batchSize int64, maxRounds int) (*RedisStreamRetryResult, error) {
	if batchSize <= 0 {
		batchSize = 10
	}
	if maxRounds <= 0 {
		maxRounds = 1
	}
	total := &RedisStreamRetryResult{}
	for i := 0; i < maxRounds; i++ {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		result, err := f.RetryPendingEnvelopes(ctx, stream, group, consumer, dlqStream, minIdle, batchSize)
		if err != nil {
			return total, err
		}
		total.Merge(result)
		if result.Claimed == 0 {
			break
		}
	}
	return total, nil
}

// SnapshotLag 返回 stream/group 的 backlog 近似观测值。
func (f *RedisStreamFeatures) SnapshotLag(ctx context.Context, stream string, group string) (*RedisStreamLagSnapshot, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return nil, fmt.Errorf("redis stream features unavailable")
	}
	stream = strings.TrimSpace(stream)
	group = strings.TrimSpace(group)
	if stream == "" || group == "" {
		return nil, fmt.Errorf("redis stream and group must not be empty")
	}
	length, err := f.adapter.Client().XLen(ctx, stream).Result()
	if err != nil {
		return nil, err
	}
	pendingSummary, err := f.adapter.Client().XPending(ctx, stream, group).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	pending := int64(0)
	if pendingSummary != nil {
		pending = pendingSummary.Count
	}
	snapshot := &RedisStreamLagSnapshot{
		Stream:       stream,
		Group:        group,
		StreamLength: length,
		PendingCount: pending,
	}
	if length > pending {
		snapshot.BacklogApprox = length - pending
	}
	return snapshot, nil
}

// StartAutoRecovery 启动定时扫描恢复循环。
func (f *RedisStreamFeatures) StartAutoRecovery(parent context.Context, stream string, group string, consumer string, policy RedisStreamRecoveryPolicy) (*RedisStreamRecoveryController, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return nil, fmt.Errorf("redis stream features unavailable")
	}
	stream = strings.TrimSpace(stream)
	group = strings.TrimSpace(group)
	consumer = strings.TrimSpace(consumer)
	if stream == "" || group == "" || consumer == "" {
		return nil, fmt.Errorf("redis stream, group, consumer must not be empty")
	}
	interval := policy.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if policy.BatchSize <= 0 {
		policy.BatchSize = 10
	}
	if policy.MaxRounds <= 0 {
		policy.MaxRounds = 1
	}

	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		runOnce := func() {
			result, err := f.ScanAndRecoverBacklog(ctx, stream, group, consumer, policy.DLQStream, policy.MinIdle, policy.BatchSize, policy.MaxRounds)
			if err != nil {
				if policy.OnTickError != nil {
					policy.OnTickError(err)
				}
				return
			}
			if policy.OnTickResult != nil {
				policy.OnTickResult(result)
			}
		}

		runOnce()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runOnce()
			}
		}
	}()

	return &RedisStreamRecoveryController{cancel: cancel, done: done}, nil
}

func claimedCount(items []RedisStreamEnvelopeMessage) int {
	return len(items)
}

// RedisStreamDLQReplayResult 描述一次 DLQ 回放结果。
type RedisStreamDLQReplayResult struct {
	// 从 DLQ stream 读取到的消息总数。
	Read int
	// 成功回放（重新写入目标 stream）的消息数。
	Replayed int
	// 因 filterRequestID 过滤而跳过的消息数。
	Skipped int
	// 已回放消息的新 ID（写入目标 stream 后的 ID）。
	ReplayedMessageIDs []string
	// ReplayedOriginalMessageIDs 已回放消息的原始 message_id（来自信封字段），用于 Arango 账本审计。
	ReplayedOriginalMessageIDs []string
	// PlannedBy 规划来源，"arango" | "redis_only" | "redis_only_fallback"。空值表示未使用 Planner（直接调用 ReplayFromDLQ）。
	PlannedBy string
	// ReplaySessionID 回放会话 ID（Arango 增强模式下写入 replay_session_node）。
	ReplaySessionID string
}

// ReplayFromDLQ 从 Dead-Letter Queue stream 将消息回放到目标 stream。
//
// 参数说明：
//   - dlqStream: DLQ stream 名称（消息来源）。
//   - targetStream: 目标 stream 名称（消息回放写入目标）。
//   - dlqGroup: DLQ stream 所属 consumer group（用于读取与 ACK）。
//   - consumer: consumer 名称。
//   - filterRequestID: 若非空，仅回放 request_id 匹配的消息（精确过滤）。
//   - limit: 单次读取上限，<=0 时默认 50。
//
// 回放策略：
//  1. 从 dlqStream/dlqGroup 的 PEL（pending entry list）读取已投递但未 ack 的消息，非阻塞。
//     调用方应先通过 ReadGroupEnvelopes 消费 DLQ 消息以建立 PEL 状态，再调用本函数回放。
//  2. 将消息 RetryCount 重置为 0，写入 targetStream（幂等：IdempotencyKey 保留用于上游去重）。
//  3. 成功写入后 ACK DLQ 中的原消息。
func (f *RedisStreamFeatures) ReplayFromDLQ(ctx context.Context, dlqStream string, targetStream string, dlqGroup string, consumer string, filterRequestID string, limit int64) (*RedisStreamDLQReplayResult, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return nil, fmt.Errorf("redis stream features unavailable")
	}
	dlqStream = strings.TrimSpace(dlqStream)
	targetStream = strings.TrimSpace(targetStream)
	dlqGroup = strings.TrimSpace(dlqGroup)
	consumer = strings.TrimSpace(consumer)
	if dlqStream == "" || targetStream == "" || dlqGroup == "" || consumer == "" {
		return nil, fmt.Errorf("dlq stream, target stream, group, consumer must not be empty")
	}
	if limit <= 0 {
		limit = 50
	}

	// Read only from the PEL (pending entry list): messages already delivered to this
	// consumer group but not yet acknowledged. Non-blocking — does not wait for new messages.
	// Callers should first consume from dlqStream with ReadGroupEnvelopes to establish PEL entries.
	all, err := f.readDLQPending(ctx, dlqStream, dlqGroup, consumer, limit)
	if err != nil {
		return nil, err
	}

	result := &RedisStreamDLQReplayResult{
		Read:                       len(all),
		ReplayedMessageIDs:         make([]string, 0),
		ReplayedOriginalMessageIDs: make([]string, 0),
	}

	for _, item := range all {
		if item.Envelope == nil {
			result.Skipped++
			continue
		}
		if filterRequestID != "" && strings.TrimSpace(item.Envelope.RequestID) != strings.TrimSpace(filterRequestID) {
			result.Skipped++
			continue
		}

		// Reset retry state; preserve idempotency key so upstream can deduplicate.
		replayed := *item.Envelope
		replayed.RetryCount = 0
		replayed.Stream = targetStream

		newID, publishErr := f.PublishEnvelope(ctx, targetStream, &replayed)
		if publishErr != nil {
			return result, fmt.Errorf("dlq replay: publish to target stream failed: %w", publishErr)
		}
		if _, ackErr := f.Ack(ctx, dlqStream, dlqGroup, item.ID); ackErr != nil {
			return result, fmt.Errorf("dlq replay: ack dlq message %s failed: %w", item.ID, ackErr)
		}
		result.Replayed++
		result.ReplayedMessageIDs = append(result.ReplayedMessageIDs, newID)
		result.ReplayedOriginalMessageIDs = append(result.ReplayedOriginalMessageIDs, item.Envelope.MessageID)
	}
	return result, nil
}

// ReplayFromDLQWithPlanner 使用 DLQReplayPlanner 规划回放范围，然后执行 DLQ → targetStream 的回放。
//
// 当 planner 返回非空 MessageIDsToReplay 时，仅回放指定 message_id 的 PEL 消息（Arango 增强模式）；
// 当 planner 返回空 MessageIDsToReplay 时，降级为 requestID 字段过滤（redis_only 降级模式）。
// 若 planner.PlanReplay 调用失败，自动降级为 redis_only_fallback 模式。
// result.PlannedBy 字段记录本次规划来源。
func (f *RedisStreamFeatures) ReplayFromDLQWithPlanner(ctx context.Context, dlqStream string, targetStream string, dlqGroup string, consumer string, requestID string, limit int64, planner DLQReplayPlanner) (*RedisStreamDLQReplayResult, error) {
	if f == nil || f.adapter == nil || f.adapter.Client() == nil {
		return nil, fmt.Errorf("redis stream features unavailable")
	}
	if planner == nil {
		planner = &RedisOnlyReplayPlanner{}
	}

	plan, planErr := planner.PlanReplay(ctx, requestID)
	if planErr != nil {
		// 规划失败时降级为 redis_only_fallback，不中止，继续执行。
		plan = &DLQReplayPlan{
			RequestID:          requestID,
			MessageIDsToReplay: nil,
			PlannedBy:          "redis_only_fallback",
		}
	}

	plannedIDs := buildMessageIDSet(plan.MessageIDsToReplay)

	if limit <= 0 {
		limit = 50
	}
	all, err := f.readDLQPending(ctx, dlqStream, dlqGroup, consumer, limit)
	if err != nil {
		return nil, err
	}

	result := &RedisStreamDLQReplayResult{
		Read:                       len(all),
		ReplayedMessageIDs:         make([]string, 0),
		ReplayedOriginalMessageIDs: make([]string, 0),
		PlannedBy:                  plan.PlannedBy,
	}

	for _, item := range all {
		if item.Envelope == nil {
			result.Skipped++
			continue
		}

		if len(plannedIDs) > 0 {
			// Arango 模式：按计划中的精确 message_id 集合过滤。
			if !plannedIDs[item.Envelope.MessageID] {
				result.Skipped++
				continue
			}
		} else if strings.TrimSpace(requestID) != "" {
			// redis_only 降级：按 requestID 过滤。
			if strings.TrimSpace(item.Envelope.RequestID) != strings.TrimSpace(requestID) {
				result.Skipped++
				continue
			}
		}

		replayed := *item.Envelope
		replayed.RetryCount = 0
		replayed.Stream = targetStream

		newID, publishErr := f.PublishEnvelope(ctx, targetStream, &replayed)
		if publishErr != nil {
			return result, fmt.Errorf("dlq replay with planner: publish to target stream failed: %w", publishErr)
		}
		if _, ackErr := f.Ack(ctx, dlqStream, dlqGroup, item.ID); ackErr != nil {
			return result, fmt.Errorf("dlq replay with planner: ack dlq message %s failed: %w", item.ID, ackErr)
		}
		result.Replayed++
		result.ReplayedMessageIDs = append(result.ReplayedMessageIDs, newID)
		result.ReplayedOriginalMessageIDs = append(result.ReplayedOriginalMessageIDs, item.Envelope.MessageID)
	}
	return result, nil
}

// ReplayFromDLQWithPlannerAndTracking 在回放成功后同步写入账本与管理事件通道。
//
// 执行顺序：
//  1. ReplayFromDLQWithPlanner（回放 + ACK）
//  2. RecordReplayResultToLedger（Arango 账本，可选）
//  3. PublishReplayResultToManagementChannel（Redis 管理通道，可选）
func (f *RedisStreamFeatures) ReplayFromDLQWithPlannerAndTracking(ctx context.Context, dlqStream string, targetStream string, dlqGroup string, consumer string, requestID string, limit int64, planner DLQReplayPlanner, arango *ArangoAdapter, mgmt *RedisManagementFeatures, mgmtNamespace string, mgmtGroup string) (*RedisStreamDLQReplayResult, error) {
	result, err := f.ReplayFromDLQWithPlanner(ctx, dlqStream, targetStream, dlqGroup, consumer, requestID, limit, planner)
	if err != nil {
		return result, err
	}
	if err := RecordReplayResultToLedger(ctx, arango, result, targetStream); err != nil {
		return result, err
	}
	if err := RecordReplaySessionToLedger(ctx, arango, result, requestID, dlqStream, targetStream, mgmtNamespace, mgmtGroup); err != nil {
		return result, err
	}
	if err := PublishReplayResultToManagementChannel(ctx, mgmt, mgmtNamespace, mgmtGroup, requestID, dlqStream, targetStream, result); err != nil {
		return result, err
	}
	return result, nil
}

// buildMessageIDSet 将 message_id 切片转换为 O(1) 查找集合。
func buildMessageIDSet(ids []string) map[string]bool {
	if len(ids) == 0 {
		return nil
	}
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

// readDLQPending 从 DLQ consumer group PEL（pending entry list）读取已投递但未 ack 的消息。
func (f *RedisStreamFeatures) readDLQPending(ctx context.Context, stream string, group string, consumer string, count int64) ([]RedisStreamEnvelopeMessage, error) {
	streams, err := f.adapter.Client().XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, "0"},
		Count:    count,
		Block:    0,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	out := make([]RedisStreamEnvelopeMessage, 0)
	for _, item := range streams {
		for _, msg := range item.Messages {
			envelope, decodeErr := decodeRedisStreamEnvelope(msg.Values)
			if decodeErr != nil {
				continue
			}
			out = append(out, RedisStreamEnvelopeMessage{
				ID:       msg.ID,
				Stream:   item.Stream,
				Envelope: envelope,
				Values:   msg.Values,
			})
		}
	}
	return out, nil
}
func decodeRedisStreamEnvelope(values map[string]interface{}) (*CollaborationMessageEnvelope, error) {
	raw, ok := values[redisCollaborationEnvelopeField]
	if !ok {
		return nil, fmt.Errorf("redis stream message missing %s field", redisCollaborationEnvelopeField)
	}
	var encoded string
	switch value := raw.(type) {
	case string:
		encoded = value
	case []byte:
		encoded = string(value)
	default:
		return nil, fmt.Errorf("redis stream envelope field must be string, got %T", raw)
	}
	return DecodeCollaborationMessageEnvelope(encoded)
}
