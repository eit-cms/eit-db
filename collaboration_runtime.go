package db

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// CollaborationRuntimeConfig 协作运行时配置。
type CollaborationRuntimeConfig struct {
	// 心跳周期，默认 15s。
	HeartbeatInterval time.Duration

	// 节点 TTL（Redis key 过期时间），默认 45s。
	// 建议 NodeTTL >= 3 × HeartbeatInterval，确保心跳丢失 2 次内不过期。
	NodeTTL time.Duration

	// Stop() 等待心跳 goroutine 退出的最长时间，默认 5s。
	StopTimeout time.Duration
}

func (c CollaborationRuntimeConfig) withDefaults() CollaborationRuntimeConfig {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 15 * time.Second
	}
	if c.NodeTTL <= 0 {
		c.NodeTTL = 45 * time.Second
	}
	if c.StopTimeout <= 0 {
		c.StopTimeout = 5 * time.Second
	}
	return c
}

// CollaborationRuntimeReport 协作运行时启动报告。
type CollaborationRuntimeReport struct {
	NodeID            string `json:"node_id"`
	Namespace         string `json:"namespace"`
	Group             string `json:"group"`
	AdapterType       string `json:"adapter_type"`
	AdapterID         string `json:"adapter_id"`
	HeartbeatInterval string `json:"heartbeat_interval"`
	NodeTTL           string `json:"node_ttl"`
	StartedAt         string `json:"started_at"`
}

// CollaborationRuntime 协作层运行时。
//
// 自动完成协作适配器节点的完整生命周期：注册 → 周期心跳 → 优雅下线。
// 业务侧无需手写注册/心跳代码，仅需 Start/Stop 两次调用。
//
// 验收标准：
//   - 无需业务侧手写注册/心跳代码即可获得在线节点视图。
//   - 节点进程退出后，离线状态在 2 个心跳周期内可见（节点 TTL 过期）。
type CollaborationRuntime struct {
	mu     sync.Mutex
	mgmt   *RedisManagementFeatures
	node   CollaborationAdapterNodePresence
	cfg    CollaborationRuntimeConfig
	cancel context.CancelFunc
	done   chan struct{}
	report *CollaborationRuntimeReport
}

// NewCollaborationRuntime 创建协作运行时。
//
// mgmt 为 Redis 管理能力视图，node 为当前节点描述，cfg 为运行时配置（零值使用默认）。
func NewCollaborationRuntime(mgmt *RedisManagementFeatures, node CollaborationAdapterNodePresence, cfg CollaborationRuntimeConfig) *CollaborationRuntime {
	return &CollaborationRuntime{
		mgmt: mgmt,
		node: node,
		cfg:  cfg.withDefaults(),
	}
}

// Start 注册节点并启动周期心跳循环。
//
// 幂等：已启动时直接返回当前报告，不重复注册。
// ctx 仅用于初次注册调用；心跳循环使用独立 context 不受外部取消影响。
func (r *CollaborationRuntime) Start(ctx context.Context) (*CollaborationRuntimeReport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.done != nil {
		return r.report, nil
	}
	if r.mgmt == nil {
		return nil, fmt.Errorf("collaboration runtime: management features not configured")
	}
	if err := validatePresenceNode(&r.node); err != nil {
		return nil, fmt.Errorf("collaboration runtime: invalid node config: %w", err)
	}

	if err := r.mgmt.RegisterAdapterNode(ctx, &r.node, r.cfg.NodeTTL); err != nil {
		return nil, fmt.Errorf("collaboration runtime: initial node registration failed: %w", err)
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.cancel = cancel
	r.done = done

	namespace := r.node.Namespace
	group := r.node.Group
	nodeID := r.node.NodeID
	ttl := r.cfg.NodeTTL
	interval := r.cfg.HeartbeatInterval
	mgmt := r.mgmt

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				_ = mgmt.HeartbeatAdapterNode(loopCtx, namespace, group, nodeID, ttl)
			}
		}
	}()

	report := &CollaborationRuntimeReport{
		NodeID:            r.node.NodeID,
		Namespace:         normalizeManagementNamespace(r.node.Namespace),
		Group:             r.node.Group,
		AdapterType:       r.node.AdapterType,
		AdapterID:         r.node.AdapterID,
		HeartbeatInterval: r.cfg.HeartbeatInterval.String(),
		NodeTTL:           r.cfg.NodeTTL.String(),
		StartedAt:         time.Now().UTC().Format(time.RFC3339),
	}
	r.report = report
	return report, nil
}

// Stop 优雅下线：停止心跳循环，将节点标记为离线。
//
// 幂等：未启动或已停止时直接返回 nil。
func (r *CollaborationRuntime) Stop(ctx context.Context) error {
	r.mu.Lock()
	cancel := r.cancel
	done := r.done
	r.cancel = nil
	r.done = nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		timer := time.NewTimer(r.cfg.StopTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
		}
	}

	if r.mgmt == nil {
		return nil
	}
	namespace := normalizeManagementNamespace(r.node.Namespace)
	group := strings.TrimSpace(r.node.Group)
	nodeID := strings.TrimSpace(r.node.NodeID)
	if group == "" || nodeID == "" {
		return nil
	}
	return r.mgmt.MarkAdapterOffline(ctx, namespace, group, nodeID)
}

// Report 返回最近的启动报告（Start 未调用时为 nil）。
func (r *CollaborationRuntime) Report() *CollaborationRuntimeReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.report
}

// IsRunning 返回心跳循环是否仍在运行。
func (r *CollaborationRuntime) IsRunning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done != nil
}
