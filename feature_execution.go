package db

import (
	"context"
	"fmt"
)

// FeatureExecutionMode 功能执行模式
// native: 使用数据库原生能力
// custom: 使用 adapter 自定义实现
// fallback: 使用降级策略
// unsupported: 不支持且无替代方案
type FeatureExecutionMode string

const (
	FeatureExecutionNative      FeatureExecutionMode = "native"
	FeatureExecutionCustom      FeatureExecutionMode = "custom"
	FeatureExecutionFallback    FeatureExecutionMode = "fallback"
	FeatureExecutionUnsupported FeatureExecutionMode = "unsupported"
)

// FeatureExecutionDecision 功能执行决策结果
type FeatureExecutionDecision struct {
	Mode     FeatureExecutionMode
	Fallback FeatureFallback
	Reason   string
}

// CustomFeatureProvider adapter 的可选扩展接口
// 用于声明和执行数据库原生不支持但 adapter 可自定义实现的能力。
type CustomFeatureProvider interface {
	HasCustomFeatureImplementation(feature string) bool
	ExecuteCustomFeature(ctx context.Context, feature string, input map[string]interface{}) (interface{}, error)
}

// DecideFeatureExecution 决策某功能应走原生、自定义、降级或报错
func (r *Repository) DecideFeatureExecution(feature, version string) (*FeatureExecutionDecision, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}

	features := r.adapter.GetDatabaseFeatures()
	if features != nil && features.SupportsFeatureWithVersion(feature, version) {
		return &FeatureExecutionDecision{
			Mode:   FeatureExecutionNative,
			Reason: "feature is supported natively",
		}, nil
	}

	if custom, ok := r.adapter.(CustomFeatureProvider); ok && custom.HasCustomFeatureImplementation(feature) {
		return &FeatureExecutionDecision{
			Mode:   FeatureExecutionCustom,
			Reason: "adapter provides custom implementation",
		}, nil
	}

	if features != nil {
		fallback := features.GetFallbackStrategy(feature)
		if fallback != FallbackNone {
			return &FeatureExecutionDecision{
				Mode:     FeatureExecutionFallback,
				Fallback: fallback,
				Reason:   "feature is not supported natively but has fallback strategy",
			}, nil
		}
	}

	return &FeatureExecutionDecision{
		Mode:   FeatureExecutionUnsupported,
		Reason: "feature is not supported and no custom or fallback implementation is available",
	}, nil
}

// ExecuteFeature 尝试执行功能；仅在 custom 模式下真正执行。
// native/fallback/unsupported 会返回解释性错误，便于上层路由决策。
func (r *Repository) ExecuteFeature(ctx context.Context, feature, version string, input map[string]interface{}) (interface{}, error) {
	decision, err := r.DecideFeatureExecution(feature, version)
	if err != nil {
		return nil, err
	}

	switch decision.Mode {
	case FeatureExecutionCustom:
		r.mu.RLock()
		adapter := r.adapter
		r.mu.RUnlock()

		custom, ok := adapter.(CustomFeatureProvider)
		if !ok {
			return nil, fmt.Errorf("feature %s expected custom provider but adapter does not implement it", feature)
		}
		return custom.ExecuteCustomFeature(ctx, feature, input)
	case FeatureExecutionNative:
		return nil, fmt.Errorf("feature %s should use native implementation", feature)
	case FeatureExecutionFallback:
		return nil, fmt.Errorf("feature %s requires fallback strategy: %s", feature, decision.Fallback)
	default:
		return nil, fmt.Errorf("feature %s is unsupported", feature)
	}
}
