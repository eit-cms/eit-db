package db

import (
	"context"
	"strings"
	"unicode"
)

// FullTextRuntimeCapability 运行时全文检索能力探测结果
type FullTextRuntimeCapability struct {
	NativeSupported       bool
	PluginChecked         bool
	PluginAvailable       bool
	PluginName            string
	TokenizationSupported bool
	TokenizationMode      string
	Notes                 string
}

// FullTextRuntimeInspector 适配器可选接口：用于运行时探测全文与分词能力
type FullTextRuntimeInspector interface {
	InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error)
}

// FuzzySearchPlan 模糊查询执行计划
type FuzzySearchPlan struct {
	Mode      string
	Condition Condition
	Tokens    []string
	Reason    string
}

func tokenizeSearchTerms(keyword string) []string {
	trimmed := strings.TrimSpace(keyword)
	if trimmed == "" {
		return nil
	}

	tokens := strings.FieldsFunc(trimmed, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r)
	})

	result := make([]string, 0, len(tokens))
	seen := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		result = append(result, token)
	}

	if len(result) == 0 {
		return []string{trimmed}
	}

	return result
}

func buildTokenizedLikeCondition(field string, tokens []string) Condition {
	if len(tokens) == 0 {
		return Like(field, "%")
	}
	if len(tokens) == 1 {
		return Like(field, "%"+tokens[0]+"%")
	}

	conditions := make([]Condition, 0, len(tokens))
	for _, token := range tokens {
		conditions = append(conditions, Like(field, "%"+token+"%"))
	}
	return And(conditions...)
}

// AnalyzeFuzzySearch 根据数据库能力（含运行时插件探测）生成模糊查询计划。
func (r *Repository) AnalyzeFuzzySearch(ctx context.Context, field, keyword, version string) (*FuzzySearchPlan, error) {
	trimmed := strings.TrimSpace(keyword)
	if trimmed == "" {
		return &FuzzySearchPlan{
			Mode:      "like",
			Condition: Like(field, "%"),
			Reason:    "empty keyword",
		}, nil
	}

	adapter := r.GetAdapter()
	supportsFullText := false
	if adapter != nil {
		if features := adapter.GetQueryFeatures(); features != nil {
			supportsFullText = features.SupportsFeatureWithVersion("full_text_search", version)
		}
	}

	tokens := tokenizeSearchTerms(trimmed)
	if adapter != nil {
		if inspector, ok := adapter.(FullTextRuntimeInspector); ok {
			runtime, err := inspector.InspectFullTextRuntime(ctx)
			if err == nil && runtime != nil {
				if runtime.TokenizationSupported && len(tokens) > 1 {
					if !supportsFullText || (runtime.PluginChecked && !runtime.PluginAvailable && strings.EqualFold(runtime.TokenizationMode, "plugin")) {
						return &FuzzySearchPlan{
							Mode:      "tokenized_like_fallback",
							Condition: buildTokenizedLikeCondition(field, tokens),
							Tokens:    tokens,
							Reason:    "full-text plugin unavailable or full-text unsupported; fallback to tokenized LIKE",
						}, nil
					}
				}

				if runtime.PluginChecked && !runtime.PluginAvailable && strings.EqualFold(runtime.TokenizationMode, "plugin") {
					return &FuzzySearchPlan{
						Mode:      "like_fallback",
						Condition: Like(field, "%"+trimmed+"%"),
						Tokens:    tokens,
						Reason:    "required full-text plugin not installed",
					}, nil
				}
			}
		}
	}

	if supportsFullText {
		return &FuzzySearchPlan{
			Mode:      "full_text",
			Condition: FullText(field, trimmed),
			Tokens:    tokens,
			Reason:    "full-text search supported",
		}, nil
	}

	if len(tokens) > 1 {
		return &FuzzySearchPlan{
			Mode:      "tokenized_like_fallback",
			Condition: buildTokenizedLikeCondition(field, tokens),
			Tokens:    tokens,
			Reason:    "full-text unsupported; fallback to tokenized LIKE",
		}, nil
	}

	return &FuzzySearchPlan{
		Mode:      "like_fallback",
		Condition: Like(field, "%"+trimmed+"%"),
		Tokens:    tokens,
		Reason:    "full-text unsupported; fallback to LIKE",
	}, nil
}

// BuildFuzzyConditionWithContext 构建兼容模糊查询条件（带上下文能力探测）
func (r *Repository) BuildFuzzyConditionWithContext(ctx context.Context, field, keyword, version string) (Condition, error) {
	plan, err := r.AnalyzeFuzzySearch(ctx, field, keyword, version)
	if err != nil {
		return nil, err
	}
	return plan.Condition, nil
}

// BuildFuzzyCondition 构建兼容模糊查询条件：
// - 支持 full_text_search 时使用 FullText
// - 不支持时自动降级为 LIKE '%keyword%'
// version 用于版本化能力判断（可为空）
func (r *Repository) BuildFuzzyCondition(field, keyword, version string) Condition {
	cond, err := r.BuildFuzzyConditionWithContext(context.Background(), field, keyword, version)
	if err != nil {
		trimmed := strings.TrimSpace(keyword)
		if trimmed == "" {
			return Like(field, "%")
		}
		return Like(field, "%"+trimmed+"%")
	}
	return cond
}
