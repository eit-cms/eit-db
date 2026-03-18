package db

import (
	"context"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ShardingTemplateKind 分表模板类型。
type ShardingTemplateKind string

const (
	ShardingByDataScale ShardingTemplateKind = "data_scale"
	ShardingByDate      ShardingTemplateKind = "date"
	ShardingByRegion    ShardingTemplateKind = "region"
)

// DateShardingGranularity 日期分表粒度。
type DateShardingGranularity string

const (
	DateShardingByYear  DateShardingGranularity = "year"
	DateShardingByMonth DateShardingGranularity = "month"
	DateShardingByDay   DateShardingGranularity = "day"
)

// AutoShardingTemplate 自动分表模板。
type AutoShardingTemplate struct {
	Kind            ShardingTemplateKind
	KeyField        string
	ShardCount      int
	DateGranularity DateShardingGranularity
	AllowedRegions  map[string]struct{}
}

// NewDataScaleShardingTemplate 基于数据规模（哈希槽）分表。
func NewDataScaleShardingTemplate(keyField string, shardCount int) *AutoShardingTemplate {
	return &AutoShardingTemplate{
		Kind:       ShardingByDataScale,
		KeyField:   keyField,
		ShardCount: shardCount,
	}
}

// NewDateShardingTemplate 基于日期字段分表。
func NewDateShardingTemplate(keyField string, granularity DateShardingGranularity) *AutoShardingTemplate {
	if granularity == "" {
		granularity = DateShardingByMonth
	}
	return &AutoShardingTemplate{
		Kind:            ShardingByDate,
		KeyField:        keyField,
		DateGranularity: granularity,
	}
}

// NewRegionShardingTemplate 基于地区字段分表。
func NewRegionShardingTemplate(keyField string, allowedRegions ...string) *AutoShardingTemplate {
	allowed := make(map[string]struct{}, len(allowedRegions))
	for _, region := range allowedRegions {
		normalized := normalizeRegion(region)
		if normalized == "" {
			continue
		}
		allowed[normalized] = struct{}{}
	}

	return &AutoShardingTemplate{
		Kind:           ShardingByRegion,
		KeyField:       keyField,
		AllowedRegions: allowed,
	}
}

// ResolveShardID 根据模板从输入记录解析分片 ID。
func (t *AutoShardingTemplate) ResolveShardID(values map[string]interface{}) (string, error) {
	if t == nil {
		return "", fmt.Errorf("sharding template is nil")
	}

	switch t.Kind {
	case ShardingByDataScale:
		return t.resolveByDataScale(values)
	case ShardingByDate:
		return t.resolveByDate(values)
	case ShardingByRegion:
		return t.resolveByRegion(values)
	default:
		return "", fmt.Errorf("unsupported sharding template kind: %s", t.Kind)
	}
}

func (t *AutoShardingTemplate) resolveByDataScale(values map[string]interface{}) (string, error) {
	if t.ShardCount < 1 {
		return "", fmt.Errorf("invalid shard count: %d", t.ShardCount)
	}
	if strings.TrimSpace(t.KeyField) == "" {
		return "", fmt.Errorf("key field is required for data-scale sharding")
	}

	value, ok := values[t.KeyField]
	if !ok {
		return "", fmt.Errorf("missing shard key field: %s", t.KeyField)
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(fmt.Sprint(value)))
	idx := int(h.Sum32()) % t.ShardCount
	return fmt.Sprintf("s%02d", idx), nil
}

func (t *AutoShardingTemplate) resolveByDate(values map[string]interface{}) (string, error) {
	if strings.TrimSpace(t.KeyField) == "" {
		return "", fmt.Errorf("key field is required for date sharding")
	}

	value, ok := values[t.KeyField]
	if !ok {
		return "", fmt.Errorf("missing date shard field: %s", t.KeyField)
	}

	timeValue, err := parseShardTime(value)
	if err != nil {
		return "", fmt.Errorf("parse date shard field %s failed: %w", t.KeyField, err)
	}

	switch t.DateGranularity {
	case DateShardingByYear:
		return timeValue.Format("2006"), nil
	case DateShardingByMonth, "":
		return timeValue.Format("200601"), nil
	case DateShardingByDay:
		return timeValue.Format("20060102"), nil
	default:
		return "", fmt.Errorf("unsupported date granularity: %s", t.DateGranularity)
	}
}

func (t *AutoShardingTemplate) resolveByRegion(values map[string]interface{}) (string, error) {
	if strings.TrimSpace(t.KeyField) == "" {
		return "", fmt.Errorf("key field is required for region sharding")
	}

	value, ok := values[t.KeyField]
	if !ok {
		return "", fmt.Errorf("missing region shard field: %s", t.KeyField)
	}

	region := normalizeRegion(fmt.Sprint(value))
	if region == "" {
		return "", fmt.Errorf("region value is empty")
	}

	if len(t.AllowedRegions) > 0 {
		if _, exists := t.AllowedRegions[region]; !exists {
			return "", fmt.Errorf("region %q is not allowed", region)
		}
	}

	return region, nil
}

func parseShardTime(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		v = strings.TrimSpace(v)
		if v == "" {
			return time.Time{}, fmt.Errorf("empty time string")
		}
		layouts := []string{time.RFC3339, "2006-01-02", "2006-01-02 15:04:05"}
		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, v); err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("unsupported time format: %s", v)
	default:
		return time.Time{}, fmt.Errorf("unsupported time type: %T", value)
	}
}

func normalizeRegion(region string) string {
	return strings.ToLower(strings.TrimSpace(region))
}

// AutoShardingManager 自动分表管理器。
// 它位于动态建表上层：根据模板计算 shard，再委托 DynamicTableHook 创建/获取表。
type AutoShardingManager struct {
	hook       DynamicTableHook
	configName string
	template   *AutoShardingTemplate
}

// NewAutoShardingManager 创建自动分表管理器。
func NewAutoShardingManager(hook DynamicTableHook, configName string, template *AutoShardingTemplate) (*AutoShardingManager, error) {
	if hook == nil {
		return nil, fmt.Errorf("dynamic table hook is required")
	}
	if strings.TrimSpace(configName) == "" {
		return nil, fmt.Errorf("config name is required")
	}
	if template == nil {
		return nil, fmt.Errorf("sharding template is required")
	}

	return &AutoShardingManager{
		hook:       hook,
		configName: configName,
		template:   template,
	}, nil
}

// ResolveShardID 仅计算分片 ID，不触发表创建。
func (m *AutoShardingManager) ResolveShardID(values map[string]interface{}) (string, error) {
	return m.template.ResolveShardID(values)
}

// EnsureShardTable 计算分片并通过动态建表钩子确保子表可用。
func (m *AutoShardingManager) EnsureShardTable(ctx context.Context, values map[string]interface{}) (string, error) {
	shardID, err := m.ResolveShardID(values)
	if err != nil {
		return "", err
	}

	return m.hook.CreateDynamicTable(ctx, m.configName, map[string]interface{}{"id": shardID})
}

// ShardingStabilityReport 分表稳定性报告。
type ShardingStabilityReport struct {
	TemplateKind    ShardingTemplateKind
	SampleCount     int
	UniqueShards    []string
	Deterministic   bool
	ShardLoads      map[string]int
	HotShards       []string
	Recommendations []string
	RiskLevel       ShardingRiskLevel
	Errors          []string
	Warnings        []string
}

// CanRelease 判断该分表策略是否满足发布门禁。
// 规则：
// - 存在错误 -> 不可发布
// - RiskLevel == high -> 不可发布
// - 其他情况可发布（包含 medium，建议人工确认）
func (r *ShardingStabilityReport) CanRelease() bool {
	if r == nil {
		return false
	}
	if len(r.Errors) > 0 {
		return false
	}
	return r.RiskLevel != ShardingRiskHigh
}

// ReleaseBlockReasons 返回阻断发布的原因列表。
func (r *ShardingStabilityReport) ReleaseBlockReasons() []string {
	if r == nil {
		return []string{"report is nil"}
	}

	reasons := make([]string, 0)
	if len(r.Errors) > 0 {
		reasons = append(reasons, r.Errors...)
	}
	if r.RiskLevel == ShardingRiskHigh {
		reasons = append(reasons, "risk level is high")
	}

	return reasons
}

// ShardingRiskLevel 分表稳定性风险等级。
type ShardingRiskLevel string

const (
	ShardingRiskLow    ShardingRiskLevel = "low"
	ShardingRiskMedium ShardingRiskLevel = "medium"
	ShardingRiskHigh   ShardingRiskLevel = "high"
)

var shardIDPattern = regexp.MustCompile(`^[a-z0-9_]+$`)

// ShardingStabilityTool 分表稳定性检查工具。
type ShardingStabilityTool struct{}

// NewShardingStabilityTool 创建分表稳定性检查工具。
func NewShardingStabilityTool() *ShardingStabilityTool {
	return &ShardingStabilityTool{}
}

// ValidateTemplate 使用样本数据验证分表模板的稳定性。
func (t *ShardingStabilityTool) ValidateTemplate(template *AutoShardingTemplate, samples []map[string]interface{}, maxShards int) *ShardingStabilityReport {
	report := &ShardingStabilityReport{
		Deterministic:   true,
		ShardLoads:      make(map[string]int),
		HotShards:       make([]string, 0),
		Recommendations: make([]string, 0),
		RiskLevel:       ShardingRiskLow,
		Errors:          make([]string, 0),
		Warnings:        make([]string, 0),
	}

	if template == nil {
		report.Deterministic = false
		report.RiskLevel = ShardingRiskHigh
		report.Errors = append(report.Errors, "template is nil")
		return report
	}

	report.TemplateKind = template.Kind
	report.SampleCount = len(samples)
	unique := make(map[string]struct{})

	for i, sample := range samples {
		first, err := template.ResolveShardID(sample)
		if err != nil {
			report.Deterministic = false
			report.Errors = append(report.Errors, fmt.Sprintf("sample[%d] resolve failed: %v", i, err))
			continue
		}

		second, err := template.ResolveShardID(sample)
		if err != nil {
			report.Deterministic = false
			report.Errors = append(report.Errors, fmt.Sprintf("sample[%d] second resolve failed: %v", i, err))
			continue
		}

		if first != second {
			report.Deterministic = false
			report.Errors = append(report.Errors, fmt.Sprintf("sample[%d] non-deterministic shard id: %s != %s", i, first, second))
			continue
		}

		unique[first] = struct{}{}
		report.ShardLoads[first]++
	}

	report.UniqueShards = make([]string, 0, len(unique))
	for shard := range unique {
		report.UniqueShards = append(report.UniqueShards, shard)
	}
	sort.Strings(report.UniqueShards)

	if maxShards > 0 && len(report.UniqueShards) > maxShards {
		report.Warnings = append(report.Warnings, fmt.Sprintf("unique shards %d exceed maxShards %d", len(report.UniqueShards), maxShards))
	}

	t.detectHotShards(report)
	t.generateMigrationSuggestions(report, template, maxShards)
	t.evaluateRiskLevel(report)

	return report
}

func (t *ShardingStabilityTool) evaluateRiskLevel(report *ShardingStabilityReport) {
	if report == nil {
		return
	}

	if !report.Deterministic || len(report.Errors) > 0 {
		report.RiskLevel = ShardingRiskHigh
		return
	}

	if len(report.HotShards) > 0 || len(report.Warnings) > 0 {
		report.RiskLevel = ShardingRiskMedium
		return
	}

	report.RiskLevel = ShardingRiskLow
}

func (t *ShardingStabilityTool) detectHotShards(report *ShardingStabilityReport) {
	if report == nil || report.SampleCount == 0 || len(report.ShardLoads) == 0 {
		return
	}

	avg := float64(report.SampleCount) / float64(len(report.ShardLoads))
	threshold := avg * 1.5
	if threshold < 2 {
		threshold = 2
	}

	hot := make([]string, 0)
	for shard, count := range report.ShardLoads {
		if float64(count) >= threshold {
			hot = append(hot, shard)
		}
	}
	sort.Strings(hot)
	report.HotShards = hot

	if len(hot) > 0 {
		report.Warnings = append(report.Warnings,
			fmt.Sprintf("hot shard detected: %v, consider increasing shard count or refining sharding key", hot))
	}
}

func (t *ShardingStabilityTool) generateMigrationSuggestions(report *ShardingStabilityReport, template *AutoShardingTemplate, maxShards int) {
	if report == nil || template == nil {
		return
	}

	suggestions := make([]string, 0)
	if maxShards > 0 && len(report.UniqueShards) > maxShards {
		suggestions = append(suggestions,
			"current shard cardinality exceeds limit; consider archiving old shards or increasing maxShards")
	}

	if len(report.HotShards) > 0 && template.Kind == ShardingByDataScale {
		suggestions = append(suggestions,
			"data_scale template shows hotspot; consider increasing ShardCount or changing to a higher-cardinality key")
	}

	if template.Kind == ShardingByDate && template.DateGranularity == DateShardingByMonth && len(report.UniqueShards) >= 24 {
		suggestions = append(suggestions,
			"date/month shards have grown large; evaluate migrating to DateShardingByDay for finer distribution")
	}

	if template.Kind == ShardingByDate && template.DateGranularity == DateShardingByYear && len(report.UniqueShards) >= 3 {
		suggestions = append(suggestions,
			"date/year shards may become hot; evaluate DateShardingByMonth to balance write/read load")
	}

	report.Recommendations = suggestions
}

// ValidateShardNaming 检查分表命名稳定性，确保生成的表名可预测且只包含安全字符。
func (t *ShardingStabilityTool) ValidateShardNaming(baseTableName string, shardIDs []string) []error {
	errs := make([]error, 0)
	base := strings.TrimSpace(baseTableName)
	if base == "" {
		return []error{fmt.Errorf("base table name is empty")}
	}

	if !shardIDPattern.MatchString(base) {
		errs = append(errs, fmt.Errorf("base table name %q contains invalid characters", base))
	}

	seen := make(map[string]struct{})
	for _, shardID := range shardIDs {
		normalized := strings.TrimSpace(shardID)
		if normalized == "" {
			errs = append(errs, fmt.Errorf("shard id is empty"))
			continue
		}
		if !shardIDPattern.MatchString(normalized) {
			errs = append(errs, fmt.Errorf("shard id %q contains invalid characters", normalized))
			continue
		}

		tableName := base + "_" + normalized
		if len(tableName) > 63 {
			errs = append(errs, fmt.Errorf("table name %q exceeds 63 characters", tableName))
		}
		if _, exists := seen[tableName]; exists {
			errs = append(errs, fmt.Errorf("duplicate table name generated: %q", tableName))
			continue
		}
		seen[tableName] = struct{}{}
	}

	return errs
}
