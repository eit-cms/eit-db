package db

import (
	"context"
	"strings"
	"testing"
	"time"
)

type stubDynamicTableHook struct {
	lastConfigName string
	lastParams     map[string]interface{}
}

func (h *stubDynamicTableHook) RegisterDynamicTable(ctx context.Context, config *DynamicTableConfig) error {
	return nil
}

func (h *stubDynamicTableHook) UnregisterDynamicTable(ctx context.Context, configName string) error {
	return nil
}

func (h *stubDynamicTableHook) ListDynamicTableConfigs(ctx context.Context) ([]*DynamicTableConfig, error) {
	return nil, nil
}

func (h *stubDynamicTableHook) GetDynamicTableConfig(ctx context.Context, configName string) (*DynamicTableConfig, error) {
	return nil, nil
}

func (h *stubDynamicTableHook) CreateDynamicTable(ctx context.Context, configName string, params map[string]interface{}) (string, error) {
	h.lastConfigName = configName
	h.lastParams = params
	if id, ok := params["id"]; ok {
		return "orders_" + id.(string), nil
	}
	return "orders", nil
}

func (h *stubDynamicTableHook) ListCreatedDynamicTables(ctx context.Context, configName string) ([]string, error) {
	return nil, nil
}

func TestDataScaleShardingTemplateDeterministic(t *testing.T) {
	tpl := NewDataScaleShardingTemplate("user_id", 16)
	v := map[string]interface{}{"user_id": 10001}

	a, err := tpl.ResolveShardID(v)
	if err != nil {
		t.Fatalf("resolve shard failed: %v", err)
	}
	b, err := tpl.ResolveShardID(v)
	if err != nil {
		t.Fatalf("resolve shard second failed: %v", err)
	}

	if a != b {
		t.Fatalf("expected deterministic shard, got %s and %s", a, b)
	}
	if !strings.HasPrefix(a, "s") {
		t.Fatalf("unexpected shard format: %s", a)
	}
}

func TestDateShardingTemplateByMonth(t *testing.T) {
	tpl := NewDateShardingTemplate("created_at", DateShardingByMonth)
	shard, err := tpl.ResolveShardID(map[string]interface{}{"created_at": time.Date(2026, 3, 17, 9, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("resolve shard failed: %v", err)
	}
	if shard != "202603" {
		t.Fatalf("expected 202603, got %s", shard)
	}
}

func TestRegionShardingTemplateAllowedRegions(t *testing.T) {
	tpl := NewRegionShardingTemplate("region", "cn", "us")
	shard, err := tpl.ResolveShardID(map[string]interface{}{"region": "CN"})
	if err != nil {
		t.Fatalf("resolve shard failed: %v", err)
	}
	if shard != "cn" {
		t.Fatalf("expected normalized region cn, got %s", shard)
	}

	_, err = tpl.ResolveShardID(map[string]interface{}{"region": "eu"})
	if err == nil {
		t.Fatal("expected error for disallowed region")
	}
}

func TestAutoShardingManagerEnsureShardTable(t *testing.T) {
	hook := &stubDynamicTableHook{}
	tpl := NewDateShardingTemplate("created_at", DateShardingByDay)
	manager, err := NewAutoShardingManager(hook, "orders_dynamic", tpl)
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}

	table, err := manager.EnsureShardTable(context.Background(), map[string]interface{}{
		"created_at": "2026-03-17",
	})
	if err != nil {
		t.Fatalf("ensure shard table failed: %v", err)
	}

	if hook.lastConfigName != "orders_dynamic" {
		t.Fatalf("expected config name orders_dynamic, got %s", hook.lastConfigName)
	}
	if hook.lastParams["id"] != "20260317" {
		t.Fatalf("expected id=20260317, got %v", hook.lastParams["id"])
	}
	if table != "orders_20260317" {
		t.Fatalf("unexpected table name: %s", table)
	}
}

func TestShardingStabilityToolValidateTemplate(t *testing.T) {
	tool := NewShardingStabilityTool()
	tpl := NewDataScaleShardingTemplate("tenant_id", 2)
	samples := []map[string]interface{}{
		{"tenant_id": 1},
		{"tenant_id": 2},
		{"tenant_id": 3},
	}

	report := tool.ValidateTemplate(tpl, samples, 1)
	if !report.Deterministic {
		t.Fatalf("expected deterministic report, got errors: %v", report.Errors)
	}
	if len(report.UniqueShards) == 0 {
		t.Fatal("expected unique shard list")
	}
	if len(report.Warnings) == 0 {
		t.Fatal("expected warning when unique shards exceed maxShards")
	}
}

func TestShardingStabilityToolHotShardAndRecommendations(t *testing.T) {
	tool := NewShardingStabilityTool()
	tpl := NewDataScaleShardingTemplate("tenant_id", 32)

	samples := make([]map[string]interface{}, 0, 12)
	for i := 0; i < 10; i++ {
		samples = append(samples, map[string]interface{}{"tenant_id": "heavy"})
	}
	samples = append(samples, map[string]interface{}{"tenant_id": "light1"})
	samples = append(samples, map[string]interface{}{"tenant_id": "light2"})

	report := tool.ValidateTemplate(tpl, samples, 2)
	if len(report.HotShards) == 0 {
		t.Fatal("expected hot shards to be detected")
	}
	if len(report.Recommendations) == 0 {
		t.Fatal("expected migration recommendations")
	}
}

func TestShardingStabilityToolDateMigrationSuggestion(t *testing.T) {
	tool := NewShardingStabilityTool()
	tpl := NewDateShardingTemplate("created_at", DateShardingByMonth)

	samples := make([]map[string]interface{}, 0, 24)
	for i := 1; i <= 24; i++ {
		year := 2024 + (i-1)/12
		month := time.Month((i-1)%12 + 1)
		samples = append(samples, map[string]interface{}{
			"created_at": time.Date(year, month, 1, 0, 0, 0, 0, time.UTC),
		})
	}

	report := tool.ValidateTemplate(tpl, samples, 48)
	found := false
	for _, rec := range report.Recommendations {
		if strings.Contains(rec, "DateShardingByDay") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected DateShardingByDay recommendation, got: %v", report.Recommendations)
	}
}

func TestShardingStabilityToolValidateShardNaming(t *testing.T) {
	tool := NewShardingStabilityTool()

	errs := tool.ValidateShardNaming("orders", []string{"202603", "202604"})
	if len(errs) != 0 {
		t.Fatalf("expected no naming errors, got: %v", errs)
	}

	errs = tool.ValidateShardNaming("orders", []string{"2026-03", "2026-03"})
	if len(errs) == 0 {
		t.Fatal("expected naming errors for invalid characters / duplicates")
	}
}

func TestShardingStabilityToolRiskLevel(t *testing.T) {
	tool := NewShardingStabilityTool()

	high := tool.ValidateTemplate(nil, nil, 0)
	if high.RiskLevel != ShardingRiskHigh {
		t.Fatalf("expected high risk for invalid template, got %s", high.RiskLevel)
	}

	mediumTpl := NewDataScaleShardingTemplate("tenant_id", 32)
	mediumSamples := make([]map[string]interface{}, 0, 8)
	for i := 0; i < 7; i++ {
		mediumSamples = append(mediumSamples, map[string]interface{}{"tenant_id": "hot"})
	}
	mediumSamples = append(mediumSamples, map[string]interface{}{"tenant_id": "cold"})
	medium := tool.ValidateTemplate(mediumTpl, mediumSamples, 2)
	if medium.RiskLevel != ShardingRiskMedium {
		t.Fatalf("expected medium risk for hotspot/warnings, got %s", medium.RiskLevel)
	}

	lowTpl := NewDateShardingTemplate("created_at", DateShardingByMonth)
	low := tool.ValidateTemplate(lowTpl, []map[string]interface{}{{"created_at": "2026-03-01"}}, 12)
	if low.RiskLevel != ShardingRiskLow {
		t.Fatalf("expected low risk for healthy case, got %s", low.RiskLevel)
	}
}

func TestShardingStabilityReportReleaseGate(t *testing.T) {
	report := &ShardingStabilityReport{RiskLevel: ShardingRiskLow}
	if !report.CanRelease() {
		t.Fatal("expected low-risk report to be releasable")
	}

	report.RiskLevel = ShardingRiskMedium
	if !report.CanRelease() {
		t.Fatal("expected medium-risk report to be releasable with manual review")
	}

	report.RiskLevel = ShardingRiskHigh
	if report.CanRelease() {
		t.Fatal("expected high-risk report to be blocked")
	}
	if len(report.ReleaseBlockReasons()) == 0 {
		t.Fatal("expected block reasons for high-risk report")
	}

	report = &ShardingStabilityReport{RiskLevel: ShardingRiskLow, Errors: []string{"template is nil"}}
	if report.CanRelease() {
		t.Fatal("expected report with errors to be blocked")
	}
	reasons := report.ReleaseBlockReasons()
	if len(reasons) == 0 || reasons[0] != "template is nil" {
		t.Fatalf("unexpected block reasons: %v", reasons)
	}
}
