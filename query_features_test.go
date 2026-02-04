package db

import (
	"testing"
)

// TestQueryFeatures 测试查询特性系统
func TestQueryFeatures(t *testing.T) {
	testCases := []struct {
		dbType   string
		features map[string]bool
	}{
		{
			"postgres",
			map[string]bool{
				"in_range":           true,
				"recursive_cte":      true,
				"window_func":        true,
				"full_outer_join":    true,
				"full_text_search":   true,
				"json_operators":     true,
			},
		},
		{
			"mysql",
			map[string]bool{
				"in_range":           true,
				"recursive_cte":      true,
				"window_func":        true,
				"full_outer_join":    false, // ❌ MySQL 不支持
				"except":             false, // ❌ MySQL 不支持
				"intersect":          false, // ❌ MySQL 不支持
				"full_text_search":   true,
				"json_operators":     true,
			},
		},
		{
			"sqlite",
			map[string]bool{
				"in_range":           true,
				"recursive_cte":      true,
				"window_func":        true,
				"full_outer_join":    false, // ❌ SQLite 不支持
				"full_text_search":   false, // ❌ SQLite 需要 FTS 扩展
				"regex_match":        false, // ❌ SQLite 需要注册函数
				"json_operators":     true,
			},
		},
		{
			"sqlserver",
			map[string]bool{
				"in_range":           true,
				"recursive_cte":      true,
				"window_func":        true,
				"full_outer_join":    true,
				"except":             true,
				"limit":              false, // ❌ SQL Server 用 OFFSET...FETCH
				"full_text_search":   true,
				"json_operators":     true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.dbType, func(t *testing.T) {
			qf := GetQueryFeatures(tc.dbType)

			for feature, expected := range tc.features {
				actual := qf.HasQueryFeature(feature)
				if actual != expected {
					t.Errorf("Feature %s: expected %v, got %v", feature, expected, actual)
				}
			}

			// 验证降级策略
			if !qf.HasQueryFeature("full_outer_join") {
				fallback := qf.GetFallbackStrategy("full_outer_join")
				if fallback == QueryFallbackNone {
					t.Logf("ℹ️  Database %s doesn't support FULL OUTER JOIN", tc.dbType)
				} else {
					t.Logf("ℹ️  Database %s uses fallback strategy: %s", tc.dbType, fallback)
				}
			}

			t.Logf("✓ %s query features validated", tc.dbType)
		})
	}
}

// TestQueryFeaturesComparison 测试查询特性对比
func TestQueryFeaturesComparison(t *testing.T) {
	pgFeatures := GetQueryFeatures("postgres")
	mysqlFeatures := GetQueryFeatures("mysql")

	comparison := CompareQueryFeatures(pgFeatures, mysqlFeatures)

	// 验证结果
	common := comparison["common_features"].([]string)
	onlyInPG := comparison["only_in_first"].([]string)
	onlyInMySQL := comparison["only_in_second"].([]string)

	t.Logf("Common features between PostgreSQL and MySQL: %d", len(common))
	for _, feat := range common {
		t.Logf("  ✓ %s", feat)
	}

	t.Logf("\nPostgreSQL-only features: %d", len(onlyInPG))
	for _, feat := range onlyInPG {
		t.Logf("  • %s", feat)
	}

	t.Logf("\nMySQL-only features: %d", len(onlyInMySQL))
	for _, feat := range onlyInMySQL {
		t.Logf("  • %s", feat)
	}

	// 至少应该有一些共同特性
	if len(common) == 0 {
		t.Error("Expected common features between PostgreSQL and MySQL")
	}

	// PostgreSQL 应该有独有特性
	if len(onlyInPG) == 0 {
		t.Error("Expected PostgreSQL-only features")
	}
}

// TestQueryFallbackStrategies 测试降级策略
func TestQueryFallbackStrategies(t *testing.T) {
	mysqlFeatures := GetQueryFeatures("mysql")

	testCases := []struct {
		feature          string
		expectedStrategy QueryFallbackStrategy
		shouldHaveNote   bool
	}{
		{
			"full_outer_join",
			QueryFallbackMultiQuery,
			true,
		},
		{
			"except",
			QueryFallbackMultiQuery,
			true,
		},
		{
			"order_by_in_aggregate",
			QueryFallbackApplicationLayer,
			true,
		},
	}

	for _, tc := range testCases {
		if mysqlFeatures.HasQueryFeature(tc.feature) {
			t.Logf("✓ MySQL supports %s natively", tc.feature)
			continue
		}

		strategy := mysqlFeatures.GetFallbackStrategy(tc.feature)
		if strategy != tc.expectedStrategy {
			t.Errorf("Feature %s: expected strategy %s, got %s",
				tc.feature, tc.expectedStrategy, strategy)
		}

		note := mysqlFeatures.GetFeatureNote(tc.feature)
		if tc.shouldHaveNote && note == "" {
			t.Errorf("Feature %s should have a note", tc.feature)
		}

		syntax := mysqlFeatures.GetAlternativeSyntax(tc.feature)
		t.Logf("✓ %s: fallback=%s, note=%s, alternative=%s",
			tc.feature, strategy, note, syntax)
	}
}

// TestQueryFeaturesWithAdapter 测试 Adapter 的查询特性
func TestQueryFeaturesWithAdapter(t *testing.T) {
	adapters := []struct {
		name    string
		adapter Adapter
	}{
		{"PostgreSQL", &PostgreSQLAdapter{}},
		{"MySQL", &MySQLAdapter{}},
		{"SQLite", &SQLiteAdapter{}},
		{"SQL Server", &SQLServerAdapter{}},
	}

	for _, tc := range adapters {
		t.Run(tc.name, func(t *testing.T) {
			qf := tc.adapter.GetQueryFeatures()
			if qf == nil {
				t.Errorf("%s adapter returned nil QueryFeatures", tc.name)
				return
			}

			// 验证基础特性都支持
			basicFeatures := []string{"in_range", "between", "like", "group_by", "order_by"}
			for _, feat := range basicFeatures {
				if !qf.HasQueryFeature(feat) {
					t.Errorf("%s doesn't support basic feature: %s", tc.name, feat)
				}
			}

			t.Logf("✓ %s adapter query features validated", tc.name)
		})
	}
}

// BenchmarkQueryFeatureCheck 基准测试：查询特性检查
func BenchmarkQueryFeatureCheck(b *testing.B) {
	qf := GetQueryFeatures("postgres")

	b.Run("HasQueryFeature", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			qf.HasQueryFeature("recursive_cte")
		}
	})

	b.Run("GetFallbackStrategy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			qf.GetFallbackStrategy("full_outer_join")
		}
	})
}
