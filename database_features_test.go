package db

import (
	"testing"
)

// TestDatabaseFeatures 测试各数据库的特性声明
func TestDatabaseFeatures(t *testing.T) {
	testCases := []struct {
		name     string
		adapter  string
		features map[string]bool
	}{
		{
			"PostgreSQL",
			"postgres",
			map[string]bool{
				"composite_keys":      true,
				"enum_type":           true,
				"composite_type":      true,
				"domain_type":         true,
				"window_functions":    true,
				"materialized_cte":    true,
				"arrays":              true,
				"returning":           true,
				"listen_notify":       true,
			},
		},
		{
			"MySQL",
			"mysql",
			map[string]bool{
				"composite_keys":      true,
				"enum_type":           true, // Column-level ENUM
				"composite_type":      false,
				"arrays":              false,
				"returning":           false,
				"listen_notify":       false,
			},
		},
		{
			"SQLite",
			"sqlite",
			map[string]bool{
				"composite_keys":      true,
				"partial_indexes":     true,
				"deferrable":          true,
				"enum_type":           false,
				"functions":           true, // ✅ Supported via Go registration
				"stored_procedures":   false,
				"arrays":              false,
			},
		},
		{
			"SQL Server",
			"sqlserver",
			map[string]bool{
				"composite_keys":      true,
				"udt":                 true,
				"enum_type":           false, // No ENUM type
				"window_functions":    true,
				"arrays":              false,
				"listen_notify":       false, // Use Service Broker
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建适配器
			var adapter Adapter

			switch tc.adapter {
			case "postgres":
				adapter = &PostgreSQLAdapter{}
			case "mysql":
				adapter = &MySQLAdapter{}
			case "sqlite":
				adapter = &SQLiteAdapter{}
			case "sqlserver":
				adapter = &SQLServerAdapter{}
			default:
				t.Fatalf("Unknown adapter: %s", tc.adapter)
			}

			// 获取特性
			features := adapter.GetDatabaseFeatures()
			if features == nil {
				t.Fatal("GetDatabaseFeatures returned nil")
			}

			// 验证数据库名称
			if features.DatabaseName == "" {
				t.Error("DatabaseName should not be empty")
			}

			// 验证预期特性
			for feature, expected := range tc.features {
				actual := features.HasFeature(feature)
				if actual != expected {
					t.Errorf("Feature %s: expected %v, got %v", feature, expected, actual)
				}
			}

			t.Logf("✓ %s features validated: %s (v%s)", tc.name, features.DatabaseName, features.DatabaseVersion)
		})
	}
}

// TestFeatureComparison 测试特性比较
func TestFeatureComparison(t *testing.T) {
	pgAdapter := &PostgreSQLAdapter{}
	mysqlAdapter := &MySQLAdapter{}

	pgFeatures := pgAdapter.GetDatabaseFeatures()
	mysqlFeatures := mysqlAdapter.GetDatabaseFeatures()

	comparison := CompareFeatures(pgFeatures, mysqlFeatures)

	// PostgreSQL 应该有 MySQL 没有的特性
	if len(comparison.OnlyInFirst) == 0 {
		t.Error("PostgreSQL should have unique features not in MySQL")
	}

	// 验证已知的 PostgreSQL 独有特性
	hasArrays := false
	hasListenNotify := false
	for _, feature := range comparison.OnlyInFirst {
		if feature == "arrays" {
			hasArrays = true
		}
		if feature == "listen_notify" {
			hasListenNotify = true
		}
	}

	if !hasArrays {
		t.Error("PostgreSQL should have 'arrays' feature that MySQL doesn't")
	}
	if !hasListenNotify {
		t.Error("PostgreSQL should have 'listen_notify' feature that MySQL doesn't")
	}

	// 共同特性应该存在
	if len(comparison.CommonFeatures) == 0 {
		t.Error("PostgreSQL and MySQL should have common features")
	}

	t.Logf("✓ Common features: %d", len(comparison.CommonFeatures))
	t.Logf("✓ PostgreSQL unique: %d", len(comparison.OnlyInFirst))
	t.Logf("✓ MySQL unique: %d", len(comparison.OnlyInSecond))
}

// TestGetFeaturesByCategory 测试按分类获取特性
func TestGetFeaturesByCategory(t *testing.T) {
	adapter := &PostgreSQLAdapter{}
	features := adapter.GetDatabaseFeatures()

	testCases := []struct {
		category       FeatureCategory
		minExpected    int
		shouldContain  string
	}{
		{CategoryIndexing, 3, "composite_keys"},
		{CategoryTypes, 3, "enum_type"},
		{CategoryFunctions, 2, "stored_procedures"},
		{CategoryAdvanced, 3, "window_functions"},
		{CategoryJSON, 2, "native_json"},
		{CategoryOther, 3, "arrays"},
	}

	for _, tc := range testCases {
		featureList := features.GetFeaturesByCategory(tc.category)

		if len(featureList) < tc.minExpected {
			t.Errorf("Category %s: expected at least %d features, got %d",
				tc.category, tc.minExpected, len(featureList))
		}

		// 验证包含特定特性
		found := false
		for _, f := range featureList {
			if f == tc.shouldContain {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Category %s should contain feature %s", tc.category, tc.shouldContain)
		}

		t.Logf("✓ Category %s: %d features", tc.category, len(featureList))
	}
}

// TestSQLiteVsPostgreSQL 测试 SQLite vs PostgreSQL 特性对比
func TestSQLiteVsPostgreSQL(t *testing.T) {
	sqliteAdapter := &SQLiteAdapter{}
	pgAdapter := &PostgreSQLAdapter{}

	sqliteFeatures := sqliteAdapter.GetDatabaseFeatures()
	pgFeatures := pgAdapter.GetDatabaseFeatures()

	comparison := CompareFeatures(sqliteFeatures, pgFeatures)

	// SQLite 缺少的重要特性
	missingInSQLite := map[string]bool{
		"enum_type":         true,
		"stored_procedures": true,
		"arrays":            true,
		"listen_notify":     true,
	}

	for feature := range missingInSQLite {
		found := false
		for _, f := range comparison.OnlyInSecond {
			if f == feature {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected %s to be only in PostgreSQL, but it wasn't", feature)
		}
	}

	// SQLite 独有的特性
	// 实际上 SQLite 和 PostgreSQL 都支持 partial_indexes 和 deferrable，所以可能没有独有的
	t.Logf("✓ SQLite vs PostgreSQL: %d common, %d unique to SQLite, %d unique to PostgreSQL",
		len(comparison.CommonFeatures), len(comparison.OnlyInFirst), len(comparison.OnlyInSecond))
}

// TestSQLServerFeatures 测试 SQL Server 特有特性
func TestSQLServerFeatures(t *testing.T) {
	adapter := &SQLServerAdapter{}
	features := adapter.GetDatabaseFeatures()

	// SQL Server 应该支持的特性
	expectedSupport := []string{
		"composite_keys",
		"udt",                 // User-Defined Types
		"stored_procedures",
		"functions",
		"window_functions",
		"cte",
		"returning",           // OUTPUT clause
		"upsert",              // MERGE
	}

	for _, feature := range expectedSupport {
		if !features.HasFeature(feature) {
			t.Errorf("SQL Server should support %s", feature)
		}
	}

	// SQL Server 不支持的特性
	expectedNoSupport := []string{
		"enum_type",      // No native ENUM
		"arrays",         // No array type
		"listen_notify",  // Use Service Broker instead
	}

	for _, feature := range expectedNoSupport {
		if features.HasFeature(feature) {
			t.Errorf("SQL Server should not support %s", feature)
		}
	}

	// 验证函数语言
	if len(features.FunctionLanguages) == 0 {
		t.Error("SQL Server should have function languages")
	}

	hasTSQL := false
	for _, lang := range features.FunctionLanguages {
		if lang == "tsql" {
			hasTSQL = true
			break
		}
	}

	if !hasTSQL {
		t.Error("SQL Server should support T-SQL")
	}

	t.Logf("✓ SQL Server features validated: %d function languages", len(features.FunctionLanguages))
}

// TestAllDatabasesSupportsComposite 测试所有数据库都支持复合键
func TestAllDatabasesSupportsComposite(t *testing.T) {
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
		features := tc.adapter.GetDatabaseFeatures()

		if !features.SupportsCompositeKeys {
			t.Errorf("%s should support composite keys", tc.name)
		}

		if !features.SupportsCompositeIndexes {
			t.Errorf("%s should support composite indexes", tc.name)
		}

		t.Logf("✓ %s supports composite keys and indexes", tc.name)
	}
}
