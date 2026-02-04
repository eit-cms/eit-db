package db

import (
	"strings"
	"testing"
)

// ==================== 构造器测试 ====================

// TestNewPostgreSQLQueryFeatures 测试 PostgreSQL 查询特性构造器
func TestNewPostgreSQLQueryFeatures(t *testing.T) {
	qf := NewPostgreSQLQueryFeatures()

	// PostgreSQL 应该支持所有主要特性
	expectations := map[string]bool{
		"in_range":              true,
		"not_in":                true,
		"between":               true,
		"like":                  true,
		"inner_join":            true,
		"left_join":             true,
		"right_join":            true,
		"cross_join":            true,
		"full_outer_join":       true,
		"cte":                   true,
		"recursive_cte":         true,
		"window_func":           true,
		"subquery":              true,
		"correlated_subquery":   true,
		"union":                 true,
		"except":                true,
		"intersect":             true,
		"full_text_search":      true,
		"regex_match":           true,
		"json_path":             true,
		"json_operators":        true,
		"upsert":                true,
	}

	for feature, expected := range expectations {
		if actual := qf.HasQueryFeature(feature); actual != expected {
			t.Errorf("PostgreSQL %s: expected %v, got %v", feature, expected, actual)
		}
	}
}

// TestNewMySQLQueryFeatures 测试 MySQL 查询特性构造器
func TestNewMySQLQueryFeatures(t *testing.T) {
	qf := NewMySQLQueryFeatures()

	// MySQL 不支持的特性
	unsupported := []string{
		"full_outer_join",
		"except",
		"intersect",
		"order_by_in_aggregate",
		"array_aggregate",
		"fuzzy_match",
	}

	for _, feature := range unsupported {
		if qf.HasQueryFeature(feature) {
			t.Errorf("MySQL should not support %s", feature)
		}
	}

	// MySQL 支持的特性
	supported := []string{
		"in_range",
		"recursive_cte",
		"window_func",
		"string_aggregate",
		"json_operators",
		"regex_match", // MySQL 支持 REGEXP
		"upsert",
	}

	for _, feature := range supported {
		if !qf.HasQueryFeature(feature) {
			t.Errorf("MySQL should support %s", feature)
		}
	}
}

// TestNewSQLiteQueryFeatures 测试 SQLite 查询特性构造器
func TestNewSQLiteQueryFeatures(t *testing.T) {
	qf := NewSQLiteQueryFeatures()

	// SQLite 特定的不支持项
	unsupported := []string{
		"full_outer_join",
		"full_text_search",
		"regex_match",
		"fuzzy_match",
	}

	for _, feature := range unsupported {
		if qf.HasQueryFeature(feature) {
			t.Errorf("SQLite should not support %s", feature)
		}
	}

	// SQLite 应该支持的特性
	supported := []string{
		"recursive_cte",
		"window_func",
		"except",
		"intersect",
		"order_by_in_aggregate",
		"json_operators",
		"upsert",
	}

	for _, feature := range supported {
		if !qf.HasQueryFeature(feature) {
			t.Errorf("SQLite should support %s", feature)
		}
	}
}

// TestNewSQLServerQueryFeatures 测试 SQL Server 查询特性构造器
func TestNewSQLServerQueryFeatures(t *testing.T) {
	qf := NewSQLServerQueryFeatures()

	// SQL Server 特定的不支持项
	unsupported := []string{
		"insert_ignore",
		"limit", // 用 OFFSET...FETCH 代替
		"regex_match",
		"fuzzy_match",
	}

	for _, feature := range unsupported {
		if qf.HasQueryFeature(feature) {
			t.Errorf("SQL Server should not support %s", feature)
		}
	}

	// SQL Server 应该支持的特性
	supported := []string{
		"full_outer_join",
		"recursive_cte",
		"window_func",
		"except",
		"intersect",
		"full_text_search",
		"json_operators",
	}

	for _, feature := range supported {
		if !qf.HasQueryFeature(feature) {
			t.Errorf("SQL Server should support %s", feature)
		}
	}
}

// ==================== HasQueryFeature 方法测试 ====================

// TestHasQueryFeatureAllDatabases 测试所有数据库的特性检查
func TestHasQueryFeatureAllDatabases(t *testing.T) {
	databases := map[string]*QueryFeatures{
		"postgres":   NewPostgreSQLQueryFeatures(),
		"mysql":      NewMySQLQueryFeatures(),
		"sqlite":     NewSQLiteQueryFeatures(),
		"sqlserver":  NewSQLServerQueryFeatures(),
	}

	// 所有数据库都应该支持的基础特性
	baselineFeatures := []string{
		"in_range", "between", "like", "distinct", "group_by", "having",
		"inner_join", "left_join", "right_join", "cross_join", "self_join",
		"cte", "subquery", "correlated_subquery", "union",
		"case", "order_by", "nulls", "cast", "coalesce",
	}

	for dbName, qf := range databases {
		for _, feature := range baselineFeatures {
			if !qf.HasQueryFeature(feature) {
				t.Errorf("%s should support baseline feature %s", dbName, feature)
			}
		}
	}
}

// TestHasQueryFeatureUnknownFeature 测试未知特性
func TestHasQueryFeatureUnknownFeature(t *testing.T) {
	qf := NewPostgreSQLQueryFeatures()
	if qf.HasQueryFeature("unknown_feature_xyz") {
		t.Error("Should return false for unknown feature")
	}
}

// ==================== GetQueryFeatures 全局函数测试 ====================

// TestGetQueryFeaturesAllDatabases 测试 GetQueryFeatures 全局函数
func TestGetQueryFeaturesAllDatabases(t *testing.T) {
	testCases := []struct {
		dbType   string
		expected bool
	}{
		{"postgres", true},
		{"postgresql", true},
		{"mysql", true},
		{"sqlite", true},
		{"sqlserver", true},
		{"unknown_db", false},
	}

	for _, tc := range testCases {
		qf := GetQueryFeatures(tc.dbType)
		hasFeatures := qf != nil && qf.SupportsIN // 检查是否返回有效的 QueryFeatures
		if hasFeatures != tc.expected {
			t.Errorf("GetQueryFeatures(%s) should be valid: %v", tc.dbType, tc.expected)
		}
	}
}

// ==================== GetFallbackStrategy 测试 ====================

// TestGetFallbackStrategy 测试降级策略获取
func TestGetFallbackStrategy(t *testing.T) {
	testCases := []struct {
		name               string
		qf                 *QueryFeatures
		feature            string
		expectedFallback   QueryFallbackStrategy
	}{
		{
			"MySQL FULL_OUTER_JOIN",
			NewMySQLQueryFeatures(),
			"full_outer_join",
			QueryFallbackMultiQuery,
		},
		{
			"SQLite full text search",
			NewSQLiteQueryFeatures(),
			"full_text_search",
			QueryFallbackApplicationLayer,
		},
		{
			"SQL Server LIMIT",
			NewSQLServerQueryFeatures(),
			"limit",
			QueryFallbackAlternativeSyntax,
		},
		{
			"Unknown fallback strategy",
			NewPostgreSQLQueryFeatures(),
			"unknown_feature",
			QueryFallbackNone,
		},
	}

	for _, tc := range testCases {
		actual := tc.qf.GetFallbackStrategy(tc.feature)
		if actual != tc.expectedFallback {
			t.Errorf("%s: expected %s, got %s", tc.name, tc.expectedFallback, actual)
		}
	}
}

// ==================== GetAlternativeSyntax 测试 ====================

// TestGetAlternativeSyntax 测试替代语法获取
func TestGetAlternativeSyntax(t *testing.T) {
	testCases := []struct {
		name                string
		qf                  *QueryFeatures
		feature             string
		shouldHaveSyntax    bool
	}{
		{
			"MySQL FULL_OUTER_JOIN",
			NewMySQLQueryFeatures(),
			"full_outer_join",
			true,
		},
		{
			"SQL Server LIMIT",
			NewSQLServerQueryFeatures(),
			"limit",
			true,
		},
		{
			"PostgreSQL upsert",
			NewPostgreSQLQueryFeatures(),
			"upsert",
			false, // PostgreSQL 无替代语法
		},
	}

	for _, tc := range testCases {
		syntax := tc.qf.GetAlternativeSyntax(tc.feature)
		hasContent := syntax != ""
		if hasContent != tc.shouldHaveSyntax {
			t.Errorf("%s: syntax presence mismatch. Expected: %v, Got: %v", 
				tc.name, tc.shouldHaveSyntax, hasContent)
		}
		if hasContent && len(syntax) < 10 {
			t.Errorf("%s: syntax too short: %s", tc.name, syntax)
		}
	}
}

// ==================== GetFeatureNote 测试 ====================

// TestGetFeatureNote 测试特性说明获取
func TestGetFeatureNote(t *testing.T) {
	testCases := []struct {
		name            string
		qf              *QueryFeatures
		feature         string
		shouldHaveNote  bool
	}{
		{
			"MySQL FULL_OUTER_JOIN note",
			NewMySQLQueryFeatures(),
			"full_outer_join",
			true,
		},
		{
			"SQLite FTS note",
			NewSQLiteQueryFeatures(),
			"full_text_search",
			true,
		},
		{
			"Unknown note",
			NewPostgreSQLQueryFeatures(),
			"nonexistent_feature",
			false,
		},
	}

	for _, tc := range testCases {
		note := tc.qf.GetFeatureNote(tc.feature)
		hasContent := note != ""
		if hasContent != tc.shouldHaveNote {
			t.Errorf("%s: note presence mismatch. Expected: %v, Got: %v",
				tc.name, tc.shouldHaveNote, hasContent)
		}
	}
}

// ==================== CompareQueryFeatures 测试 ====================

// TestCompareQueryFeaturesPostgresVsMysql 测试 PostgreSQL vs MySQL 对比
func TestCompareQueryFeaturesPostgresVsMysql(t *testing.T) {
	pg := NewPostgreSQLQueryFeatures()
	mysql := NewMySQLQueryFeatures()

	comparison := CompareQueryFeatures(pg, mysql)

	// 验证返回结构
	if _, hasCommon := comparison["common_features"]; !hasCommon {
		t.Error("Comparison should have common_features")
	}
	if _, hasFirst := comparison["only_in_first"]; !hasFirst {
		t.Error("Comparison should have only_in_first")
	}
	if _, hasSecond := comparison["only_in_second"]; !hasSecond {
		t.Error("Comparison should have only_in_second")
	}

	// PostgreSQL 应该有 MySQL 没有的特性
	onlyInPG := comparison["only_in_first"].([]string)
	if len(onlyInPG) == 0 {
		t.Error("PostgreSQL should have exclusive features over MySQL")
	}

	// 验证 full_outer_join 在 PostgreSQL 独有列表中
	found := false
	for _, f := range onlyInPG {
		if f == "full_outer_join" {
			found = true
			break
		}
	}
	if !found {
		t.Error("full_outer_join should be only in PostgreSQL")
	}
}

// TestCompareQueryFeaturesSQLiteVsPostgres 测试 SQLite vs PostgreSQL 对比
func TestCompareQueryFeaturesSQLiteVsPostgres(t *testing.T) {
	sqlite := NewSQLiteQueryFeatures()
	pg := NewPostgreSQLQueryFeatures()

	comparison := CompareQueryFeatures(sqlite, pg)

	// PostgreSQL 应该有更多特性
	commonFeatures := comparison["common_features"].([]string)
	onlyInPG := comparison["only_in_second"].([]string)
	onlyInSQLite := comparison["only_in_first"].([]string)

	if len(onlyInPG) <= len(onlyInSQLite) {
		t.Errorf("PostgreSQL should have more exclusive features than SQLite. PG: %d, SQLite: %d",
			len(onlyInPG), len(onlyInSQLite))
	}

	if len(commonFeatures) == 0 {
		t.Error("SQLite and PostgreSQL should have common features")
	}
}

// TestCompareQueryFeaturesIdentical 测试相同数据库对比
func TestCompareQueryFeaturesIdentical(t *testing.T) {
	mysql1 := NewMySQLQueryFeatures()
	mysql2 := NewMySQLQueryFeatures()

	comparison := CompareQueryFeatures(mysql1, mysql2)

	onlyInFirst := comparison["only_in_first"].([]string)
	onlyInSecond := comparison["only_in_second"].([]string)

	if len(onlyInFirst) != 0 || len(onlyInSecond) != 0 {
		t.Error("Identical databases should have no exclusive features")
	}

	common := comparison["common_features"].([]string)
	if len(common) == 0 {
		t.Error("Identical databases should have all features in common")
	}
}

// ==================== PrintQueryFeatureMatrix 测试 ====================

// TestPrintQueryFeatureMatrix 测试特性矩阵打印
func TestPrintQueryFeatureMatrix(t *testing.T) {
	matrix := PrintQueryFeatureMatrix()

	// 验证输出格式
	if !strings.Contains(matrix, "特性\\数据库") {
		t.Error("Matrix should contain header")
	}

	if !strings.Contains(matrix, "postgres") {
		t.Error("Matrix should contain PostgreSQL")
	}

	if !strings.Contains(matrix, "mysql") {
		t.Error("Matrix should contain MySQL")
	}

	if !strings.Contains(matrix, "sqlite") {
		t.Error("Matrix should contain SQLite")
	}

	if !strings.Contains(matrix, "sqlserver") {
		t.Error("Matrix should contain SQL Server")
	}

	// 验证有 checkmark
	if !strings.Contains(matrix, "✅") {
		t.Error("Matrix should contain supported features")
	}

	// 验证有不支持的标记
	if !strings.Contains(matrix, "❌") && !strings.Contains(matrix, "⚠️") {
		t.Error("Matrix should show unsupported features")
	}
}

// TestPrintQueryFeatureMatrixCustomDatabases 测试自定义数据库的矩阵
func TestPrintQueryFeatureMatrixCustomDatabases(t *testing.T) {
	matrix := PrintQueryFeatureMatrix("postgres", "sqlite")

	// 应该包含请求的数据库名
	if !strings.Contains(matrix, "postgres") {
		t.Error("Matrix should contain postgres database")
	}

	if !strings.Contains(matrix, "sqlite") {
		t.Error("Matrix should contain sqlite database")
	}

	// 验证矩阵有内容
	lines := strings.Split(matrix, "\n")
	if len(lines) < 5 {
		t.Errorf("Matrix should have multiple lines, got %d", len(lines))
	}
}

// ==================== 特性检查的边界情况 ====================

// TestQueryFeaturesEdgeCases 测试边界情况
func TestQueryFeaturesEdgeCases(t *testing.T) {
	qf := NewPostgreSQLQueryFeatures()

	// 空字符串特性
	if qf.HasQueryFeature("") {
		t.Error("Empty feature name should return false")
	}

	// 特大字符串
	longFeature := strings.Repeat("x", 1000)
	if qf.HasQueryFeature(longFeature) {
		t.Error("Very long feature name should return false")
	}

	// 特殊字符
	specialFeatures := []string{
		"feature@123",
		"feature#456",
		"feature$789",
		"feature!test",
	}

	for _, feature := range specialFeatures {
		if qf.HasQueryFeature(feature) {
			t.Errorf("Special character feature %s should return false", feature)
		}
	}
}

// ==================== 数据一致性检查 ====================

// TestQueryFeaturesConsistency 测试特性数据一致性
func TestQueryFeaturesConsistency(t *testing.T) {
	databases := []string{"postgres", "mysql", "sqlite", "sqlserver"}

	for _, dbType := range databases {
		qf := GetQueryFeatures(dbType)
		
		// 检查布尔字段的一致性
		// 注："limit"不是所有数据库都支持（SQL Server用OFFSET...FETCH）
		supported := []string{
			"in_range", "between", "like", "distinct",
			"inner_join", "left_join", "cte", "subquery",
			"case", "order_by",
		}

		for _, feature := range supported {
			// 所有数据库都应该支持这些基础特性
			if !qf.HasQueryFeature(feature) {
				t.Errorf("%s should support baseline feature %s", dbType, feature)
			}
		}

		// 检查是否存在不一致的降级策略
		fallbacks := qf.FallbackStrategies
		for feature, fallback := range fallbacks {
			// 检查是否有实际的降级策略
			if fallback == QueryFallbackNone {
				// 这个特性不应该有降级策略条目
				if qf.HasQueryFeature(feature) {
					t.Errorf("%s has QueryFallbackNone for supported feature %s", dbType, feature)
				}
			}
		}
	}
}

// ==================== 性能基准测试 ====================

// BenchmarkHasQueryFeature 基准测试特性检查
func BenchmarkHasQueryFeature(b *testing.B) {
	qf := NewPostgreSQLQueryFeatures()
	features := []string{
		"in_range", "recursive_cte", "window_func", "full_outer_join",
		"full_text_search", "json_operators", "upsert",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, feature := range features {
			qf.HasQueryFeature(feature)
		}
	}
}

// BenchmarkCompareQueryFeatures 基准测试特性对比
func BenchmarkCompareQueryFeatures(b *testing.B) {
	pg := NewPostgreSQLQueryFeatures()
	mysql := NewMySQLQueryFeatures()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompareQueryFeatures(pg, mysql)
	}
}

// BenchmarkGetFallbackStrategy 基准测试降级策略获取
func BenchmarkGetFallbackStrategy(b *testing.B) {
	qf := NewMySQLQueryFeatures()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qf.GetFallbackStrategy("full_outer_join")
		qf.GetFallbackStrategy("except")
		qf.GetFallbackStrategy("intersect")
	}
}

// ==================== 高级覆盖测试 ====================

// TestPrintQueryFeatureMatrixWithFallback 测试矩阵中的fallback显示
func TestPrintQueryFeatureMatrixWithFallback(t *testing.T) {
	// MySQL缺少一些特性，应该显示⚠️
	matrix := PrintQueryFeatureMatrix("mysql")

	if !strings.Contains(matrix, "⚠️") {
		t.Error("Matrix should show warning symbols for features with fallbacks")
	}

	// 验证矩阵包含fallback策略标记
	if !strings.Contains(matrix, "multi_query") && !strings.Contains(matrix, "application_layer") {
		t.Error("Matrix should show fallback strategy names")
	}
}

// TestPrintQueryFeatureMatrixWithUnsupported 测试矩阵中的不支持特性
func TestPrintQueryFeatureMatrixWithUnsupported(t *testing.T) {
	// SQLite缺少一些特性，应该显示❌和⚠️
	matrix := PrintQueryFeatureMatrix("sqlite")

	if !strings.Contains(matrix, "❌") && !strings.Contains(matrix, "⚠️") {
		t.Error("Matrix should show unsupported feature markers for SQLite")
	}

	// 应该包含sqlite列
	if !strings.Contains(matrix, "sqlite") {
		t.Error("Matrix should contain sqlite database name")
	}
}

// TestQueryFeatureMatrixAllDatabases 测试所有数据库的完整矩阵
func TestQueryFeatureMatrixAllDatabases(t *testing.T) {
	matrix := PrintQueryFeatureMatrix()

	// 应该包含所有4个数据库
	dbs := []string{"postgres", "mysql", "sqlite", "sqlserver"}
	for _, db := range dbs {
		if !strings.Contains(matrix, db) {
			t.Errorf("Matrix should contain %s database", db)
		}
	}

	// 应该包含支持特性的标记
	if !strings.Contains(matrix, "✅") {
		t.Error("Matrix should have supported features (✅)")
	}

	// 应该包含fallback特性的标记
	if !strings.Contains(matrix, "⚠️") {
		t.Error("Matrix should have fallback features (⚠️)")
	}

	// 验证矩阵有足够的内容
	lines := strings.Split(matrix, "\n")
	if len(lines) < 15 {
		t.Errorf("Matrix should have multiple feature rows, got %d lines", len(lines))
	}
}

// TestPrintQueryFeatureMatrixEmptyDatabases 测试没有参数时使用默认数据库
func TestPrintQueryFeatureMatrixEmptyDatabases(t *testing.T) {
	// 不传递参数，应该使用默认的4个数据库
	matrix := PrintQueryFeatureMatrix()
	
	// 应该包含默认的4个数据库
	defaultDbs := []string{"postgres", "mysql", "sqlite", "sqlserver"}
	for _, db := range defaultDbs {
		if !strings.Contains(matrix, db) {
			t.Errorf("Default matrix should contain %s", db)
		}
	}

	// 也可以使用PrintQueryFeatureMatrix()和PrintQueryFeatureMatrix("")都应该有默认行为
	matrix2 := PrintQueryFeatureMatrix("postgres")
	if !strings.Contains(matrix2, "postgres") {
		t.Error("Single database matrix should contain postgres")
	}
}
