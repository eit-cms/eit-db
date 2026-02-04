package db

import (
	"testing"
)

// ==================== View 支持测试 ====================

// TestViewSupport 测试数据库的View支持能力
func TestViewSupport(t *testing.T) {
	tests := []struct {
		name                 string
		dbType               string
		expectedView         bool
		expectedMaterialized bool
		expectedPreload      bool
	}{
		{
			name:                 "PostgreSQL View Support",
			dbType:               "postgres",
			expectedView:         true,
			expectedMaterialized: true,
			expectedPreload:      true,
		},
		{
			name:                 "MySQL View Support",
			dbType:               "mysql",
			expectedView:         true,
			expectedMaterialized: false,
			expectedPreload:      true,
		},
		{
			name:                 "SQLite View Support",
			dbType:               "sqlite",
			expectedView:         true,
			expectedMaterialized: false,
			expectedPreload:      true,
		},
		{
			name:                 "SQL Server View Support",
			dbType:               "sqlserver",
			expectedView:         true,
			expectedMaterialized: false,
			expectedPreload:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qf := GetQueryFeatures(tt.dbType)

			if qf.SupportsView != tt.expectedView {
				t.Errorf("SupportsView=%v, expected %v", qf.SupportsView, tt.expectedView)
			}

			if qf.SupportsMaterializedView != tt.expectedMaterialized {
				t.Errorf("SupportsMaterializedView=%v, expected %v", qf.SupportsMaterializedView, tt.expectedMaterialized)
			}

			if qf.SupportsViewForPreload != tt.expectedPreload {
				t.Errorf("SupportsViewForPreload=%v, expected %v", qf.SupportsViewForPreload, tt.expectedPreload)
			}
		})
	}
}

// ==================== 多 Adapter 路由优化测试 ====================

// TestSearchOptimization 测试Search操作的Adapter优先级
func TestSearchOptimization(t *testing.T) {
	postgres := NewPostgreSQLQueryFeatures()
	mysql := NewMySQLQueryFeatures()
	sqlite := NewSQLiteQueryFeatures()

	// PostgreSQL 和 MySQL 都支持 search，但优先级不同
	if !postgres.SearchOptimizationSupported {
		t.Error("PostgreSQL should support search optimization")
	}
	if !mysql.SearchOptimizationSupported {
		t.Error("MySQL should support search optimization")
	}
	if sqlite.SearchOptimizationSupported {
		t.Error("SQLite should not support search optimization")
	}

	// PostgreSQL 优先级应该比 MySQL 高 (数字越小优先级越高)
	if postgres.SearchOptimizationPriority >= mysql.SearchOptimizationPriority {
		t.Errorf("PostgreSQL priority (%d) should be higher than MySQL (%d)",
			postgres.SearchOptimizationPriority, mysql.SearchOptimizationPriority)
	}

	// MySQL 优先级应该比 SQLite 高
	if mysql.SearchOptimizationPriority >= sqlite.SearchOptimizationPriority {
		t.Errorf("MySQL priority (%d) should be higher than SQLite (%d)",
			mysql.SearchOptimizationPriority, sqlite.SearchOptimizationPriority)
	}
}

// TestRecursiveOptimization 测试Recursive查询的Adapter优先级
func TestRecursiveOptimization(t *testing.T) {
	postgres := NewPostgreSQLQueryFeatures()
	mysql := NewMySQLQueryFeatures()
	sqlserver := NewSQLServerQueryFeatures()

	// 都支持递归查询
	if !postgres.RecursiveOptimizationSupported {
		t.Error("PostgreSQL should support recursive optimization")
	}
	if !mysql.RecursiveOptimizationSupported {
		t.Error("MySQL should support recursive optimization")
	}
	if !sqlserver.RecursiveOptimizationSupported {
		t.Error("SQL Server should support recursive optimization")
	}

	// SQL Server 应该是递归查询的最优选择
	if !sqlserver.RecursiveOptimizationIsOptimal {
		t.Error("SQL Server should be optimal for recursive queries")
	}
	if postgres.RecursiveOptimizationIsOptimal {
		t.Error("PostgreSQL should not be optimal for recursive (SQL Server is better)")
	}
	if mysql.RecursiveOptimizationIsOptimal {
		t.Error("MySQL should not be optimal for recursive")
	}

	// 优先级检查 (SQL Server > PostgreSQL > MySQL)
	if sqlserver.RecursiveOptimizationPriority >= postgres.RecursiveOptimizationPriority {
		t.Error("SQL Server priority should be better than PostgreSQL")
	}
	if postgres.RecursiveOptimizationPriority >= mysql.RecursiveOptimizationPriority {
		t.Error("PostgreSQL priority should be better than MySQL")
	}

	// 原生语法检查
	if !sqlserver.RecursiveOptimizationHasNativeSyntax {
		t.Error("SQL Server should have native recursive syntax")
	}
	if !postgres.RecursiveOptimizationHasNativeSyntax {
		t.Error("PostgreSQL should have native recursive syntax")
	}
	if !mysql.RecursiveOptimizationHasNativeSyntax {
		t.Error("MySQL should have native recursive syntax")
	}
}

// TestAdapterTags 测试Adapter功能标签
func TestAdapterTags(t *testing.T) {
	postgres := NewPostgreSQLQueryFeatures()
	mysql := NewMySQLQueryFeatures()
	sqlite := NewSQLiteQueryFeatures()
	sqlserver := NewSQLServerQueryFeatures()

	// PostgreSQL 应该有多个标签
	if len(postgres.AdapterTags) < 3 {
		t.Errorf("PostgreSQL should have at least 3 tags, got %d", len(postgres.AdapterTags))
	}

	// SQLite 应该标记为 embedded
	hasEmbedded := false
	for _, tag := range sqlite.AdapterTags {
		if tag == "embedded" {
			hasEmbedded = true
			break
		}
	}
	if !hasEmbedded {
		t.Error("SQLite should have 'embedded' tag")
	}

	// SQL Server 应该标记为 enterprise
	hasEnterprise := false
	for _, tag := range sqlserver.AdapterTags {
		if tag == "enterprise" {
			hasEnterprise = true
			break
		}
	}
	if !hasEnterprise {
		t.Error("SQL Server should have 'enterprise' tag")
	}

	// 所有都应该有 relational 标签
	expectTag := func(qf *QueryFeatures, tag string, name string) {
		hasTag := false
		for _, t := range qf.AdapterTags {
			if t == tag {
				hasTag = true
				break
			}
		}
		if !hasTag {
			t.Errorf("%s should have '%s' tag", name, tag)
		}
	}

	expectTag(postgres, "relational", "PostgreSQL")
	expectTag(mysql, "relational", "MySQL")
	expectTag(sqlite, "relational", "SQLite") // 虽然是 embedded，但也是 relational
	expectTag(sqlserver, "relational", "SQL Server")
}

// TestOptimizationNotes 测试优化说明
func TestOptimizationNotes(t *testing.T) {
	postgres := NewPostgreSQLQueryFeatures()
	mysql := NewMySQLQueryFeatures()
	sqlite := NewSQLiteQueryFeatures()
	sqlserver := NewSQLServerQueryFeatures()

	// PostgreSQL 应该有优化说明
	if len(postgres.OptimizationNotes) == 0 {
		t.Error("PostgreSQL should have optimization notes")
	}

	// MySQL 应该有 limited_view 说明
	if note, ok := mysql.OptimizationNotes["limited_view"]; !ok || note == "" {
		t.Error("MySQL should have limited_view optimization note")
	}

	// SQLite 应该有 limited_features 说明
	if note, ok := sqlite.OptimizationNotes["limited_features"]; !ok || note == "" {
		t.Error("SQLite should have limited_features optimization note")
	}

	// SQL Server 应该有 recursive_optimal 说明
	if note, ok := sqlserver.OptimizationNotes["recursive_optimal"]; !ok || note == "" {
		t.Error("SQL Server should have recursive_optimal optimization note")
	}
}

// TestMultiAdapterRouting 测试多Adapter路由场景
func TestMultiAdapterRouting(t *testing.T) {
	// 模拟多Adapter激活场景
	postgres := NewPostgreSQLQueryFeatures()
	sqlserver := NewSQLServerQueryFeatures()

	// 场景1: 执行递归查询，应该选择 SQL Server
	if !sqlserver.RecursiveOptimizationIsOptimal {
		t.Error("Should route recursive queries to SQL Server when available")
	}
	if sqlserver.RecursiveOptimizationPriority > postgres.RecursiveOptimizationPriority {
		t.Errorf("SQL Server (%d) should have better priority than PostgreSQL (%d) for recursive",
			sqlserver.RecursiveOptimizationPriority, postgres.RecursiveOptimizationPriority)
	}

	// 场景2: 执行普通查询，可以用当前adapter
	if !postgres.SupportsIN && !postgres.SupportsBetween && !postgres.SupportsGroupBy {
		t.Error("PostgreSQL should support basic query operations")
	}

	// 场景3: PostgreSQL 的View支持更好
	if !postgres.SupportsMaterializedView && !sqlserver.SupportsMaterializedView {
		t.Error("Should have at least one adapter with materialized view support")
	}
	if postgres.SupportsMaterializedView != true {
		t.Error("PostgreSQL should support materialized views for complex preload")
	}
}
