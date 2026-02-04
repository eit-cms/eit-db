package db

import (
	"fmt"
	"strings"
)

// ==================== Query Feature Categories ====================

// QueryFeatureCategory 查询特性分类
type QueryFeatureCategory string

const (
	// 基础查询操作
	QueryCategoryBasicOps QueryFeatureCategory = "basic_operations"
	// JOIN 操作
	QueryCategoryJoinOps QueryFeatureCategory = "join_operations"
	// 高级查询特性
	QueryCategoryAdvancedQueries QueryFeatureCategory = "advanced_queries"
	// 聚合和分析
	QueryCategoryAggregation QueryFeatureCategory = "aggregation"
	// 文本搜索
	QueryCategoryTextSearch QueryFeatureCategory = "text_search"
	// JSON 相关操作
	QueryCategoryJSON QueryFeatureCategory = "json_operations"
)

// ==================== Query Fallback Strategies ====================

// QueryFallbackStrategy 查询特性不支持时的降级策略
type QueryFallbackStrategy string

const (
	// 不支持，无替代方案
	QueryFallbackNone QueryFallbackStrategy = "none"
	// 在应用层处理（客户端过滤/排序等）
	QueryFallbackApplicationLayer QueryFallbackStrategy = "application_layer"
	// 使用替代语法（例如 SQL Server vs PostgreSQL）
	QueryFallbackAlternativeSyntax QueryFallbackStrategy = "alternative_syntax"
	// 分解为多个简单查询
	QueryFallbackMultiQuery QueryFallbackStrategy = "multi_query"
	// 使用临时表
	QueryFallbackTemporaryTable QueryFallbackStrategy = "temporary_table"
)

// ==================== Query Feature Definitions ====================

// QueryFeature 单个查询特性描述
type QueryFeature struct {
	Name      string                  // 特性名称（如 "IN_RANGE_QUERY"）
	Supported bool                    // 是否支持
	Fallback  QueryFallbackStrategy   // 不支持时的降级策略
	Notes     string                  // 备注说明
	SQLExamples map[string]string     // 各数据库的 SQL 示例 (数据库名 => SQL)
}

// FeatureSupport 特性支持建模（支持版本与备注）
type FeatureSupport struct {
	Supported  bool   // 是否支持
	MinVersion string // 最低支持版本（可选）
	Notes      string // 备注说明（可选）
}

// ==================== QueryFeatures 完整特性集 ====================

// QueryFeatures 数据库查询特性集合
type QueryFeatures struct {
	// 基础查询特性
	SupportsIN            bool   // IN (value1, value2, ...) 范围查询
	SupportsNotIN         bool   // NOT IN 查询
	SupportsBetween       bool   // BETWEEN 查询
	SupportsLike          bool   // LIKE 模式匹配
	SupportsDistinct      bool   // DISTINCT 去重
	SupportsGroupBy       bool   // GROUP BY 分组
	SupportsHaving        bool   // HAVING 条件过滤

	// JOIN 操作
	SupportsInnerJoin     bool   // INNER JOIN
	SupportsLeftJoin      bool   // LEFT JOIN
	SupportsRightJoin     bool   // RIGHT JOIN
	SupportsCrossJoin     bool   // CROSS JOIN
	SupportsFullOuterJoin bool   // FULL OUTER JOIN
	SupportsSelfJoin      bool   // 自连接

	// 高级查询特性
	SupportsCTE           bool   // 公用表表达式 (WITH ... AS)
	SupportsRecursiveCTE  bool   // 递归 CTE
	SupportsWindowFunc    bool   // 窗口函数 (ROW_NUMBER, RANK, etc)
	SupportsSubquery      bool   // 子查询
	SupportsCorrelatedSubquery bool // 关联子查询
	SupportsUnion         bool   // UNION / UNION ALL
	SupportsExcept        bool   // EXCEPT / MINUS
	SupportsIntersect     bool   // INTERSECT

	// 聚合和分析函数
	SupportsOrderByInAggregate bool // 聚合函数中的 ORDER BY (如 STRING_AGG(...ORDER BY...))
	SupportsArrayAggregate bool    // 数组聚合
	SupportsStringAggregate bool   // 字符串聚合

	// 文本搜索
	SupportsFullTextSearch bool   // 全文搜索
	SupportsRegexMatch     bool   // 正则表达式匹配
	SupportsFuzzyMatch     bool   // 模糊匹配

	// JSON 操作
	SupportsJSONPath       bool   // JSON 路径提取
	SupportsJSONType       bool   // JSON 数据类型
	SupportsJSONOperators  bool   // JSON 运算符
	SupportsJSONAgg        bool   // JSON 聚合

	// 案例表达式
	SupportsCase           bool   // CASE WHEN THEN ELSE END
	SupportsCaseWithElse   bool   // CASE 带 ELSE

	// 其他特性
	SupportsLimit          bool   // LIMIT 限制行数
	SupportsOffset         bool   // OFFSET 偏移
	SupportsOrderBy        bool   // ORDER BY 排序
	SupportsNulls          bool   // IS NULL / IS NOT NULL
	SupportsCastType       bool   // CAST(...AS type)
	SupportsCoalesce       bool   // COALESCE 函数

	// 特殊数据库特性
	SupportsIfExists       bool   // IF EXISTS 子句
	SupportsInsertIgnore   bool   // INSERT IGNORE (MySQL) 或 ON CONFLICT (PostgreSQL)
	SupportsUpsert         bool   // INSERT ... ON DUPLICATE KEY UPDATE 或 ON CONFLICT

	// ==================== VIEW 支持 ====================
	SupportsView               bool   // 是否支持 VIEW
	SupportsMaterializedView   bool   // 物化视图支持（如 PostgreSQL）
	SupportsViewForPreload     bool   // 是否支持用 VIEW 实现 Preload 优化

	// ==================== 多 Adapter 路由优化信息 ====================
	// Search 操作优化
	SearchOptimizationSupported    bool   // 该 adapter 是否支持 search
	SearchOptimizationIsOptimal    bool   // 是否是 search 的最优 adapter
	SearchOptimizationPriority     int    // 路由优先级 (1=最优, 2=次优, 3=备选)

	// Recursive 查询优化
	RecursiveOptimizationSupported     bool   // 是否支持递归查询
	RecursiveOptimizationIsOptimal     bool   // 是否是递归查询的最优 adapter
	RecursiveOptimizationPriority      int    // 路由优先级
	RecursiveOptimizationHasNativeSyntax bool  // 是否有原生递归语法（否则需要补偿）

	// Adapter 功能标签
	AdapterTags []string // 功能标签，如 ["text_search", "graph", "relational", "time_series"]

	// 优化说明
	OptimizationNotes map[string]string // 各种优化信息的说明

	// 映射：特性名 => 降级策略
	FallbackStrategies map[string]QueryFallbackStrategy
	// 特性说明
	FeatureNotes map[string]string
	// 替代语法映射
	AlternativeSyntax map[string]string

	// 版本化特性支持（可选）
	FeatureSupport map[string]FeatureSupport
}

// ==================== Default Feature Sets ====================

// NewPostgreSQLQueryFeatures PostgreSQL 查询特性
func NewPostgreSQLQueryFeatures() *QueryFeatures {
	return &QueryFeatures{
		// 基础特性 - 全部支持
		SupportsIN:            true,
		SupportsNotIN:         true,
		SupportsBetween:       true,
		SupportsLike:          true,
		SupportsDistinct:      true,
		SupportsGroupBy:       true,
		SupportsHaving:        true,

		// JOIN 操作 - 全部支持
		SupportsInnerJoin:     true,
		SupportsLeftJoin:      true,
		SupportsRightJoin:     true,
		SupportsCrossJoin:     true,
		SupportsFullOuterJoin: true,
		SupportsSelfJoin:      true,

		// 高级特性 - 全部支持
		SupportsCTE:           true,
		SupportsRecursiveCTE:  true,
		SupportsWindowFunc:    true,
		SupportsSubquery:      true,
		SupportsCorrelatedSubquery: true,
		SupportsUnion:         true,
		SupportsExcept:        true,
		SupportsIntersect:     true,

		// 聚合 - 全部支持
		SupportsOrderByInAggregate: true,
		SupportsArrayAggregate:     true,
		SupportsStringAggregate:    true,

		// 文本搜索 - 全部支持
		SupportsFullTextSearch:     true,
		SupportsRegexMatch:         true,
		SupportsFuzzyMatch:         true,

		// JSON - 全部支持
		SupportsJSONPath:       true,
		SupportsJSONType:       true,
		SupportsJSONOperators:  true,
		SupportsJSONAgg:        true,

		// 其他 - 全部支持
		SupportsCase:           true,
		SupportsCaseWithElse:   true,
		SupportsLimit:          true,
		SupportsOffset:         true,
		SupportsOrderBy:        true,
		SupportsNulls:          true,
		SupportsCastType:       true,
		SupportsCoalesce:       true,
		SupportsIfExists:       true,
		SupportsInsertIgnore:   true,
		SupportsUpsert:         true,

		// VIEW 支持
		SupportsView:               true,
		SupportsMaterializedView:   true,  // PostgreSQL 支持物化视图
		SupportsViewForPreload:     true,  // 可用于 Preload 优化

		// 多 Adapter 优化
		SearchOptimizationSupported:    true,
		SearchOptimizationIsOptimal:    false,
		SearchOptimizationPriority:     2,
		RecursiveOptimizationSupported:     true,
		RecursiveOptimizationIsOptimal:     false,
		RecursiveOptimizationPriority:      2,
		RecursiveOptimizationHasNativeSyntax: true,

		AdapterTags: []string{"relational", "full_text_search", "json", "array"},

		OptimizationNotes: map[string]string{
			"view_for_preload": "使用物化视图可以优化复杂的Preload操作",
		},

		FallbackStrategies: map[string]QueryFallbackStrategy{},
		FeatureNotes: map[string]string{
			"recursive_cte": "支持 WITH RECURSIVE",
			"window_func": "支持 OVER() 子句和所有窗口函数",
			"array_aggregate": "支持 ARRAY_AGG() 聚合",
		},
		AlternativeSyntax: map[string]string{},
		FeatureSupport: map[string]FeatureSupport{},
	}
}

// NewMySQLQueryFeatures MySQL 查询特性
func NewMySQLQueryFeatures() *QueryFeatures {
	return &QueryFeatures{
		// 基础特性 - 全部支持
		SupportsIN:            true,
		SupportsNotIN:         true,
		SupportsBetween:       true,
		SupportsLike:          true,
		SupportsDistinct:      true,
		SupportsGroupBy:       true,
		SupportsHaving:        true,

		// JOIN 操作 - 全部支持
		SupportsInnerJoin:     true,
		SupportsLeftJoin:      true,
		SupportsRightJoin:     true,
		SupportsCrossJoin:     true,
		SupportsFullOuterJoin: false, // ❌ MySQL 不支持 FULL OUTER JOIN
		SupportsSelfJoin:      true,

		// 高级特性
		SupportsCTE:           true,  // ✅ MySQL 8.0+ 支持
		SupportsRecursiveCTE:  true,  // ✅ MySQL 8.0+ 支持
		SupportsWindowFunc:    true,  // ✅ MySQL 8.0+ 支持
		SupportsSubquery:      true,
		SupportsCorrelatedSubquery: true,
		SupportsUnion:         true,
		SupportsExcept:        false, // ❌ MySQL 不支持 EXCEPT
		SupportsIntersect:     false, // ❌ MySQL 不支持 INTERSECT

		// 聚合
		SupportsOrderByInAggregate: false, // ❌ MySQL 不支持聚合函数中的 ORDER BY
		SupportsArrayAggregate:     false, // ❌ MySQL 无原生数组类型
		SupportsStringAggregate:    true,  // ✅ GROUP_CONCAT()

		// 文本搜索
		SupportsFullTextSearch:     true,
		SupportsRegexMatch:         true,
		SupportsFuzzyMatch:         false, // ❌ MySQL 不原生支持模糊匹配

		// JSON - MySQL 5.7+ 支持
		SupportsJSONPath:       true,
		SupportsJSONType:       true,
		SupportsJSONOperators:  true,
		SupportsJSONAgg:        true,  // ✅ MySQL 5.7+

		// 其他
		SupportsCase:           true,
		SupportsCaseWithElse:   true,
		SupportsLimit:          true,
		SupportsOffset:         true,
		SupportsOrderBy:        true,
		SupportsNulls:          true,
		SupportsCastType:       true,
		SupportsCoalesce:       true,
		SupportsIfExists:       true,
		SupportsInsertIgnore:   true,  // ✅ INSERT IGNORE
		SupportsUpsert:         true,  // ✅ INSERT ... ON DUPLICATE KEY UPDATE

		// VIEW 支持
		SupportsView:               true,
		SupportsMaterializedView:   false,
		SupportsViewForPreload:     true,  // 可用于简单的 Preload

		// 多 Adapter 优化
		SearchOptimizationSupported:    true,
		SearchOptimizationIsOptimal:    false,
		SearchOptimizationPriority:     3,
		RecursiveOptimizationSupported:     true,
		RecursiveOptimizationIsOptimal:     false,
		RecursiveOptimizationPriority:      3,
		RecursiveOptimizationHasNativeSyntax: true, // MySQL 8.0+ WITH RECURSIVE

		AdapterTags: []string{"relational", "full_text_search"},

		OptimizationNotes: map[string]string{
			"limited_view": "VIEW 功能有限，可能需要应用层处理",
		},

		FallbackStrategies: map[string]QueryFallbackStrategy{
			"full_outer_join":           QueryFallbackMultiQuery,    // 用 LEFT JOIN + RIGHT JOIN + UNION
			"except":                    QueryFallbackMultiQuery,    // 用 NOT IN 或 NOT EXISTS
			"intersect":                 QueryFallbackAlternativeSyntax, // 用 INNER JOIN
			"order_by_in_aggregate":     QueryFallbackApplicationLayer,
		},
		FeatureNotes: map[string]string{
			"full_outer_join": "MySQL 不支持，可用 LEFT JOIN ... UNION ... RIGHT JOIN 模拟",
			"except": "MySQL 不支持，可用 NOT IN 或 NOT EXISTS",
			"recursive_cte": "MySQL 8.0+ 支持",
			"window_func": "MySQL 8.0+ 支持",
			"order_by_in_aggregate": "MySQL 不支持聚合函数中的 ORDER BY",
		},
		AlternativeSyntax: map[string]string{
			"full_outer_join": "SELECT ... FROM a LEFT JOIN b ... UNION SELECT ... FROM a RIGHT JOIN b ...",
			"upsert": "INSERT INTO table (...) VALUES (...) ON DUPLICATE KEY UPDATE ...",
		},
		FeatureSupport: map[string]FeatureSupport{
			"cte": {Supported: true, MinVersion: "8.0"},
			"recursive_cte": {Supported: true, MinVersion: "8.0"},
			"window_func": {Supported: true, MinVersion: "8.0"},
			"json_path": {Supported: true, MinVersion: "5.7"},
			"json_type": {Supported: true, MinVersion: "5.7"},
			"json_agg": {Supported: true, MinVersion: "5.7"},
		},
	}
}

// NewSQLiteQueryFeatures SQLite 查询特性
func NewSQLiteQueryFeatures() *QueryFeatures {
	return &QueryFeatures{
		// 基础特性 - 全部支持
		SupportsIN:            true,
		SupportsNotIN:         true,
		SupportsBetween:       true,
		SupportsLike:          true,
		SupportsDistinct:      true,
		SupportsGroupBy:       true,
		SupportsHaving:        true,

		// JOIN 操作 - 全部支持
		SupportsInnerJoin:     true,
		SupportsLeftJoin:      true,
		SupportsRightJoin:     true,
		SupportsCrossJoin:     true,
		SupportsFullOuterJoin: false, // ❌ SQLite 不支持
		SupportsSelfJoin:      true,

		// 高级特性
		SupportsCTE:           true,  // ✅ SQLite 3.8.4+ 支持
		SupportsRecursiveCTE:  true,  // ✅ SQLite 3.8.4+ 支持
		SupportsWindowFunc:    true,  // ✅ SQLite 3.25.0+ 支持
		SupportsSubquery:      true,
		SupportsCorrelatedSubquery: true,
		SupportsUnion:         true,
		SupportsExcept:        true,  // ✅ EXCEPT
		SupportsIntersect:     true,  // ✅ INTERSECT

		// 聚合
		SupportsOrderByInAggregate: true,  // ✅ SQLite 3.30+ 支持 ORDER BY in aggregate
		SupportsArrayAggregate:     false, // ❌ SQLite 无原生数组
		SupportsStringAggregate:    true,  // ✅ GROUP_CONCAT()

		// 文本搜索
		SupportsFullTextSearch:     false, // ❌ 需要 FTS 扩展
		SupportsRegexMatch:         false, // ❌ 需要 REGEXP 函数注册
		SupportsFuzzyMatch:         false, // ❌ 不支持

		// JSON
		SupportsJSONPath:       true,  // ✅ SQLite 3.9.0+
		SupportsJSONType:       false, // ❌ 文本存储
		SupportsJSONOperators:  true,  // ✅ JSON 函数
		SupportsJSONAgg:        true,  // ✅ JSON_GROUP_ARRAY()

		// 其他
		SupportsCase:           true,
		SupportsCaseWithElse:   true,
		SupportsLimit:          true,
		SupportsOffset:         true,
		SupportsOrderBy:        true,
		SupportsNulls:          true,
		SupportsCastType:       true,
		SupportsCoalesce:       true,
		SupportsIfExists:       true,
		SupportsInsertIgnore:   false, // ❌ 用 INSERT OR IGNORE
		SupportsUpsert:         true,  // ✅ INSERT ... ON CONFLICT

		// VIEW 支持
		SupportsView:               true,
		SupportsMaterializedView:   false,
		SupportsViewForPreload:     true,  // 可用于简单的 Preload

		// 多 Adapter 优化
		SearchOptimizationSupported:    false, // FTS 需要扩展
		SearchOptimizationIsOptimal:    false,
		SearchOptimizationPriority:     4,
		RecursiveOptimizationSupported:     true,
		RecursiveOptimizationIsOptimal:     false,
		RecursiveOptimizationPriority:      4,
		RecursiveOptimizationHasNativeSyntax: true, // WITH RECURSIVE

		AdapterTags: []string{"embedded", "lightweight", "relational"},

		OptimizationNotes: map[string]string{
			"limited_features": "SQLite 功能有限，详细操作供应用层处理",
		},

		FallbackStrategies: map[string]QueryFallbackStrategy{
			"full_outer_join":     QueryFallbackMultiQuery,
			"full_text_search":    QueryFallbackApplicationLayer,
			"regex_match":         QueryFallbackApplicationLayer,
		},
		FeatureNotes: map[string]string{
			"full_outer_join": "SQLite 不支持，可用 LEFT JOIN UNION RIGHT JOIN",
			"full_text_search": "需要启用 FTS 扩展或在应用层处理",
			"regex_match": "需要通过 Go 注册 REGEXP 函数",
			"recursive_cte": "SQLite 3.8.4+ 支持",
		},
		AlternativeSyntax: map[string]string{
			"insert_ignore": "INSERT OR IGNORE INTO ...",
			"upsert": "INSERT INTO ... ON CONFLICT ... DO UPDATE SET ...",
		},
		FeatureSupport: map[string]FeatureSupport{
			"recursive_cte": {Supported: true, MinVersion: "3.8.4"},
			"window_func": {Supported: true, MinVersion: "3.25.0"},
			"json_path": {Supported: true, MinVersion: "3.9.0"},
			"order_by_in_aggregate": {Supported: true, MinVersion: "3.30.0"},
		},
	}
}

// NewSQLServerQueryFeatures SQL Server 查询特性
func NewSQLServerQueryFeatures() *QueryFeatures {
	return &QueryFeatures{
		// 基础特性 - 全部支持
		SupportsIN:            true,
		SupportsNotIN:         true,
		SupportsBetween:       true,
		SupportsLike:          true,
		SupportsDistinct:      true,
		SupportsGroupBy:       true,
		SupportsHaving:        true,

		// JOIN 操作 - 全部支持
		SupportsInnerJoin:     true,
		SupportsLeftJoin:      true,
		SupportsRightJoin:     true,
		SupportsCrossJoin:     true,
		SupportsFullOuterJoin: true,
		SupportsSelfJoin:      true,

		// 高级特性
		SupportsCTE:           true,
		SupportsRecursiveCTE:  true,  // ✅ WITH ... AS RECURSIVE
		SupportsWindowFunc:    true,
		SupportsSubquery:      true,
		SupportsCorrelatedSubquery: true,
		SupportsUnion:         true,
		SupportsExcept:        true,  // ✅ EXCEPT (而非 MINUS)
		SupportsIntersect:     true,

		// 聚合
		SupportsOrderByInAggregate: true,
		SupportsArrayAggregate:     false, // ❌ 无原生数组，但可用 STRING_AGG
		SupportsStringAggregate:    true,  // ✅ STRING_AGG()

		// 文本搜索
		SupportsFullTextSearch:     true,  // ✅ Full-Text Search
		SupportsRegexMatch:         false, // ❌ 不原生支持
		SupportsFuzzyMatch:         false, // ❌ 不原生支持

		// JSON
		SupportsJSONPath:       true,  // ✅ JSON_VALUE, JSON_QUERY
		SupportsJSONType:       false, // ❌ 以文本形式存储
		SupportsJSONOperators:  true,  // ✅ JSON 函数
		SupportsJSONAgg:        true,  // ✅ JSON_QUERY FOR JSON

		// 其他
		SupportsCase:           true,
		SupportsCaseWithElse:   true,
		SupportsLimit:          false, // ❌ 用 OFFSET...FETCH 或 TOP
		SupportsOffset:         true,  // ✅ OFFSET ... ROWS FETCH NEXT
		SupportsOrderBy:        true,
		SupportsNulls:          true,
		SupportsCastType:       true,
		SupportsCoalesce:       true,
		SupportsIfExists:       true,
		SupportsInsertIgnore:   false, // ❌ 用 MERGE
		SupportsUpsert:         true,  // ✅ MERGE ... WHEN MATCHED THEN

		// VIEW 支持
		SupportsView:               true,
		SupportsMaterializedView:   false, // ❌ 但有类似的优化视图
		SupportsViewForPreload:     true,  // ✅ 可用于 Preload 优化

		// 多 Adapter 优化
		SearchOptimizationSupported:    true,
		SearchOptimizationIsOptimal:    false,
		SearchOptimizationPriority:     3,
		RecursiveOptimizationSupported:     true,
		RecursiveOptimizationIsOptimal:     true, // ✅ SQL Server 是递归优于 PostgreSQL 的选择
		RecursiveOptimizationPriority:      1,
		RecursiveOptimizationHasNativeSyntax: true, // ✅ WITH ... AS RECURSIVE

		AdapterTags: []string{"relational", "enterprise", "full_text_search"},

		OptimizationNotes: map[string]string{
			"recursive_optimal": "SQL Server 的递归CTE性能优于 PostgreSQL",
			"full_text_search": "有专门的全文搜索功能",
		},

		FallbackStrategies: map[string]QueryFallbackStrategy{
			"limit":              QueryFallbackAlternativeSyntax, // 用 OFFSET...FETCH NEXT
			"array_aggregate":    QueryFallbackAlternativeSyntax, // 用 STRING_AGG
			"regex_match":        QueryFallbackApplicationLayer,
		},
		FeatureNotes: map[string]string{
			"limit": "SQL Server 用 OFFSET ... ROWS FETCH NEXT ... ROWS ONLY",
			"recursive_cte": "WITH ... AS RECURSIVE",
			"full_text_search": "启用全文搜索后支持",
		},
		AlternativeSyntax: map[string]string{
			"limit": "SELECT ... FROM ... ORDER BY ... OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY",
			"upsert": "MERGE INTO target USING source ... WHEN MATCHED THEN UPDATE ...",
		},
		FeatureSupport: map[string]FeatureSupport{},
	}
}

// ==================== Query Features Manager ====================

// GetQueryFeatures 获取数据库的查询特性
func GetQueryFeatures(dbType string) *QueryFeatures {
	switch dbType {
	case "postgres", "postgresql":
		return NewPostgreSQLQueryFeatures()
	case "mysql":
		return NewMySQLQueryFeatures()
	case "sqlite":
		return NewSQLiteQueryFeatures()
	case "sqlserver":
		return NewSQLServerQueryFeatures()
	default:
		// 默认返回最小特性集
		return &QueryFeatures{}
	}
}

// ==================== Query Feature Helpers ====================

// HasQueryFeature 检查是否支持某个查询特性
func (qf *QueryFeatures) HasQueryFeature(feature string) bool {
	switch feature {
	case "in_range":
		return qf.SupportsIN
	case "not_in":
		return qf.SupportsNotIN
	case "between":
		return qf.SupportsBetween
	case "like":
		return qf.SupportsLike
	case "distinct":
		return qf.SupportsDistinct
	case "group_by":
		return qf.SupportsGroupBy
	case "having":
		return qf.SupportsHaving
	case "inner_join":
		return qf.SupportsInnerJoin
	case "left_join":
		return qf.SupportsLeftJoin
	case "right_join":
		return qf.SupportsRightJoin
	case "cross_join":
		return qf.SupportsCrossJoin
	case "full_outer_join":
		return qf.SupportsFullOuterJoin
	case "self_join":
		return qf.SupportsSelfJoin
	case "cte":
		return qf.SupportsCTE
	case "recursive_cte":
		return qf.SupportsRecursiveCTE
	case "window_func":
		return qf.SupportsWindowFunc
	case "subquery":
		return qf.SupportsSubquery
	case "correlated_subquery":
		return qf.SupportsCorrelatedSubquery
	case "union":
		return qf.SupportsUnion
	case "except":
		return qf.SupportsExcept
	case "intersect":
		return qf.SupportsIntersect
	case "order_by_in_aggregate":
		return qf.SupportsOrderByInAggregate
	case "array_aggregate":
		return qf.SupportsArrayAggregate
	case "string_aggregate":
		return qf.SupportsStringAggregate
	case "full_text_search":
		return qf.SupportsFullTextSearch
	case "regex_match":
		return qf.SupportsRegexMatch
	case "fuzzy_match":
		return qf.SupportsFuzzyMatch
	case "json_path":
		return qf.SupportsJSONPath
	case "json_type":
		return qf.SupportsJSONType
	case "json_operators":
		return qf.SupportsJSONOperators
	case "json_agg":
		return qf.SupportsJSONAgg
	case "case":
		return qf.SupportsCase
	case "limit":
		return qf.SupportsLimit
	case "offset":
		return qf.SupportsOffset
	case "order_by":
		return qf.SupportsOrderBy
	case "nulls":
		return qf.SupportsNulls
	case "cast":
		return qf.SupportsCastType
	case "coalesce":
		return qf.SupportsCoalesce
	case "if_exists":
		return qf.SupportsIfExists
	case "insert_ignore":
		return qf.SupportsInsertIgnore
	case "upsert":
		return qf.SupportsUpsert
	default:
		return false
	}
}

// GetFallbackStrategy 获取不支持特性时的降级策略
func (qf *QueryFeatures) GetFallbackStrategy(feature string) QueryFallbackStrategy {
	if strategy, ok := qf.FallbackStrategies[feature]; ok {
		return strategy
	}
	return QueryFallbackNone
}

// GetAlternativeSyntax 获取替代语法
func (qf *QueryFeatures) GetAlternativeSyntax(feature string) string {
	if syntax, ok := qf.AlternativeSyntax[feature]; ok {
		return syntax
	}
	return ""
}

// GetFeatureNote 获取特性说明
func (qf *QueryFeatures) GetFeatureNote(feature string) string {
	if note, ok := qf.FeatureNotes[feature]; ok {
		return note
	}
	return ""
}

// GetFeatureSupport 获取特性支持详情（若未定义则返回零值）
func (qf *QueryFeatures) GetFeatureSupport(feature string) FeatureSupport {
	if qf.FeatureSupport == nil {
		return FeatureSupport{Supported: qf.HasQueryFeature(feature)}
	}
	if support, ok := qf.FeatureSupport[feature]; ok {
		return support
	}
	return FeatureSupport{Supported: qf.HasQueryFeature(feature)}
}

// SupportsFeatureWithVersion 根据版本判断特性支持（version 为空时退回到常规判断）
func (qf *QueryFeatures) SupportsFeatureWithVersion(feature, version string) bool {
	support := qf.GetFeatureSupport(feature)
	if version == "" || support.MinVersion == "" {
		return support.Supported
	}
	if !support.Supported {
		return false
	}
	return compareVersion(version, support.MinVersion) >= 0
}

// CompareQueryFeatures 对比两个数据库的查询特性
func CompareQueryFeatures(qf1, qf2 *QueryFeatures) map[string]interface{} {
	result := map[string]interface{}{
		"common_features": []string{},
		"only_in_first":   []string{},
		"only_in_second":  []string{},
		"different_fallback": map[string][2]QueryFallbackStrategy{},
	}

	// 所有可能的特性名
	features := []string{
		"in_range", "not_in", "between", "like", "distinct", "group_by", "having",
		"inner_join", "left_join", "right_join", "cross_join", "full_outer_join", "self_join",
		"cte", "recursive_cte", "window_func", "subquery", "correlated_subquery",
		"union", "except", "intersect",
		"order_by_in_aggregate", "array_aggregate", "string_aggregate",
		"full_text_search", "regex_match", "fuzzy_match",
		"json_path", "json_type", "json_operators", "json_agg",
		"case", "limit", "offset", "order_by", "nulls", "cast", "coalesce",
		"if_exists", "insert_ignore", "upsert",
	}

	for _, feat := range features {
		has1 := qf1.HasQueryFeature(feat)
		has2 := qf2.HasQueryFeature(feat)

		if has1 && has2 {
			result["common_features"] = append(result["common_features"].([]string), feat)
		} else if has1 {
			result["only_in_first"] = append(result["only_in_first"].([]string), feat)
		} else if has2 {
			result["only_in_second"] = append(result["only_in_second"].([]string), feat)
		}
	}

	return result
}

// PrintQueryFeatureMatrix 打印查询特性矩阵
func PrintQueryFeatureMatrix(databases ...string) string {
	if len(databases) == 0 {
		databases = []string{"postgres", "mysql", "sqlite", "sqlserver"}
	}

	output := "查询特性对比矩阵\n"
	output += "================\n\n"

	features := []string{
		"in_range", "between", "like", "distinct", "group_by",
		"inner_join", "left_join", "full_outer_join",
		"cte", "recursive_cte", "window_func", "subquery",
		"union", "except", "intersect",
		"full_text_search", "regex_match",
		"json_path", "json_operators", "upsert", "limit",
	}

	// 表头
	output += "特性\\数据库 |"
	for _, db := range databases {
		output += fmt.Sprintf(" %-12s |", db)
	}
	output += "\n"
	output += "-" + strings.Repeat("-", len(databases)*16) + "\n"

	// 特性行
	for _, feat := range features {
		output += fmt.Sprintf("%-20s |", feat)
		for _, db := range databases {
			qf := GetQueryFeatures(db)
			if qf.HasQueryFeature(feat) {
				output += " ✅           |"
			} else {
				fallback := qf.GetFallbackStrategy(feat)
				if fallback == QueryFallbackNone {
					output += " ❌           |"
				} else {
					output += fmt.Sprintf(" ⚠️  (%s) |", fallback)
				}
			}
		}
		output += "\n"
	}

	return output
}

// compareVersion 简单版本比较：返回 1(>) / 0(=) / -1(<)
func compareVersion(a, b string) int {
	if a == b {
		return 0
	}
	parse := func(v string) []int {
		parts := strings.Split(v, ".")
		nums := make([]int, 0, len(parts))
		for _, p := range parts {
			var n int
			if _, err := fmt.Sscanf(p, "%d", &n); err != nil {
				n = 0
			}
			nums = append(nums, n)
		}
		return nums
	}

	av := parse(a)
	bv := parse(b)
	max := len(av)
	if len(bv) > max {
		max = len(bv)
	}
	for i := 0; i < max; i++ {
		var ai, bi int
		if i < len(av) {
			ai = av[i]
		}
		if i < len(bv) {
			bi = bv[i]
		}
		if ai > bi {
			return 1
		}
		if ai < bi {
			return -1
		}
	}
	return 0
}
