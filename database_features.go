package db

// DatabaseFeatures 数据库特性声明
// 每个 Adapter 通过此结构声明其支持的数据库特性
type DatabaseFeatures struct {
	// ===== 索引和约束 =====
	SupportsCompositeKeys    bool // 复合主键 (所有数据库都支持)
	SupportsCompositeIndexes bool // 复合索引 (所有数据库都支持)
	SupportsPartialIndexes   bool // 部分索引 (WHERE 子句)
	SupportsDeferrable       bool // 延迟约束 (PostgreSQL, SQLite)

	// ===== 自定义类型 =====
	SupportsEnumType      bool // ENUM 类型
	SupportsCompositeType bool // 复合/结构体类型 (PostgreSQL)
	SupportsDomainType    bool // DOMAIN 类型 (PostgreSQL)
	SupportsUDT           bool // User-Defined Types (SQL Server, PostgreSQL)

	// ===== 函数和过程 =====
	SupportsStoredProcedures bool     // 存储过程
	SupportsFunctions        bool     // 自定义函数
	SupportsAggregateFuncs   bool     // 自定义聚合函数
	FunctionLanguages        []string // 支持的函数语言 (如 "plpgsql", "tsql", "sql")

	// ===== 高级查询 =====
	SupportsWindowFunctions bool // 窗口函数 (ROW_NUMBER, RANK, etc.)
	SupportsCTE             bool // 公共表表达式 (WITH)
	SupportsRecursiveCTE    bool // 递归 CTE
	SupportsMaterializedCTE bool // 物化 CTE (PostgreSQL)

	// ===== JSON 支持 =====
	HasNativeJSON     bool // 原生 JSON 类型
	SupportsJSONPath  bool // JSON 路径查询
	SupportsJSONIndex bool // JSON 索引

	// ===== 全文搜索 =====
	SupportsFullTextSearch bool     // 全文搜索
	FullTextLanguages      []string // 支持的语言

	// ===== 其他特性 =====
	SupportsArrays       bool // 数组类型 (PostgreSQL)
	SupportsGenerated    bool // 生成列 (Computed/Generated columns)
	SupportsReturning    bool // RETURNING 子句 (PostgreSQL, SQLite 3.35+)
	SupportsUpsert       bool // UPSERT 操作 (ON CONFLICT / ON DUPLICATE KEY)
	SupportsListenNotify bool // LISTEN/NOTIFY (PostgreSQL)

	// ===== 元信息 =====
	DatabaseName    string // 数据库名称
	DatabaseVersion string // 版本信息
	Description     string // 特性描述
}

// FeatureCategory 特性分类
type FeatureCategory string

const (
	CategoryIndexing    FeatureCategory = "indexing"     // 索引和约束
	CategoryTypes       FeatureCategory = "types"        // 自定义类型
	CategoryFunctions   FeatureCategory = "functions"    // 函数和存储过程
	CategoryAdvanced    FeatureCategory = "advanced"     // 高级查询
	CategoryJSON        FeatureCategory = "json"         // JSON 支持
	CategoryFullText    FeatureCategory = "full_text"    // 全文搜索
	CategoryOther       FeatureCategory = "other"        // 其他特性
)

// FeatureFallback 特性降级策略
type FeatureFallback string

const (
	FallbackNone            FeatureFallback = "none"              // 不支持，返回错误
	FallbackCheckConstraint FeatureFallback = "check_constraint"  // 使用 CHECK 约束
	FallbackDynamicTable    FeatureFallback = "dynamic_table"     // 使用动态类型表
	FallbackApplicationLayer FeatureFallback = "application_layer" // 应用层处理
)

// HasFeature 检查是否支持特定特性
func (f *DatabaseFeatures) HasFeature(feature string) bool {
	switch feature {
	case "composite_keys":
		return f.SupportsCompositeKeys
	case "composite_indexes":
		return f.SupportsCompositeIndexes
	case "partial_indexes":
		return f.SupportsPartialIndexes
	case "deferrable":
		return f.SupportsDeferrable
	case "enum_type":
		return f.SupportsEnumType
	case "composite_type":
		return f.SupportsCompositeType
	case "domain_type":
		return f.SupportsDomainType
	case "udt":
		return f.SupportsUDT
	case "stored_procedures":
		return f.SupportsStoredProcedures
	case "functions":
		return f.SupportsFunctions
	case "aggregate_funcs":
		return f.SupportsAggregateFuncs
	case "window_functions":
		return f.SupportsWindowFunctions
	case "cte":
		return f.SupportsCTE
	case "recursive_cte":
		return f.SupportsRecursiveCTE
	case "materialized_cte":
		return f.SupportsMaterializedCTE
	case "native_json":
		return f.HasNativeJSON
	case "json_path":
		return f.SupportsJSONPath
	case "json_index":
		return f.SupportsJSONIndex
	case "full_text_search":
		return f.SupportsFullTextSearch
	case "arrays":
		return f.SupportsArrays
	case "generated":
		return f.SupportsGenerated
	case "returning":
		return f.SupportsReturning
	case "upsert":
		return f.SupportsUpsert
	case "listen_notify":
		return f.SupportsListenNotify
	default:
		return false
	}
}

// GetFeaturesByCategory 按分类获取支持的特性列表
func (f *DatabaseFeatures) GetFeaturesByCategory(category FeatureCategory) []string {
	features := []string{}

	switch category {
	case CategoryIndexing:
		if f.SupportsCompositeKeys {
			features = append(features, "composite_keys")
		}
		if f.SupportsCompositeIndexes {
			features = append(features, "composite_indexes")
		}
		if f.SupportsPartialIndexes {
			features = append(features, "partial_indexes")
		}
		if f.SupportsDeferrable {
			features = append(features, "deferrable")
		}

	case CategoryTypes:
		if f.SupportsEnumType {
			features = append(features, "enum_type")
		}
		if f.SupportsCompositeType {
			features = append(features, "composite_type")
		}
		if f.SupportsDomainType {
			features = append(features, "domain_type")
		}
		if f.SupportsUDT {
			features = append(features, "udt")
		}

	case CategoryFunctions:
		if f.SupportsStoredProcedures {
			features = append(features, "stored_procedures")
		}
		if f.SupportsFunctions {
			features = append(features, "functions")
		}
		if f.SupportsAggregateFuncs {
			features = append(features, "aggregate_funcs")
		}

	case CategoryAdvanced:
		if f.SupportsWindowFunctions {
			features = append(features, "window_functions")
		}
		if f.SupportsCTE {
			features = append(features, "cte")
		}
		if f.SupportsRecursiveCTE {
			features = append(features, "recursive_cte")
		}
		if f.SupportsMaterializedCTE {
			features = append(features, "materialized_cte")
		}

	case CategoryJSON:
		if f.HasNativeJSON {
			features = append(features, "native_json")
		}
		if f.SupportsJSONPath {
			features = append(features, "json_path")
		}
		if f.SupportsJSONIndex {
			features = append(features, "json_index")
		}

	case CategoryFullText:
		if f.SupportsFullTextSearch {
			features = append(features, "full_text_search")
		}

	case CategoryOther:
		if f.SupportsArrays {
			features = append(features, "arrays")
		}
		if f.SupportsGenerated {
			features = append(features, "generated")
		}
		if f.SupportsReturning {
			features = append(features, "returning")
		}
		if f.SupportsUpsert {
			features = append(features, "upsert")
		}
		if f.SupportsListenNotify {
			features = append(features, "listen_notify")
		}
	}

	return features
}

// CompareFeatures 比较两个数据库的特性差异
func CompareFeatures(f1, f2 *DatabaseFeatures) *FeatureComparison {
	return &FeatureComparison{
		Database1:      f1.DatabaseName,
		Database2:      f2.DatabaseName,
		CommonFeatures: findCommonFeatures(f1, f2),
		OnlyInFirst:    findUniqueFeatures(f1, f2),
		OnlyInSecond:   findUniqueFeatures(f2, f1),
	}
}

// FeatureComparison 特性比较结果
type FeatureComparison struct {
	Database1      string
	Database2      string
	CommonFeatures []string // 共同支持的特性
	OnlyInFirst    []string // 仅第一个数据库支持
	OnlyInSecond   []string // 仅第二个数据库支持
}

// 辅助函数：查找共同特性
func findCommonFeatures(f1, f2 *DatabaseFeatures) []string {
	common := []string{}
	allFeatures := []string{
		"composite_keys", "composite_indexes", "partial_indexes", "deferrable",
		"enum_type", "composite_type", "domain_type", "udt",
		"stored_procedures", "functions", "aggregate_funcs",
		"window_functions", "cte", "recursive_cte", "materialized_cte",
		"native_json", "json_path", "json_index",
		"full_text_search", "arrays", "generated", "returning", "upsert", "listen_notify",
	}

	for _, feature := range allFeatures {
		if f1.HasFeature(feature) && f2.HasFeature(feature) {
			common = append(common, feature)
		}
	}

	return common
}

// 辅助函数：查找唯一特性
func findUniqueFeatures(f1, f2 *DatabaseFeatures) []string {
	unique := []string{}
	allFeatures := []string{
		"composite_keys", "composite_indexes", "partial_indexes", "deferrable",
		"enum_type", "composite_type", "domain_type", "udt",
		"stored_procedures", "functions", "aggregate_funcs",
		"window_functions", "cte", "recursive_cte", "materialized_cte",
		"native_json", "json_path", "json_index",
		"full_text_search", "arrays", "generated", "returning", "upsert", "listen_notify",
	}

	for _, feature := range allFeatures {
		if f1.HasFeature(feature) && !f2.HasFeature(feature) {
			unique = append(unique, feature)
		}
	}

	return unique
}
