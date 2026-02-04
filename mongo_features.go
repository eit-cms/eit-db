package db

// NewMongoDatabaseFeatures MongoDB 数据库特性（最小占位实现）
func NewMongoDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{
		// 索引和约束
		SupportsCompositeKeys:    false,
		SupportsCompositeIndexes: true,
		SupportsPartialIndexes:   true,
		SupportsDeferrable:       false,

		// 自定义类型
		SupportsEnumType:      false,
		SupportsCompositeType: false,
		SupportsDomainType:    false,
		SupportsUDT:           false,

		// 函数和过程
		SupportsStoredProcedures: false,
		SupportsFunctions:        true,
		SupportsAggregateFuncs:   true,
		FunctionLanguages:        []string{"javascript"},

		// 高级查询
		SupportsWindowFunctions: false,
		SupportsCTE:             false,
		SupportsRecursiveCTE:    false,
		SupportsMaterializedCTE: false,

		// JSON
		HasNativeJSON:     true,
		SupportsJSONPath:  true,
		SupportsJSONIndex: true,

		// 全文搜索
		SupportsFullTextSearch: true,
		FullTextLanguages:      []string{"en", "zh"},

		// 其他
		SupportsArrays:       true,
		SupportsGenerated:    false,
		SupportsReturning:    false,
		SupportsUpsert:       true,
		SupportsListenNotify: false,

		// 元信息
		DatabaseName:    "mongodb",
		DatabaseVersion: "",
		Description:     "MongoDB document database (non-SQL)",
	}
}

// NewMongoQueryFeatures MongoDB 查询特性（最小占位实现）
func NewMongoQueryFeatures() *QueryFeatures {
	return &QueryFeatures{
		// MongoDB 不走 SQL，以下为近似映射/最小实现
		SupportsIN:            true,
		SupportsNotIN:         true,
		SupportsBetween:       true,
		SupportsLike:          false,
		SupportsDistinct:      true,
		SupportsGroupBy:       true,
		SupportsHaving:        false,

		SupportsInnerJoin:     false,
		SupportsLeftJoin:      false,
		SupportsRightJoin:     false,
		SupportsCrossJoin:     false,
		SupportsFullOuterJoin: false,
		SupportsSelfJoin:      false,

		SupportsCTE:           false,
		SupportsRecursiveCTE:  false,
		SupportsWindowFunc:    false,
		SupportsSubquery:      true,
		SupportsCorrelatedSubquery: false,
		SupportsUnion:         false,
		SupportsExcept:        false,
		SupportsIntersect:     false,

		SupportsOrderByInAggregate: false,
		SupportsArrayAggregate:     true,
		SupportsStringAggregate:    true,

		SupportsFullTextSearch: true,
		SupportsRegexMatch:     true,
		SupportsFuzzyMatch:     true,

		SupportsJSONPath:       true,
		SupportsJSONType:       true,
		SupportsJSONOperators:  false,
		SupportsJSONAgg:        true,

		SupportsCase:           false,
		SupportsCaseWithElse:   false,

		SupportsLimit:          true,
		SupportsOffset:         true,
		SupportsOrderBy:        true,
		SupportsNulls:          true,
		SupportsCastType:       false,
		SupportsCoalesce:       false,

		SupportsIfExists:       true,
		SupportsInsertIgnore:   false,
		SupportsUpsert:         true,

		SupportsView:               false,
		SupportsMaterializedView:   false,
		SupportsViewForPreload:     false,

		SearchOptimizationSupported:    true,
		SearchOptimizationIsOptimal:    true,
		SearchOptimizationPriority:     1,
		RecursiveOptimizationSupported: false,
		RecursiveOptimizationIsOptimal: false,
		RecursiveOptimizationPriority:  0,
		RecursiveOptimizationHasNativeSyntax: false,

		AdapterTags: []string{"document", "text_search"},

		OptimizationNotes: map[string]string{
			"search_optimal": "MongoDB 原生全文搜索/索引适配搜索场景",
		},

		FallbackStrategies: map[string]QueryFallbackStrategy{},
		FeatureNotes:       map[string]string{},
		AlternativeSyntax:  map[string]string{},
	}
}
