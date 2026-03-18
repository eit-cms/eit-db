package db

// NewMongoDatabaseFeatures MongoDB 数据库特性（最小占位实现）
func NewMongoDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{
		// 索引和约束
		SupportsCompositeKeys:        false,
		SupportsForeignKeys:          false,
		SupportsCompositeForeignKeys: false,
		SupportsCompositeIndexes:     true,
		SupportsPartialIndexes:       true,
		SupportsDeferrable:           false,

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
		Description:     "MongoDB document database (non-SQL, no FK constraints; join via aggregation/$lookup)",

		FeatureSupport: map[string]FeatureSupport{
			"foreign_keys":           {Supported: false, Notes: "MongoDB has no FK constraints; use aggregation/$lookup"},
			"composite_foreign_keys": {Supported: false, Notes: "MongoDB has no composite FK constraints; use pipeline join"},
			"ttl_index":              {Supported: true, Notes: "native TTL index via expireAfterSeconds; use MongoTTLFeatures API"},
			"local_cache_join":       {Supported: true, Notes: "application-layer preload + JoinWith via MongoLocalCache; replaces $lookup for small collections"},
			"virtual_view":           {Supported: true, Notes: "in-process aggregation result cache via MongoVirtualView; replaces materialized views"},
			"scheduled_task":         {Supported: false, Notes: "MongoDB has no native scheduled task DDL; use APOC or application-layer scheduler"},
			"transactions":           {Supported: true, Notes: "MongoDB 4.0+ multi-document transactions via session; not wrapped in SQL Tx interface"},
		},
		FallbackStrategies: map[string]FeatureFallback{
			"foreign_keys":           FallbackApplicationLayer,
			"composite_foreign_keys": FallbackApplicationLayer,
			"scheduled_task":         FallbackApplicationLayer,
		},
	}
}

// NewMongoQueryFeatures MongoDB 查询特性（最小占位实现）
func NewMongoQueryFeatures() *QueryFeatures {
	return &QueryFeatures{
		// MongoDB 不走 SQL，以下为近似映射/最小实现
		SupportsIN:       true,
		SupportsNotIN:    true,
		SupportsBetween:  true,
		SupportsLike:     false,
		SupportsDistinct: true,
		SupportsGroupBy:  true,
		SupportsHaving:   false,

		SupportsInnerJoin:     false,
		SupportsLeftJoin:      false,
		SupportsRightJoin:     false,
		SupportsCrossJoin:     false,
		SupportsFullOuterJoin: false,
		SupportsSelfJoin:      false,

		SupportsCTE:                false,
		SupportsRecursiveCTE:       false,
		SupportsWindowFunc:         false,
		SupportsSubquery:           true,
		SupportsCorrelatedSubquery: false,
		SupportsUnion:              false,
		SupportsExcept:             false,
		SupportsIntersect:          false,

		SupportsOrderByInAggregate: false,
		SupportsArrayAggregate:     true,
		SupportsStringAggregate:    true,

		SupportsFullTextSearch: true,
		SupportsRegexMatch:     true,
		SupportsFuzzyMatch:     true,

		SupportsJSONPath:      true,
		SupportsJSONType:      true,
		SupportsJSONOperators: false,
		SupportsJSONAgg:       true,

		SupportsCase:         false,
		SupportsCaseWithElse: false,

		SupportsLimit:    true,
		SupportsOffset:   true,
		SupportsOrderBy:  true,
		SupportsNulls:    true,
		SupportsCastType: false,
		SupportsCoalesce: false,

		SupportsIfExists:     true,
		SupportsInsertIgnore: false,
		SupportsUpsert:       true,

		SupportsView:             false,
		SupportsMaterializedView: false,
		SupportsViewForPreload:   false,

		SearchOptimizationSupported:          true,
		SearchOptimizationIsOptimal:          true,
		SearchOptimizationPriority:           1,
		RecursiveOptimizationSupported:       false,
		RecursiveOptimizationIsOptimal:       false,
		RecursiveOptimizationPriority:        0,
		RecursiveOptimizationHasNativeSyntax: false,

		AdapterTags: []string{"document", "text_search"},

		OptimizationNotes: map[string]string{
			"search_optimal": "MongoDB 原生全文搜索/索引适配搜索场景",
		},

		FallbackStrategies: map[string]QueryFallbackStrategy{
			// MongoDB 不支持 SQL JOIN，通过应用层或 $lookup aggregation 替代
			"inner_join":      QueryFallbackApplicationLayer,
			"left_join":       QueryFallbackApplicationLayer,
			"right_join":      QueryFallbackNone,
			"cross_join":      QueryFallbackNone,
			"full_outer_join": QueryFallbackNone,
			// MongoDB 不支持 SQL LIKE，通过 $regex 替代
			"like": QueryFallbackCustomFunction, // use $regex
			// CASE/COALESCE/CAST 通过 $cond / $ifNull / $convert 替代
			"case":      QueryFallbackCustomFunction,
			"cast_type": QueryFallbackCustomFunction,
			"coalesce":  QueryFallbackCustomFunction,
			// VIEW / 物化视图通过 MongoVirtualView 替代
			"materialized_view": QueryFallbackApplicationLayer,
		},
		FeatureNotes: map[string]string{
			"inner_join":        "use $lookup aggregation pipeline or MongoLocalCache.JoinWith for preloaded collections",
			"left_join":         "use $lookup with optional match semantics or MongoLocalCache.JoinWith",
			"like":              "use $regex operator with $options: 'i' for case-insensitive",
			"materialized_view": "use MongoVirtualView to define and cache aggregation pipeline results in-process",
			"ttl":               "use MongoTTLFeatures.EnsureTTLIndex + InsertWithExpiry for document auto-expiration",
		},
		AlternativeSyntax: map[string]string{},
	}
}
