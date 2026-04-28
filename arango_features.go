package db

// NewArangoDatabaseFeatures ArangoDB 数据库特性声明（MVP）。
func NewArangoDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{
		SupportsCompositeKeys:        false,
		SupportsForeignKeys:          false,
		SupportsCompositeForeignKeys: false,
		SupportsCompositeIndexes:     true,
		SupportsPartialIndexes:       true,
		SupportsDeferrable:           false,

		SupportsEnumType:      false,
		SupportsCompositeType: false,
		SupportsDomainType:    false,
		SupportsUDT:           false,

		SupportsStoredProcedures: false,
		SupportsFunctions:        true,
		SupportsAggregateFuncs:   true,
		FunctionLanguages:        []string{"aql"},

		SupportsWindowFunctions: false,
		SupportsCTE:             false,
		SupportsRecursiveCTE:    false,
		SupportsMaterializedCTE: false,

		HasNativeJSON:     true,
		SupportsJSONPath:  true,
		SupportsJSONIndex: true,

		SupportsFullTextSearch: true,

		SupportsArrays:       true,
		SupportsGenerated:    false,
		SupportsReturning:    true,
		SupportsUpsert:       true,
		SupportsListenNotify: false,

		DatabaseName:    "ArangoDB",
		DatabaseVersion: "3.x",
		Description:     "Multi-model database with document and graph capabilities (AQL)",
		FeatureSupport: map[string]FeatureSupport{
			"document": {Supported: true, Notes: "native document collections"},
			"graph":    {Supported: true, Notes: "native graph edges and traversals"},
			"aql":      {Supported: true, Notes: "native AQL query language"},
		},
		FallbackStrategies: map[string]FeatureFallback{
			"scheduled_task": FallbackApplicationLayer,
		},
	}
}

// NewArangoQueryFeatures Arango 查询特性声明（MVP）。
func NewArangoQueryFeatures() *QueryFeatures {
	return &QueryFeatures{
		SupportsIN:       true,
		SupportsNotIN:    true,
		SupportsBetween:  true,
		SupportsLike:     true,
		SupportsDistinct: true,
		SupportsGroupBy:  true,
		SupportsHaving:   true,

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
		SupportsCorrelatedSubquery: true,
		SupportsUnion:              true,
		SupportsExcept:             true,
		SupportsIntersect:          true,

		SupportsOrderByInAggregate: true,
		SupportsArrayAggregate:     true,
		SupportsStringAggregate:    true,
	}
}
