package db

// NewNeo4jDatabaseFeatures Neo4j 数据库特性声明。
func NewNeo4jDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{
		// 索引和约束
		SupportsCompositeKeys: false,
		// Neo4j 不提供 SQL FK 约束，但通过关系边表达同等/更强的关联语义。
		SupportsForeignKeys:          true,
		SupportsCompositeForeignKeys: true,
		SupportsCompositeIndexes:     true,
		SupportsPartialIndexes:       false,
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
		FunctionLanguages:        []string{"cypher"},

		// 高级查询
		SupportsWindowFunctions: false,
		SupportsCTE:             false,
		SupportsRecursiveCTE:    true,
		SupportsMaterializedCTE: false,

		// JSON
		HasNativeJSON:     true,
		SupportsJSONPath:  true,
		SupportsJSONIndex: false,

		// 全文搜索
		SupportsFullTextSearch: true,
		FullTextLanguages:      []string{"en", "zh"},

		// 其他
		SupportsArrays:       true,
		SupportsGenerated:    false,
		SupportsReturning:    true,
		SupportsUpsert:       true,
		SupportsListenNotify: false,

		// 元信息
		DatabaseName:    "Neo4j",
		DatabaseVersion: "5+",
		Description:     "Graph database using Cypher; relationships act as native association model",

		FeatureSupport: map[string]FeatureSupport{
			"foreign_keys":           {Supported: true, Notes: "native relationship edges replace SQL foreign keys"},
			"composite_foreign_keys": {Supported: true, Notes: "multi-property relationship patterns replace composite FK constraints"},
			"enum_type":              {Supported: false, Notes: "no native ENUM DDL; model via taxonomy nodes/relationships or JSON properties"},
			"composite_type":         {Supported: false, Notes: "no native composite type DDL; model via subgraph or map/JSON properties"},
			"domain_type":            {Supported: false, Notes: "no native DOMAIN DDL; enforce via constraints/procedures and graph modeling"},
			"udt":                    {Supported: false, Notes: "no SQL-style UDT; model via graph structures or JSON"},
			"full_text_search":       {Supported: true, Notes: "requires fulltext index"},
		},
		FallbackStrategies: map[string]FeatureFallback{
			// Neo4j 通过关系边原生实现关联查询；SQL JOIN 语义映射为 MATCH 模式。
			"inner_join": FallbackApplicationLayer, // MATCH pattern replace SQL INNER JOIN
			"left_join":  FallbackApplicationLayer, // OPTIONAL MATCH replace SQL LEFT JOIN
			"right_join": FallbackNone,             // RIGHT JOIN 不被 Cypher 支持，返回错误
			"cross_join": FallbackApplicationLayer, // separate MATCH replaces CROSS JOIN
			// 事务 / 定时任务通过原生 LockableDriver / APOC 降级
			"sql_transaction": FallbackApplicationLayer, // 使用 session.RunImplicitTransaction 替代
			"scheduled_task":  FallbackApplicationLayer,
		},
	}
}
