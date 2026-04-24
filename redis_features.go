package db

// NewRedisDatabaseFeatures Redis 数据库特性声明。
func NewRedisDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{
		// 索引和约束：Redis 是 key-value 存储，不支持关系型约束
		SupportsCompositeKeys:        false,
		SupportsForeignKeys:          false,
		SupportsCompositeForeignKeys: false,
		SupportsCompositeIndexes:     false,
		SupportsPartialIndexes:       false,
		SupportsDeferrable:           false,

		// 自定义类型
		SupportsEnumType:      false,
		SupportsCompositeType: false,
		SupportsDomainType:    false,
		SupportsUDT:           false,

		// 函数和过程
		SupportsStoredProcedures: false,
		SupportsFunctions:        true, // Lua scripting / Redis Functions (v7+)
		SupportsAggregateFuncs:   false,
		FunctionLanguages:        []string{"lua"},

		// 高级查询
		SupportsWindowFunctions: false,
		SupportsCTE:             false,
		SupportsRecursiveCTE:    false,
		SupportsMaterializedCTE: false,

		// JSON
		HasNativeJSON:     true,  // RedisJSON 模块支持
		SupportsJSONPath:  true,  // RedisJSON 支持 JSONPath
		SupportsJSONIndex: false, // 需要 RediSearch 配合

		// 全文搜索
		SupportsFullTextSearch: false, // 需要 RediSearch 模块

		// 其他
		SupportsArrays:       false,
		SupportsGenerated:    false,
		SupportsReturning:    false,
		SupportsUpsert:       true, // SET / SETNX / HSET 等天然支持 upsert 语义
		SupportsListenNotify: true, // Pub/Sub

		// 元信息
		DatabaseName:    "Redis",
		DatabaseVersion: "6+",
		Description:     "In-memory key-value store; supports strings, hashes, lists, sets, sorted sets, streams, and pub/sub",

		FeatureSupport: map[string]FeatureSupport{
			"functions":    {Supported: true, Notes: "Lua scripts and Redis Functions (v7+)"},
			"json":         {Supported: true, Notes: "requires RedisJSON module"},
			"full_text":    {Supported: false, Notes: "requires RediSearch module"},
			"transactions": {Supported: true, Notes: "MULTI/EXEC pipeline; not ACID; use with WATCH for optimistic locking"},
			"pub_sub":      {Supported: true, Notes: "PUBLISH/SUBSCRIBE/PSUBSCRIBE"},
			"streams":      {Supported: true, Notes: "XADD/XREAD/XRANGE (Redis 5+)"},
		},
		FallbackStrategies: map[string]FeatureFallback{
			"sql_query":      FallbackNone,             // Redis 不支持 SQL 查询
			"joins":          FallbackNone,             // 不支持 JOIN
			"scheduled_task": FallbackApplicationLayer, // 使用应用层调度器
			"aggregation":    FallbackApplicationLayer, // 聚合需在应用层处理
		},
	}
}

// NewRedisQueryFeatures Redis 查询特性声明。
func NewRedisQueryFeatures() *QueryFeatures {
	return &QueryFeatures{
		// Redis 不支持 SQL 式查询
		SupportsIN:       false,
		SupportsNotIN:    false,
		SupportsBetween:  false,
		SupportsLike:     false,
		SupportsDistinct: false,
		SupportsGroupBy:  false,
		SupportsHaving:   false,

		// JOIN 操作全不支持
		SupportsInnerJoin:     false,
		SupportsLeftJoin:      false,
		SupportsRightJoin:     false,
		SupportsCrossJoin:     false,
		SupportsFullOuterJoin: false,
		SupportsSelfJoin:      false,

		// 高级查询
		SupportsCTE:                false,
		SupportsRecursiveCTE:       false,
		SupportsWindowFunc:         false,
		SupportsSubquery:           false,
		SupportsCorrelatedSubquery: false,
		SupportsUnion:              false,
		SupportsExcept:             false,
		SupportsIntersect:          false,

		// 聚合
		SupportsOrderByInAggregate: false,
		SupportsArrayAggregate:     false,
		SupportsStringAggregate:    false,
	}
}
