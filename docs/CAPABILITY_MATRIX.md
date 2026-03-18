# EIT-DB 能力支持矩阵（v1.0 基线）

本文给出 v1.0 对外能力边界，包含数据库能力、查询能力、版本门槛、插件依赖与降级路径。

## 1. 数据库能力矩阵（DatabaseFeatures）

| 能力 | PostgreSQL | MySQL | SQLite | SQL Server | MongoDB | Neo4j |
|---|---|---|---|---|---|---|
| 复合主键 | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| 外键 | ✅ | ✅ | ✅ | ✅ | ❌ | ✅(关系语义替代 SQL FK) |
| 复合外键 | ✅ | ✅ | ✅ | ✅ | ❌ | ✅(多属性关系模式替代) |
| 复合索引 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 部分索引 | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ |
| 延迟约束 | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ |
| 原生 JSON 类型 | ✅(json/jsonb) | ✅ | ❌ | ❌(内建函数 + NVARCHAR 承载) | ✅ | ✅(属性/map) |
| JSON 路径 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| JSON 索引 | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ |
| 全文搜索 | ✅ | ✅ | ✅(依赖 FTS 扩展) | ✅ | ✅ | ✅ |
| RETURNING | ✅ | ❌ | ✅(3.35+) | ✅(OUTPUT) | ❌ | ✅ |
| UPSERT | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

来源：
- PostgreSQL: GetDatabaseFeatures in postgres_adapter.go
- MySQL: GetDatabaseFeatures in mysql_adapter.go
- SQLite: GetDatabaseFeatures in sqlite_adapter.go
- SQL Server: GetDatabaseFeatures in sqlserver_adapter.go
- MongoDB: NewMongoDatabaseFeatures in mongo_features.go
- Neo4j: NewNeo4jDatabaseFeatures in neo4j_features.go

## 2. 版本门槛（FeatureSupport）

### PostgreSQL

| 功能 | 最低版本 | 备注 |
|---|---|---|
| json | 9.2 | 内建类型 |
| jsonb | 9.4 | 内建类型，推荐作为默认映射 |

配置说明：
- `TypeJSON` 默认映射为 `jsonb`。
- 可通过 `Config.Options.postgres_json_type=json`（或 `json_type=json`）切换为 `json`。

### MySQL

| 功能 | 最低版本 | 降级策略 |
|---|---|---|
| window_functions | 8.0 | application_layer |
| cte / recursive_cte | 8.0 | application_layer |
| native_json / json_path | 5.7 | application_layer |
| json_index | 8.0.13 | application_layer |
| generated | 5.7 | application_layer |
| full_text_search | 5.6 | 无固定降级（查询层可降级 LIKE） |

### SQLite

| 功能 | 最低版本 | 降级策略 |
|---|---|---|
| window_functions | 3.25.0 | application_layer |
| cte / recursive_cte | 3.8.4 | application_layer |
| returning | 3.35.0 | application_layer |
| generated | 3.31.0 | application_layer |
| json_path | 3.9.0 | custom_function / application_layer |

SQLite JSON 降级说明：
- 若环境未启用 JSON1 扩展，可注册自定义函数驱动作为降级：
	- `RegisterSQLiteJSONFallbackDriver(...)`
	- `JSON_EXTRACT_GO(payload, '$.path')`
- 自定义函数方案适用于兼容场景；性能与完整语义不等同于原生 JSON1。

说明：版本门槛来自 DatabaseFeatures.FeatureSupport；查询层的特性门槛来自 QueryFeatures.FeatureSupport。

## 3. 查询能力矩阵（QueryFeatures）

| 查询能力 | PostgreSQL | MySQL | SQLite | SQL Server | MongoDB |
|---|---|---|---|---|---|
| FULL OUTER JOIN | ✅ | ❌(multi_query) | ❌(multi_query) | ✅ | ❌ |
| CTE | ✅ | ✅(8.0+) | ✅(3.8.4+) | ✅ | ❌ |
| 递归 CTE | ✅ | ✅(8.0+) | ✅(3.8.4+) | ✅ | ❌ |
| 窗口函数 | ✅ | ✅(8.0+) | ✅(3.25+) | ✅ | ❌ |
| 全文搜索 | ✅ | ✅ | ❌(alternative_syntax) | ✅ | ✅ |
| 正则匹配 | ✅ | ✅ | ❌(application_layer) | ❌(application_layer) | ✅ |
| LIMIT | ✅ | ✅ | ✅ | ❌(alternative_syntax: OFFSET/FETCH) | ✅ |

说明：括号中的内容表示 QueryFallbackStrategy。

## 4. 全文搜索与分词能力（运行时探测）

| 数据库 | 运行时探测 | 分词能力模式 | 插件/服务依赖 | 默认降级路径 |
|---|---|---|---|---|
| PostgreSQL | InspectFullTextRuntime | plugin | zhparser / pg_jieba / pgroonga（可选） | LIKE / tokenized LIKE |
| MySQL | 无运行时插件探测 | native | InnoDB FULLTEXT 索引 | LIKE（通过 BuildFuzzyCondition） |
| SQLite | InspectFullTextRuntime | builtin_fts | 编译选项 ENABLE_FTS3/4/5 | LIKE / tokenized LIKE |
| SQL Server | InspectFullTextRuntime | native | Full-Text Service 安装 | LIKE（通过 BuildFuzzyCondition） |
| MongoDB | InspectFullTextRuntime | application | 无固定插件要求 | tokenized pipeline（custom feature） |

SQL Server JSON 说明：
- JSON 函数能力为 SQL Server 内建（2016+），不是独立 JSON 插件生态。
- 原生 `json` 数据类型在 SQL Server 2025+ 预览/后续版本可用。

相关代码：
- fuzzy_query.go
- postgres_adapter.go
- sqlite_adapter.go
- sqlserver_adapter.go
- mongo_adapter.go

## 5. 自定义能力（CustomFeatureProvider）

目前 MongoDB 已实现自定义能力：

- document_join / custom_joiner（$lookup 聚合）
- full_text_search / tokenized_full_text_search（应用层分词增强计划）

Neo4j 已实现的关系导向能力：

- relationship_association_query（基于关系模式的关联查询，无需中间表）
- relationship_with_payload（关系携带业务属性，如请求消息、时间戳、状态）
- bidirectional_relationship_semantics（单向请求到双向关系转换，如好友请求接受）

派发顺序：native -> custom -> fallback -> unsupported。

## 6. 测试对齐（可核验）

### 数据库能力与版本

- database_features_test.go
- adapter-backend-tests/mysql_backend_test.go
- adapter-backend-tests/postgres_backend_test.go
- adapter-backend-tests/sqlserver_backend_test.go

### 查询能力与降级

- query_features_test.go
- query_features_comprehensive_test.go
- query_features_adapter_routing_test.go

### 复合约束与迁移

- migration_v2_composite_constraints_test.go
- migration_v2_foreign_key_test.go

### 全文搜索与模糊降级

- fuzzy_query_test.go
- feature_execution_test.go

## 7. v1.0 支持声明

- 保证支持：表中标记为 ✅ 且具备测试覆盖的能力。
- 最佳努力支持：依赖数据库版本、插件或运行时环境的能力（见版本门槛与运行时探测章节）。
- 不保证支持：未声明能力，或明确标记为 ❌ 且无 fallback 的路径。
