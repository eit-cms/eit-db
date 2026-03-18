# EIT-DB Blueprint v2 草案（Draft）

状态：Draft
版本：0.1
日期：2026-03-18
范围：v2.0 设计阶段

## 1. 目标与定位

Blueprint 是 v2.0 的跨库协作层，负责统一描述以下内容：

- 数据定义：实体、字段、后端绑定（哪个数据在什么 Adapter/数据库中）
- 查询定义：可编译查询模板、跨库查询约束、执行策略
- 关系定义：跨库实体关系图（图结构）
- 治理定义：能力约束、一致性策略、访问策略、版本策略

Blueprint 不仅用于管理，还作为 Schema 和 Query Builder 的协作协议。

## 2. 核心设计原则

1. 正确性优先：所有跨库查询必须先基于 Blueprint 校验可行性。
2. 治理单一真相：跨库关系与跨库执行配置以 ArangoDB 控制面为唯一事实源。
3. 执行加速分层：运行时优先加载本地编译产物，控制面负责校验与版本仲裁。
4. 显式版本化：所有跨库计划必须绑定 BlueprintVersion。
5. 可回退：当版本不匹配或能力漂移时，可回退到安全执行路径。
6. 监控先行：跨库执行前必须具备有效的 Adapter 监控树快照。

## 3. 双层 Blueprint 模型

### 3.1 Control Blueprint（ArangoDB）

存储在 ArangoDB，面向治理与校验。

包含：

- 跨库实体关系图（节点/边）
- 数据后端绑定规则
- 能力约束与一致性策略
- 跨库查询执行配置
- Blueprint 元数据（版本、状态、发布时间、兼容声明）

不包含：

- 大体量编译产物
- 高频执行中间缓存

### 3.2 Runtime Blueprint Artifact（本地二进制）

由 eit-db-cli 在构建期生成，面向执行加速。

包含：

- 已编译 Schema 结构索引
- 已编译 Query 模板
- Planner hint 和路由快照
- 依赖的能力指纹摘要

建议格式：

- 生产：二进制（紧凑高效）
- 调试：文本镜像（JSON 或 YAML）

## 4. 最小数据结构（MVP）

### 4.1 Blueprint 顶层

```json
{
  "blueprint_id": "bp_ordering",
  "version": "2.0.0-alpha.1",
  "status": "active",
  "published_at": "2026-03-18T12:00:00Z",
  "domains": [],
  "entities": [],
  "relations": [],
  "queries": [],
  "policies": {},
  "capability_fingerprint": "sha256:..."
}
```

### 4.2 EntityBinding

```json
{
  "entity": "Order",
  "adapter": "postgres",
  "backend": "orders_db",
  "physical": {
    "kind": "table",
    "name": "orders"
  },
  "keys": {
    "primary": ["id"],
    "join_keys": ["user_id"]
  }
}
```

### 4.3 RelationEdge

```json
{
  "name": "Order.belongsTo.User",
  "from": "Order.user_id",
  "to": "User.id",
  "cardinality": "N:1",
  "cross_adapter": true,
  "consistency": "eventual",
  "nullable": false
}
```

### 4.4 QuerySpec

```json
{
  "name": "query_user_orders",
  "kind": "cross_db",
  "inputs": ["user_id", "page", "size"],
  "required_relations": ["Order.belongsTo.User"],
  "required_capabilities": [
    {"feature": "filter", "min_level": "L2"},
    {"feature": "sort", "min_level": "L2"},
    {"feature": "pagination", "min_level": "L3"}
  ],
  "plan_policy": {
    "must_validate_in_arango": true,
    "cache_plan": true,
    "cache_ttl_sec": 300
  }
}
```

## 4.5 特性支持强度标准

为了精确描述 Adapter 的能力，需要定义不同的支持强度等级，而不仅仅是"支持"或"不支持"。

### 4.5.1 支持强度级别定义

| 强度 | 代码 | 描述 | 示例 |
|------|------|------|------|
| **Full** | `full` | 完整支持，原生实现，性能无损 | PostgreSQL 的 transaction、MySQL 的 index |
| **Partial** | `partial` | 部分支持，某些场景受限 | SQLite 的 transaction（不支持 savepoint）、MongoDB 的 join（跨集合 join 受限） |
| **Weak** | `weak` | 有限支持，需要应用层补偿 | SQLite 的 fulltext（需要特殊扩展）、非分布式数据库的分布式事务 |
| **Emulated** | `emulated` | 通过模拟或降级支持 | SQLite 的 cte（版本 3.8.4+ 才有）、NoSQL 的复杂 join（在应用层实现） |
| **None** | `none` | 不支持 | SQLite 的 window_functions（版本 < 3.25.0）、普通 MySQL 的 json_path（5.6 不支持） |

### 4.5.2 支持强度的约束与降级链

- `full` 可直接用于最严格的查询需求
- `partial` 需要在 where 条件中详细说明限制
- `weak` 需要显式配置降级路径或应用层补偿
- `emulated` 仅在明确允许的场景使用（性能可能较差）
- `none` 不允许使用，必须有替代方案

示例：

```
query query_complex_json_agg
  where hasFeatures{json@L3:full, aggregation@L2:full} or query_fallback_agg
  // 只有在 json 支持为 full 且 aggregation 为 full 时才用这个查询
  
query query_partial_json_agg
  where hasFeatures{json@L2:partial, aggregation@L2:full}
  // json 为 partial 也可以，但需要清楚地记录限制
```

## 4.6 Adapter Capability Manifest

每个 Adapter 需要提供一份 Capability Manifest，声明自己支持哪些特性以及支持强度。

### 4.6.1 Manifest 格式

Adapter 在自己的 blueprint 文件中声明能力：

```blueprint
// adapters/postgres_adapter.blueprint

adapter PostgreSQL {
  version: "14.0+"
  kind: "sql"
  
  features: {
    transaction: {
      strength: full
      level: L4
      note: "Full ACID compliance, supports savepoints"
    }
    
    json: {
      strength: full
      level: L3
      note: "Native JSON type with jsonb_agg, json_path extraction"
    }
    
    fulltext: {
      strength: full
      level: L3
      note: "Built-in full-text search with various languages"
    }
    
    cte: {
      strength: full
      level: L3
      note: "Common Table Expressions including recursive"
    }
    
    window_functions: {
      strength: full
      level: L3
      note: "Row_number, rank, lead/lag, etc."
    }
    
    aggregation: {
      strength: full
      level: L3
      note: "Standard SQL aggregation functions"
    }
    
    partition: {
      strength: partial
      level: L2
      note: "Table partitioning supported but management is manual"
      constraints: ["declarative partitioning only", "no dynamic re-partitioning"]
    }
    
    vector: {
      strength: weak
      level: L1
      note: "Vector support requires pgvector extension"
      dependencies: ["pgvector extension must be installed"]
    }
    
    graph: {
      strength: none
      level: L0
      note: "Not supported, use Neo4j for graph queries"
    }
  }
  
  # 性能特征（可选，用于 Linker 优化选择）
  performance_profile: {
    latency_p99_ms: 10
    throughput_qps: 10000
    connection_pool_size: 50
    max_concurrent_queries: 100
  }
}
```

### 4.6.2 Manifest 的其他 Adapter 示例

```blueprint
// adapters/mongodb_adapter.blueprint

adapter MongoDB {
  version: "5.0+"
  kind: "nosql"
  
  features: {
    transaction: {
      strength: partial
      level: L2
      note: "Multi-document ACID transactions in replica sets, single-document always"
      constraints: ["only in replica sets", "limited to 16MB per transaction"]
    }
    
    json: {
      strength: full
      level: L4
      note: "Native document storage, powerful query language for nested data"
    }
    
    aggregation: {
      strength: full
      level: L3
      note: "Aggregation pipeline is very powerful"
    }
    
    join: {
      strength: partial
      level: L2
      note: "Cross-collection join via $lookup, limited recursion"
    }
    
    fulltext: {
      strength: full
      level: L2
      note: "Built-in text search index"
    }
    
    graph: {
      strength: weak
      level: L1
      note: "Limited graph traversal via nested arrays and $lookup"
    }
    
    cte: {
      strength: none
      level: L0
      note: "Not applicable for document database"
    }
  }
}

// adapters/neo4j_adapter.blueprint

adapter Neo4j {
  version: "5.0+"
  kind: "graph"
  
  features: {
    transaction: {
      strength: full
      level: L3
      note: "ACID transactions with full isolation"
    }
    
    graph: {
      strength: full
      level: L4
      note: "Native graph storage with Cypher query language"
    }
    
    traversal: {
      strength: full
      level: L4
      note: "Powerful graph traversal, pattern matching"
    }
    
    json: {
      strength: emulated
      level: L1
      note: "No native JSON, can store as string properties"
    }
    
    fulltext: {
      strength: full
      level: L2
      note: "Full-text search on node/relationship properties"
    }
    
    sql: {
      strength: none
      level: L0
      note: "SQL not applicable, use Cypher"
    }
  }
}
```

### 4.6.3 Manifest 与编译/运行时匹配

1. **编译期**
- 从 Adapter manifest 读取 features 与其 strength level
- 检查 Blueprint 中的 `where hasFeatures{feature:min_strength}` 是否与 Adapter 能力匹配
- 若 Adapter 的 strength < min_strength，该分支不可用，尝试 or 降级

2. **运行时**
- Linker 从监控树读取候选 Adapter
- 每个候选都包含其 capability_fingerprint（manifest 的摘要）
- 若候选的 strength 低于查询需求，排除该候选
- 继续尝试其他候选或触发降级

3. **示例对比**

```
Blueprint Query 需求：json@L3:full

候选 Adapter 及其 manifest：
- postgres: json@L3:full ✓ 可用
- mongodb: json@L4:full ✓ 可用（超满足）
- sqlite: json@L1:weak ✗ 不可用（强度太低）
→ LinkerActor 会选择 postgres 或 mongodb，不会选择 sqlite
```

## 4.7 Blueprint DSL 与模块化设计

Blueprint 采用可扩展的 DSL 来描述 Schema、关系、查询与约束，支持模块化和条件编译。

### 4.7.1 DSL 模块化（Module & Include）

支持多文件组织，避免单个文件过大。

**项目结构示例：**

```
project/
├── blueprint/
│   ├── adapters/
│   │   ├── postgres_adapter.blueprint    # PostgreSQL 能力声明
│   │   ├── mongodb_adapter.blueprint     # MongoDB 能力声明
│   │   └── neo4j_adapter.blueprint       # Neo4j 能力声明
│   ├── common/
│   │   ├── base_types.blueprint    # 通用类型定义
│   │   └── timestamps.blueprint    # 时间戳相关定义
│   ├── schemas/
│   │   ├── order.blueprint         # Order schema
│   │   ├── user.blueprint          # User schema
│   │   └── invoice.blueprint       # Invoice schema
│   ├── queries/
│   │   ├── order_queries.blueprint
│   │   └── advanced_queries.blueprint
│   └── main.blueprint              # 总入口
```

**Include 语法：**

```blueprint
// schemas/order.blueprint
include "common/base_types.blueprint"
include "common/timestamps.blueprint"
include "adapters/postgres_adapter.blueprint"

schema Order {
  id: int { primary_key: true }
  user_id: int { external_ref: User.id }
  total: decimal
  metadata: json { required_feature: json@L2:full }
  created_at: timestamp_utc
  
  requirements {
    must_have: transaction@L3:full + json@L2:full
    consistency: eventual
  }
}
```

### 4.7.2 条件编译（Conditional Compilation with where hasFeatures）

某些 Schema 或查询可能只在特定 Adapter 支持特定能力时才有意义。使用 `where hasFeatures{...}` 语法进行条件编译。

**示例 1：PostgreSQL 特定的强化 Schema**

```blueprint
schema OrderWithJSONAgg where hasFeatures{json@L3:full, aggregation@L2:full} or Order {
  // 只在 Adapter 支持 JSON L3:full + aggregation L2:full 时才编译这个 schema
  // 否则运行时自动降级到 Order schema
  
  id: int { primary_key: true }
  user_id: int
  
  // PostgreSQL JSON 强化聚合
  items_aggregate: json {
    computed_from: "SELECT jsonb_agg(...) FROM order_items"
  }
  
  metadata: json { required_feature: json@L3 }
  
  requirements {
    must_have: transaction@L3 + json@L3 + aggregation@L2
    consistency: eventual
  }
}
```

**示例 2：Graph 特定的查询**

```blueprint
query query_recommendation_graph($user_id: int, $depth: int) -> QueryHandle 
  where hasFeatures{graph@L3:full, traversal@L2:full} or query_basic_recommendation {
  
  symbol_id: "query_recommendation_graph_exec"
  
  required_capabilities: [
    graph@L3,
    traversal@L2,
    transaction@L2
  ]
  
  compile_hints {
    prefer_adapter_graph: true
  }
}

query query_basic_recommendation($user_id: int) -> QueryHandle {
  // 没有 where 条件，始终编译（作为降级版本）
  symbol_id: "query_basic_recommendation_exec"
  required_capabilities: [
    filter@L2,
    sort@L2
  ]
}
```

**示例 3：向量搜索特定能力**

```blueprint
schema ProductWithVector where hasFeatures{vector@L2:full, embedding@L1:full} or Product {
  id: int { primary_key: true }
  name: string
  description: string
  
  // 向量字段，仅在支持 vector 能力时编译
  embedding: vector(1536) {
    required_feature: vector@L2
  }
  
  requirements {
    must_have: vector@L2 + embedding@L1
  }
}

schema Product {
  // 基础版本，不包含向量字段，始终编译
  id: int { primary_key: true }
  name: string
  description: string
}
```

### 4.7.3 编译时决策与运行时降级

编译器会根据 `where hasFeatures{...} or fallback_name` 条件进行以下操作：

1. **编译期决策**
- 扫描所有 schema 和 query 定义
- 对每个带条件的定义，关联到显式指定的降级目标（或默认降级规则）
- 生成符号表时，为每个条件符号标记 feature 需求和对应的降级目标
- 验证降级链的完整性（无循环、无悬空引用）

2. **符号表结构**

```json
{
  "symbol_id": "order_with_json_agg",
  "kind": "schema",
  "entity": "OrderWithJSONAgg",
  "required_features": [
    {"feature": "json", "min_level": "L3"},
    {"feature": "aggregation", "min_level": "L2"}
  ],
  "compilation_condition": "where hasFeatures{json@L3, aggregation@L2}",
  "fallback_symbol": "order",           // 显式指定的降级目标（来自 or 子句）
  "fallback_type": "same_kind"         // 同类型降级（schema -> schema）
}
```

3. **运行时链接决策**

当 LinkerActor 进行符号绑定时：

```
For each symbol to bind:
  1. 检查 feature 需求
  2. 从监控树找满足需求的 Adapter 候选
  3. 如无候选：
     - 若 fallback_symbol 存在，使用降级符号
     - 否则拒绝执行并返回错误
  4. 更新 Link Table
```

### 4.7.4 CLI 对模块化与条件编译的支持

```bash
# 编译时会自动处理模块化和条件编译
eit-db-cli blueprint compile \
  --schema-dir ./blueprint/schemas \
  --queries-dir ./blueprint/queries \
  --common-dir ./blueprint/common \
  --adapters-dir ./blueprint/adapters \
  --output ./blueprint.bin \
  --include-conditional true

# 验证条件编译的降级链是否完整（确保每个条件符号都有降级路径）
eit-db-cli blueprint validate ./blueprint.bin \
  --check-fallback-chain true

# 生成调试镜像，显示所有条件符号和降级关系
eit-db-cli blueprint inspect ./blueprint.bin \
  --include-conditional-info true
```

### 4.7.5 特殊约定

1. **条件与降级的完整性**
- 对于每个 `where hasFeatures{...}` 的定义，必须用 `or fallback_name` 显式指定降级目标
- 降级目标必须是同类型的定义（schema 降级到 schema，query 降级到 query）
- 编译器验证所有降级链无循环引用

2. **or 子句的语义**
- `schema Foo where hasFeatures{feature:strength} or Bar` 意味着：
  - 当 Adapter 支持所需特性且强度满足时，使用 Foo
  - 当 Adapter 不支持或强度不足时，自动降级到 Bar
  - 两个 schema 在编译输出中都会出现在符号表中
  - 运行时 LinkerActor 根据实际能力自动选择正确版本

3. **版本管理**
- 条件编译不改变 Blueprint version
- 但条件集合、降级目标或 or 链变化时需要 MINOR 或 PATCH 版本升级

4. **运行时透明性**
- 从应用层看，调用 query 或 schema 时不需要知道是否在使用条件版本或降级版本
- 但执行计划和审计日志必须记录实际使用的符号 ID 和所在条件分支

### 4.7.6 细粒度条件化（When 子句）

除了整体级别的 `where...or` 降级外，还支持在字段和执行模式级别进行细粒度的条件化，通过 `when feature:strength` 子句实现。

**用途**：
- 在单个 schema 中，某些字段只在 Adapter 支持特定能力时才包含
- 在单个 query 中，执行策略根据 Adapter 能力动态调整
- 避免为每个强度级别创建多个 schema/query 定义

#### 4.7.6.1 Schema 中的 When 子句

```blueprint
schema Order {
  id: int { primary_key: true }
  user_id: int { index: true }
  total: decimal
  
  // 高级字段：仅在 json 支持 L3:full 时包含
  advanced_metadata: json { required_feature: json@L3:full }
    when json@L3:full
  
  // 中等字段：当 json 只有 L2:partial 时使用
  metadata_dict: json { required_feature: json@L2:partial }
    when json@L2:partial
  
  // 备选字段：当 json 根本不支持时使用
  metadata_text: text
    when json@none
  
  requirements {
    // 最少要求：即使 json 完全不支持也能工作（使用 metadata_text）
    must_have: transaction@L2:full
  }
}
```

**编译时处理**：
- 编译器见到 `when feature:strength` 时，为该字段生成多个版本
- 每个版本都被包含在符号表中，带上对应的 when 条件
- 如果某个强度级别没有对应的 when 字段，使用默认策略（通常是删除该字段或使用文本替代）

**运行时处理**：
- Linker 根据候选 Adapter 的实际能力，选择合适的字段集合
- 同一个 Order schema 可能在不同 Adapter 上有不同的字段组成
- 应用层看起来仍是单一 Order 类型，但字段差异由框架层透明处理

#### 4.7.6.2 Query 中的 When 子句

```blueprint
query query_complex_aggregation($conditions: FilterObject) -> QueryResult {
  symbol_id: "query_complex_aggregation_exec"
  
  // 高效路径：向下推送所有计算到 Adapter（需要 full 支持）
  execution_hints when aggregation@L3:full {
    push_aggregation: true
    push_window_functions: true
    allow_complex_expressions: true
    cache_plan: 10m
    expected_latency_p99: "50ms"
  }
  
  // 中等路径：部分计算在 Adapter，部分在应用层
  execution_hints when aggregation@L2:partial {
    push_aggregation: simple_only
    post_aggregate: true
    allow_complex_expressions: false
    cache_plan: 5m
    expected_latency_p99: "200ms"
  }
  
  // 降级路径：全部计算在应用层
  execution_hints when aggregation@L1:weak {
    push_aggregation: false
    full_post_aggregate: true
    cache_plan: 0            # 无缓存，每次重新计算
    expected_latency_p99: "1000ms"
    fallback_warning: "Aggregation is CPU-intensive on application layer"
  }
}
```

#### 4.7.6.3 When 子句的优先级与冲突处理

1. **多个 when 条件的选择顺序**
- 编译器按从最严格到最宽松的顺序排列 when 条件
- 运行时 Linker 从最严格条件开始尝试，若 Adapter 满足则使用，否则向下降级

2. **无匹配 when 时的处理**
- 若所有 when 条件都不匹配（Adapter 能力超出预期或不足），使用最宽松的条件或默认值
- 编译器可配置是否允许超匹配（能力超出预期）

3. **示例：三层降级**

```
when aggregation@L3:full     ← 最严格，性能最优
  ↓
when aggregation@L2:partial   ← 中等，需要应用补偿
  ↓
when aggregation@L1:weak      ← 最宽松，纯应用层
  ↓
[no match] → 使用 L1:weak 或拒绝执行
```

#### 4.7.6.4 编译与运行时的协作

1. **编译期**
- 为每个包含 when 子句的 schema/query，生成一个"条件执行计划树"
- 每个节点是一个 (feature:strength → execution_variant) 映射
- 验证所有路径都有终点（无悬空分支）

2. **运行时**
- Linker 拿到候选 Adapter 的 feature_level_map
- 从根节点开始遍历条件树
- 按实际 Adapter 能力选择对应分支
- 如果无分支匹配，触发降级策略

3. **符号表体现**

```json
{
  "symbol_id": "order_schema",
  "kind": "schema",
  "field_variants": [
    {
      "field_name": "advanced_metadata",
      "type": "json",
      "required_feature": {"feature": "json", "min_level": "L3:full"},
      "when_condition": "json@L3:full"
    },
    {
      "field_name": "metadata_dict",
      "type": "json",
      "required_feature": {"feature": "json", "min_level": "L2:partial"},
      "when_condition": "json@L2:partial"
    },
    {
      "field_name": "metadata_text",
      "type": "text",
      "when_condition": "json@none"
    }
  ]
}
```

#### 4.7.6.5 When 与 Where 的关系

- **Where**：全局级别降级，整个 schema/query 被替换
- **When**：局部级别差异，同一 schema/query 内的字段或执行策略差异化

| 维度 | Where | When |
|------|-------|------|
| 粒度 | 整体（全 schema/query） | 细粒度（字段/执行策略） |
| 创建方式 | 创建两个独立定义 + or 关联 | 单个定义内部多分支 |
| 符号表 | 两个 symbol | 一个 symbol + 多个 variant |
| 应用场景 | 能力差异很大，需要完全不同逻辑 | 能力差异小，只需调整某些参数或字段 |
| 运行时成本 | 逻辑分支简单（二选一） | 可能更多分支判断 |

## 5. 跨库查询强约束（v2.0 MUST）

1. 没有 Blueprint，不允许跨库查询。
2. Blueprint 状态不是 active，不允许跨库查询。
3. 任意跨库查询必须经过 ArangoDB 校验：
- 关系合法性
- 能力可用性
- 一致性策略匹配
4. 执行计划必须包含 BlueprintVersion。
5. 计划缓存键必须包含 BlueprintVersion 与 capability_fingerprint。
6. 执行记录必须可追溯到 BlueprintVersion。
7. 没有有效 Adapter 监控树快照，不允许跨库查询。
8. Adapter 运行时能力指纹与 Blueprint 不一致时，必须重校验或降级。

## 6. Adapter 监控树（P0 前置）

在 Blueprint 进入跨库协调之前，必须先完成 Adapter 监控树。

目标：把“当前可协调能力”从隐式状态变为显式可查询状态。

### 6.1 监控树分层

1. Root: Project
2. Group: FeatureDomain（按能力域分组，如 transaction/json/fulltext/graph/vector）
3. Node: AdapterInstance
4. Leaf: BackendEndpoint / Shard / Replica
5. EdgeMeta: health, latency, consistency_mode, capability_fingerprint, feature_level_map

### 6.2 必备字段（MVP）

- adapter_id
- adapter_kind（mysql/postgres/sqlserver/mongo/neo4j/arangodb...）
- version
- topology_role（primary/replica/shard/router）
- health_status（up/degraded/down/readonly）
- last_heartbeat_at
- capability_fingerprint
- feature_level_map（例如：{"transaction":"L3","json":"L2","graph":"L0"}）
- feature_confidence（能力检测可信度）
- region
- tenant_scope

### 6.3 与 Blueprint 的联动规则

1. Planner 生成跨库计划前，必须读取最新监控树快照。
2. 监控树变化事件触发计划缓存失效（按拓扑 hash 或能力指纹）。
3. 监控树与 Blueprint 约束冲突时，优先执行安全策略：降级或拒绝执行。
4. 监控树快照过期时，跨库计划进入只读保守模式或直接拒绝。

### 6.4 事件类型（建议）

- AdapterJoined
- AdapterLeft
- AdapterHealthChanged
- AdapterCapabilityChanged
- TopologyChanged
- ClusterRoleChanged

### 6.5 能力匹配与选择规则（关键）

协调和路由不按 SQL/NoSQL 分类，而按 feature 与支持级别决策。

1. QuerySpec 声明 required_capabilities（feature + min_level）。
2. Planner 从监控树提取候选 Adapter 的 feature_level_map。
3. 仅保留满足 min_level 的 Adapter 进入计划阶段。
4. 多候选时按优先级排序：
- health_status
- latency
- consistency_mode
- cost_hint（可选）
5. 若无 Adapter 满足最小能力级别：
- 触发降级策略（若 Blueprint 允许）
- 否则拒绝执行并返回可解释错误。

## 7. 运行时架构草案（AppHost + Actor 混合）

建议采用分层混合架构，而不是纯 AppHost 或纯 Actor。

- AppHost 层负责治理和控制面。
- Actor 层负责高并发执行和隔离。

### 7.1 AppHost 层职责

1. Blueprint 生命周期管理（发布、激活、回滚）
2. Adapter 监控树汇聚与快照发布
3. 全局策略下发（能力约束、一致性策略、访问策略）
4. 审计与可观测性入口

### 7.2 Actor 层职责

1. AdapterActor：单后端执行单元，处理本地查询与局部缓存
2. CoordinatorActor：跨库计划执行编排
3. MonitorActor：心跳和能力变化上报
4. CacheActor：计划缓存与失效广播

### 7.3 消息协议（MVP）

- PlanRequest
- PlanValidated
- ExecuteRequest
- ExecuteResult
- TopologyChanged
- CapabilityChanged
- PlanInvalidate

示例：

```json
{
  "type": "PlanRequest",
  "blueprint_id": "bp_ordering",
  "blueprint_version": "2.0.0-alpha.1",
  "query_name": "query_user_orders",
  "tenant": "t1",
  "param_shape_hash": "sha256:..."
}
```

### 7.4 并发与隔离建议

1. 一个 Adapter 可创建多个 Actor（按租户/业务域/分片键分区）。
2. 每个 Actor 持有局部热缓存，避免全局锁争用。
3. 通过 Supervisor 统一处理失败重启和限流。
4. 所有 Actor 执行都必须带 BlueprintVersion。

## 8. Blueprint 链接模型（Symbol & Dynamic Linking）

Blueprint 编译输出不是直接可执行的二进制，而是"可链接的中间产物"。

核心思想：把链接推迟到运行时，这样迁移和故障恢复只需要改链接表，无需重编译。

### 8.1 编译期产物结构

1. 抽象类型图（Type Graph）
- Entity 定义与字段约束
- 各字段的能力需求（capability requirements）

2. 查询模板与执行策略（Query Templates）
- 参数化的查询框架
- 每个查询的能力需求与降级策略

3. 符号表（Symbol Table）
- symbol_id：唯一标识，例如 `order_entity_binding`, `query_user_orders_exec`
- requirement：该符号需要的能力需求
  ```json
  {
    "symbol_id": "order_entity_binding",
    "entity": "Order",
    "required_features": [
      {"feature": "transaction", "min_level": "L3"},
      {"feature": "json", "min_level": "L2"}
    ],
    "fallback_policy": "migrate_to_compatible"
  }
  ```

### 8.2 运行时链接表（Link Table）

由 Linker Actor 维护，格式：

```json
{
  "symbol_bindings": {
    "order_entity_binding": {
      "required_features": [...],
      "candidates": ["postgres_primary", "postgres_replica"],
      "selected": "postgres_primary",
      "binding_version": 3,
      "bound_at": "2026-03-18T14:30:00Z"
    }
  },
  "total_relinks": 2,
  "last_relink_trigger": "postgres_primary:feature_downgrade"
}
```

### 8.3 链接决策规则

当 Linker 需要为一个 symbol 选择 Adapter 时：

1. 读取 monitoring tree 快照
2. 过滤出满足 required_features 的 Adapter 候选
3. 按优先级排序：
- health_status（up > degraded > down）
- latency（期望值内的优先）
- consistency_mode（匹配 Blueprint 要求的优先）
- region（本地优先）
4. 选择最优候选并记录绑定时间戳
5. 如果无满足候选，触发 fallback policy（降级/拒绝/告警）

### 8.4 迁移场景

1. Adapter 故障（health 从 up → down）
- 触发 TopologyChanged 事件
- Linker 立即重新求解受影响 symbol
- 如有候选，自动切换绑定
- 执行计划自动使用新绑定 Adapter

2. Adapter 能力降级（feature_level 下降）
- 监控树检测并触发 CapabilityChanged 事件
- Linker 校验当前绑定是否仍满足需求
- 如不满足，重新求解；如满足，保持当前绑定

3. 多个候选时的平衡
- 可配置"粘性"策略：倾向保持已绑定 Adapter
- 或采用"探索"策略：定期尝试切换到更优候选

## 9. Linker Actor 与自愈决策

新增一类核心 Actor：LinkerActor，负责符号绑定与可用性维护。

### 9.1 LinkerActor 职责

1. 维护全局 Link Table
2. 订阅 monitoring tree 变化事件
3. 当 Adapter 能力变化时，触发依赖 symbol 的重绑定决策
4. 记录绑定历史（审计与性能分析）
5. 定期验证当前绑定的可用性

### 9.2 自愈策略（BEAM 风格）

1. 检测：监控树发现 Adapter 故障或降级
2. 标记：将 Adapter 标记为 degraded 或 down
3. 尝试迁移：Linker 重新求解绑定到其他满足需求的 Adapter
4. 如迁移成功：后续查询自动用新 Adapter，无需介入
5. 如无可迁移地：
- 触发告警或降级策略（readonly、延迟、部分功能关闭）
- 尝试重启原 Adapter（若 Blueprint 允许）
- 若仍无法恢复，拒绝执行

### 9.3 事件流示例

```
监控树检测 postgres_primary 故障
  ↓
TopologyChanged 事件
  ↓
LinkerActor 收到事件
  ↓
查询受影响的 symbol（"order_entity_binding" 依赖 postgres_primary）
  ↓
从监控树读取候选（postgres_replica 满足 transaction L3）
  ↓
更新 Link Table: order_entity_binding.selected = "postgres_replica"
  ↓
广播 LinkingUpdated 事件
  ↓
下一条查询自动使用 postgres_replica
```

### 9.4 与 CacheActor 的联动

1. Link Table 版本变化时，自动失效相关计划缓存
2. CacheActor 订阅 LinkingUpdated 事件
3. 根据受影响 symbol 快速定位需失效的计划
4. 避免全量清空，提升缓存命中率

## 10. 版本与失效协议

### 10.1 版本规则

- MAJOR：关系语义或约束破坏性变更
- MINOR：新增兼容实体/关系/查询定义
- PATCH：注释、元数据或非语义修正

### 10.2 失效规则

以下事件必须失效计划缓存：

- Blueprint version 变化
- capability_fingerprint 变化
- 关系边定义变化
- 一致性策略变化

### 10.3 安全回退

当 Runtime Artifact 与 Control Blueprint 版本不一致时：

1. 尝试重新拉取控制面元数据并重建计划。
2. 若仍不一致，回退到保守执行策略。
3. 仍失败则拒绝执行并输出可诊断错误。

## 11. 计划缓存键规范（草案）

建议键：

query_name + blueprint_id + blueprint_version + capability_fingerprint + tenant_scope + adapter_topology_hash + param_shape_hash

说明：

- 不含原始参数值，避免缓存爆炸
- 使用参数形状 hash（param_shape_hash）
- 通过 topology hash 避免后端拓扑变化导致错计划

## 12. eit-db-cli 预编译流水线（草案）

### 12.1 新增命令建议

- eit-db-cli blueprint compile
- eit-db-cli blueprint validate
- eit-db-cli blueprint package
- eit-db-cli blueprint inspect
- eit-db-cli adapter-tree snapshot
- eit-db-cli adapter-tree watch

### 12.2 编译流程

1. 收集 Schema 与 Query Builder 定义
2. 解析为中间 Blueprint IR
3. 绑定 Adapter 能力矩阵
4. 输出 Runtime Artifact（二进制）
5. 输出调试镜像（文本）
6. 可选发布元数据到 ArangoDB

### 12.3 运行时加载顺序

1. 加载本地 Runtime Artifact
2. 拉取 Control Blueprint 元数据（轻量）
3. 拉取 Adapter 监控树快照（轻量）
4. 校验版本和能力指纹
5. 校验拓扑 hash 和健康状态
6. 命中计划缓存则执行
7. 未命中则生成并回写计划缓存

## 13. ArangoDB 专项版本边界（建议 v2.1.0）

v2.0 先定义协议和抽象，不强耦合 Arango 的高级特性。
v2.1.0 专项引入：

- Arango Graph 优化关系遍历
- 跨库不可联结数据的默认托管策略
- 基于图关系的计划优化 hint

## 14. 风险与缓解

1. Arango 成为关键路径瓶颈
- 缓解：元数据本地缓存 + 版本戳校验 + 后台预热

2. Blueprint 过大、维护成本高
- 缓解：分域拆分 blueprint（按业务域/租户）

3. 缓存失效不正确导致错计划
- 缓解：统一版本戳驱动，禁止手工局部失效

4. 多 Adapter 能力漂移
- 缓解：启动体检 + 定时能力探针 + 指纹重算

5. Actor 规模扩大后的消息风暴
- 缓解：分区 Actor + 事件去重 + 批量失效广播

6. 监控树快照延迟导致误判
- 缓解：快照 TTL + 增量事件流 + 兜底即时探测

## 15. 实现架构与技术栈选择

本章讨论 v2.0 如何在实际系统中落地 AppHost + Actor 混合架构，以及关键技术选择。

### 15.1 架构分层与协作流

```
┌─────────────────────────────────────────────────────┐
│           Application Layer / gRPC Endpoint         │
├─────────────────────────────────────────────────────┤
│                  AppHost Control Plane               │
│  ┌─────────────┬──────────────┬──────────────────┐  │
│  │ Blueprint   │ Adapter      │ Global Registry  │  │
│  │ Manager     │ Monitor      │ & Planner        │  │
│  ├─────────────┴──────────────┴──────────────────┤  │
│  │ Event Bus / Message Broker (Redis / NATS)    │  │
│  └────────┬──────────────────────────────────────┘  │
├──────────┼───────────────────────────────────────────┤
│          │        Actor Runtime Layer                │
│  ┌───────▼─────────────────────────────────────────┐ │
│  │ Actor Supervisor (Fault Tolerance)             │ │
│  ├────────────────────────────────────────────────┤ │
│  │ AdapterActors (Per Adapter Instance)           │ │
│  │ CoordinatorActor (Cross-DB Orchestration)      │ │
│  │ MonitorActor (Heartbeat & Capability Sync)     │ │
│  │ LinkerActor (Symbol Binding & Routing)         │ │
│  │ CacheActor (Plan Cache & Invalidation)         │ │
│  └────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────┤
│              Storage & External Systems              │
│  ┌──────────────┬──────────────┬─────────────────┐  │
│  │ ArangoDB     │ Multiple     │ Monitoring      │  │
│  │ (Control BP) │ Adapters     │ System (Prom)   │  │
│  └──────────────┴──────────────┴─────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### 15.2 AppHost 层（控制面）职责与实现

**职责**：
1. Blueprint 生命周期管理（加载、验证、激活、回滚）
2. Adapter 监控树汇聚（从 MonitorActor 收集心跳与能力变化）
3. 全局策略下发（能力约束、一致性等级、访问控制）
4. Planner 协调（维护全局查询规划器状态）
5. 审计与可观测性

**实现建议**：
- 不追求高并发，追求强一致性
- 可采用同步 API（REST / gRPC）
- 单线程处理关键路径（Blueprint 更新、策略变更）
- 用 Channel / Queue 与 Actor 层通信：
  ```go
  AppHost EventLoop:
    for event := range eventChan {
      case BlueprintPublished:
        invalidatePlan()
        broadcastToLinker()
      case AdapterHealthChanged:
        updateMonitoringTree()
        triggerRelink()
    }
  ```

### 15.3 Actor 层（执行面）与监控树协作

**核心 Actors**：

1. **MonitorActor**（监控树源头）
   - 定期发起向所有 Adapter 的心跳探测
   - 收集每个 Adapter 的健康状态、延迟、能力指纹
   - 检测能力降级（对比上次记录的 feature_level_map）
   - 若能力变化，发送 CapabilityChanged 事件到 EventBus

2. **LinkerActor**（符号动态链接）
   - 订阅 MonitorActor 的 TopologyChanged 事件
   - 维护 Link Table（symbol_id → selected_adapter 映射）
   - 当拓扑变化时，重新求解受影响 symbols 的最优绑定
   - 广播 LinkingUpdated 事件

3. **AdapterActor**（每个 Adapter 实例一个）
   - 处理该 Adapter 的所有查询执行
   - 维护连接池与本地缓存
   - 向 MonitorActor 上报心跳与能力信息

4. **CoordinatorActor**（跨库编排）
   - 接收来自应用层的跨库查询请求
   - 查询 Planner 得到执行计划
   - 根据 Link Table 获取候选 Adapter
   - 协调多个 AdapterActor 完成查询
   - 汇总结果并处理一致性

5. **CacheActor**（计划缓存）
   - 维护计划缓存与结果缓存
   - 订阅 LinkingUpdated 事件，按 symbol 精确失效缓存
   - 提供缓存命中和失效统计

**事件流示例**：

```
User Query
  ↓
CoordinatorActor::PlanRequest
  ↓
Planner (AppHost) 返回执行计划
  ↓
CoordinatorActor 查 Link Table 获取 Adapter 列表
  ↓
监控树反馈 postgres_primary 故障
  ↓
MonitorActor 发 TopologyChanged
  ↓
LinkerActor 重新绑定 → LinkingUpdated
  ↓
CacheActor 收到 LinkingUpdated，失效相关计划
  ↓
CoordinatorActor 自动使用新的 Adapter 绑定
  ↓
查询在新 Adapter 上执行成功
```

### 15.4 核心依赖库选择

#### 15.4.1 Actor 框架

| 候选 | 语言 | 优点 | 缺点 | 适配 EIT-DB | 建议 |
|------|------|------|------|-------------|------|
| **Akka.NET** | C# | 成熟、功能完整、Cluster 支持 | 非 Go，跨域麻烦 | ✗ | 否 |
| **Akka** (Java) | Java | 久经沙场，Akka Cluster / Streams | 非 Go，JVM 开销 | ✗ | 否 |
| **Protoactor-Go** | Go | 轻量级、与 Go 协程良好配合 | 生态不如 Akka | ✓ | 可考虑 |
| **Dapr** | Go | 分布式应用工具集，Actor 模式内置 | 较重，学习曲线陡 | ✓ | 备选 |
| **原生 Go 协程 + Channel** | Go | 简单、无依赖、与 Go 生态统一 | 需要自己实现 Supervisor、路由等 | ✓ | **推荐** |

**建议**：  
在 v2.0 MVP 阶段，**使用原生 Go 协程 + Channel**，不引入重型依赖。理由：
- EIT-DB 的 Actor 需求不是最复杂的（无远程 Actor、单机或 K8s 内部通信）
- 使用 Protoactor-Go 反而增加了理解成本和维护负担
- 后续若确实需要分布式 Actor，再切换或扩展

#### 15.4.2 消息队列 / Event Bus

| 候选 | 用途 | 优点 | 缺点 | 适配 | 建议 |
|------|------|------|------|------|------|
| **Redis Pub/Sub** | 事件广播 | 内存快、简单、支持模式订阅 | 消息不持久化 | ✓ | 开发/测试 |
| **NATS** | 事件系统 | 轻量级、可持久化 (JetStream)、云友好 | 学习成本略高 | ✓ | **推荐** |
| **Kafka** | 分布式事件流 | 海量数据、持久化、容错强 | 过重，运维成本高 | ✗ | 过度设计 |
| **Channel-based** | 本地消息 | 零依赖、与 Actor 紧密集成 | 仅本进程 | ✓ | 单机实现 |

**建议**：  
- v2.0.0-alpha：用 Channel-based 本地实现
- v2.0.0-beta：集成 NATS，支持多进程 / 分布式 Actor
- v2.2.0：若需要分布式，升级到 NATS JetStream 或 Kafka

#### 15.4.3 监控树存储与查询

| 组件 | 候选 | 优点 | 缺点 | 建议 |
|------|------|------|------|------|
| 元数据存储 | Redis + 定期持久化 | 快速、支持 TTL | 故障后丢失 | MVP 阶段可用 |
|  | etcd | 强一致性、Lease TTL、Watch 机制 | 运维成本、写吞吐限制 | 中期考虑 |
|  | ArangoDB | 已用于 Control BP，复用存储 | 稍微过重 | 长期方案 |
| 快照 | 内存 Snapshot + 定期备份 | 多快好省 | 故障重启需要恢复 | MVP 推荐 |
| 版本控制 | Git-like log | 完整审计、可回滚 | 查询性能一般 | 备选 |

**建议**：  
- v2.0 使用 Redis + 内存快照，通过 AppHost 定期 flush 到本地文件
- 监控树 TTL = 60s（心跳间隔），不需要强持久化
- 建立一个 "topology_checkpoint" 机制，每 5 分钟自动备份到文件或 ArangoDB

#### 15.4.4 RPC 通信

| 框架 | 特点 | 适配 | 建议 |
|------|------|------|------|
| **gRPC** | 类型安全、高效、Streaming | ✓ | Actor 间通信首选 |
| **HTTP REST** | 简单、浏览器友好 | ✓ | 面向外部 API 首选 |
| **Cap'n Proto** | 零拷贝、低延迟 | ✓ | 性能关键路径备选 |

**建议**：  
- Actor 间：gRPC（类型安全 + 性能）
- 外部 API（应用层调用）：REST / OpenAPI
- 跨语言 SDK：支持 gRPC + JSON 双协议

### 15.5 造轮子 vs 用现成库决策表

| 功能 | 造轮子 | 用现成库 | 建议 |
|------|--------|---------|------|
| **Actor 调度与消息派发** | 复杂度中 | Protoactor-Go | **造轮子**（v2.0 MVP） |
| **Event Bus / Pub-Sub** | 复杂度中 | NATS / Redis | **用现成库**（NATS）|
| **Symbol Linker 逻辑** | 复杂度高，业务特定 | 无通用库 | **造轮子**（核心逻辑） |
| **Query Planner（IR Compiler）** | 复杂度高 | 无通用库 | **造轮子**（已有架构） |
| **Monitoring Tree 快照与查询** | 复杂度低 | Redis / etcd | **用现成库**（Redis） |
| **Blueprint DSL Parser** | 复杂度中 | ANTLR / Pest | **用现成库**（Go 的 pest 等） |
| **Cache 管理** | 复杂度中 | go-cache / freecache | **用现成库** |
| **Distributed Tracing** | 复杂度中 | Jaeger / Zipkin | **用现成库**（Jaeger） |

### 15.6 v2.0 物理架构建议

**单机部署（开发/小规模）**：
```
┌────────────────────────────────────┐
│  EIT-DB v2.0 (Single Process)      │
├────────────────────────────────────┤
│  AppHost (goroutine)               │
│  + Actor Supervisor (goroutine)    │
│  + MonitorActor / LinkerActor etc. │
├────────────────────────────────────┤
│  Local Redis (embedded or external)│
│  Local File (Blueprint + Snapshot) │
└────────────────────────────────────┘
      ↓
External Services:
  - Multiple DB Adapters (MySQL, PG, Mongo, etc.)
  - ArangoDB (Control BP)
  - Optional: Jaeger (tracing)
```

**分布式部署（生产/多进程）**：
```
┌──────────────────────────────────────────────────┐
│ Kubernetes Cluster                               │
├──────────────────────────────────────────────────┤
│ Pod: EIT-DB AppHost                              │
│   ├─ AppHost (single replica, Leader Election)   │
│   └─ Integrated gRPC server                      │
├──────────────────────────────────────────────────┤
│ Pod: EIT-DB Actor Workers (多副本)               │
│   ├─ Actor Supervisor                           │
│   ├─ AdapterActors (连接到各后端)                │
│   └─ gRPC client 连接 AppHost                    │
├──────────────────────────────────────────────────┤
│ Shared Infrastructure                            │
│   ├─ Redis (Event Bus + Monitoring Tree)         │
│   ├─ NATS (分布式事件流)                         │
│   ├─ ArangoDB (Control Blueprint)                │
│   └─ Jaeger / Prometheus (observability)         │
└──────────────────────────────────────────────────┘
```

### 15.7 关键路径性能与可靠性

**跨库查询关键路径耗时分解**（目标 < 500ms）：

```
User Query arrives
  ├─ AppHost Planner                 ~5ms
  │  ├─ Link Table lookup            ~1ms
  │  ├─ Capability match             ~2ms
  │  └─ Plan generation              ~2ms
  │
  ├─ CoordinatorActor dispatch       ~2ms
  │
  ├─ Execute on Adapters            (主要时间)
  │  ├─ Adapter 1 query              ~150ms
  │  ├─ Adapter 2 query              ~120ms
  │  └─ [Parallel]                   ~0ms additional
  │
  ├─ Result aggregation              ~10ms
  │
  └─ Return to client                ~5ms

  Total: ~150ms (Adapter-bound)
```

**故障检测与恢复时间**：
- Adapter 故障检测：60s 心跳超时 → 标记 degraded
- Linker 重新绑定：< 100ms
- 缓存失效：< 10ms
- 总故障转移时间：< 1 分钟

## 16. 下一步（建议 2 周内）

1. 先落地 Adapter 监控树 MVP（快照 + 事件 + 指纹）
2. 锁定 Blueprint MVP 字段（实体、关系、查询、策略）
3. 产出 JSON Schema（或 Go struct）定义
4. 定义 ArangoDB Control 集合结构（顶点/边）
5. 做一个端到端 PoC：2 个 SQL Adapter + 1 个跨库查询
6. 输出第一版计划缓存实现与命中统计

---

该文档为设计草案，供 v2.0 架构评审使用。
