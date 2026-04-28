# Arango + Blueprint + Query 增强执行草案（Draft v0.1）

更新时间：2026-04-24
状态：架构草案冻结（进入 Collaboration Mode 实施准备）

## 1. 决策结论

按当前优先级执行：

1. 先做 ArangoDB 适配器（MVP）
2. 基于 ArangoDB 做跨库查询缓存与跨库关联关系记录（最小闭环）
3. 先做 Blueprint（规范先行 + 最小运行时）
4. 再推进 Query 增强主线
5. 最后做 Schema 定义和 Relation 封装的系统性优化

该顺序的目标是先固定架构锚点，降低 Query 增强与 Schema 重构的返工概率。

---

## 2. 总体目标

### 2.1 业务目标

1. 引入一个既支持图关系又适合文档缓存的中枢后端（ArangoDB）。
2. 在框架层形成跨库数据编排能力，不把跨库能力硬编码到某个 Adapter。 
3. 通过 Blueprint 统一描述数据集边界、关系路径、缓存策略与跨库映射。 
4. 为模块化查询增强提供可验证的真实落地点，而不是纯抽象设计。

### 2.2 交付约束

1. 每一阶段都必须可独立发布、可灰度回滚。 
2. 不在单阶段内同时引入大规模破坏性 API。 
3. 保持旧 API 可用，通过兼容层渐进切换。 
4. 所有新能力必须有最低限度集成测试。

### 2.2.1 当前 API 边界决策（新增）

当前阶段对执行 API 的约束如下：

1. 保留统一应用层执行入口方向（ExecuteAuto / auto route）。
2. 不继续单独推进 AQL/Cypher 的类事务 API 抽象。
3. 若后续 Query 增强进入中心化执行路径，则优先考虑：
	`aql like -> query ir -> adapter compiler -> database language/binary files`
4. 因此 `ExecuteUnit` / 类事务语义必须在 Query IR 与 Adapter Compiler 边界明确后再决定，不提前冻结。

### 2.3 运行模式与架构分层（新增）

本项目明确支持两种运行模式：

1. 单适配器模式（ORM Mode）
- 用户显式配置单一主 Adapter。
- 系统行为等价于 ORM，不自动拉起任何协作基础设施。
- 跨库查询、跨库关系记录、跨库缓存默认全部关闭。

2. 协作层模式（Collaboration Mode）
- 用户显式启用跨库查询与/或多级缓存能力。
- 当启用跨库查询时，系统自动拉起“内部托管 ArangoDB Adapter”（Managed Arango）。
- 当启用多级缓存时，系统自动拉起“内部托管 Redis Adapter”（Managed Redis）。
- 内部托管 Adapter 不暴露给用户直接管理，不占用用户业务 Adapter 配额。

边界说明：

1. 用户仍可显式启用 ArangoDB/Redis 作为业务 Adapter。
2. 内部托管 Adapter 与用户业务 Adapter 必须逻辑隔离（命名空间、连接池、指标标签、权限）。
3. 协作层能力由系统托管适配器提供，业务侧可无感使用但可观测。

---

## 2.4 托管适配器治理规则（新增）

### 命名与隔离

1. Managed Arango 使用固定内部实例标识（例如 system-managed-arango）。
2. Managed Redis 使用固定内部实例标识（例如 system-managed-redis）。
3. 与用户显式配置实例必须在以下维度隔离：
- 连接配置域
- 缓存 key 前缀域
- 集合/图命名空间
- 指标与日志标签

### 生命周期

1. 托管实例按能力开关懒加载。
2. 关闭对应能力时托管实例可优雅回收。
3. 托管实例异常不应直接导致主业务 Adapter 退出，需降级处理。

### 冲突处理

1. 用户显式启用 Arango/Redis 不替代托管实例。
2. 若用户希望复用同一物理服务，仅允许通过“受控桥接配置”启用，默认禁止共享命名空间。
3. 默认策略为“逻辑双实例、可同物理地址、强命名空间隔离”。

### 安全与权限

1. 托管实例建议使用独立账号与最小权限。
2. 不允许托管实例读写用户业务保留命名空间。
3. 凭据来源与刷新策略需可审计。

---

## 2.5 协作消息层设计（新增）

为替代“中心化调度器优先”路径，协作模式下新增消息协作主路径：

1. Managed Redis 从“缓存能力”扩展为“缓存 + 消息协作者”。
2. Managed Arango 承接消息持久化与图语义追踪（低层消息传递图，不引入高层社交语义）。
3. 适配器间通信拆分为“请求”和“消息”两个层面：
- 请求层：适配器发起协作请求，由协作层编排并下发。
- 消息层：协作层通过 Redis 发送事件，并在 Arango 记录 sender/receiver/message 关系链。

### 消息协作原则

1. 主路径采用 Redis Streams + Consumer Group（持久、可重放、可转移消费）。
2. 采用 at-least-once 语义，消费端必须幂等。
3. 使用 ticks/version 字段记录消息发送版本与消费版本，辅助去重与乱序诊断。
4. 消息堆积通过定时主动消费与重扫机制缓解（非仅依赖被动订阅）。
5. 命名空间通过内部前缀 + GUID/UUID（可选雪花 ID）强隔离。

### Arango 侧图模型（消息账本）

1. 顶点 online_adapter_node：在线适配器实例元数据。
2. 顶点 request_node：协作请求元数据。
3. 顶点 message_node：消息元数据（topic/stream、ticks、状态）。
4. 边 emits：adapter -> message。
5. 边 delivers_to：message -> adapter。
6. 边 belongs_to_request：message -> request。

该模型用于追踪消息来源、投递路径、失败重试与因果关系，不替代业务实体关系图。

---

## 3. 分阶段路线

## P0（1 周）：ArangoDB Adapter MVP

### 目标

完成 ArangoDB 的最小适配能力，作为后续跨库缓存与关系记录的落点。

### 范围

1. 新增 ArangoDB ConnectionConfig 与配置解析。 
2. 新增 ArangoDB Adapter 基础能力：Connect、Ping、Close、GetRawConn。 
3. 新增最小查询执行入口（建议先走 native 文本/AQL 透传，不做完整 QueryBuilder）。 
4. 注册为 Descriptor 模式，并补 Metadata。 
5. 启动能力检查中加入 arango runtime 的最小检查项。
6. 支持托管实例启动路径（Managed Arango）与显式实例路径并存。

### 非目标

1. 不做完整 AQL QueryBuilder。 
2. 不做复杂事务语义兼容。 
3. 不做跨库编排。

### 验收标准

1. 可以通过 Config 和 NewRepository 正常创建 Arango Repository。 
2. 能执行最小查询与写入（至少 1 读 1 写）。 
3. Startup capability 报告可见 Arango 关键状态。 
4. go test 全量通过。
5. 协作层模式下可自动拉起 Managed Arango，单适配器模式下不会拉起。

---

## P1（1 周）：跨库缓存与关联记录最小闭环

### 目标

利用 ArangoDB 建立跨库协作的基础设施：

1. 跨库查询缓存记录（查询意图与结果摘要索引）。 
2. 跨库关联关系记录（实体之间的跨源边）。

### 范围

1. 新增 CrossSourceCacheStore 抽象（建议接口独立于 CacheBackend）。 
2. Arango 实现 CrossSourceCacheStore。 
3. 新增 CrossSourceRelationStore 抽象。 
4. Arango 实现 CrossSourceRelationStore。 
5. 在现有执行路径上增加可选写入钩子（默认关闭，配置开启）。
6. 当多级缓存能力开启时自动拉起 Managed Redis（独立命名空间）。

### 数据模型建议

1. 集合 cross_source_cache
- key: cache_key
- fields: source_adapters, query_fingerprint, ttl, payload_ref, updated_at

2. 集合 cross_source_relation
- key: relation_id
- fields: left_entity, right_entity, relation_type, source, confidence, updated_at

3. 图 cross_source_graph
- 顶点：entity 节点
- 边：relation 记录

### 非目标

1. 不做复杂一致性协议。 
2. 不做强事务跨库写入。 
3. 不做多后端实现（先只实现 Arango 版本）。

### 验收标准

1. 通过配置开关可启停跨库缓存写入。 
2. 通过配置开关可启停跨库关系写入。 
3. 至少 3 个适配器组合场景能写入并查询回读记录。 
4. 性能回归在可控范围（单请求新增耗时有上限阈值）。
5. Managed Redis 与用户显式 Redis 同时存在时无键空间冲突。

---

## P1.5（并行 3-5 天）：协作消息总线最小落地（新增）

### 目标

在不引入复杂调度器重构的前提下，建立协作模式的事件通信主路径。

### 范围

1. 基于 Managed Redis 增加协作消息 stream（按 adapter/domain 分段）。
2. 增加消息 envelope（message_id、request_id、trace_id、ticks、payload_ref）。
3. 增加消费幂等记录（idempotency_key + consumed_tick）。
4. 增加定时主动消费任务与积压重扫逻辑。
5. 在 Managed Arango 写入 message_node/request_node/online_adapter_node 及三类边。
6. 保留旧调度入口作为兼容 fallback（默认不作为主路径）。

### 非目标

1. 不实现全局消息编排优化器。
2. 不实现跨消息事务一致性。
3. 不实现通用消息中间件抽象（先固定 Redis 实现）。

### 验收标准

1. 至少 2 组适配器可通过协作消息链完成请求到投递闭环。
2. 重复消息可被幂等拦截，ticks 版本可用于诊断。
3. 积压消息可由主动消费任务显著回收（阈值可配置）。
4. Arango 可查询 sender -> message -> receiver 与 request 关联链。

---

## P2（1 周）：Blueprint 规范与最小运行时

### 目标

先定义并落地 Blueprint 的最小可运行框架，作为后续 Query 增强和 Schema 重构的上位规范。

### 当前落地状态（2026-04-24）

1. 已新增 Go 侧 Blueprint 核心类型：datasets、entities、relations、module slots、cache policy。
2. 已新增 Blueprint 校验器与 BlueprintRegistry。
3. 已支持 JSON/YAML Blueprint 加载。
4. 已支持 Repository 读取 Blueprint RouteHint，用于运行时路由提示。
5. 当前仍不接管完整执行链，保持最小运行时边界。

### Blueprint 定位

Blueprint 是数据集级别的编排描述，不替代 Schema 本身。

它描述：

1. 数据集边界（dataset）。 
2. 跨库实体映射（entity map）。 
3. 关系路径与语义（relation map）。 
4. 查询模块挂载点（module slots）。 
5. 缓存策略（cache policy）。

### 建议结构（草案）

1. BlueprintID、Version、Owner。 
2. Datasets：参与的 Adapter 与库域。 
3. Entities：统一实体名与各源映射。 
4. Relations：关系类型、方向、来源与置信度策略。 
5. Modules：可挂接 QueryModule 的节点。 
6. CachePolicy：L1/L2/L3 与 cross-source 策略。

### 最小运行时范围

1. Blueprint 文件/结构加载。 
2. Blueprint 校验（字段完整性、映射合法性）。 
3. BlueprintRegistry 管理。 
4. 运行时读取 Blueprint 并提供路由元信息，不接管完整执行。

### 非目标

1. 不做 Blueprint 驱动的自动执行引擎。 
2. 不做复杂可视化。

### 验收标准

1. 至少 1 个 Blueprint 可通过校验并注册。 
2. 查询入口可读取 Blueprint 元信息用于日志与路由提示。 
3. 无 Blueprint 时系统行为保持兼容。
4. Repository 可通过 BlueprintRegistry 按 Blueprint ID 解析 RouteHint。

---

## P3（2 周）：Query 增强落地（基于 Blueprint 与 Arango）

### 目标

把既有 Query 增强设计从文档推进到可运行版本，优先实现可组合与可门禁，不追求一步到位优化器。

### 范围

1. QueryModule 与 QueryRef 基础类型落地。 
2. QueryUnit 执行单元落地。 
3. Compose -> Bind -> Plan -> Gate -> Execute 最小流水线。 
4. RelationCapabilityGate 接 Blueprint 关系约束。 
5. 执行结果可选写入 CrossSourceCacheStore 和 RelationStore。

### 与 Arango 的结合点

1. 跨库缓存索引写入 Arango。 
2. 关系推断结果写入 Arango 图边。 
3. 查询模块可读取 Arango 关系作为辅助输入。

### 非目标

1. 不做全局代价优化器。 
2. 不做分布式事务。

### 验收标准

1. 至少 2 个跨适配器协作查询样例可运行。 
2. Gate 能阻断不支持关系能力的错误派发。 
3. 缓存命中与关系写入可观测。 
4. 回退开关可关闭新增路径并恢复旧行为。

---

## P4（1-2 周）：Schema/Relation 定向优化（由 Blueprint 反向驱动）

### 目标

不做全量重写，按 Blueprint 与 Query 增强暴露的问题点进行定向优化。

### 优先优化点

1. Schema 字段映射一致性与版本标识。 
2. Relation 定义中的方向、基数、可选约束。 
3. 跨源实体 ID 规范。 
4. 与 QueryRef/RelationRef 的对齐层。

### 验收标准

1. 现有 Schema/Relation API 保持兼容。 
2. Blueprint 映射冲突率下降。 
3. Query 增强路径中的 Relation 错误率下降。

---

## 4. 技术切片建议（可并行）

### 切片 A：Adapter 与配置

1. Arango Config + Validate + DefaultConfig。 
2. Descriptor 注册 + Metadata。 
3. Startup capability 最小探针。

### 切片 B：跨库记录存储

1. CrossSourceCacheStore 抽象。 
2. CrossSourceRelationStore 抽象。 
3. Arango 实现 + 开关控制。

### 切片 C：Blueprint

1. 类型定义。 
2. 校验器。 
3. Registry。 
4. 执行入口读取集成。

### 切片 D：Query 增强执行链

1. QueryUnit 生成。 
2. Gate。 
3. 执行器最小串联。 
4. 结果写回存储。

### 切片 E：协作消息层（Redis + Arango）

1. Message envelope 与 ticks/version 协议。 
2. Redis Stream 生产、消费、主动重扫。 
3. 幂等键与消费版本记录。 
4. Arango 消息账本图写入与查询。

---

## 5. 风险与回滚

### 主要风险

1. Arango 引入后增加运维复杂度。 
2. 跨库关系记录可能带来数据噪声。 
3. Query 增强与旧执行路径冲突。 
4. Blueprint 过早做重导致推进变慢。
5. 托管实例与用户实例配置混淆导致数据污染。
6. 消息重复消费、乱序与积压导致协作结果抖动。

### 控制策略

1. 功能开关默认关闭，按模块灰度。 
2. 先写索引与摘要，不写全量大对象。 
3. 关键路径保留旧执行 fallback。 
4. Blueprint 先最小运行时，避免过度设计。
5. 托管实例强制命名空间隔离并输出冲突告警。
6. 统一消息 envelope，强制 idempotency_key + ticks/version 双校验。
7. 增加主动消费与积压扫描定时任务，避免长时间失效消息堆积。

### 回滚方案

1. 关闭跨库存储开关即可回退到旧路径。 
2. 关闭 Query 增强开关，恢复原执行入口。 
3. 保留旧 Schema/Relation API，避免硬切换。

---

## 6. 观测与测试要求

### 观测

1. 查询命中来源：本地缓存、跨库缓存、原始执行。 
2. 关系记录写入量与失败量。 
3. Blueprint 命中率与校验失败类型。 
4. Gate 拦截统计。
5. 托管实例健康度、启停次数、降级次数。
6. 用户实例与托管实例冲突告警计数。
7. 消息生产/消费速率、积压长度、重试次数、死信数量。
8. sender/request/message/receiver 图链路完整率。

### 测试

1. 单元测试：Config、Descriptor、Store、Blueprint 校验。 
2. 集成测试：多 Adapter 协作 + Arango 写入回读。 
3. 回归测试：旧 API 与旧查询路径稳定性。 
4. 压测：跨库记录打开后的延迟与吞吐变化。

---

## 7. 需要你确认的 8 个点

1. Arango 在当前体系中的定位：官方 Adapter 还是实验 Adapter。 
2. CrossSourceCacheStore 默认开关：全局关还是按 Blueprint 开。 
3. 关系记录是否允许异步写入（建议允许）。 
4. Blueprint 存储形式：代码结构体优先还是支持外部文件。 
5. Blueprint 版本策略：语义版本还是时间戳版本。 
6. Query 增强首批支持的 Adapter 组合。 
7. 跨库实体主键标准化规则。 
8. 可接受的性能预算上限。

补充确认项（新增）：

9. 托管实例是否允许与用户实例复用同一物理地址（默认允许但强制逻辑隔离）。
10. 托管实例账号是否由系统自动创建还是用户预置。
11. 托管实例失败时的默认策略：功能降级继续服务还是严格失败。
12. 协作层模式默认是否启用托管实例观测面板。
13. ticks 字段采用单调版本还是逻辑时钟方案。
14. 主动消费定时任务的默认间隔与批次上限。
15. 消息 ID 生成策略：UUID 还是雪花 ID（或混合）。
16. online adapter 元数据的过期与下线判定策略。

---

## 8. 下一步（建议）

若该草案方向确认，下一步直接进入 P0 设计细化，产出：

1. Arango Adapter 接口清单。 
2. Arango Config 字段与默认值。 
3. P0 对应测试清单与样例。

执行任务清单（新增）：

1. COLLAB_IMPLEMENTATION_TASKLIST.md（先 Redis subscriber 暴露，再 Arango MVP）。
2. COLLAB_MESSAGE_PROTOCOL_DRAFT.md（消息协议与账本模型）。
