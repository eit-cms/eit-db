# EIT-DB v2.0 Roadmap

**文档版本**：1.0  
**发布日期**：2026-03-18  
**周期范围**：v2.0.0-alpha ~ v2.2.0  
**预计完成时间**：24 周（6 个月）

---

## 产品愿景

在 v1.0 基础上，实现**多数据库智能协调体系**，通过 Blueprint + Adapter 监控树 + 动态链接，提供：
1. 跨库查询的可靠性与性能保证
2. Adapter 能力的自适应调度与故障自愈
3. 计划缓存与热点优化
4. 生产级别的可观测性与审计

---

## 版本里程碑

### v2.0.0-alpha（8 周，第 1-2 个月）
**目标**：验证核心基础设施可行性  
**发布形式**：内部验证构建

#### α1 Phase（第 1-2 周）
**主题**：Adapter 监控树 MVP

- [ ] 定义监控树数据结构（TopologySnapshot + CapabilityFingerprint）
- [ ] 实现 MonitorActor（心跳探测与能力指纹收集）
- [ ] Redis 存储监控树快照
- [ ] 编写单元测试与集成测试
- [ ] 达成 Adapter 故障检测延迟 < 60s

**交付物**：
- `pkg/monitor/topology.go` - 监控树核心数据结构
- `pkg/actor/monitor_actor.go` - 心跳与能力探测逻辑
- `tests/integration/monitor_tree_test.go`

#### α2 Phase（第 3-4 周）
**主题**：Actor 框架搭建与 Symbol Linker 原型

- [ ] 搭建 AppHost + Actor Supervisor 基础架构（原生 Go 协程 + Channel）
- [ ] 实现 LinkerActor：Link Table 维护与符号绑定逻辑
- [ ] 实现符号表编译（从 Blueprint DSL 到符号表 JSON）
- [ ] Symbol Linker 与监控树事件绑定
- [ ] 端到端测试：2 个 SQL Adapter + 简单跨库查询

**交付物**：
- `pkg/apphost/apphost.go` - AppHost 核心控制面
- `pkg/actor/actor_supervisor.go` - Actor 生命周期管理
- `pkg/linker/linker_actor.go` - 符号链接与路由
- `pkg/blueprint/symbol_table.go` - 符号表结构与编译

#### α3 Phase（第 5-6 周）
**主题**：Blueprint 双层模型与 ArangoDB 集成

- [ ] 定义 Blueprint 字段集（MVP：Entity + Relationship + Query + Policy）
- [ ] 实现 ArangoDB Control Plane 存储（顶点/边集合）
- [ ] Blueprint 加载、验证、激活流程
- [ ] 热加载机制与版本管理
- [ ] 编写 Blueprint 样例与验证工具

**交付物**：
- `pkg/blueprint/blueprint.go` - Blueprint 数据结构
- `pkg/blueprint/arango_store.go` - ArangoDB 存储与查询
- `pkg/blueprint/loader.go` - Blueprint 加载与验证
- `examples/blueprint_*.json` - 示例 Blueprint

#### α4 Phase（第 7-8 周）
**主题**：测试与文档

- [ ] 监控树拓扑变化时的事件流测试
- [ ] Linker 重新绑定正确性验证
- [ ] CacheActor 失效协调测试
- [ ] 性能基准测试（规划缓存命中率、符号绑定延迟）
- [ ] 编写 v2.0-alpha 设计文档与 API spec

**交付物**：
- `tests/e2e/topology_change_test.go`
- `tests/benchmark/linker_binding_bench.go`
- `docs/V2_0_API_SPEC.md`

---

### v2.0.0-beta（8 周，第 3-4 个月）
**目标**：完整功能集 + 生产就绪度提升  
**发布形式**：可用于内部集成测试

#### β1 Phase（第 9-10 周）
**主题**：Blueprint DSL 与编译流水线

- [ ] 设计 Blueprint DSL（includes、where/or、when 子句）
- [ ] 实现 DSL Parser（用 Go 现成库如 `goyacc` 或直接递归下降）
- [ ] DSL → 符号表编译器
- [ ] 条件编译：特性强度匹配、优先级解析
- [ ] CLI 子命令：`eit-db compile --blueprint <file>`

**交付物**：
- `pkg/blueprint/dsl/parser.go` - DSL 词法分析与语法分析
- `pkg/blueprint/dsl/compiler.go` - DSL 到符号表编译
- `cmd/eit-migrate/commands.go` - 新增 `compile` 命令

#### β2 Phase（第 11-12 周）
**主题**：计划缓存与 CacheActor

- [ ] 定义计划缓存键（blueprint_version + symbol_ids 指纹）
- [ ] 实现 CacheActor：缓存存储、命中统计、失效协调
- [ ] LinkingUpdated 事件触发精确失效
- [ ] 缓存预热策略（启动时加载热点计划）
- [ ] 性能监测：缓存命中率、失效延迟

**交付物**：
- `pkg/actor/cache_actor.go` - 计划缓存与失效协调
- `pkg/cache/plan_cache.go` - 缓存存储与键生成
- `tests/integration/cache_invalidation_test.go`

#### β3 Phase（第 13-14 周）
**主题**：CoordinatorActor 与跨库编排

- [ ] 实现 CoordinatorActor：查询规划、Adapter 选择、结果汇总
- [ ] 与 Planner 集成（使用已有 IR 编程）
- [ ] Link Table 落地与查询路由
- [ ] 一致性等级支持（eventual / strong / read-after-write）
- [ ] 故障转移逻辑（自动 fallback 到次选 Adapter）

**交付物**：
- `pkg/actor/coordinator_actor.go` - 跨库编排逻辑
- `pkg/router/link_table.go` - Link Table 查询与路由
- `pkg/consistency/policy.go` - 一致性等级与执行策略

#### β4 Phase（第 15-16 周）
**主题**：可观测性 + 自愈机制

- [ ] 集成 Jaeger / Prometheus（或兼容系统）
- [ ] Linker Actor 自愈决策（detect → mark → migrate → restart）
- [ ] 审计日志（谁、何时、做什么、结果）
- [ ] 告警规则（能力降级、缓存失效风暴、计划编译错误）
- [ ] 故障演练与恢复文档

**交付物**：
- `pkg/observability/tracing.go` - Jaeger 集成
- `pkg/observability/metrics.go` - Prometheus 指标
- `pkg/observability/audit.go` - 审计日志
- `docs/OPERATIONAL_GUIDE.md` - 运维文档

---

### v2.0.0（4 周，第 5 个月）
**目标**：生产就绪（GA）  
**发布形式**：稳定发布

#### GA Phase（第 17-20 周）
**主题**：最后冲刺与稳定性

- [ ] 回归测试：所有 v1.0 功能继续可用
- [ ] 性能测试：跨库查询延迟 < 500ms、故障转移 < 1min
- [ ] 生产环境压测（单机 10K tps 以上）
- [ ] 安全审计（权限、审计、数据隔离）
- [ ] Release Notes、Migration Guide v2.0、API 文档终稿

**关键约束**：
- 所有已知 bug 必须修复（P0/P1）
- 代码覆盖率 ≥ 75%
- 文档完整性评分 ≥ 90%

**交付物**：
- `v2.0.0` 标签与二进制发布
- `docs/RELEASE_NOTES_v2_0.md`
- `docs/MIGRATION_GUIDE_v2_0.md`
- 官方容器镜像 & Helm Chart

---

### v2.1.0（6 周，第 5-6 个月，与 GA 并行准备）
**目标**：ArangoDB 专项与高级特性  
**发布形式**：功能发布，兼容 GA

#### Arc（第 21-26 周，v2.0 发布后立即启动）
**主题**：ArangoDB 深度集成

- [ ] Adapter Indexing Strategy（利用 ArangoDB 图索引优化查询规划）
- [ ] 图查询原生支持（跨库关系查询 via ArangoDB Traversal）
- [ ] 元数据预加载与缓存同步
- [ ] Adapter 推送条件谓词到 ArangoDB（查询下推）
- [ ] OLAP 模式支持（大规模分析型跨库查询）

**新增特性**：
- [ ] 查询模板与自动参数化
- [ ] 跨库事务支持（二阶段提交框架）
- [ ] 增量同步（CDC 适配）

**交付物**：
- `pkg/arango/indexing_strategy.go`
- `pkg/arango/graph_traversal.go`
- `docs/ARANGO_ADVANCED_GUIDE.md`

---

### v2.2.0（6 周，第 6 个月 +）
**目标**：分布式与集群支持  
**发布形式**：企业级功能包

#### Cluster（部署模型扩展）
**主题**：分布式 EIT-DB 编排

- [ ] Multi-Instance Coordination（多个 EIT-DB 实例间的 AppHost 协调）
- [ ] Service Discovery（etcd / Consul 集成）
- [ ] Distributed Actor（NATS / Kafka 事件总线升级）
- [ ] Global Query Optimizer（跨多个 EIT-DB 实例的规划优化）
- [ ] Load Balancing（查询分发与结果缓存一致性）
- [ ] Failover 与升级策略（蓝绿部署、灰度更新）

**新增特性**：
- [ ] 跨集群 Adapter 分析与推荐
- [ ] 全局熔断与限流
- [ ] 分布式计划缓存（Redis Cluster / Memcached）

**交付物**：
- `pkg/cluster/coordinator.go`
- `pkg/cluster/service_discovery.go`
- `docs/CLUSTER_DEPLOYMENT.md`
- Kubernetes Operator（可选）

---

## 版本发布时间表

| 版本 | 计划开始 | 计划完成 | 状态 | 对标日期 |
|------|---------|---------|------|---------|
| v2.0.0-alpha | 2026-03-19 | 2026-05-14 | 📋 计划中 | 第 8 周 |
| v2.0.0-beta | 2026-05-15 | 2026-07-09 | 📋 待启动 | 第 16 周 |
| v2.0.0 GA | 2026-07-10 | 2026-07-31 | 📋 待启动 | 第 20 周 |
| v2.1.0 | 2026-07-31 | 2026-09-11 | 📋 待启动 | 第 26 周 |
| v2.2.0 | 2026-09-12 | 2026-10-23 | 📋 待启动 | 第 32 周 |

---

## 每个阶段的成功标准

### v2.0.0-alpha 验收标准
- [ ] MonitorActor 能准确检测 Adapter 故障（> 95% 准确率）
- [ ] Symbol Linker 在 100ms 内完成绑定更新
- [ ] Blueprint 可通过 ArangoDB 持久化与加载
- [ ] 无内存泄漏（5 天运行测试）
- [ ] 文档完整（API spec、设计决策、故障排查）

### v2.0.0-beta 验收标准
- [ ] DSL 编译器支持所有语法特性且正确率 100%
- [ ] 计划缓存命中率 > 80%（在典型工作负载下）
- [ ] 跨库查询延迟 < 500ms（99%(百分位数）
- [ ] 能力降级自动恢复（用时 < 2 分钟）
- [ ] 代码覆盖率 ≥ 75%

### v2.0.0 GA 验收标准
- [ ] 生产压测：单机 10K+ tps，延迟 p99 < 800ms
- [ ] 故障演练成功：Adapter 宕机自动转移，无查询丢失
- [ ] 与 v1.0 完全向后兼容（可平滑升级）
- [ ] 安全审计通过
- [ ] 用户文档完整并经过 2 轮审查

---

## 风险与缓解策略

| 风险 | 影响 | 概率 | 缓解计划 |
|------|------|------|---------|
| ArangoDB 性能不达预期 | 元数据查询延迟高 | 中 | α2 时进行压测，若不达预期考虑本地缓存 |
| Actor 框架选择失误 | 需要重构 | 低 | 在 α1 结束前进行架构评审 |
| 跨库事务需求超出预期 | v2.1 无法按时交付 | 中 | 优先级后移到 v2.2，v2.1 专注 ArangoDB |
| 监控树事件风暴 | 系统可靠性下降 | 低 | 实现事件去重、批量失效、限流机制 |
| DSL 表达能力不足 | 需要版本 2 设计 | 低 | 在 β1 进行试点，根据反馈迭代 |

---

## 团队分工建议

### 核心团队（建议 3-4 人）

**角色 1：Infrastructure Lead**
- 负责：AppHost、Actor 框架、Event Bus、监控树
- 时间投入：100%，v2.0.0 期间全职

**角色 2：Blueprint & Compiler Lead**
- 负责：Blueprint DSL、编译流水线、Symbol Linker
- 时间投入：100%，从 β1 开始全职

**角色 3：Query & Optimization Lead**
- 负责：Planner 集成、CoordinatorActor、计划缓存、性能优化
- 时间投入：100%，从 α2 开始全职

**角色 4：QA Lead（可选）**
- 负责：集成测试、性能测试、压力测试、生产验证
- 时间投入：80%，从 α3 开始

---

## 依赖关系与并行机会

```
α1 (监控树)
  ├─→ α2 (Actor 框架 + LinkerActor) ← 并行准备 β1
  │    ├─→ α3 (Blueprint DSL)
  │    │    ├─→ β1 (DSL Parser & Compiler)
  │    │    └─→ β2 (计划缓存)
  │    │         ├─→ β3 (CoordinatorActor)
  │    │         └─→ GA (稳定性冲刺)
  │
  ├─→ α4 (文档与测试)
  │
  └─→ 性能基准建立（贯穿全程）

v2.1.0: ArangoDB 深度集成（与 GA 并行准备，完全独立）
v2.2.0: 分布式支持（基于 v2.0 GA 的扩展）
```

---

## 检查点与回顾

- **α1 结束检查**（第 2 周）：监控树数据结构定义 & MonitorActor MVP
- **α2 结束检查**（第 4 周）：Actor 框架 & Linker 原型运行
- **α 完成评审**（第 8 周）：整体设计评审（架构、设计决策、风险）
- **β1 完成评审**（第 10 周）：DSL 设计评审与用例验证
- **β 完成评审**（第 16 周）：功能完整性评审与性能基准验收
- **GA 发布评审**（第 20 周）：生产就绪评审

---

## 后续计划（v2.1+ 路线）

- **v2.3.0**：分布式事务支持（3PL）
- **v2.4.0**：机器学习 Adapter 选择优化
- **v3.0.0**：多租户隔离与企业级治理

---

该 Roadmap 为动态文档，将根据进度和反馈每两周更新一次。
