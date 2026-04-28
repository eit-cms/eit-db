# 协作层收尾任务清单（vNext 前置）

目标：在进入后端无关 API 改造前，把协作层从“可跑通”收敛到“可稳定上线”。

协作模式口径（vNext）：

1. ArangoDB 作为协作层默认增强面（默认启动）。
2. Redis 继续承担实时消息与控制面执行真相。
3. 若 ArangoDB 不可用，则降级为 fallback 模式，不阻断主链路执行，但追踪能力降级。

更新时间：2026-04-28
状态：Ready for Execution

## 1. 执行顺序

1. 先完成运行时接线与生命周期自动化。
2. 再补齐故障恢复闭环（含回放与观测）。
3. 最后完成调度最小闭环与发布门禁。

## 2. Closeout-A 运行时接线与生命周期

### A1. 管理路径自动接入

- [x] 默认协作适配器启动时自动执行节点注册。
- [x] 周期心跳与优雅下线自动接线。
- [ ] managed 与 explicit 适配器行为一致性门禁接入启动流程。（下一迭代）

验收标准：

1. 无需业务侧手写注册/心跳代码即可获得在线节点视图。
2. 节点进程退出后，离线状态在 2 个心跳周期内可见。

### A2. 协作运行时启动器

- [x] 提供统一协作运行时启动入口（CollaborationRuntime.Start/Stop）。
- [x] 提供健康检查与启动报告输出（CollaborationRuntimeReport）。
- [x] 提供最小可用默认参数与超时策略（CollaborationRuntimeConfig.withDefaults）。

验收标准：

1. 应用侧仅需一次启动调用即可完成协作层初始化。
2. 启动报告包含 namespace、group、node、stream 等关键字段。

## 3. Closeout-B 故障恢复闭环

### B1. Dead Letter 回放

- [x] 增加 DLQ 回放入口（RedisStreamFeatures.ReplayFromDLQ，支持 request_id 过滤）。
- [x] 支持幂等回放（RetryCount 重置为 0，IdempotencyKey 保留）。
- [x] Arango 可用时默认启用"账本辅助回放规划"（ArangoReplayPlanner，按 message_node 关系筛选回放候选）；不可用时自动 fallback 到 RedisOnlyReplayPlanner。
- [x] ReplayFromDLQWithPlanner：Arango 增强模式 + redis_only 降级模式自动切换，PlannedBy 字段记录来源。
- [x] RecordReplayResultToLedger：回放成功后批量更新 Arango message_node.status=replayed。
- [x] Arango 回放会话账本：新增 replay_session_node / replay_checkpoint_node，以及 session_replays_message / session_has_checkpoint 关系。
- [x] Redis 管理事件瘦身：回放事件仅发送摘要 + replay_session_id，避免 Redis 承担大规模明细数据。
- [x] Arango replay 查询接口：支持按 session/checkpoint 提取回放序列（为断点续跑与任意锚点跳转执行层提供入口）。
- [x] 断点续跑执行接口：`ResumeReplayFromCheckpoint`（CheckpointReplayPlanner + 现有 tracking 流程打通）。
- [x] 任意锚点跳转接口：`JumpReplayToAnchor`（按 message_id/tick/time 锚点动态创建 checkpoint 并触发回放）。
- [x] ReplayStatus 枚举：candidate / planned / replayed / failed / skipped / audited（与状态机文档对齐）。
- [x] 回放事件同步写入 Redis 管理事件通道（dlq.replay.published 事件）。

验收标准：

1. 指定 request_id 的 DLQ 消息可回放并被消费确认。
2. 回放行为可在 Redis 管理事件与 Arango 账本中追踪。

### B2. Backlog 与 Lag 治理

- [ ] 定义 lag/pending 阈值与告警级别。
- [ ] 增加积压清理策略（重试、转移、丢弃需显式策略）。
- [ ] 输出统一协作指标快照结构。

验收标准：

1. 在压力场景下可稳定输出 backlog 与 lag 快照。
2. 出现积压时可自动或手动触发治理动作并可审计。

## 4. Closeout-C 调度最小闭环（Blueprint）

### C1. Tick 与选路稳定性

- [x] 在协作消息信封中增加 blueprint_tick、route_tick、selected_node_id、scheduler_policy 字段。
- [x] 实现同类型多实例 HRW 稳定选路（HRWSelectNode / HRWRouteToOnlineNodeWithFallback）。
- [x] 记录 selected_node_id 字段供账本写入与回放验证使用。

验收标准：

1. 同 request_key 在 route_tick 不变时路由稳定率不低于 99.9%。
2. 单节点下线后可在 2 倍心跳周期内重路由成功。

### C2. 账本可追踪性

- [ ] request 到 message 到 node 的全链路字段补齐。
- [ ] remap 事件写入 request 维度轨迹。
- [x] 增加链路回放查询模板。

验收标准：

1. 任意 request_id 可完整还原投递与重路由路径。
2. 审计查询无需跨多处手工拼接。

## 5. Closeout-D 测试与发布门禁

### D1. 集成测试包

- [x] 增加生命周期自动接线集成测试（TestCollaborationRuntimeLifecycle_StartStop）。
- [x] 增加 DLQ 回放集成测试（TestCollaborationDLQReplay_Integration）。
- [x] 增加 HRW 稳定选路与节点故障接管集成测试（TestCollaborationHRWStableRouting_Integration）。
- [x] 增加完整回放能力综合集成测试（TestCollaborationReplayE2E，覆盖 Arango 增强/断点续跑/锚点跳转/Redis-only 降级）。

### D2. 发布门禁

- [ ] 将协作收尾关键用例纳入默认 integration 套件。
- [x] 增加协作层 smoke 测试用例集（TestSmokeTest_ 前缀）与基础失败定位字段说明。

验收标准：

1. 主分支默认集成测试可覆盖协作关键路径。
2. 门禁失败时可快速定位是管理面、消息面还是账本面问题。

## 6. 进入 API 改造的 Gate

全部满足后进入新 API 与迁移阶段：

1. 生命周期自动接线完成并稳定。
2. DLQ 回放与积压治理闭环可用。
3. Blueprint 最小调度闭环通过回归。
4. 协作层发布门禁稳定运行 1 个迭代周期。

## 7. Next Stage（协作自动化管理）

目标：从“功能闭环”升级到“配置驱动的自动化节点编排与集群扩缩容”。

### E1. 自动化管理适配器生命周期

- [ ] 新增 AdapterManager：统一管理 register / heartbeat / offline / recover。
- [ ] 支持 managed 与 explicit 适配器一致性治理（状态机统一）。
- [ ] 引入故障摘除与自动恢复重挂策略。

验收标准：

1. 节点生命周期不再依赖业务代码手动维护。
2. 节点故障后可在策略窗口内完成摘除与恢复。

### E2. 按配置自动扩展适配器集群

- [ ] 在配置中声明适配器副本目标（replicas）与能力标签（capabilities）。
- [ ] 基于 backlog/lag 阈值实现自动 scale up/down。
- [ ] 支持 cooldown、最小副本、最大副本、dry-run 策略。

验收标准：

1. 调整配置后集群副本可自动收敛到目标值。
2. 压力升高时自动扩容，压力回落后按策略缩容。

### E3. 扩缩容可观测与审计

- [ ] 发布 `adapter.scale.up` / `adapter.scale.down` 管理事件。
- [ ] Arango 账本记录扩缩容动作、触发原因与指标快照。
- [ ] 提供“扩缩容影响回放与路由”的排障查询模板。

验收标准：

1. 任意扩缩容动作均可追踪触发原因与执行结果。
2. 发布门禁失败时可快速判断问题属于容量策略、路由策略或执行策略。
