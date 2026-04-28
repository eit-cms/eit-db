# PR Report: 协作层回放增强与冒烟测试补强（2026-04-28）

## 1. 背景与目标

本次 PR 聚焦两件事：

1. 将协作层回放能力从“可追踪”推进到“可执行”（断点续跑 + 任意锚点跳转）。
2. 补齐用户可直接使用的文档与测试门禁（综合集成 + 冒烟测试）。

核心口径：

1. Arango 为默认增强面，承载回放会话、断点和时序关系。
2. Redis 承担实时消息与管理控制面，管理事件默认摘要化。
3. Arango 不可用时自动降级到 Redis-only，主链路不阻断。

## 2. 变更概览

### 2.1 运行时能力

1. 新增断点规划器：`CheckpointReplayPlanner`。
2. 新增断点续跑 API：`ResumeReplayFromCheckpoint`。
3. 新增任意锚点跳转 API：`JumpReplayToAnchor`。
4. 新增 Arango replay 查询能力：
   - `QueryReplaySessionMessages`
   - `QueryReplaySessionCheckpoints`
   - `QueryReplayMessagesFromCheckpoint`

### 2.2 测试能力

1. 新增综合 E2E 测试：`TestCollaborationReplayE2E`。
2. 新增冒烟测试集（`TestSmokeTest_` 前缀）共 11 条，覆盖连通性、运行时、管理面、消息面、回放面、降级路径。
3. 保留并通过既有回放/协作集成测试。

### 2.3 文档能力

1. 扩展 `docs/COLLAB_QUICK_START.md`：
   - 完整回放能力操作指南（增强回放、断点续跑、锚点跳转、降级）。
   - 冒烟测试与 E2E 命令。
2. 更新 `docs/COLLAB_CLOSEOUT_TASKLIST_vNEXT.md`：
   - 勾选综合回放集成测试与 smoke 测试项。
3. 更新 `README.md` 文档索引：
   - 增加协作层快速上手与本 PR 报告入口。

## 3. 用户可见改进

1. 用户可直接按文档执行回放增强能力，不再只停留在模型定义层。
2. 故障恢复路径支持两类执行入口：
   - 从 checkpoint 恢复。
   - 跳转到任意锚点后恢复。
3. 发布前可以一条命令执行协作层 smoke 测试，降低回归漏检风险。

## 4. 验证结果

### 4.1 定向验证

1. `go test -run "TestCollaborationReplayE2E" -v -timeout 90s`
   - 结果：PASS
2. `go test -run "TestSmokeTest_" -v -timeout 90s`
   - 结果：PASS（11/11）

### 4.2 全量回归

1. 根模块：`go test ./... -count=1`
   - 结果：PASS
2. 集成包：`adapter-application-tests` 下 `go test ./... -count=1`
   - 结果：PASS

## 5. 风险与边界

1. `JumpReplayToAnchor` 目前支持 `message_id/tick/time` 三种锚点类型，但底层序列定位主路径仍以消息序列为中心，复杂时间窗口语义后续可继续增强。
2. 管理事件默认摘要化，历史明细应通过 `replay_session_id` 回查 Arango，不建议在 Redis 事件中恢复全量 ID 列表。
3. smoke 目前是测试用例集，尚未抽成独立脚本（可在后续 D2 中补脚本化与诊断模板）。

## 6. 建议的合并后动作

1. 将 `TestSmokeTest_` 与 `TestCollaborationReplayE2E` 纳入默认 CI 门禁。
2. 为 `ResumeReplayFromCheckpoint`/`JumpReplayToAnchor` 增加对外服务层封装（如 HTTP 或 RPC 入口）。
3. 增加“失败分层诊断”模板（管理面/消息面/账本面）以配套 D2 门禁要求。

## 7. 当前进度快照（用于 PR 描述同步）

截至 2026-04-28，本轮协作层进度为：

1. 回放增强主路径已闭环：Arango 增强规划、断点续跑、锚点跳转、Redis-only 降级均可执行。
2. 测试门禁已补强：综合 E2E + 冒烟测试集可直接用于 PR 回归证据。
3. 用户文档已补齐：快速上手、回放操作路径、测试命令与风险边界均可对外说明。

可直接用于 PR 摘要的一段话：

`本次迭代完成了协作层回放能力的执行化落地（非仅模型化），并通过综合 E2E 与冒烟测试实现了可回归、可发布的最小门禁。当前能力支持 Arango 优先增强与 Redis-only 降级，满足链路可追踪与故障恢复双目标。`

## 8. 下一阶段计划：自动化管理适配器与按配置扩展集群

下一阶段目标是把协作模式从“手工管理节点”推进到“配置驱动的自动化节点编排”。

### 8.1 阶段目标

1. 自动化管理适配器生命周期（注册、心跳、下线、故障摘除、恢复重挂）。
2. 根据配置自动扩展适配器集群（按组、按能力、按副本数、按策略）。
3. 提供可观测与可审计的扩缩容轨迹（事件 + 账本双面追踪）。

### 8.2 最小交付清单（建议 Sprint-N）

1. `AdapterManager` 运行时：
   - 读取声明式配置生成目标拓扑。
   - 对比当前在线节点与目标副本，执行增减操作。
2. `ScalerPolicy`：
   - `fixed_replicas`（固定副本）
   - `threshold_based`（按 backlog/lag 阈值扩缩）
3. 配置模型扩展：
   - `collaboration.adapters[].replicas`
   - `collaboration.adapters[].capabilities`
   - `collaboration.scaling.*`
4. 事件与审计：
   - 发布 `adapter.scale.up/down` 管理事件。
   - Arango 记录扩缩容动作与原因字段（reason/metrics snapshot）。

### 8.3 验收标准

1. 改配置后无需重启业务即可完成副本收敛。
2. 在 backlog 持续超阈值时能自动扩容；恢复后按策略缩容。
3. 任意时刻可通过 `request_id` + 节点事件回溯扩缩容对路由与回放的影响。

### 8.4 风险与缓解

1. 抖动扩缩容风险：引入冷却时间（cooldown）与最小稳定窗口。
2. 脑裂与重复消费风险：强制 consumer group 归属策略 + 幂等防重。
3. 配置误操作风险：新增 dry-run 模式与变更审批事件。
