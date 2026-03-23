# EIT-DB v1.1.1

发布日期：2026-03-23  
发布类型：Patch

## 中文说明

v1.1.1 是一个以“优化可用性 + 强化 NoSQL 特色能力复用”为目标的小版本。

### 核心改进

1. 定时任务能力覆盖优化
- 当某些 adapter 不支持原生定时任务时，Repository 层可自动回退到统一调度器。
- 回退默认开启，支持显式关闭。
- 目标是让跨数据库场景下的任务能力具备稳定可用下限。

2. MongoDB 特色能力增强
- 日志分析：热词统计、规则分词、按级别统计、按时间窗口统计。
- 文档工作流：草稿管理、查询计划、模板预设库、模板渲染与安全策略。
- 应用层可以直接调用这些能力，减少重复实现。

3. Neo4j 社交/聊天模型增强
- 新增或增强一对一聊天、群聊房间、已读回执、禁言/封禁治理、消息全文检索。
- Emoji 建模升级为静态节点复用，并使用 INCLUDED_BY(index) 与消息占位符绑定。

4. 文档与测试完善
- README 与适配器文档同步更新。
- 回归测试通过，保持与 v1.1.0 的向后兼容。

### 升级

go get github.com/eit-cms/eit-db@v1.1.1

---

## English Notes

v1.1.1 is a patch release focused on usability improvements and reusable NoSQL-native capabilities.

### Highlights

1. Scheduled task coverage improvement
- When an adapter does not support native scheduling, Repository can automatically fall back to a unified in-process scheduler.
- Fallback is enabled by default and can be explicitly disabled.
- This provides a stable baseline for cross-database task execution.

2. MongoDB capability enhancements
- Log analytics: hot words, rule-based tokenization, by-level and by-time-window analysis.
- Document workflow: draft lifecycle, query plan output, template preset library, and secure template rendering.
- These features can be consumed directly by application services to reduce repetitive glue code.

3. Neo4j social/chat model enhancements
- Added or enhanced one-to-one chat, group chat rooms, read receipts, moderation (mute/ban), and message fulltext search templates.
- Emoji modeling updated to reusable static nodes with INCLUDED_BY(index) placeholder binding.

4. Docs and tests
- README and adapter docs are updated in sync.
- Regression tests pass with backward compatibility from v1.1.0.

### Upgrade

go get github.com/eit-cms/eit-db@v1.1.1
