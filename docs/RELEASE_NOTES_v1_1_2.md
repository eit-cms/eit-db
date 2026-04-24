# EIT-DB v1.1.2 Release Notes

> 发布日期：2026-04-22  
> 类型：Patch（定时任务增强与回退可观测性升级）

---

## 概述

v1.1.2 聚焦“定时任务能力增强”。

相较于 v1.1.1 的“可回退可用”目标，本版本进一步补齐了：

1. SQL 适配器原生调度能力路径。
2. 跨适配器统一 fallback 错误语义与 reason 可观测性。
3. 文档层面的完成度报告与接入指南。

该版本不引入破坏性变更，保持与 v1.1.1 向后兼容。

---

## 主要变更

### 1) SQL 适配器原生调度路径完善

- PostgreSQL：
  - 定时任务元数据持久化增强。
  - pg_cron 可用时走原生调度，不可用时可由 Repository fallback。
  - 任务列表包含更丰富运行状态信息。

- SQL Server：
  - 基于 SQL Server Agent 的注册/注销/列表完整路径。
  - 增加 Agent 可用性探测和 cron 形态限制校验。

- MySQL：
  - 基于 EVENT 的注册/注销/列表完整路径。
  - 增加 EVENT scheduler 可用性探测。

### 2) fallback 语义标准化与可观测

新增统一的 fallback 语义 API：

- `ErrScheduledTaskFallbackRequired`
- `NewScheduledTaskFallbackErrorWithReason(adapter, reason, detail)`
- `IsScheduledTaskFallbackError(err)`
- `ScheduledTaskFallbackReasonOf(err)`

reason 枚举：

- `adapter_unsupported`
- `native_capability_missing`
- `cron_expression_unsupported`
- `unknown`

这使应用层可按 reason 做监控统计、告警分类与策略分流。

### 3) Repository fallback 接管一致性

Repository 在以下路径统一 fallback 接管语义：

- `RegisterScheduledTask`
- `UnregisterScheduledTask`
- `ListScheduledTasks`

当 adapter 返回 fallback 语义错误且开关开启时，行为保持一致。

### 4) 文档增强

README 新增并整理：

- 定时任务能力补充章节。
- 完成度矩阵。
- 接入示例（注册/列表/注销）。
- fallback 开关与 reason 观测指南。

---

## 对应用层的价值

- 原生优先：可用原生调度器时获得更强运行一致性。
- 回退稳定：无原生能力时仍可使用应用层调度器保障可用下限。
- 观测清晰：通过 reason 精准区分“能力缺失”与“表达式不兼容”。

---

## 兼容性与升级

- 兼容性：与 v1.1.1 向后兼容。
- 建议升级：所有 v1.1.1 用户可直接升级到 v1.1.2。

```bash
go get github.com/eit-cms/eit-db@v1.1.2
```

---

## 验证建议

发布前建议至少执行：

```bash
go test ./... -run 'ScheduledTask|DynamicTable'
```

如需候选发布门禁，参考：

- `docs/RELEASE_GATE.md`
