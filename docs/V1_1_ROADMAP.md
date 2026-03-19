# EIT-DB v1.1 Roadmap

**文档版本**：1.0  
**发布日期**：2026-03-19  
**目标版本**：v1.1.0

---

## 背景与问题定义

v1.0.x 已具备 SQL 多方言迁移能力，但迁移入口仍存在分叉：

1. 迁移工具中同时存在 Schema 驱动路径与手写 SQL 路径。
2. 部分迁移元数据写入逻辑绕过统一方言编译，易引入占位符与语法不一致问题。
3. MongoDB / Neo4j 尚未进入统一迁移框架，迁移能力与 SQL 适配器不对齐。

v1.1 的核心目标是将迁移工具升级为“Schema/IR 驱动 + Adapter 编译执行”的统一架构。

---

## v1.1 版本目标

### Goal 1: 统一迁移入口（Schema -> Migration IR -> Adapter Compiler）

- [ ] 所有框架内迁移操作（包括 `schema_migrations` 维护）统一走 Migration IR。
- [ ] Migration Runner 不再直接拼接方言 SQL 片段。
- [ ] 迁移占位符、默认值、标识符转义全部由方言编译器负责。

### Goal 2: SQL / NoSQL 迁移执行模型统一

- [ ] 定义统一迁移操作集：CreateEntity/DropEntity/AddField/AlterField/AddIndex/DropIndex 等。
- [ ] SQL Adapter 将操作编译为 SQL 命令计划。
- [ ] MongoDB Adapter 将操作编译为 Collection/Index/Validator 命令计划。
- [ ] Neo4j Adapter 将操作编译为 Label/Constraint/Index/Cypher 命令计划。

### Goal 3: 保持兼容并收敛风格

- [ ] 保留 `RawSQLMigration` 作为 escape hatch，但标记为高级入口。
- [ ] CLI 默认模板改为 Schema/IR 风格。
- [ ] v1.0.x 迁移文件可继续运行，不强制一次性改写。

---

## 范围定义

### In Scope

1. Migration IR 抽象与编译执行框架。
2. SQL 四适配器（MySQL/PostgreSQL/SQLite/SQLServer）迁移路径统一。
3. MongoDB / Neo4j 的“最小可用迁移能力”接入。
4. 迁移日志记录统一纳入 IR。

### Out of Scope

1. 自动 schema diff 生成器（另开 v1.2 议题）。
2. 大规模数据回填编排系统（保留为手工迁移或独立任务系统）。
3. 跨数据库分布式事务迁移（非 v1.1 目标）。

---

## 里程碑计划（建议 8 周）

### M1（第 1-2 周）：IR 基础设施

- [ ] 定义 Migration IR 数据结构与接口。
- [ ] 引入 Command Plan 执行器。
- [ ] 将 `schema_migrations` 的记录/删除流程迁移到 IR。

### M2（第 3-4 周）：SQL 路径收敛

- [ ] v1/v2 迁移器的 SQL 拼接路径迁移到统一编译器。
- [ ] 补齐 SQL 方言占位符与默认值回归测试。
- [ ] 对 `RawSQLMigration` 增加使用边界说明与告警。

### M3（第 5-6 周）：MongoDB / Neo4j 接入

- [ ] MongoDB: CreateCollection/CreateIndex/DropCollection 最小能力上线。
- [ ] Neo4j: Label/Constraint/Index 最小能力上线。
- [ ] 统一迁移状态记录与错误语义。

### M4（第 7-8 周）：稳定性与发布

- [ ] 集成测试矩阵纳入 SQL + MongoDB + Neo4j。
- [ ] 发布 `MIGRATION_GUIDE_v1_1.md`。
- [ ] 发布 `RELEASE_NOTES_v1_1.md` 与升级说明。

---

## 验收标准（Definition of Done）

1. 迁移框架主路径不再出现硬编码方言占位符（例如直接写 `?`）。
2. SQL 适配器迁移测试覆盖默认值、占位符、约束、回滚。
3. MongoDB / Neo4j 至少各有一套可执行迁移集成测试（创建 + 索引 + 回滚）。
4. CLI 新生成迁移文件默认使用 Schema/IR 风格模板。
5. v1.0.x 既有迁移文件可无破坏运行。

---

## 风险与缓解

1. **风险**：旧迁移器与新迁移器并行导致行为分歧。  
   **缓解**：新增兼容层测试，所有迁移路径共用同一编译器内核。

2. **风险**：NoSQL 语义与 SQL 语义映射不完全对称。  
   **缓解**：先定义“最小公共操作集”，高级能力通过 adapter-specific extension 暴露。

3. **风险**：发布前回归范围扩大。  
   **缓解**：按里程碑逐步扩展测试矩阵，先锁定框架内表迁移和核心 DDL 再扩面。

---

## 与 v2.0 的关系

v1.1 是 v2.0 之前的工程收敛版本，目标是把迁移能力从“方言 patch 集合”升级为“可扩展编译框架”，为后续多数据库智能编排打稳定底座。