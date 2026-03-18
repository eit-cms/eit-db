# QueryConstructor 自动执行结果对照

该文档定义 `ExecuteQueryConstructorAuto` 在不同 Adapter 下的统一返回语义，作为 README 与 Adapter 文档的单一事实来源。

## 返回字段对照

| Adapter | `result.Mode` | 主要结果字段 | 说明 |
|---|---|---|---|
| SQL | `query` / `exec` | `Rows` / `Exec.RowsAffected` | `exec` 下可用 `Exec.LastInsertID`（驱动支持时） |
| Neo4j | `query` / `exec` | `Rows` / `Exec.Counters` | `Counters` 包含 nodes/relationships/properties/labels 统计 |
| MongoDB | `query` / `exec` | `Rows` / `Exec.Counters` | `exec` 支持 inserted/matched/modified/deleted 等统计；可选细节在 `Exec.Details` |

## MongoDB 可选细节字段

当 Mongo 写入计划启用 `ReturnInsertedID()` 或 `ReturnWriteDetail()` 时，`result.Exec.Details` 可能包含以下字段：

- `inserted_id`
- `inserted_ids`
- `inserted_count`
- `matched_count`
- `modified_count`
- `upserted_count`
- `upserted_id`
- `deleted_count`
