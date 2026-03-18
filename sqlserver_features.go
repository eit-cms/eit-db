package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ==================== SQL Server 特色功能入口 ====================

// SQLServerFeatures 提供 SQL Server 特有的高级数据库功能。
// 通过 GetSQLServerFeatures(adapter) 获取实例，非 SQL Server 返回 false。
//
// 示例：
//
//	features, ok := db.GetSQLServerFeatures(repo.GetAdapter())
//	if !ok {
//	    return errors.New("not SQL Server")
//	}
//	rows, err := features.RecursiveQuery("org_tree").
//	    Anchor("SELECT id, name, parent_id, 0 AS depth FROM departments WHERE parent_id IS NULL").
//	    Recursive("SELECT d.id, d.name, d.parent_id, t.depth+1 FROM departments d JOIN org_tree t ON d.parent_id = t.id").
//	    SelectAll().
//	    MaxRecursion(50).
//	    Execute(ctx)
type SQLServerFeatures struct {
	adapter *SQLServerAdapter
}

// GetSQLServerFeatures 从 Adapter 中提取 SQLServerFeatures。
// 若传入的不是 *SQLServerAdapter，则 ok == false。
func GetSQLServerFeatures(adapter Adapter) (*SQLServerFeatures, bool) {
	ss, ok := adapter.(*SQLServerAdapter)
	if !ok {
		return nil, false
	}
	return &SQLServerFeatures{adapter: ss}, true
}

// RecursiveQuery 开始构建一个递归 CTE 查询（WITH ... UNION ALL ...）。
// cteName 是 CTE 的名称，在 T-SQL 中作为临时视图引用。
func (f *SQLServerFeatures) RecursiveQuery(cteName string) *RecursiveQueryBuilder {
	return &RecursiveQueryBuilder{
		adapter:      f.adapter,
		cteName:      cteName,
		maxRecursion: 100, // SQL Server 默认递归上限
	}
}

// MergeInto 开始构建 SQL Server MERGE 语句（常用于 Upsert / 同步）。
//
// 示例：
//
//	err := features.MergeInto("dbo.users").
//		Using("(SELECT 1 AS id, N'Alice' AS name) AS s").
//		On("t.[id] = s.[id]").
//		WhenMatchedUpdate("t.[name] = s.[name]").
//		WhenNotMatchedInsert([]string{"id", "name"}, "s.[id], s.[name]").
//		Execute(ctx)
func (f *SQLServerFeatures) MergeInto(targetTable string) *SQLServerMergeBuilder {
	return &SQLServerMergeBuilder{
		adapter:      f.adapter,
		targetTable:  strings.TrimSpace(targetTable),
		targetAlias:  "t",
		sourceAlias:  "s",
		hintHoldLock: true,
	}
}

// TempTable 开始构建 SQL Server 临时表（默认局部临时表 #name）。
//
// 对跨语句/跨连接场景，建议使用 Global() 生成全局临时表 ##name。
func (f *SQLServerFeatures) TempTable(name string) *SQLServerTempTableBuilder {
	return &SQLServerTempTableBuilder{
		adapter:      f.adapter,
		name:         strings.TrimSpace(name),
		dropIfExists: true,
	}
}

// View 开始构建 SQL Server 视图语句。
// 默认使用 CREATE OR ALTER VIEW，便于幂等更新。
func (f *SQLServerFeatures) View(name string) *SQLServerViewBuilder {
	return &SQLServerViewBuilder{
		adapter:       f.adapter,
		name:          strings.TrimSpace(name),
		createOrAlter: true,
		dropIfExists:  true,
	}
}

// ==================== SQLServerMergeBuilder ====================

// SQLServerMergeBuilder 构建 SQL Server MERGE 语句。
type SQLServerMergeBuilder struct {
	adapter *SQLServerAdapter

	targetTable string
	targetAlias string

	sourceSQL   string
	sourceAlias string

	onClause string

	whenMatchedUpdateSQL      string
	whenNotMatchedInsertCols  []string
	whenNotMatchedInsertValue string
	whenNotMatchedBySrcDelete bool

	outputSQL string

	hintHoldLock bool
	args         []interface{}
}

// TargetAlias 设置目标表别名（默认 t）。
func (b *SQLServerMergeBuilder) TargetAlias(alias string) *SQLServerMergeBuilder {
	b.targetAlias = strings.TrimSpace(alias)
	return b
}

// Using 设置 MERGE USING 源（通常是子查询 + 别名）。
// 例如："(SELECT ... ) AS s"。
func (b *SQLServerMergeBuilder) Using(sourceSQL string) *SQLServerMergeBuilder {
	b.sourceSQL = strings.TrimSpace(sourceSQL)
	return b
}

// SourceAlias 设置源别名（默认 s）。
func (b *SQLServerMergeBuilder) SourceAlias(alias string) *SQLServerMergeBuilder {
	b.sourceAlias = strings.TrimSpace(alias)
	return b
}

// On 设置目标与源的匹配条件。
func (b *SQLServerMergeBuilder) On(condition string) *SQLServerMergeBuilder {
	b.onClause = strings.TrimSpace(condition)
	return b
}

// WhenMatchedUpdate 设置匹配后 UPDATE 子句（仅 SET 部分）。
// 例如："t.[name] = s.[name], t.[updated_at] = SYSUTCDATETIME()"。
func (b *SQLServerMergeBuilder) WhenMatchedUpdate(setClause string) *SQLServerMergeBuilder {
	b.whenMatchedUpdateSQL = strings.TrimSpace(setClause)
	return b
}

// WhenNotMatchedInsert 设置未匹配时 INSERT 子句。
// columns 是目标列列表，valuesExpr 是 VALUES(...) 内表达式。
func (b *SQLServerMergeBuilder) WhenNotMatchedInsert(columns []string, valuesExpr string) *SQLServerMergeBuilder {
	b.whenNotMatchedInsertCols = columns
	b.whenNotMatchedInsertValue = strings.TrimSpace(valuesExpr)
	return b
}

// WhenNotMatchedBySourceDelete 设置源不存在时删除目标数据。
func (b *SQLServerMergeBuilder) WhenNotMatchedBySourceDelete() *SQLServerMergeBuilder {
	b.whenNotMatchedBySrcDelete = true
	return b
}

// Output 设置 OUTPUT 子句（例如 "$action, inserted.id"）。
func (b *SQLServerMergeBuilder) Output(outputExpr string) *SQLServerMergeBuilder {
	b.outputSQL = strings.TrimSpace(outputExpr)
	return b
}

// DisableHoldLock 关闭 WITH (HOLDLOCK) 提示。
func (b *SQLServerMergeBuilder) DisableHoldLock() *SQLServerMergeBuilder {
	b.hintHoldLock = false
	return b
}

// Args 设置 Execute 时传递给 Query/Exec 的参数。
func (b *SQLServerMergeBuilder) Args(args ...interface{}) *SQLServerMergeBuilder {
	b.args = args
	return b
}

// Build 生成完整 MERGE T-SQL。
func (b *SQLServerMergeBuilder) Build() (string, error) {
	if err := b.validate(); err != nil {
		return "", err
	}

	targetAlias := b.targetAlias
	if targetAlias == "" {
		targetAlias = "t"
	}

	var sb strings.Builder
	sb.WriteString("MERGE INTO ")
	sb.WriteString(normalizeSQLServerTableReference(b.targetTable))
	if b.hintHoldLock {
		sb.WriteString(" WITH (HOLDLOCK)")
	}
	sb.WriteString(" AS ")
	sb.WriteString(quoteSQLServerIdentifier(targetAlias))

	sb.WriteString("\nUSING ")
	sb.WriteString(b.sourceSQL)

	sb.WriteString("\nON ")
	sb.WriteString(b.onClause)

	if b.whenMatchedUpdateSQL != "" {
		sb.WriteString("\nWHEN MATCHED THEN\n    UPDATE SET ")
		sb.WriteString(b.whenMatchedUpdateSQL)
	}

	if len(b.whenNotMatchedInsertCols) > 0 {
		quotedCols := make([]string, len(b.whenNotMatchedInsertCols))
		for i, col := range b.whenNotMatchedInsertCols {
			quotedCols[i] = quoteSQLServerIdentifier(col)
		}
		sb.WriteString("\nWHEN NOT MATCHED BY TARGET THEN\n    INSERT (")
		sb.WriteString(strings.Join(quotedCols, ", "))
		sb.WriteString(")\n    VALUES (")
		sb.WriteString(b.whenNotMatchedInsertValue)
		sb.WriteString(")")
	}

	if b.whenNotMatchedBySrcDelete {
		sb.WriteString("\nWHEN NOT MATCHED BY SOURCE THEN\n    DELETE")
	}

	if b.outputSQL != "" {
		sb.WriteString("\nOUTPUT ")
		sb.WriteString(b.outputSQL)
	}

	// SQL Server 官方建议 MERGE 以分号结束。
	sb.WriteString(";")

	return sb.String(), nil
}

// Execute 执行 MERGE（不返回 OUTPUT 结果）。
func (b *SQLServerMergeBuilder) Execute(ctx context.Context) (sql.Result, error) {
	query, err := b.Build()
	if err != nil {
		return nil, err
	}
	return b.adapter.Exec(ctx, query, b.args...)
}

// Query 执行 MERGE 并返回输出结果（用于 OUTPUT 子句场景）。
func (b *SQLServerMergeBuilder) Query(ctx context.Context) (*sql.Rows, error) {
	query, err := b.Build()
	if err != nil {
		return nil, err
	}
	return b.adapter.Query(ctx, query, b.args...)
}

func (b *SQLServerMergeBuilder) validate() error {
	if strings.TrimSpace(b.targetTable) == "" {
		return fmt.Errorf("sqlserver merge: target table is required")
	}
	if strings.TrimSpace(b.sourceSQL) == "" {
		return fmt.Errorf("sqlserver merge: source SQL is required")
	}
	if strings.TrimSpace(b.onClause) == "" {
		return fmt.Errorf("sqlserver merge: ON clause is required")
	}
	if b.whenMatchedUpdateSQL == "" && len(b.whenNotMatchedInsertCols) == 0 && !b.whenNotMatchedBySrcDelete {
		return fmt.Errorf("sqlserver merge: at least one action is required (update/insert/delete)")
	}
	if len(b.whenNotMatchedInsertCols) > 0 && strings.TrimSpace(b.whenNotMatchedInsertValue) == "" {
		return fmt.Errorf("sqlserver merge: insert values expression is required")
	}
	return nil
}

// ==================== SQLServerTempTableBuilder ====================

type SQLServerTempTableColumn struct {
	Name     string
	Type     string
	Nullable bool
}

// SQLServerTempTableBuilder 构建 SQL Server 临时表语句。
// 默认创建局部临时表（#name），可通过 Global() 切换为全局临时表（##name）。
type SQLServerTempTableBuilder struct {
	adapter *SQLServerAdapter

	name    string
	global  bool
	columns []SQLServerTempTableColumn

	asSelectSQL  string
	dropIfExists bool
	args         []interface{}
}

// Global 使用全局临时表（##name）。
func (b *SQLServerTempTableBuilder) Global() *SQLServerTempTableBuilder {
	b.global = true
	return b
}

// Local 使用局部临时表（#name，默认）。
func (b *SQLServerTempTableBuilder) Local() *SQLServerTempTableBuilder {
	b.global = false
	return b
}

// DropIfExists 创建前先执行 IF OBJECT_ID(tempdb..) IS NOT NULL DROP TABLE。
func (b *SQLServerTempTableBuilder) DropIfExists() *SQLServerTempTableBuilder {
	b.dropIfExists = true
	return b
}

// KeepIfExists 保留已存在临时表，不自动 Drop。
func (b *SQLServerTempTableBuilder) KeepIfExists() *SQLServerTempTableBuilder {
	b.dropIfExists = false
	return b
}

// Column 添加列定义。
func (b *SQLServerTempTableBuilder) Column(name, dataType string, nullable bool) *SQLServerTempTableBuilder {
	b.columns = append(b.columns, SQLServerTempTableColumn{
		Name:     strings.TrimSpace(name),
		Type:     strings.TrimSpace(dataType),
		Nullable: nullable,
	})
	return b
}

// AsSelect 使用 SELECT ... INTO 临时表（自动忽略 Column 定义）。
func (b *SQLServerTempTableBuilder) AsSelect(selectSQL string) *SQLServerTempTableBuilder {
	b.asSelectSQL = strings.TrimSpace(selectSQL)
	return b
}

// Args 设置 Execute 时参数。
func (b *SQLServerTempTableBuilder) Args(args ...interface{}) *SQLServerTempTableBuilder {
	b.args = args
	return b
}

// TempTableName 返回实际临时表名（#xxx 或 ##xxx）。
func (b *SQLServerTempTableBuilder) TempTableName() string {
	name := strings.TrimSpace(b.name)
	if strings.HasPrefix(name, "#") {
		if b.global && !strings.HasPrefix(name, "##") {
			return "#" + name
		}
		if !b.global && strings.HasPrefix(name, "##") {
			return strings.TrimPrefix(name, "#")
		}
		return name
	}
	if b.global {
		return "##" + name
	}
	return "#" + name
}

// BuildCreate 生成创建临时表 SQL。
func (b *SQLServerTempTableBuilder) BuildCreate() (string, error) {
	if err := b.validate(); err != nil {
		return "", err
	}

	tmpName := b.TempTableName()
	var sb strings.Builder

	if b.dropIfExists {
		sb.WriteString(fmt.Sprintf("IF OBJECT_ID('tempdb..%s') IS NOT NULL DROP TABLE %s;\n", tmpName, tmpName))
	}

	if b.asSelectSQL != "" {
		sb.WriteString("SELECT * INTO ")
		sb.WriteString(tmpName)
		sb.WriteString(" FROM (\n")
		sb.WriteString(b.asSelectSQL)
		sb.WriteString("\n) AS src;")
		return sb.String(), nil
	}

	cols := make([]string, len(b.columns))
	for i, col := range b.columns {
		nullSQL := "NOT NULL"
		if col.Nullable {
			nullSQL = "NULL"
		}
		cols[i] = fmt.Sprintf("%s %s %s", quoteSQLServerIdentifier(col.Name), col.Type, nullSQL)
	}

	sb.WriteString("CREATE TABLE ")
	sb.WriteString(tmpName)
	sb.WriteString(" (\n    ")
	sb.WriteString(strings.Join(cols, ",\n    "))
	sb.WriteString("\n);")

	return sb.String(), nil
}

// BuildDrop 生成删除临时表 SQL。
func (b *SQLServerTempTableBuilder) BuildDrop() string {
	tmpName := b.TempTableName()
	return fmt.Sprintf("IF OBJECT_ID('tempdb..%s') IS NOT NULL DROP TABLE %s;", tmpName, tmpName)
}

// ExecuteCreate 执行创建临时表 SQL。
func (b *SQLServerTempTableBuilder) ExecuteCreate(ctx context.Context) error {
	query, err := b.BuildCreate()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, query, b.args...)
	return err
}

// Drop 执行删除临时表 SQL。
func (b *SQLServerTempTableBuilder) Drop(ctx context.Context) error {
	_, err := b.adapter.Exec(ctx, b.BuildDrop())
	return err
}

func (b *SQLServerTempTableBuilder) validate() error {
	if strings.TrimSpace(b.name) == "" {
		return fmt.Errorf("sqlserver temp table: name is required")
	}
	if b.asSelectSQL == "" && len(b.columns) == 0 {
		return fmt.Errorf("sqlserver temp table %q: columns or AsSelect SQL is required", b.name)
	}
	for _, col := range b.columns {
		if col.Name == "" || col.Type == "" {
			return fmt.Errorf("sqlserver temp table %q: column name and type are required", b.name)
		}
	}
	return nil
}

// ==================== SQLServerViewBuilder ====================

// SQLServerViewBuilder 构建 SQL Server VIEW 语句。
type SQLServerViewBuilder struct {
	adapter *SQLServerAdapter

	name          string
	selectSQL     string
	createOrAlter bool
	withCheckOpt  bool
	dropIfExists  bool
	args          []interface{}
}

// As 设置视图查询定义（AS 后的 SELECT 语句）。
func (b *SQLServerViewBuilder) As(selectSQL string) *SQLServerViewBuilder {
	b.selectSQL = strings.TrimSpace(selectSQL)
	return b
}

// CreateOnly 使用 CREATE VIEW（若视图已存在会报错）。
func (b *SQLServerViewBuilder) CreateOnly() *SQLServerViewBuilder {
	b.createOrAlter = false
	return b
}

// CreateOrAlter 使用 CREATE OR ALTER VIEW（默认）。
func (b *SQLServerViewBuilder) CreateOrAlter() *SQLServerViewBuilder {
	b.createOrAlter = true
	return b
}

// WithCheckOption 在视图尾部追加 WITH CHECK OPTION。
func (b *SQLServerViewBuilder) WithCheckOption() *SQLServerViewBuilder {
	b.withCheckOpt = true
	return b
}

// DropIfExists 删除视图时使用 IF EXISTS（默认 true）。
func (b *SQLServerViewBuilder) DropIfExists() *SQLServerViewBuilder {
	b.dropIfExists = true
	return b
}

// DropStrict 删除视图时不使用 IF EXISTS。
func (b *SQLServerViewBuilder) DropStrict() *SQLServerViewBuilder {
	b.dropIfExists = false
	return b
}

// Args 设置创建视图 SQL 的参数（一般较少使用）。
func (b *SQLServerViewBuilder) Args(args ...interface{}) *SQLServerViewBuilder {
	b.args = args
	return b
}

// BuildCreate 生成 CREATE/ALTER VIEW SQL。
func (b *SQLServerViewBuilder) BuildCreate() (string, error) {
	if err := b.validate(); err != nil {
		return "", err
	}

	var sb strings.Builder
	if b.createOrAlter {
		sb.WriteString("CREATE OR ALTER VIEW ")
	} else {
		sb.WriteString("CREATE VIEW ")
	}
	sb.WriteString(normalizeSQLServerTableReference(b.name))
	sb.WriteString(" AS\n")
	sb.WriteString(b.selectSQL)
	if b.withCheckOpt {
		sb.WriteString("\nWITH CHECK OPTION")
	}
	sb.WriteString(";")

	return sb.String(), nil
}

// BuildDrop 生成 DROP VIEW SQL。
func (b *SQLServerViewBuilder) BuildDrop() (string, error) {
	if strings.TrimSpace(b.name) == "" {
		return "", fmt.Errorf("sqlserver view: name is required")
	}

	viewName := normalizeSQLServerTableReference(b.name)
	if b.dropIfExists {
		return "DROP VIEW IF EXISTS " + viewName + ";", nil
	}
	return "DROP VIEW " + viewName + ";", nil
}

// ExecuteCreate 执行 CREATE/ALTER VIEW。
func (b *SQLServerViewBuilder) ExecuteCreate(ctx context.Context) error {
	query, err := b.BuildCreate()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, query, b.args...)
	return err
}

// Drop 执行 DROP VIEW。
func (b *SQLServerViewBuilder) Drop(ctx context.Context) error {
	query, err := b.BuildDrop()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, query)
	return err
}

func (b *SQLServerViewBuilder) validate() error {
	if strings.TrimSpace(b.name) == "" {
		return fmt.Errorf("sqlserver view: name is required")
	}
	if strings.TrimSpace(b.selectSQL) == "" {
		return fmt.Errorf("sqlserver view %q: select SQL is required", b.name)
	}
	return nil
}

func normalizeSQLServerTableReference(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return trimmed
	}
	if strings.Contains(trimmed, "[") || strings.Contains(trimmed, "#") {
		return trimmed
	}
	parts := strings.Split(trimmed, ".")
	for i, p := range parts {
		parts[i] = quoteSQLServerIdentifier(strings.TrimSpace(p))
	}
	return strings.Join(parts, ".")
}

// ==================== RecursiveQueryBuilder ====================

// RecursiveQueryBuilder 构建 SQL Server T-SQL 递归 CTE 查询。
//
// 生成的 T-SQL 格式：
//
//	WITH [cte_name] ([col1], [col2], ...) AS (
//	    <anchor>          -- 锚定成员（基准结果集）
//	    UNION ALL
//	    <recursive>       -- 递归成员（自我引用 CTE）
//	)
//	SELECT <select> FROM [cte_name]
//	OPTION (MAXRECURSION <n>)
type RecursiveQueryBuilder struct {
	adapter       *SQLServerAdapter
	cteName       string
	columns       []string // 可选：显示声明 CTE 列名
	anchorSQL     string   // 锚定成员
	recursiveSQL  string   // 递归成员
	selectSQL     string   // 最终 SELECT（默认 *）
	whereClause   string   // 最终查询 WHERE
	orderByClause string   // 最终查询 ORDER BY
	maxRecursion  int      // MAXRECURSION，0 = 不限制
	args          []interface{}
}

// Columns 可选：显式声明 CTE 的列名列表（对应 WITH name (col1, col2) AS ...）。
// 若不调用则不生成列名声明，由 anchor SELECT 决定列名。
func (b *RecursiveQueryBuilder) Columns(cols ...string) *RecursiveQueryBuilder {
	b.columns = cols
	return b
}

// Anchor 设置递归 CTE 的锚定成员（基准查询，非递归部分）。
// 应为完整的 SELECT 语句，不含 UNION。
func (b *RecursiveQueryBuilder) Anchor(sql string) *RecursiveQueryBuilder {
	b.anchorSQL = strings.TrimSpace(sql)
	return b
}

// Recursive 设置递归成员（自我引用上一步结果的查询）。
// 应为完整的 SELECT 语句，JOIN 部分引用 cteName。
func (b *RecursiveQueryBuilder) Recursive(sql string) *RecursiveQueryBuilder {
	b.recursiveSQL = strings.TrimSpace(sql)
	return b
}

// Select 设置从 CTE 中最终 SELECT 的列表达式，如 "id, name, depth"。
// 若不调用，默认使用 SELECT *。
func (b *RecursiveQueryBuilder) Select(selectExpr string) *RecursiveQueryBuilder {
	b.selectSQL = strings.TrimSpace(selectExpr)
	return b
}

// SelectAll 设置最终查询为 SELECT *（默认行为，显式调用以提高可读性）。
func (b *RecursiveQueryBuilder) SelectAll() *RecursiveQueryBuilder {
	b.selectSQL = "*"
	return b
}

// Where 设置最终 SELECT 的 WHERE 过滤条件（不含 WHERE 关键字）。
// 参数化值使用 @p1、@p2 ... 作为占位符（SQL Server 风格）。
//
// 示例：Where("depth <= @p1", 3)
func (b *RecursiveQueryBuilder) Where(condition string, args ...interface{}) *RecursiveQueryBuilder {
	b.whereClause = strings.TrimSpace(condition)
	b.args = args
	return b
}

// OrderBy 设置最终 SELECT 的 ORDER BY 子句（不含 ORDER BY 关键字）。
//
// 示例：OrderBy("depth ASC, name")
func (b *RecursiveQueryBuilder) OrderBy(expr string) *RecursiveQueryBuilder {
	b.orderByClause = strings.TrimSpace(expr)
	return b
}

// MaxRecursion 设置 OPTION(MAXRECURSION n)。
// n = 0 表示不限制（等同于 OPTION(MAXRECURSION 0)）；默认为 100（SQL Server 默认）。
func (b *RecursiveQueryBuilder) MaxRecursion(n int) *RecursiveQueryBuilder {
	if n < 0 {
		n = 0
	}
	b.maxRecursion = n
	return b
}

// Build 生成最终的 T-SQL 字符串。不执行查询，可用于日志或调试。
func (b *RecursiveQueryBuilder) Build() (string, error) {
	if err := b.validate(); err != nil {
		return "", err
	}

	var sb strings.Builder

	// WITH [cte_name] ([columns]) AS (
	sb.WriteString("WITH ")
	sb.WriteString(quoteSQLServerIdentifier(b.cteName))
	if len(b.columns) > 0 {
		sb.WriteString(" (")
		quoted := make([]string, len(b.columns))
		for i, c := range b.columns {
			quoted[i] = quoteSQLServerIdentifier(c)
		}
		sb.WriteString(strings.Join(quoted, ", "))
		sb.WriteString(")")
	}
	sb.WriteString(" AS (\n")

	// anchor
	sb.WriteString("    ")
	sb.WriteString(b.anchorSQL)
	sb.WriteString("\n    UNION ALL\n")

	// recursive
	sb.WriteString("    ")
	sb.WriteString(b.recursiveSQL)
	sb.WriteString("\n)\n")

	// final SELECT
	selectExpr := b.selectSQL
	if selectExpr == "" {
		selectExpr = "*"
	}
	sb.WriteString("SELECT ")
	sb.WriteString(selectExpr)
	sb.WriteString("\nFROM ")
	sb.WriteString(quoteSQLServerIdentifier(b.cteName))

	if b.whereClause != "" {
		sb.WriteString("\nWHERE ")
		sb.WriteString(b.whereClause)
	}

	if b.orderByClause != "" {
		sb.WriteString("\nORDER BY ")
		sb.WriteString(b.orderByClause)
	}

	sb.WriteString(fmt.Sprintf("\nOPTION (MAXRECURSION %d)", b.maxRecursion))

	return sb.String(), nil
}

// Execute 构建并执行递归 CTE 查询，返回原始 *sql.Rows。
// 调用方负责关闭 rows（defer rows.Close()）。
func (b *RecursiveQueryBuilder) Execute(ctx context.Context) (*sql.Rows, error) {
	query, err := b.Build()
	if err != nil {
		return nil, err
	}
	return b.adapter.Query(ctx, query, b.args...)
}

// ScanRows 是 Execute 的便捷包装，将每一行扫描为 map[string]interface{}。
// 适合字段集不确定的场景；对于已知结构推荐直接使用 Execute 手动 Scan。
func (b *RecursiveQueryBuilder) ScanRows(ctx context.Context) ([]map[string]interface{}, error) {
	rows, err := b.Execute(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			val := vals[i]
			// sql.RawBytes → string，方便序列化
			if rb, ok := val.([]byte); ok {
				val = string(rb)
			}
			row[col] = val
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (b *RecursiveQueryBuilder) validate() error {
	if strings.TrimSpace(b.cteName) == "" {
		return fmt.Errorf("sqlserver recursive query: cte name is required")
	}
	if strings.TrimSpace(b.anchorSQL) == "" {
		return fmt.Errorf("sqlserver recursive query: anchor member (Anchor) is required")
	}
	if strings.TrimSpace(b.recursiveSQL) == "" {
		return fmt.Errorf("sqlserver recursive query: recursive member (Recursive) is required")
	}
	return nil
}

// quoteSQLServerIdentifier 用 [] 包裹标识符，避免保留字冲突。
// 内部 ] 使用 ]] 转义（T-SQL 规范）。
func quoteSQLServerIdentifier(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}
