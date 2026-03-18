package db

import (
	"context"
	"fmt"
	"strings"
)

func quoteIdentifierWithDelimiter(name, leftDelim, rightDelim string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return trimmed
	}

	// 常见表达式保持原样，避免破坏函数调用/别名等高级用法。
	if strings.ContainsAny(trimmed, " ()\t\n") {
		return trimmed
	}

	if trimmed == "*" {
		return trimmed
	}

	parts := strings.Split(trimmed, ".")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "*" {
			parts[i] = part
			continue
		}

		escaped := strings.ReplaceAll(part, rightDelim, rightDelim+rightDelim)
		parts[i] = leftDelim + escaped + rightDelim
	}

	return strings.Join(parts, ".")
}

// ==================== SQL Query Builder 实现 ====================

// SQLQueryConstructor SQL 查询构造器 - 底层执行层
// 实现 QueryConstructor 接口，生成标准 SQL
// 每个 Adapter 可以通过继承和覆写方法来实现方言特定的 SQL 生成
type SQLQueryConstructor struct {
	schema             Schema
	dialect            SQLDialect
	compiler           QueryCompiler
	selectedCols       []string
	conditions         []Condition
	orderBys           []OrderBy
	limitVal           *int
	offsetVal          *int
	fromAlias          string
	joins              []sqlJoinClause
	crossTableStrategy CrossTableStrategy
	// viewRegistry 指定查询跨表视图注册表；nil 时使用 GlobalCrossTableViewRegistry。
	viewRegistry *CrossTableViewRegistry
}

type sqlJoinClause struct {
	joinType string
	table    string
	alias    string
	onClause string
}

func (qb *SQLQueryConstructor) baseTableName() string {
	if strings.TrimSpace(qb.fromAlias) != "" {
		return strings.TrimSpace(qb.fromAlias)
	}

	table := strings.TrimSpace(qb.schema.TableName())
	if table == "" {
		return table
	}

	parts := strings.Split(table, ".")
	return strings.TrimSpace(parts[len(parts)-1])
}

func (qb *SQLQueryConstructor) shouldUseASForAlias() bool {
	name := strings.ToLower(strings.TrimSpace(qb.dialect.Name()))
	return name == "postgres" || name == "postgresql" || name == "sqlserver"
}

func (qb *SQLQueryConstructor) joinKeywordWithAlias(alias string) string {
	if strings.TrimSpace(alias) == "" {
		return ""
	}
	if qb.shouldUseASForAlias() {
		return " AS " + qb.dialect.QuoteIdentifier(alias)
	}
	return " " + qb.dialect.QuoteIdentifier(alias)
}

func (qb *SQLQueryConstructor) nextAutoAlias(tableName string, used map[string]bool) string {
	table := strings.TrimSpace(tableName)
	if table == "" {
		for i := 1; ; i++ {
			candidate := fmt.Sprintf("t%d", i)
			if !used[candidate] {
				return candidate
			}
		}
	}

	parts := strings.Split(table, ".")
	base := strings.TrimSpace(parts[len(parts)-1])
	base = strings.Trim(base, "[]\"`")
	if base == "" {
		base = "t"
	}
	if strings.HasPrefix(base, "#") {
		base = strings.TrimLeft(base, "#")
	}
	if base == "" {
		base = "t"
	}

	if !used[base] {
		return base
	}
	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s_%d", base, i)
		if !used[candidate] {
			return candidate
		}
	}
}

func (qb *SQLQueryConstructor) addJoin(joinType, table, onClause string, alias ...string) *SQLQueryConstructor {
	resolvedAlias := ""
	if len(alias) > 0 {
		resolvedAlias = strings.TrimSpace(alias[0])
	}

	qb.joins = append(qb.joins, sqlJoinClause{
		joinType: strings.TrimSpace(joinType),
		table:    strings.TrimSpace(table),
		alias:    resolvedAlias,
		onClause: strings.TrimSpace(onClause),
	})

	return qb
}

func (qb *SQLQueryConstructor) shouldUseSQLServerTempTableStrategy() bool {
	if !strings.EqualFold(qb.dialect.Name(), "sqlserver") {
		return false
	}
	if len(qb.joins) == 0 {
		return false
	}

	s := qb.crossTableStrategy
	if s == "" {
		s = CrossTableStrategyAuto
	}
	if s == CrossTableStrategyForceDirectJoin {
		return false
	}
	if s == CrossTableStrategyPreferTempTable {
		return true
	}

	// auto: SQL Server 复杂跨表默认使用临时表策略。
	return true
}

func (qb *SQLQueryConstructor) sqlServerTempTableName() string {
	base := qb.baseTableName()
	if base == "" {
		base = "src"
	}
	base = strings.Trim(base, "[]\"`")
	base = strings.TrimLeft(base, "#")
	if base == "" {
		base = "src"
	}
	return "#eit_qb_" + base
}

func (qb *SQLQueryConstructor) renderJoinSQL(sql *strings.Builder, usedAliases map[string]bool) error {
	if len(qb.joins) == 0 {
		return nil
	}

	for i := range qb.joins {
		join := qb.joins[i]
		if strings.TrimSpace(join.table) == "" {
			return fmt.Errorf("join table is required")
		}

		alias := strings.TrimSpace(join.alias)
		if alias == "" && qb.shouldUseASForAlias() {
			alias = qb.nextAutoAlias(join.table, usedAliases)
		}
		if alias != "" {
			usedAliases[alias] = true
		}

		joinType := strings.ToUpper(strings.TrimSpace(join.joinType))
		if joinType == "" {
			joinType = "INNER"
		}

		sql.WriteString(" ")
		sql.WriteString(joinType)
		sql.WriteString(" JOIN ")
		sql.WriteString(qb.dialect.QuoteIdentifier(join.table))
		sql.WriteString(qb.joinKeywordWithAlias(alias))

		if joinType != "CROSS" {
			if strings.TrimSpace(join.onClause) == "" {
				return fmt.Errorf("%s JOIN %q requires ON clause", joinType, join.table)
			}
			sql.WriteString(" ON ")
			sql.WriteString(join.onClause)
		}
	}

	return nil
}

func (qb *SQLQueryConstructor) renderSelectColumns(sql *strings.Builder) {
	if len(qb.selectedCols) > 0 {
		for i, col := range qb.selectedCols {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(qb.dialect.QuoteIdentifier(qb.qualifyIdentifier(col)))
		}
		return
	}
	sql.WriteString("*")
}

func (qb *SQLQueryConstructor) qualifyIdentifier(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" || trimmed == "*" {
		return trimmed
	}

	// 对表达式、函数、别名保持原样。
	if strings.ContainsAny(trimmed, " ()\t\n") {
		return trimmed
	}

	// 已经是限定名时不重复补前缀。
	if strings.Contains(trimmed, ".") {
		return trimmed
	}

	baseTable := qb.baseTableName()
	if baseTable == "" {
		return trimmed
	}

	return baseTable + "." + trimmed
}

func (qb *SQLQueryConstructor) qualifyCondition(condition Condition) Condition {
	switch c := condition.(type) {
	case *SimpleCondition:
		copied := *c
		copied.Field = qb.qualifyIdentifier(c.Field)
		return &copied
	case *CompositeCondition:
		copied := *c
		copied.Conditions = make([]Condition, len(c.Conditions))
		for i, inner := range c.Conditions {
			copied.Conditions[i] = qb.qualifyCondition(inner)
		}
		return &copied
	case *NotCondition:
		copied := *c
		copied.Condition = qb.qualifyCondition(c.Condition)
		return &copied
	default:
		return condition
	}
}

// OrderBy 排序条件
type OrderBy struct {
	Field     string
	Direction string // "ASC" | "DESC"
}

// SQLDialect SQL 方言接口
// 不同的数据库可以实现此接口来提供方言特定的 SQL 生成
type SQLDialect interface {
	// 获取方言名称
	Name() string

	// 转义标识符（表名、列名）
	QuoteIdentifier(name string) string

	// 转义字符串值
	QuoteValue(value interface{}) string

	// 返回参数化占位符（? 或 $1 等）
	GetPlaceholder(index int) string

	// 生成 LIMIT/OFFSET 子句
	GenerateLimitOffset(limit *int, offset *int) string

	// 转换条件为 SQL（可选的方言特定优化）
	TranslateCondition(condition Condition, argIndex *int) (string, []interface{}, error)
}

// DefaultSQLDialect 默认 SQL 方言（MySQL 兼容）
type DefaultSQLDialect struct {
	name           string
	parameterStyle string // "?" | "$n" | "@n"
}

func (d *DefaultSQLDialect) Name() string {
	return d.name
}

func (d *DefaultSQLDialect) QuoteIdentifier(name string) string {
	return quoteIdentifierWithDelimiter(name, "`", "`")
}

func (d *DefaultSQLDialect) QuoteValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	if str, ok := value.(string); ok {
		return "'" + strings.ReplaceAll(str, "'", "''") + "'"
	}
	return fmt.Sprintf("%v", value)
}

func (d *DefaultSQLDialect) GetPlaceholder(index int) string {
	return "?"
}

func (d *DefaultSQLDialect) GenerateLimitOffset(limit *int, offset *int) string {
	var parts []string
	if limit != nil {
		parts = append(parts, fmt.Sprintf("LIMIT %d", *limit))
	}
	if offset != nil {
		parts = append(parts, fmt.Sprintf("OFFSET %d", *offset))
	}
	return strings.Join(parts, " ")
}

func (d *DefaultSQLDialect) TranslateCondition(condition Condition, argIndex *int) (string, []interface{}, error) {
	translator := &DefaultSQLTranslator{dialect: d}
	return translator.TranslateCondition(condition)
}

// PostgreSQL 方言
type PostgreSQLDialect struct {
	DefaultSQLDialect
	nextParamIndex int
}

func NewPostgreSQLDialect() *PostgreSQLDialect {
	return &PostgreSQLDialect{
		DefaultSQLDialect: DefaultSQLDialect{
			name:           "postgresql",
			parameterStyle: "$n",
		},
		nextParamIndex: 1,
	}
}

func (d *PostgreSQLDialect) QuoteIdentifier(name string) string {
	return quoteIdentifierWithDelimiter(name, `"`, `"`)
}

func (d *PostgreSQLDialect) GetPlaceholder(index int) string {
	return fmt.Sprintf("$%d", index)
}

// MySQL 方言
type MySQLDialect struct {
	DefaultSQLDialect
}

func NewMySQLDialect() *MySQLDialect {
	return &MySQLDialect{
		DefaultSQLDialect: DefaultSQLDialect{
			name:           "mysql",
			parameterStyle: "?",
		},
	}
}

// SQLite 方言
type SQLiteDialect struct {
	DefaultSQLDialect
}

func NewSQLiteDialect() *SQLiteDialect {
	return &SQLiteDialect{
		DefaultSQLDialect: DefaultSQLDialect{
			name:           "sqlite",
			parameterStyle: "?",
		},
	}
}

// SQL Server 方言
type SQLServerDialect struct {
	nextParamIndex int
}

func NewSQLServerDialect() *SQLServerDialect {
	return &SQLServerDialect{
		nextParamIndex: 1,
	}
}

func (d *SQLServerDialect) Name() string {
	return "sqlserver"
}

// SQL Server 使用方括号引用标识符
func (d *SQLServerDialect) QuoteIdentifier(name string) string {
	return quoteIdentifierWithDelimiter(name, "[", "]")
}

func (d *SQLServerDialect) QuoteValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	if str, ok := value.(string); ok {
		return "'" + strings.ReplaceAll(str, "'", "''") + "'"
	}
	return fmt.Sprintf("%v", value)
}

// SQL Server 使用 @p1, @p2 形式的参数
func (d *SQLServerDialect) GetPlaceholder(index int) string {
	return fmt.Sprintf("@p%d", index)
}

// SQL Server 使用 OFFSET...ROWS FETCH...ROWS ONLY 语法
func (d *SQLServerDialect) GenerateLimitOffset(limit *int, offset *int) string {
	if limit == nil && offset == nil {
		return ""
	}

	// SQL Server 中 OFFSET 是必须的，没有 OFFSET 则必须用 FETCH FIRST
	var clause string

	if offset != nil {
		clause = fmt.Sprintf("OFFSET %d ROWS", *offset)
		if limit != nil {
			clause += fmt.Sprintf(" FETCH NEXT %d ROWS ONLY", *limit)
		}
	} else if limit != nil {
		// 如果只有 LIMIT 没有 OFFSET，使用 FETCH FIRST
		clause = fmt.Sprintf("OFFSET 0 ROWS FETCH NEXT %d ROWS ONLY", *limit)
	}

	return clause
}

func (d *SQLServerDialect) TranslateCondition(condition Condition, argIndex *int) (string, []interface{}, error) {
	translator := &DefaultSQLTranslator{dialect: d}
	return translator.TranslateCondition(condition)
}

// ==================== SQLQueryBuilder 实现 ====================

// NewSQLQueryConstructor 创建新的 SQL 查询构造器
func NewSQLQueryConstructor(schema Schema, dialect SQLDialect) *SQLQueryConstructor {
	return NewSQLQueryConstructorWithCompiler(schema, dialect, nil)
}

// NewSQLQueryConstructorWithCompiler 使用指定编译器创建查询构造器。
// 若 compiler 为 nil，则默认使用 BaseSQLCompiler。
func NewSQLQueryConstructorWithCompiler(schema Schema, dialect SQLDialect, compiler QueryCompiler) *SQLQueryConstructor {
	if dialect == nil {
		dialect = &DefaultSQLDialect{
			name:           "mysql",
			parameterStyle: "?",
		}
	}
	if compiler == nil {
		compiler = NewBaseSQLCompiler(dialect)
	}
	if c, ok := compiler.(DialectAwareQueryCompiler); ok {
		c.SetDialect(dialect)
	}
	return &SQLQueryConstructor{
		schema:             schema,
		dialect:            dialect,
		compiler:           compiler,
		selectedCols:       make([]string, 0),
		conditions:         make([]Condition, 0),
		orderBys:           make([]OrderBy, 0),
		joins:              make([]sqlJoinClause, 0),
		crossTableStrategy: CrossTableStrategyAuto,
	}
}

// FromAlias 为主表设置别名，便于跨表查询中进行字段映射。
func (qb *SQLQueryConstructor) FromAlias(alias string) QueryConstructor {
	qb.fromAlias = strings.TrimSpace(alias)
	return qb
}

// CrossTableStrategy 设置跨表查询策略。
func (qb *SQLQueryConstructor) CrossTableStrategy(strategy CrossTableStrategy) QueryConstructor {
	strategy = CrossTableStrategy(strings.TrimSpace(string(strategy)))
	if strategy == "" {
		strategy = CrossTableStrategyAuto
	}
	qb.crossTableStrategy = strategy
	return qb
}

// Join 添加 INNER JOIN。
// 若 alias 为空，则在 PostgreSQL/SQL Server 下会自动生成别名并采用 AS 语法。
func (qb *SQLQueryConstructor) Join(table, onClause string, alias ...string) QueryConstructor {
	return qb.addJoin("INNER", table, onClause, alias...)
}

// LeftJoin 添加 LEFT JOIN。
func (qb *SQLQueryConstructor) LeftJoin(table, onClause string, alias ...string) QueryConstructor {
	return qb.addJoin("LEFT", table, onClause, alias...)
}

// RightJoin 添加 RIGHT JOIN。
func (qb *SQLQueryConstructor) RightJoin(table, onClause string, alias ...string) QueryConstructor {
	return qb.addJoin("RIGHT", table, onClause, alias...)
}

// CrossJoin 添加 CROSS JOIN（可选别名，无 ON 条件）。
func (qb *SQLQueryConstructor) CrossJoin(table string, alias ...string) QueryConstructor {
	return qb.addJoin("CROSS", table, "", alias...)
}

// Where 添加单个条件
func (qb *SQLQueryConstructor) Where(condition Condition) QueryConstructor {
	if condition != nil {
		qb.conditions = append(qb.conditions, qb.qualifyCondition(condition))
	}
	return qb
}

// WhereAll 添加多个 AND 条件
func (qb *SQLQueryConstructor) WhereAll(conditions ...Condition) QueryConstructor {
	if len(conditions) > 0 {
		qualified := make([]Condition, 0, len(conditions))
		for _, condition := range conditions {
			qualified = append(qualified, qb.qualifyCondition(condition))
		}
		qb.conditions = append(qb.conditions, And(qualified...))
	}
	return qb
}

// WhereAny 添加多个 OR 条件
func (qb *SQLQueryConstructor) WhereAny(conditions ...Condition) QueryConstructor {
	if len(conditions) > 0 {
		qualified := make([]Condition, 0, len(conditions))
		for _, condition := range conditions {
			qualified = append(qualified, qb.qualifyCondition(condition))
		}
		qb.conditions = append(qb.conditions, Or(qualified...))
	}
	return qb
}

// Select 选择字段
func (qb *SQLQueryConstructor) Select(fields ...string) QueryConstructor {
	qb.selectedCols = append(qb.selectedCols, fields...)
	return qb
}

// OrderBy 排序
func (qb *SQLQueryConstructor) OrderBy(field string, direction string) QueryConstructor {
	direction = strings.ToUpper(direction)
	if direction != "ASC" && direction != "DESC" {
		direction = "ASC"
	}
	qb.orderBys = append(qb.orderBys, OrderBy{
		Field:     field,
		Direction: direction,
	})
	return qb
}

// Limit 限制行数
func (qb *SQLQueryConstructor) Limit(count int) QueryConstructor {
	qb.limitVal = &count
	return qb
}

// Offset 偏移行数
func (qb *SQLQueryConstructor) Offset(count int) QueryConstructor {
	qb.offsetVal = &count
	return qb
}

// Build 构建 SQL 查询
func (qb *SQLQueryConstructor) Build(ctx context.Context) (string, []interface{}, error) {
	ir, err := qb.BuildIR(ctx)
	if err != nil {
		return "", nil, err
	}

	compiler := qb.compiler
	if compiler == nil {
		compiler = NewBaseSQLCompiler(qb.dialect)
	}
	if c, ok := compiler.(DialectAwareQueryCompiler); ok {
		c.SetDialect(qb.dialect)
	}

	return compiler.Compile(ctx, ir)
}

// BuildIR 构建独立查询 IR（中间表示）。
// 该方法不会生成最终 SQL/Cypher，便于后续接入多语言编译器。
func (qb *SQLQueryConstructor) BuildIR(ctx context.Context) (*QueryIR, error) {
	ir := &QueryIR{
		Source: QuerySourceIR{
			Table: qb.schema.TableName(),
			Alias: strings.TrimSpace(qb.fromAlias),
		},
		Projections:        append([]string(nil), qb.selectedCols...),
		Conditions:         append([]Condition(nil), qb.conditions...),
		Limit:              qb.limitVal,
		Offset:             qb.offsetVal,
		CrossTableStrategy: qb.crossTableStrategy,
		Joins:              make([]QueryJoinIR, 0, len(qb.joins)),
		OrderBys:           make([]QueryOrderIR, 0, len(qb.orderBys)),
	}

	for _, order := range qb.orderBys {
		ir.OrderBys = append(ir.OrderBys, QueryOrderIR{Field: order.Field, Direction: order.Direction})
	}

	for _, join := range qb.joins {
		ir.Joins = append(ir.Joins, QueryJoinIR{
			JoinType: join.joinType,
			Table:    join.table,
			Alias:    join.alias,
			OnClause: join.onClause,
		})
	}

	if len(qb.joins) == 1 {
		reg := qb.viewRegistry
		if reg == nil {
			reg = GlobalCrossTableViewRegistry
		}
		mainTable := qb.schema.TableName()
		joinTable := qb.joins[0].table
		if entry, ok := reg.LookupAny(mainTable, joinTable); ok {
			viewAlias := strings.TrimSpace(qb.fromAlias)
			if viewAlias == "" {
				viewAlias = deriveViewAlias(entry.ViewName)
			}
			ir.Hints.ViewRoute = &QueryViewRouteIR{ViewName: entry.ViewName, Alias: viewAlias}
		}
	}

	if qb.shouldUseSQLServerTempTableStrategy() {
		ir.Hints.UseTempTable = true
		ir.Hints.TempTable = qb.sqlServerTempTableName()
	}

	return ir, nil
}

// appendWhereOrderLimit 将 WHERE / ORDER BY / LIMIT 拼接到 sql。
func (qb *SQLQueryConstructor) appendWhereOrderLimit(sql *strings.Builder, args []interface{}, argIndex *int) (string, []interface{}, error) {
	// WHERE 部分
	if len(qb.conditions) > 0 {
		sql.WriteString(" WHERE ")
		translator := &DefaultSQLTranslator{
			dialect:  qb.dialect,
			argIndex: argIndex,
		}
		for i, condition := range qb.conditions {
			if i > 0 {
				sql.WriteString(" AND ")
			}
			condSQL, condArgs, err := condition.Translate(translator)
			if err != nil {
				return "", nil, fmt.Errorf("failed to translate condition: %w", err)
			}
			sql.WriteString(condSQL)
			args = append(args, condArgs...)
		}
	}

	// ORDER BY 部分
	if len(qb.orderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		for i, order := range qb.orderBys {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(qb.dialect.QuoteIdentifier(qb.qualifyIdentifier(order.Field)))
			sql.WriteString(" ")
			sql.WriteString(order.Direction)
		}
	}

	// LIMIT/OFFSET 部分
	limitOffset := qb.dialect.GenerateLimitOffset(qb.limitVal, qb.offsetVal)
	if limitOffset != "" {
		sql.WriteString(" ")
		sql.WriteString(limitOffset)
	}

	return sql.String(), args, nil
}

// GetNativeBuilder 获取底层查询构造器（返回自身）
func (qb *SQLQueryConstructor) GetNativeBuilder() interface{} {
	return qb
}

// ==================== Default SQL Translator ====================

// DefaultSQLTranslator 默认 SQL 转义器
type DefaultSQLTranslator struct {
	dialect  SQLDialect
	argIndex *int
}

// TranslateCondition 转义单个条件
func (t *DefaultSQLTranslator) TranslateCondition(condition Condition) (string, []interface{}, error) {
	switch c := condition.(type) {
	case *SimpleCondition:
		return t.translateSimpleCondition(c)
	case *CompositeCondition:
		return t.translateCompositeCondition(c)
	case *NotCondition:
		return t.translateNotCondition(c)
	default:
		return "", nil, fmt.Errorf("unknown condition type: %T", condition)
	}
}

func (t *DefaultSQLTranslator) translateSimpleCondition(cond *SimpleCondition) (string, []interface{}, error) {
	if cond.Operator == "full_text" {
		return t.translateFullTextCondition(cond)
	}

	var sql strings.Builder
	var args []interface{}

	sql.WriteString(t.dialect.QuoteIdentifier(cond.Field))
	sql.WriteString(" ")

	switch cond.Operator {
	case "eq":
		sql.WriteString("= " + t.dialect.GetPlaceholder(*t.argIndex))
		args = append(args, cond.Value)
		*t.argIndex++
	case "ne":
		sql.WriteString("!= " + t.dialect.GetPlaceholder(*t.argIndex))
		args = append(args, cond.Value)
		*t.argIndex++
	case "gt":
		sql.WriteString("> " + t.dialect.GetPlaceholder(*t.argIndex))
		args = append(args, cond.Value)
		*t.argIndex++
	case "lt":
		sql.WriteString("< " + t.dialect.GetPlaceholder(*t.argIndex))
		args = append(args, cond.Value)
		*t.argIndex++
	case "gte":
		sql.WriteString(">= " + t.dialect.GetPlaceholder(*t.argIndex))
		args = append(args, cond.Value)
		*t.argIndex++
	case "lte":
		sql.WriteString("<= " + t.dialect.GetPlaceholder(*t.argIndex))
		args = append(args, cond.Value)
		*t.argIndex++
	case "in":
		values := cond.Value.([]interface{})
		sql.WriteString("IN (")
		for i := range values {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(t.dialect.GetPlaceholder(*t.argIndex))
			*t.argIndex++
		}
		sql.WriteString(")")
		args = append(args, values...)
	case "like":
		sql.WriteString("LIKE " + t.dialect.GetPlaceholder(*t.argIndex))
		args = append(args, cond.Value)
		*t.argIndex++
	case "between":
		minMax := cond.Value.([]interface{})
		sql.WriteString("BETWEEN " + t.dialect.GetPlaceholder(*t.argIndex))
		*t.argIndex++
		sql.WriteString(" AND " + t.dialect.GetPlaceholder(*t.argIndex))
		*t.argIndex++
		args = append(args, minMax...)
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", cond.Operator)
	}

	return sql.String(), args, nil
}

func (t *DefaultSQLTranslator) translateFullTextCondition(cond *SimpleCondition) (string, []interface{}, error) {
	field := t.dialect.QuoteIdentifier(cond.Field)
	query, ok := cond.Value.(string)
	if !ok {
		return "", nil, fmt.Errorf("full_text condition value must be string")
	}

	placeholder := t.dialect.GetPlaceholder(*t.argIndex)
	*t.argIndex++

	switch strings.ToLower(t.dialect.Name()) {
	case "mysql":
		return fmt.Sprintf("MATCH (%s) AGAINST (%s IN NATURAL LANGUAGE MODE)", field, placeholder), []interface{}{query}, nil
	case "postgres", "postgresql":
		return fmt.Sprintf("to_tsvector('simple', %s) @@ plainto_tsquery('simple', %s)", field, placeholder), []interface{}{query}, nil
	case "sqlserver":
		return fmt.Sprintf("CONTAINS(%s, %s)", field, placeholder), []interface{}{query}, nil
	default:
		// 不支持原生全文语法时，回退到 LIKE 兼容模式
		return fmt.Sprintf("%s LIKE %s", field, placeholder), []interface{}{"%" + query + "%"}, nil
	}
}

func (t *DefaultSQLTranslator) translateCompositeCondition(cond *CompositeCondition) (string, []interface{}, error) {
	return t.TranslateComposite(cond.Operator, cond.Conditions)
}

func (t *DefaultSQLTranslator) translateNotCondition(cond *NotCondition) (string, []interface{}, error) {
	innerSQL, args, err := cond.Condition.Translate(t)
	if err != nil {
		return "", nil, err
	}
	return "NOT (" + innerSQL + ")", args, nil
}

// TranslateComposite 转义复合条件（AND/OR）
func (t *DefaultSQLTranslator) TranslateComposite(operator string, conditions []Condition) (string, []interface{}, error) {
	if len(conditions) == 0 {
		return "", nil, fmt.Errorf("composite condition must have at least one condition")
	}

	var sql strings.Builder
	var args []interface{}

	sqlOperator := "AND"
	if operator == "or" {
		sqlOperator = "OR"
	}

	sql.WriteString("(")
	for i, cond := range conditions {
		if i > 0 {
			sql.WriteString(" " + sqlOperator + " ")
		}
		condSQL, condArgs, err := cond.Translate(t)
		if err != nil {
			return "", nil, err
		}
		sql.WriteString(condSQL)
		args = append(args, condArgs...)
	}
	sql.WriteString(")")

	return sql.String(), args, nil
}

// ==================== SQL Query Constructor Provider ====================

// DefaultSQLQueryConstructorProvider 默认 SQL 查询构造器提供者
type DefaultSQLQueryConstructorProvider struct {
	dialect      SQLDialect
	compiler     QueryCompiler
	capabilities *QueryBuilderCapabilities
}

// NewDefaultSQLQueryConstructorProvider 创建默认 SQL 查询构造器提供者
func NewDefaultSQLQueryConstructorProvider(dialect SQLDialect) *DefaultSQLQueryConstructorProvider {
	return &DefaultSQLQueryConstructorProvider{
		dialect:      dialect,
		compiler:     NewBaseSQLCompiler(dialect),
		capabilities: DefaultQueryBuilderCapabilities(),
	}
}

// SetCompiler 设置该 provider 使用的查询编译器。
// 可用于按 adapter 方言增强 SQL 编译，或替换为其他目标语言编译器。
func (p *DefaultSQLQueryConstructorProvider) SetCompiler(compiler QueryCompiler) *DefaultSQLQueryConstructorProvider {
	if compiler != nil {
		p.compiler = compiler
	}
	return p
}

// NewQueryConstructor 创建新的查询构造器
func (p *DefaultSQLQueryConstructorProvider) NewQueryConstructor(schema Schema) QueryConstructor {
	return NewSQLQueryConstructorWithCompiler(schema, p.dialect, p.compiler)
}

// GetCapabilities 获取查询能力声明
func (p *DefaultSQLQueryConstructorProvider) GetCapabilities() *QueryBuilderCapabilities {
	return p.capabilities
}
