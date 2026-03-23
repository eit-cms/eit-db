package db

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
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
	countExpr          *string
	conditions         []Condition
	orderBys           []OrderBy
	limitVal           *int
	offsetVal          *int
	fromAlias          string
	joins              []sqlJoinClause
	crossTableStrategy CrossTableStrategy
	customQueryMode    bool
	buildErr           error
	// viewRegistry 指定查询跨表视图注册表；nil 时使用 GlobalCrossTableViewRegistry。
	viewRegistry *CrossTableViewRegistry
}

type sqlJoinClause struct {
	joinType string
	semantic JoinSemantic // JoinWith: 已解析的语义意图
	table    string
	alias    string
	onClause string
	schema   Schema      // JoinWith: 目标实体 Schema（Schema 感知 JOIN，用于 IR 元数据）
	filters  []Condition // JoinWith: 对连接目标实体的额外过滤条件
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

func (qb *SQLQueryConstructor) setBuildErr(err error) {
	if err != nil && qb.buildErr == nil {
		qb.buildErr = err
	}
}

func (qb *SQLQueryConstructor) isKnownSchemaField(name string) bool {
	if qb.schema == nil {
		return false
	}
	return qb.schema.GetField(name) != nil
}

func (qb *SQLQueryConstructor) validateFieldReference(name string, allowStar bool) error {
	if qb.customQueryMode {
		return nil
	}
	if qb.schema == nil || len(qb.schema.Fields()) == 0 {
		// 兼容历史使用：部分调用只传表名，不显式注册字段定义。
		return nil
	}

	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return fmt.Errorf("field name cannot be empty")
	}
	if allowStar && trimmed == "*" {
		return nil
	}
	if strings.ContainsAny(trimmed, " ()\t\n") {
		return fmt.Errorf("field %q looks like expression; call CustomMode() to allow custom query expressions", name)
	}

	base := trimmed
	if strings.Contains(base, ".") {
		parts := strings.Split(base, ".")
		base = strings.TrimSpace(parts[len(parts)-1])
	}
	base = strings.Trim(base, "`\"[]")
	if base == "" {
		return fmt.Errorf("field %q is invalid", name)
	}

	if !qb.isKnownSchemaField(base) {
		if len(qb.joins) > 0 {
			// JOIN 场景下允许关联表字段（例如视图路由后的字段）。
			return nil
		}
		return fmt.Errorf("field %q does not exist in schema %q", base, qb.schema.TableName())
	}
	return nil
}

func (qb *SQLQueryConstructor) validateConditionFields(condition Condition) error {
	if qb.customQueryMode || condition == nil {
		return nil
	}

	switch c := condition.(type) {
	case *SimpleCondition:
		return qb.validateFieldReference(c.Field, false)
	case *CompositeCondition:
		for _, inner := range c.Conditions {
			if err := qb.validateConditionFields(inner); err != nil {
				return err
			}
		}
		return nil
	case *NotCondition:
		return qb.validateConditionFields(c.Condition)
	default:
		return nil
	}
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

func normalizeOrderFieldName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, ".") {
		parts := strings.Split(trimmed, ".")
		trimmed = strings.TrimSpace(parts[len(parts)-1])
	}
	return strings.Trim(trimmed, "`\"[]")
}

func normalizeOrderDirection(direction string) string {
	direction = strings.ToUpper(strings.TrimSpace(direction))
	if direction != "DESC" {
		return "ASC"
	}
	return "DESC"
}

func cursorComparisonCondition(field string, direction string, value interface{}) Condition {
	if normalizeOrderDirection(direction) == "DESC" {
		return Lt(field, value)
	}
	return Gt(field, value)
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
	nextParamIndex     int
	manyToManyStrategy string
	recursiveCTEDepth  int
	maxRecursion       int
}

func NewSQLServerDialect() *SQLServerDialect {
	return NewSQLServerDialectWithManyToManyStrategy("direct_join")
}

func NewSQLServerDialectWithManyToManyStrategy(strategy string) *SQLServerDialect {
	return NewSQLServerDialectWithOptions(strategy, 8, 100)
}

func NewSQLServerDialectWithOptions(strategy string, recursiveCTEDepth int, maxRecursion int) *SQLServerDialect {
	v := strings.ToLower(strings.TrimSpace(strategy))
	if v != "recursive_cte" {
		v = "direct_join"
	}
	if recursiveCTEDepth <= 0 {
		recursiveCTEDepth = 8
	}
	if maxRecursion <= 0 {
		maxRecursion = 100
	}
	return &SQLServerDialect{
		nextParamIndex:     1,
		manyToManyStrategy: v,
		recursiveCTEDepth:  recursiveCTEDepth,
		maxRecursion:       maxRecursion,
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

func (d *SQLServerDialect) SQLManyToManyStrategy() string {
	v := strings.ToLower(strings.TrimSpace(d.manyToManyStrategy))
	if v == "recursive_cte" {
		return v
	}
	return "direct_join"
}

func (d *SQLServerDialect) SQLRecursiveCTEDepth() int {
	if d.recursiveCTEDepth <= 0 {
		return 8
	}
	return d.recursiveCTEDepth
}

func (d *SQLServerDialect) SQLRecursiveCTEMaxRecursion() int {
	if d.maxRecursion <= 0 {
		return 100
	}
	return d.maxRecursion
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

func (qb *SQLQueryConstructor) CustomMode() QueryConstructor {
	qb.customQueryMode = true
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

// JoinWith 使用 JoinBuilder 进行 Schema 感知的跨表连接。
// 表名从 builder.schema.TableName() 取得；Filter() 条件以 join alias（或表名）限定后追加到 WHERE。
func (qb *SQLQueryConstructor) JoinWith(builder *JoinBuilder) QueryConstructor {
	if builder == nil || builder.schema == nil {
		return qb
	}
	table := strings.TrimSpace(builder.schema.TableName())
	if table == "" {
		return qb
	}
	alias := strings.TrimSpace(builder.alias)

	resolved := resolveJoinSemantic(builder.semantic, qb.schema, builder.schema)
	qb.joins = append(qb.joins, sqlJoinClause{
		joinType: semanticToSQLJoinType(resolved),
		semantic: resolved,
		table:    table,
		alias:    alias,
		onClause: strings.TrimSpace(builder.onClause),
		schema:   builder.schema,
		filters:  append([]Condition(nil), builder.filters...),
	})

	// filter 条件以 join alias（回退到表名）限定后追加到全局 WHERE。
	filterPrefix := alias
	if filterPrefix == "" {
		filterPrefix = table
	}
	for _, f := range builder.filters {
		qb.conditions = append(qb.conditions, qb.qualifyConditionWithPrefix(f, filterPrefix))
	}
	return qb
}

// qualifyConditionWithPrefix 使用指定前缀（而非主表前缀）限定条件字段。
func (qb *SQLQueryConstructor) qualifyConditionWithPrefix(condition Condition, prefix string) Condition {
	switch c := condition.(type) {
	case *SimpleCondition:
		copied := *c
		field := strings.TrimSpace(c.Field)
		if !strings.Contains(field, ".") && prefix != "" {
			copied.Field = prefix + "." + field
		}
		return &copied
	case *CompositeCondition:
		copied := *c
		copied.Conditions = make([]Condition, len(c.Conditions))
		for i, inner := range c.Conditions {
			copied.Conditions[i] = qb.qualifyConditionWithPrefix(inner, prefix)
		}
		return &copied
	case *NotCondition:
		copied := *c
		copied.Condition = qb.qualifyConditionWithPrefix(c.Condition, prefix)
		return &copied
	default:
		return condition
	}
}

// Where 添加单个条件
func (qb *SQLQueryConstructor) Where(condition Condition) QueryConstructor {
	if condition != nil {
		qb.conditions = append(qb.conditions, qb.qualifyCondition(condition))
	}
	return qb
}

func (qb *SQLQueryConstructor) WhereWith(builder *WhereBuilder) QueryConstructor {
	if builder == nil {
		return qb
	}
	return qb.Where(builder.Build())
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
	qb.countExpr = nil
	qb.selectedCols = append(qb.selectedCols, fields...)
	return qb
}

func (qb *SQLQueryConstructor) Count(fieldName ...string) QueryConstructor {
	expr := "COUNT(*)"
	if len(fieldName) > 0 && strings.TrimSpace(fieldName[0]) != "" && strings.TrimSpace(fieldName[0]) != "*" {
		expr = "COUNT(" + qb.dialect.QuoteIdentifier(qb.qualifyIdentifier(fieldName[0])) + ")"
	}
	qb.countExpr = &expr
	qb.selectedCols = nil
	qb.limitVal = nil
	qb.offsetVal = nil
	qb.orderBys = nil
	return qb
}

func (qb *SQLQueryConstructor) CountWith(builder *CountBuilder) QueryConstructor {
	if builder == nil {
		return qb.Count()
	}

	field := strings.TrimSpace(builder.field)
	if field == "" {
		field = "*"
	}
	if field != "*" {
		qb.setBuildErr(qb.validateFieldReference(field, false))
	}

	base := "COUNT(*)"
	if field != "*" {
		qualified := qb.dialect.QuoteIdentifier(qb.qualifyIdentifier(field))
		if builder.distinct {
			base = "COUNT(DISTINCT " + qualified + ")"
		} else {
			base = "COUNT(" + qualified + ")"
		}
	}

	if alias := strings.TrimSpace(builder.alias); alias != "" {
		base += " AS " + qb.dialect.QuoteIdentifier(alias)
	}

	qb.countExpr = &base
	qb.selectedCols = nil
	qb.limitVal = nil
	qb.offsetVal = nil
	qb.orderBys = nil
	return qb
}

// OrderBy 排序
func (qb *SQLQueryConstructor) OrderBy(field string, direction string) QueryConstructor {
	direction = normalizeOrderDirection(direction)
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

// Page 按页设置 LIMIT/OFFSET。
func (qb *SQLQueryConstructor) Page(page int, pageSize int) QueryConstructor {
	normalizedPage, normalizedPageSize, offset := normalizePaginationParams(page, pageSize)
	pkField := primaryKeyFieldNameOrDefault(qb.schema, "")
	qb.orderBys = ensureStableOffsetOrders(qb.orderBys, pkField)
	qb.limitVal = &normalizedPageSize
	if normalizedPage <= 1 {
		qb.offsetVal = nil
		return qb
	}
	qb.offsetVal = &offset
	return qb
}

// CursorPage 使用游标分页替代大 OFFSET，要求首个排序字段与游标字段一致。
// 当排序字段不是主键时，需要同时提供 cursorPrimaryValue 作为稳定 tie-breaker。
func (qb *SQLQueryConstructor) CursorPage(field string, direction string, cursorValue interface{}, cursorPrimaryValue interface{}, pageSize int) *SQLQueryConstructor {
	field = strings.TrimSpace(field)
	if field == "" {
		qb.setBuildErr(fmt.Errorf("cursor pagination requires a sort field"))
		return qb
	}
	qb.setBuildErr(qb.validateFieldReference(field, false))
	if qb.buildErr != nil {
		return qb
	}

	direction = normalizeOrderDirection(direction)
	pkField := primaryKeyFieldNameOrDefault(qb.schema, "")
	if pkField == "" {
		qb.setBuildErr(fmt.Errorf("cursor pagination requires a primary key field"))
		return qb
	}

	if len(qb.orderBys) > 0 && normalizeOrderFieldName(qb.orderBys[0].Field) != normalizeOrderFieldName(field) {
		qb.setBuildErr(fmt.Errorf("cursor pagination requires the first ORDER BY field to match %q", field))
		return qb
	}

	qOrders := buildStableCursorOrders(field, direction, pkField)
	qb.orderBys = mergeOrderBysIfMissing(qb.orderBys, qOrders)

	cursorCond, err := buildStableCursorCondition(field, direction, cursorValue, cursorPrimaryValue, pkField, true)
	if err != nil {
		qb.setBuildErr(err)
		return qb
	}
	if cursorCond != nil {
		qb.Where(cursorCond)
	}

	_, normalizedPageSize, _ := normalizePaginationParams(1, pageSize)
	qb.limitVal = &normalizedPageSize
	qb.offsetVal = nil
	return qb
}

func (qb *SQLQueryConstructor) Paginate(builder *PaginationBuilder) QueryConstructor {
	if builder == nil {
		return qb.Page(1, defaultQueryPageSize)
	}

	mode := builder.Mode
	if mode == "" {
		mode = PaginationModeAuto
	}

	switch mode {
	case PaginationModeCursor:
		return qb.CursorPage(builder.CursorField, builder.CursorDirection, builder.CursorValue, builder.CursorPrimaryValue, builder.PageSize)
	case PaginationModeOffset:
		return qb.Page(builder.Page, builder.PageSize)
	default:
		if strings.TrimSpace(builder.CursorField) != "" {
			return qb.CursorPage(builder.CursorField, builder.CursorDirection, builder.CursorValue, builder.CursorPrimaryValue, builder.PageSize)
		}
		return qb.Page(builder.Page, builder.PageSize)
	}
}

func (qb *SQLQueryConstructor) buildCountConstructor() QueryConstructor {
	clone := &SQLQueryConstructor{
		schema:             qb.schema,
		dialect:            qb.dialect,
		compiler:           qb.compiler,
		selectedCols:       append([]string(nil), qb.selectedCols...),
		conditions:         append([]Condition(nil), qb.conditions...),
		orderBys:           append([]OrderBy(nil), qb.orderBys...),
		fromAlias:          qb.fromAlias,
		joins:              append([]sqlJoinClause(nil), qb.joins...),
		crossTableStrategy: qb.crossTableStrategy,
		customQueryMode:    qb.customQueryMode,
		buildErr:           qb.buildErr,
		viewRegistry:       qb.viewRegistry,
	}
	if qb.countExpr != nil {
		expr := *qb.countExpr
		clone.countExpr = &expr
		clone.limitVal = nil
		clone.offsetVal = nil
		clone.orderBys = nil
		return clone
	}
	clone.Count()
	return clone
}

// Build 构建 SQL 查询
func (qb *SQLQueryConstructor) Build(ctx context.Context) (string, []interface{}, error) {
	if qb.buildErr != nil {
		return "", nil, qb.buildErr
	}
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

// SelectCount 统计匹配记录数，忽略当前构造器上的分页设置。
func (qb *SQLQueryConstructor) SelectCount(ctx context.Context, repo *Repository) (int64, error) {
	if repo == nil {
		return 0, fmt.Errorf("repository is nil")
	}
	if qb.buildErr != nil {
		return 0, qb.buildErr
	}

	ir, err := qb.BuildIR(ctx)
	if err != nil {
		return 0, err
	}

	countExpr := "COUNT(*)"
	if qb.countExpr != nil {
		countExpr = *qb.countExpr
	}
	ir.Projections = []string{countExpr}
	ir.OrderBys = nil
	ir.Limit = nil
	ir.Offset = nil

	compiler := qb.compiler
	if compiler == nil {
		compiler = NewBaseSQLCompiler(qb.dialect)
	}
	if c, ok := compiler.(DialectAwareQueryCompiler); ok {
		c.SetDialect(qb.dialect)
	}

	query, args, err := compiler.Compile(ctx, ir)
	if err != nil {
		return 0, err
	}

	row := repo.QueryRow(ctx, query, args...)
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// Upsert 基于 Changeset 执行 upsert。
// 支持方言原生 upsert；不支持时回退到事务模拟（先 UPDATE，后 INSERT）。
func (qb *SQLQueryConstructor) Upsert(ctx context.Context, repo *Repository, cs *Changeset, conflictColumns ...string) (sql.Result, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is nil")
	}
	if cs == nil {
		return nil, fmt.Errorf("changeset is nil")
	}
	if !cs.IsValid() {
		return nil, fmt.Errorf("changeset validation failed: %v", cs.Errors())
	}

	cs.ForceChanges()
	changes := cs.Changes()
	if len(changes) == 0 {
		return nil, fmt.Errorf("no fields to upsert")
	}

	normalizedConflict := normalizeConflictColumns(conflictColumns)
	if len(normalizedConflict) == 0 {
		normalizedConflict = inferConflictColumnsFromSchema(qb.schema)
	}
	if len(normalizedConflict) == 0 {
		return nil, fmt.Errorf("upsert requires conflict columns")
	}

	keys := sortedMapKeys(changes)
	dialectName := strings.ToLower(strings.TrimSpace(qb.dialect.Name()))
	supportsNativeUpsert := false
	if features := repo.GetAdapter().GetQueryFeatures(); features != nil {
		supportsNativeUpsert = features.SupportsUpsert
	}

	if supportsNativeUpsert {
		query, args, err := qb.buildNativeUpsertSQL(dialectName, keys, changes, normalizedConflict)
		if err != nil {
			return nil, err
		}
		return repo.Exec(ctx, query, args...)
	}

	return qb.upsertWithTransactionFallback(ctx, repo, keys, changes, normalizedConflict)
}

func (qb *SQLQueryConstructor) buildNativeUpsertSQL(dialectName string, keys []string, changes map[string]interface{}, conflictColumns []string) (string, []interface{}, error) {
	quotedTable := qb.dialect.QuoteIdentifier(qb.schema.TableName())
	insertCols := make([]string, 0, len(keys))
	placeholders := make([]string, 0, len(keys))
	args := make([]interface{}, 0, len(keys))

	argIndex := 1
	for _, key := range keys {
		insertCols = append(insertCols, qb.dialect.QuoteIdentifier(key))
		placeholders = append(placeholders, qb.dialect.GetPlaceholder(argIndex))
		args = append(args, changes[key])
		argIndex++
	}

	nonConflict := nonConflictColumns(keys, conflictColumns)
	nonConflict = excludePrimaryColumns(qb.schema, nonConflict)
	if len(nonConflict) == 0 {
		nonConflict = []string{conflictColumns[0]}
	}

	switch dialectName {
	case "postgres", "postgresql", "sqlite":
		sets := make([]string, 0, len(nonConflict))
		for _, col := range nonConflict {
			quotedCol := qb.dialect.QuoteIdentifier(col)
			if containsString(conflictColumns, col) {
				sets = append(sets, fmt.Sprintf("%s = %s", quotedCol, quotedCol))
			} else {
				sets = append(sets, fmt.Sprintf("%s = excluded.%s", quotedCol, quotedCol))
			}
		}
		conflictCols := make([]string, 0, len(conflictColumns))
		for _, col := range conflictColumns {
			conflictCols = append(conflictCols, qb.dialect.QuoteIdentifier(col))
		}
		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			quotedTable,
			strings.Join(insertCols, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(conflictCols, ", "),
			strings.Join(sets, ", "),
		)
		return query, args, nil

	case "mysql":
		sets := make([]string, 0, len(nonConflict))
		for _, col := range nonConflict {
			quotedCol := qb.dialect.QuoteIdentifier(col)
			if containsString(conflictColumns, col) {
				sets = append(sets, fmt.Sprintf("%s = %s", quotedCol, quotedCol))
			} else {
				sets = append(sets, fmt.Sprintf("%s = VALUES(%s)", quotedCol, quotedCol))
			}
		}
		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			quotedTable,
			strings.Join(insertCols, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(sets, ", "),
		)
		return query, args, nil

	case "sqlserver":
		srcCols := make([]string, 0, len(keys))
		for _, col := range keys {
			srcCols = append(srcCols, qb.dialect.QuoteIdentifier(col))
		}
		onClauses := make([]string, 0, len(conflictColumns))
		for _, col := range conflictColumns {
			quotedCol := qb.dialect.QuoteIdentifier(col)
			onClauses = append(onClauses, fmt.Sprintf("t.%s = s.%s", quotedCol, quotedCol))
		}
		sets := make([]string, 0, len(nonConflict))
		for _, col := range nonConflict {
			quotedCol := qb.dialect.QuoteIdentifier(col)
			if containsString(conflictColumns, col) {
				sets = append(sets, fmt.Sprintf("t.%s = t.%s", quotedCol, quotedCol))
			} else {
				sets = append(sets, fmt.Sprintf("t.%s = s.%s", quotedCol, quotedCol))
			}
		}
		query := fmt.Sprintf(
			"MERGE INTO %s AS t USING (VALUES (%s)) AS s (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
			quotedTable,
			strings.Join(placeholders, ", "),
			strings.Join(srcCols, ", "),
			strings.Join(onClauses, " AND "),
			strings.Join(sets, ", "),
			strings.Join(insertCols, ", "),
			buildSQLServerSourceProjection(keys, qb.dialect),
		)
		return query, args, nil

	default:
		return "", nil, fmt.Errorf("native upsert is not implemented for dialect %q", dialectName)
	}
}

func (qb *SQLQueryConstructor) upsertWithTransactionFallback(ctx context.Context, repo *Repository, keys []string, changes map[string]interface{}, conflictColumns []string) (sql.Result, error) {
	tx, err := repo.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	quotedTable := qb.dialect.QuoteIdentifier(qb.schema.TableName())
	nonConflict := nonConflictColumns(keys, conflictColumns)
	nonConflict = excludePrimaryColumns(qb.schema, nonConflict)
	if len(nonConflict) == 0 {
		nonConflict = []string{conflictColumns[0]}
	}

	setClauses := make([]string, 0, len(nonConflict))
	updateArgs := make([]interface{}, 0, len(nonConflict)+len(conflictColumns))
	argIndex := 1
	for _, col := range nonConflict {
		quotedCol := qb.dialect.QuoteIdentifier(col)
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", quotedCol, qb.dialect.GetPlaceholder(argIndex)))
		updateArgs = append(updateArgs, changes[col])
		argIndex++
	}

	whereClauses := make([]string, 0, len(conflictColumns))
	for _, col := range conflictColumns {
		quotedCol := qb.dialect.QuoteIdentifier(col)
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", quotedCol, qb.dialect.GetPlaceholder(argIndex)))
		updateArgs = append(updateArgs, changes[col])
		argIndex++
	}

	updateSQL := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		quotedTable,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	updateRes, err := tx.Exec(ctx, updateSQL, updateArgs...)
	if err != nil {
		return nil, err
	}
	if rows, _ := updateRes.RowsAffected(); rows > 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return updateRes, nil
	}

	insertCols := make([]string, 0, len(keys))
	insertVals := make([]string, 0, len(keys))
	insertArgs := make([]interface{}, 0, len(keys))
	argIndex = 1
	for _, col := range keys {
		insertCols = append(insertCols, qb.dialect.QuoteIdentifier(col))
		insertVals = append(insertVals, qb.dialect.GetPlaceholder(argIndex))
		insertArgs = append(insertArgs, changes[col])
		argIndex++
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quotedTable,
		strings.Join(insertCols, ", "),
		strings.Join(insertVals, ", "),
	)
	insertRes, err := tx.Exec(ctx, insertSQL, insertArgs...)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return insertRes, nil
}

func sortedMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func normalizeConflictColumns(cols []string) []string {
	if len(cols) == 0 {
		return nil
	}
	result := make([]string, 0, len(cols))
	seen := map[string]struct{}{}
	for _, col := range cols {
		trimmed := strings.TrimSpace(col)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func inferConflictColumnsFromSchema(schema Schema) []string {
	if schema == nil {
		return nil
	}
	uniqueCols := make([]string, 0)
	primaryCols := make([]string, 0)
	for _, f := range schema.Fields() {
		if f.Unique {
			uniqueCols = append(uniqueCols, f.Name)
		}
		if f.Primary {
			primaryCols = append(primaryCols, f.Name)
		}
	}
	if len(uniqueCols) > 0 {
		return normalizeConflictColumns(uniqueCols)
	}
	return normalizeConflictColumns(primaryCols)
}

func nonConflictColumns(keys, conflicts []string) []string {
	result := make([]string, 0, len(keys))
	for _, k := range keys {
		if !containsString(conflicts, k) {
			result = append(result, k)
		}
	}
	return result
}

func excludePrimaryColumns(schema Schema, columns []string) []string {
	if schema == nil {
		return columns
	}
	result := make([]string, 0, len(columns))
	for _, col := range columns {
		field := schema.GetField(col)
		if field != nil && field.Primary {
			continue
		}
		result = append(result, col)
	}
	return result
}

func containsString(items []string, target string) bool {
	for _, it := range items {
		if it == target {
			return true
		}
	}
	return false
}

func buildSQLServerSourceProjection(keys []string, dialect SQLDialect) string {
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, "s."+dialect.QuoteIdentifier(k))
	}
	return strings.Join(parts, ", ")
}

// BuildIR 构建独立查询 IR（中间表示）。
// 该方法不会生成最终 SQL/Cypher，便于后续接入多语言编译器。
func (qb *SQLQueryConstructor) BuildIR(ctx context.Context) (*QueryIR, error) {
	if qb.buildErr != nil {
		return nil, qb.buildErr
	}

	if !qb.customQueryMode {
		for _, f := range qb.selectedCols {
			if err := qb.validateFieldReference(f, true); err != nil {
				return nil, err
			}
		}
		for _, o := range qb.orderBys {
			if err := qb.validateFieldReference(o.Field, false); err != nil {
				return nil, err
			}
		}
		for _, c := range qb.conditions {
			if err := qb.validateConditionFields(c); err != nil {
				return nil, err
			}
		}
	}

	projections := append([]string(nil), qb.selectedCols...)
	if qb.countExpr != nil {
		projections = []string{*qb.countExpr}
	}

	ir := &QueryIR{
		Source: QuerySourceIR{
			Table:  qb.schema.TableName(),
			Alias:  strings.TrimSpace(qb.fromAlias),
			Schema: qb.schema,
		},
		Projections:        projections,
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
			Semantic: join.semantic,
			Relation: buildQueryJoinRelationIR(qb.schema, join.schema),
			Table:    join.table,
			Alias:    join.alias,
			OnClause: join.onClause,
			Schema:   join.schema,
			Filters:  append([]Condition(nil), join.filters...),
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
