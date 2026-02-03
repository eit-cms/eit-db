package db

import (
	"context"
	"fmt"
	"strings"
)

// ==================== SQL Query Builder 实现 ====================

// SQLQueryConstructor SQL 查询构造器 - 底层执行层
// 实现 QueryConstructor 接口，生成标准 SQL
// 每个 Adapter 可以通过继承和覆写方法来实现方言特定的 SQL 生成
type SQLQueryConstructor struct {
	schema       Schema
	dialect      SQLDialect
	selectedCols []string
	conditions   []Condition
	orderBys     []OrderBy
	limitVal     *int
	offsetVal    *int
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
	return "`" + name + "`"
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
	return `"` + name + `"`
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
	return "[" + name + "]"
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
	if dialect == nil {
		dialect = &DefaultSQLDialect{
			name:           "mysql",
			parameterStyle: "?",
		}
	}
	return &SQLQueryConstructor{
		schema:       schema,
		dialect:      dialect,
		selectedCols: make([]string, 0),
		conditions:   make([]Condition, 0),
		orderBys:     make([]OrderBy, 0),
	}
}

// Where 添加单个条件
func (qb *SQLQueryConstructor) Where(condition Condition) QueryConstructor {
	if condition != nil {
		qb.conditions = append(qb.conditions, condition)
	}
	return qb
}

// WhereAll 添加多个 AND 条件
func (qb *SQLQueryConstructor) WhereAll(conditions ...Condition) QueryConstructor {
	if len(conditions) > 0 {
		qb.conditions = append(qb.conditions, And(conditions...))
	}
	return qb
}

// WhereAny 添加多个 OR 条件
func (qb *SQLQueryConstructor) WhereAny(conditions ...Condition) QueryConstructor {
	if len(conditions) > 0 {
		qb.conditions = append(qb.conditions, Or(conditions...))
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
	var sql strings.Builder
	var args []interface{}
	var argIndex int = 1
	
	// SELECT 部分
	sql.WriteString("SELECT ")
	if len(qb.selectedCols) > 0 {
		for i, col := range qb.selectedCols {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(qb.dialect.QuoteIdentifier(col))
		}
	} else {
		// 默认选择所有字段
		sql.WriteString("*")
	}
	
	// FROM 部分
	sql.WriteString(" FROM ")
	sql.WriteString(qb.dialect.QuoteIdentifier(qb.schema.TableName()))
	
	// WHERE 部分
	if len(qb.conditions) > 0 {
		sql.WriteString(" WHERE ")
		translator := &DefaultSQLTranslator{
			dialect:  qb.dialect,
			argIndex: &argIndex,
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
			sql.WriteString(qb.dialect.QuoteIdentifier(order.Field))
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
	capabilities *QueryBuilderCapabilities
}

// NewDefaultSQLQueryConstructorProvider 创建默认 SQL 查询构造器提供者
func NewDefaultSQLQueryConstructorProvider(dialect SQLDialect) *DefaultSQLQueryConstructorProvider {
	return &DefaultSQLQueryConstructorProvider{
		dialect:      dialect,
		capabilities: DefaultQueryBuilderCapabilities(),
	}
}

// NewQueryConstructor 创建新的查询构造器
func (p *DefaultSQLQueryConstructorProvider) NewQueryConstructor(schema Schema) QueryConstructor {
	return NewSQLQueryConstructor(schema, p.dialect)
}

// GetCapabilities 获取查询能力声明
func (p *DefaultSQLQueryConstructorProvider) GetCapabilities() *QueryBuilderCapabilities {
	return p.capabilities
}
