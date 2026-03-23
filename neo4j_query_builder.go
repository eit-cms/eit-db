package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// Neo4jQueryConstructor 面向 Neo4j 的 Cypher 查询构造器。
type Neo4jQueryConstructor struct {
	schema       Schema
	compiler     QueryCompiler
	selectedCols []string
	countExpr    *string
	conditions   []Condition
	orderBys     []OrderBy
	limitVal     *int
	offsetVal    *int
	fromAlias    string
	joins        []cypherJoinClause
	customMode   bool
}

type cypherJoinClause struct {
	joinType string
	semantic JoinSemantic // JoinWith: 已解析的语义意图
	table    string
	alias    string
	onClause string
	schema   Schema      // JoinWith: 目标实体 Schema，nil 表示 raw string JOIN
	filters  []Condition // JoinWith: 对连接节点的额外过滤条件
}

// CypherCompiler 将 QueryIR 编译为 Cypher。
type CypherCompiler struct{}

func NewCypherCompiler() *CypherCompiler {
	return &CypherCompiler{}
}

func NewNeo4jQueryConstructor(schema Schema) *Neo4jQueryConstructor {
	return NewNeo4jQueryConstructorWithCompiler(schema, nil)
}

func NewNeo4jQueryConstructorWithCompiler(schema Schema, compiler QueryCompiler) *Neo4jQueryConstructor {
	if compiler == nil {
		compiler = NewCypherCompiler()
	}
	return &Neo4jQueryConstructor{
		schema:       schema,
		compiler:     compiler,
		selectedCols: make([]string, 0),
		conditions:   make([]Condition, 0),
		orderBys:     make([]OrderBy, 0),
		joins:        make([]cypherJoinClause, 0),
	}
}

func (qb *Neo4jQueryConstructor) FromAlias(alias string) QueryConstructor {
	qb.fromAlias = sanitizeSymbol(alias, "n")
	return qb
}

func (qb *Neo4jQueryConstructor) CrossTableStrategy(strategy CrossTableStrategy) QueryConstructor {
	// Neo4j 不使用 SQL 跨表策略，保留接口兼容。
	return qb
}

func (qb *Neo4jQueryConstructor) CustomMode() QueryConstructor {
	qb.customMode = true
	return qb
}

func (qb *Neo4jQueryConstructor) addJoin(joinType, table, onClause string, alias ...string) *Neo4jQueryConstructor {
	joinAlias := ""
	if len(alias) > 0 {
		joinAlias = sanitizeSymbol(alias[0], "")
	}
	qb.joins = append(qb.joins, cypherJoinClause{
		joinType: strings.TrimSpace(joinType),
		table:    strings.TrimSpace(table),
		alias:    joinAlias,
		onClause: strings.TrimSpace(onClause),
	})
	return qb
}

func (qb *Neo4jQueryConstructor) Join(table, onClause string, alias ...string) QueryConstructor {
	return qb.addJoin("INNER", table, onClause, alias...)
}

func (qb *Neo4jQueryConstructor) LeftJoin(table, onClause string, alias ...string) QueryConstructor {
	return qb.addJoin("LEFT", table, onClause, alias...)
}

func (qb *Neo4jQueryConstructor) RightJoin(table, onClause string, alias ...string) QueryConstructor {
	return qb.addJoin("RIGHT", table, onClause, alias...)
}

func (qb *Neo4jQueryConstructor) CrossJoin(table string, alias ...string) QueryConstructor {
	return qb.addJoin("CROSS", table, "", alias...)
}

// JoinWith 使用 JoinBuilder 进行 Schema 感知的连接。
// Neo4j 端：label 取自 Schema.TableName()；On() 为空时从 Schema FK 约束推断关系类型；
// Filter() 条件在 WHERE 子句中以 join alias 限定输出。
func (qb *Neo4jQueryConstructor) JoinWith(builder *JoinBuilder) QueryConstructor {
	if builder == nil || builder.schema == nil {
		return qb
	}
	table := strings.TrimSpace(builder.schema.TableName())
	if table == "" {
		return qb
	}
	joinAlias := sanitizeSymbol(builder.alias, "")
	resolved := resolveJoinSemantic(builder.semantic, qb.schema, builder.schema)
	qb.joins = append(qb.joins, cypherJoinClause{
		joinType: semanticToSQLJoinType(resolved), // INNER/LEFT/CROSS → CypherCompiler 映射为 MATCH/OPTIONAL MATCH
		semantic: resolved,
		table:    table,
		alias:    joinAlias,
		onClause: builder.onClause,
		schema:   builder.schema,
		filters:  append([]Condition(nil), builder.filters...),
	})
	return qb
}

func (qb *Neo4jQueryConstructor) Where(condition Condition) QueryConstructor {
	if condition != nil {
		qb.conditions = append(qb.conditions, condition)
	}
	return qb
}

func (qb *Neo4jQueryConstructor) WhereWith(builder *WhereBuilder) QueryConstructor {
	if builder == nil {
		return qb
	}
	return qb.Where(builder.Build())
}

func (qb *Neo4jQueryConstructor) WhereAll(conditions ...Condition) QueryConstructor {
	if len(conditions) > 0 {
		qb.conditions = append(qb.conditions, And(conditions...))
	}
	return qb
}

func (qb *Neo4jQueryConstructor) WhereAny(conditions ...Condition) QueryConstructor {
	if len(conditions) > 0 {
		qb.conditions = append(qb.conditions, Or(conditions...))
	}
	return qb
}

func (qb *Neo4jQueryConstructor) Select(fields ...string) QueryConstructor {
	qb.countExpr = nil
	qb.selectedCols = append(qb.selectedCols, fields...)
	return qb
}

func (qb *Neo4jQueryConstructor) Count(fieldName ...string) QueryConstructor {
	expr := "count(*)"
	if len(fieldName) > 0 && strings.TrimSpace(fieldName[0]) != "" && strings.TrimSpace(fieldName[0]) != "*" {
		expr = "count(" + strings.TrimSpace(fieldName[0]) + ")"
	}
	qb.countExpr = &expr
	qb.selectedCols = nil
	qb.limitVal = nil
	qb.offsetVal = nil
	qb.orderBys = nil
	return qb
}

func (qb *Neo4jQueryConstructor) CountWith(builder *CountBuilder) QueryConstructor {
	if builder == nil {
		return qb.Count()
	}
	field := strings.TrimSpace(builder.field)
	if field == "" {
		field = "*"
	}
	expr := "count(*)"
	if field != "*" {
		if builder.distinct {
			expr = "count(DISTINCT " + field + ")"
		} else {
			expr = "count(" + field + ")"
		}
	}
	if alias := strings.TrimSpace(builder.alias); alias != "" {
		expr += " AS " + alias
	}
	qb.countExpr = &expr
	qb.selectedCols = nil
	qb.limitVal = nil
	qb.offsetVal = nil
	qb.orderBys = nil
	return qb
}

func (qb *Neo4jQueryConstructor) OrderBy(field string, direction string) QueryConstructor {
	direction = strings.ToUpper(strings.TrimSpace(direction))
	if direction != "DESC" {
		direction = "ASC"
	}
	qb.orderBys = append(qb.orderBys, OrderBy{Field: field, Direction: direction})
	return qb
}

func (qb *Neo4jQueryConstructor) Limit(count int) QueryConstructor {
	qb.limitVal = &count
	return qb
}

func (qb *Neo4jQueryConstructor) Offset(count int) QueryConstructor {
	qb.offsetVal = &count
	return qb
}

func (qb *Neo4jQueryConstructor) Page(page int, pageSize int) QueryConstructor {
	_, normalizedPageSize, offset := normalizePaginationParams(page, pageSize)
	qb.limitVal = &normalizedPageSize
	if offset <= 0 {
		qb.offsetVal = nil
		return qb
	}
	qb.offsetVal = &offset
	return qb
}

func (qb *Neo4jQueryConstructor) Paginate(builder *PaginationBuilder) QueryConstructor {
	if builder == nil {
		return qb.Page(1, defaultQueryPageSize)
	}

	mode := builder.Mode
	if mode == "" {
		mode = PaginationModeAuto
	}

	if mode == PaginationModeCursor || (mode == PaginationModeAuto && strings.TrimSpace(builder.CursorField) != "") {
		field := strings.TrimSpace(builder.CursorField)
		if field != "" {
			direction := normalizeOrderDirection(builder.CursorDirection)
			pkField := primaryKeyFieldNameOrDefault(qb.schema, "id")
			qOrders := buildStableCursorOrders(field, direction, pkField)
			qb.orderBys = mergeOrderBysIfMissing(qb.orderBys, qOrders)

			cursorCond, err := buildStableCursorCondition(field, direction, builder.CursorValue, builder.CursorPrimaryValue, pkField, false)
			if err != nil {
				return qb
			}
			if cursorCond != nil {
				qb.Where(cursorCond)
			}
		}
		return qb.Page(1, builder.PageSize)
	}

	return qb.Page(builder.Page, builder.PageSize)
}

func (qb *Neo4jQueryConstructor) Build(ctx context.Context) (string, []interface{}, error) {
	ir, err := qb.BuildIR(ctx)
	if err != nil {
		return "", nil, err
	}
	compiler := qb.compiler
	if compiler == nil {
		compiler = NewCypherCompiler()
	}
	return compiler.Compile(ctx, ir)
}

func (qb *Neo4jQueryConstructor) SelectCount(ctx context.Context, repo *Repository) (int64, error) {
	return 0, fmt.Errorf("neo4j query constructor does not support SelectCount via SQL-style API")
}

func (qb *Neo4jQueryConstructor) Upsert(ctx context.Context, repo *Repository, cs *Changeset, conflictColumns ...string) (sql.Result, error) {
	return nil, fmt.Errorf("neo4j query constructor does not support SQL-style Upsert; use Cypher MERGE workflow")
}

func (qb *Neo4jQueryConstructor) BuildIR(ctx context.Context) (*QueryIR, error) {
	projections := append([]string(nil), qb.selectedCols...)
	if qb.countExpr != nil {
		projections = []string{*qb.countExpr}
	}

	ir := &QueryIR{
		Source: QuerySourceIR{
			Table:  qb.schema.TableName(),
			Alias:  sanitizeSymbol(qb.fromAlias, "n"),
			Schema: qb.schema,
		},
		Projections: projections,
		Conditions:  append([]Condition(nil), qb.conditions...),
		Limit:       qb.limitVal,
		Offset:      qb.offsetVal,
		Joins:       make([]QueryJoinIR, 0, len(qb.joins)),
		OrderBys:    make([]QueryOrderIR, 0, len(qb.orderBys)),
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

	for _, order := range qb.orderBys {
		ir.OrderBys = append(ir.OrderBys, QueryOrderIR{Field: order.Field, Direction: order.Direction})
	}

	return ir, nil
}

func (qb *Neo4jQueryConstructor) GetNativeBuilder() interface{} {
	return qb
}

func (c *CypherCompiler) Compile(ctx context.Context, ir *QueryIR) (string, []interface{}, error) {
	_ = ctx
	if ir == nil {
		return "", nil, fmt.Errorf("query ir is nil")
	}
	if strings.TrimSpace(ir.Source.Table) == "" {
		return "", nil, fmt.Errorf("query ir source table is required")
	}

	var cypher strings.Builder
	args := make([]interface{}, 0)
	argIndex := 1

	sourceAlias := sanitizeSymbol(ir.Source.Alias, "n")
	sourceLabel := sanitizeLabel(ir.Source.Table, "Node")
	cypher.WriteString("MATCH (")
	cypher.WriteString(sourceAlias)
	cypher.WriteString(":")
	cypher.WriteString(sourceLabel)
	cypher.WriteString(")")

	// 记录每个 join 解析后的别名，供后续 Filter 条件限定使用。
	resolvedJoinAliases := make([]string, len(ir.Joins))

	for i, join := range ir.Joins {
		joinType := strings.ToUpper(strings.TrimSpace(join.JoinType))
		if joinType == "" {
			joinType = "INNER"
		}
		if joinType == "RIGHT" {
			return "", nil, fmt.Errorf("neo4j query builder does not support RIGHT JOIN semantics")
		}

		joinAlias := sanitizeSymbol(join.Alias, fmt.Sprintf("j%d", i+1))
		resolvedJoinAliases[i] = joinAlias
		joinLabel := sanitizeLabel(join.Table, "Node")

		if joinType == "CROSS" {
			cypher.WriteString(" MATCH (")
			cypher.WriteString(joinAlias)
			cypher.WriteString(":")
			cypher.WriteString(joinLabel)
			cypher.WriteString(")")
			continue
		}

		relType, direction := parseRelationshipSpec(join.OnClause)
		// JoinWith：OnClause 为空且 Schema 已提供时，从 FK 约束推断关系类型（Out 方向）。
		if strings.TrimSpace(join.OnClause) == "" && join.Schema != nil {
			relType = inferNeo4jRelType(ir.Source.Schema, join.Schema)
			if join.Relation != nil && strings.TrimSpace(join.Relation.Name) != "" {
				relType = normalizeCypherRelType(join.Relation.Name)
			}
			direction = "out"
		}
		keyword := "MATCH"
		if joinType == "LEFT" {
			keyword = "OPTIONAL MATCH"
		}

		if join.Relation != nil &&
			join.Relation.Type == RelationManyToMany &&
			join.Relation.Through != nil &&
			strings.TrimSpace(join.Relation.Through.Table) != "" {
			relEdgeType := "RELATED_TO"
			if strings.TrimSpace(join.Relation.Name) != "" {
				relEdgeType = normalizeCypherRelType(join.Relation.Name)
			}
			throughAlias := sanitizeSymbol(fmt.Sprintf("m%d", i+1), fmt.Sprintf("m%d", i+1))
			throughLabel := sanitizeLabel(join.Relation.Through.Table, "Middle")
			cypher.WriteString(" ")
			cypher.WriteString(keyword)
			cypher.WriteString(" (")
			cypher.WriteString(sourceAlias)
			cypher.WriteString(")-")
			cypher.WriteString("[r")
			cypher.WriteString(strconv.Itoa(i + 1))
			cypher.WriteString("a:")
			cypher.WriteString(relEdgeType)
			cypher.WriteString("]->(")
			cypher.WriteString(throughAlias)
			cypher.WriteString(":")
			cypher.WriteString(throughLabel)
			cypher.WriteString(")-")
			cypher.WriteString("[r")
			cypher.WriteString(strconv.Itoa(i + 1))
			cypher.WriteString("b:")
			cypher.WriteString(relEdgeType)
			cypher.WriteString("]->(")
			cypher.WriteString(joinAlias)
			cypher.WriteString(":")
			cypher.WriteString(joinLabel)
			cypher.WriteString(")")
			continue
		}

		cypher.WriteString(" ")
		cypher.WriteString(keyword)
		cypher.WriteString(" ")
		relAlias := fmt.Sprintf("r%d", i+1)
		cypher.WriteString(buildRelationshipPattern(sourceAlias, joinAlias, joinLabel, relAlias, relType, direction))
	}

	// 收集所有过滤条件：主实体条件（source alias 限定）+ 各 join 节点的 Filter 条件（join alias 限定）。
	type cypherCondEntry struct {
		cond  Condition
		alias string
	}
	allConds := make([]cypherCondEntry, 0, len(ir.Conditions))
	for _, c := range ir.Conditions {
		allConds = append(allConds, cypherCondEntry{c, sourceAlias})
	}
	for i, join := range ir.Joins {
		jAlias := resolvedJoinAliases[i]
		for _, f := range join.Filters {
			allConds = append(allConds, cypherCondEntry{f, jAlias})
		}
	}

	if len(allConds) > 0 {
		cypher.WriteString(" WHERE ")
		for i, ce := range allConds {
			if i > 0 {
				cypher.WriteString(" AND ")
			}
			translator := &CypherConditionTranslator{sourceAlias: ce.alias, argIndex: &argIndex}
			clause, clauseArgs, err := ce.cond.Translate(translator)
			if err != nil {
				return "", nil, fmt.Errorf("failed to translate condition: %w", err)
			}
			cypher.WriteString(clause)
			args = append(args, clauseArgs...)
		}
	}

	cypher.WriteString(" RETURN ")
	if len(ir.Projections) == 0 {
		cypher.WriteString(sourceAlias)
	} else {
		for i, field := range ir.Projections {
			if i > 0 {
				cypher.WriteString(", ")
			}
			cypher.WriteString(qualifyCypherField(field, sourceAlias))
		}
	}

	if len(ir.OrderBys) > 0 {
		cypher.WriteString(" ORDER BY ")
		for i, order := range ir.OrderBys {
			if i > 0 {
				cypher.WriteString(", ")
			}
			cypher.WriteString(qualifyCypherField(order.Field, sourceAlias))
			cypher.WriteString(" ")
			cypher.WriteString(order.Direction)
		}
	}

	if ir.Offset != nil {
		cypher.WriteString(fmt.Sprintf(" SKIP %d", *ir.Offset))
	}
	if ir.Limit != nil {
		cypher.WriteString(fmt.Sprintf(" LIMIT %d", *ir.Limit))
	}

	return cypher.String(), args, nil
}

// CypherConditionTranslator 将 Condition 转换为 Cypher 过滤表达式。
type CypherConditionTranslator struct {
	sourceAlias string
	argIndex    *int
}

func (t *CypherConditionTranslator) TranslateCondition(condition Condition) (string, []interface{}, error) {
	switch c := condition.(type) {
	case *SimpleCondition:
		return t.translateSimple(c)
	case *CompositeCondition:
		return t.TranslateComposite(c.Operator, c.Conditions)
	case *NotCondition:
		inner, args, err := c.Condition.Translate(t)
		if err != nil {
			return "", nil, err
		}
		return "NOT (" + inner + ")", args, nil
	default:
		return "", nil, fmt.Errorf("unknown condition type: %T", condition)
	}
}

func (t *CypherConditionTranslator) TranslateComposite(operator string, conditions []Condition) (string, []interface{}, error) {
	if len(conditions) == 0 {
		return "", nil, fmt.Errorf("composite condition must have at least one condition")
	}
	joiner := "AND"
	if strings.EqualFold(strings.TrimSpace(operator), "or") {
		joiner = "OR"
	}

	var b strings.Builder
	args := make([]interface{}, 0)
	b.WriteString("(")
	for i, cond := range conditions {
		if i > 0 {
			b.WriteString(" ")
			b.WriteString(joiner)
			b.WriteString(" ")
		}
		clause, clauseArgs, err := cond.Translate(t)
		if err != nil {
			return "", nil, err
		}
		b.WriteString(clause)
		args = append(args, clauseArgs...)
	}
	b.WriteString(")")
	return b.String(), args, nil
}

func (t *CypherConditionTranslator) translateSimple(cond *SimpleCondition) (string, []interface{}, error) {
	field := qualifyCypherField(cond.Field, t.sourceAlias)

	if cond.Operator == "full_text" {
		placeholder := nextCypherPlaceholder(t.argIndex)
		return field + " CONTAINS " + placeholder, []interface{}{cond.Value}, nil
	}

	switch cond.Operator {
	case "eq", "ne", "gt", "lt", "gte", "lte":
		placeholder := nextCypherPlaceholder(t.argIndex)
		op := map[string]string{"eq": "=", "ne": "<>", "gt": ">", "lt": "<", "gte": ">=", "lte": "<="}[cond.Operator]
		return field + " " + op + " " + placeholder, []interface{}{cond.Value}, nil
	case "in":
		placeholder := nextCypherPlaceholder(t.argIndex)
		values, ok := cond.Value.([]interface{})
		if !ok {
			return "", nil, fmt.Errorf("in condition value must be []interface{}")
		}
		return field + " IN " + placeholder, []interface{}{values}, nil
	case "between":
		minMax, ok := cond.Value.([]interface{})
		if !ok || len(minMax) != 2 {
			return "", nil, fmt.Errorf("between condition value must contain 2 items")
		}
		left := nextCypherPlaceholder(t.argIndex)
		right := nextCypherPlaceholder(t.argIndex)
		return "(" + field + " >= " + left + " AND " + field + " <= " + right + ")", minMax, nil
	case "like":
		pattern, ok := cond.Value.(string)
		if !ok {
			return "", nil, fmt.Errorf("like condition value must be string")
		}
		op, value := translateLikeToCypher(pattern)
		placeholder := nextCypherPlaceholder(t.argIndex)
		return field + " " + op + " " + placeholder, []interface{}{value}, nil
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", cond.Operator)
	}
}

func nextCypherPlaceholder(index *int) string {
	current := *index
	*index++
	return fmt.Sprintf("$p%d", current)
}

func translateLikeToCypher(pattern string) (string, string) {
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		return "CONTAINS", strings.Trim(pattern, "%")
	}
	if strings.HasPrefix(pattern, "%") {
		return "ENDS WITH", strings.TrimPrefix(pattern, "%")
	}
	if strings.HasSuffix(pattern, "%") {
		return "STARTS WITH", strings.TrimSuffix(pattern, "%")
	}
	return "CONTAINS", pattern
}

func qualifyCypherField(field string, sourceAlias string) string {
	trimmed := strings.TrimSpace(field)
	if trimmed == "" {
		return sourceAlias
	}
	if trimmed == "*" {
		return sourceAlias
	}
	if strings.ContainsAny(trimmed, "() \t\n") {
		return trimmed
	}
	if strings.Contains(trimmed, ".") {
		return trimmed
	}
	return sourceAlias + "." + trimmed
}

func sanitizeSymbol(value string, fallback string) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return fallback
	}
	var b strings.Builder
	for _, r := range v {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return fallback
	}
	return b.String()
}

func sanitizeLabel(value string, fallback string) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return fallback
	}
	if strings.Contains(v, ".") {
		parts := strings.Split(v, ".")
		v = parts[len(parts)-1]
	}
	v = strings.Trim(v, "`\"[] ")
	v = sanitizeSymbol(v, fallback)
	if v == "" {
		return fallback
	}
	return v
}

// inferNeo4jRelType 从通用 Schema FK 约束推断 Neo4j 关系类型（供 JoinWith 无 On 子句时使用）。
//
// 推断约定（可通过 On() 显式覆盖；Neo4j 专属深层优化由适配器扩展接口另行提供）：
//   - joinSchema 持有 FK 指向 sourceSchema → 一对多，source -[:HAS]-> join
//     （若 FK 字段为 unique → BINDS 表示一对一）
//   - sourceSchema 持有 FK 指向 joinSchema → 多对一，source -[:HAS]-> join
//     （若 FK 字段为 unique → BINDS 表示一对一）
//   - 无法从约束推断 → RELATED_TO（通用兜底）
func inferNeo4jRelType(sourceSchema, joinSchema Schema) string {
	if sourceSchema == nil || joinSchema == nil {
		return "RELATED_TO"
	}

	// join 持有 FK 指向 source（source HAS_MANY join）
	if cs, ok := joinSchema.(ConstrainedSchema); ok {
		for _, tc := range cs.Constraints() {
			if tc.Kind != ConstraintForeignKey {
				continue
			}
			if !strings.EqualFold(tc.RefTable, sourceSchema.TableName()) {
				continue
			}
			for _, fkField := range tc.Fields {
				if f := joinSchema.GetField(fkField); f != nil && f.Unique {
					return "BINDS"
				}
			}
			return "HAS"
		}
	}

	// source 持有 FK 指向 join（source BELONGS_TO join，仍用 HAS 表示归属方向）
	if cs, ok := sourceSchema.(ConstrainedSchema); ok {
		for _, tc := range cs.Constraints() {
			if tc.Kind != ConstraintForeignKey {
				continue
			}
			if !strings.EqualFold(tc.RefTable, joinSchema.TableName()) {
				continue
			}
			for _, fkField := range tc.Fields {
				if f := sourceSchema.GetField(fkField); f != nil && f.Unique {
					return "BINDS"
				}
			}
			return "HAS"
		}
	}

	return "RELATED_TO"
}

func parseRelationshipSpec(raw string) (string, string) {
	spec := strings.TrimSpace(raw)
	if spec == "" {
		return "RELATED_TO", "out"
	}

	direction := "out"
	if strings.Contains(spec, "<-") && strings.Contains(spec, "->") {
		direction = "both"
	} else if strings.Contains(spec, "<-") {
		direction = "in"
	}

	if strings.Contains(spec, "=") || strings.Contains(spec, " AND ") || strings.Contains(spec, " OR ") {
		return "RELATED_TO", direction
	}

	cleaned := strings.ReplaceAll(spec, "<", "")
	cleaned = strings.ReplaceAll(cleaned, ">", "")
	cleaned = strings.ReplaceAll(cleaned, "-", "")
	cleaned = strings.ReplaceAll(cleaned, "[", "")
	cleaned = strings.ReplaceAll(cleaned, "]", "")
	cleaned = strings.TrimSpace(cleaned)
	if strings.ContainsAny(cleaned, " ()") {
		return "RELATED_TO", direction
	}

	if idx := strings.Index(cleaned, ":"); idx >= 0 {
		cleaned = strings.TrimSpace(cleaned[idx+1:])
	}

	relType := sanitizeSymbol(cleaned, "RELATED_TO")
	if relType == "" {
		relType = "RELATED_TO"
	}
	return relType, direction
}

func buildRelationshipPattern(sourceAlias, targetAlias, targetLabel, relAlias, relType, direction string) string {
	source := "(" + sourceAlias + ")"
	target := "(" + targetAlias + ":" + targetLabel + ")"
	rel := "[" + relAlias + ":" + relType + "]"

	switch direction {
	case "both":
		return source + "-" + rel + "-" + target
	case "in":
		return source + "<-" + rel + "-" + target
	default:
		return source + "-" + rel + "->" + target
	}
}

func normalizeCypherRelType(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "RELATED_TO"
	}
	var b strings.Builder
	lastUnderscore := false
	for _, r := range trimmed {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToUpper(r))
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteRune('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "RELATED_TO"
	}
	return out
}

// Neo4jQueryConstructorProvider Neo4j 查询构造器提供者。
type Neo4jQueryConstructorProvider struct {
	compiler     QueryCompiler
	capabilities *QueryBuilderCapabilities
}

func NewNeo4jQueryConstructorProvider() *Neo4jQueryConstructorProvider {
	capabilities := DefaultQueryBuilderCapabilities()
	capabilities.SupportsSubquery = false
	capabilities.SupportsQueryPlan = false
	capabilities.SupportsIndex = false
	capabilities.SupportsNativeQuery = true
	capabilities.NativeQueryLang = "cypher"
	capabilities.Description = "Neo4j Cypher Query Builder"

	return &Neo4jQueryConstructorProvider{
		compiler:     NewCypherCompiler(),
		capabilities: capabilities,
	}
}

func (p *Neo4jQueryConstructorProvider) SetCompiler(compiler QueryCompiler) *Neo4jQueryConstructorProvider {
	if compiler != nil {
		p.compiler = compiler
	}
	return p
}

func (p *Neo4jQueryConstructorProvider) NewQueryConstructor(schema Schema) QueryConstructor {
	return NewNeo4jQueryConstructorWithCompiler(schema, p.compiler)
}

func (p *Neo4jQueryConstructorProvider) GetCapabilities() *QueryBuilderCapabilities {
	return p.capabilities
}
