package db

import (
	"context"
	"fmt"
	"strings"
)

// QueryIR 表示查询的中间表示（IR），用于解耦构建阶段与目标语言编译阶段。
type QueryIR struct {
	Source             QuerySourceIR
	Projections        []string
	Conditions         []Condition
	OrderBys           []QueryOrderIR
	Limit              *int
	Offset             *int
	Joins              []QueryJoinIR
	CrossTableStrategy CrossTableStrategy
	Hints              QueryCompileHints
}

// QuerySourceIR 查询数据源。
type QuerySourceIR struct {
	Table  string
	Alias  string
	Schema Schema // Schema 引用，nil 表示非 Schema 感知模式
}

// QueryOrderIR 排序信息。
type QueryOrderIR struct {
	Field     string
	Direction string
}

// QueryJoinIR 连接信息。
type QueryJoinIR struct {
	JoinType string
	Semantic JoinSemantic // 语义意图（来自 JoinBuilder.semantic）；"" 表示 raw Join 或未设置
	Relation *QueryJoinRelationIR
	Table    string
	Schema   Schema       // Schema 引用，来自 JoinBuilder，nil 表示 raw string JOIN
	Alias    string
	OnClause string
	Filters  []Condition  // 对连接目标实体的额外过滤条件（各后端自行处理）
}

// QueryJoinRelationIR 关系元数据（用于适配器优化）。
// 例如：
// - SQL Server 可基于 ManyToMany + Through 选择递归 CTE 或中间表路径。
// - Neo4j 可基于 Through 将中间关系映射为边/路径展开。
// - Mongo 可生成多段 $lookup 管道。
type QueryJoinRelationIR struct {
	Type       RelationType
	Name       string
	Direction  string // "forward" | "reverse"
	Reversible bool
	ResolvedBy string // "relation_registry" | "foreign_key"
	Through    *QueryRelationThroughIR
}

// QueryRelationThroughIR 多对多中间关系元数据。
type QueryRelationThroughIR struct {
	Table     string
	SourceKey string
	TargetKey string
}

// buildQueryJoinRelationIR 从关系注册表 / FK 约束推断关系元数据，供 IR 使用。
func buildQueryJoinRelationIR(sourceSchema, joinSchema Schema) *QueryJoinRelationIR {
	if sourceSchema == nil || joinSchema == nil {
		return nil
	}
	joinTable := joinSchema.TableName()
	sourceTable := sourceSchema.TableName()

	if rs, ok := sourceSchema.(RelationalSchema); ok {
		if rel := rs.FindRelation(joinTable); rel != nil {
			meta := &QueryJoinRelationIR{
				Type:       rel.Type,
				Name:       strings.TrimSpace(rel.Name),
				Direction:  "forward",
				Reversible: rel.Reversible,
				ResolvedBy: "relation_registry",
			}
			if rel.Through != nil {
				table := strings.TrimSpace(rel.Through.Table)
				if table == "" && rel.Through.Schema != nil {
					table = strings.TrimSpace(rel.Through.Schema.TableName())
				}
				meta.Through = &QueryRelationThroughIR{
					Table:     table,
					SourceKey: strings.TrimSpace(rel.Through.SourceKey),
					TargetKey: strings.TrimSpace(rel.Through.TargetKey),
				}
			}
			return meta
		}
	}

	if rj, ok := joinSchema.(RelationalSchema); ok {
		if rel := rj.FindRelation(sourceTable); rel != nil {
			meta := &QueryJoinRelationIR{
				Type:       rel.Type,
				Name:       strings.TrimSpace(rel.Name),
				Direction:  "reverse",
				Reversible: rel.Reversible,
				ResolvedBy: "relation_registry",
			}
			if rel.Through != nil {
				table := strings.TrimSpace(rel.Through.Table)
				if table == "" && rel.Through.Schema != nil {
					table = strings.TrimSpace(rel.Through.Schema.TableName())
				}
				meta.Through = &QueryRelationThroughIR{
					Table:     table,
					SourceKey: strings.TrimSpace(rel.Through.SourceKey),
					TargetKey: strings.TrimSpace(rel.Through.TargetKey),
				}
			}
			return meta
		}
	}

	if cs, ok := sourceSchema.(ConstrainedSchema); ok {
		for _, tc := range cs.Constraints() {
			if tc.Kind == ConstraintForeignKey && strings.EqualFold(tc.RefTable, joinTable) {
				return &QueryJoinRelationIR{Type: RelationBelongsTo, Direction: "forward", ResolvedBy: "foreign_key"}
			}
		}
	}
	if cs, ok := joinSchema.(ConstrainedSchema); ok {
		for _, tc := range cs.Constraints() {
			if tc.Kind == ConstraintForeignKey && strings.EqualFold(tc.RefTable, sourceTable) {
				return &QueryJoinRelationIR{Type: RelationHasMany, Direction: "reverse", ResolvedBy: "foreign_key"}
			}
		}
	}

	return nil
}

// QueryCompileHints 编译阶段提示信息。
type QueryCompileHints struct {
	UseTempTable bool
	TempTable    string
	ViewRoute    *QueryViewRouteIR
}

// QueryViewRouteIR 视图路由提示。
type QueryViewRouteIR struct {
	ViewName string
	Alias    string
}

// QueryCompiler 查询编译器接口。
// Builder 负责构造 IR，Compiler 负责将 IR 编译为目标数据库语言。
type QueryCompiler interface {
	Compile(ctx context.Context, ir *QueryIR) (string, []interface{}, error)
}

// DialectAwareQueryCompiler 可感知 SQL 方言的编译器。
type DialectAwareQueryCompiler interface {
	SetDialect(dialect SQLDialect)
}

// BaseSQLCompiler 默认 SQL 编译器。
// 各 SQL Adapter 可在此基础上定制或替换。
type BaseSQLCompiler struct {
	dialect SQLDialect
}

func NewBaseSQLCompiler(dialect SQLDialect) *BaseSQLCompiler {
	if dialect == nil {
		dialect = &DefaultSQLDialect{name: "mysql", parameterStyle: "?"}
	}
	return &BaseSQLCompiler{dialect: dialect}
}

func (c *BaseSQLCompiler) SetDialect(dialect SQLDialect) {
	if dialect != nil {
		c.dialect = dialect
	}
}

func (c *BaseSQLCompiler) Compile(ctx context.Context, ir *QueryIR) (string, []interface{}, error) {
	if ir == nil {
		return "", nil, fmt.Errorf("query ir is nil")
	}
	if strings.TrimSpace(ir.Source.Table) == "" {
		return "", nil, fmt.Errorf("query ir source table is required")
	}

	var sql strings.Builder
	var args []interface{}
	argIndex := 1
	usedAliases := map[string]bool{}
	if strings.TrimSpace(ir.Source.Alias) != "" {
		usedAliases[strings.TrimSpace(ir.Source.Alias)] = true
	}

	if sqlText, args, ok, err := tryCompileSQLServerRecursiveManyToMany(c.dialect, ir); ok || err != nil {
		return sqlText, args, err
	}

	if ir.Hints.ViewRoute != nil {
		sql.WriteString("SELECT ")
		renderSelectColumnsIR(c.dialect, ir, &sql)
		sql.WriteString(" FROM ")
		sql.WriteString(c.dialect.QuoteIdentifier(ir.Hints.ViewRoute.ViewName))
		sql.WriteString(joinKeywordWithAliasIR(c.dialect, strings.TrimSpace(ir.Hints.ViewRoute.Alias)))
		return appendWhereOrderLimitIR(c.dialect, ir, &sql, args, &argIndex)
	}

	if ir.Hints.UseTempTable {
		tmpName := strings.TrimSpace(ir.Hints.TempTable)
		if tmpName == "" {
			return "", nil, fmt.Errorf("temp table strategy enabled but temp table name is empty")
		}

		baseAlias := strings.TrimSpace(ir.Source.Alias)
		if baseAlias == "" {
			baseAlias = baseTableNameIR(ir)
			if baseAlias != "" {
				usedAliases[baseAlias] = true
			}
		}

		sql.WriteString("IF OBJECT_ID('tempdb..")
		sql.WriteString(tmpName)
		sql.WriteString("') IS NOT NULL DROP TABLE ")
		sql.WriteString(tmpName)
		sql.WriteString(";")
		sql.WriteString(" SELECT * INTO ")
		sql.WriteString(tmpName)
		sql.WriteString(" FROM ")
		sql.WriteString(c.dialect.QuoteIdentifier(ir.Source.Table))
		if baseAlias != "" {
			sql.WriteString(joinKeywordWithAliasIR(c.dialect, baseAlias))
		}
		sql.WriteString(";")

		sql.WriteString(" SELECT ")
		renderSelectColumnsIR(c.dialect, ir, &sql)
		sql.WriteString(" FROM ")
		sql.WriteString(tmpName)
		if baseAlias != "" {
			sql.WriteString(joinKeywordWithAliasIR(c.dialect, baseAlias))
		}
		if err := renderJoinSQLIR(c.dialect, ir, &sql, usedAliases); err != nil {
			return "", nil, err
		}
	} else {
		sql.WriteString("SELECT ")
		renderSelectColumnsIR(c.dialect, ir, &sql)
		sql.WriteString(" FROM ")
		sql.WriteString(c.dialect.QuoteIdentifier(ir.Source.Table))
		if strings.TrimSpace(ir.Source.Alias) != "" {
			sql.WriteString(joinKeywordWithAliasIR(c.dialect, strings.TrimSpace(ir.Source.Alias)))
		}
		if err := renderJoinSQLIR(c.dialect, ir, &sql, usedAliases); err != nil {
			return "", nil, err
		}
	}

	return appendWhereOrderLimitIR(c.dialect, ir, &sql, args, &argIndex)
}

func tryCompileSQLServerRecursiveManyToMany(dialect SQLDialect, ir *QueryIR) (string, []interface{}, bool, error) {
	if !strings.EqualFold(strings.TrimSpace(dialect.Name()), "sqlserver") {
		return "", nil, false, nil
	}
	if len(ir.Joins) != 1 {
		return "", nil, false, nil
	}
	join := ir.Joins[0]
	if strings.TrimSpace(join.OnClause) != "" || join.Relation == nil || join.Relation.Type != RelationManyToMany || join.Relation.Through == nil {
		return "", nil, false, nil
	}
	if resolveSQLManyToManyStrategyIR(dialect) != "recursive_cte" {
		return "", nil, false, nil
	}
	throughTable := strings.TrimSpace(join.Relation.Through.Table)
	if throughTable == "" {
		return "", nil, false, fmt.Errorf("many_to_many recursive_cte requires through table")
	}
	sourceKey := strings.TrimSpace(join.Relation.Through.SourceKey)
	targetKey := strings.TrimSpace(join.Relation.Through.TargetKey)
	if sourceKey == "" || targetKey == "" {
		return "", nil, false, fmt.Errorf("many_to_many recursive_cte requires through source_key and target_key")
	}

	sourceAlias := strings.TrimSpace(ir.Source.Alias)
	if sourceAlias == "" {
		sourceAlias = nextAutoAliasIR(ir.Source.Table, map[string]bool{})
	}
	joinAlias := strings.TrimSpace(join.Alias)
	if joinAlias == "" {
		joinAlias = nextAutoAliasIR(join.Table, map[string]bool{sourceAlias: true})
	}
	bridgeAlias := "m2m_bridge"
	cteName := "m2m_recursive"
	recursiveDepth := resolveSQLServerRecursiveCTEDepthIR(dialect)
	maxRecursion := resolveSQLServerRecursiveCTEMaxRecursionIR(dialect)
	sourcePK := primaryKeyNameOrDefaultIR(ir.Source.Schema)
	targetPK := primaryKeyNameOrDefaultIR(join.Schema)
	joinType := strings.ToUpper(strings.TrimSpace(join.JoinType))
	if joinType == "" {
		joinType = "INNER"
	}

	var sql strings.Builder
	sql.WriteString("WITH ")
	sql.WriteString(dialect.QuoteIdentifier(cteName))
	sql.WriteString(" AS (")
	sql.WriteString(" SELECT ")
	sql.WriteString(dialect.QuoteIdentifier(sourceKey))
	sql.WriteString(", ")
	sql.WriteString(dialect.QuoteIdentifier(targetKey))
	sql.WriteString(", 1 AS ")
	sql.WriteString(dialect.QuoteIdentifier("depth"))
	sql.WriteString(" FROM ")
	sql.WriteString(dialect.QuoteIdentifier(throughTable))
	sql.WriteString(" UNION ALL SELECT t.")
	sql.WriteString(dialect.QuoteIdentifier(sourceKey))
	sql.WriteString(", t.")
	sql.WriteString(dialect.QuoteIdentifier(targetKey))
	sql.WriteString(", r.")
	sql.WriteString(dialect.QuoteIdentifier("depth"))
	sql.WriteString(" + 1 FROM ")
	sql.WriteString(dialect.QuoteIdentifier(throughTable))
	sql.WriteString(" AS ")
	sql.WriteString(dialect.QuoteIdentifier("t"))
	sql.WriteString(" INNER JOIN ")
	sql.WriteString(dialect.QuoteIdentifier(cteName))
	sql.WriteString(" AS ")
	sql.WriteString(dialect.QuoteIdentifier("r"))
	sql.WriteString(" ON t.")
	sql.WriteString(dialect.QuoteIdentifier(sourceKey))
	sql.WriteString(" = r.")
	sql.WriteString(dialect.QuoteIdentifier(targetKey))
	sql.WriteString(" WHERE r.")
	sql.WriteString(dialect.QuoteIdentifier("depth"))
	sql.WriteString(" < ")
	sql.WriteString(fmt.Sprintf("%d", recursiveDepth))
	sql.WriteString(") ")

	sql.WriteString("SELECT ")
	renderSelectColumnsIR(dialect, ir, &sql)
	sql.WriteString(" FROM ")
	sql.WriteString(dialect.QuoteIdentifier(ir.Source.Table))
	sql.WriteString(joinKeywordWithAliasIR(dialect, sourceAlias))

	sql.WriteString(" ")
	sql.WriteString(joinType)
	sql.WriteString(" JOIN ")
	sql.WriteString(dialect.QuoteIdentifier(cteName))
	sql.WriteString(joinKeywordWithAliasIR(dialect, bridgeAlias))
	sql.WriteString(" ON ")
	sql.WriteString(quoteQualifiedIdentifierIR(dialect, sourceAlias, sourcePK))
	sql.WriteString(" = ")
	sql.WriteString(quoteQualifiedIdentifierIR(dialect, bridgeAlias, sourceKey))

	sql.WriteString(" ")
	sql.WriteString(joinType)
	sql.WriteString(" JOIN ")
	sql.WriteString(dialect.QuoteIdentifier(join.Table))
	sql.WriteString(joinKeywordWithAliasIR(dialect, joinAlias))
	sql.WriteString(" ON ")
	sql.WriteString(quoteQualifiedIdentifierIR(dialect, bridgeAlias, targetKey))
	sql.WriteString(" = ")
	sql.WriteString(quoteQualifiedIdentifierIR(dialect, joinAlias, targetPK))

	args := make([]interface{}, 0)
	argIndex := 1
	out, outArgs, err := appendWhereOrderLimitIR(dialect, ir, &sql, args, &argIndex)
	if err != nil {
		return "", nil, true, err
	}
	out += fmt.Sprintf(" OPTION (MAXRECURSION %d)", maxRecursion)
	return out, outArgs, true, nil
}

func appendWhereOrderLimitIR(dialect SQLDialect, ir *QueryIR, sql *strings.Builder, args []interface{}, argIndex *int) (string, []interface{}, error) {
	if len(ir.Conditions) > 0 {
		sql.WriteString(" WHERE ")
		translator := &DefaultSQLTranslator{dialect: dialect, argIndex: argIndex}
		for i, condition := range ir.Conditions {
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

	if len(ir.OrderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		for i, order := range ir.OrderBys {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(dialect.QuoteIdentifier(qualifyIdentifierIR(ir, order.Field)))
			sql.WriteString(" ")
			sql.WriteString(order.Direction)
		}
	}

	limitOffset := dialect.GenerateLimitOffset(ir.Limit, ir.Offset)
	if limitOffset != "" {
		sql.WriteString(" ")
		sql.WriteString(limitOffset)
	}

	return sql.String(), args, nil
}

func renderSelectColumnsIR(dialect SQLDialect, ir *QueryIR, sql *strings.Builder) {
	if len(ir.Projections) > 0 {
		for i, col := range ir.Projections {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(dialect.QuoteIdentifier(qualifyIdentifierIR(ir, col)))
		}
		return
	}
	sql.WriteString("*")
}

func renderJoinSQLIR(dialect SQLDialect, ir *QueryIR, sql *strings.Builder, usedAliases map[string]bool) error {
	if len(ir.Joins) == 0 {
		return nil
	}

	sourceRef := strings.TrimSpace(ir.Source.Alias)
	if sourceRef == "" {
		sourceRef = strings.TrimSpace(ir.Source.Table)
	}

	for i := range ir.Joins {
		join := ir.Joins[i]
		if strings.TrimSpace(join.Table) == "" {
			return fmt.Errorf("join table is required")
		}

		alias := strings.TrimSpace(join.Alias)
		if alias == "" && shouldUseASForAliasIR(dialect) {
			alias = nextAutoAliasIR(join.Table, usedAliases)
		}
		if alias != "" {
			usedAliases[alias] = true
		}

		joinType := strings.ToUpper(strings.TrimSpace(join.JoinType))
		if joinType == "" {
			joinType = "INNER"
		}

		if joinType != "CROSS" &&
			strings.TrimSpace(join.OnClause) == "" &&
			join.Relation != nil &&
			join.Relation.Type == RelationManyToMany &&
			join.Relation.Through != nil &&
			strings.TrimSpace(join.Relation.Through.Table) != "" {
			strategy := resolveSQLManyToManyStrategyIR(dialect)

			throughTable := strings.TrimSpace(join.Relation.Through.Table)
			throughAlias := nextAutoAliasIR(throughTable, usedAliases)
			usedAliases[throughAlias] = true

			sourceKey := strings.TrimSpace(join.Relation.Through.SourceKey)
			targetKey := strings.TrimSpace(join.Relation.Through.TargetKey)
			if sourceKey == "" || targetKey == "" {
				return fmt.Errorf("many_to_many join %q requires through source_key and target_key", join.Table)
			}

			sourcePK := primaryKeyNameOrDefaultIR(ir.Source.Schema)
			targetPK := primaryKeyNameOrDefaultIR(join.Schema)
			targetRef := alias
			if strings.TrimSpace(targetRef) == "" {
				targetRef = strings.TrimSpace(join.Table)
			}

			if strategy == "recursive_cte" && strings.EqualFold(strings.TrimSpace(dialect.Name()), "sqlserver") {
				sql.WriteString(" ")
				sql.WriteString(joinType)
				sql.WriteString(" JOIN (SELECT ")
				sql.WriteString(dialect.QuoteIdentifier(sourceKey))
				sql.WriteString(", ")
				sql.WriteString(dialect.QuoteIdentifier(targetKey))
				sql.WriteString(" FROM ")
				sql.WriteString(dialect.QuoteIdentifier(throughTable))
				sql.WriteString(")")
				sql.WriteString(joinKeywordWithAliasIR(dialect, throughAlias))
				sql.WriteString(" ON ")
				sql.WriteString(quoteQualifiedIdentifierIR(dialect, sourceRef, sourcePK))
				sql.WriteString(" = ")
				sql.WriteString(quoteQualifiedIdentifierIR(dialect, throughAlias, sourceKey))
			} else {
				sql.WriteString(" ")
				sql.WriteString(joinType)
				sql.WriteString(" JOIN ")
				sql.WriteString(dialect.QuoteIdentifier(throughTable))
				sql.WriteString(joinKeywordWithAliasIR(dialect, throughAlias))
				sql.WriteString(" ON ")
				sql.WriteString(quoteQualifiedIdentifierIR(dialect, sourceRef, sourcePK))
				sql.WriteString(" = ")
				sql.WriteString(quoteQualifiedIdentifierIR(dialect, throughAlias, sourceKey))
			}

			sql.WriteString(" ")
			sql.WriteString(joinType)
			sql.WriteString(" JOIN ")
			sql.WriteString(dialect.QuoteIdentifier(join.Table))
			sql.WriteString(joinKeywordWithAliasIR(dialect, alias))
			sql.WriteString(" ON ")
			sql.WriteString(quoteQualifiedIdentifierIR(dialect, throughAlias, targetKey))
			sql.WriteString(" = ")
			sql.WriteString(quoteQualifiedIdentifierIR(dialect, targetRef, targetPK))
			continue
		}

		sql.WriteString(" ")
		sql.WriteString(joinType)
		sql.WriteString(" JOIN ")
		sql.WriteString(dialect.QuoteIdentifier(join.Table))
		sql.WriteString(joinKeywordWithAliasIR(dialect, alias))

		if joinType != "CROSS" {
			if strings.TrimSpace(join.OnClause) == "" {
				return fmt.Errorf("%s JOIN %q requires ON clause", joinType, join.Table)
			}
			sql.WriteString(" ON ")
			sql.WriteString(join.OnClause)
		}
	}

	return nil
}

func primaryKeyNameOrDefaultIR(schema Schema) string {
	if schema != nil {
		if pk := schema.PrimaryKeyField(); pk != nil {
			name := strings.TrimSpace(pk.Name)
			if name != "" {
				return name
			}
		}
	}
	return "id"
}

func quoteQualifiedIdentifierIR(dialect SQLDialect, qualifier, field string) string {
	q := strings.TrimSpace(qualifier)
	f := strings.TrimSpace(field)
	if q == "" {
		return dialect.QuoteIdentifier(f)
	}
	return dialect.QuoteIdentifier(q) + "." + dialect.QuoteIdentifier(f)
}

type sqlManyToManyStrategyDialect interface {
	SQLManyToManyStrategy() string
}

type sqlServerRecursiveCTEDialect interface {
	SQLRecursiveCTEDepth() int
	SQLRecursiveCTEMaxRecursion() int
}

func resolveSQLManyToManyStrategyIR(dialect SQLDialect) string {
	if s, ok := dialect.(sqlManyToManyStrategyDialect); ok {
		v := strings.ToLower(strings.TrimSpace(s.SQLManyToManyStrategy()))
		if v == "recursive_cte" {
			return v
		}
	}
	return "direct_join"
}

func resolveSQLServerRecursiveCTEDepthIR(dialect SQLDialect) int {
	if d, ok := dialect.(sqlServerRecursiveCTEDialect); ok {
		v := d.SQLRecursiveCTEDepth()
		if v > 0 {
			return v
		}
	}
	return 8
}

func resolveSQLServerRecursiveCTEMaxRecursionIR(dialect SQLDialect) int {
	if d, ok := dialect.(sqlServerRecursiveCTEDialect); ok {
		v := d.SQLRecursiveCTEMaxRecursion()
		if v > 0 {
			return v
		}
	}
	return 100
}

func shouldUseASForAliasIR(dialect SQLDialect) bool {
	name := strings.ToLower(strings.TrimSpace(dialect.Name()))
	return name == "postgres" || name == "postgresql" || name == "sqlserver"
}

func joinKeywordWithAliasIR(dialect SQLDialect, alias string) string {
	if strings.TrimSpace(alias) == "" {
		return ""
	}
	if shouldUseASForAliasIR(dialect) {
		return " AS " + dialect.QuoteIdentifier(alias)
	}
	return " " + dialect.QuoteIdentifier(alias)
}

func nextAutoAliasIR(tableName string, used map[string]bool) string {
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

func baseTableNameIR(ir *QueryIR) string {
	if strings.TrimSpace(ir.Source.Alias) != "" {
		return strings.TrimSpace(ir.Source.Alias)
	}
	table := strings.TrimSpace(ir.Source.Table)
	if table == "" {
		return table
	}
	parts := strings.Split(table, ".")
	return strings.TrimSpace(parts[len(parts)-1])
}

func qualifyIdentifierIR(ir *QueryIR, name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" || trimmed == "*" {
		return trimmed
	}
	if strings.ContainsAny(trimmed, " ()\t\n") {
		return trimmed
	}
	if strings.Contains(trimmed, ".") {
		return trimmed
	}
	baseTable := baseTableNameIR(ir)
	if baseTable == "" {
		return trimmed
	}
	return baseTable + "." + trimmed
}
