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
	Table string
	Alias string
}

// QueryOrderIR 排序信息。
type QueryOrderIR struct {
	Field     string
	Direction string
}

// QueryJoinIR 连接信息。
type QueryJoinIR struct {
	JoinType string
	Table    string
	Alias    string
	OnClause string
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
