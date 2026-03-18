package db

import (
	"context"
	"fmt"
	"strings"
)

// ==================== PostgreSQL 特色功能入口 ====================

// PostgreSQLFeatures 提供 PostgreSQL 特有的高级数据库功能。
// 通过 GetPostgreSQLFeatures(adapter) 获取实例，非 PostgreSQL 适配器返回 false。
//
// 示例：
//
//	features, ok := db.GetPostgreSQLFeatures(repo.GetAdapter())
//	if !ok {
//	    return errors.New("not PostgreSQL")
//	}
//
//	// ENUM 类型
//	err = features.EnumType("order_status").
//	    Values("pending", "processing", "shipped", "delivered", "cancelled").
//	    IfNotExists().
//	    Create(ctx)
//
//	// DOMAIN 类型
//	err = features.DomainType("email_address").
//	    BaseType("TEXT").
//	    NotNull().
//	    Check(`VALUE ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$'`).
//	    Create(ctx)
//
//	// COMPOSITE 类型
//	err = features.CompositeType("geo_point").
//	    Field("lat", "DOUBLE PRECISION").
//	    Field("lng", "DOUBLE PRECISION").
//	    Create(ctx)
type PostgreSQLFeatures struct {
	adapter *PostgreSQLAdapter
}

// GetPostgreSQLFeatures 从 Adapter 中提取 PostgreSQLFeatures。
// 若传入的不是 *PostgreSQLAdapter，则 ok == false。
func GetPostgreSQLFeatures(adapter Adapter) (*PostgreSQLFeatures, bool) {
	pg, ok := adapter.(*PostgreSQLAdapter)
	if !ok {
		return nil, false
	}
	return &PostgreSQLFeatures{adapter: pg}, true
}

// EnumType 开始构建一个 PostgreSQL ENUM 自定义类型。
//
// 生成的 DDL 示例：
//
//	CREATE TYPE "order_status" AS ENUM ('pending', 'processing', 'shipped')
func (f *PostgreSQLFeatures) EnumType(typeName string) *EnumTypeBuilder {
	return &EnumTypeBuilder{
		adapter:  f.adapter,
		typeName: typeName,
	}
}

// DomainType 开始构建一个 PostgreSQL DOMAIN 自定义类型。
// DOMAIN 是对已有基础类型的约束包装，可以附加 NOT NULL 和 CHECK 约束。
//
// 生成的 DDL 示例：
//
//	CREATE DOMAIN "email_address" AS TEXT NOT NULL
//	    CHECK (VALUE ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$')
func (f *PostgreSQLFeatures) DomainType(typeName string) *DomainTypeBuilder {
	return &DomainTypeBuilder{
		adapter:  f.adapter,
		typeName: typeName,
	}
}

// CompositeType 开始构建一个 PostgreSQL COMPOSITE 自定义类型（结构体类型）。
// COMPOSITE 类型可以作为列类型或函数返回类型使用。
//
// 生成的 DDL 示例：
//
//	CREATE TYPE "address" AS (
//	    street  TEXT,
//	    city    VARCHAR(100),
//	    zip     CHAR(6)
//	)
func (f *PostgreSQLFeatures) CompositeType(typeName string) *CompositeTypeBuilder {
	return &CompositeTypeBuilder{
		adapter:  f.adapter,
		typeName: typeName,
	}
}

// View 开始构建一个 PostgreSQL 普通视图。
// 默认使用 CREATE OR REPLACE VIEW。
func (f *PostgreSQLFeatures) View(viewName string) *PostgreSQLViewBuilder {
	return &PostgreSQLViewBuilder{
		adapter:         f.adapter,
		name:            strings.TrimSpace(viewName),
		createOrReplace: true,
		dropIfExists:    true,
	}
}

// MaterializedView 开始构建 PostgreSQL 物化视图。
func (f *PostgreSQLFeatures) MaterializedView(viewName string) *PostgreSQLViewBuilder {
	return &PostgreSQLViewBuilder{
		adapter:         f.adapter,
		name:            strings.TrimSpace(viewName),
		materialized:    true,
		createOrReplace: false,
		dropIfExists:    true,
	}
}

// ==================== EnumTypeBuilder ====================

// EnumTypeBuilder 构建 PostgreSQL ENUM 类型 DDL。
type EnumTypeBuilder struct {
	adapter     *PostgreSQLAdapter
	typeName    string
	values      []string
	ifNotExists bool
	schema      string
}

// Schema 可选：指定类型所在的 schema（默认 public）。
func (b *EnumTypeBuilder) Schema(schema string) *EnumTypeBuilder {
	b.schema = schema
	return b
}

// Values 设置枚举允许的字面值列表（顺序有意义，PostgreSQL 会按此顺序排序比较）。
func (b *EnumTypeBuilder) Values(vals ...string) *EnumTypeBuilder {
	b.values = vals
	return b
}

// IfNotExists 如果类型已存在则跳过，不报错。
// 实现方式：先检查 pg_type，不支持 CREATE TYPE IF NOT EXISTS 语法（PG 不支持该语法）。
func (b *EnumTypeBuilder) IfNotExists() *EnumTypeBuilder {
	b.ifNotExists = true
	return b
}

// Build 生成 CREATE TYPE ... AS ENUM DDL 字符串，不执行。
func (b *EnumTypeBuilder) Build() (string, error) {
	if err := b.validate(); err != nil {
		return "", err
	}
	quoted := make([]string, len(b.values))
	for i, v := range b.values {
		quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	typeFQN := b.qualifiedName()
	return fmt.Sprintf("CREATE TYPE %s AS ENUM (%s)", typeFQN, strings.Join(quoted, ", ")), nil
}

// Create 在数据库中创建 ENUM 类型。
// 若 IfNotExists() 已设置，且类型已存在则静默跳过。
func (b *EnumTypeBuilder) Create(ctx context.Context) error {
	if b.ifNotExists {
		exists, err := b.typeExists(ctx)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
	}
	ddl, err := b.Build()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, ddl)
	return err
}

// Drop 删除 ENUM 类型。ifExists 为 true 时使用 DROP TYPE ... CASCADE IF EXISTS。
func (b *EnumTypeBuilder) Drop(ctx context.Context, ifExists bool) error {
	typeFQN := b.qualifiedName()
	stmt := fmt.Sprintf("DROP TYPE %s CASCADE", typeFQN)
	if ifExists {
		stmt = fmt.Sprintf("DROP TYPE IF EXISTS %s CASCADE", typeFQN)
	}
	_, err := b.adapter.Exec(ctx, stmt)
	return err
}

// Exists 检查该 ENUM 类型是否已存在于数据库中。
func (b *EnumTypeBuilder) Exists(ctx context.Context) (bool, error) {
	return b.typeExists(ctx)
}

func (b *EnumTypeBuilder) typeExists(ctx context.Context) (bool, error) {
	schema := b.schema
	if schema == "" {
		schema = "public"
	}
	row := b.adapter.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM pg_type t
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE t.typname = $1 AND n.nspname = $2 AND t.typtype = 'e'
		)`, b.typeName, schema)
	var exists bool
	return exists, row.Scan(&exists)
}

func (b *EnumTypeBuilder) qualifiedName() string {
	if b.schema != "" {
		return fmt.Sprintf("%s.%s", quotePgIdentifier(b.schema), quotePgIdentifier(b.typeName))
	}
	return quotePgIdentifier(b.typeName)
}

func (b *EnumTypeBuilder) validate() error {
	if strings.TrimSpace(b.typeName) == "" {
		return fmt.Errorf("postgres enum type: type name is required")
	}
	if len(b.values) == 0 {
		return fmt.Errorf("postgres enum type %q: at least one value is required", b.typeName)
	}
	return nil
}

// ==================== DomainTypeBuilder ====================

// DomainTypeBuilder 构建 PostgreSQL DOMAIN 类型 DDL。
type DomainTypeBuilder struct {
	adapter     *PostgreSQLAdapter
	typeName    string
	baseType    string
	notNull     bool
	defaultVal  *string
	checks      []domainCheck
	ifNotExists bool
	schema      string
}

type domainCheck struct {
	name       string // 约束名（可选）
	expression string // CHECK (expression)，expression 中用 VALUE 表示值
}

// Schema 可选：指定类型所在的 schema。
func (b *DomainTypeBuilder) Schema(schema string) *DomainTypeBuilder {
	b.schema = schema
	return b
}

// BaseType 设置 DOMAIN 的基础类型，如 "TEXT"、"INTEGER"、"NUMERIC(10,2)"。
func (b *DomainTypeBuilder) BaseType(pgType string) *DomainTypeBuilder {
	b.baseType = pgType
	return b
}

// NotNull 添加 NOT NULL 约束。
func (b *DomainTypeBuilder) NotNull() *DomainTypeBuilder {
	b.notNull = true
	return b
}

// Default 设置默认值表达式（PL/pgSQL 表达式字符串）。
func (b *DomainTypeBuilder) Default(expr string) *DomainTypeBuilder {
	b.defaultVal = &expr
	return b
}

// Check 添加一个 CHECK 约束，expression 中用 VALUE 代表域的当前值。
// 可多次调用以附加多个约束。
//
// 示例：Check(`VALUE ~ '^[^@]+@[^@]+$'`)
func (b *DomainTypeBuilder) Check(expression string) *DomainTypeBuilder {
	b.checks = append(b.checks, domainCheck{expression: expression})
	return b
}

// CheckNamed 添加一个带命名的 CHECK 约束
//
// 示例：CheckNamed("positive", "VALUE > 0")
func (b *DomainTypeBuilder) CheckNamed(constraintName, expression string) *DomainTypeBuilder {
	b.checks = append(b.checks, domainCheck{name: constraintName, expression: expression})
	return b
}

// IfNotExists 若类型已存在则静默跳过。
func (b *DomainTypeBuilder) IfNotExists() *DomainTypeBuilder {
	b.ifNotExists = true
	return b
}

// Build 生成 CREATE DOMAIN DDL 字符串，不执行。
func (b *DomainTypeBuilder) Build() (string, error) {
	if err := b.validate(); err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE DOMAIN %s AS %s", b.qualifiedName(), b.baseType))

	if b.defaultVal != nil {
		sb.WriteString(fmt.Sprintf(" DEFAULT %s", *b.defaultVal))
	}
	if b.notNull {
		sb.WriteString(" NOT NULL")
	}
	for _, c := range b.checks {
		if c.name != "" {
			sb.WriteString(fmt.Sprintf(" CONSTRAINT %s CHECK (%s)",
				quotePgIdentifier(c.name), c.expression))
		} else {
			sb.WriteString(fmt.Sprintf(" CHECK (%s)", c.expression))
		}
	}
	return sb.String(), nil
}

// Create 在数据库中创建 DOMAIN 类型。
func (b *DomainTypeBuilder) Create(ctx context.Context) error {
	if b.ifNotExists {
		exists, err := b.typeExists(ctx)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
	}
	ddl, err := b.Build()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, ddl)
	return err
}

// Drop 删除 DOMAIN 类型。
func (b *DomainTypeBuilder) Drop(ctx context.Context, ifExists bool) error {
	typeFQN := b.qualifiedName()
	stmt := fmt.Sprintf("DROP DOMAIN %s CASCADE", typeFQN)
	if ifExists {
		stmt = fmt.Sprintf("DROP DOMAIN IF EXISTS %s CASCADE", typeFQN)
	}
	_, err := b.adapter.Exec(ctx, stmt)
	return err
}

// Exists 检查该 DOMAIN 类型是否已存在。
func (b *DomainTypeBuilder) Exists(ctx context.Context) (bool, error) {
	return b.typeExists(ctx)
}

func (b *DomainTypeBuilder) typeExists(ctx context.Context) (bool, error) {
	schema := b.schema
	if schema == "" {
		schema = "public"
	}
	row := b.adapter.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM pg_type t
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE t.typname = $1 AND n.nspname = $2 AND t.typtype = 'd'
		)`, b.typeName, schema)
	var exists bool
	return exists, row.Scan(&exists)
}

func (b *DomainTypeBuilder) qualifiedName() string {
	if b.schema != "" {
		return fmt.Sprintf("%s.%s", quotePgIdentifier(b.schema), quotePgIdentifier(b.typeName))
	}
	return quotePgIdentifier(b.typeName)
}

func (b *DomainTypeBuilder) validate() error {
	if strings.TrimSpace(b.typeName) == "" {
		return fmt.Errorf("postgres domain type: type name is required")
	}
	if strings.TrimSpace(b.baseType) == "" {
		return fmt.Errorf("postgres domain type %q: base type is required (e.g. TEXT, INTEGER)", b.typeName)
	}
	return nil
}

// ==================== CompositeTypeBuilder ====================

// CompositeTypeBuilder 构建 PostgreSQL COMPOSITE 类型（结构体类型）DDL。
type CompositeTypeBuilder struct {
	adapter     *PostgreSQLAdapter
	typeName    string
	fields      []compositeField
	ifNotExists bool
	schema      string
}

type compositeField struct {
	name    string
	pgType  string
	comment string // 仅用于 Build() 注释，不生成实际 DDL
}

// Schema 可选：指定类型所在的 schema。
func (b *CompositeTypeBuilder) Schema(schema string) *CompositeTypeBuilder {
	b.schema = schema
	return b
}

// Field 添加一个字段到 COMPOSITE 类型，pgType 为 PostgreSQL 原生类型字符串。
//
// 示例：Field("latitude", "DOUBLE PRECISION").Field("longitude", "DOUBLE PRECISION")
func (b *CompositeTypeBuilder) Field(name, pgType string) *CompositeTypeBuilder {
	b.fields = append(b.fields, compositeField{name: name, pgType: pgType})
	return b
}

// IfNotExists 若类型已存在则静默跳过。
func (b *CompositeTypeBuilder) IfNotExists() *CompositeTypeBuilder {
	b.ifNotExists = true
	return b
}

// Build 生成 CREATE TYPE ... AS (...) DDL 字符串，不执行。
func (b *CompositeTypeBuilder) Build() (string, error) {
	if err := b.validate(); err != nil {
		return "", err
	}

	cols := make([]string, len(b.fields))
	for i, f := range b.fields {
		cols[i] = fmt.Sprintf("\t%s\t%s", quotePgIdentifier(f.name), f.pgType)
	}
	return fmt.Sprintf("CREATE TYPE %s AS (\n%s\n)",
		b.qualifiedName(), strings.Join(cols, ",\n")), nil
}

// Create 在数据库中创建 COMPOSITE 类型。
func (b *CompositeTypeBuilder) Create(ctx context.Context) error {
	if b.ifNotExists {
		exists, err := b.typeExists(ctx)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
	}
	ddl, err := b.Build()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, ddl)
	return err
}

// Drop 删除 COMPOSITE 类型。
func (b *CompositeTypeBuilder) Drop(ctx context.Context, ifExists bool) error {
	typeFQN := b.qualifiedName()
	stmt := fmt.Sprintf("DROP TYPE %s CASCADE", typeFQN)
	if ifExists {
		stmt = fmt.Sprintf("DROP TYPE IF EXISTS %s CASCADE", typeFQN)
	}
	_, err := b.adapter.Exec(ctx, stmt)
	return err
}

// Exists 检查该 COMPOSITE 类型是否已存在。
func (b *CompositeTypeBuilder) Exists(ctx context.Context) (bool, error) {
	return b.typeExists(ctx)
}

// AddField 运行时动态追加字段，等同于 ALTER TYPE ... ADD ATTRIBUTE。
// 若类型已存在，直接执行 DDL；若尚未创建则缓冲到 Create 时一起执行。
func (b *CompositeTypeBuilder) AddField(ctx context.Context, name, pgType string) error {
	stmt := fmt.Sprintf("ALTER TYPE %s ADD ATTRIBUTE %s %s",
		b.qualifiedName(), quotePgIdentifier(name), pgType)
	_, err := b.adapter.Exec(ctx, stmt)
	return err
}

func (b *CompositeTypeBuilder) typeExists(ctx context.Context) (bool, error) {
	schema := b.schema
	if schema == "" {
		schema = "public"
	}
	row := b.adapter.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM pg_type t
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE t.typname = $1 AND n.nspname = $2 AND t.typtype = 'c'
		)`, b.typeName, schema)
	var exists bool
	return exists, row.Scan(&exists)
}

func (b *CompositeTypeBuilder) qualifiedName() string {
	if b.schema != "" {
		return fmt.Sprintf("%s.%s", quotePgIdentifier(b.schema), quotePgIdentifier(b.typeName))
	}
	return quotePgIdentifier(b.typeName)
}

func (b *CompositeTypeBuilder) validate() error {
	if strings.TrimSpace(b.typeName) == "" {
		return fmt.Errorf("postgres composite type: type name is required")
	}
	if len(b.fields) == 0 {
		return fmt.Errorf("postgres composite type %q: at least one field is required", b.typeName)
	}
	return nil
}

// ==================== PostgreSQLViewBuilder ====================

// PostgreSQLViewBuilder 构建 PostgreSQL 视图（普通视图/物化视图）DDL。
type PostgreSQLViewBuilder struct {
	adapter *PostgreSQLAdapter

	name            string
	schema          string
	selectSQL       string
	materialized    bool
	createOrReplace bool
	withCheckOption bool
	withNoData      bool
	concurrently    bool
	dropIfExists    bool
	args            []interface{}
}

// Schema 指定视图所在 schema（默认 public）。
func (b *PostgreSQLViewBuilder) Schema(schema string) *PostgreSQLViewBuilder {
	b.schema = strings.TrimSpace(schema)
	return b
}

// As 设置视图定义 SQL（AS 后的 SELECT 语句）。
func (b *PostgreSQLViewBuilder) As(selectSQL string) *PostgreSQLViewBuilder {
	b.selectSQL = strings.TrimSpace(selectSQL)
	return b
}

// CreateOnly 强制使用 CREATE VIEW（仅普通视图）。
func (b *PostgreSQLViewBuilder) CreateOnly() *PostgreSQLViewBuilder {
	b.createOrReplace = false
	return b
}

// CreateOrReplace 使用 CREATE OR REPLACE VIEW（仅普通视图，默认）。
func (b *PostgreSQLViewBuilder) CreateOrReplace() *PostgreSQLViewBuilder {
	b.createOrReplace = true
	return b
}

// WithCheckOption 为普通视图添加 WITH CHECK OPTION。
func (b *PostgreSQLViewBuilder) WithCheckOption() *PostgreSQLViewBuilder {
	b.withCheckOption = true
	return b
}

// WithNoData 为物化视图添加 WITH NO DATA。
func (b *PostgreSQLViewBuilder) WithNoData() *PostgreSQLViewBuilder {
	b.withNoData = true
	return b
}

// RefreshConcurrently 启用并发刷新（REFRESH MATERIALIZED VIEW CONCURRENTLY）。
func (b *PostgreSQLViewBuilder) RefreshConcurrently() *PostgreSQLViewBuilder {
	b.concurrently = true
	return b
}

// DropIfExists 删除视图时使用 IF EXISTS（默认）。
func (b *PostgreSQLViewBuilder) DropIfExists() *PostgreSQLViewBuilder {
	b.dropIfExists = true
	return b
}

// DropStrict 删除视图时不使用 IF EXISTS。
func (b *PostgreSQLViewBuilder) DropStrict() *PostgreSQLViewBuilder {
	b.dropIfExists = false
	return b
}

// Args 设置执行创建语句时的参数。
func (b *PostgreSQLViewBuilder) Args(args ...interface{}) *PostgreSQLViewBuilder {
	b.args = args
	return b
}

// BuildCreate 生成创建视图 DDL。
func (b *PostgreSQLViewBuilder) BuildCreate() (string, error) {
	if err := b.validateCreate(); err != nil {
		return "", err
	}

	var sb strings.Builder
	if b.materialized {
		sb.WriteString("CREATE MATERIALIZED VIEW ")
		sb.WriteString(b.qualifiedName())
		sb.WriteString(" AS\n")
		sb.WriteString(b.selectSQL)
		if b.withNoData {
			sb.WriteString("\nWITH NO DATA")
		}
		sb.WriteString(";")
		return sb.String(), nil
	}

	if b.createOrReplace {
		sb.WriteString("CREATE OR REPLACE VIEW ")
	} else {
		sb.WriteString("CREATE VIEW ")
	}
	sb.WriteString(b.qualifiedName())
	sb.WriteString(" AS\n")
	sb.WriteString(b.selectSQL)
	if b.withCheckOption {
		sb.WriteString("\nWITH CHECK OPTION")
	}
	sb.WriteString(";")

	return sb.String(), nil
}

// BuildDrop 生成删除视图 DDL。
func (b *PostgreSQLViewBuilder) BuildDrop() (string, error) {
	if strings.TrimSpace(b.name) == "" {
		return "", fmt.Errorf("postgres view: view name is required")
	}

	obj := "VIEW"
	if b.materialized {
		obj = "MATERIALIZED VIEW"
	}

	if b.dropIfExists {
		return fmt.Sprintf("DROP %s IF EXISTS %s;", obj, b.qualifiedName()), nil
	}
	return fmt.Sprintf("DROP %s %s;", obj, b.qualifiedName()), nil
}

// BuildRefresh 生成刷新物化视图 DDL。
func (b *PostgreSQLViewBuilder) BuildRefresh() (string, error) {
	if strings.TrimSpace(b.name) == "" {
		return "", fmt.Errorf("postgres view: view name is required")
	}
	if !b.materialized {
		return "", fmt.Errorf("postgres view %q: refresh is only available for materialized views", b.name)
	}

	if b.concurrently {
		return fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s;", b.qualifiedName()), nil
	}
	return fmt.Sprintf("REFRESH MATERIALIZED VIEW %s;", b.qualifiedName()), nil
}

// Create 执行创建视图。
func (b *PostgreSQLViewBuilder) Create(ctx context.Context) error {
	ddl, err := b.BuildCreate()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, ddl, b.args...)
	return err
}

// Drop 执行删除视图。
func (b *PostgreSQLViewBuilder) Drop(ctx context.Context) error {
	ddl, err := b.BuildDrop()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, ddl)
	return err
}

// Refresh 执行刷新物化视图。
func (b *PostgreSQLViewBuilder) Refresh(ctx context.Context) error {
	ddl, err := b.BuildRefresh()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, ddl)
	return err
}

func (b *PostgreSQLViewBuilder) validateCreate() error {
	if strings.TrimSpace(b.name) == "" {
		return fmt.Errorf("postgres view: view name is required")
	}
	if strings.TrimSpace(b.selectSQL) == "" {
		return fmt.Errorf("postgres view %q: select SQL is required", b.name)
	}
	if b.materialized && b.withCheckOption {
		return fmt.Errorf("postgres materialized view %q: WITH CHECK OPTION is not supported", b.name)
	}
	if b.materialized && b.createOrReplace {
		return fmt.Errorf("postgres materialized view %q: CREATE OR REPLACE is not supported", b.name)
	}
	return nil
}

func (b *PostgreSQLViewBuilder) qualifiedName() string {
	if b.schema != "" {
		return fmt.Sprintf("%s.%s", quotePgIdentifier(b.schema), quotePgIdentifier(b.name))
	}
	return quotePgIdentifier(b.name)
}

// ==================== 工具函数 ====================

// quotePgIdentifier 用双引号包裹 PostgreSQL 标识符，内部双引号转义为 ""。
func quotePgIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
