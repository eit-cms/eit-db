package db

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"time"
)

// FieldType 字段类型定义
type FieldType string

const (
	TypeString   FieldType = "string"
	TypeInteger  FieldType = "integer"
	TypeFloat    FieldType = "float"
	TypeBoolean  FieldType = "boolean"
	TypeTime     FieldType = "time"
	TypeBinary   FieldType = "binary"
	TypeDecimal  FieldType = "decimal"
	TypeMap      FieldType = "map"
	TypeArray    FieldType = "array"
	TypeJSON     FieldType = "json"
	TypeLocation FieldType = "location"
)

// Field 定义模式中的字段
type Field struct {
	Name         string
	Type         FieldType
	Default      interface{}
	Null         bool
	Primary      bool
	Autoinc      bool
	Index        bool
	Unique       bool
	Validators   []Validator
	Transformers []Transformer
}

// ConstraintKind 表级约束类型
type ConstraintKind string

const (
	ConstraintPrimaryKey ConstraintKind = "primary_key"
	ConstraintUnique     ConstraintKind = "unique"
	ConstraintForeignKey ConstraintKind = "foreign_key"
)

// TableConstraint 表级约束定义（用于复合主键、复合唯一约束、复合外键等）
type TableConstraint struct {
	Name   string
	Kind   ConstraintKind
	Fields []string
	// 外键专属字段（Kind == ConstraintForeignKey 时有效）
	RefTable  string   // 被引用表名
	RefFields []string // 被引用列名（顺序与 Fields 对应）
	OnDelete  string   // "CASCADE" | "SET NULL" | "RESTRICT" | "NO ACTION" | ""
	OnUpdate  string   // "CASCADE" | "SET NULL" | "RESTRICT" | "NO ACTION" | ""
	// Neo4jRelType 可选：在 Neo4j 中对应的关系类型（如 "FOLLOWS"、"OWNS"）。
	// 仅当 Kind == ConstraintForeignKey 时有效。
	// 空时由 Neo4j 编译器根据约束名推断（去除 "fk_"/"rel_" 等常见前缀后转大写）。
	Neo4jRelType string
	// 可选：外键热点查询视图声明（仅当 Kind == ConstraintForeignKey 时有意义）
	ViewHint *ViewHint
}

// ViewHint 外键约束上的视图提示，声明该关联应创建（或复用）的跨表视图。
// 适用于高热点跨表查询场景，由 Migration 阶段自动生成视图 DDL，
// 运行时由 QueryBuilder 自动路由到视图而非直接 JOIN。
type ViewHint struct {
	// ViewName 视图名称；空时自动生成为 "<localTable>_<refTable>_view"。
	ViewName string
	// Materialized 是否物化视图（PostgreSQL MATERIALIZED VIEW）。
	// SQL Server 忽略此标志，始终创建普通视图。
	Materialized bool
	// Columns 视图 SELECT 的列表达式列表；空表示 "local_alias.*, ref_alias.*"。
	// 建议指定明确的列以避免歧义，例如 []string{"o.id AS order_id", "u.name"}。
	Columns []string
	// JoinType LEFT/RIGHT/INNER；空时默认为 INNER。
	JoinType string
}

// FKOption 外键约束选项（函数选项模式）。
type FKOption func(*TableConstraint)

// WithViewHint 为外键约束附加视图提示，声明该关联的热点查询视图。
//
//	// 创建 user_orders_view，查询 orders JOIN users：
//	schema.AddForeignKey("fk_orders_users",
//		[]string{"user_id"}, "users", []string{"id"}, "CASCADE", "",
//		WithViewHint("user_orders_view", false))
func WithViewHint(viewName string, materialized bool, columns ...string) FKOption {
	return func(tc *TableConstraint) {
		tc.ViewHint = &ViewHint{
			ViewName:     viewName,
			Materialized: materialized,
			Columns:      append([]string(nil), columns...),
		}
	}
}

// WithJoinType 为外键约束的视图提示指定 JOIN 类型（"LEFT" / "INNER" / "RIGHT"）。
// 仅当已通过 WithViewHint 设置视图提示时才有效。
func WithJoinType(joinType string) FKOption {
	return func(tc *TableConstraint) {
		if tc.ViewHint == nil {
			return
		}
		tc.ViewHint.JoinType = strings.ToUpper(strings.TrimSpace(joinType))
	}
}

// Schema 定义数据模式接口 (参考 Ecto.Schema)
type Schema interface {
	// 获取模式名称（表名）
	TableName() string

	// 获取所有字段
	Fields() []*Field

	// 获取字段
	GetField(name string) *Field

	// 获取主键字段
	PrimaryKeyField() *Field
}

// ConstrainedSchema 扩展 Schema，允许访问表级约束（外键、唯一约束等）。
// BaseSchema 实现此接口；外部自定义 Schema 实现可选择性实现。
type ConstrainedSchema interface {
	Schema
	Constraints() []TableConstraint
}

// ─── 关系注册表 ───────────────────────────────────────────────────────────────

// Schema 关系类型常量（ORM 风格，用于 SchemaRelation 关系注册表）。
// 使用与 relationship.go 中相同的 RelationType 类型，添加 HasMany/HasOne/BelongsTo 语义。
const (
	// RelationHasMany 一对多：本 Schema 是"一"侧，目标 Schema 持有外键（FK 在目标侧）。
	// 连接时默认 optional（本实体可能有 0 个目标实体）。
	RelationHasMany RelationType = "has_many"

	// RelationHasOne 一对一：本 Schema 是"一"侧，目标 Schema 持有外键（FK 在目标侧）。
	// 连接时默认 optional（本实体可能没有对应目标实体）。
	RelationHasOne RelationType = "has_one"

	// RelationBelongsTo 多对一：本 Schema 持有外键（FK 在本侧），目标 Schema 是"一"侧。
	// 连接时默认 required（本实体必须有对应父实体）。
	RelationBelongsTo RelationType = "belongs_to"

	// RelationManyToMany 多对多：通常需要 Through 中间关系（中间表/中间边）。
	// 连接时默认 optional。
	RelationManyToMany RelationType = ManyToMany
)

// RelationThrough 声明多对多关系的中间关系信息。
//
// SQL 可将 Table 视为中间表（可做递归 CTE / join 表优化）；
// Neo4j 可将其映射为边关系类型或中间节点；
// Mongo 可将其用于两段 $lookup 管道。
type RelationThrough struct {
	Schema    Schema // 中间关系 Schema（优先）
	Table     string // 中间关系表/集合名（Schema 为空时兜底）
	SourceKey string // source -> through 的连接键
	TargetKey string // through -> target 的连接键
}

// SchemaRelation 声明两个 Schema 之间的关系。
//
// 字段语义：
//   - HasMany / HasOne：FK 在目标 Schema 侧；ForeignKey 是目标侧字段，OriginKey 是本侧字段（通常是 PK）。
//   - BelongsTo：FK 在本 Schema 侧；ForeignKey 是本侧字段，OriginKey 是目标侧字段（通常是 PK）。
//
// 适配器行为：
//   - SQL：自然使用 FK 约束；关系声明补充语义推断。
//   - Neo4j：ForeignKey/OriginKey 是逻辑字段，关系以边（edge）存储；不物化为节点属性。
//   - MongoDB：适配器根据 ForeignKey/OriginKey 生成 $lookup 管道；字段是否物化由适配器自决。
type SchemaRelation struct {
	Type         RelationType
	TargetSchema Schema
	Name         string // 关系名称（适配器可据此映射关系类型/边类型）
	ForeignKey   string // 持有 FK 的一侧字段名
	OriginKey    string // 被引用的一侧字段名（通常是 PK）
	Through      *RelationThrough
	Reversible   bool  // 一对一可逆标记（或显式声明允许反向路径优化）
	optional     *bool  // nil = 按 RelationType 默认；true/false 显式覆盖
}

// Semantic 根据关系类型和显式覆盖推断 JoinSemantic。
func (r *SchemaRelation) Semantic() JoinSemantic {
	if r.optional != nil {
		if *r.optional {
			return JoinSemanticOptional
		}
		return JoinSemanticRequired
	}
	if r.Type == RelationBelongsTo {
		return JoinSemanticRequired
	}
	return JoinSemanticOptional
}

// SemanticFromSource 从"源"的视角推断语义：
// 当 source 连接到 join Schema，而 join Schema 声明了指向 source 的关系时使用。
// 规则：join BelongsTo source → source 视角为 optional（source 可能有 0 个 join）
//       join HasMany/HasOne source → source 视角为 required（source 是 join 的父实体，join 必有 FK）
func (r *SchemaRelation) SemanticFromSource() JoinSemantic {
	if r.optional != nil {
		if *r.optional {
			return JoinSemanticOptional
		}
		return JoinSemanticRequired
	}
	switch r.Type {
	case RelationBelongsTo:
		return JoinSemanticOptional
	case RelationManyToMany:
		return JoinSemanticOptional
	default:
		return JoinSemanticRequired
	}
}

// SchemaRelationBuilder 关系声明的流式构造器，通过 HasMany/HasOne/BelongsTo 创建。
type SchemaRelationBuilder struct {
	rel *SchemaRelation
}

// Over 声明关联字段。
// foreignKey 是持有 FK 的一侧字段名；originKey 是被引用的一侧字段名（通常是 PK）。
// HasMany/HasOne：foreignKey 在目标侧，originKey 在本侧。
// BelongsTo：foreignKey 在本侧，originKey 在目标侧。
func (b *SchemaRelationBuilder) Over(foreignKey, originKey string) *SchemaRelationBuilder {
	b.rel.ForeignKey = strings.TrimSpace(foreignKey)
	b.rel.OriginKey = strings.TrimSpace(originKey)
	return b
}

// Through 声明多对多关系使用的中间关系（中间表/集合/边）。
// sourceKey 表示 source -> through 的连接键，targetKey 表示 through -> target 的连接键。
func (b *SchemaRelationBuilder) Through(through Schema, sourceKey, targetKey string) *SchemaRelationBuilder {
	name := ""
	if through != nil {
		name = strings.TrimSpace(through.TableName())
	}
	b.rel.Through = &RelationThrough{
		Schema:    through,
		Table:     name,
		SourceKey: strings.TrimSpace(sourceKey),
		TargetKey: strings.TrimSpace(targetKey),
	}
	return b
}

// Named 为关系声明一个语义名称。
// 适配器可将其映射为底层关系类型（例如 Neo4j 边类型）。
func (b *SchemaRelationBuilder) Named(name string) *SchemaRelationBuilder {
	b.rel.Name = strings.TrimSpace(name)
	return b
}

// Reversible 标记该关系允许反向路径优化（典型用于一对一）。
func (b *SchemaRelationBuilder) Reversible(enabled bool) *SchemaRelationBuilder {
	b.rel.Reversible = enabled
	return b
}

// Optional 显式覆盖为 optional 语义（不论关系类型默认值）。
func (b *SchemaRelationBuilder) Optional() *SchemaRelationBuilder {
	t := true
	b.rel.optional = &t
	return b
}

// Required 显式覆盖为 required 语义（不论关系类型默认值）。
func (b *SchemaRelationBuilder) Required() *SchemaRelationBuilder {
	f := false
	b.rel.optional = &f
	return b
}

// Relation 返回构建好的 SchemaRelation（主要用于测试断言）。
func (b *SchemaRelationBuilder) Relation() *SchemaRelation {
	return b.rel
}

// RelationalSchema 扩展 Schema，支持显式关系注册（HasMany / HasOne / BelongsTo）。
// BaseSchema 实现此接口；关系注册优先于 FK 约束用于连接语义推断和跨集合关联。
type RelationalSchema interface {
	Schema
	Relations() []SchemaRelation
	FindRelation(targetTable string) *SchemaRelation
}

// ─── BaseSchema ───────────────────────────────────────────────────────────────

// BaseSchema 基础模式实现
type BaseSchema struct {
	tableName   string
	fields      map[string]*Field
	fieldList   []*Field
	constraints []TableConstraint
	relations   []SchemaRelation // 关系注册表
}

// NewBaseSchema 创建基础模式
func NewBaseSchema(tableName string) *BaseSchema {
	return &BaseSchema{
		tableName:   tableName,
		fields:      make(map[string]*Field),
		fieldList:   make([]*Field, 0),
		constraints: make([]TableConstraint, 0),
		relations:   make([]SchemaRelation, 0),
	}
}

// TableName 返回表名
func (s *BaseSchema) TableName() string {
	return s.tableName
}

// AddField 添加字段
func (s *BaseSchema) AddField(field *Field) *BaseSchema {
	s.fields[field.Name] = field
	s.fieldList = append(s.fieldList, field)
	return s
}

// AddTimestamps 添加常用时间戳字段 created_at / updated_at。
// 默认行为：
// - 字段类型为 TypeTime
// - 非空（NOT NULL）
// - 默认值为 CURRENT_TIMESTAMP
// - 若字段已存在则跳过，避免重复添加
func (s *BaseSchema) AddTimestamps() *BaseSchema {
	if s.GetField("created_at") == nil {
		s.AddField(NewField("created_at", TypeTime).
			Null(false).
			Default("CURRENT_TIMESTAMP").
			Build())
	}

	if s.GetField("updated_at") == nil {
		s.AddField(NewField("updated_at", TypeTime).
			Null(false).
			Default("CURRENT_TIMESTAMP").
			Build())
	}

	return s
}

// AddSoftDelete 添加软删除字段 deleted_at。
// 默认行为：
// - 字段类型为 TypeTime
// - 可空（NULL）
// - 若字段已存在则跳过，避免重复添加
func (s *BaseSchema) AddSoftDelete() *BaseSchema {
	if s.GetField("deleted_at") == nil {
		s.AddField(NewField("deleted_at", TypeTime).Null(true).Build())
	}

	return s
}

// Fields 返回所有字段
func (s *BaseSchema) Fields() []*Field {
	return s.fieldList
}

// GetField 获取字段
func (s *BaseSchema) GetField(name string) *Field {
	return s.fields[name]
}

// PrimaryKeyField 获取主键字段
func (s *BaseSchema) PrimaryKeyField() *Field {
	for _, field := range s.fieldList {
		if field.Primary {
			return field
		}
	}
	return nil
}

// Constraints 返回所有表级约束
func (s *BaseSchema) Constraints() []TableConstraint {
	return append([]TableConstraint(nil), s.constraints...)
}

// ─── 关系注册表方法 ────────────────────────────────────────────────────────────

// HasMany 声明本 Schema 是"一"侧，目标 Schema 持有外键（一对多）。
// 连接语义默认 optional（本实体可能有 0 个目标实体）。
// 流式调用 .Over(foreignKey, originKey) 可声明关联字段；.Required()/.Optional() 可覆盖默认语义。
//
//	userSchema.HasMany(orderSchema).Over("user_id", "id")
func (s *BaseSchema) HasMany(target Schema) *SchemaRelationBuilder {
	s.relations = append(s.relations, SchemaRelation{Type: RelationHasMany, TargetSchema: target})
	return &SchemaRelationBuilder{rel: &s.relations[len(s.relations)-1]}
}

// HasOne 声明本 Schema 是"一"侧，目标 Schema 持有外键（一对一）。
// 连接语义默认 optional（本实体可能没有对应目标实体）。
//
//	userSchema.HasOne(profileSchema).Over("user_id", "id")
func (s *BaseSchema) HasOne(target Schema) *SchemaRelationBuilder {
	s.relations = append(s.relations, SchemaRelation{Type: RelationHasOne, TargetSchema: target})
	return &SchemaRelationBuilder{rel: &s.relations[len(s.relations)-1]}
}

// BelongsTo 声明本 Schema 持有外键，归属于目标 Schema（多对一）。
// 连接语义默认 required（本实体必须有对应父实体）。
//
//	orderSchema.BelongsTo(userSchema).Over("user_id", "id")
func (s *BaseSchema) BelongsTo(target Schema) *SchemaRelationBuilder {
	s.relations = append(s.relations, SchemaRelation{Type: RelationBelongsTo, TargetSchema: target})
	return &SchemaRelationBuilder{rel: &s.relations[len(s.relations)-1]}
}

// ManyToMany 声明多对多关系。
// 推荐配合 Through 指定中间关系（中间表/集合/边），便于各适配器做深度优化。
//
//	userSchema.ManyToMany(roleSchema).Through(userRoleSchema, "user_id", "role_id")
func (s *BaseSchema) ManyToMany(target Schema) *SchemaRelationBuilder {
	s.relations = append(s.relations, SchemaRelation{Type: RelationManyToMany, TargetSchema: target})
	return &SchemaRelationBuilder{rel: &s.relations[len(s.relations)-1]}
}

// Relations 返回本 Schema 上所有已声明的关系。
func (s *BaseSchema) Relations() []SchemaRelation {
	return append([]SchemaRelation(nil), s.relations...)
}

// FindRelation 查找本 Schema 与目标表名之间的第一个关系声明；未找到时返回 nil。
func (s *BaseSchema) FindRelation(targetTable string) *SchemaRelation {
	for i := range s.relations {
		if s.relations[i].TargetSchema != nil &&
			strings.EqualFold(s.relations[i].TargetSchema.TableName(), targetTable) {
			return &s.relations[i]
		}
	}
	return nil
}

// AddPrimaryKey 添加表级主键约束（支持复合主键）
func (s *BaseSchema) AddPrimaryKey(fields ...string) *BaseSchema {
	normalized := normalizeConstraintFields(fields)
	if len(normalized) == 0 {
		return s
	}

	s.constraints = append(s.constraints, TableConstraint{
		Kind:   ConstraintPrimaryKey,
		Fields: normalized,
	})

	return s
}

// AddUniqueConstraint 添加表级唯一约束（支持复合唯一）
func (s *BaseSchema) AddUniqueConstraint(name string, fields ...string) *BaseSchema {
	normalized := normalizeConstraintFields(fields)
	if len(normalized) == 0 {
		return s
	}

	s.constraints = append(s.constraints, TableConstraint{
		Name:   name,
		Kind:   ConstraintUnique,
		Fields: normalized,
	})

	return s
}

// AddForeignKey 添加表级外键约束（支持复合外键）。
// onDelete/onUpdate 可选值："CASCADE", "SET NULL", "RESTRICT", "NO ACTION", ""（使用数据库默认）。
// opts 可选附加 WithViewHint / WithJoinType 等函数选项以声明热点查询视图。
func (s *BaseSchema) AddForeignKey(name string, localFields []string, refTable string, refFields []string, onDelete, onUpdate string, opts ...FKOption) *BaseSchema {
	normalized := normalizeConstraintFields(localFields)
	if len(normalized) == 0 || refTable == "" {
		return s
	}
	normalizedRef := normalizeConstraintFields(refFields)
	tc := TableConstraint{
		Name:      name,
		Kind:      ConstraintForeignKey,
		Fields:    normalized,
		RefTable:  refTable,
		RefFields: normalizedRef,
		OnDelete:  onDelete,
		OnUpdate:  onUpdate,
	}
	for _, opt := range opts {
		opt(&tc)
	}
	s.constraints = append(s.constraints, tc)
	return s
}

func normalizeConstraintFields(fields []string) []string {
	result := make([]string, 0, len(fields))
	seen := make(map[string]struct{})
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		result = append(result, field)
	}

	return result
}

// FieldBuilder 字段构造器
type FieldBuilder struct {
	field *Field
}

// NewField 创建新字段
func NewField(name string, fieldType FieldType) *FieldBuilder {
	return &FieldBuilder{
		field: &Field{
			Name:         name,
			Type:         fieldType,
			Validators:   make([]Validator, 0),
			Transformers: make([]Transformer, 0),
		},
	}
}

// Default 设置默认值
func (fb *FieldBuilder) Default(value interface{}) *FieldBuilder {
	fb.field.Default = value
	return fb
}

// Null 设置是否允许为空
func (fb *FieldBuilder) Null(allow bool) *FieldBuilder {
	fb.field.Null = allow
	return fb
}

// PrimaryKey 标记为主键
func (fb *FieldBuilder) PrimaryKey() *FieldBuilder {
	fb.field.Primary = true
	fb.field.Autoinc = true
	return fb
}

// Index 添加索引
func (fb *FieldBuilder) Index() *FieldBuilder {
	fb.field.Index = true
	return fb
}

// Unique 添加唯一约束
func (fb *FieldBuilder) Unique() *FieldBuilder {
	fb.field.Unique = true
	return fb
}

// Validate 添加验证器
func (fb *FieldBuilder) Validate(validator Validator) *FieldBuilder {
	fb.field.Validators = append(fb.field.Validators, validator)
	return fb
}

// Transform 添加转换器
func (fb *FieldBuilder) Transform(transformer Transformer) *FieldBuilder {
	fb.field.Transformers = append(fb.field.Transformers, transformer)
	return fb
}

// Build 构建字段
func (fb *FieldBuilder) Build() *Field {
	return fb.field
}

// Validator 验证器接口
type Validator interface {
	// 验证值，返回错误信息或 nil
	Validate(value interface{}) error
}

// Transformer 转换器接口
type Transformer interface {
	// 转换值
	Transform(value interface{}) (interface{}, error)
}

// 内置验证器

// RequiredValidator 必填验证器
type RequiredValidator struct{}

func (v *RequiredValidator) Validate(value interface{}) error {
	if value == nil || value == "" {
		return NewValidationError("required", "字段为必填项")
	}
	return nil
}

// LengthValidator 长度验证器
type LengthValidator struct {
	Min int
	Max int
}

func (v *LengthValidator) Validate(value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return NewValidationError("length", "字段类型必须为字符串")
	}

	len := len(str)
	if v.Min > 0 && len < v.Min {
		return NewValidationError("length", "字段长度不能小于 "+string(rune(v.Min)))
	}
	if v.Max > 0 && len > v.Max {
		return NewValidationError("length", "字段长度不能大于 "+string(rune(v.Max)))
	}
	return nil
}

// PatternValidator 正则验证器
type PatternValidator struct {
	Pattern string
}

func (v *PatternValidator) Validate(value interface{}) error {
	// 实现正则验证
	return nil
}

// UniqueValidator 唯一性验证器
type UniqueValidator struct {
	Schema Schema
	Field  string
}

func (v *UniqueValidator) Validate(value interface{}) error {
	// 需要从数据库查询
	return nil
}

// 内置转换器

// TrimTransformer 字符串修剪转换器
type TrimTransformer struct{}

func (t *TrimTransformer) Transform(value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return value, nil
	}
	// 修剪字符串
	return str, nil
}

// LowercaseTransformer 小写转换器
type LowercaseTransformer struct{}

func (t *LowercaseTransformer) Transform(value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return value, nil
	}
	// 转换为小写
	return str, nil
}

// TypeConversionError 类型转换错误
type TypeConversionError struct {
	From string
	To   string
}

func (e *TypeConversionError) Error() string {
	return "cannot convert " + e.From + " to " + e.To
}

// ConvertValue 值类型转换
func ConvertValue(value interface{}, targetType FieldType) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch targetType {
	case TypeString:
		return valueToString(value), nil
	case TypeInteger:
		return valueToInt64(value)
	case TypeFloat:
		return valueToFloat64(value)
	case TypeBoolean:
		return valueToBoolean(value)
	case TypeTime:
		return valueToTime(value)
	default:
		return value, nil
	}
}

func valueToString(value interface{}) interface{} {
	return value
}

func valueToInt64(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		return reflect.ValueOf(v).Int(), nil
	case float32, float64:
		return int64(reflect.ValueOf(v).Float()), nil
	case string:
		// TODO: 实现字符串到 int64 的转换
		return nil, &TypeConversionError{From: "string", To: "int64"}
	default:
		return nil, &TypeConversionError{From: reflect.TypeOf(value).String(), To: "int64"}
	}
}

func valueToFloat64(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case float32, float64:
		return reflect.ValueOf(v).Float(), nil
	case int, int8, int16, int32, int64:
		return float64(reflect.ValueOf(v).Int()), nil
	default:
		return nil, &TypeConversionError{From: reflect.TypeOf(value).String(), To: "float64"}
	}
}

func valueToBoolean(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return v == "true" || v == "1" || v == "yes", nil
	case int:
		return v != 0, nil
	default:
		return nil, &TypeConversionError{From: reflect.TypeOf(value).String(), To: "bool"}
	}
}

func valueToTime(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		// 尝试解析时间字符串
		t, err := time.Parse(time.RFC3339, v)
		return t, err
	default:
		return nil, &TypeConversionError{From: reflect.TypeOf(value).String(), To: "time.Time"}
	}
}

// ValidationError 验证错误
type ValidationError struct {
	Code    string
	Message string
}

func (e *ValidationError) Error() string {
	return e.Code + ": " + e.Message
}

// NewValidationError 创建验证错误
func NewValidationError(code, message string) *ValidationError {
	return &ValidationError{
		Code:    code,
		Message: message,
	}
}

// ==================== Schema Registry ====================

// SchemaRegistry Schema 注册表，便于查找和管理多个 Schema
type SchemaRegistry struct {
	schemas map[string]Schema
}

// NewSchemaRegistry 创建空的 Schema 注册表
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas: make(map[string]Schema),
	}
}

// Register 注册一个 Schema
func (r *SchemaRegistry) Register(name string, schema Schema) {
	r.schemas[name] = schema
}

// Get 获取指定名称的 Schema
func (r *SchemaRegistry) Get(name string) Schema {
	return r.schemas[name]
}

// GetAllSchemaNames 获取所有已注册的 Schema 名称
func (r *SchemaRegistry) GetAllSchemaNames() []string {
	names := make([]string, 0, len(r.schemas))
	for name := range r.schemas {
		names = append(names, name)
	}
	return names
}

// Timestamp 获取当前时间（用于 created_at/updated_at 字段）
func Timestamp() time.Time {
	return time.Now()
}

// ==================== Query Builder (v0.4.1) ====================

// QueryConstructor 查询构造器接口 - 顶层 API
// 用户通过此接口构建查询，具体实现由适配器提供
type QueryConstructor interface {
	// 条件查询
	Where(condition Condition) QueryConstructor
	WhereWith(builder *WhereBuilder) QueryConstructor

	// 多条件 AND 组合
	WhereAll(conditions ...Condition) QueryConstructor

	// 多条件 OR 组合
	WhereAny(conditions ...Condition) QueryConstructor

	// 字段选择
	Select(fields ...string) QueryConstructor
	Count(fieldName ...string) QueryConstructor
	CountWith(builder *CountBuilder) QueryConstructor

	// 排序
	OrderBy(field string, direction string) QueryConstructor // direction: "ASC" | "DESC"

	// 分页
	Limit(count int) QueryConstructor
	Offset(count int) QueryConstructor

	// 跨表查询（raw string JOIN）
	FromAlias(alias string) QueryConstructor
	Join(table, onClause string, alias ...string) QueryConstructor
	LeftJoin(table, onClause string, alias ...string) QueryConstructor
	RightJoin(table, onClause string, alias ...string) QueryConstructor
	CrossJoin(table string, alias ...string) QueryConstructor

	// JoinWith 使用 JoinBuilder 进行 Schema 感知的跨表连接。
	// SQL 后端：从 Schema.TableName() 取表名；Filter 条件追加到 WHERE。
	// Neo4j 后端：从 Schema FK 约束推断关系类型（On 为空时自动推断）；
	//             Filter 条件转换为对连接节点的属性过滤。
	JoinWith(builder *JoinBuilder) QueryConstructor

	// 跨表查询策略（方言级默认 + 显式覆盖）
	CrossTableStrategy(strategy CrossTableStrategy) QueryConstructor
	CustomMode() QueryConstructor

	// 构建查询
	Build(ctx context.Context) (string, []interface{}, error)

	// 统计结果数量（忽略当前分页设置）
	SelectCount(ctx context.Context, repo *Repository) (int64, error)

	// UPSERT（支持方言原生语法；不支持时事务模拟）
	Upsert(ctx context.Context, repo *Repository, cs *Changeset, conflictColumns ...string) (sql.Result, error)

	// 获取底层查询构造器（用于 Adapter 特定优化）
	GetNativeBuilder() interface{}
}

// CrossTableStrategy 跨表查询策略。
type CrossTableStrategy string

const (
	// CrossTableStrategyAuto 自动策略：由方言默认行为决定（推荐默认）。
	CrossTableStrategyAuto CrossTableStrategy = "auto"
	// CrossTableStrategyPreferTempTable 优先临时表策略（例如 SQL Server 的复杂跨表查询）。
	CrossTableStrategyPreferTempTable CrossTableStrategy = "prefer_temp_table"
	// CrossTableStrategyForceDirectJoin 强制直接 JOIN，不走临时表改写。
	CrossTableStrategyForceDirectJoin CrossTableStrategy = "force_direct_join"
)

// Condition 条件接口 - 中层转义
// Adapter 实现此接口将条件转换为数据库特定的形式
type Condition interface {
	// 获取条件类型
	Type() string

	// 将条件转换为 SQL/Cypher/etc
	Translate(translator ConditionTranslator) (string, []interface{}, error)
}

// ConditionTranslator 条件转义器接口
// 由每个 Adapter 的 QueryConstructor 实现
type ConditionTranslator interface {
	TranslateCondition(condition Condition) (string, []interface{}, error)
	TranslateComposite(operator string, conditions []Condition) (string, []interface{}, error)
}

// ==================== 内置 Condition 实现 ====================

// SimpleCondition 简单条件（字段 操作符 值）
type SimpleCondition struct {
	Field    string
	Operator string // "eq", "ne", "gt", "lt", "gte", "lte", "in", "like", "between", "full_text"
	Value    interface{}
}

func (c *SimpleCondition) Type() string {
	return "simple"
}

func (c *SimpleCondition) Translate(translator ConditionTranslator) (string, []interface{}, error) {
	return translator.TranslateCondition(c)
}

// CompositeCondition 复合条件（AND/OR）
type CompositeCondition struct {
	Operator   string // "and" | "or"
	Conditions []Condition
}

func (c *CompositeCondition) Type() string {
	return "composite"
}

func (c *CompositeCondition) Translate(translator ConditionTranslator) (string, []interface{}, error) {
	return translator.TranslateComposite(c.Operator, c.Conditions)
}

// NotCondition 非条件
type NotCondition struct {
	Condition Condition
}

func (c *NotCondition) Type() string {
	return "not"
}

func (c *NotCondition) Translate(translator ConditionTranslator) (string, []interface{}, error) {
	innerSQL, args, err := c.Condition.Translate(translator)
	if err != nil {
		return "", nil, err
	}
	return "NOT (" + innerSQL + ")", args, nil
}

// ==================== Condition Builder (Fluent API) ====================

// WhereBuilder 条件构造器（独立于 QueryConstructor）。
// 可先构造完整条件树，再一次性注入 Where 子句。
type WhereBuilder struct {
	condition Condition
}

// NewWhereBuilder 创建独立 WhereBuilder。
func NewWhereBuilder(condition Condition) *WhereBuilder {
	return &WhereBuilder{condition: condition}
}

// Build 返回构造后的根条件。
func (b *WhereBuilder) Build() Condition {
	if b == nil {
		return nil
	}
	return b.condition
}

// And 追加 AND 条件。
func (b *WhereBuilder) And(condition Condition) *WhereBuilder {
	if condition == nil {
		return b
	}
	if b == nil {
		return NewWhereBuilder(condition)
	}
	if b.condition == nil {
		b.condition = condition
		return b
	}
	b.condition = And(b.condition, condition)
	return b
}

// Or 追加 OR 条件。
func (b *WhereBuilder) Or(condition Condition) *WhereBuilder {
	if condition == nil {
		return b
	}
	if b == nil {
		return NewWhereBuilder(condition)
	}
	if b.condition == nil {
		b.condition = condition
		return b
	}
	b.condition = Or(b.condition, condition)
	return b
}

// Not 将当前条件取反。
func (b *WhereBuilder) Not() *WhereBuilder {
	if b == nil || b.condition == nil {
		return b
	}
	b.condition = Not(b.condition)
	return b
}

// ==================== Join Semantic ====================

// JoinSemantic 表达 JOIN 的语义意图，与 SQL 方言和数据库类型无关。
// 适配器根据此语义自行选择最优实现（JOIN 关键词、关系模式、MATCH 类型等）。
type JoinSemantic string

const (
	// JoinSemanticRequired 必须匹配：双方都必须存在（SQL: INNER JOIN；Neo4j: MATCH）。
	JoinSemanticRequired JoinSemantic = "required"
	// JoinSemanticOptional 连接目标可不存在（SQL: LEFT JOIN；Neo4j: OPTIONAL MATCH）。
	JoinSemanticOptional JoinSemantic = "optional"
	// JoinSemanticCross 笛卡尔积（SQL: CROSS JOIN；Neo4j: 独立 MATCH）。
	JoinSemanticCross JoinSemantic = "cross"
	// JoinSemanticInfer 由 Schema FK 关系自动推断（默认值）：
	// FK 字段 Null:true → JoinSemanticOptional；否则 → JoinSemanticRequired。
	JoinSemanticInfer JoinSemantic = ""
)

// ==================== Join Builder ====================

// JoinBuilder 连接构造器，携带目标 Schema，支持跨数据库通用表达。
//
// 以语义意图（JoinSemantic）驱动，无需关心 SQL JOIN 方向细节：
//   - SQL 后端：semantic → INNER/LEFT/CROSS JOIN；On() 字符串作为 ON 子句；
//     Filter() 以 join alias 限定后追加到 WHERE。
//   - Neo4j 后端：semantic → MATCH / OPTIONAL MATCH；On() 为空时从 Schema FK
//     推断关系类型；Filter() 转换为节点属性过滤。
type JoinBuilder struct {
	semantic JoinSemantic // 语义意图；JoinSemanticInfer 时由适配器从 Schema 推断
	schema   Schema       // 目标实体 Schema（必填）
	alias    string       // 可选别名
	onClause string       // SQL: ON 表达式；Neo4j: 关系模式字符串（可选，为空时推断）
	filters  []Condition  // 对连接目标实体的额外过滤条件
}

// NewJoinWith 创建 Schema 感知 JoinBuilder，语义由 Schema FK 关系自动推断。
// FK 字段 Null:true → 自动使用 optional（LEFT JOIN / OPTIONAL MATCH）；否则 → required。
// 可通过 .Required() / .Optional() 显式覆盖。
func NewJoinWith(schema Schema) *JoinBuilder {
	return &JoinBuilder{semantic: JoinSemanticInfer, schema: schema}
}

// NewRequiredJoin 创建语义为"必须匹配"的 JoinBuilder（SQL: INNER JOIN；Neo4j: MATCH）。
func NewRequiredJoin(schema Schema) *JoinBuilder {
	return &JoinBuilder{semantic: JoinSemanticRequired, schema: schema}
}

// NewOptionalJoin 创建语义为"可选匹配"的 JoinBuilder（SQL: LEFT JOIN；Neo4j: OPTIONAL MATCH）。
func NewOptionalJoin(schema Schema) *JoinBuilder {
	return &JoinBuilder{semantic: JoinSemanticOptional, schema: schema}
}

// NewJoinBuilder 创建 JoinBuilder（兼容旧代码；推荐改用 NewJoinWith / NewRequiredJoin / NewOptionalJoin）。
func NewJoinBuilder(joinType string, schema Schema) *JoinBuilder {
	switch strings.ToUpper(strings.TrimSpace(joinType)) {
	case "LEFT", "RIGHT": // RIGHT 统一视为 optional，跨 DB 均安全
		return NewOptionalJoin(schema)
	case "CROSS":
		return &JoinBuilder{semantic: JoinSemanticCross, schema: schema}
	default: // INNER, ""
		return NewRequiredJoin(schema)
	}
}

// NewInnerJoin 创建 INNER JOIN builder（等同于 NewRequiredJoin）。
func NewInnerJoin(schema Schema) *JoinBuilder { return NewRequiredJoin(schema) }

// NewLeftJoin 创建 LEFT JOIN builder（等同于 NewOptionalJoin）。
func NewLeftJoin(schema Schema) *JoinBuilder { return NewOptionalJoin(schema) }

// NewRightJoin 创建 RIGHT JOIN builder。
// 注意：RIGHT JOIN 在 NoSQL 适配器中无方向概念，统一视为 optional 语义，跨数据库均安全。
func NewRightJoin(schema Schema) *JoinBuilder { return NewOptionalJoin(schema) }

// NewCrossJoin 创建 CROSS JOIN builder（无 ON 条件）。
func NewCrossJoin(schema Schema) *JoinBuilder {
	return &JoinBuilder{semantic: JoinSemanticCross, schema: schema}
}

// Required 显式设置语义为"必须匹配"，覆盖自动推断。
func (b *JoinBuilder) Required() *JoinBuilder {
	if b != nil {
		b.semantic = JoinSemanticRequired
	}
	return b
}

// Optional 显式设置语义为"可选匹配"，覆盖自动推断。
func (b *JoinBuilder) Optional() *JoinBuilder {
	if b != nil {
		b.semantic = JoinSemanticOptional
	}
	return b
}

// As 设置连接别名。
func (b *JoinBuilder) As(alias string) *JoinBuilder {
	if b != nil {
		b.alias = strings.TrimSpace(alias)
	}
	return b
}

// On 设置连接表达式。
// SQL: 直接作为 ON <expr>，例如 "users.id = orders.user_id"。
// Neo4j: 关系模式字符串，例如 "->[:OWNS]->" 或 "<-[:BELONGS_TO]-"。
// 为空时 Neo4j 编译器自动从目标 Schema 的 FK 约束推断关系类型。
func (b *JoinBuilder) On(expr string) *JoinBuilder {
	if b != nil {
		b.onClause = strings.TrimSpace(expr)
	}
	return b
}

// Filter 追加对连接目标实体的过滤条件。
// SQL: 以 join alias 限定后追加到 WHERE 子句。
// Neo4j: 转换为对连接节点的属性过滤（WHERE alias.prop op val）。
func (b *JoinBuilder) Filter(conds ...Condition) *JoinBuilder {
	if b != nil {
		b.filters = append(b.filters, conds...)
	}
	return b
}

// resolveJoinSemantic 将 JoinSemanticInfer 解析为具体语义。
// 优先级：① 关系注册表（HasMany/HasOne/BelongsTo 声明）→ ② FK 约束 → ③ 默认 required。
func resolveJoinSemantic(semantic JoinSemantic, sourceSchema, joinSchema Schema) JoinSemantic {
	if semantic != JoinSemanticInfer {
		return semantic
	}

	// ① 关系注册表（优先）
	if s, ok := findSemanticFromRelations(sourceSchema, joinSchema); ok {
		return s
	}

	// ② FK 约束（向后兼容）
	// joinSchema 持有 FK 指向 sourceSchema，且 FK 字段可空
	// → source 可能没有对应 join 行 → optional
	if joinSchema != nil && sourceSchema != nil {
		if cs, ok := joinSchema.(ConstrainedSchema); ok {
			for _, tc := range cs.Constraints() {
				if tc.Kind != ConstraintForeignKey {
					continue
				}
				if !strings.EqualFold(tc.RefTable, sourceSchema.TableName()) {
					continue
				}
				for _, fkField := range tc.Fields {
					if f := joinSchema.GetField(fkField); f != nil && f.Null {
						return JoinSemanticOptional
					}
				}
			}
		}
	}
	// sourceSchema 持有 FK 指向 joinSchema，且 FK 字段可空
	// → source 不一定有对应 join 目标 → optional
	if sourceSchema != nil && joinSchema != nil {
		if cs, ok := sourceSchema.(ConstrainedSchema); ok {
			for _, tc := range cs.Constraints() {
				if tc.Kind != ConstraintForeignKey {
					continue
				}
				if !strings.EqualFold(tc.RefTable, joinSchema.TableName()) {
					continue
				}
				for _, fkField := range tc.Fields {
					if f := sourceSchema.GetField(fkField); f != nil && f.Null {
						return JoinSemanticOptional
					}
				}
			}
		}
	}
	return JoinSemanticRequired
}

// findSemanticFromRelations 从关系注册表推断连接语义。
// 先查 source→join 直接声明，再查 join→source 反向声明。
func findSemanticFromRelations(sourceSchema, joinSchema Schema) (JoinSemantic, bool) {
	if sourceSchema == nil || joinSchema == nil {
		return JoinSemanticRequired, false
	}
	joinTable := joinSchema.TableName()
	sourceTable := sourceSchema.TableName()

	// source 持有直接声明的关系（source→join）
	if rs, ok := sourceSchema.(RelationalSchema); ok {
		if rel := rs.FindRelation(joinTable); rel != nil {
			return rel.Semantic(), true
		}
	}

	// join 持有指向 source 的反向声明（join→source）：翻转从 source 视角的语义
	if rj, ok := joinSchema.(RelationalSchema); ok {
		if rel := rj.FindRelation(sourceTable); rel != nil {
			return rel.SemanticFromSource(), true
		}
	}

	return JoinSemanticRequired, false
}

// semanticToSQLJoinType 将语义映射为 SQL JOIN 关键词（INNER / LEFT / CROSS）。
func semanticToSQLJoinType(s JoinSemantic) string {
	switch s {
	case JoinSemanticOptional:
		return "LEFT"
	case JoinSemanticCross:
		return "CROSS"
	default: // required
		return "INNER"
	}
}

// CountBuilder 统计投影构造器。
// 用于表达 COUNT(field) / COUNT(DISTINCT field) 及别名。
type CountBuilder struct {
	field    string
	distinct bool
	alias    string
}

// NewCountBuilder 创建 CountBuilder。
// 为空或传入 "*" 时表示 COUNT(*)。
func NewCountBuilder(fieldName ...string) *CountBuilder {
	field := "*"
	if len(fieldName) > 0 {
		trimmed := strings.TrimSpace(fieldName[0])
		if trimmed != "" {
			field = trimmed
		}
	}
	return &CountBuilder{field: field}
}

// As 为 COUNT 投影设置别名。
func (b *CountBuilder) As(alias string) *CountBuilder {
	if b != nil {
		b.alias = strings.TrimSpace(alias)
	}
	return b
}

// Distinct 切换为 COUNT(DISTINCT field)。
func (b *CountBuilder) Distinct() *CountBuilder {
	if b != nil {
		b.distinct = true
	}
	return b
}

// Eq 等于条件
func Eq(field string, value interface{}) Condition {
	return &SimpleCondition{
		Field:    field,
		Operator: "eq",
		Value:    value,
	}
}

// EqFields 多字段等值条件（适用于复合主键/复合唯一键定位）
func EqFields(values map[string]interface{}) Condition {
	conditions := make([]Condition, 0, len(values))
	for field, value := range values {
		conditions = append(conditions, Eq(field, value))
	}

	if len(conditions) == 0 {
		return nil
	}

	if len(conditions) == 1 {
		return conditions[0]
	}

	return And(conditions...)
}

// Ne 不等于条件
func Ne(field string, value interface{}) Condition {
	return &SimpleCondition{
		Field:    field,
		Operator: "ne",
		Value:    value,
	}
}

// Gt 大于条件
func Gt(field string, value interface{}) Condition {
	return &SimpleCondition{
		Field:    field,
		Operator: "gt",
		Value:    value,
	}
}

// Lt 小于条件
func Lt(field string, value interface{}) Condition {
	return &SimpleCondition{
		Field:    field,
		Operator: "lt",
		Value:    value,
	}
}

// Gte 大于等于条件
func Gte(field string, value interface{}) Condition {
	return &SimpleCondition{
		Field:    field,
		Operator: "gte",
		Value:    value,
	}
}

// Lte 小于等于条件
func Lte(field string, value interface{}) Condition {
	return &SimpleCondition{
		Field:    field,
		Operator: "lte",
		Value:    value,
	}
}

// In IN 条件
func In(field string, values ...interface{}) Condition {
	return &SimpleCondition{
		Field:    field,
		Operator: "in",
		Value:    values,
	}
}

// Between BETWEEN 条件
func Between(field string, min, max interface{}) Condition {
	return &SimpleCondition{
		Field:    field,
		Operator: "between",
		Value:    []interface{}{min, max},
	}
}

// Like LIKE 条件（模糊匹配）
func Like(field string, pattern string) Condition {
	return &SimpleCondition{
		Field:    field,
		Operator: "like",
		Value:    pattern,
	}
}

// FullText 全文检索条件（由方言翻译器决定具体语法）
func FullText(field string, query string) Condition {
	return &SimpleCondition{
		Field:    field,
		Operator: "full_text",
		Value:    query,
	}
}

// And AND 条件
func And(conditions ...Condition) Condition {
	return &CompositeCondition{
		Operator:   "and",
		Conditions: conditions,
	}
}

// Or OR 条件
func Or(conditions ...Condition) Condition {
	return &CompositeCondition{
		Operator:   "or",
		Conditions: conditions,
	}
}

// Not NOT 条件
func Not(condition Condition) Condition {
	return &NotCondition{
		Condition: condition,
	}
}
