package db

import (
	"reflect"
	"time"
)

// FieldType 字段类型定义
type FieldType string

const (
	TypeString    FieldType = "string"
	TypeInteger   FieldType = "integer"
	TypeFloat     FieldType = "float"
	TypeBoolean   FieldType = "boolean"
	TypeTime      FieldType = "time"
	TypeBinary    FieldType = "binary"
	TypeDecimal   FieldType = "decimal"
	TypeMap       FieldType = "map"
	TypeArray     FieldType = "array"
	TypeJSON      FieldType = "json"
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

// BaseSchema 基础模式实现
type BaseSchema struct {
	tableName string
	fields    map[string]*Field
	fieldList []*Field
}

// NewBaseSchema 创建基础模式
func NewBaseSchema(tableName string) *BaseSchema {
	return &BaseSchema{
		tableName: tableName,
		fields:    make(map[string]*Field),
		fieldList: make([]*Field, 0),
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
