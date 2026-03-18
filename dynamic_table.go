package db

import (
	"context"
	"fmt"
	"sync"
)

// DynamicTableConfig 动态表配置
// 定义触发条件和表的创建规则
type DynamicTableConfig struct {
	// 表名称
	TableName string

	// 表描述
	Description string

	// 表字段定义
	Fields []*DynamicTableField

	// 触发条件：关联的父表（当父表插入或更新时触发建表）
	ParentTable string

	// 触发条件：检查父表的字段值
	// 例如：type = 'custom' 时才创建此动态表
	TriggerCondition string

	// 表创建策略：auto 自动创建，manual 手动创建
	Strategy string // "auto" or "manual"

	// 额外参数（适配器特定）
	Options map[string]interface{}
}

// DynamicTableField 动态表的字段定义
type DynamicTableField struct {
	Name        string
	Type        FieldType
	Primary     bool
	Autoinc     bool
	Null        bool
	Default     interface{}
	Index       bool
	Unique      bool
	Description string
}

// DynamicTableHook 动态表钩子接口
// 实现者需要在适配器中实现此接口
type DynamicTableHook interface {
	// 注册动态表配置
	RegisterDynamicTable(ctx context.Context, config *DynamicTableConfig) error

	// 注销动态表配置
	UnregisterDynamicTable(ctx context.Context, configName string) error

	// 列出所有已注册的动态表配置
	ListDynamicTableConfigs(ctx context.Context) ([]*DynamicTableConfig, error)

	// 获取特定的动态表配置
	GetDynamicTableConfig(ctx context.Context, configName string) (*DynamicTableConfig, error)

	// 手动触发动态表创建（当 Strategy = manual 时）
	CreateDynamicTable(ctx context.Context, configName string, params map[string]interface{}) (string, error)

	// 获取已创建的动态表列表
	ListCreatedDynamicTables(ctx context.Context, configName string) ([]string, error)
}

// DynamicTableRegistry 动态表配置注册表
type DynamicTableRegistry struct {
	configs map[string]*DynamicTableConfig
	mu      sync.RWMutex
}

// NewDynamicTableRegistry 创建动态表注册表
func NewDynamicTableRegistry() *DynamicTableRegistry {
	return &DynamicTableRegistry{
		configs: make(map[string]*DynamicTableConfig),
	}
}

// Register 注册配置
func (r *DynamicTableRegistry) Register(name string, config *DynamicTableConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.configs[name]; exists {
		return fmt.Errorf("dynamic table config already registered: %s", name)
	}

	if config.TableName == "" {
		return fmt.Errorf("table name is required")
	}

	if config.Strategy != "auto" && config.Strategy != "manual" {
		config.Strategy = "auto"
	}

	r.configs[name] = config
	return nil
}

// Unregister 注销配置
func (r *DynamicTableRegistry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.configs[name]; !exists {
		return fmt.Errorf("dynamic table config not found: %s", name)
	}

	delete(r.configs, name)
	return nil
}

// Get 获取配置
func (r *DynamicTableRegistry) Get(name string) (*DynamicTableConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	config, exists := r.configs[name]
	if !exists {
		return nil, fmt.Errorf("dynamic table config not found: %s", name)
	}

	return config, nil
}

// List 列出所有配置
func (r *DynamicTableRegistry) List() []*DynamicTableConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()

	configs := make([]*DynamicTableConfig, 0, len(r.configs))
	for _, config := range r.configs {
		configs = append(configs, config)
	}

	return configs
}

// DynamicTableHelper 辅助函数
// 用于快速创建动态表配置

// NewDynamicTableConfig 创建新的动态表配置
func NewDynamicTableConfig(tableName string) *DynamicTableConfig {
	return &DynamicTableConfig{
		TableName: tableName,
		Fields:    make([]*DynamicTableField, 0),
		Strategy:  "auto",
		Options:   make(map[string]interface{}),
	}
}

// AddField 添加字段到动态表配置
func (c *DynamicTableConfig) AddField(field *DynamicTableField) *DynamicTableConfig {
	c.Fields = append(c.Fields, field)
	return c
}

// WithParentTable 设置父表（用于自动触发）
func (c *DynamicTableConfig) WithParentTable(parentTable, triggerCondition string) *DynamicTableConfig {
	c.ParentTable = parentTable
	c.TriggerCondition = triggerCondition
	return c
}

// WithStrategy 设置创建策略
func (c *DynamicTableConfig) WithStrategy(strategy string) *DynamicTableConfig {
	if strategy == "auto" || strategy == "manual" {
		c.Strategy = strategy
	}
	return c
}

// WithDescription 设置描述
func (c *DynamicTableConfig) WithDescription(desc string) *DynamicTableConfig {
	c.Description = desc
	return c
}

// WithOption 设置选项
func (c *DynamicTableConfig) WithOption(key string, value interface{}) *DynamicTableConfig {
	c.Options[key] = value
	return c
}

// NewDynamicTableField 创建新的字段
func NewDynamicTableField(name string, fieldType FieldType) *DynamicTableField {
	return &DynamicTableField{
		Name: name,
		Type: fieldType,
		Null: true,
	}
}

// 链式方法

// AsPrimaryKey 设置为主键
func (f *DynamicTableField) AsPrimaryKey() *DynamicTableField {
	f.Primary = true
	f.Null = false
	return f
}

// WithAutoinc 启用自增
func (f *DynamicTableField) WithAutoinc() *DynamicTableField {
	f.Autoinc = true
	return f
}

// AsNotNull 设置为 NOT NULL
func (f *DynamicTableField) AsNotNull() *DynamicTableField {
	f.Null = false
	return f
}

// WithIndex 添加索引
func (f *DynamicTableField) WithIndex() *DynamicTableField {
	f.Index = true
	return f
}

// WithUnique 添加唯一约束
func (f *DynamicTableField) WithUnique() *DynamicTableField {
	f.Unique = true
	return f
}

// WithDefault 设置默认值
func (f *DynamicTableField) WithDefault(value interface{}) *DynamicTableField {
	f.Default = value
	return f
}

// WithDescription 设置字段描述
func (f *DynamicTableField) WithDescription(desc string) *DynamicTableField {
	f.Description = desc
	return f
}

// toSchema 将动态表配置转换为统一的 Schema 定义，便于复用框架级建表逻辑。
func (c *DynamicTableConfig) toSchema(tableName string) Schema {
	schema := NewBaseSchema(tableName)
	for _, field := range c.Fields {
		schema.AddField(&Field{
			Name:    field.Name,
			Type:    field.Type,
			Default: field.Default,
			Null:    field.Null,
			Primary: field.Primary,
			Autoinc: field.Autoinc,
			Index:   field.Index,
			Unique:  field.Unique,
		})
	}

	return schema
}
