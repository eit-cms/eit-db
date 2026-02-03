package db

import (
	"context"
	"fmt"
)

// RelationshipSupport 关系支持能力声明
type RelationshipSupport struct {
	// 基础关系类型支持
	OneToOne   bool
	OneToMany  bool
	ManyToMany bool
	
	// 关系特性
	SupportsForeignKey bool // 是否支持外键约束
	SupportsJoin       bool // 是否支持 JOIN 操作
	SupportsNested     bool // 是否支持嵌套关系查询
	
	// 实现方式
	Strategy RelationshipStrategy // 如何实现关系
}

type RelationshipStrategy string

const (
	// 原生支持（PostgreSQL, SQL Server）
	StrategyNative RelationshipStrategy = "native"
	
	// 通过中间表模拟多对多（MySQL, SQLite）
	StrategyJoinTable RelationshipStrategy = "join_table"
	
	// 通过外键实现关系（所有关系型数据库）
	StrategyForeignKey RelationshipStrategy = "foreign_key"
	
	// 应用层实现（非关系型数据库）
	StrategyApplication RelationshipStrategy = "application"
	
	// 完全不支持
	StrategyNotSupported RelationshipStrategy = "not_supported"
)

// Relationship 表示表之间的关系定义
type Relationship struct {
	// 关系名称（用于查询时引用）
	Name string
	
	// 源表和目标表
	FromSchema Schema
	ToSchema   Schema
	
	// 关系类型
	Type RelationType
	
	// 外键定义
	ForeignKey *ForeignKeyDef
	
	// 关联表（仅用于多对多）
	JoinTable *JoinTableDef
}

type RelationType string

const (
	OneToOne   RelationType = "one_to_one"
	OneToMany  RelationType = "one_to_many"
	ManyToOne  RelationType = "many_to_one"
	ManyToMany RelationType = "many_to_many"
)

// ForeignKeyDef 外键定义
type ForeignKeyDef struct {
	// 源表的列
	FromColumn string
	
	// 目标表的列
	ToColumn string
	
	// 级联删除
	OnDelete ForeignKeyAction
	OnUpdate ForeignKeyAction
}

type ForeignKeyAction string

const (
	ActionRestrict ForeignKeyAction = "RESTRICT"
	ActionCascade  ForeignKeyAction = "CASCADE"
	ActionSetNull  ForeignKeyAction = "SET NULL"
	ActionNoAction ForeignKeyAction = "NO ACTION"
)

// JoinTableDef 关联表定义（多对多）
type JoinTableDef struct {
	// 关联表名称
	TableName string
	
	// 源表外键列和目标表外键列
	FromForeignKey string
	ToForeignKey   string
	
	// 额外的列（如时间戳等）
	ExtraColumns []*Field
}

// RelationshipManager Adapter 需要实现的关系管理接口
type RelationshipManager interface {
	// 获取该 Adapter 的关系支持能力
	GetRelationshipSupport() *RelationshipSupport
	
	// 创建关系（创建外键、中间表等）
	CreateRelationship(ctx context.Context, rel *Relationship) error
	
	// 删除关系
	DropRelationship(ctx context.Context, fromTable, relName string) error
	
	// 查询关系（带关联数据）
	QueryWithRelation(ctx context.Context, schema Schema, rel *Relationship, query string, args ...interface{}) (interface{}, error)
	
	// 检查关系是否存在
	HasRelationship(ctx context.Context, fromTable, relName string) (bool, error)
}

// 扩展 Adapter 接口
// type Adapter interface {
//     // ... 现有方法
//     RelationshipManager
// }

// SchemaRelationshipBuilder Schema 中的关系构建
type SchemaRelationshipBuilder struct {
	schema         *BaseSchema
	relationships  []*Relationship
}

// NewSchemaRelationshipBuilder 创建关系构建器
func NewSchemaRelationshipBuilder(schema *BaseSchema) *SchemaRelationshipBuilder {
	return &SchemaRelationshipBuilder{
		schema:        schema,
		relationships: make([]*Relationship, 0),
	}
}

// HasOne 定义一对一关系
func (b *SchemaRelationshipBuilder) HasOne(name string, toSchema Schema, foreignKey, primaryKey string) *SchemaRelationshipBuilder {
	rel := &Relationship{
		Name:       name,
		FromSchema: b.schema,
		ToSchema:   toSchema,
		Type:       OneToOne,
		ForeignKey: &ForeignKeyDef{
			FromColumn: foreignKey,
			ToColumn:   primaryKey,
		},
	}
	b.relationships = append(b.relationships, rel)
	return b
}

// HasMany 定义一对多关系
func (b *SchemaRelationshipBuilder) HasMany(name string, toSchema Schema, foreignKey, primaryKey string) *SchemaRelationshipBuilder {
	rel := &Relationship{
		Name:       name,
		FromSchema: b.schema,
		ToSchema:   toSchema,
		Type:       OneToMany,
		ForeignKey: &ForeignKeyDef{
			FromColumn: primaryKey,
			ToColumn:   foreignKey,
		},
	}
	b.relationships = append(b.relationships, rel)
	return b
}

// BelongsTo 定义多对一关系（反向一对多）
func (b *SchemaRelationshipBuilder) BelongsTo(name string, toSchema Schema, foreignKey, primaryKey string) *SchemaRelationshipBuilder {
	rel := &Relationship{
		Name:       name,
		FromSchema: b.schema,
		ToSchema:   toSchema,
		Type:       ManyToOne,
		ForeignKey: &ForeignKeyDef{
			FromColumn: foreignKey,
			ToColumn:   primaryKey,
		},
	}
	b.relationships = append(b.relationships, rel)
	return b
}

// HasAndBelongsToMany 定义多对多关系
func (b *SchemaRelationshipBuilder) HasAndBelongsToMany(
	name string,
	toSchema Schema,
	joinTableName string,
	fromForeignKey string,
	toForeignKey string,
) *SchemaRelationshipBuilder {
	rel := &Relationship{
		Name:       name,
		FromSchema: b.schema,
		ToSchema:   toSchema,
		Type:       ManyToMany,
		JoinTable: &JoinTableDef{
			TableName:      joinTableName,
			FromForeignKey: fromForeignKey,
			ToForeignKey:   toForeignKey,
		},
	}
	b.relationships = append(b.relationships, rel)
	return b
}

// GetRelationships 获取所有关系
func (b *SchemaRelationshipBuilder) GetRelationships() []*Relationship {
	return b.relationships
}

// RelationshipValidator 用于验证关系定义的有效性和兼容性
type RelationshipValidator struct {
	support *RelationshipSupport
}

// NewRelationshipValidator 创建验证器
func NewRelationshipValidator(support *RelationshipSupport) *RelationshipValidator {
	return &RelationshipValidator{support: support}
}

// ValidateRelationship 验证单个关系是否在 Adapter 支持范围内
func (v *RelationshipValidator) ValidateRelationship(rel *Relationship) error {
	switch rel.Type {
	case OneToOne:
		if !v.support.OneToOne {
			return fmt.Errorf("adapter does not support OneToOne relationships")
		}
	case OneToMany:
		if !v.support.OneToMany {
			return fmt.Errorf("adapter does not support OneToMany relationships")
		}
	case ManyToMany:
		if !v.support.ManyToMany {
			// 可以通过 JoinTable 策略进行转发
			if v.support.OneToMany && rel.JoinTable != nil {
				// 可以转发为两个 OneToMany 关系
				return nil
			}
			return fmt.Errorf("adapter does not support ManyToMany relationships and cannot emulate with JoinTable")
		}
	}
	return nil
}

// NeedsManyToManyEmulation 检查是否需要多对多转发
func (v *RelationshipValidator) NeedsManyToManyEmulation(rel *Relationship) bool {
	return rel.Type == ManyToMany && !v.support.ManyToMany && v.support.OneToMany
}

// CanJoin 检查是否可以执行 JOIN 操作
func (v *RelationshipValidator) CanJoin() bool {
	return v.support.SupportsJoin
}

// GetSupportSummary 获取支持情况的总结
func (v *RelationshipValidator) GetSupportSummary() map[string]bool {
	return map[string]bool{
		"OneToOne":        v.support.OneToOne,
		"OneToMany":       v.support.OneToMany,
		"ManyToMany":      v.support.ManyToMany,
		"ForeignKey":      v.support.SupportsForeignKey,
		"Join":            v.support.SupportsJoin,
		"Nested":          v.support.SupportsNested,
	}
}

// 预定义的常见数据库支持情况
var (
	PostgreSQLSupport = &RelationshipSupport{
		OneToOne:       true,
		OneToMany:      true,
		ManyToMany:     true,
		SupportsForeignKey: true,
		SupportsJoin:   true,
		SupportsNested: true,
		Strategy:       StrategyNative,
	}
	
	SQLServerSupport = &RelationshipSupport{
		OneToOne:       true,
		OneToMany:      true,
		ManyToMany:     true,
		SupportsForeignKey: true,
		SupportsJoin:   true,
		SupportsNested: true,
		Strategy:       StrategyNative,
	}
	
	MySQLSupport = &RelationshipSupport{
		OneToOne:       true,
		OneToMany:      true,
		ManyToMany:     false, // 需要转发
		SupportsForeignKey: true,
		SupportsJoin:   true,
		SupportsNested: false,
		Strategy:       StrategyJoinTable,
	}
	
	SQLiteSupport = &RelationshipSupport{
		OneToOne:       true,
		OneToMany:      true,
		ManyToMany:     false, // 需要转发
		SupportsForeignKey: false, // SQLite 默认不启用外键
		SupportsJoin:   true,
		SupportsNested: false,
		Strategy:       StrategyJoinTable,
	}
	
	MongoDBSupport = &RelationshipSupport{
		OneToOne:       true,
		OneToMany:      true,
		ManyToMany:     true, // 通过数组引用
		SupportsForeignKey: false,
		SupportsJoin:   false, // 需要应用层 lookup
		SupportsNested: true, // MongoDB 支持嵌套查询
		Strategy:       StrategyApplication,
	}
	
	GraphDatabaseSupport = &RelationshipSupport{
		OneToOne:       true,
		OneToMany:      true,
		ManyToMany:     true,
		SupportsForeignKey: false,
		SupportsJoin:   false, // 使用图的遍历而非 JOIN
		SupportsNested: true,
		Strategy:       StrategyNative,
	}
	
	NoRelationshipSupport = &RelationshipSupport{
		OneToOne:       false,
		OneToMany:      false,
		ManyToMany:     false,
		SupportsForeignKey: false,
		SupportsJoin:   false,
		SupportsNested: false,
		Strategy:       StrategyNotSupported,
	}
)
