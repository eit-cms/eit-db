package db

import (
	"sync"
)

// CrossTableViewEntry 描述一个通过视图加速的跨表关联。
// 由 Schema FK 的 ViewHint 声明，在 app 启动时注册到 CrossTableViewRegistry，
// QueryBuilder 在构建 JOIN 时自动查询并路由到视图。
type CrossTableViewEntry struct {
	// LocalTable 拥有外键的表名（FK 所在表）
	LocalTable string
	// RefTable 被引用的表名
	RefTable string
	// ViewName 已创建的视图名
	ViewName string
	// Materialized 是否为物化视图（PostgreSQL），影响 DROP 语句
	Materialized bool
	// LocalAlias 视图中本地表的别名（与 buildViewFromFKHintSQL 保持一致）
	LocalAlias string
	// RefAlias 视图中引用表的别名
	RefAlias string
	// JoinFields FK 本地列列表
	JoinFields []string
	// RefFields FK 引用列列表
	RefFields []string
}

// fkRegistryKey 生成注册表 key。
func fkRegistryKey(localTable, refTable string) string {
	return localTable + "|" + refTable
}

// CrossTableViewRegistry 跨表视图注册表（线程安全）。
//
// 典型使用流程：
//  1. Schema 声明阶段：AddForeignKey(..., WithViewHint(...))
//  2. App 启动：RegisterSchemasIntoGlobal(ordersSchema, ...)
//  3. Migration Up：自动 CREATE VIEW
//  4. 查询阶段：NewQueryConstructor(schema).Join(...) 自动路由到视图
type CrossTableViewRegistry struct {
	mu      sync.RWMutex
	entries map[string]*CrossTableViewEntry
}

// NewCrossTableViewRegistry 创建一个新的空注册表（用于测试或隔离注册）。
func NewCrossTableViewRegistry() *CrossTableViewRegistry {
	return &CrossTableViewRegistry{
		entries: make(map[string]*CrossTableViewEntry),
	}
}

// GlobalCrossTableViewRegistry 全局视图注册表，app 启动时调用 RegisterSchemasIntoGlobal 填充。
var GlobalCrossTableViewRegistry = NewCrossTableViewRegistry()

// Register 注册一条跨表视图关联。
func (r *CrossTableViewRegistry) Register(entry *CrossTableViewEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[fkRegistryKey(entry.LocalTable, entry.RefTable)] = entry
}

// Lookup 精确查找：以 localTable 为主表、refTable 为关联表的视图条目。
// 返回 (entry, true) 或 (nil, false)。
func (r *CrossTableViewRegistry) Lookup(localTable, refTable string) (*CrossTableViewEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[fkRegistryKey(localTable, refTable)]
	return e, ok
}

// LookupAny 双向查找：先查 (local, ref)，再查反向 (ref, local)。
// 适用于 QueryBuilder：无论 FROM 哪张表，都能找到对应的视图。
func (r *CrossTableViewRegistry) LookupAny(tableA, tableB string) (*CrossTableViewEntry, bool) {
	if e, ok := r.Lookup(tableA, tableB); ok {
		return e, true
	}
	return r.Lookup(tableB, tableA)
}

// RegisterFromSchema 扫描 Schema 上所有带 ViewHint 的 FK 约束并注册到本注册表。
func (r *CrossTableViewRegistry) RegisterFromSchema(schema Schema) {
	cs, ok := schema.(constraintSchema)
	if !ok {
		return
	}
	for _, c := range cs.Constraints() {
		if c.Kind != ConstraintForeignKey || c.ViewHint == nil {
			continue
		}
		hint := c.ViewHint
		viewName := hint.ViewName
		if viewName == "" {
			viewName = schema.TableName() + "_" + c.RefTable + "_view"
		}
		localAlias := deriveViewAlias(schema.TableName())
		refAlias := deriveViewAlias(c.RefTable)
		if localAlias == refAlias {
			refAlias = refAlias + "2"
		}
		r.Register(&CrossTableViewEntry{
			LocalTable:   schema.TableName(),
			RefTable:     c.RefTable,
			ViewName:     viewName,
			Materialized: hint.Materialized,
			LocalAlias:   localAlias,
			RefAlias:     refAlias,
			JoinFields:   append([]string(nil), c.Fields...),
			RefFields:    append([]string(nil), c.RefFields...),
		})
	}
}

// RegisterSchemasIntoGlobal 将多个 Schema 的 FK ViewHint 注册到全局注册表。
// 通常在 main() 或 app 初始化时调用一次。
//
//	db.RegisterSchemasIntoGlobal(usersSchema, ordersSchema, productSchema)
func RegisterSchemasIntoGlobal(schemas ...Schema) {
	for _, s := range schemas {
		GlobalCrossTableViewRegistry.RegisterFromSchema(s)
	}
}
