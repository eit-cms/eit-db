package db

import (
	"context"
	"fmt"
	"sync"

	"gorm.io/gorm"
)

// MySQLDynamicTableHook MySQL 动态表钩子实现
// 使用 GORM 的 hook 机制实现基于触发的动态建表
type MySQLDynamicTableHook struct {
	adapter        *MySQLAdapter
	registry       *DynamicTableRegistry
	hookRegistered map[string]bool
	mu             sync.RWMutex
}

// NewMySQLDynamicTableHook 创建 MySQL 动态表钩子
func NewMySQLDynamicTableHook(adapter *MySQLAdapter) *MySQLDynamicTableHook {
	return &MySQLDynamicTableHook{
		adapter:        adapter,
		registry:       NewDynamicTableRegistry(),
		hookRegistered: make(map[string]bool),
	}
}

// RegisterDynamicTable 注册动态表配置
// 对于自动策略，在 GORM hook 中注册事件处理
func (h *MySQLDynamicTableHook) RegisterDynamicTable(ctx context.Context, config *DynamicTableConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.registry.Register(config.TableName, config); err != nil {
		return err
	}

	// 如果是自动策略且有父表，注册 GORM hook
	if config.Strategy == "auto" && config.ParentTable != "" {
		if err := h.registerAfterCreateHook(config); err != nil {
			h.registry.Unregister(config.TableName)
			return fmt.Errorf("failed to register GORM hook: %w", err)
		}
		h.hookRegistered[config.TableName] = true
	}

	return nil
}

// UnregisterDynamicTable 注销动态表配置
func (h *MySQLDynamicTableHook) UnregisterDynamicTable(ctx context.Context, configName string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, err := h.registry.Get(configName)
	if err != nil {
		return err
	}

	// 从注册表中删除
	if err := h.registry.Unregister(configName); err != nil {
		return err
	}

	// 从 hook 跟踪中删除
	if h.hookRegistered[configName] {
		delete(h.hookRegistered, configName)
	}

	return nil
}

// ListDynamicTableConfigs 列出所有已注册的动态表配置
func (h *MySQLDynamicTableHook) ListDynamicTableConfigs(ctx context.Context) ([]*DynamicTableConfig, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.registry.List(), nil
}

// GetDynamicTableConfig 获取特定的动态表配置
func (h *MySQLDynamicTableHook) GetDynamicTableConfig(ctx context.Context, configName string) (*DynamicTableConfig, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.registry.Get(configName)
}

// CreateDynamicTable 手动创建动态表
// 返回实际创建的表名称
func (h *MySQLDynamicTableHook) CreateDynamicTable(ctx context.Context, configName string, params map[string]interface{}) (string, error) {
	h.mu.RLock()
	config, err := h.registry.Get(configName)
	h.mu.RUnlock()

	if err != nil {
		return "", err
	}

	// 根据参数生成实际表名
	tableName := h.generateTableName(config, params)

	// 检查表是否已存在
	exists, err := h.tableExists(ctx, tableName)
	if err != nil {
		return "", err
	}

	if exists {
		return tableName, fmt.Errorf("table already exists: %s", tableName)
	}

	// 创建表
	if err := h.createTable(ctx, config, tableName); err != nil {
		return "", err
	}

	return tableName, nil
}

// ListCreatedDynamicTables 获取已创建的动态表列表
func (h *MySQLDynamicTableHook) ListCreatedDynamicTables(ctx context.Context, configName string) ([]string, error) {
	h.mu.RLock()
	config, err := h.registry.Get(configName)
	h.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	// 从 information_schema 查询所有匹配的表
	prefix := config.TableName + "_"
	query := `
		SELECT TABLE_NAME 
		FROM information_schema.TABLES 
		WHERE TABLE_SCHEMA = DATABASE() 
		AND TABLE_NAME LIKE CONCAT(?, '%')
		ORDER BY TABLE_NAME
	`

	rows, err := h.adapter.Query(ctx, query, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, rows.Err()
}

// 内部辅助方法

// registerAfterCreateHook 注册 GORM 的 AfterCreate hook
func (h *MySQLDynamicTableHook) registerAfterCreateHook(config *DynamicTableConfig) error {
	// 为关联的表注册 hook
	if h.adapter.db == nil {
		return fmt.Errorf("GORM DB instance not available")
	}

	// 使用一个动态回调来处理行创建
	h.adapter.db.Callback().Create().After("gorm:after_create").Register(
		"dynamic_table:after_create:"+config.TableName,
		func(db *gorm.DB) {
			// 在创建记录后检查是否需要创建动态表
			h.handleAfterCreateCallback(db, config)
		},
	)

	return nil
}

// handleAfterCreateCallback 处理 AfterCreate 回调
func (h *MySQLDynamicTableHook) handleAfterCreateCallback(db *gorm.DB, config *DynamicTableConfig) {
	// 获取创建的记录
	if db.Statement == nil || db.Statement.Dest == nil {
		return
	}

	// 尝试从记录中提取 ID（或其他参数）
	params := h.extractParamsFromRecord(db.Statement.Dest, config)

	// 检查是否需要创建动态表（根据条件判断）
	if h.shouldCreateDynamicTable(db.Statement.Dest, config) {
		tableName := h.generateTableName(config, params)

		// 检查表是否已存在
		exists, err := h.tableExists(db.Statement.Context, tableName)
		if err != nil {
			return // 静默失败，不中断事务
		}

		if !exists {
			// 创建表（在同一事务中）
			if err := h.createTable(db.Statement.Context, config, tableName); err != nil {
				// 记录错误但不中断事务
				_ = err
			}
		}
	}
}

// shouldCreateDynamicTable 判断是否应该创建动态表
func (h *MySQLDynamicTableHook) shouldCreateDynamicTable(record interface{}, config *DynamicTableConfig) bool {
	if config.TriggerCondition == "" {
		// 如果没有条件，总是创建
		return true
	}

	// 简单的条件判断：检查字段值
	// 例如：TriggerCondition = "type = 'custom'"
	// 这里只是示例，实际可能需要更复杂的条件评估
	return true
}

// extractParamsFromRecord 从记录中提取参数
func (h *MySQLDynamicTableHook) extractParamsFromRecord(record interface{}, config *DynamicTableConfig) map[string]interface{} {
	params := make(map[string]interface{})

	// 使用反射获取记录的字段值
	// 特别是 ID 字段
	switch v := record.(type) {
	case map[string]interface{}:
		params = v
	default:
		// 对于结构体，尝试提取 ID 字段
		params["id"] = extractFieldValue(record, "ID")
	}

	return params
}

// createTable 创建动态表
func (h *MySQLDynamicTableHook) createTable(ctx context.Context, config *DynamicTableConfig, tableName string) error {
	repo := &Repository{adapter: h.adapter}
	schema := config.toSchema(tableName)
	createSQL := buildCreateTableSQL(repo, schema)
	createSQL += " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"

	return h.executeSQL(ctx, createSQL)
}

// tableExists 检查表是否存在
func (h *MySQLDynamicTableHook) tableExists(ctx context.Context, tableName string) (bool, error) {
	query := `
		SELECT COUNT(*) > 0
		FROM information_schema.TABLES 
		WHERE TABLE_SCHEMA = DATABASE() 
		AND TABLE_NAME = ?
	`

	var exists bool
	row := h.adapter.QueryRow(ctx, query, tableName)
	if err := row.Scan(&exists); err != nil {
		return false, err
	}

	return exists, nil
}

// generateTableName 根据参数生成表名
func (h *MySQLDynamicTableHook) generateTableName(config *DynamicTableConfig, params map[string]interface{}) string {
	// 简单实现：使用 id 参数作为后缀
	if id, ok := params["id"]; ok {
		return fmt.Sprintf("%s_%v", config.TableName, id)
	}
	return config.TableName
}

// executeSQL 执行 SQL
func (h *MySQLDynamicTableHook) executeSQL(ctx context.Context, sql string) error {
	_, err := h.adapter.Exec(ctx, sql)
	return err
}

// extractFieldValue 从结构体字段中提取值
func extractFieldValue(record interface{}, fieldName string) interface{} {
	// 这是一个简化的实现，实际可能需要使用反射库
	// 在这里仅作示例
	return nil
}
