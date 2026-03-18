package db

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// PostgreSQLDynamicTableHook PostgreSQL 动态表钩子实现
// 使用触发器和自定义函数实现自动化动态建表
type PostgreSQLDynamicTableHook struct {
	adapter  *PostgreSQLAdapter
	registry *DynamicTableRegistry
	mu       sync.RWMutex
}

// NewPostgreSQLDynamicTableHook 创建 PostgreSQL 动态表钩子
func NewPostgreSQLDynamicTableHook(adapter *PostgreSQLAdapter) *PostgreSQLDynamicTableHook {
	return &PostgreSQLDynamicTableHook{
		adapter:  adapter,
		registry: NewDynamicTableRegistry(),
	}
}

// RegisterDynamicTable 注册动态表配置
// 对于自动策略，创建触发器和存储函数来自动化建表
func (h *PostgreSQLDynamicTableHook) RegisterDynamicTable(ctx context.Context, config *DynamicTableConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.registry.Register(config.TableName, config); err != nil {
		return err
	}

	// 如果是自动策略且有父表，创建触发器
	if config.Strategy == "auto" && config.ParentTable != "" {
		if err := h.createAutoTrigger(ctx, config); err != nil {
			h.registry.Unregister(config.TableName)
			return fmt.Errorf("failed to create trigger: %w", err)
		}
	}

	return nil
}

// UnregisterDynamicTable 注销动态表配置
func (h *PostgreSQLDynamicTableHook) UnregisterDynamicTable(ctx context.Context, configName string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	config, err := h.registry.Get(configName)
	if err != nil {
		return err
	}

	// 如果是自动策略，删除触发器
	if config.Strategy == "auto" && config.ParentTable != "" {
		triggerName := h.generateTriggerName(config)
		functionName := h.generateFunctionName(config)

		// 删除触发器
		if err := h.dropTrigger(ctx, config.ParentTable, triggerName); err != nil {
			return fmt.Errorf("failed to drop trigger: %w", err)
		}

		// 删除函数
		if err := h.dropFunction(ctx, functionName); err != nil {
			return fmt.Errorf("failed to drop function: %w", err)
		}
	}

	return h.registry.Unregister(configName)
}

// ListDynamicTableConfigs 列出所有已注册的动态表配置
func (h *PostgreSQLDynamicTableHook) ListDynamicTableConfigs(ctx context.Context) ([]*DynamicTableConfig, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.registry.List(), nil
}

// GetDynamicTableConfig 获取特定的动态表配置
func (h *PostgreSQLDynamicTableHook) GetDynamicTableConfig(ctx context.Context, configName string) (*DynamicTableConfig, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.registry.Get(configName)
}

// CreateDynamicTable 手动创建动态表
// 返回实际创建的表名称
func (h *PostgreSQLDynamicTableHook) CreateDynamicTable(ctx context.Context, configName string, params map[string]interface{}) (string, error) {
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
func (h *PostgreSQLDynamicTableHook) ListCreatedDynamicTables(ctx context.Context, configName string) ([]string, error) {
	h.mu.RLock()
	config, err := h.registry.Get(configName)
	h.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	// 使用表名前缀搜索创建的表
	prefix := config.TableName + "_"
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_name LIKE $1 
		ORDER BY table_name
	`

	rows, err := h.adapter.Query(ctx, query, prefix+"%")
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

// createAutoTrigger 创建自动触发的触发器和函数
func (h *PostgreSQLDynamicTableHook) createAutoTrigger(ctx context.Context, config *DynamicTableConfig) error {
	functionName := h.generateFunctionName(config)
	triggerName := h.generateTriggerName(config)

	// 创建存储函数
	functionSQL := h.generatePLPgSQLFunction(config)
	if err := h.executeSQL(ctx, functionSQL); err != nil {
		return err
	}

	// 创建触发器
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s
		AFTER INSERT ON %s
		FOR EACH ROW
		WHEN (%s)
		EXECUTE FUNCTION %s();
	`,
		h.quoteIdentifier(triggerName),
		h.quoteIdentifier(config.ParentTable),
		h.buildTriggerCondition(config),
		h.quoteIdentifier(functionName),
	)

	return h.executeSQL(ctx, triggerSQL)
}

// generatePLPgSQLFunction 生成 PL/pgSQL 函数
func (h *PostgreSQLDynamicTableHook) generatePLPgSQLFunction(config *DynamicTableConfig) string {
	functionName := h.generateFunctionName(config)
	tableTemplate := config.TableName + "_" + "NEW.id"

	createTableSQL := h.generateCreateTableSQL(config, "v_table_name")

	return fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s()
		RETURNS TRIGGER AS $$
		DECLARE
			v_table_name TEXT;
		BEGIN
			-- 生成表名
			v_table_name := '%s_' || NEW.id;

			-- 检查表是否已存在
			IF NOT EXISTS(
				SELECT 1 FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = v_table_name
			) THEN
				-- 动态执行 CREATE TABLE
				EXECUTE %s;
			END IF;

			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`,
		h.quoteIdentifier(functionName),
		strings.TrimSuffix(tableTemplate, "_NEW.id"),
		h.quoteStringLiteral(createTableSQL),
	)
}

// generateCreateTableSQL 生成创建表的 SQL（用于函数中动态执行）
func (h *PostgreSQLDynamicTableHook) generateCreateTableSQL(config *DynamicTableConfig, tableNameVar string) string {
	columnsSQL := h.buildDynamicTableColumnsSQL(config)
	return fmt.Sprintf("CREATE TABLE \" || %s || \" (%s)", tableNameVar, columnsSQL)
}

// buildTriggerCondition 构建触发器条件
func (h *PostgreSQLDynamicTableHook) buildTriggerCondition(config *DynamicTableConfig) string {
	if config.TriggerCondition != "" {
		return "NEW." + config.TriggerCondition
	}
	return "TRUE"
}

// dropTrigger 删除触发器
func (h *PostgreSQLDynamicTableHook) dropTrigger(ctx context.Context, tableName, triggerName string) error {
	sql := fmt.Sprintf(
		"DROP TRIGGER IF EXISTS %s ON %s CASCADE",
		h.quoteIdentifier(triggerName),
		h.quoteIdentifier(tableName),
	)
	return h.executeSQL(ctx, sql)
}

// dropFunction 删除函数
func (h *PostgreSQLDynamicTableHook) dropFunction(ctx context.Context, functionName string) error {
	sql := fmt.Sprintf("DROP FUNCTION IF EXISTS %s() CASCADE", h.quoteIdentifier(functionName))
	return h.executeSQL(ctx, sql)
}

// createTable 创建动态表
func (h *PostgreSQLDynamicTableHook) createTable(ctx context.Context, config *DynamicTableConfig, tableName string) error {
	repo := &Repository{adapter: h.adapter}
	schema := config.toSchema(tableName)
	createSQL := buildCreateTableSQL(repo, schema)

	return h.executeSQL(ctx, createSQL)
}

func (h *PostgreSQLDynamicTableHook) buildDynamicTableColumnsSQL(config *DynamicTableConfig) string {
	adapter := Adapter(h.adapter)
	dialect := NewPostgreSQLDialect()
	schema := config.toSchema("dynamic_table_template")

	primaryFields, uniqueConstraints, fkConstraints := collectTableConstraints(adapter, schema)
	effectiveInlinePrimary := ""
	if len(primaryFields) == 1 {
		effectiveInlinePrimary = primaryFields[0]
	}

	columns := make([]string, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		columns = append(columns, buildColumnDefinition(adapter, dialect, field, field.Name == effectiveInlinePrimary))
	}

	if len(primaryFields) > 1 {
		columns = append(columns, fmt.Sprintf("PRIMARY KEY (%s)", joinQuotedIdentifiers(dialect, primaryFields)))
	}

	for _, unique := range uniqueConstraints {
		uniqueSQL := fmt.Sprintf("UNIQUE (%s)", joinQuotedIdentifiers(dialect, unique.Fields))
		if unique.Name != "" {
			uniqueSQL = fmt.Sprintf("CONSTRAINT %s %s", dialect.QuoteIdentifier(unique.Name), uniqueSQL)
		}
		columns = append(columns, uniqueSQL)
	}

	for _, fk := range fkConstraints {
		localCols := joinQuotedIdentifiers(dialect, fk.Fields)
		refTable := dialect.QuoteIdentifier(fk.RefTable)
		refCols := joinQuotedIdentifiers(dialect, fk.RefFields)
		fkSQL := fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s)", localCols, refTable, refCols)
		if fk.OnDelete != "" {
			fkSQL += " ON DELETE " + fk.OnDelete
		}
		if fk.OnUpdate != "" {
			fkSQL += " ON UPDATE " + fk.OnUpdate
		}
		if fk.Name != "" {
			fkSQL = fmt.Sprintf("CONSTRAINT %s %s", dialect.QuoteIdentifier(fk.Name), fkSQL)
		}
		columns = append(columns, fkSQL)
	}

	return strings.Join(columns, ", ")
}

// tableExists 检查表是否存在
func (h *PostgreSQLDynamicTableHook) tableExists(ctx context.Context, tableName string) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = $1
		)
	`

	var exists bool
	row := h.adapter.QueryRow(ctx, query, tableName)
	if err := row.Scan(&exists); err != nil {
		return false, err
	}

	return exists, nil
}

// generateTableName 根据参数生成表名
func (h *PostgreSQLDynamicTableHook) generateTableName(config *DynamicTableConfig, params map[string]interface{}) string {
	// 简单实现：使用 id 参数作为后缀
	if id, ok := params["id"]; ok {
		return fmt.Sprintf("%s_%v", config.TableName, id)
	}
	return config.TableName
}

// generateFunctionName 生成函数名
func (h *PostgreSQLDynamicTableHook) generateFunctionName(config *DynamicTableConfig) string {
	return "fn_create_" + config.TableName + "_table"
}

// generateTriggerName 生成触发器名
func (h *PostgreSQLDynamicTableHook) generateTriggerName(config *DynamicTableConfig) string {
	return "trg_auto_" + config.TableName
}

// quoteIdentifier 引用标识符
func (h *PostgreSQLDynamicTableHook) quoteIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

// quoteStringLiteral 引用字符串字面量
func (h *PostgreSQLDynamicTableHook) quoteStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// executeSQL 执行 SQL
func (h *PostgreSQLDynamicTableHook) executeSQL(ctx context.Context, sql string) error {
	_, err := h.adapter.Exec(ctx, sql)
	return err
}
