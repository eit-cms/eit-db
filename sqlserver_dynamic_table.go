package db

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// SQLServerDynamicTableHook SQL Server 动态表钩子实现。
// 使用原生 Trigger + T-SQL Procedure 方案实现自动建表。
type SQLServerDynamicTableHook struct {
	adapter  *SQLServerAdapter
	registry *DynamicTableRegistry
	mu       sync.RWMutex
}

// NewSQLServerDynamicTableHook 创建 SQL Server 动态表钩子。
func NewSQLServerDynamicTableHook(adapter *SQLServerAdapter) *SQLServerDynamicTableHook {
	return &SQLServerDynamicTableHook{
		adapter:  adapter,
		registry: NewDynamicTableRegistry(),
	}
}

// RegisterDynamicTable 注册动态表配置。
func (h *SQLServerDynamicTableHook) RegisterDynamicTable(ctx context.Context, config *DynamicTableConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.registry.Register(config.TableName, config); err != nil {
		return err
	}

	if config.Strategy == "auto" && config.ParentTable != "" {
		if err := h.createAutoTrigger(ctx, config); err != nil {
			_ = h.registry.Unregister(config.TableName)
			return fmt.Errorf("failed to create sqlserver trigger/procedure: %w", err)
		}
	}

	return nil
}

// UnregisterDynamicTable 注销动态表配置。
func (h *SQLServerDynamicTableHook) UnregisterDynamicTable(ctx context.Context, configName string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	config, err := h.registry.Get(configName)
	if err != nil {
		return err
	}

	if config.Strategy == "auto" && config.ParentTable != "" {
		triggerName := h.generateTriggerName(config)
		procName := h.generateProcedureName(config)

		if err := h.dropTrigger(ctx, config.ParentTable, triggerName); err != nil {
			return fmt.Errorf("failed to drop sqlserver trigger: %w", err)
		}
		if err := h.dropProcedure(ctx, procName); err != nil {
			return fmt.Errorf("failed to drop sqlserver procedure: %w", err)
		}
	}

	return h.registry.Unregister(configName)
}

// ListDynamicTableConfigs 列出所有已注册的动态表配置。
func (h *SQLServerDynamicTableHook) ListDynamicTableConfigs(ctx context.Context) ([]*DynamicTableConfig, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.registry.List(), nil
}

// GetDynamicTableConfig 获取特定的动态表配置。
func (h *SQLServerDynamicTableHook) GetDynamicTableConfig(ctx context.Context, configName string) (*DynamicTableConfig, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.registry.Get(configName)
}

// CreateDynamicTable 手动创建动态表。
func (h *SQLServerDynamicTableHook) CreateDynamicTable(ctx context.Context, configName string, params map[string]interface{}) (string, error) {
	h.mu.RLock()
	config, err := h.registry.Get(configName)
	h.mu.RUnlock()
	if err != nil {
		return "", err
	}

	tableName := h.generateTableName(config, params)
	exists, err := h.tableExists(ctx, tableName)
	if err != nil {
		return "", err
	}
	if exists {
		return tableName, fmt.Errorf("table already exists: %s", tableName)
	}

	if err := h.createTable(ctx, config, tableName); err != nil {
		return "", err
	}
	return tableName, nil
}

// ListCreatedDynamicTables 获取已创建的动态表列表。
func (h *SQLServerDynamicTableHook) ListCreatedDynamicTables(ctx context.Context, configName string) ([]string, error) {
	h.mu.RLock()
	config, err := h.registry.Get(configName)
	h.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	prefix := config.TableName + "_"
	query := `
		SELECT t.name
		FROM sys.tables t
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = 'dbo' AND t.name LIKE @p1
		ORDER BY t.name
	`

	rows, err := h.adapter.Query(ctx, query, prefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var tableName string
		if scanErr := rows.Scan(&tableName); scanErr != nil {
			return nil, scanErr
		}
		tables = append(tables, tableName)
	}
	return tables, rows.Err()
}

func (h *SQLServerDynamicTableHook) createAutoTrigger(ctx context.Context, config *DynamicTableConfig) error {
	procSQL := h.generateTSQLProcedure(config)
	if err := h.executeSQL(ctx, procSQL); err != nil {
		return err
	}

	triggerSQL := h.generateTSQLTrigger(config)
	return h.executeSQL(ctx, triggerSQL)
}

func (h *SQLServerDynamicTableHook) generateTSQLProcedure(config *DynamicTableConfig) string {
	procName := h.generateProcedureName(config)
	columnsSQL := h.buildDynamicTableColumnsSQL(config)
	prefixLiteral := h.quoteStringLiteral(config.TableName)

	return fmt.Sprintf(`
CREATE OR ALTER PROCEDURE %s
	@entity_id NVARCHAR(128)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @base_table_name NVARCHAR(256) = %s + '_' + @entity_id;
	DECLARE @quoted_table_name NVARCHAR(256) = QUOTENAME(@base_table_name);
	DECLARE @sql NVARCHAR(MAX);

	IF NOT EXISTS (
		SELECT 1
		FROM sys.tables t
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = 'dbo' AND t.name = @base_table_name
	)
	BEGIN
		SET @sql = N'CREATE TABLE [dbo].' + @quoted_table_name + N' (%s)';
		EXEC sp_executesql @sql;
	END
END
`, h.quoteIdentifier(procName), prefixLiteral, columnsSQL)
}

func (h *SQLServerDynamicTableHook) generateTSQLTrigger(config *DynamicTableConfig) string {
	triggerName := h.generateTriggerName(config)
	parentTable := h.quoteIdentifier(config.ParentTable)
	procName := h.quoteIdentifier(h.generateProcedureName(config))
	condition := h.buildTriggerCondition(config)

	return fmt.Sprintf(`
CREATE OR ALTER TRIGGER %s
ON [dbo].%s
AFTER INSERT
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @id NVARCHAR(128);
	DECLARE cur_inserted_id CURSOR LOCAL FAST_FORWARD FOR
		SELECT CAST(i.[id] AS NVARCHAR(128))
		FROM inserted i
		WHERE %s;

	OPEN cur_inserted_id;
	FETCH NEXT FROM cur_inserted_id INTO @id;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		EXEC %s @entity_id = @id;
		FETCH NEXT FROM cur_inserted_id INTO @id;
	END

	CLOSE cur_inserted_id;
	DEALLOCATE cur_inserted_id;
END
`, h.quoteIdentifier(triggerName), parentTable, condition, procName)
}

func (h *SQLServerDynamicTableHook) buildDynamicTableColumnsSQL(config *DynamicTableConfig) string {
	adapter := Adapter(h.adapter)
	dialect := NewSQLServerDialect()
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

func (h *SQLServerDynamicTableHook) buildTriggerCondition(config *DynamicTableConfig) string {
	if strings.TrimSpace(config.TriggerCondition) == "" {
		return "1 = 1"
	}
	condition := strings.TrimSpace(config.TriggerCondition)
	if strings.HasPrefix(condition, "i.") || strings.HasPrefix(condition, "I.") {
		return condition
	}
	return "i." + condition
}

func (h *SQLServerDynamicTableHook) dropTrigger(ctx context.Context, tableName, triggerName string) error {
	sql := fmt.Sprintf(`
IF OBJECT_ID(N'[dbo].%s', N'TR') IS NOT NULL
	DROP TRIGGER [dbo].%s;
`, h.escapeIdentifierName(triggerName), h.escapeIdentifierName(triggerName))
	return h.executeSQL(ctx, sql)
}

func (h *SQLServerDynamicTableHook) dropProcedure(ctx context.Context, procName string) error {
	sql := fmt.Sprintf(`
IF OBJECT_ID(N'[dbo].%s', N'P') IS NOT NULL
	DROP PROCEDURE [dbo].%s;
`, h.escapeIdentifierName(procName), h.escapeIdentifierName(procName))
	return h.executeSQL(ctx, sql)
}

func (h *SQLServerDynamicTableHook) createTable(ctx context.Context, config *DynamicTableConfig, tableName string) error {
	repo := &Repository{adapter: h.adapter}
	schema := config.toSchema(tableName)
	createSQL := buildCreateTableSQL(repo, schema)
	return h.executeSQL(ctx, createSQL)
}

func (h *SQLServerDynamicTableHook) tableExists(ctx context.Context, tableName string) (bool, error) {
	query := `
		SELECT COUNT(1)
		FROM sys.tables t
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = 'dbo' AND t.name = @p1
	`
	var count int
	row := h.adapter.QueryRow(ctx, query, tableName)
	if err := row.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (h *SQLServerDynamicTableHook) generateTableName(config *DynamicTableConfig, params map[string]interface{}) string {
	if id, ok := params["id"]; ok {
		return fmt.Sprintf("%s_%v", config.TableName, id)
	}
	return config.TableName
}

func (h *SQLServerDynamicTableHook) generateProcedureName(config *DynamicTableConfig) string {
	return "sp_create_" + config.TableName + "_table"
}

func (h *SQLServerDynamicTableHook) generateTriggerName(config *DynamicTableConfig) string {
	return "trg_auto_" + config.TableName
}

func (h *SQLServerDynamicTableHook) quoteIdentifier(name string) string {
	return "[" + h.escapeIdentifierName(name) + "]"
}

func (h *SQLServerDynamicTableHook) escapeIdentifierName(name string) string {
	return strings.ReplaceAll(name, "]", "]]")
}

func (h *SQLServerDynamicTableHook) quoteStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func (h *SQLServerDynamicTableHook) executeSQL(ctx context.Context, sql string) error {
	_, err := h.adapter.Exec(ctx, sql)
	return err
}
