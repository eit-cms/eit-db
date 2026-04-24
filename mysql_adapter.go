package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// MySQLAdapter MySQL 数据库适配器
type MySQLAdapter struct {
	config *Config
	db     *gorm.DB
	sqlDB  *sql.DB
}

type mysqlScheduledTaskRecord struct {
	TaskName       string
	TaskType       string
	CronExpression string
	Description    string
	Enabled        bool
	ConfigJSON     string
	ProcedureName  string
	ScheduleMode   string
	EventName      string
	LastStatus     sql.NullString
	LastError      sql.NullString
	LastExecutedAt sql.NullTime
	NextExecuteAt  sql.NullTime
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type mysqlEventRuntime struct {
	Status       sql.NullString
	Starts       sql.NullTime
	LastExecuted sql.NullTime
}

// MySQLFactory MySQL 适配器工厂
type MySQLFactory struct{}

// Name 返回适配器名称
func (f *MySQLFactory) Name() string {
	return "mysql"
}

// Create 创建 MySQL 适配器
func (f *MySQLFactory) Create(config *Config) (Adapter, error) {
	adapter := &MySQLAdapter{config: config}
	if err := adapter.Connect(context.Background(), config); err != nil {
		return nil, err
	}
	return adapter, nil
}

// Connect 连接到 MySQL 数据库
func (a *MySQLAdapter) Connect(ctx context.Context, config *Config) error {
	if config == nil {
		config = a.config
	}
	resolved := config.ResolvedMySQLConfig()

	// 验证必需字段
	if resolved.Host == "" {
		resolved.Host = "localhost"
	}
	if resolved.Port == 0 {
		resolved.Port = 3306
	}
	if resolved.Username == "" && strings.TrimSpace(resolved.DSN) == "" {
		return fmt.Errorf("MySQL: username is required")
	}
	if resolved.Database == "" && strings.TrimSpace(resolved.DSN) == "" {
		return fmt.Errorf("MySQL: database name is required")
	}

	// 处理空密码
	password := resolved.Password
	connectTimeout := 30
	if config != nil && config.Pool != nil && config.Pool.ConnectTimeout > 0 {
		connectTimeout = config.Pool.ConnectTimeout
	}

	// 构建 DSN (Data Source Name)
	// 格式: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	var dsn string
	if strings.TrimSpace(resolved.DSN) != "" {
		dsn = withMySQLTimeoutParams(resolved.DSN, connectTimeout)
	} else {
		dsn = fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true&timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
			resolved.Username,
			password,
			resolved.Host,
			resolved.Port,
			resolved.Database,
			connectTimeout,
			connectTimeout,
			connectTimeout,
		)
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL (host=%s, port=%d, user=%s, db=%s): %w",
			resolved.Host, resolved.Port, resolved.Username, resolved.Database, err)
	}

	a.db = db

	// 获取底层 sql.DB 对象
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}
	a.sqlDB = sqlDB

	// 配置连接池（使用Config中的Pool设置）
	if config.Pool != nil {
		maxConns := config.Pool.MaxConnections
		if maxConns <= 0 {
			maxConns = 25
		}
		sqlDB.SetMaxOpenConns(maxConns)

		idleTimeout := config.Pool.IdleTimeout
		if idleTimeout <= 0 {
			idleTimeout = 300 // 5分钟
		}
		sqlDB.SetConnMaxIdleTime(time.Duration(idleTimeout) * time.Second)

		if config.Pool.MaxLifetime > 0 {
			sqlDB.SetConnMaxLifetime(time.Duration(config.Pool.MaxLifetime) * time.Second)
		}
	} else {
		// 默认连接池配置
		sqlDB.SetMaxOpenConns(25)
		sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	}

	return nil
}

// Close 关闭数据库连接
func (a *MySQLAdapter) Close() error {
	if a.sqlDB != nil {
		return a.sqlDB.Close()
	}
	return nil
}

// Ping 测试数据库连接
func (a *MySQLAdapter) Ping(ctx context.Context) error {
	if a.sqlDB == nil {
		return fmt.Errorf("database not connected")
	}
	return a.sqlDB.PingContext(ctx)
}

// Query 执行查询 (返回多行)
func (a *MySQLAdapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return a.sqlDB.QueryContext(ctx, query, args...)
}

// QueryRow 执行查询 (返回单行)
func (a *MySQLAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return a.sqlDB.QueryRowContext(ctx, query, args...)
}

// Exec 执行操作 (INSERT/UPDATE/DELETE)
func (a *MySQLAdapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return a.sqlDB.ExecContext(ctx, query, args...)
}

// Begin 开始事务
func (a *MySQLAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	txOpts := &sql.TxOptions{}
	for _, opt := range opts {
		if o, ok := opt.(*sql.TxOptions); ok {
			txOpts = o
		}
	}

	sqlTx, err := a.sqlDB.BeginTx(ctx, txOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &MySQLTx{tx: sqlTx}, nil
}

// GetRawConn 获取底层连接 (返回 *sql.DB)
func (a *MySQLAdapter) GetRawConn() interface{} {
	return a.sqlDB
}

// GetGormDB 获取GORM实例（用于直接访问GORM）
// Deprecated: Adapter 层不再暴露 GORM 连接。
func (a *MySQLAdapter) GetGormDB() *gorm.DB {
	return nil
}

func withMySQLTimeoutParams(dsn string, timeoutSeconds int) string {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return trimmed
	}

	params := []string{
		fmt.Sprintf("timeout=%ds", timeoutSeconds),
		fmt.Sprintf("readTimeout=%ds", timeoutSeconds),
		fmt.Sprintf("writeTimeout=%ds", timeoutSeconds),
	}

	if !strings.Contains(trimmed, "?") {
		return trimmed + "?" + strings.Join(params, "&")
	}

	for _, p := range []string{"timeout=", "readTimeout=", "writeTimeout="} {
		if strings.Contains(trimmed, p) {
			return trimmed
		}
	}

	separator := "&"
	if strings.HasSuffix(trimmed, "?") || strings.HasSuffix(trimmed, "&") {
		separator = ""
	}
	return trimmed + separator + strings.Join(params, "&")
}

func (a *MySQLAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	if task == nil {
		return fmt.Errorf("task cannot be nil")
	}
	if err := task.Validate(); err != nil {
		return err
	}
	if task.Type != TaskTypeMonthlyTableCreation {
		return fmt.Errorf("unsupported task type for MySQL: %s", task.Type)
	}

	eventSchedulerEnabled, err := a.isMySQLEventSchedulerEnabled(ctx)
	if err != nil {
		return fmt.Errorf("failed to detect MySQL EVENT scheduler availability: %w", err)
	}
	if !eventSchedulerEnabled {
		return NewScheduledTaskFallbackErrorWithReason(
			"mysql",
			ScheduledTaskFallbackReasonNativeCapabilityMissing,
			"EVENT scheduler is disabled or unavailable",
		)
	}

	if err := a.ensureScheduledTaskMetadataTable(ctx); err != nil {
		return err
	}

	existingRecord, err := a.getScheduledTaskRecord(ctx, task.Name)
	if err != nil {
		return err
	}

	procedureName := scheduledTaskCreateTableRoutineName(task.Name)
	if err := a.registerMonthlyTableCreationProcedure(ctx, procedureName, task); err != nil {
		return err
	}
	if err := a.preWarmMonthlyTable(ctx, task); err != nil {
		return err
	}

	if existingRecord != nil {
		if strings.TrimSpace(existingRecord.EventName) != "" {
			if err := a.dropEvent(ctx, existingRecord.EventName); err != nil {
				return fmt.Errorf("failed to replace existing MySQL EVENT for %s: %w", task.Name, err)
			}
		}
	}

	scheduleSpec := strings.TrimSpace(task.CronExpression)
	if scheduleSpec == "" {
		scheduleSpec = defaultMonthlyCronSpec
	}
	nextExecutionAt, err := computeNextScheduledRun(scheduleSpec, time.Now())
	if err != nil {
		return fmt.Errorf("invalid cron expression %q for task %s: %w", scheduleSpec, task.Name, err)
	}

	eventName := scheduledTaskEventName(task.Name)
	if err := a.createMySQLEvent(ctx, eventName, procedureName, nextExecutionAt); err != nil {
		return err
	}

	configJSON, err := json.Marshal(task.Config)
	if err != nil {
		return fmt.Errorf("failed to encode task config: %w", err)
	}

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (
			task_name, task_type, cron_expression, description, enabled,
			config_json, procedure_name, schedule_mode, event_name,
			last_status, last_error, last_executed_at, next_execute_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(), UTC_TIMESTAMP())
		ON DUPLICATE KEY UPDATE
			task_type = VALUES(task_type),
			cron_expression = VALUES(cron_expression),
			description = VALUES(description),
			enabled = VALUES(enabled),
			config_json = VALUES(config_json),
			procedure_name = VALUES(procedure_name),
			schedule_mode = VALUES(schedule_mode),
			event_name = VALUES(event_name),
			last_status = VALUES(last_status),
			last_error = VALUES(last_error),
			last_executed_at = VALUES(last_executed_at),
			next_execute_at = VALUES(next_execute_at),
			updated_at = UTC_TIMESTAMP()
	`, a.quoteIdentifier(scheduledTaskMetadataTable))

	if err := a.db.WithContext(ctx).Exec(
		insertSQL,
		task.Name,
		string(task.Type),
		scheduleSpec,
		task.Description,
		task.Enabled,
		string(configJSON),
		procedureName,
		"mysql_event",
		eventName,
		"scheduled",
		nil,
		nil,
		nextExecutionAt,
	).Error; err != nil {
		return fmt.Errorf("failed to persist MySQL scheduled task metadata for %s: %w", task.Name, err)
	}

	return nil
}

func (a *MySQLAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	if strings.TrimSpace(taskName) == "" {
		return fmt.Errorf("task name cannot be empty")
	}
	if err := a.ensureScheduledTaskMetadataTable(ctx); err != nil {
		return err
	}

	record, err := a.getScheduledTaskRecord(ctx, taskName)
	if err != nil {
		return err
	}
	if record == nil {
		return fmt.Errorf("scheduled task not found: %s", taskName)
	}

	if strings.TrimSpace(record.EventName) != "" {
		if err := a.dropEvent(ctx, record.EventName); err != nil && !strings.Contains(strings.ToLower(err.Error()), "unknown event") {
			return fmt.Errorf("failed to drop MySQL EVENT %s: %w", record.EventName, err)
		}
	}
	if strings.TrimSpace(record.ProcedureName) != "" {
		dropProcSQL := fmt.Sprintf("DROP PROCEDURE IF EXISTS %s", a.quoteIdentifier(record.ProcedureName))
		if err := a.db.WithContext(ctx).Exec(dropProcSQL).Error; err != nil {
			return fmt.Errorf("failed to drop MySQL scheduled task procedure %s: %w", record.ProcedureName, err)
		}
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE task_name = ?", a.quoteIdentifier(scheduledTaskMetadataTable))
	if err := a.db.WithContext(ctx).Exec(deleteSQL, taskName).Error; err != nil {
		return fmt.Errorf("failed to delete MySQL scheduled task metadata for %s: %w", taskName, err)
	}

	return nil
}

func (a *MySQLAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	if err := a.ensureScheduledTaskMetadataTable(ctx); err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT task_name, task_type, cron_expression, description, enabled,
			config_json, procedure_name, schedule_mode, event_name,
			last_status, last_error, last_executed_at, next_execute_at,
			created_at, updated_at
		FROM %s
		ORDER BY created_at ASC
	`, a.quoteIdentifier(scheduledTaskMetadataTable))

	rows, err := a.sqlDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list MySQL scheduled tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]*ScheduledTaskStatus, 0)
	for rows.Next() {
		var record mysqlScheduledTaskRecord
		if err := rows.Scan(
			&record.TaskName,
			&record.TaskType,
			&record.CronExpression,
			&record.Description,
			&record.Enabled,
			&record.ConfigJSON,
			&record.ProcedureName,
			&record.ScheduleMode,
			&record.EventName,
			&record.LastStatus,
			&record.LastError,
			&record.LastExecutedAt,
			&record.NextExecuteAt,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan MySQL scheduled task metadata: %w", err)
		}

		info := map[string]interface{}{
			"description":    record.Description,
			"enabled":        record.Enabled,
			"procedure_name": record.ProcedureName,
			"schedule_mode":  record.ScheduleMode,
			"event_name":     record.EventName,
		}
		if strings.TrimSpace(record.CronExpression) != "" {
			info["cron"] = record.CronExpression
		}
		if record.LastStatus.Valid {
			info["last_status"] = record.LastStatus.String
		}
		if record.LastError.Valid {
			info["last_error"] = record.LastError.String
		}
		if strings.TrimSpace(record.EventName) != "" {
			if runtime, runtimeErr := a.getEventRuntime(ctx, record.EventName); runtimeErr == nil && runtime != nil {
				if runtime.Status.Valid {
					info["event_status"] = runtime.Status.String
				}
				if runtime.LastExecuted.Valid {
					record.LastExecutedAt = runtime.LastExecuted
				}
				if runtime.Starts.Valid {
					record.NextExecuteAt = runtime.Starts
				}
			} else if runtimeErr != nil {
				info["event_state_error"] = runtimeErr.Error()
			}
		}

		status := &ScheduledTaskStatus{
			Name:      record.TaskName,
			Type:      ScheduledTaskType(record.TaskType),
			Running:   false,
			CreatedAt: record.CreatedAt.Unix(),
			Info:      info,
		}
		if record.LastExecutedAt.Valid {
			status.LastExecutedAt = record.LastExecutedAt.Time.Unix()
		}
		if record.NextExecuteAt.Valid {
			status.NextExecutedAt = record.NextExecuteAt.Time.Unix()
		}
		tasks = append(tasks, status)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate MySQL scheduled tasks: %w", err)
	}

	return tasks, nil
}

func (a *MySQLAdapter) ensureScheduledTaskMetadataTable(ctx context.Context) error {
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			task_name VARCHAR(255) NOT NULL PRIMARY KEY,
			task_type VARCHAR(100) NOT NULL,
			cron_expression VARCHAR(255) NULL,
			description VARCHAR(1000) NULL,
			enabled BOOLEAN NOT NULL DEFAULT TRUE,
			config_json JSON NOT NULL,
			procedure_name VARCHAR(255) NOT NULL,
			schedule_mode VARCHAR(100) NOT NULL,
			event_name VARCHAR(255) NULL,
			last_status VARCHAR(100) NULL,
			last_error TEXT NULL,
			last_executed_at DATETIME NULL,
			next_execute_at DATETIME NULL,
			created_at DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
			updated_at DATETIME NOT NULL DEFAULT UTC_TIMESTAMP() ON UPDATE UTC_TIMESTAMP()
		)
	`, a.quoteIdentifier(scheduledTaskMetadataTable))

	if err := a.db.WithContext(ctx).Exec(createTableSQL).Error; err != nil {
		return fmt.Errorf("failed to ensure MySQL scheduled task metadata table: %w", err)
	}
	return nil
}

func (a *MySQLAdapter) isMySQLEventSchedulerEnabled(ctx context.Context) (bool, error) {
	rows, err := a.sqlDB.QueryContext(ctx, "SHOW VARIABLES LIKE 'event_scheduler'")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		var variableName string
		var value string
		if err := rows.Scan(&variableName, &value); err != nil {
			return false, err
		}
		return strings.EqualFold(value, "ON") || value == "1", nil
	}

	return false, rows.Err()
}

func (a *MySQLAdapter) registerMonthlyTableCreationProcedure(ctx context.Context, procedureName string, task *ScheduledTaskConfig) error {
	config := task.GetMonthlyTableConfig()
	tableName, _ := config["tableName"].(string)
	monthFormat, _ := config["monthFormat"].(string)
	fieldDefs, _ := config["fieldDefinitions"].(string)
	mysqlFormat := mysqlDateFormatFromGoLayout(monthFormat)

	procedureSQL := fmt.Sprintf(`
		CREATE PROCEDURE %s()
		BEGIN
			DECLARE new_table_name VARCHAR(255);
			SET new_table_name = CONCAT('%s_', DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 1 MONTH), '%s'));
			IF NOT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = DATABASE() AND table_name = new_table_name
			) THEN
				SET @ddl = CONCAT('CREATE TABLE ', CHAR(96), REPLACE(new_table_name, CHAR(96), CONCAT(CHAR(96), CHAR(96))), CHAR(96), ' (%s)');
				PREPARE stmt FROM @ddl;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
			END IF;
		END
	`, a.quoteIdentifier(procedureName), a.escapeStringLiteral(tableName), a.escapeStringLiteral(mysqlFormat), a.escapeStringLiteral(fieldDefs))

	if err := a.db.WithContext(ctx).Exec(fmt.Sprintf("DROP PROCEDURE IF EXISTS %s", a.quoteIdentifier(procedureName))).Error; err != nil {
		return fmt.Errorf("failed to reset MySQL scheduled task procedure %s: %w", procedureName, err)
	}
	if err := a.db.WithContext(ctx).Exec(procedureSQL).Error; err != nil {
		return fmt.Errorf("failed to create MySQL scheduled task procedure %s: %w", procedureName, err)
	}
	return nil
}

func (a *MySQLAdapter) preWarmMonthlyTable(ctx context.Context, task *ScheduledTaskConfig) error {
	config := task.GetMonthlyTableConfig()
	tableName, _ := config["tableName"].(string)
	monthFormat, _ := config["monthFormat"].(string)
	fieldDefs, _ := config["fieldDefinitions"].(string)
	currentTable := fmt.Sprintf("%s_%s", tableName, time.Now().Format(monthFormat))
	preWarmSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", a.quoteIdentifier(currentTable), fieldDefs)
	if err := a.db.WithContext(ctx).Exec(preWarmSQL).Error; err != nil {
		return fmt.Errorf("failed to pre-warm MySQL monthly table %s: %w", currentTable, err)
	}
	return nil
}

func (a *MySQLAdapter) createMySQLEvent(ctx context.Context, eventName, procedureName string, startsAt time.Time) error {
	if err := a.dropEvent(ctx, eventName); err != nil && !strings.Contains(strings.ToLower(err.Error()), "unknown event") {
		return fmt.Errorf("failed to reset MySQL EVENT %s: %w", eventName, err)
	}

	eventSQL := fmt.Sprintf(`
		CREATE EVENT %s
		ON SCHEDULE EVERY 1 MONTH STARTS '%s'
		DO CALL %s()
	`, a.quoteIdentifier(eventName), startsAt.UTC().Format("2006-01-02 15:04:05"), a.quoteIdentifier(procedureName))

	if err := a.db.WithContext(ctx).Exec(eventSQL).Error; err != nil {
		return fmt.Errorf("failed to create MySQL EVENT %s: %w", eventName, err)
	}
	return nil
}

func (a *MySQLAdapter) dropEvent(ctx context.Context, eventName string) error {
	return a.db.WithContext(ctx).Exec(fmt.Sprintf("DROP EVENT IF EXISTS %s", a.quoteIdentifier(eventName))).Error
}

func (a *MySQLAdapter) getScheduledTaskRecord(ctx context.Context, taskName string) (*mysqlScheduledTaskRecord, error) {
	query := fmt.Sprintf(`
		SELECT task_name, task_type, cron_expression, description, enabled,
			config_json, procedure_name, schedule_mode, event_name,
			last_status, last_error, last_executed_at, next_execute_at,
			created_at, updated_at
		FROM %s WHERE task_name = ?
	`, a.quoteIdentifier(scheduledTaskMetadataTable))

	var record mysqlScheduledTaskRecord
	err := a.sqlDB.QueryRowContext(ctx, query, taskName).Scan(
		&record.TaskName,
		&record.TaskType,
		&record.CronExpression,
		&record.Description,
		&record.Enabled,
		&record.ConfigJSON,
		&record.ProcedureName,
		&record.ScheduleMode,
		&record.EventName,
		&record.LastStatus,
		&record.LastError,
		&record.LastExecutedAt,
		&record.NextExecuteAt,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query MySQL scheduled task metadata for %s: %w", taskName, err)
	}
	return &record, nil
}

func (a *MySQLAdapter) getEventRuntime(ctx context.Context, eventName string) (*mysqlEventRuntime, error) {
	query := `
		SELECT STATUS, STARTS, LAST_EXECUTED
		FROM information_schema.EVENTS
		WHERE EVENT_SCHEMA = DATABASE() AND EVENT_NAME = ?
	`

	var runtime mysqlEventRuntime
	err := a.sqlDB.QueryRowContext(ctx, query, eventName).Scan(&runtime.Status, &runtime.Starts, &runtime.LastExecuted)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &runtime, nil
}

func mysqlDateFormatFromGoLayout(layout string) string {
	switch strings.TrimSpace(layout) {
	case "", "2006_01":
		return "%Y_%m"
	case "200601":
		return "%Y%m"
	case "2006-01":
		return "%Y-%m"
	default:
		return "%Y_%m"
	}
}

func (a *MySQLAdapter) quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (a *MySQLAdapter) escapeStringLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// MySQLTx MySQL 事务实现
type MySQLTx struct {
	tx *sql.Tx
}

// Commit 提交事务
func (t *MySQLTx) Commit(ctx context.Context) error {
	return t.tx.Commit()
}

// Rollback 回滚事务
func (t *MySQLTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback()
}

// Exec 在事务中执行
func (t *MySQLTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query 在事务中查询
func (t *MySQLTx) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// QueryRow 在事务中查询单行
func (t *MySQLTx) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

// GetQueryBuilderProvider 返回查询构造器提供者
func (a *MySQLAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return NewDefaultSQLQueryConstructorProvider(NewMySQLDialect())
}

// GetDatabaseFeatures 返回 MySQL 数据库特性
func (a *MySQLAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{
		// 索引和约束
		SupportsCompositeKeys:        true,
		SupportsForeignKeys:          true,
		SupportsCompositeForeignKeys: true,
		SupportsCompositeIndexes:     true,
		SupportsPartialIndexes:       false, // 8.0.13+ functional indexes only
		SupportsDeferrable:           false,

		// 自定义类型
		SupportsEnumType:      true, // Column-level ENUM
		SupportsCompositeType: false,
		SupportsDomainType:    false,
		SupportsUDT:           false,

		// 函数和过程
		SupportsStoredProcedures: true,
		SupportsFunctions:        true,
		SupportsAggregateFuncs:   false,
		FunctionLanguages:        []string{"sql"},

		// 高级查询
		SupportsWindowFunctions: true, // 8.0+
		SupportsCTE:             true, // 8.0+
		SupportsRecursiveCTE:    true, // 8.0+
		SupportsMaterializedCTE: false,

		// JSON 支持
		HasNativeJSON:     true, // 5.7+
		SupportsJSONPath:  true,
		SupportsJSONIndex: true, // 8.0+

		// 全文搜索
		SupportsFullTextSearch: true,
		FullTextLanguages:      []string{"english"},

		// 其他特性
		SupportsArrays:       false,
		SupportsGenerated:    true, // 5.7+
		SupportsReturning:    false,
		SupportsUpsert:       true, // ON DUPLICATE KEY UPDATE
		SupportsListenNotify: false,

		// 元信息
		DatabaseName:    "MySQL",
		DatabaseVersion: "8.0+",
		Description:     "Popular open-source database with good performance",

		FeatureSupport: map[string]FeatureSupport{
			"window_functions": {Supported: true, MinVersion: "8.0", Notes: "MySQL 8.0+"},
			"cte":              {Supported: true, MinVersion: "8.0", Notes: "MySQL 8.0+"},
			"recursive_cte":    {Supported: true, MinVersion: "8.0", Notes: "MySQL 8.0+"},
			"native_json":      {Supported: true, MinVersion: "5.7", Notes: "MySQL 5.7+"},
			"json_path":        {Supported: true, MinVersion: "5.7", Notes: "MySQL 5.7+"},
			"json_index":       {Supported: true, MinVersion: "8.0.13", Notes: "functional index on JSON expression"},
			"generated":        {Supported: true, MinVersion: "5.7", Notes: "generated columns"},
			"full_text_search": {Supported: true, MinVersion: "5.6", Notes: "InnoDB FTS in modern versions"},
		},
		FallbackStrategies: map[string]FeatureFallback{
			"window_functions": FallbackApplicationLayer,
			"cte":              FallbackApplicationLayer,
			"recursive_cte":    FallbackApplicationLayer,
			"native_json":      FallbackApplicationLayer,
			"json_path":        FallbackApplicationLayer,
			"json_index":       FallbackApplicationLayer,
			"generated":        FallbackApplicationLayer,
		},
	}
}

// GetQueryFeatures 返回 MySQL 的查询特性
func (a *MySQLAdapter) GetQueryFeatures() *QueryFeatures {
	return NewMySQLQueryFeatures()
}

// init 自动注册 MySQL 适配器
func init() {
	MustRegisterAdapterDescriptor("mysql", newMySQLAdapterDescriptor())
}
