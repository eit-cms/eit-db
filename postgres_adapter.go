package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// PostgreSQLAdapter PostgreSQL 数据库适配器
type PostgreSQLAdapter struct {
	config *Config
	db     *gorm.DB
	sqlDB  *sql.DB
}

type postgresScheduledTaskRecord struct {
	TaskName       string
	TaskType       string
	CronExpression string
	Description    string
	Enabled        bool
	ConfigJSON     string
	FunctionName   string
	ScheduleMode   string
	PGCronJobID    sql.NullInt64
	LastStatus     sql.NullString
	LastError      sql.NullString
	LastExecutedAt sql.NullTime
	NextExecuteAt  sql.NullTime
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type postgresCronJobRuntime struct {
	Schedule string
	Command  string
	Active   bool
}

type postgresCronJobRunDetail struct {
	Status        sql.NullString
	ReturnMessage sql.NullString
	StartedAt     sql.NullTime
	FinishedAt    sql.NullTime
}

// PostgreSQLFactory PostgreSQL 适配器工厂
type PostgreSQLFactory struct{}

// Name 返回适配器名称
func (f *PostgreSQLFactory) Name() string {
	return "postgres"
}

// Create 创建 PostgreSQL 适配器
func (f *PostgreSQLFactory) Create(config *Config) (Adapter, error) {
	adapter := &PostgreSQLAdapter{config: config}
	if err := adapter.Connect(context.Background(), config); err != nil {
		return nil, err
	}
	return adapter, nil
}

// Connect 连接到 PostgreSQL 数据库
func (a *PostgreSQLAdapter) Connect(ctx context.Context, config *Config) error {
	if config == nil {
		config = a.config
	}
	resolved := config.ResolvedPostgresConfig()

	// 验证必需字段
	if resolved.Host == "" {
		resolved.Host = "localhost"
	}
	if resolved.Port == 0 {
		resolved.Port = 5432
	}
	if resolved.Username == "" && strings.TrimSpace(resolved.DSN) == "" {
		return fmt.Errorf("PostgreSQL: username is required")
	}
	if resolved.Database == "" && strings.TrimSpace(resolved.DSN) == "" {
		return fmt.Errorf("PostgreSQL: database name is required")
	}
	if resolved.SSLMode == "" {
		resolved.SSLMode = "disable"
	}

	// 处理空密码（支持trust和ident认证）
	password := resolved.Password

	// 构建 DSN (Data Source Name)
	// lib/pq 格式: postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
	// 或使用键值格式: host=localhost port=5432 user=postgres password=secret dbname=mydb
	var dsn string
	if strings.TrimSpace(resolved.DSN) != "" {
		dsn = resolved.DSN
	} else if password != "" {
		dsn = fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			resolved.Host,
			resolved.Port,
			resolved.Username,
			password,
			resolved.Database,
			resolved.SSLMode,
		)
	} else {
		// 处理无密码的情况（信任认证）
		dsn = fmt.Sprintf(
			"host=%s port=%d user=%s dbname=%s sslmode=%s",
			resolved.Host,
			resolved.Port,
			resolved.Username,
			resolved.Database,
			resolved.SSLMode,
		)
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL (host=%s, port=%d, user=%s, db=%s, ssl=%s): %w",
			resolved.Host, resolved.Port, resolved.Username, resolved.Database, resolved.SSLMode, err)
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
func (a *PostgreSQLAdapter) Close() error {
	if a.sqlDB != nil {
		return a.sqlDB.Close()
	}
	return nil
}

// Ping 测试数据库连接
func (a *PostgreSQLAdapter) Ping(ctx context.Context) error {
	if a.sqlDB == nil {
		return fmt.Errorf("database not connected")
	}
	return a.sqlDB.PingContext(ctx)
}

// InspectFullTextRuntime 检查 PostgreSQL 全文能力与常见分词插件（zhparser/pg_jieba/pgroonga）。
// 约定：当调用方要求插件分词加速时，若插件不存在可据此降级。
func (a *PostgreSQLAdapter) InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error) {
	if a.sqlDB == nil {
		return nil, fmt.Errorf("database not connected")
	}

	rows, err := a.sqlDB.QueryContext(ctx, `
		SELECT extname
		FROM pg_extension
		WHERE extname IN ('zhparser', 'pg_jieba', 'pgroonga')
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	plugins := make([]string, 0)
	for rows.Next() {
		var ext string
		if scanErr := rows.Scan(&ext); scanErr != nil {
			return nil, scanErr
		}
		plugins = append(plugins, ext)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	cap := &FullTextRuntimeCapability{
		NativeSupported:       true,
		PluginChecked:         true,
		PluginAvailable:       len(plugins) > 0,
		TokenizationSupported: true,
		TokenizationMode:      "plugin",
		Notes:                 "PostgreSQL built-in tsvector is available; plugin can accelerate tokenizer scenarios",
	}
	if len(plugins) > 0 {
		cap.PluginName = strings.Join(plugins, ",")
	}

	return cap, nil
}

// PostgresJSONType 返回 PostgreSQL 的 JSON 字段映射类型。
// 默认使用 jsonb；可通过 config.Options["postgres_json_type"] 或 config.Options["json_type"] 配置为 json。
func (a *PostgreSQLAdapter) PostgresJSONType() string {
	if a == nil || a.config == nil || a.config.Options == nil {
		return "JSONB"
	}

	if v, ok := a.config.Options["postgres_json_type"]; ok {
		if resolved := normalizePostgresJSONType(v); resolved != "" {
			return resolved
		}
	}

	if v, ok := a.config.Options["json_type"]; ok {
		if resolved := normalizePostgresJSONType(v); resolved != "" {
			return resolved
		}
	}

	return "JSONB"
}

// InspectJSONRuntime 检查 PostgreSQL JSON 能力（json/jsonb 类型可用性）。
func (a *PostgreSQLAdapter) InspectJSONRuntime(ctx context.Context) (*JSONRuntimeCapability, error) {
	if a.sqlDB == nil {
		return nil, fmt.Errorf("database not connected")
	}

	var version string
	if err := a.sqlDB.QueryRowContext(ctx, "SHOW server_version").Scan(&version); err != nil {
		return nil, err
	}

	rows, err := a.sqlDB.QueryContext(ctx, "SELECT typname FROM pg_type WHERE typname IN ('json','jsonb')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hasJSON := false
	hasJSONB := false
	for rows.Next() {
		var typ string
		if scanErr := rows.Scan(&typ); scanErr != nil {
			return nil, scanErr
		}
		switch strings.ToLower(strings.TrimSpace(typ)) {
		case "json":
			hasJSON = true
		case "jsonb":
			hasJSONB = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	notes := "PostgreSQL JSON is built-in; jsonb is recommended for indexing and operator richness"
	if !hasJSONB {
		notes = "PostgreSQL json type found but jsonb unavailable; verify server compatibility"
	}

	return &JSONRuntimeCapability{
		NativeSupported:         hasJSON || hasJSONB,
		NativeJSONTypeSupported: hasJSONB,
		Version:                 version,
		PluginChecked:           false,
		PluginAvailable:         false,
		PluginName:              "",
		Notes:                   notes,
	}, nil
}

func normalizePostgresJSONType(value interface{}) string {
	raw, ok := value.(string)
	if !ok {
		return ""
	}

	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "json":
		return "JSON"
	case "jsonb":
		return "JSONB"
	default:
		return ""
	}
}

// Query 执行查询 (返回多行)
func (a *PostgreSQLAdapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return a.sqlDB.QueryContext(ctx, query, args...)
}

// QueryRow 执行查询 (返回单行)
func (a *PostgreSQLAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return a.sqlDB.QueryRowContext(ctx, query, args...)
}

// Exec 执行操作 (INSERT/UPDATE/DELETE)
func (a *PostgreSQLAdapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return a.sqlDB.ExecContext(ctx, query, args...)
}

// Begin 开始事务
func (a *PostgreSQLAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
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

	return &PostgreSQLTx{tx: sqlTx}, nil
}

// GetRawConn 获取底层连接 (返回 *sql.DB)
func (a *PostgreSQLAdapter) GetRawConn() interface{} {
	return a.sqlDB
}

// GetGormDB 获取GORM实例（用于直接访问GORM）
// Deprecated: Adapter 层不再暴露 GORM 连接。
func (a *PostgreSQLAdapter) GetGormDB() *gorm.DB {
	return nil
}

// RegisterScheduledTask 在 PostgreSQL 中注册定时任务
// 使用 PostgreSQL 的触发器 + 函数来实现按月自动创建表的功能
func (a *PostgreSQLAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	if task == nil {
		return fmt.Errorf("task cannot be nil")
	}

	if err := task.Validate(); err != nil {
		return err
	}

	switch task.Type {
	case TaskTypeMonthlyTableCreation:
		return a.registerMonthlyTableCreation(ctx, task)
	default:
		return fmt.Errorf("unsupported task type for PostgreSQL: %s", task.Type)
	}
}

// registerMonthlyTableCreation 注册按月自动创建表的任务
func (a *PostgreSQLAdapter) registerMonthlyTableCreation(ctx context.Context, task *ScheduledTaskConfig) error {
	if err := a.ensureScheduledTaskMetadataTable(ctx); err != nil {
		return err
	}

	existingRecord, err := a.getScheduledTaskRecord(ctx, task.Name)
	if err != nil {
		return err
	}

	config := task.GetMonthlyTableConfig()

	tableName, _ := config["tableName"].(string)
	monthFormat, _ := config["monthFormat"].(string)
	fieldDefs, _ := config["fieldDefinitions"].(string)
	functionName := scheduledTaskCreateTableRoutineName(task.Name)

	// 创建存储过程：create_monthly_log_table
	createProcSQL := fmt.Sprintf(`
CREATE OR REPLACE FUNCTION %s()
RETURNS void AS $$
DECLARE
	new_table_name TEXT;
	full_sql TEXT;
BEGIN
	new_table_name := '%s_' || TO_CHAR(CURRENT_DATE + INTERVAL '1 month', '%s');
	
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.tables t
		WHERE t.table_schema = 'public' 
		AND t.table_name = new_table_name
	) THEN
		full_sql := 'CREATE TABLE ' || new_table_name || ' (%s)';
		EXECUTE full_sql;
		RAISE NOTICE 'Created table: %%', new_table_name;
	END IF;
END;
$$ LANGUAGE plpgsql;
	`, a.quoteIdentifier(functionName), tableName, monthFormat, fieldDefs)

	if err := a.db.WithContext(ctx).Exec(createProcSQL).Error; err != nil {
		return fmt.Errorf("failed to create function %s: %w", functionName, err)
	}

	// 预热当前月份的表
	warmTableSQL := fmt.Sprintf(`
DO $$
DECLARE
	table_name_var TEXT;
	full_sql TEXT;
BEGIN
	FOR i IN 0..0 LOOP
		table_name_var := '%s_' || TO_CHAR(CURRENT_DATE + (i || ' months')::INTERVAL, '%s');
		
		IF NOT EXISTS (
			SELECT 1 FROM information_schema.tables t
			WHERE t.table_schema = 'public' 
			AND t.table_name = table_name_var
		) THEN
			full_sql := 'CREATE TABLE ' || table_name_var || ' (%s)';
			EXECUTE full_sql;
			RAISE NOTICE 'Pre-warmed table: %%', table_name_var;
		END IF;
	END LOOP;
END $$;
	`, tableName, monthFormat, fieldDefs)

	if err := a.db.WithContext(ctx).Exec(warmTableSQL).Error; err != nil {
		return fmt.Errorf("failed to pre-warm tables: %w", err)
	}

	scheduleMode := "function_only"
	var pgCronJobID sql.NullInt64
	pgCronAvailable, err := a.hasPgCronExtension(ctx)
	if err != nil {
		return fmt.Errorf("failed to detect pg_cron availability: %w", err)
	}
	if existingRecord != nil && existingRecord.ScheduleMode == "pg_cron" && existingRecord.PGCronJobID.Valid && pgCronAvailable {
		if err := a.unschedulePGCronJob(ctx, existingRecord.PGCronJobID.Int64); err != nil {
			return fmt.Errorf("failed to replace existing pg_cron job for %s: %w", task.Name, err)
		}
	}

	spec := strings.TrimSpace(task.CronExpression)
	if spec == "" {
		spec = defaultMonthlyCronSpec
	}
	nextExecutionAt, err := computeNextScheduledRun(spec, time.Now())
	if err != nil {
		return fmt.Errorf("invalid cron expression %q for task %s: %w", spec, task.Name, err)
	}
	if pgCronAvailable {
		command := fmt.Sprintf("SELECT %s()", a.quoteQualifiedFunctionCall(functionName))
		if err := a.db.WithContext(ctx).Raw("SELECT cron.schedule(?, ?)", spec, command).Scan(&pgCronJobID).Error; err != nil {
			return fmt.Errorf("failed to schedule pg_cron job for %s: %w", task.Name, err)
		}
		scheduleMode = "pg_cron"
	}

	configJSON, err := json.Marshal(task.Config)
	if err != nil {
		return fmt.Errorf("failed to encode task config: %w", err)
	}

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (
			task_name, task_type, cron_expression, description, enabled,
			config_json, function_name, schedule_mode, pg_cron_job_id,
			last_status, last_error, last_executed_at, next_execute_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		ON CONFLICT (task_name) DO UPDATE SET
			task_type = EXCLUDED.task_type,
			cron_expression = EXCLUDED.cron_expression,
			description = EXCLUDED.description,
			enabled = EXCLUDED.enabled,
			config_json = EXCLUDED.config_json,
			function_name = EXCLUDED.function_name,
			schedule_mode = EXCLUDED.schedule_mode,
			pg_cron_job_id = EXCLUDED.pg_cron_job_id,
			last_status = EXCLUDED.last_status,
			last_error = EXCLUDED.last_error,
			last_executed_at = EXCLUDED.last_executed_at,
			next_execute_at = EXCLUDED.next_execute_at,
			updated_at = NOW()
	`, a.quoteIdentifier(scheduledTaskMetadataTable))

	lastStatus := "registered"
	if scheduleMode == "pg_cron" {
		lastStatus = "scheduled"
	}

	if err := a.db.WithContext(ctx).Exec(
		insertSQL,
		task.Name,
		string(task.Type),
		spec,
		task.Description,
		task.Enabled,
		string(configJSON),
		functionName,
		scheduleMode,
		pgCronJobID,
		lastStatus,
		nil,
		nil,
		nextExecutionAt,
	).Error; err != nil {
		return fmt.Errorf("failed to persist scheduled task metadata for %s: %w", task.Name, err)
	}

	return nil
}

// UnregisterScheduledTask 注销定时任务
func (a *PostgreSQLAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	if taskName == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	if err := a.ensureScheduledTaskMetadataTable(ctx); err != nil {
		return err
	}

	record, err := a.getScheduledTaskRecord(ctx, taskName)
	if err != nil {
		return err
	}

	functionName := scheduledTaskCreateTableRoutineName(taskName)
	if record != nil && strings.TrimSpace(record.FunctionName) != "" {
		functionName = record.FunctionName
	}

	if record != nil && record.ScheduleMode == "pg_cron" && record.PGCronJobID.Valid {
		if err := a.db.WithContext(ctx).Exec("SELECT cron.unschedule(?)", record.PGCronJobID.Int64).Error; err != nil {
			return fmt.Errorf("failed to unschedule pg_cron job for %s: %w", taskName, err)
		}
	}

	// 删除存储过程
	dropFuncSQL := fmt.Sprintf(`
		DROP FUNCTION IF EXISTS %s() CASCADE;
	`, a.quoteIdentifier(functionName))

	if err := a.db.WithContext(ctx).Exec(dropFuncSQL).Error; err != nil {
		return fmt.Errorf("failed to drop function %s: %w", functionName, err)
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE task_name = ?", a.quoteIdentifier(scheduledTaskMetadataTable))
	if err := a.db.WithContext(ctx).Exec(deleteSQL, taskName).Error; err != nil {
		return fmt.Errorf("failed to delete scheduled task metadata for %s: %w", taskName, err)
	}

	return nil
}

// ListScheduledTasks 列出所有已注册的定时任务
func (a *PostgreSQLAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	if err := a.ensureScheduledTaskMetadataTable(ctx); err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT task_name, task_type, cron_expression, description, enabled,
			function_name, schedule_mode, pg_cron_job_id,
			last_status, last_error, last_executed_at, next_execute_at,
			created_at, updated_at
		FROM %s
		ORDER BY created_at ASC
	`, a.quoteIdentifier(scheduledTaskMetadataTable))

	rows, err := a.sqlDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list PostgreSQL scheduled tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]*ScheduledTaskStatus, 0)
	for rows.Next() {
		var record postgresScheduledTaskRecord
		if err := rows.Scan(
			&record.TaskName,
			&record.TaskType,
			&record.CronExpression,
			&record.Description,
			&record.Enabled,
			&record.FunctionName,
			&record.ScheduleMode,
			&record.PGCronJobID,
			&record.LastStatus,
			&record.LastError,
			&record.LastExecutedAt,
			&record.NextExecuteAt,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan PostgreSQL scheduled task metadata: %w", err)
		}

		info := map[string]interface{}{
			"description":   record.Description,
			"enabled":       record.Enabled,
			"function_name": record.FunctionName,
			"schedule_mode": record.ScheduleMode,
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
		if record.PGCronJobID.Valid {
			info["pg_cron_job_id"] = record.PGCronJobID.Int64
			if runtime, runtimeErr := a.getPGCronJobRuntime(ctx, record.PGCronJobID.Int64); runtimeErr == nil && runtime != nil {
				info["pg_cron_active"] = runtime.Active
				info["pg_cron_schedule"] = runtime.Schedule
				info["pg_cron_command"] = runtime.Command
			} else if runtimeErr != nil {
				info["pg_cron_state_error"] = runtimeErr.Error()
			}
			if runDetail, detailErr := a.getLatestPGCronRunDetail(ctx, record.PGCronJobID.Int64); detailErr == nil && runDetail != nil {
				if runDetail.Status.Valid {
					info["pg_cron_last_status"] = runDetail.Status.String
				}
				if runDetail.ReturnMessage.Valid {
					info["pg_cron_last_message"] = runDetail.ReturnMessage.String
				}
				if runDetail.FinishedAt.Valid {
					record.LastExecutedAt = runDetail.FinishedAt
				} else if runDetail.StartedAt.Valid {
					record.LastExecutedAt = runDetail.StartedAt
				}
			} else if detailErr != nil {
				info["pg_cron_run_error"] = detailErr.Error()
			}
		}
		if record.NextExecuteAt.Valid {
			info["next_execute_at"] = record.NextExecuteAt.Time.Unix()
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
		if record.LastStatus.Valid && record.LastStatus.String == "running" {
			status.Running = true
		}

		tasks = append(tasks, status)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate PostgreSQL scheduled tasks: %w", err)
	}

	return tasks, nil
}

func (a *PostgreSQLAdapter) ensureScheduledTaskMetadataTable(ctx context.Context) error {
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			task_name TEXT PRIMARY KEY,
			task_type TEXT NOT NULL,
			cron_expression TEXT,
			description TEXT,
			enabled BOOLEAN NOT NULL DEFAULT TRUE,
			config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			function_name TEXT NOT NULL,
			schedule_mode TEXT NOT NULL DEFAULT 'function_only',
			pg_cron_job_id BIGINT NULL,
			last_status TEXT NULL,
			last_error TEXT NULL,
			last_executed_at TIMESTAMPTZ NULL,
			next_execute_at TIMESTAMPTZ NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, a.quoteIdentifier(scheduledTaskMetadataTable))

	if err := a.db.WithContext(ctx).Exec(createTableSQL).Error; err != nil {
		return fmt.Errorf("failed to ensure PostgreSQL scheduled task metadata table: %w", err)
	}

	alterStatements := []string{
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS last_status TEXT NULL", a.quoteIdentifier(scheduledTaskMetadataTable)),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS last_error TEXT NULL", a.quoteIdentifier(scheduledTaskMetadataTable)),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS last_executed_at TIMESTAMPTZ NULL", a.quoteIdentifier(scheduledTaskMetadataTable)),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS next_execute_at TIMESTAMPTZ NULL", a.quoteIdentifier(scheduledTaskMetadataTable)),
	}
	for _, stmt := range alterStatements {
		if err := a.db.WithContext(ctx).Exec(stmt).Error; err != nil {
			return fmt.Errorf("failed to ensure PostgreSQL scheduled task metadata columns: %w", err)
		}
	}

	return nil
}

func (a *PostgreSQLAdapter) hasPgCronExtension(ctx context.Context) (bool, error) {
	var count int
	err := a.db.WithContext(ctx).Raw(
		"SELECT COUNT(1) FROM pg_extension WHERE extname = 'pg_cron'",
	).Scan(&count).Error
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func (a *PostgreSQLAdapter) getScheduledTaskRecord(ctx context.Context, taskName string) (*postgresScheduledTaskRecord, error) {
	query := fmt.Sprintf(`
		SELECT task_name, task_type, cron_expression, description, enabled,
			config_json::text, function_name, schedule_mode, pg_cron_job_id,
			last_status, last_error, last_executed_at, next_execute_at,
			created_at, updated_at
		FROM %s
		WHERE task_name = ?
	`, a.quoteIdentifier(scheduledTaskMetadataTable))

	var record postgresScheduledTaskRecord
	err := a.sqlDB.QueryRowContext(ctx, strings.Replace(query, "?", "$1", 1), taskName).Scan(
		&record.TaskName,
		&record.TaskType,
		&record.CronExpression,
		&record.Description,
		&record.Enabled,
		&record.ConfigJSON,
		&record.FunctionName,
		&record.ScheduleMode,
		&record.PGCronJobID,
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
		return nil, fmt.Errorf("failed to query scheduled task metadata for %s: %w", taskName, err)
	}

	return &record, nil
}

func (a *PostgreSQLAdapter) unschedulePGCronJob(ctx context.Context, jobID int64) error {
	return a.db.WithContext(ctx).Exec("SELECT cron.unschedule(?)", jobID).Error
}

func (a *PostgreSQLAdapter) getPGCronJobRuntime(ctx context.Context, jobID int64) (*postgresCronJobRuntime, error) {
	row := a.sqlDB.QueryRowContext(ctx, `
		SELECT schedule, command, active
		FROM cron.job
		WHERE jobid = $1
	`, jobID)

	var runtime postgresCronJobRuntime
	if err := row.Scan(&runtime.Schedule, &runtime.Command, &runtime.Active); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &runtime, nil
}

func (a *PostgreSQLAdapter) getLatestPGCronRunDetail(ctx context.Context, jobID int64) (*postgresCronJobRunDetail, error) {
	row := a.sqlDB.QueryRowContext(ctx, `
		SELECT status, return_message, start_time, end_time
		FROM cron.job_run_details
		WHERE jobid = $1
		ORDER BY start_time DESC
		LIMIT 1
	`, jobID)

	var detail postgresCronJobRunDetail
	if err := row.Scan(&detail.Status, &detail.ReturnMessage, &detail.StartedAt, &detail.FinishedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &detail, nil
}

func (a *PostgreSQLAdapter) quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (a *PostgreSQLAdapter) quoteQualifiedFunctionCall(functionName string) string {
	return a.quoteIdentifier("public") + "." + a.quoteIdentifier(functionName)
}

// PostgreSQLTx PostgreSQL 事务实现
type PostgreSQLTx struct {
	tx *sql.Tx
}

// Commit 提交事务
func (t *PostgreSQLTx) Commit(ctx context.Context) error {
	return t.tx.Commit()
}

// Rollback 回滚事务
func (t *PostgreSQLTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback()
}

// Exec 在事务中执行
func (t *PostgreSQLTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query 在事务中查询
func (t *PostgreSQLTx) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// QueryRow 在事务中查询单行
func (t *PostgreSQLTx) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

// GetQueryBuilderProvider 返回查询构造器提供者
func (a *PostgreSQLAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return NewDefaultSQLQueryConstructorProvider(NewPostgreSQLDialect())
}

// GetDatabaseFeatures 返回 PostgreSQL 数据库特性
func (a *PostgreSQLAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{
		// 索引和约束
		SupportsCompositeKeys:        true,
		SupportsForeignKeys:          true,
		SupportsCompositeForeignKeys: true,
		SupportsCompositeIndexes:     true,
		SupportsPartialIndexes:       true,
		SupportsDeferrable:           true,

		// 自定义类型
		SupportsEnumType:      true,
		SupportsCompositeType: true,
		SupportsDomainType:    true,
		SupportsUDT:           true,

		// 函数和过程
		SupportsStoredProcedures: true,
		SupportsFunctions:        true,
		SupportsAggregateFuncs:   true,
		FunctionLanguages:        []string{"plpgsql", "sql", "python", "perl"},

		// 高级查询
		SupportsWindowFunctions: true,
		SupportsCTE:             true,
		SupportsRecursiveCTE:    true,
		SupportsMaterializedCTE: true,

		// JSON 支持
		HasNativeJSON:     true,
		SupportsJSONPath:  true,
		SupportsJSONIndex: true,

		// 全文搜索
		SupportsFullTextSearch: true,
		FullTextLanguages:      []string{"english", "chinese", "japanese"},

		// 其他特性
		SupportsArrays:       true,
		SupportsGenerated:    true,
		SupportsReturning:    true,
		SupportsUpsert:       true,
		SupportsListenNotify: true,

		// 元信息
		DatabaseName:    "PostgreSQL",
		DatabaseVersion: "12+",
		Description:     "Full-featured enterprise database with extensive type system",
	}
}

// GetQueryFeatures 返回 PostgreSQL 的查询特性
func (a *PostgreSQLAdapter) GetQueryFeatures() *QueryFeatures {
	return NewPostgreSQLQueryFeatures()
}

// init 自动注册 PostgreSQL 适配器
func init() {
	RegisterAdapter(&PostgreSQLFactory{})
}
