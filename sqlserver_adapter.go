package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

// SQLServerAdapter SQL Server 数据库适配器
type SQLServerAdapter struct {
	config *Config
	db     *gorm.DB
	sqlDB  *sql.DB
}

type sqlServerScheduledTaskRecord struct {
	TaskName       string
	TaskType       string
	CronExpression string
	Description    string
	Enabled        bool
	ConfigJSON     string
	ProcedureName  string
	ScheduleMode   string
	AgentJobName   string
	LastStatus     sql.NullString
	LastError      sql.NullString
	LastExecutedAt sql.NullTime
	NextExecuteAt  sql.NullTime
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type sqlServerAgentJobRuntime struct {
	Enabled       bool
	NextRunDate   sql.NullInt64
	NextRunTime   sql.NullInt64
	LastRunDate   sql.NullInt64
	LastRunTime   sql.NullInt64
	LastRunStatus sql.NullInt64
}

// SQLServerFactory SQL Server 适配器工厂
type SQLServerFactory struct{}

// Name 返回适配器名称
func (f *SQLServerFactory) Name() string {
	return "sqlserver"
}

// Create 创建 SQL Server 适配器
func (f *SQLServerFactory) Create(config *Config) (Adapter, error) {
	adapter := &SQLServerAdapter{config: config}
	if err := adapter.Connect(context.Background(), config); err != nil {
		return nil, err
	}
	return adapter, nil
}

// Connect 连接到 SQL Server 数据库
func (a *SQLServerAdapter) Connect(ctx context.Context, config *Config) error {
	if config == nil {
		config = a.config
	}
	resolved := config.ResolvedSQLServerConfig()

	// 验证必需字段
	if resolved.Host == "" {
		resolved.Host = "localhost"
	}
	if resolved.Port == 0 {
		resolved.Port = 1433 // SQL Server 默认端口
	}
	if resolved.Username == "" && strings.TrimSpace(resolved.DSN) == "" {
		return fmt.Errorf("SQL Server: username is required")
	}
	if resolved.Database == "" && strings.TrimSpace(resolved.DSN) == "" {
		return fmt.Errorf("SQL Server: database name is required")
	}

	// 处理空密码
	password := resolved.Password

	// 构建 DSN (Data Source Name)
	// 格式: sqlserver://username:password@host:port?database=dbname
	var dsn string
	if strings.TrimSpace(resolved.DSN) != "" {
		dsn = resolved.DSN
	} else {
		dsn = fmt.Sprintf(
			"sqlserver://%s:%s@%s:%d?database=%s&connection+timeout=30&encrypt=disable",
			resolved.Username,
			password,
			resolved.Host,
			resolved.Port,
			resolved.Database,
		)
	}

	db, err := gorm.Open(sqlserver.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to SQL Server (host=%s, port=%d, user=%s, db=%s): %w",
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
func (a *SQLServerAdapter) Close() error {
	if a.sqlDB != nil {
		return a.sqlDB.Close()
	}
	return nil
}

// Ping 测试数据库连接
func (a *SQLServerAdapter) Ping(ctx context.Context) error {
	if a.sqlDB == nil {
		return fmt.Errorf("database not connected")
	}
	return a.sqlDB.PingContext(ctx)
}

// InspectFullTextRuntime 检查 SQL Server Full-Text Search 是否安装。
func (a *SQLServerAdapter) InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error) {
	if a.sqlDB == nil {
		return nil, fmt.Errorf("database not connected")
	}

	var installed sql.NullInt64
	err := a.sqlDB.QueryRowContext(ctx, "SELECT FULLTEXTSERVICEPROPERTY('IsFullTextInstalled')").Scan(&installed)
	if err != nil {
		return nil, err
	}

	available := installed.Valid && installed.Int64 == 1
	return &FullTextRuntimeCapability{
		NativeSupported:       available,
		PluginChecked:         true,
		PluginAvailable:       available,
		PluginName:            "sqlserver_full_text_service",
		TokenizationSupported: available,
		TokenizationMode:      "native",
		Notes:                 "SQL Server full-text requires Full-Text service/catlog/index",
	}, nil
}

// InspectJSONRuntime 检查 SQL Server JSON 运行时能力。
// 说明：SQL Server 的 JSON 函数能力为内建能力（2016+），并非独立插件生态。
func (a *SQLServerAdapter) InspectJSONRuntime(ctx context.Context) (*JSONRuntimeCapability, error) {
	if a.sqlDB == nil {
		return nil, fmt.Errorf("database not connected")
	}

	var version sql.NullString
	if err := a.sqlDB.QueryRowContext(ctx, "SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))").Scan(&version); err != nil {
		return nil, err
	}

	// JSON 函数能力（SQL Server 2016+）
	hasJSONFunctions := false
	var isJSON sql.NullInt64
	if err := a.sqlDB.QueryRowContext(ctx, "SELECT ISJSON('{\"k\":1}')").Scan(&isJSON); err == nil {
		hasJSONFunctions = isJSON.Valid && isJSON.Int64 == 1
	}

	// 原生 JSON 类型（SQL Server 2025 预览及后续版本）
	hasNativeJSONType := false
	var jsonTypeID sql.NullInt64
	if err := a.sqlDB.QueryRowContext(ctx, "SELECT TYPE_ID('json')").Scan(&jsonTypeID); err == nil {
		hasNativeJSONType = jsonTypeID.Valid
	}

	notes := "SQL Server JSON is built-in (2016+); no separate JSON plugin installer is required"
	if !hasJSONFunctions {
		notes = "JSON functions not detected; ensure SQL Server 2016+ engine/version compatibility"
	} else if !hasNativeJSONType {
		notes = "JSON functions are available; native json data type requires SQL Server 2025+ preview/newer"
	}

	return &JSONRuntimeCapability{
		NativeSupported:         hasJSONFunctions,
		NativeJSONTypeSupported: hasNativeJSONType,
		Version:                 strings.TrimSpace(version.String),
		PluginChecked:           true,
		PluginAvailable:         false,
		PluginName:              "not_applicable",
		Notes:                   notes,
	}, nil
}

// Query 执行查询 (返回多行)
func (a *SQLServerAdapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return a.sqlDB.QueryContext(ctx, query, args...)
}

// QueryRow 执行查询 (返回单行)
func (a *SQLServerAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return a.sqlDB.QueryRowContext(ctx, query, args...)
}

// Exec 执行操作 (INSERT/UPDATE/DELETE)
func (a *SQLServerAdapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return a.sqlDB.ExecContext(ctx, query, args...)
}

// Begin 开始事务
func (a *SQLServerAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
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

	return &SQLServerTx{tx: sqlTx}, nil
}

// GetRawConn 获取底层连接 (返回 *sql.DB)
func (a *SQLServerAdapter) GetRawConn() interface{} {
	return a.sqlDB
}

// GetGormDB 获取GORM实例（用于直接访问GORM）
// Deprecated: Adapter 层不再暴露 GORM 连接。
func (a *SQLServerAdapter) GetGormDB() *gorm.DB {
	return nil
}

// RegisterScheduledTask SQL Server 适配器支持 SQL Server Agent 方式的定时任务
// 注意：需要 SQL Server Agent 服务运行，且用户需要相应权限
func (a *SQLServerAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	if task == nil {
		return fmt.Errorf("task cannot be nil")
	}
	if err := task.Validate(); err != nil {
		return err
	}
	if task.Type != TaskTypeMonthlyTableCreation {
		return fmt.Errorf("unsupported task type for SQL Server: %s", task.Type)
	}

	agentAvailable, err := a.isSQLServerAgentAvailable(ctx)
	if err != nil {
		return fmt.Errorf("failed to detect SQL Server Agent availability: %w", err)
	}
	if !agentAvailable {
		return NewScheduledTaskFallbackErrorWithReason(
			"sqlserver",
			ScheduledTaskFallbackReasonNativeCapabilityMissing,
			"SQL Server Agent not available in this environment",
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

	if existingRecord != nil && strings.TrimSpace(existingRecord.AgentJobName) != "" {
		if err := a.deleteSQLServerAgentJob(ctx, existingRecord.AgentJobName); err != nil {
			return fmt.Errorf("failed to replace existing SQL Server Agent job for %s: %w", task.Name, err)
		}
	}

	scheduleSpec := strings.TrimSpace(task.CronExpression)
	if scheduleSpec == "" {
		scheduleSpec = defaultMonthlyCronSpec
	}
	dayOfMonth, startTime, err := parseSQLServerAgentMonthlySchedule(scheduleSpec)
	if err != nil {
		return NewScheduledTaskFallbackErrorWithReason(
			"sqlserver",
			ScheduledTaskFallbackReasonCronExpressionUnsupported,
			fmt.Sprintf("cron %q is unsupported by SQL Server Agent: %v", scheduleSpec, err),
		)
	}
	nextExecutionAt, err := computeNextScheduledRun(scheduleSpec, time.Now())
	if err != nil {
		return fmt.Errorf("invalid cron expression %q for task %s: %w", scheduleSpec, task.Name, err)
	}

	jobName := scheduledTaskAgentJobName(task.Name)
	if err := a.createSQLServerAgentJob(ctx, jobName, procedureName, task.Description, dayOfMonth, startTime); err != nil {
		return err
	}

	configJSON, err := json.Marshal(task.Config)
	if err != nil {
		return fmt.Errorf("failed to encode task config: %w", err)
	}

	insertSQL := fmt.Sprintf(`
		MERGE %s AS target
		USING (SELECT ? AS task_name) AS source
		ON target.task_name = source.task_name
		WHEN MATCHED THEN
			UPDATE SET
				task_type = ?, cron_expression = ?, description = ?, enabled = ?,
				config_json = ?, procedure_name = ?, schedule_mode = ?, agent_job_name = ?,
				last_status = ?, last_error = NULL, last_executed_at = NULL, next_execute_at = ?, updated_at = SYSUTCDATETIME()
		WHEN NOT MATCHED THEN
			INSERT (
				task_name, task_type, cron_expression, description, enabled,
				config_json, procedure_name, schedule_mode, agent_job_name,
				last_status, last_error, last_executed_at, next_execute_at, created_at, updated_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, SYSUTCDATETIME(), SYSUTCDATETIME());
	`, a.qualifyScheduledTaskTable())

	if err := a.db.WithContext(ctx).Exec(
		insertSQL,
		task.Name,
		string(task.Type),
		scheduleSpec,
		task.Description,
		task.Enabled,
		string(configJSON),
		procedureName,
		"sqlserver_agent",
		jobName,
		"scheduled",
		nextExecutionAt,
		task.Name,
		string(task.Type),
		scheduleSpec,
		task.Description,
		task.Enabled,
		string(configJSON),
		procedureName,
		"sqlserver_agent",
		jobName,
		"scheduled",
		nextExecutionAt,
	).Error; err != nil {
		return fmt.Errorf("failed to persist SQL Server scheduled task metadata for %s: %w", task.Name, err)
	}

	return nil
}

// UnregisterScheduledTask SQL Server 适配器暂不支持
func (a *SQLServerAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
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

	if strings.TrimSpace(record.AgentJobName) != "" {
		if err := a.deleteSQLServerAgentJob(ctx, record.AgentJobName); err != nil && !strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			return fmt.Errorf("failed to delete SQL Server Agent job %s: %w", record.AgentJobName, err)
		}
	}
	if strings.TrimSpace(record.ProcedureName) != "" {
		dropProcSQL := fmt.Sprintf("DROP PROCEDURE IF EXISTS %s", a.quoteQualifiedProcedure(record.ProcedureName))
		if err := a.db.WithContext(ctx).Exec(dropProcSQL).Error; err != nil {
			return fmt.Errorf("failed to drop SQL Server scheduled task procedure %s: %w", record.ProcedureName, err)
		}
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE task_name = ?", a.qualifyScheduledTaskTable())
	if err := a.db.WithContext(ctx).Exec(deleteSQL, taskName).Error; err != nil {
		return fmt.Errorf("failed to delete SQL Server scheduled task metadata for %s: %w", taskName, err)
	}

	return nil
}

// ListScheduledTasks SQL Server 适配器暂不支持
func (a *SQLServerAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	if err := a.ensureScheduledTaskMetadataTable(ctx); err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT task_name, task_type, cron_expression, description, enabled,
			config_json, procedure_name, schedule_mode, agent_job_name,
			last_status, last_error, last_executed_at, next_execute_at,
			created_at, updated_at
		FROM %s
		ORDER BY created_at ASC
	`, a.qualifyScheduledTaskTable())

	rows, err := a.sqlDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list SQL Server scheduled tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]*ScheduledTaskStatus, 0)
	for rows.Next() {
		var record sqlServerScheduledTaskRecord
		if err := rows.Scan(
			&record.TaskName,
			&record.TaskType,
			&record.CronExpression,
			&record.Description,
			&record.Enabled,
			&record.ConfigJSON,
			&record.ProcedureName,
			&record.ScheduleMode,
			&record.AgentJobName,
			&record.LastStatus,
			&record.LastError,
			&record.LastExecutedAt,
			&record.NextExecuteAt,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan SQL Server scheduled task metadata: %w", err)
		}

		info := map[string]interface{}{
			"description":    record.Description,
			"enabled":        record.Enabled,
			"procedure_name": record.ProcedureName,
			"schedule_mode":  record.ScheduleMode,
			"agent_job_name": record.AgentJobName,
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
		if strings.TrimSpace(record.AgentJobName) != "" {
			if runtime, runtimeErr := a.getSQLServerAgentJobRuntime(ctx, record.AgentJobName); runtimeErr == nil && runtime != nil {
				info["agent_enabled"] = runtime.Enabled
				if nextRun, ok := sqlServerAgentDateTimeToTime(runtime.NextRunDate, runtime.NextRunTime); ok {
					record.NextExecuteAt = sql.NullTime{Time: nextRun, Valid: true}
				}
				if lastRun, ok := sqlServerAgentDateTimeToTime(runtime.LastRunDate, runtime.LastRunTime); ok {
					record.LastExecutedAt = sql.NullTime{Time: lastRun, Valid: true}
				}
				if statusText := sqlServerAgentRunStatusText(runtime.LastRunStatus); statusText != "" {
					info["agent_last_status"] = statusText
				}
			} else if runtimeErr != nil {
				info["agent_state_error"] = runtimeErr.Error()
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
		if record.LastStatus.Valid && record.LastStatus.String == "running" {
			status.Running = true
		}
		tasks = append(tasks, status)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate SQL Server scheduled tasks: %w", err)
	}

	return tasks, nil
}

func (a *SQLServerAdapter) ensureScheduledTaskMetadataTable(ctx context.Context) error {
	createTableSQL := fmt.Sprintf(`
		IF OBJECT_ID(N'%s', N'U') IS NULL
		BEGIN
			CREATE TABLE %s (
				task_name NVARCHAR(255) NOT NULL PRIMARY KEY,
				task_type NVARCHAR(100) NOT NULL,
				cron_expression NVARCHAR(255) NULL,
				description NVARCHAR(1000) NULL,
				enabled BIT NOT NULL DEFAULT 1,
				config_json NVARCHAR(MAX) NOT NULL DEFAULT N'{}',
				procedure_name NVARCHAR(255) NOT NULL,
				schedule_mode NVARCHAR(100) NOT NULL,
				agent_job_name NVARCHAR(255) NULL,
				last_status NVARCHAR(100) NULL,
				last_error NVARCHAR(MAX) NULL,
				last_executed_at DATETIME2 NULL,
				next_execute_at DATETIME2 NULL,
				created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
				updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
			)
		END
	`, a.qualifyScheduledTaskObject(), a.qualifyScheduledTaskTable())

	if err := a.db.WithContext(ctx).Exec(createTableSQL).Error; err != nil {
		return fmt.Errorf("failed to ensure SQL Server scheduled task metadata table: %w", err)
	}

	return nil
}

func (a *SQLServerAdapter) isSQLServerAgentAvailable(ctx context.Context) (bool, error) {
	var edition string
	if err := a.sqlDB.QueryRowContext(ctx, "SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128))").Scan(&edition); err != nil {
		return false, err
	}
	if strings.Contains(strings.ToLower(edition), "express") {
		return false, nil
	}

	var objectID sql.NullInt64
	if err := a.sqlDB.QueryRowContext(ctx, "SELECT OBJECT_ID(N'msdb.dbo.sp_add_job')").Scan(&objectID); err != nil {
		return false, err
	}

	return objectID.Valid, nil
}

func (a *SQLServerAdapter) registerMonthlyTableCreationProcedure(ctx context.Context, procedureName string, task *ScheduledTaskConfig) error {
	config := task.GetMonthlyTableConfig()
	tableName, _ := config["tableName"].(string)
	monthFormat, _ := config["monthFormat"].(string)
	fieldDefs, _ := config["fieldDefinitions"].(string)
	tsqlFormat := sqlServerDateFormatFromGoLayout(monthFormat)

	procedureSQL := fmt.Sprintf(`
		CREATE OR ALTER PROCEDURE %s
		AS
		BEGIN
			SET NOCOUNT ON;
			DECLARE @new_table_name NVARCHAR(255);
			DECLARE @full_sql NVARCHAR(MAX);
			SET @new_table_name = N'%s_' + FORMAT(DATEADD(MONTH, 1, SYSUTCDATETIME()), '%s');

			IF NOT EXISTS (
				SELECT 1
				FROM sys.tables t
				JOIN sys.schemas s ON s.schema_id = t.schema_id
				WHERE s.name = N'dbo' AND t.name = @new_table_name
			)
			BEGIN
				SET @full_sql = N'CREATE TABLE [dbo].' + QUOTENAME(@new_table_name) + N' (%s)';
				EXEC sp_executesql @full_sql;
			END
		END
	`, a.quoteQualifiedProcedure(procedureName), a.escapeStringLiteral(tableName), a.escapeStringLiteral(tsqlFormat), a.escapeStringLiteral(fieldDefs))

	if err := a.db.WithContext(ctx).Exec(procedureSQL).Error; err != nil {
		return fmt.Errorf("failed to create SQL Server scheduled task procedure %s: %w", procedureName, err)
	}

	return nil
}

func (a *SQLServerAdapter) preWarmMonthlyTable(ctx context.Context, task *ScheduledTaskConfig) error {
	config := task.GetMonthlyTableConfig()
	tableName, _ := config["tableName"].(string)
	monthFormat, _ := config["monthFormat"].(string)
	fieldDefs, _ := config["fieldDefinitions"].(string)
	currentTable := fmt.Sprintf("%s_%s", tableName, time.Now().Format(monthFormat))

	preWarmSQL := fmt.Sprintf(`
		IF OBJECT_ID(N'[dbo].%s', N'U') IS NULL
		BEGIN
			EXEC(N'CREATE TABLE [dbo].%s (%s)')
		END
	`, a.escapeIdentifierName(currentTable), a.escapeIdentifierName(currentTable), a.escapeStringLiteral(fieldDefs))

	if err := a.db.WithContext(ctx).Exec(preWarmSQL).Error; err != nil {
		return fmt.Errorf("failed to pre-warm SQL Server monthly table %s: %w", currentTable, err)
	}

	return nil
}

func (a *SQLServerAdapter) createSQLServerAgentJob(ctx context.Context, jobName, procedureName, description string, dayOfMonth int, startTime int) error {
	databaseName := "master"
	if a != nil && a.config != nil {
		resolved := a.config.ResolvedSQLServerConfig()
		if strings.TrimSpace(resolved.Database) != "" {
			databaseName = resolved.Database
		}
	}
	command := fmt.Sprintf("EXEC %s", a.quoteQualifiedProcedure(procedureName))
	scheduleName := jobName + "_schedule"

	jobSQL := fmt.Sprintf(`
		EXEC msdb.dbo.sp_add_job @job_name = N'%s', @enabled = 1, @description = N'%s';
		EXEC msdb.dbo.sp_add_jobstep @job_name = N'%s', @step_name = N'run_task', @subsystem = N'TSQL', @database_name = N'%s', @command = N'%s';
		EXEC msdb.dbo.sp_add_jobschedule @job_name = N'%s', @name = N'%s', @enabled = 1, @freq_type = 16, @freq_interval = %d, @active_start_time = %d;
		EXEC msdb.dbo.sp_add_jobserver @job_name = N'%s';
	`,
		a.escapeStringLiteral(jobName),
		a.escapeStringLiteral(description),
		a.escapeStringLiteral(jobName),
		a.escapeStringLiteral(databaseName),
		a.escapeStringLiteral(command),
		a.escapeStringLiteral(jobName),
		a.escapeStringLiteral(scheduleName),
		dayOfMonth,
		startTime,
		a.escapeStringLiteral(jobName),
	)

	if err := a.db.WithContext(ctx).Exec(jobSQL).Error; err != nil {
		return fmt.Errorf("failed to create SQL Server Agent job %s: %w", jobName, err)
	}

	return nil
}

func (a *SQLServerAdapter) deleteSQLServerAgentJob(ctx context.Context, jobName string) error {
	deleteSQL := fmt.Sprintf("EXEC msdb.dbo.sp_delete_job @job_name = N'%s';", a.escapeStringLiteral(jobName))
	return a.db.WithContext(ctx).Exec(deleteSQL).Error
}

func (a *SQLServerAdapter) getScheduledTaskRecord(ctx context.Context, taskName string) (*sqlServerScheduledTaskRecord, error) {
	query := fmt.Sprintf(`
		SELECT task_name, task_type, cron_expression, description, enabled,
			config_json, procedure_name, schedule_mode, agent_job_name,
			last_status, last_error, last_executed_at, next_execute_at,
			created_at, updated_at
		FROM %s
		WHERE task_name = @p1
	`, a.qualifyScheduledTaskTable())

	var record sqlServerScheduledTaskRecord
	err := a.sqlDB.QueryRowContext(ctx, query, taskName).Scan(
		&record.TaskName,
		&record.TaskType,
		&record.CronExpression,
		&record.Description,
		&record.Enabled,
		&record.ConfigJSON,
		&record.ProcedureName,
		&record.ScheduleMode,
		&record.AgentJobName,
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
		return nil, fmt.Errorf("failed to query SQL Server scheduled task metadata for %s: %w", taskName, err)
	}

	return &record, nil
}

func (a *SQLServerAdapter) getSQLServerAgentJobRuntime(ctx context.Context, jobName string) (*sqlServerAgentJobRuntime, error) {
	query := `
		SELECT j.enabled, js.next_run_date, js.next_run_time, jh.run_date, jh.run_time, jh.run_status
		FROM msdb.dbo.sysjobs j
		LEFT JOIN msdb.dbo.sysjobschedules js ON js.job_id = j.job_id
		OUTER APPLY (
			SELECT TOP (1) run_date, run_time, run_status
			FROM msdb.dbo.sysjobhistory h
			WHERE h.job_id = j.job_id AND h.step_id = 0
			ORDER BY instance_id DESC
		) jh
		WHERE j.name = @p1
	`

	var enabled int
	var runtime sqlServerAgentJobRuntime
	err := a.sqlDB.QueryRowContext(ctx, query, jobName).Scan(
		&enabled,
		&runtime.NextRunDate,
		&runtime.NextRunTime,
		&runtime.LastRunDate,
		&runtime.LastRunTime,
		&runtime.LastRunStatus,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	runtime.Enabled = enabled == 1
	return &runtime, nil
}

func parseSQLServerAgentMonthlySchedule(spec string) (int, int, error) {
	parts := strings.Fields(strings.TrimSpace(spec))
	if len(parts) != 5 {
		return 0, 0, fmt.Errorf("expected standard 5-field cron expression")
	}
	minute, err := strconv.Atoi(parts[0])
	if err != nil || minute < 0 || minute > 59 {
		return 0, 0, fmt.Errorf("invalid minute field")
	}
	hour, err := strconv.Atoi(parts[1])
	if err != nil || hour < 0 || hour > 23 {
		return 0, 0, fmt.Errorf("invalid hour field")
	}
	dayOfMonth, err := strconv.Atoi(parts[2])
	if err != nil || dayOfMonth < 1 || dayOfMonth > 31 {
		return 0, 0, fmt.Errorf("invalid day-of-month field")
	}
	if parts[3] != "*" || parts[4] != "*" {
		return 0, 0, fmt.Errorf("only monthly cron expressions of form 'm h d * *' are supported")
	}

	return dayOfMonth, hour*10000 + minute*100, nil
}

func sqlServerAgentDateTimeToTime(dateVal, timeVal sql.NullInt64) (time.Time, bool) {
	if !dateVal.Valid || !timeVal.Valid || dateVal.Int64 == 0 {
		return time.Time{}, false
	}
	dateStr := fmt.Sprintf("%08d", dateVal.Int64)
	timeStr := fmt.Sprintf("%06d", timeVal.Int64)
	parsed, err := time.Parse("20060102150405", dateStr+timeStr)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func sqlServerAgentRunStatusText(status sql.NullInt64) string {
	if !status.Valid {
		return ""
	}
	switch status.Int64 {
	case 0:
		return "failed"
	case 1:
		return "succeeded"
	case 2:
		return "retry"
	case 3:
		return "canceled"
	case 4:
		return "running"
	default:
		return "unknown"
	}
}

func sqlServerDateFormatFromGoLayout(layout string) string {
	switch strings.TrimSpace(layout) {
	case "", "2006_01":
		return "yyyy_MM"
	case "200601":
		return "yyyyMM"
	case "2006-01":
		return "yyyy-MM"
	default:
		return "yyyy_MM"
	}
}

func (a *SQLServerAdapter) qualifyScheduledTaskObject() string {
	return "dbo." + scheduledTaskMetadataTable
}

func (a *SQLServerAdapter) qualifyScheduledTaskTable() string {
	return a.quoteIdentifier("dbo") + "." + a.quoteIdentifier(scheduledTaskMetadataTable)
}

func (a *SQLServerAdapter) quoteQualifiedProcedure(procedureName string) string {
	return a.quoteIdentifier("dbo") + "." + a.quoteIdentifier(procedureName)
}

func (a *SQLServerAdapter) quoteIdentifier(name string) string {
	return "[" + a.escapeIdentifierName(name) + "]"
}

func (a *SQLServerAdapter) escapeIdentifierName(name string) string {
	return strings.ReplaceAll(name, "]", "]]")
}

func (a *SQLServerAdapter) escapeStringLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// SQLServerTx SQL Server 事务实现
type SQLServerTx struct {
	tx *sql.Tx
}

// Commit 提交事务
func (t *SQLServerTx) Commit(ctx context.Context) error {
	return t.tx.Commit()
}

// Rollback 回滚事务
func (t *SQLServerTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback()
}

// Exec 在事务中执行
func (t *SQLServerTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query 在事务中查询
func (t *SQLServerTx) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// QueryRow 在事务中查询单行
func (t *SQLServerTx) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

// GetQueryBuilderProvider 返回查询构造器提供者
func (a *SQLServerAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	strategy := "direct_join"
	if a != nil && a.config != nil {
		resolved := a.config.ResolvedSQLServerConfig()
		if strings.TrimSpace(resolved.ManyToManyStrategy) != "" {
			strategy = resolved.ManyToManyStrategy
		}
		return NewDefaultSQLQueryConstructorProvider(NewSQLServerDialectWithOptions(
			strategy,
			resolved.RecursiveCTEDepth,
			resolved.RecursiveCTEMaxRecursion,
		))
	}
	return NewDefaultSQLQueryConstructorProvider(NewSQLServerDialectWithManyToManyStrategy(strategy))
}

// GetDatabaseFeatures 返回 SQL Server 数据库特性
func (a *SQLServerAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return &DatabaseFeatures{
		// 索引和约束
		SupportsCompositeKeys:        true,
		SupportsForeignKeys:          true,
		SupportsCompositeForeignKeys: true,
		SupportsCompositeIndexes:     true,
		SupportsPartialIndexes:       true, // Filtered indexes
		SupportsDeferrable:           false,

		// 自定义类型
		SupportsEnumType:      false,
		SupportsCompositeType: false,
		SupportsDomainType:    false,
		SupportsUDT:           true,

		// 函数和过程
		SupportsStoredProcedures: true,
		SupportsFunctions:        true,
		SupportsAggregateFuncs:   true,
		FunctionLanguages:        []string{"tsql", "clr"},

		// 高级查询
		SupportsWindowFunctions: true,
		SupportsCTE:             true,
		SupportsRecursiveCTE:    true,
		SupportsMaterializedCTE: false,

		// JSON 支持
		HasNativeJSON:     false, // Stored as NVARCHAR
		SupportsJSONPath:  true,  // JSON functions since 2016
		SupportsJSONIndex: true,  // Via computed columns

		// 全文搜索
		SupportsFullTextSearch: true,
		FullTextLanguages:      []string{"english", "chinese", "japanese"},

		// 其他特性
		SupportsArrays:       false,
		SupportsGenerated:    true,  // Computed columns
		SupportsReturning:    true,  // OUTPUT clause
		SupportsUpsert:       true,  // MERGE
		SupportsListenNotify: false, // Use Service Broker instead

		// 元信息
		DatabaseName:    "SQL Server",
		DatabaseVersion: "2016+",
		Description:     "Enterprise database with T-SQL and CLR integration",
	}
}

// GetQueryFeatures 返回 SQL Server 的查询特性
func (a *SQLServerAdapter) GetQueryFeatures() *QueryFeatures {
	return NewSQLServerQueryFeatures()
}

// init 自动注册 SQL Server 适配器
func init() {
	MustRegisterAdapterDescriptor("sqlserver", newSQLServerAdapterDescriptor())
}
