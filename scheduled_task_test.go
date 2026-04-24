package db

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestScheduledTaskConfigValidation 测试任务配置验证
func TestScheduledTaskConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *ScheduledTaskConfig
		wantErr bool
	}{
		{name: "nil config", config: nil, wantErr: true},
		{
			name:    "empty name",
			config:  &ScheduledTaskConfig{Name: "", Type: TaskTypeMonthlyTableCreation},
			wantErr: true,
		},
		{
			name:    "empty type",
			config:  &ScheduledTaskConfig{Name: "task1", Type: ""},
			wantErr: true,
		},
		{
			name: "monthly_table_creation without tableName",
			config: &ScheduledTaskConfig{
				Name:   "task1",
				Type:   TaskTypeMonthlyTableCreation,
				Config: map[string]interface{}{"monthFormat": "2006_01"},
			},
			wantErr: true,
		},
		{
			name: "valid monthly_table_creation",
			config: &ScheduledTaskConfig{
				Name:   "task1",
				Type:   TaskTypeMonthlyTableCreation,
				Config: map[string]interface{}{"tableName": "page_logs"},
			},
			wantErr: false,
		},
		{
			name:    "unsupported task type",
			config:  &ScheduledTaskConfig{Name: "task1", Type: ScheduledTaskType("unsupported")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetMonthlyTableConfig(t *testing.T) {
	config := &ScheduledTaskConfig{
		Name:   "task1",
		Type:   TaskTypeMonthlyTableCreation,
		Config: map[string]interface{}{"tableName": "page_logs"},
	}

	monthlyConfig := config.GetMonthlyTableConfig()
	if monthlyConfig == nil {
		t.Fatal("GetMonthlyTableConfig returned nil")
	}
	if tableName, ok := monthlyConfig["tableName"].(string); !ok || tableName != "page_logs" {
		t.Errorf("tableName mismatch: got %v", tableName)
	}
	if monthFormat, ok := monthlyConfig["monthFormat"].(string); !ok || monthFormat != "2006_01" {
		t.Errorf("monthFormat should have default value, got %v", monthFormat)
	}
	if fieldDefs, ok := monthlyConfig["fieldDefinitions"].(string); !ok || fieldDefs == "" {
		t.Errorf("fieldDefinitions should have default value, got %v", fieldDefs)
	}
}

// TestPostgreSQLRegisterScheduledTask 测试 PostgreSQL 注册定时任务
func TestPostgreSQLRegisterScheduledTask(t *testing.T) {
	pgHost := os.Getenv("PG_HOST")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDatabase := os.Getenv("PG_DATABASE")
	if pgHost == "" || pgUser == "" || pgDatabase == "" {
		t.Skip("PostgreSQL environment variables not set")
	}

	config := &Config{Adapter: "postgres", Host: pgHost, Port: 5432, Username: pgUser, Password: pgPassword, Database: pgDatabase, SSLMode: "disable"}
	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	task := &ScheduledTaskConfig{
		Name:        "test_monthly_pages",
		Type:        TaskTypeMonthlyTableCreation,
		Description: "Test monthly table creation",
		Enabled:     true,
		Config: map[string]interface{}{
			"tableName":   "test_page_logs",
			"monthFormat": "2006_01",
			"fieldDefinitions": `
				id BIGSERIAL PRIMARY KEY,
				user_id INTEGER,
				action VARCHAR(100),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			`,
		},
	}

	err = repo.RegisterScheduledTask(ctx, task)
	if err != nil {
		t.Fatalf("failed to register scheduled task: %v", err)
	}
	defer func() {
		_ = repo.UnregisterScheduledTask(ctx, task.Name)
		_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS test_page_logs_"+time.Now().Format("2006_01")+" CASCADE")
		_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS test_page_logs_"+time.Now().AddDate(0, 1, 0).Format("2006_01")+" CASCADE")
	}()

	tasks, err := repo.ListScheduledTasks(ctx)
	if err != nil {
		t.Fatalf("failed to list scheduled tasks after register: %v", err)
	}

	var found bool
	for _, scheduled := range tasks {
		if scheduled.Name == task.Name {
			found = true
			if scheduled.Type != task.Type {
				t.Fatalf("unexpected task type: got %s want %s", scheduled.Type, task.Type)
			}
			if scheduled.Info["function_name"] == "" {
				t.Fatalf("expected PostgreSQL scheduled task function_name metadata")
			}
			if scheduled.NextExecutedAt == 0 {
				t.Fatalf("expected PostgreSQL scheduled task next execution time")
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected registered PostgreSQL task %s to be listed", task.Name)
	}
}

// TestPostgreSQLUnregisterScheduledTask 测试注销定时任务
func TestPostgreSQLUnregisterScheduledTask(t *testing.T) {
	pgHost := os.Getenv("PG_HOST")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDatabase := os.Getenv("PG_DATABASE")
	if pgHost == "" || pgUser == "" || pgDatabase == "" {
		t.Skip("PostgreSQL environment variables not set")
	}

	config := &Config{Adapter: "postgres", Host: pgHost, Port: 5432, Username: pgUser, Password: pgPassword, Database: pgDatabase, SSLMode: "disable"}
	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	task := &ScheduledTaskConfig{
		Name:        "test_cleanup",
		Type:        TaskTypeMonthlyTableCreation,
		Description: "Test cleanup",
		Enabled:     true,
		Config:      map[string]interface{}{"tableName": "test_cleanup_logs"},
	}

	err = repo.RegisterScheduledTask(ctx, task)
	if err != nil {
		t.Fatalf("failed to register task: %v", err)
	}
	defer func() {
		_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS test_cleanup_logs_"+time.Now().Format("2006_01")+" CASCADE")
		_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS test_cleanup_logs_"+time.Now().AddDate(0, 1, 0).Format("2006_01")+" CASCADE")
	}()

	err = repo.UnregisterScheduledTask(ctx, task.Name)
	if err != nil {
		t.Fatalf("failed to unregister task: %v", err)
	}

	tasks, err := repo.ListScheduledTasks(ctx)
	if err != nil {
		t.Fatalf("failed to list scheduled tasks after unregister: %v", err)
	}
	for _, scheduled := range tasks {
		if scheduled.Name == task.Name {
			t.Fatalf("expected task %s to be removed from PostgreSQL metadata list", task.Name)
		}
	}
}

// TestPostgreSQLListScheduledTasks 测试列举定时任务
func TestPostgreSQLListScheduledTasks(t *testing.T) {
	pgHost := os.Getenv("PG_HOST")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDatabase := os.Getenv("PG_DATABASE")
	if pgHost == "" || pgUser == "" || pgDatabase == "" {
		t.Skip("PostgreSQL environment variables not set")
	}

	config := &Config{Adapter: "postgres", Host: pgHost, Port: 5432, Username: pgUser, Password: pgPassword, Database: pgDatabase, SSLMode: "disable"}
	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	taskName := fmt.Sprintf("test_list_pg_%d", time.Now().UnixNano())
	task := &ScheduledTaskConfig{Name: taskName, Type: TaskTypeMonthlyTableCreation, Config: map[string]interface{}{"tableName": "test_list_logs"}}
	if err := repo.RegisterScheduledTask(ctx, task); err != nil {
		t.Fatalf("failed to register PostgreSQL task for listing: %v", err)
	}
	defer func() {
		_ = repo.UnregisterScheduledTask(ctx, taskName)
		_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS test_list_logs_"+time.Now().Format("2006_01")+" CASCADE")
		_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS test_list_logs_"+time.Now().AddDate(0, 1, 0).Format("2006_01")+" CASCADE")
	}()

	tasks, err := repo.ListScheduledTasks(ctx)
	if err != nil {
		t.Fatalf("failed to list scheduled tasks: %v", err)
	}
	if tasks == nil {
		t.Error("expected tasks list, got nil")
	}

	var found bool
	for _, scheduled := range tasks {
		if scheduled.Name == taskName {
			found = true
			if scheduled.Info["schedule_mode"] == "" {
				t.Fatalf("expected PostgreSQL schedule_mode metadata")
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected listed PostgreSQL task %s", taskName)
	}
}

func TestPostgreSQLRegisterScheduledTaskIsIdempotent(t *testing.T) {
	pgHost := os.Getenv("PG_HOST")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDatabase := os.Getenv("PG_DATABASE")
	if pgHost == "" || pgUser == "" || pgDatabase == "" {
		t.Skip("PostgreSQL environment variables not set")
	}

	config := &Config{Adapter: "postgres", Host: pgHost, Port: 5432, Username: pgUser, Password: pgPassword, Database: pgDatabase, SSLMode: "disable"}
	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	taskName := fmt.Sprintf("test_idempotent_pg_%d", time.Now().UnixNano())
	task := &ScheduledTaskConfig{
		Name:           taskName,
		Type:           TaskTypeMonthlyTableCreation,
		CronExpression: "15 4 1 * *",
		Config: map[string]interface{}{
			"tableName": "test_idempotent_logs",
		},
	}

	if err := repo.RegisterScheduledTask(ctx, task); err != nil {
		t.Fatalf("first register failed: %v", err)
	}
	if err := repo.RegisterScheduledTask(ctx, task); err != nil {
		t.Fatalf("second register failed: %v", err)
	}
	defer func() {
		_ = repo.UnregisterScheduledTask(ctx, taskName)
		_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS test_idempotent_logs_"+time.Now().Format("2006_01")+" CASCADE")
		_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS test_idempotent_logs_"+time.Now().AddDate(0, 1, 0).Format("2006_01")+" CASCADE")
	}()

	tasks, err := repo.ListScheduledTasks(ctx)
	if err != nil {
		t.Fatalf("failed to list scheduled tasks after repeated register: %v", err)
	}

	matched := 0
	for _, scheduled := range tasks {
		if scheduled.Name != taskName {
			continue
		}
		matched++
		if scheduled.NextExecutedAt == 0 {
			t.Fatalf("expected idempotent PostgreSQL task next execution time")
		}
		if got := scheduled.Info["cron"]; got != task.CronExpression {
			t.Fatalf("unexpected cron metadata: got %v want %s", got, task.CronExpression)
		}
	}
	if matched != 1 {
		t.Fatalf("expected exactly one PostgreSQL task record after repeated register, got %d", matched)
	}
}

func TestParseSQLServerAgentMonthlySchedule(t *testing.T) {
	dayOfMonth, startTime, err := parseSQLServerAgentMonthlySchedule("15 4 1 * *")
	if err != nil {
		t.Fatalf("expected SQL Server monthly cron parsing to succeed: %v", err)
	}
	if dayOfMonth != 1 {
		t.Fatalf("unexpected SQL Server Agent day of month: got %d want 1", dayOfMonth)
	}
	if startTime != 41500 {
		t.Fatalf("unexpected SQL Server Agent start time: got %d want 41500", startTime)
	}
}

func TestParseSQLServerAgentMonthlyScheduleRejectsUnsupported(t *testing.T) {
	if _, _, err := parseSQLServerAgentMonthlySchedule("*/5 * * * *"); err == nil {
		t.Fatalf("expected SQL Server Agent cron parser to reject non-monthly expression")
	}
}

func TestSQLServerRegisterScheduledTask(t *testing.T) {
	sqlServerHost := os.Getenv("SQLSERVER_HOST")
	sqlServerUser := os.Getenv("SQLSERVER_USER")
	sqlServerPassword := os.Getenv("SQLSERVER_PASSWORD")
	sqlServerDatabase := os.Getenv("SQLSERVER_DB")
	if sqlServerHost == "" || sqlServerUser == "" || sqlServerDatabase == "" {
		t.Skip("SQL Server environment variables not set")
	}

	config := &Config{
		Adapter:  "sqlserver",
		Host:     sqlServerHost,
		Port:     1433,
		Username: sqlServerUser,
		Password: sqlServerPassword,
		Database: sqlServerDatabase,
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create SQL Server repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	taskName := fmt.Sprintf("test_sqlserver_task_%d", time.Now().UnixNano())
	task := &ScheduledTaskConfig{
		Name:           taskName,
		Type:           TaskTypeMonthlyTableCreation,
		CronExpression: "0 1 1 * *",
		Config: map[string]interface{}{
			"tableName": "test_sqlserver_logs",
		},
	}

	if err := repo.RegisterScheduledTask(ctx, task); err != nil {
		t.Fatalf("expected SQL Server scheduled task registration to succeed natively or via fallback, got: %v", err)
	}
	defer func() {
		_ = repo.UnregisterScheduledTask(ctx, taskName)
		_, _ = repo.Exec(ctx, "IF OBJECT_ID(N'[dbo].[test_sqlserver_logs_"+time.Now().Format("2006_01")+" ]', N'U') IS NOT NULL DROP TABLE [dbo].[test_sqlserver_logs_"+time.Now().Format("2006_01")+"]")
		_, _ = repo.Exec(ctx, "IF OBJECT_ID(N'[dbo].[test_sqlserver_logs_"+time.Now().AddDate(0, 1, 0).Format("2006_01")+" ]', N'U') IS NOT NULL DROP TABLE [dbo].[test_sqlserver_logs_"+time.Now().AddDate(0, 1, 0).Format("2006_01")+"]")
	}()

	tasks, err := repo.ListScheduledTasks(ctx)
	if err != nil {
		t.Fatalf("failed to list SQL Server scheduled tasks: %v", err)
	}

	var found bool
	for _, scheduled := range tasks {
		if scheduled.Name != taskName {
			continue
		}
		found = true
		if scheduled.Type != task.Type {
			t.Fatalf("unexpected SQL Server task type: got %s want %s", scheduled.Type, task.Type)
		}
		break
	}
	if !found {
		t.Fatalf("expected SQL Server scheduled task %s to be listed", taskName)
	}

	if err := repo.UnregisterScheduledTask(ctx, taskName); err != nil {
		t.Fatalf("failed to unregister SQL Server scheduled task: %v", err)
	}
}

// TestMySQLRegisterScheduledTask 测试 MySQL 通过回退调度器支持定时任务
func TestMySQLRegisterScheduledTask(t *testing.T) {
	mysqlHost := os.Getenv("MYSQL_HOST")
	mysqlUser := os.Getenv("MYSQL_USER")
	mysqlPassword := os.Getenv("MYSQL_PASSWORD")
	mysqlDatabase := os.Getenv("MYSQL_DATABASE")

	if mysqlHost == "" || mysqlUser == "" || mysqlDatabase == "" {
		t.Skip("MySQL environment variables not set")
	}

	config := &Config{
		Adapter:  "mysql",
		Host:     mysqlHost,
		Port:     3306,
		Username: mysqlUser,
		Password: mysqlPassword,
		Database: mysqlDatabase,
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	task := &ScheduledTaskConfig{
		Name: "test_task",
		Type: TaskTypeMonthlyTableCreation,
		Config: map[string]interface{}{
			"tableName": "test_logs",
		},
	}

	err = repo.RegisterScheduledTask(ctx, task)
	if err != nil {
		t.Fatalf("expected fallback scheduler to register mysql task, got error: %v", err)
	}

	tasks, err := repo.ListScheduledTasks(ctx)
	if err != nil {
		t.Fatalf("list scheduled tasks failed: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatalf("expected fallback task to be listed")
	}

	if err := repo.UnregisterScheduledTask(ctx, task.Name); err != nil {
		t.Fatalf("unregister scheduled task failed: %v", err)
	}

	t.Logf("✓ MySQL fallback scheduler supports register/list/unregister")
}

// TestSQLiteRegisterScheduledTask 测试 SQLite 通过回退调度器支持定时任务
func TestSQLiteRegisterScheduledTask(t *testing.T) {
	config := &Config{
		Adapter:  "sqlite",
		Database: ":memory:",
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	task := &ScheduledTaskConfig{
		Name: "test_task",
		Type: TaskTypeMonthlyTableCreation,
		Config: map[string]interface{}{
			"tableName": "test_logs",
		},
	}

	err = repo.RegisterScheduledTask(ctx, task)
	if err != nil {
		t.Fatalf("expected fallback scheduler to register sqlite task, got error: %v", err)
	}

	tasks, err := repo.ListScheduledTasks(ctx)
	if err != nil {
		t.Fatalf("list scheduled tasks failed: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatalf("expected fallback task to be listed")
	}

	if err := repo.UnregisterScheduledTask(ctx, task.Name); err != nil {
		t.Fatalf("unregister scheduled task failed: %v", err)
	}

	t.Logf("✓ SQLite fallback scheduler supports register/list/unregister")
}

// TestInvalidScheduledTaskConfig 测试无效的任务配置
func TestInvalidScheduledTaskConfig(t *testing.T) {
	config := &Config{
		Adapter:  "sqlite",
		Database: ":memory:",
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// 测试 nil 配置
	err = repo.RegisterScheduledTask(ctx, nil)
	if err == nil {
		t.Error("expected error for nil task config")
	}

	// 测试空名称
	task := &ScheduledTaskConfig{
		Name: "",
		Type: TaskTypeMonthlyTableCreation,
	}

	err = repo.RegisterScheduledTask(ctx, task)
	if err == nil {
		t.Error("expected error for empty task name")
	}

	t.Log("✓ Invalid task configs properly rejected")
}

// TestUnregisterScheduledTaskEmptyName 测试注销空名称的任务
func TestUnregisterScheduledTaskEmptyName(t *testing.T) {
	config := &Config{
		Adapter:  "sqlite",
		Database: ":memory:",
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	err = repo.UnregisterScheduledTask(ctx, "")
	if err == nil {
		t.Error("expected error for empty task name")
	}

	t.Log("✓ Empty task name properly rejected")
}

func TestSQLiteScheduledTaskFallbackDisabled(t *testing.T) {
	disabled := false
	config := &Config{
		Adapter:                     "sqlite",
		Database:                    ":memory:",
		EnableScheduledTaskFallback: &disabled,
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	task := &ScheduledTaskConfig{
		Name: "test_task_fallback_disabled",
		Type: TaskTypeMonthlyTableCreation,
		Config: map[string]interface{}{
			"tableName": "test_logs",
		},
	}

	err = repo.RegisterScheduledTask(ctx, task)
	if err == nil {
		t.Fatalf("expected adapter native error when fallback is disabled")
	}
}

func TestConfigScheduledTaskFallbackEnabledDefaultAndOption(t *testing.T) {
	cfg := &Config{}
	if !cfg.ScheduledTaskFallbackEnabled() {
		t.Fatalf("expected default scheduled task fallback enabled")
	}

	cfg = &Config{Options: map[string]interface{}{"scheduled_task_fallback": false}}
	if cfg.ScheduledTaskFallbackEnabled() {
		t.Fatalf("expected fallback disabled from options boolean")
	}

	cfg = &Config{Options: map[string]interface{}{"scheduled_task_fallback": "false"}}
	if cfg.ScheduledTaskFallbackEnabled() {
		t.Fatalf("expected fallback disabled from options string")
	}
}

func TestScheduledTaskFallbackErrorHelpers(t *testing.T) {
	err := NewScheduledTaskFallbackError("sqlite", "native scheduled tasks not implemented")
	if !IsScheduledTaskFallbackError(err) {
		t.Fatalf("expected shared fallback error to be detected")
	}
	if reason, ok := ScheduledTaskFallbackReasonOf(err); !ok || reason != ScheduledTaskFallbackReasonUnknown {
		t.Fatalf("expected unknown fallback reason for legacy constructor, got reason=%s ok=%v", reason, ok)
	}

	typed := NewScheduledTaskFallbackErrorWithReason(
		"sqlserver",
		ScheduledTaskFallbackReasonCronExpressionUnsupported,
		"cron shape not supported",
	)
	if !IsScheduledTaskFallbackError(typed) {
		t.Fatalf("expected typed fallback error to be detected")
	}
	if reason, ok := ScheduledTaskFallbackReasonOf(typed); !ok || reason != ScheduledTaskFallbackReasonCronExpressionUnsupported {
		t.Fatalf("expected typed fallback reason to be extracted, got reason=%s ok=%v", reason, ok)
	}

	legacy := fmt.Errorf("SQLite adapter: scheduled tasks not implemented")
	if !IsScheduledTaskFallbackError(legacy) {
		t.Fatalf("expected legacy fallback error text to still be detected")
	}
	if reason, ok := ScheduledTaskFallbackReasonOf(legacy); !ok || reason != ScheduledTaskFallbackReasonUnknown {
		t.Fatalf("expected legacy fallback reason to default unknown, got reason=%s ok=%v", reason, ok)
	}

	nonFallback := fmt.Errorf("invalid cron expression")
	if IsScheduledTaskFallbackError(nonFallback) {
		t.Fatalf("expected non-fallback error to be rejected")
	}
	if _, ok := ScheduledTaskFallbackReasonOf(nonFallback); ok {
		t.Fatalf("expected non-fallback error to have no fallback reason")
	}
}
