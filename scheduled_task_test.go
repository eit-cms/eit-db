package db

import (
	"context"
	"os"
	"testing"
)

// TestScheduledTaskConfigValidation 测试任务配置验证
func TestScheduledTaskConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *ScheduledTaskConfig
		wantErr bool
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "empty name",
			config: &ScheduledTaskConfig{
				Name: "",
				Type: TaskTypeMonthlyTableCreation,
			},
			wantErr: true,
		},
		{
			name: "empty type",
			config: &ScheduledTaskConfig{
				Name: "task1",
				Type: "",
			},
			wantErr: true,
		},
		{
			name: "monthly_table_creation without tableName",
			config: &ScheduledTaskConfig{
				Name: "task1",
				Type: TaskTypeMonthlyTableCreation,
				Config: map[string]interface{}{
					"monthFormat": "2006_01",
				},
			},
			wantErr: true,
		},
		{
			name: "valid monthly_table_creation",
			config: &ScheduledTaskConfig{
				Name: "task1",
				Type: TaskTypeMonthlyTableCreation,
				Config: map[string]interface{}{
					"tableName": "page_logs",
				},
			},
			wantErr: false,
		},
		{
			name: "unsupported task type",
			config: &ScheduledTaskConfig{
				Name: "task1",
				Type: ScheduledTaskType("unsupported"),
			},
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

// TestGetMonthlyTableConfig 测试获取按月表的配置
func TestGetMonthlyTableConfig(t *testing.T) {
	config := &ScheduledTaskConfig{
		Name: "task1",
		Type: TaskTypeMonthlyTableCreation,
		Config: map[string]interface{}{
			"tableName": "page_logs",
		},
	}

	monthlyConfig := config.GetMonthlyTableConfig()

	if monthlyConfig == nil {
		t.Fatal("GetMonthlyTableConfig returned nil")
	}

	if tableName, ok := monthlyConfig["tableName"].(string); !ok || tableName != "page_logs" {
		t.Errorf("tableName mismatch: got %v", tableName)
	}

	// 验证默认值
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

	config := &Config{
		Adapter:  "postgres",
		Host:     pgHost,
		Port:     5432,
		Username: pgUser,
		Password: pgPassword,
		Database: pgDatabase,
		SSLMode:  "disable",
	}

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
			"tableName": "test_page_logs",
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

	t.Log("✓ PostgreSQL RegisterScheduledTask passed")

	// 清理：删除创建的函数和表
	defer func() {
		_, _ = repo.Exec(ctx, "DROP FUNCTION IF EXISTS test_monthly_pages_create_table() CASCADE")
		_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS test_page_logs_* CASCADE")
	}()
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

	config := &Config{
		Adapter:  "postgres",
		Host:     pgHost,
		Port:     5432,
		Username: pgUser,
		Password: pgPassword,
		Database: pgDatabase,
		SSLMode:  "disable",
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// 先注册
	task := &ScheduledTaskConfig{
		Name:        "test_cleanup",
		Type:        TaskTypeMonthlyTableCreation,
		Description: "Test cleanup",
		Enabled:     true,
		Config: map[string]interface{}{
			"tableName": "test_cleanup_logs",
		},
	}

	err = repo.RegisterScheduledTask(ctx, task)
	if err != nil {
		t.Fatalf("failed to register task: %v", err)
	}

	// 然后注销
	err = repo.UnregisterScheduledTask(ctx, task.Name)
	if err != nil {
		t.Fatalf("failed to unregister task: %v", err)
	}

	t.Log("✓ PostgreSQL UnregisterScheduledTask passed")
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

	config := &Config{
		Adapter:  "postgres",
		Host:     pgHost,
		Port:     5432,
		Username: pgUser,
		Password: pgPassword,
		Database: pgDatabase,
		SSLMode:  "disable",
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// 列举任务（PostgreSQL 版本返回空列表）
	tasks, err := repo.ListScheduledTasks(ctx)
	if err != nil {
		t.Fatalf("failed to list scheduled tasks: %v", err)
	}

	if tasks == nil {
		t.Error("expected tasks list, got nil")
	}

	t.Logf("✓ PostgreSQL ListScheduledTasks passed (count: %d)", len(tasks))
}

// TestMySQLRegisterScheduledTask 测试 MySQL 不支持定时任务
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
	if err == nil {
		t.Error("expected error for MySQL RegisterScheduledTask, got nil")
	}

	t.Logf("✓ MySQL correctly returns not supported error")
}

// TestSQLiteRegisterScheduledTask 测试 SQLite 不支持定时任务
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
	if err == nil {
		t.Error("expected error for SQLite RegisterScheduledTask, got nil")
	}

	t.Logf("✓ SQLite correctly returns not supported error")
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
