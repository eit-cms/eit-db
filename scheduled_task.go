package db

import (
	"context"
	"fmt"
)

// ScheduledTaskType 定时任务类型枚举
type ScheduledTaskType string

const (
	// TaskTypeMonthlyTableCreation 按月自动创建表的任务
	TaskTypeMonthlyTableCreation ScheduledTaskType = "monthly_table_creation"
)

// ScheduledTaskConfig 定时任务配置
// 这个结构体定义了要执行的定时任务的参数，不同的数据库适配器
// 可以根据不同的实现方式（如 PostgreSQL 触发器、应用层 cron）来处理
type ScheduledTaskConfig struct {
	// 任务的唯一标识符（如 "monthly_page_logs"）
	Name string

	// 任务类型
	Type ScheduledTaskType

	// 任务的 Cron 表达式（如 "0 0 1 * *" 表示每月1号0点）
	// 某些数据库实现可能不使用此字段
	CronExpression string

	// 任务特定的配置参数，根据任务类型而定
	// 对于 TaskTypeMonthlyTableCreation，应包含：
	//   - tableName: string - 表名前缀
	//   - monthFormat: string - 月份格式（如 "2006_01"）
	//   - fieldDefinitions: string - 表字段定义（SQL DDL）
	Config map[string]interface{}

	// 任务描述
	Description string

	// 是否启用此任务
	Enabled bool
}

// Validate 验证任务配置的有效性
func (c *ScheduledTaskConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("task config cannot be nil")
	}

	if c.Name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	if c.Type == "" {
		return fmt.Errorf("task type cannot be empty")
	}

	// 根据任务类型进行特定验证
	switch c.Type {
	case TaskTypeMonthlyTableCreation:
		return c.validateMonthlyTableCreation()
	default:
		return fmt.Errorf("unsupported task type: %s", c.Type)
	}
}

// validateMonthlyTableCreation 验证按月创建表任务的配置
func (c *ScheduledTaskConfig) validateMonthlyTableCreation() error {
	if c.Config == nil || len(c.Config) == 0 {
		return fmt.Errorf("task config cannot be empty for monthly_table_creation")
	}

	// 验证必需的 tableName
	tableName, ok := c.Config["tableName"].(string)
	if !ok || tableName == "" {
		return fmt.Errorf("tableName is required and must be a string")
	}

	// monthFormat 可选，有默认值
	// fieldDefinitions 可选，有默认值

	return nil
}

// GetMonthlyTableConfig 便捷方法：获取按月创建表的配置
func (c *ScheduledTaskConfig) GetMonthlyTableConfig() map[string]interface{} {
	if c.Type != TaskTypeMonthlyTableCreation {
		return nil
	}

	config := make(map[string]interface{})
	for k, v := range c.Config {
		config[k] = v
	}

	// 填充默认值
	if _, ok := config["monthFormat"].(string); !ok {
		config["monthFormat"] = "2006_01"
	}

	if _, ok := config["fieldDefinitions"].(string); !ok {
		config["fieldDefinitions"] = `
			id BIGSERIAL PRIMARY KEY,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		`
	}

	return config
}

// ScheduledTaskStatus 定时任务执行状态
type ScheduledTaskStatus struct {
	// 任务名称
	Name string

	// 任务类型
	Type ScheduledTaskType

	// 是否正在运行
	Running bool

	// 上次执行时间（Unix 时间戳）
	LastExecutedAt int64

	// 下次执行时间（Unix 时间戳，如果适用）
	NextExecutedAt int64

	// 任务创建者信息（可选）
	CreatedAt int64

	// 额外的状态信息
	Info map[string]interface{}
}

// ScheduledTaskExecutor 定时任务执行器接口
// 由应用层实现，用于执行不同类型的定时任务
type ScheduledTaskExecutor interface {
	// Execute 执行定时任务
	// taskName: 任务名称
	// config: 完整的任务配置
	Execute(ctx context.Context, taskName string, config *ScheduledTaskConfig) error
}

// ScheduledTaskEventListener 定时任务事件监听器接口
// 用于监听定时任务的生命周期事件
type ScheduledTaskEventListener interface {
	OnTaskRegistered(ctx context.Context, config *ScheduledTaskConfig) error
	OnTaskUnregistered(ctx context.Context, taskName string) error
	OnTaskExecutionStarted(ctx context.Context, taskName string) error
	OnTaskExecutionCompleted(ctx context.Context, taskName string, err error) error
}
