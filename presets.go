package db

import "time"

// PresetRegistry 统一预置工具入口。
// 后续可继续扩展为 Presets.User / Presets.Order / Presets.Task 等模块。
// 约束：这里仅放应用层语义预置（adapter-agnostic）。
// 例如 Mongo pipeline / collection / BSON 这类 Adapter 细节不应进入 Presets，
// 需放在各 Adapter 自己的能力层实现与文档中。
type PresetRegistry struct {
	Date DatePresetRegistry
}

// DatePresetRegistry 日期相关预置工具集合。
type DatePresetRegistry struct{}

// Presets 全局预置入口。
var Presets = PresetRegistry{
	Date: DatePresetRegistry{},
}

// ActiveUsersInBusinessHours 预置条件：活跃用户 + 今天工作时段。
func (DatePresetRegistry) ActiveUsersInBusinessHours() Condition {
	return ActiveUsersInBusinessHours()
}

// ActiveUsersInBusinessHoursBy 可配置版本。
func (DatePresetRegistry) ActiveUsersInBusinessHoursBy(statusField string, activeValue interface{}, timeField string, startHour, endHour int) Condition {
	return ActiveUsersInBusinessHoursBy(statusField, activeValue, timeField, startHour, endHour)
}

// OrdersInCurrentQuarter 预置条件：订单创建时间在当前季度。
func (DatePresetRegistry) OrdersInCurrentQuarter() Condition {
	return OrdersInCurrentQuarter()
}

// OrdersInCurrentQuarterBy 可配置版本。
func (DatePresetRegistry) OrdersInCurrentQuarterBy(timeField string) Condition {
	return OrdersInCurrentQuarterBy(timeField)
}

// DueSoonTasksInBusinessDays 预置条件：待处理任务且截止时间在未来 N 个工作日内。
func (DatePresetRegistry) DueSoonTasksInBusinessDays(days int, holidays ...time.Time) Condition {
	return DueSoonTasksInBusinessDays(days, holidays...)
}
