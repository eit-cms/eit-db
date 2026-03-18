package db

import "time"

// ActiveUsersInBusinessHours 预置条件：活跃用户 + 今天工作时段。
// 默认约定：status = "active"，时间字段为 updated_at，工作时段为 09:00-18:00。
func ActiveUsersInBusinessHours() Condition {
	return ActiveUsersInBusinessHoursBy("status", "active", "updated_at", 9, 18)
}

// ActiveUsersInBusinessHoursBy 可配置版本。
func ActiveUsersInBusinessHoursBy(statusField string, activeValue interface{}, timeField string, startHour, endHour int) Condition {
	return And(
		Eq(statusField, activeValue),
		DateInBusinessHours(timeField, startHour, endHour),
	)
}

// OrdersInCurrentQuarter 预置条件：订单创建时间在当前季度。
// 默认约定：订单创建时间字段为 created_at。
func OrdersInCurrentQuarter() Condition {
	return OrdersInCurrentQuarterBy("created_at")
}

// OrdersInCurrentQuarterBy 可配置版本。
func OrdersInCurrentQuarterBy(timeField string) Condition {
	return DateInCurrentQuarter(timeField)
}

// DueSoonTasksInBusinessDays 预置条件：待处理任务且截止时间在未来 N 个工作日内。
// 默认约定：status = "pending"，截止字段为 due_at。
func DueSoonTasksInBusinessDays(days int, holidays ...time.Time) Condition {
	return dueSoonTasksInBusinessDaysFrom(time.Now(), days, holidays...)
}

func dueSoonTasksInBusinessDaysFrom(now time.Time, days int, holidays ...time.Time) Condition {
	deadline := businessDaysDeadlineFrom(now, days, holidays...)
	return And(
		Eq("status", "pending"),
		Gte("due_at", now),
		Lte("due_at", deadline),
	)
}

func businessDaysDeadlineFrom(start time.Time, days int, holidays ...time.Time) time.Time {
	if days < 0 {
		days = 0
	}

	holidaySet := buildHolidaySet(holidays...)
	loc := start.Location()
	cursor := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, loc)
	count := 0
	for count < days {
		cursor = cursor.AddDate(0, 0, 1)
		if isBusinessDay(cursor, holidaySet) {
			count++
		}
	}

	return time.Date(cursor.Year(), cursor.Month(), cursor.Day(), 23, 59, 59, 0, loc)
}
