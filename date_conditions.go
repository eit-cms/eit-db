package db

import "time"

// DateOn 生成“某一天内”的条件（闭开区间：[dayStart, nextDayStart)）。
func DateOn(field string, year, month, day int) Condition {
	dayStart := DateByYMD(year, month, day).Build()
	nextDayStart := DateByYMD(year, month, day).AddDays(1).Build()
	return And(Gte(field, dayStart), Lt(field, nextDayStart))
}

// DateFrom 生成“字段时间 >= start”的条件。
func DateFrom(field string, start time.Time) Condition {
	return Gte(field, start)
}

// DateTo 生成“字段时间 <= end”的条件。
func DateTo(field string, end time.Time) Condition {
	return Lte(field, end)
}

// DateRange 生成“字段时间在 [start, end] 区间内”的条件。
// 若 end 在 start 之前，将自动交换顺序。
func DateRange(field string, start, end time.Time) Condition {
	if end.Before(start) {
		start, end = end, start
	}
	return Between(field, start, end)
}

// DateInCurrentMonth 生成“字段时间位于当前月”的条件（闭开区间：[monthStart, nextMonthStart)）。
func DateInCurrentMonth(field string) Condition {
	monthStart := StartOfCurrentMonth()
	nextMonthStart := DateByYMD(monthStart.Year(), int(monthStart.Month()), 1).AddMonths(1).Build()
	return And(Gte(field, monthStart), Lt(field, nextMonthStart))
}

// DateInCurrentQuarter 生成“字段时间位于当前季度”的条件（闭开区间：[quarterStart, nextQuarterStart)）。
func DateInCurrentQuarter(field string) Condition {
	quarterStart := StartOfCurrentQuarter()
	nextQuarterStart := DateByYMD(quarterStart.Year(), int(quarterStart.Month()), 1).AddMonths(3).Build()
	return And(Gte(field, quarterStart), Lt(field, nextQuarterStart))
}

// DateInBusinessHours 生成“当前日期工作时段”的条件（闭开区间：[start, end)）。
// 默认基于本地时区今天，startHour/endHour 建议范围为 0-23。
// 当 endHour <= startHour 时，视为跨天时段（例如 22 -> 6）。
func DateInBusinessHours(field string, startHour, endHour int) Condition {
	return dateInBusinessHoursFrom(field, time.Now(), startHour, endHour)
}

func dateInBusinessHoursFrom(field string, now time.Time, startHour, endHour int) Condition {
	loc := now.Location()
	startHour = clampHour(startHour)
	endHour = clampHour(endHour)

	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	start := dayStart.Add(time.Duration(startHour) * time.Hour)
	end := dayStart.Add(time.Duration(endHour) * time.Hour)
	if endHour <= startHour {
		end = end.Add(24 * time.Hour)
	}

	return And(Gte(field, start), Lt(field, end))
}

// DateInLastBusinessDays 生成“最近 N 个工作日”的条件。
// 工作日定义为周一到周五；holidays 可传入节假日日期（只比较年月日）。
// 返回按天窗口 OR 组合条件，每天为闭开区间 [dayStart, nextDayStart)。
func DateInLastBusinessDays(field string, days int, holidays ...time.Time) Condition {
	return dateInLastBusinessDaysFrom(field, time.Now(), days, holidays...)
}

func dateInLastBusinessDaysFrom(field string, now time.Time, days int, holidays ...time.Time) Condition {
	if days < 1 {
		days = 1
	}

	holidaySet := buildHolidaySet(holidays...)
	loc := now.Location()
	cursor := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	conditions := make([]Condition, 0, days)

	for len(conditions) < days {
		if isBusinessDay(cursor, holidaySet) {
			next := cursor.Add(24 * time.Hour)
			conditions = append(conditions, And(Gte(field, cursor), Lt(field, next)))
		}
		cursor = cursor.AddDate(0, 0, -1)
	}

	if len(conditions) == 1 {
		return conditions[0]
	}
	return Or(conditions...)
}

func buildHolidaySet(holidays ...time.Time) map[string]struct{} {
	set := make(map[string]struct{}, len(holidays))
	for _, h := range holidays {
		key := h.Format("2006-01-02")
		set[key] = struct{}{}
	}
	return set
}

func isBusinessDay(day time.Time, holidaySet map[string]struct{}) bool {
	if day.Weekday() == time.Saturday || day.Weekday() == time.Sunday {
		return false
	}
	if _, ok := holidaySet[day.Format("2006-01-02")]; ok {
		return false
	}
	return true
}

func clampHour(hour int) int {
	if hour < 0 {
		return 0
	}
	if hour > 23 {
		return 23
	}
	return hour
}
