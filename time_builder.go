package db

import "time"

// DateBuilder 提供应用层日期构建能力，适合生成业务时间边界或默认时间值。
type DateBuilder struct {
	t time.Time
}

// DateByYMD 基于年月日创建日期构建器，时间部分默认为 00:00:00（本地时区）。
func DateByYMD(year, month, day int) *DateBuilder {
	return &DateBuilder{
		t: time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local),
	}
}

// TodayBuilder 基于当前日期创建构建器，时间部分为 00:00:00（当前时区）。
func TodayBuilder() *DateBuilder {
	now := time.Now()
	return &DateBuilder{
		t: time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()),
	}
}

// TodayUTCBuilder 基于当前 UTC 日期创建构建器，时间部分为 00:00:00（UTC）。
func TodayUTCBuilder() *DateBuilder {
	now := time.Now().UTC()
	return &DateBuilder{
		t: time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC),
	}
}

// AddYears 增加（或减少）年数。
func (b *DateBuilder) AddYears(years int) *DateBuilder {
	b.t = b.t.AddDate(years, 0, 0)
	return b
}

// AddMonths 增加（或减少）月数。
func (b *DateBuilder) AddMonths(months int) *DateBuilder {
	b.t = b.t.AddDate(0, months, 0)
	return b
}

// AddDays 增加（或减少）天数。
func (b *DateBuilder) AddDays(days int) *DateBuilder {
	b.t = b.t.AddDate(0, 0, days)
	return b
}

// StartOfMonth 对齐到当月月初 00:00:00。
func (b *DateBuilder) StartOfMonth() *DateBuilder {
	b.t = time.Date(b.t.Year(), b.t.Month(), 1, 0, 0, 0, 0, b.t.Location())
	return b
}

// EndOfMonth 对齐到当月月末 23:59:59。
func (b *DateBuilder) EndOfMonth() *DateBuilder {
	nextMonthStart := time.Date(b.t.Year(), b.t.Month()+1, 1, 0, 0, 0, 0, b.t.Location())
	b.t = nextMonthStart.Add(-time.Second)
	return b
}

// StartOfQuarter 对齐到当前季度起始日 00:00:00。
func (b *DateBuilder) StartOfQuarter() *DateBuilder {
	quarterStartMonth := ((int(b.t.Month())-1)/3)*3 + 1
	b.t = time.Date(b.t.Year(), time.Month(quarterStartMonth), 1, 0, 0, 0, 0, b.t.Location())
	return b
}

// EndOfQuarter 对齐到当前季度末日 23:59:59。
func (b *DateBuilder) EndOfQuarter() *DateBuilder {
	quarterStartMonth := ((int(b.t.Month())-1)/3)*3 + 1
	nextQuarterStart := time.Date(b.t.Year(), time.Month(quarterStartMonth+3), 1, 0, 0, 0, 0, b.t.Location())
	b.t = nextQuarterStart.Add(-time.Second)
	return b
}

// At 设置时分秒，便于从日期边界构造完整时间戳。
func (b *DateBuilder) At(hour, minute, second int) *DateBuilder {
	b.t = time.Date(
		b.t.Year(),
		b.t.Month(),
		b.t.Day(),
		hour,
		minute,
		second,
		0,
		b.t.Location(),
	)
	return b
}

// In 将构建器时间转换到指定时区。
func (b *DateBuilder) In(loc *time.Location) *DateBuilder {
	if loc != nil {
		b.t = b.t.In(loc)
	}
	return b
}

// Build 返回最终时间。
func (b *DateBuilder) Build() time.Time {
	return b.t
}

// YearsLater 基于当前日期返回 N 年后日期（00:00:00）。
func YearsLater(years int) time.Time {
	return TodayBuilder().AddYears(years).Build()
}

// MonthsLater 基于当前日期返回 N 个月后日期（00:00:00）。
func MonthsLater(months int) time.Time {
	return TodayBuilder().AddMonths(months).Build()
}

// DaysLater 基于当前日期返回 N 天后日期（00:00:00）。
func DaysLater(days int) time.Time {
	return TodayBuilder().AddDays(days).Build()
}

// OneYearLater 返回当前日期一年后的日期（00:00:00）。
func OneYearLater() time.Time {
	return YearsLater(1)
}

// YearsLaterUTC 基于当前 UTC 日期返回 N 年后日期（00:00:00 UTC）。
func YearsLaterUTC(years int) time.Time {
	return TodayUTCBuilder().AddYears(years).Build()
}

// MonthsLaterUTC 基于当前 UTC 日期返回 N 个月后日期（00:00:00 UTC）。
func MonthsLaterUTC(months int) time.Time {
	return TodayUTCBuilder().AddMonths(months).Build()
}

// DaysLaterUTC 基于当前 UTC 日期返回 N 天后日期（00:00:00 UTC）。
func DaysLaterUTC(days int) time.Time {
	return TodayUTCBuilder().AddDays(days).Build()
}

// OneYearLaterUTC 返回当前 UTC 日期一年后的日期（00:00:00 UTC）。
func OneYearLaterUTC() time.Time {
	return YearsLaterUTC(1)
}

// StartOfCurrentMonth 返回当前月月初（本地时区，00:00:00）。
func StartOfCurrentMonth() time.Time {
	return TodayBuilder().StartOfMonth().Build()
}

// EndOfCurrentMonth 返回当前月月末（本地时区，23:59:59）。
func EndOfCurrentMonth() time.Time {
	return TodayBuilder().EndOfMonth().Build()
}

// StartOfCurrentQuarter 返回当前季度起始时间（本地时区，00:00:00）。
func StartOfCurrentQuarter() time.Time {
	return TodayBuilder().StartOfQuarter().Build()
}

// EndOfCurrentQuarter 返回当前季度结束时间（本地时区，23:59:59）。
func EndOfCurrentQuarter() time.Time {
	return TodayBuilder().EndOfQuarter().Build()
}

// NextBillingDate 返回“下一个账单日”日期（本地时区，00:00:00）。
// 规则：若本月账单日尚未到达（含当天）则返回本月，否则返回下月；超出月天数时自动对齐到月末。
func NextBillingDate(day int) time.Time {
	return nextBillingDateFrom(time.Now(), day)
}

func nextBillingDateFrom(now time.Time, day int) time.Time {
	loc := now.Location()
	if day < 1 {
		day = 1
	}

	year, month, currentDay := now.Date()
	today := time.Date(year, month, currentDay, 0, 0, 0, 0, loc)

	thisMonthDay := clampDay(year, month, day, loc)
	thisMonthBilling := time.Date(year, month, thisMonthDay, 0, 0, 0, 0, loc)
	if !thisMonthBilling.Before(today) {
		return thisMonthBilling
	}

	nextMonthAnchor := time.Date(year, month+1, 1, 0, 0, 0, 0, loc)
	ny, nm, _ := nextMonthAnchor.Date()
	nextMonthDay := clampDay(ny, nm, day, loc)
	return time.Date(ny, nm, nextMonthDay, 0, 0, 0, 0, loc)
}

// SLADeadline 返回 SLA 截止时间。
// workdaysOnly 为 false：按自然时间增加小时；为 true：仅计入工作日（周一到周五）小时。
func SLADeadline(hours int, workdaysOnly bool) time.Time {
	return slaDeadlineFrom(time.Now(), hours, workdaysOnly)
}

func slaDeadlineFrom(start time.Time, hours int, workdaysOnly bool) time.Time {
	if !workdaysOnly {
		return start.Add(time.Duration(hours) * time.Hour)
	}
	if hours == 0 {
		return start
	}

	step := 1
	remaining := hours
	if hours < 0 {
		step = -1
		remaining = -hours
	}

	current := start
	for remaining > 0 {
		current = current.Add(time.Duration(step) * time.Hour)
		if current.Weekday() != time.Saturday && current.Weekday() != time.Sunday {
			remaining--
		}
	}

	return current
}

func clampDay(year int, month time.Month, day int, loc *time.Location) int {
	if day < 1 {
		return 1
	}
	last := time.Date(year, month+1, 0, 0, 0, 0, 0, loc).Day()
	if day > last {
		return last
	}
	return day
}
