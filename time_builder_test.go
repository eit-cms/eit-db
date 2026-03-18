package db

import (
	"testing"
	"time"
)

func TestDateByYMDBuild(t *testing.T) {
	got := DateByYMD(2026, 3, 17).Build()
	if got.Year() != 2026 || int(got.Month()) != 3 || got.Day() != 17 {
		t.Fatalf("unexpected date: %v", got)
	}
	if got.Hour() != 0 || got.Minute() != 0 || got.Second() != 0 {
		t.Fatalf("expected midnight, got %v", got)
	}
}

func TestDateBuilderAddAndAt(t *testing.T) {
	got := DateByYMD(2026, 1, 31).
		AddMonths(1).
		AddDays(2).
		At(10, 30, 15).
		Build()

	want := time.Date(2026, 3, 5, 10, 30, 15, 0, time.Local)
	if !got.Equal(want) {
		t.Fatalf("unexpected datetime, want=%v got=%v", want, got)
	}
}

func TestYearsMonthsDaysLater(t *testing.T) {
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	y1 := YearsLater(1)
	if !y1.Equal(today.AddDate(1, 0, 0)) {
		t.Fatalf("YearsLater mismatch, want=%v got=%v", today.AddDate(1, 0, 0), y1)
	}

	m2 := MonthsLater(2)
	if !m2.Equal(today.AddDate(0, 2, 0)) {
		t.Fatalf("MonthsLater mismatch, want=%v got=%v", today.AddDate(0, 2, 0), m2)
	}

	d10 := DaysLater(10)
	if !d10.Equal(today.AddDate(0, 0, 10)) {
		t.Fatalf("DaysLater mismatch, want=%v got=%v", today.AddDate(0, 0, 10), d10)
	}
}

func TestOneYearLater(t *testing.T) {
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	got := OneYearLater()
	want := today.AddDate(1, 0, 0)
	if !got.Equal(want) {
		t.Fatalf("OneYearLater mismatch, want=%v got=%v", want, got)
	}
}

func TestDateBuilderStartAndEndOfMonth(t *testing.T) {
	start := DateByYMD(2026, 2, 18).StartOfMonth().Build()
	wantStart := time.Date(2026, 2, 1, 0, 0, 0, 0, time.Local)
	if !start.Equal(wantStart) {
		t.Fatalf("StartOfMonth mismatch, want=%v got=%v", wantStart, start)
	}

	end := DateByYMD(2026, 2, 18).EndOfMonth().Build()
	wantEnd := time.Date(2026, 2, 28, 23, 59, 59, 0, time.Local)
	if !end.Equal(wantEnd) {
		t.Fatalf("EndOfMonth mismatch, want=%v got=%v", wantEnd, end)
	}
}

func TestUTCDateHelpers(t *testing.T) {
	nowUTC := time.Now().UTC()
	todayUTC := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), 0, 0, 0, 0, time.UTC)

	y1 := YearsLaterUTC(1)
	if !y1.Equal(todayUTC.AddDate(1, 0, 0)) {
		t.Fatalf("YearsLaterUTC mismatch, want=%v got=%v", todayUTC.AddDate(1, 0, 0), y1)
	}
	if y1.Location() != time.UTC {
		t.Fatalf("YearsLaterUTC should be UTC, got %v", y1.Location())
	}

	m2 := MonthsLaterUTC(2)
	if !m2.Equal(todayUTC.AddDate(0, 2, 0)) {
		t.Fatalf("MonthsLaterUTC mismatch, want=%v got=%v", todayUTC.AddDate(0, 2, 0), m2)
	}

	d10 := DaysLaterUTC(10)
	if !d10.Equal(todayUTC.AddDate(0, 0, 10)) {
		t.Fatalf("DaysLaterUTC mismatch, want=%v got=%v", todayUTC.AddDate(0, 0, 10), d10)
	}

	oneYear := OneYearLaterUTC()
	if !oneYear.Equal(todayUTC.AddDate(1, 0, 0)) {
		t.Fatalf("OneYearLaterUTC mismatch, want=%v got=%v", todayUTC.AddDate(1, 0, 0), oneYear)
	}
}

func TestDateBuilderStartAndEndOfQuarter(t *testing.T) {
	start := DateByYMD(2026, 5, 20).StartOfQuarter().Build()
	wantStart := time.Date(2026, 4, 1, 0, 0, 0, 0, time.Local)
	if !start.Equal(wantStart) {
		t.Fatalf("StartOfQuarter mismatch, want=%v got=%v", wantStart, start)
	}

	end := DateByYMD(2026, 5, 20).EndOfQuarter().Build()
	wantEnd := time.Date(2026, 6, 30, 23, 59, 59, 0, time.Local)
	if !end.Equal(wantEnd) {
		t.Fatalf("EndOfQuarter mismatch, want=%v got=%v", wantEnd, end)
	}
}

func TestNextBillingDateFrom(t *testing.T) {
	loc := time.UTC

	currentMonth := nextBillingDateFrom(time.Date(2026, 3, 10, 8, 0, 0, 0, loc), 15)
	wantCurrent := time.Date(2026, 3, 15, 0, 0, 0, 0, loc)
	if !currentMonth.Equal(wantCurrent) {
		t.Fatalf("NextBillingDate current month mismatch, want=%v got=%v", wantCurrent, currentMonth)
	}

	nextMonth := nextBillingDateFrom(time.Date(2026, 3, 20, 8, 0, 0, 0, loc), 15)
	wantNext := time.Date(2026, 4, 15, 0, 0, 0, 0, loc)
	if !nextMonth.Equal(wantNext) {
		t.Fatalf("NextBillingDate next month mismatch, want=%v got=%v", wantNext, nextMonth)
	}

	clampedDay := nextBillingDateFrom(time.Date(2026, 2, 20, 8, 0, 0, 0, loc), 31)
	wantClamped := time.Date(2026, 2, 28, 0, 0, 0, 0, loc)
	if !clampedDay.Equal(wantClamped) {
		t.Fatalf("NextBillingDate clamp mismatch, want=%v got=%v", wantClamped, clampedDay)
	}
}

func TestSLADeadlineFrom(t *testing.T) {
	start := time.Date(2026, 3, 13, 16, 0, 0, 0, time.UTC) // Friday

	natural := slaDeadlineFrom(start, 10, false)
	wantNatural := time.Date(2026, 3, 14, 2, 0, 0, 0, time.UTC) // Saturday
	if !natural.Equal(wantNatural) {
		t.Fatalf("SLADeadline natural mismatch, want=%v got=%v", wantNatural, natural)
	}

	business := slaDeadlineFrom(start, 10, true)
	wantBusiness := time.Date(2026, 3, 16, 2, 0, 0, 0, time.UTC) // Monday
	if !business.Equal(wantBusiness) {
		t.Fatalf("SLADeadline business mismatch, want=%v got=%v", wantBusiness, business)
	}
}
