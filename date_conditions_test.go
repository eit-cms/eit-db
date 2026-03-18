package db

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDateOnConditionBuild(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("created_at", TypeTime).Build())

	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(DateOn("created_at", 2026, 3, 17))

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(sql, "WHERE") || !strings.Contains(sql, ">=") || !strings.Contains(sql, "<") {
		t.Fatalf("expected range comparison SQL, got: %s", sql)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}

	loc := time.Local
	wantStart := time.Date(2026, 3, 17, 0, 0, 0, 0, loc)
	wantEnd := time.Date(2026, 3, 18, 0, 0, 0, 0, loc)
	if !args[0].(time.Time).Equal(wantStart) {
		t.Fatalf("unexpected start arg, want=%v got=%v", wantStart, args[0])
	}
	if !args[1].(time.Time).Equal(wantEnd) {
		t.Fatalf("unexpected end arg, want=%v got=%v", wantEnd, args[1])
	}
}

func TestDateRangeConditionNormalize(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("updated_at", TypeTime).Build())

	start := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(DateRange("updated_at", start, end))

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(sql, "BETWEEN") {
		t.Fatalf("expected BETWEEN in SQL, got: %s", sql)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
	if !args[0].(time.Time).Equal(end) || !args[1].(time.Time).Equal(start) {
		t.Fatalf("expected normalized args [%v, %v], got %v", end, start, args)
	}
}

func TestDateFromAndDateToBuild(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("updated_at", TypeTime).Build())

	from := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 31, 23, 59, 59, 0, time.UTC)

	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.WhereAll(DateFrom("updated_at", from), DateTo("updated_at", to))

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(sql, ">=") || !strings.Contains(sql, "<=") {
		t.Fatalf("expected >= and <= operators, got: %s", sql)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
}

func TestDateInCurrentMonthBuild(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("created_at", TypeTime).Build())

	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(DateInCurrentMonth("created_at"))

	_, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}

	now := time.Now()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	nextMonthStart := monthStart.AddDate(0, 1, 0)
	if !args[0].(time.Time).Equal(monthStart) {
		t.Fatalf("unexpected month start arg, want=%v got=%v", monthStart, args[0])
	}
	if !args[1].(time.Time).Equal(nextMonthStart) {
		t.Fatalf("unexpected next month start arg, want=%v got=%v", nextMonthStart, args[1])
	}
}

func TestDateInCurrentQuarterBuild(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("created_at", TypeTime).Build())

	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(DateInCurrentQuarter("created_at"))

	_, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}

	now := time.Now()
	quarterStartMonth := ((int(now.Month())-1)/3)*3 + 1
	quarterStart := time.Date(now.Year(), time.Month(quarterStartMonth), 1, 0, 0, 0, 0, now.Location())
	nextQuarterStart := quarterStart.AddDate(0, 3, 0)
	if !args[0].(time.Time).Equal(quarterStart) {
		t.Fatalf("unexpected quarter start arg, want=%v got=%v", quarterStart, args[0])
	}
	if !args[1].(time.Time).Equal(nextQuarterStart) {
		t.Fatalf("unexpected next quarter start arg, want=%v got=%v", nextQuarterStart, args[1])
	}
}

func TestDateInBusinessHoursBuild(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("created_at", TypeTime).Build())

	base := time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC)
	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(dateInBusinessHoursFrom("created_at", base, 9, 18))

	_, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}

	wantStart := time.Date(2026, 3, 17, 9, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 3, 17, 18, 0, 0, 0, time.UTC)
	if !args[0].(time.Time).Equal(wantStart) {
		t.Fatalf("unexpected business-hour start, want=%v got=%v", wantStart, args[0])
	}
	if !args[1].(time.Time).Equal(wantEnd) {
		t.Fatalf("unexpected business-hour end, want=%v got=%v", wantEnd, args[1])
	}
}

func TestDateInBusinessHoursOvernightBuild(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("created_at", TypeTime).Build())

	base := time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC)
	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(dateInBusinessHoursFrom("created_at", base, 22, 6))

	_, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}

	wantStart := time.Date(2026, 3, 17, 22, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 3, 18, 6, 0, 0, 0, time.UTC)
	if !args[0].(time.Time).Equal(wantStart) {
		t.Fatalf("unexpected overnight start, want=%v got=%v", wantStart, args[0])
	}
	if !args[1].(time.Time).Equal(wantEnd) {
		t.Fatalf("unexpected overnight end, want=%v got=%v", wantEnd, args[1])
	}
}

func TestDateInLastBusinessDaysBuild(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("created_at", TypeTime).Build())

	// 2026-03-17 是周二；最近 3 个工作日应为 Tue/Mon/Fri。
	base := time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC)
	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(dateInLastBusinessDaysFrom("created_at", base, 3))

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(sql, " OR ") {
		t.Fatalf("expected OR-combined windows, got: %s", sql)
	}
	if len(args) != 6 {
		t.Fatalf("expected 6 args (3 windows), got %d", len(args))
	}

	dayTue := time.Date(2026, 3, 17, 0, 0, 0, 0, time.UTC)
	dayMon := time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC)
	dayFri := time.Date(2026, 3, 13, 0, 0, 0, 0, time.UTC)

	if !args[0].(time.Time).Equal(dayTue) || !args[2].(time.Time).Equal(dayMon) || !args[4].(time.Time).Equal(dayFri) {
		t.Fatalf("unexpected business day windows starts: %v", args)
	}
}

func TestDateInLastBusinessDaysWithHoliday(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("created_at", TypeTime).Build())

	base := time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC) // Tue
	holiday := time.Date(2026, 3, 17, 0, 0, 0, 0, time.UTC)
	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(dateInLastBusinessDaysFrom("created_at", base, 1, holiday))

	_, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}

	want := time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC) // Monday
	if !args[0].(time.Time).Equal(want) {
		t.Fatalf("expected holiday to be skipped, want=%v got=%v", want, args[0])
	}
}
