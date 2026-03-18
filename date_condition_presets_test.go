package db

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestActiveUsersInBusinessHoursBuild(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("status", TypeString).Build())
	schema.AddField(NewField("updated_at", TypeTime).Build())

	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(ActiveUsersInBusinessHours())

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(sql, "status") || !strings.Contains(sql, "updated_at") {
		t.Fatalf("expected status and updated_at in SQL, got: %s", sql)
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(args))
	}
	if args[0] != "active" {
		t.Fatalf("expected first arg active, got %v", args[0])
	}
}

func TestOrdersInCurrentQuarterBuild(t *testing.T) {
	schema := NewBaseSchema("orders")
	schema.AddField(NewField("created_at", TypeTime).Build())

	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(OrdersInCurrentQuarter())

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(sql, "created_at") {
		t.Fatalf("expected created_at in SQL, got: %s", sql)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
}

func TestDueSoonTasksInBusinessDaysFrom(t *testing.T) {
	schema := NewBaseSchema("tasks")
	schema.AddField(NewField("status", TypeString).Build())
	schema.AddField(NewField("due_at", TypeTime).Build())

	base := time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC)   // Tue
	holiday := time.Date(2026, 3, 18, 0, 0, 0, 0, time.UTC) // Wed holiday
	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(dueSoonTasksInBusinessDaysFrom(base, 2, holiday))

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(sql, "due_at") || !strings.Contains(sql, "status") {
		t.Fatalf("expected due_at/status in SQL, got: %s", sql)
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(args))
	}
	if args[0] != "pending" {
		t.Fatalf("expected pending status arg, got %v", args[0])
	}
	if !args[1].(time.Time).Equal(base) {
		t.Fatalf("expected lower bound equals now, want=%v got=%v", base, args[1])
	}

	// Tue + 2 个工作日，且 Wed 为节假日，应落到 Fri 的 23:59:59
	wantDeadline := time.Date(2026, 3, 20, 23, 59, 59, 0, time.UTC)
	if !args[2].(time.Time).Equal(wantDeadline) {
		t.Fatalf("unexpected deadline, want=%v got=%v", wantDeadline, args[2])
	}
}

func TestPresetsDateEntryActiveUsers(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(NewField("status", TypeString).Build())
	schema.AddField(NewField("updated_at", TypeTime).Build())

	qc := NewSQLQueryConstructor(schema, NewMySQLDialect())
	qc.Where(Presets.Date.ActiveUsersInBusinessHours())

	sql, args, err := qc.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(sql, "status") || !strings.Contains(sql, "updated_at") {
		t.Fatalf("expected status/updated_at in SQL, got: %s", sql)
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(args))
	}
}

func TestPresetsDateEntryOrdersAndTasks(t *testing.T) {
	orders := NewBaseSchema("orders")
	orders.AddField(NewField("created_at", TypeTime).Build())

	orderQC := NewSQLQueryConstructor(orders, NewMySQLDialect())
	orderQC.Where(Presets.Date.OrdersInCurrentQuarter())
	_, orderArgs, err := orderQC.Build(context.Background())
	if err != nil {
		t.Fatalf("orders Build failed: %v", err)
	}
	if len(orderArgs) != 2 {
		t.Fatalf("expected 2 order args, got %d", len(orderArgs))
	}

	tasks := NewBaseSchema("tasks")
	tasks.AddField(NewField("status", TypeString).Build())
	tasks.AddField(NewField("due_at", TypeTime).Build())

	taskQC := NewSQLQueryConstructor(tasks, NewMySQLDialect())
	taskQC.Where(Presets.Date.DueSoonTasksInBusinessDays(2))
	_, taskArgs, err := taskQC.Build(context.Background())
	if err != nil {
		t.Fatalf("tasks Build failed: %v", err)
	}
	if len(taskArgs) != 3 {
		t.Fatalf("expected 3 task args, got %d", len(taskArgs))
	}
}
