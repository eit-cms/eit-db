package db

import (
	"strings"
	"testing"
)

func TestSQLServerGenerateTSQLProcedureContainsCreateTable(t *testing.T) {
	hook := NewSQLServerDynamicTableHook(&SQLServerAdapter{})
	config := NewDynamicTableConfig("events").
		AddField(NewDynamicTableField("id", TypeInteger).AsPrimaryKey().WithAutoinc()).
		AddField(NewDynamicTableField("name", TypeString).AsNotNull().WithDefault("'guest'"))

	sql := hook.generateTSQLProcedure(config)

	if !strings.Contains(sql, "CREATE OR ALTER PROCEDURE") {
		t.Fatalf("expected procedure DDL, got: %s", sql)
	}
	if !strings.Contains(sql, "CREATE TABLE [dbo].") {
		t.Fatalf("expected dynamic CREATE TABLE, got: %s", sql)
	}
	if !strings.Contains(sql, "[id] INT IDENTITY(1,1) PRIMARY KEY") {
		t.Fatalf("expected sqlserver identity primary key column, got: %s", sql)
	}
	if !strings.Contains(sql, "[name] NVARCHAR(255) NOT NULL DEFAULT 'guest'") {
		t.Fatalf("expected name column default and not-null constraints, got: %s", sql)
	}
}

func TestSQLServerGenerateTSQLTriggerContainsInsertedCursor(t *testing.T) {
	hook := NewSQLServerDynamicTableHook(&SQLServerAdapter{})
	config := NewDynamicTableConfig("events").
		WithParentTable("projects", "status = 'active'")

	sql := hook.generateTSQLTrigger(config)

	if !strings.Contains(sql, "CREATE OR ALTER TRIGGER") {
		t.Fatalf("expected trigger ddl, got: %s", sql)
	}
	if !strings.Contains(sql, "FROM inserted i") {
		t.Fatalf("expected inserted pseudo-table usage, got: %s", sql)
	}
	if !strings.Contains(sql, "WHERE i.status = 'active'") {
		t.Fatalf("expected trigger condition aliasing, got: %s", sql)
	}
	if !strings.Contains(sql, "EXEC [sp_create_events_table] @entity_id = @id") {
		t.Fatalf("expected procedure execution in trigger, got: %s", sql)
	}
}
