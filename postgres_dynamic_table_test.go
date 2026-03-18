package db

import (
	"strings"
	"testing"
)

func TestPostgresGenerateCreateTableSQLUsesSchemaBuilderColumns(t *testing.T) {
	hook := NewPostgreSQLDynamicTableHook(&PostgreSQLAdapter{})
	config := NewDynamicTableConfig("events").
		AddField(NewDynamicTableField("id", TypeInteger).AsPrimaryKey().WithAutoinc()).
		AddField(NewDynamicTableField("name", TypeString).AsNotNull().WithDefault("'guest'"))

	sql := hook.generateCreateTableSQL(config, "v_table_name")

	if !strings.Contains(sql, `CREATE TABLE " || v_table_name || " (`) {
		t.Fatalf("expected dynamic table name interpolation, got: %s", sql)
	}
	if !strings.Contains(sql, `"id" SERIAL PRIMARY KEY`) {
		t.Fatalf("expected id to use SERIAL PRIMARY KEY, got: %s", sql)
	}
	if !strings.Contains(sql, `"name" VARCHAR(255) NOT NULL DEFAULT 'guest'`) {
		t.Fatalf("expected name column default and not-null constraints, got: %s", sql)
	}
}
