package db

import (
	"strings"
	"testing"
)

func TestGetViewFeatures_TypeAssertion(t *testing.T) {
	cases := []struct {
		name    string
		adapter Adapter
		ok      bool
	}{
		{name: "postgres", adapter: &PostgreSQLAdapter{}, ok: true},
		{name: "sqlserver", adapter: &SQLServerAdapter{}, ok: true},
		{name: "mysql", adapter: &MySQLAdapter{}, ok: true},
		{name: "sqlite", adapter: &SQLiteAdapter{}, ok: true},
		{name: "mongodb", adapter: &MongoAdapter{}, ok: false},
	}

	for _, tt := range cases {
		_, ok := GetViewFeatures(tt.adapter)
		if ok != tt.ok {
			t.Fatalf("%s: GetViewFeatures ok=%v, expected %v", tt.name, ok, tt.ok)
		}
	}
}

func TestViewBuilder_BuildCreate_SQLServer(t *testing.T) {
	vf, _ := GetViewFeatures(&SQLServerAdapter{})
	sql, err := vf.View("dbo.v_users").As("SELECT id, name FROM [dbo].[users]").BuildCreate()
	if err != nil {
		t.Fatalf("BuildCreate() error: %v", err)
	}
	if !strings.Contains(sql, "CREATE OR ALTER VIEW [dbo].[v_users] AS") {
		t.Fatalf("unexpected sql: %s", sql)
	}
}

func TestViewBuilder_BuildCreate_PostgresReplace(t *testing.T) {
	vf, _ := GetViewFeatures(&PostgreSQLAdapter{})
	sql, err := vf.View("public.v_users").As("SELECT id, name FROM users").BuildCreate()
	if err != nil {
		t.Fatalf("BuildCreate() error: %v", err)
	}
	if !strings.Contains(sql, "CREATE OR REPLACE VIEW \"public\".\"v_users\" AS") {
		t.Fatalf("unexpected sql: %s", sql)
	}
}

func TestViewBuilder_BuildCreate_SQLiteCreateOnly(t *testing.T) {
	vf, _ := GetViewFeatures(&SQLiteAdapter{})
	sql, err := vf.View("v_users").As("SELECT id FROM users").BuildCreate()
	if err != nil {
		t.Fatalf("BuildCreate() error: %v", err)
	}
	if !strings.Contains(sql, "CREATE VIEW \"v_users\" AS") {
		t.Fatalf("unexpected sql: %s", sql)
	}
}

func TestViewBuilder_BuildCreate_MaterializedPostgres(t *testing.T) {
	vf, _ := GetViewFeatures(&PostgreSQLAdapter{})
	sql, err := vf.View("public.mv_users").Materialized().As("SELECT id FROM users").BuildCreate()
	if err != nil {
		t.Fatalf("BuildCreate() error: %v", err)
	}
	if !strings.Contains(sql, "CREATE MATERIALIZED VIEW \"public\".\"mv_users\" AS") {
		t.Fatalf("unexpected sql: %s", sql)
	}
}

func TestViewBuilder_BuildDrop_MaterializedPostgres(t *testing.T) {
	vf, _ := GetViewFeatures(&PostgreSQLAdapter{})
	sql, err := vf.View("public.mv_users").Materialized().BuildDrop()
	if err != nil {
		t.Fatalf("BuildDrop() error: %v", err)
	}
	if sql != "DROP MATERIALIZED VIEW IF EXISTS \"public\".\"mv_users\";" {
		t.Fatalf("unexpected drop sql: %s", sql)
	}
}

func TestViewBuilder_Validate_UnsupportedMaterialized(t *testing.T) {
	vf, _ := GetViewFeatures(&SQLServerAdapter{})
	_, err := vf.View("dbo.mv_users").Materialized().As("SELECT id FROM [dbo].[users]").BuildCreate()
	if err == nil {
		t.Fatal("expected error for unsupported materialized view")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "materialized view") {
		t.Fatalf("unexpected error: %v", err)
	}
}
