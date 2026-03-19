package db

import (
	"strings"
	"testing"
)

func TestBuildCreateTableSQL_DefaultStringIsQuotedAcrossDialects(t *testing.T) {
	testCases := []struct {
		name    string
		adapter Adapter
		expect  string
	}{
		{
			name:    "postgres",
			adapter: &PostgreSQLAdapter{},
			expect:  `"status" VARCHAR(255) NOT NULL DEFAULT 'active'`,
		},
		{
			name:    "mysql",
			adapter: &MySQLAdapter{},
			expect:  "`status` VARCHAR(255) NOT NULL DEFAULT 'active'",
		},
		{
			name:    "sqlite",
			adapter: &SQLiteAdapter{},
			expect:  "`status` TEXT NOT NULL DEFAULT 'active'",
		},
		{
			name:    "sqlserver",
			adapter: &SQLServerAdapter{},
			expect:  "[status] NVARCHAR(255) NOT NULL DEFAULT 'active'",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &Repository{adapter: tc.adapter}
			schema := NewBaseSchema("users")
			schema.AddField(&Field{Name: "status", Type: TypeString, Null: false, Default: "active"})

			sql := buildCreateTableSQL(repo, schema)
			if !strings.Contains(sql, tc.expect) {
				t.Fatalf("expected quoted string default for %s, got:\n%s", tc.name, sql)
			}
		})
	}
}

func TestBuildCreateTableSQL_DefaultExpressionPreserved(t *testing.T) {
	testCases := []struct {
		name    string
		adapter Adapter
		expect  string
	}{
		{
			name:    "postgres",
			adapter: &PostgreSQLAdapter{},
			expect:  `"created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP`,
		},
		{
			name:    "mysql",
			adapter: &MySQLAdapter{},
			expect:  "`created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
		},
		{
			name:    "sqlite",
			adapter: &SQLiteAdapter{},
			expect:  "`created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
		},
		{
			name:    "sqlserver",
			adapter: &SQLServerAdapter{},
			expect:  "[created_at] DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &Repository{adapter: tc.adapter}
			schema := NewBaseSchema("events")
			schema.AddField(&Field{Name: "created_at", Type: TypeTime, Null: false, Default: "CURRENT_TIMESTAMP"})

			sql := buildCreateTableSQL(repo, schema)
			if !strings.Contains(sql, tc.expect) {
				t.Fatalf("expected expression default preserved for %s, got:\n%s", tc.name, sql)
			}
		})
	}
}

func TestBuildCreateTableSQL_SQLServerBoolDefaultUsesBitLiteral(t *testing.T) {
	repo := &Repository{adapter: &SQLServerAdapter{}}
	schema := NewBaseSchema("flags")
	schema.AddField(&Field{Name: "enabled", Type: TypeBoolean, Null: false, Default: true})

	sql := buildCreateTableSQL(repo, schema)
	if !strings.Contains(sql, "[enabled] BIT NOT NULL DEFAULT 1") {
		t.Fatalf("expected SQL Server boolean default to use bit literal, got:\n%s", sql)
	}
}

func TestBuildCreateTableSQL_PreQuotedStringDefaultCompatible(t *testing.T) {
	repo := &Repository{adapter: &PostgreSQLAdapter{}}
	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "nickname", Type: TypeString, Null: false, Default: "'guest'"})

	sql := buildCreateTableSQL(repo, schema)
	if !strings.Contains(sql, `"nickname" VARCHAR(255) NOT NULL DEFAULT 'guest'`) {
		t.Fatalf("expected pre-quoted default to remain compatible, got:\n%s", sql)
	}
}

func TestBuildCreateTableSQL_StringDefaultWithCommaAndParensIsQuoted(t *testing.T) {
	repo := &Repository{adapter: &PostgreSQLAdapter{}}
	schema := NewBaseSchema("places")
	schema.AddField(&Field{Name: "label", Type: TypeString, Null: false, Default: "POINT(1,2)"})
	schema.AddField(&Field{Name: "code", Type: TypeString, Null: false, Default: "a,b"})

	sql := buildCreateTableSQL(repo, schema)
	if !strings.Contains(sql, `"label" VARCHAR(255) NOT NULL DEFAULT 'POINT(1,2)'`) {
		t.Fatalf("expected string default with parentheses/comma to be quoted, got:\n%s", sql)
	}
	if !strings.Contains(sql, `"code" VARCHAR(255) NOT NULL DEFAULT 'a,b'`) {
		t.Fatalf("expected string default with comma to be quoted, got:\n%s", sql)
	}
}
