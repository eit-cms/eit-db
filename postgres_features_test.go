package db

import (
	"strings"
	"testing"
)

// ==================== GetPostgreSQLFeatures 类型断言 ====================

func TestGetPostgreSQLFeatures_TypeAssertion(t *testing.T) {
	// PostgreSQL adapter → 成功
	pg := &PostgreSQLAdapter{}
	features, ok := GetPostgreSQLFeatures(pg)
	if !ok {
		t.Fatal("GetPostgreSQLFeatures should return ok=true for *PostgreSQLAdapter")
	}
	if features == nil {
		t.Fatal("GetPostgreSQLFeatures should not return nil")
	}

	// 非 PostgreSQL → false
	ss := &SQLServerAdapter{}
	_, ok = GetPostgreSQLFeatures(ss)
	if ok {
		t.Fatal("GetPostgreSQLFeatures should return ok=false for non-PostgreSQL adapter")
	}
}

// ==================== EnumTypeBuilder ====================

func TestEnumTypeBuilder_Build_Basic(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.EnumType("order_status").
		Values("pending", "processing", "shipped", "delivered", "cancelled").
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if !strings.Contains(ddl, `CREATE TYPE "order_status" AS ENUM`) {
		t.Errorf("expected CREATE TYPE ... AS ENUM, got:\n%s", ddl)
	}
	for _, v := range []string{"pending", "processing", "shipped", "delivered", "cancelled"} {
		if !strings.Contains(ddl, "'"+v+"'") {
			t.Errorf("expected value %q in DDL, got:\n%s", v, ddl)
		}
	}
	t.Logf("Generated DDL:\n%s", ddl)
}

func TestEnumTypeBuilder_Build_WithSchema(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.EnumType("mood").
		Schema("myapp").
		Values("happy", "sad", "neutral").
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if !strings.Contains(ddl, `"myapp"."mood"`) {
		t.Errorf("expected schema-qualified type name, got:\n%s", ddl)
	}
	t.Logf("Generated DDL:\n%s", ddl)
}

func TestEnumTypeBuilder_Build_QuotesSpecialChars(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	// 枚举值含单引号需要转义
	ddl, err := features.EnumType("q_type").
		Values("it's fine", "ok").
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if !strings.Contains(ddl, `'it''s fine'`) {
		t.Errorf("expected single-quote escaped as '', got:\n%s", ddl)
	}
}

func TestEnumTypeBuilder_Validate_EmptyName(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})
	_, err := features.EnumType("").Values("a").Build()
	if err == nil {
		t.Fatal("expected error for empty type name")
	}
}

func TestEnumTypeBuilder_Validate_NoValues(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})
	_, err := features.EnumType("empty_enum").Build()
	if err == nil {
		t.Fatal("expected error when no values provided")
	}
	if !strings.Contains(err.Error(), "value") {
		t.Errorf("expected 'value' in error, got: %v", err)
	}
}

// ==================== DomainTypeBuilder ====================

func TestDomainTypeBuilder_Build_Basic(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.DomainType("positive_int").
		BaseType("INTEGER").
		NotNull().
		Check("VALUE > 0").
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if !strings.Contains(ddl, `CREATE DOMAIN "positive_int" AS INTEGER`) {
		t.Errorf("expected CREATE DOMAIN ... AS INTEGER, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "NOT NULL") {
		t.Errorf("expected NOT NULL, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "CHECK (VALUE > 0)") {
		t.Errorf("expected CHECK clause, got:\n%s", ddl)
	}
	t.Logf("Generated DDL:\n%s", ddl)
}

func TestDomainTypeBuilder_Build_EmailDomain(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.DomainType("email_address").
		BaseType("TEXT").
		NotNull().
		Check(`VALUE ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$'`).
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if !strings.Contains(ddl, `"email_address"`) {
		t.Errorf("expected type name in DDL, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "CHECK") {
		t.Errorf("expected CHECK clause, got:\n%s", ddl)
	}
	t.Logf("Generated DDL:\n%s", ddl)
}

func TestDomainTypeBuilder_Build_WithNamedConstraint(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.DomainType("score").
		BaseType("NUMERIC(5,2)").
		CheckNamed("score_range", "VALUE BETWEEN 0 AND 100").
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if !strings.Contains(ddl, `CONSTRAINT "score_range"`) {
		t.Errorf("expected named constraint, got:\n%s", ddl)
	}
	t.Logf("Generated DDL:\n%s", ddl)
}

func TestDomainTypeBuilder_Build_WithDefault(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.DomainType("flag").
		BaseType("BOOLEAN").
		Default("FALSE").
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if !strings.Contains(ddl, "DEFAULT FALSE") {
		t.Errorf("expected DEFAULT clause, got:\n%s", ddl)
	}
}

func TestDomainTypeBuilder_Validate_MissingBaseType(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})
	_, err := features.DomainType("no_base").Build()
	if err == nil {
		t.Fatal("expected error for missing base type")
	}
	if !strings.Contains(err.Error(), "base type") {
		t.Errorf("expected 'base type' in error, got: %v", err)
	}
}

func TestDomainTypeBuilder_Build_MultipleChecks(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.DomainType("zip_code").
		BaseType("VARCHAR(10)").
		NotNull().
		Check("char_length(VALUE) >= 5").
		Check(`VALUE ~ '^\d'`).
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	// 两个 CHECK 都应出现
	if strings.Count(ddl, "CHECK") != 2 {
		t.Errorf("expected 2 CHECK clauses, got:\n%s", ddl)
	}
	t.Logf("Generated DDL:\n%s", ddl)
}

// ==================== CompositeTypeBuilder ====================

func TestCompositeTypeBuilder_Build_Basic(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.CompositeType("address").
		Field("street", "TEXT").
		Field("city", "VARCHAR(100)").
		Field("zip", "CHAR(6)").
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if !strings.Contains(ddl, `CREATE TYPE "address" AS`) {
		t.Errorf("expected CREATE TYPE ... AS, got:\n%s", ddl)
	}
	for _, field := range []string{"street", "city", "zip"} {
		if !strings.Contains(ddl, `"`+field+`"`) {
			t.Errorf("expected field %q in DDL, got:\n%s", field, ddl)
		}
	}
	t.Logf("Generated DDL:\n%s", ddl)
}

func TestCompositeTypeBuilder_Build_GeoPoint(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.CompositeType("geo_point").
		Field("lat", "DOUBLE PRECISION").
		Field("lng", "DOUBLE PRECISION").
		Field("altitude", "REAL").
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if !strings.Contains(ddl, "DOUBLE PRECISION") {
		t.Errorf("expected DOUBLE PRECISION type in DDL, got:\n%s", ddl)
	}
	t.Logf("Generated DDL:\n%s", ddl)
}

func TestCompositeTypeBuilder_Build_WithSchema(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.CompositeType("money_amount").
		Schema("finance").
		Field("amount", "NUMERIC(15,4)").
		Field("currency", "CHAR(3)").
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if !strings.Contains(ddl, `"finance"."money_amount"`) {
		t.Errorf("expected schema-qualified name, got:\n%s", ddl)
	}
	t.Logf("Generated DDL:\n%s", ddl)
}

func TestCompositeTypeBuilder_Validate_NoFields(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})
	_, err := features.CompositeType("empty").Build()
	if err == nil {
		t.Fatal("expected error when no fields defined")
	}
	if !strings.Contains(err.Error(), "field") {
		t.Errorf("expected 'field' in error, got: %v", err)
	}
}

// ==================== quotePgIdentifier ====================

func TestQuotePgIdentifier(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"employees", `"employees"`},
		{"order_status", `"order_status"`},
		{`has"quote`, `"has""quote"`}, // " 转义为 ""
		{"SELECT", `"SELECT"`},        // 保留字
		{"public", `"public"`},
	}
	for _, c := range cases {
		got := quotePgIdentifier(c.input)
		if got != c.expected {
			t.Errorf("quotePgIdentifier(%q): expected %q, got %q", c.input, c.expected, got)
		}
	}
}

// ==================== PostgreSQLViewBuilder ====================

func TestPostgreSQLViewBuilder_BuildCreate_View(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.View("active_users").
		Schema("public").
		As("SELECT id, name FROM users WHERE active = true").
		WithCheckOption().
		BuildCreate()

	if err != nil {
		t.Fatalf("BuildCreate() error: %v", err)
	}
	if !strings.Contains(ddl, `CREATE OR REPLACE VIEW "public"."active_users" AS`) {
		t.Errorf("expected CREATE OR REPLACE VIEW, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "WITH CHECK OPTION") {
		t.Errorf("expected WITH CHECK OPTION, got:\n%s", ddl)
	}
}

func TestPostgreSQLViewBuilder_BuildCreate_MaterializedView(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	ddl, err := features.MaterializedView("mv_user_stats").
		Schema("analytics").
		As("SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id").
		WithNoData().
		BuildCreate()

	if err != nil {
		t.Fatalf("BuildCreate() error: %v", err)
	}
	if !strings.Contains(ddl, `CREATE MATERIALIZED VIEW "analytics"."mv_user_stats" AS`) {
		t.Errorf("expected CREATE MATERIALIZED VIEW, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "WITH NO DATA") {
		t.Errorf("expected WITH NO DATA, got:\n%s", ddl)
	}
}

func TestPostgreSQLViewBuilder_BuildDropAndRefresh(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	dropDDL, err := features.View("v_users").Schema("public").BuildDrop()
	if err != nil {
		t.Fatalf("BuildDrop() error: %v", err)
	}
	if dropDDL != `DROP VIEW IF EXISTS "public"."v_users";` {
		t.Errorf("unexpected drop ddl: %s", dropDDL)
	}

	refreshDDL, err := features.MaterializedView("mv_users").Schema("public").BuildRefresh()
	if err != nil {
		t.Fatalf("BuildRefresh() error: %v", err)
	}
	if refreshDDL != `REFRESH MATERIALIZED VIEW "public"."mv_users";` {
		t.Errorf("unexpected refresh ddl: %s", refreshDDL)
	}

	refreshConcurrentlyDDL, err := features.MaterializedView("mv_users").Schema("public").RefreshConcurrently().BuildRefresh()
	if err != nil {
		t.Fatalf("BuildRefresh() concurrently error: %v", err)
	}
	if refreshConcurrentlyDDL != `REFRESH MATERIALIZED VIEW CONCURRENTLY "public"."mv_users";` {
		t.Errorf("unexpected concurrent refresh ddl: %s", refreshConcurrentlyDDL)
	}
}

func TestPostgreSQLViewBuilder_Validate(t *testing.T) {
	features, _ := GetPostgreSQLFeatures(&PostgreSQLAdapter{})

	_, err := features.View("").As("SELECT 1").BuildCreate()
	if err == nil {
		t.Fatal("expected error for empty view name")
	}

	_, err = features.View("v_x").As("").BuildCreate()
	if err == nil {
		t.Fatal("expected error for empty select SQL")
	}

	_, err = features.MaterializedView("mv_x").CreateOrReplace().As("SELECT 1").BuildCreate()
	if err == nil {
		t.Fatal("expected error for materialized view create or replace")
	}

	_, err = features.View("v_y").BuildRefresh()
	if err == nil {
		t.Fatal("expected error for refreshing non-materialized view")
	}
}
