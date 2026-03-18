package db

import (
	"strings"
	"testing"
)

// TestGetSQLServerFeatures_TypeAssertion 验证 GetSQLServerFeatures 的类型断言行为
func TestGetSQLServerFeatures_TypeAssertion(t *testing.T) {
	// SQL Server adapter → 应该成功
	ssAdapter := &SQLServerAdapter{}
	features, ok := GetSQLServerFeatures(ssAdapter)
	if !ok {
		t.Fatal("GetSQLServerFeatures should return ok=true for *SQLServerAdapter")
	}
	if features == nil {
		t.Fatal("GetSQLServerFeatures should not return nil features")
	}

	// 非 SQL Server adapter → 应该返回 false
	pgAdapter := &PostgreSQLAdapter{}
	_, ok = GetSQLServerFeatures(pgAdapter)
	if ok {
		t.Fatal("GetSQLServerFeatures should return ok=false for non-SQLServer adapter")
	}
}

// TestRecursiveQueryBuilder_Build_Basic 验证基本递归 CTE 的 T-SQL 生成
func TestRecursiveQueryBuilder_Build_Basic(t *testing.T) {
	hook := &SQLServerAdapter{}
	features, _ := GetSQLServerFeatures(hook)

	sql, err := features.RecursiveQuery("org_tree").
		Anchor("SELECT id, name, parent_id, 0 AS depth FROM departments WHERE parent_id IS NULL").
		Recursive("SELECT d.id, d.name, d.parent_id, t.depth + 1 FROM departments d INNER JOIN org_tree t ON d.parent_id = t.id").
		Select("id, name, parent_id, depth").
		MaxRecursion(50).
		Build()

	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}

	// 验证 CTE 名称被正确引用
	if !strings.Contains(sql, "[org_tree]") {
		t.Errorf("expected [org_tree] in SQL, got:\n%s", sql)
	}
	// 验证 UNION ALL 存在
	if !strings.Contains(sql, "UNION ALL") {
		t.Errorf("expected UNION ALL in SQL, got:\n%s", sql)
	}
	// 验证 anchor 存在
	if !strings.Contains(sql, "parent_id IS NULL") {
		t.Errorf("expected anchor member in SQL, got:\n%s", sql)
	}
	// 验证递归成员存在
	if !strings.Contains(sql, "JOIN org_tree t") {
		t.Errorf("expected recursive member referencing CTE in SQL, got:\n%s", sql)
	}
	// 验证 MAXRECURSION
	if !strings.Contains(sql, "MAXRECURSION 50") {
		t.Errorf("expected OPTION(MAXRECURSION 50) in SQL, got:\n%s", sql)
	}
	// 验证 SELECT 列
	if !strings.Contains(sql, "SELECT id, name, parent_id, depth") {
		t.Errorf("expected SELECT columns in SQL, got:\n%s", sql)
	}

	t.Logf("Generated T-SQL:\n%s", sql)
}

// TestRecursiveQueryBuilder_Build_WithColumns 验证显式列名声明
func TestRecursiveQueryBuilder_Build_WithColumns(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	sql, err := features.RecursiveQuery("cat_path").
		Columns("id", "name", "path", "level").
		Anchor("SELECT id, name, CAST(name AS NVARCHAR(MAX)) AS path, 0 FROM categories WHERE parent_id IS NULL").
		Recursive("SELECT c.id, c.name, t.path + N' > ' + c.name, t.level + 1 FROM categories c JOIN cat_path t ON c.parent_id = t.id").
		SelectAll().
		Build()

	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}
	// 验证列名声明
	if !strings.Contains(sql, "[cat_path] ([id], [name], [path], [level])") {
		t.Errorf("expected explicit column list in CTE header, got:\n%s", sql)
	}

	t.Logf("Generated T-SQL:\n%s", sql)
}

// TestRecursiveQueryBuilder_Build_WithWhereAndOrderBy 验证 WHERE 和 ORDER BY 子句
func TestRecursiveQueryBuilder_Build_WithWhereAndOrderBy(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	sql, err := features.RecursiveQuery("hier").
		Anchor("SELECT id, parent_id, 0 AS level FROM nodes WHERE parent_id IS NULL").
		Recursive("SELECT n.id, n.parent_id, t.level + 1 FROM nodes n JOIN hier t ON n.parent_id = t.id").
		SelectAll().
		Where("level <= @p1", 3).
		OrderBy("level ASC, id").
		MaxRecursion(100).
		Build()

	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}
	if !strings.Contains(sql, "WHERE level <= @p1") {
		t.Errorf("expected WHERE clause in final SELECT, got:\n%s", sql)
	}
	if !strings.Contains(sql, "ORDER BY level ASC, id") {
		t.Errorf("expected ORDER BY in SQL, got:\n%s", sql)
	}

	t.Logf("Generated T-SQL:\n%s", sql)
}

// TestRecursiveQueryBuilder_Build_MaxRecursion0 验证 MAXRECURSION 0（不限制）
func TestRecursiveQueryBuilder_Build_MaxRecursion0(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	sql, err := features.RecursiveQuery("infinite").
		Anchor("SELECT 1 AS n").
		Recursive("SELECT n + 1 FROM infinite WHERE n < 10").
		SelectAll().
		MaxRecursion(0).
		Build()

	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}
	if !strings.Contains(sql, "MAXRECURSION 0") {
		t.Errorf("expected MAXRECURSION 0 in SQL, got:\n%s", sql)
	}
}

// TestRecursiveQueryBuilder_Build_DefaultMaxRecursion 验证默认 MAXRECURSION 为 100
func TestRecursiveQueryBuilder_Build_DefaultMaxRecursion(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	sql, err := features.RecursiveQuery("t").
		Anchor("SELECT 1 AS n").
		Recursive("SELECT n + 1 FROM t WHERE n < 5").
		SelectAll().
		Build()

	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}
	if !strings.Contains(sql, "MAXRECURSION 100") {
		t.Errorf("expected default MAXRECURSION 100, got:\n%s", sql)
	}
}

// TestRecursiveQueryBuilder_Validate_MissingAnchor 验证缺少 anchor 时报错
func TestRecursiveQueryBuilder_Validate_MissingAnchor(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	_, err := features.RecursiveQuery("tree").
		Recursive("SELECT id FROM tree").
		SelectAll().
		Build()

	if err == nil {
		t.Fatal("expected error for missing anchor, got nil")
	}
	if !strings.Contains(err.Error(), "anchor") {
		t.Errorf("expected 'anchor' in error message, got: %v", err)
	}
}

// TestRecursiveQueryBuilder_Validate_MissingRecursive 验证缺少递归成员时报错
func TestRecursiveQueryBuilder_Validate_MissingRecursive(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	_, err := features.RecursiveQuery("tree").
		Anchor("SELECT id FROM tree WHERE parent_id IS NULL").
		SelectAll().
		Build()

	if err == nil {
		t.Fatal("expected error for missing recursive member, got nil")
	}
	if !strings.Contains(err.Error(), "recursive") {
		t.Errorf("expected 'recursive' in error message, got: %v", err)
	}
}

// TestRecursiveQueryBuilder_Validate_EmptyCTEName 验证空 CTE 名称时报错
func TestRecursiveQueryBuilder_Validate_EmptyCTEName(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	_, err := features.RecursiveQuery("").
		Anchor("SELECT 1").
		Recursive("SELECT 2").
		SelectAll().
		Build()

	if err == nil {
		t.Fatal("expected error for empty CTE name, got nil")
	}
}

// TestQuoteSQLServerIdentifier 验证标识符引用和转义
func TestQuoteSQLServerIdentifier(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"employees", "[employees]"},
		{"org_tree", "[org_tree]"},
		{"has]bracket", "[has]]bracket]"}, // ] 需要转义为 ]]
		{"SELECT", "[SELECT]"},            // 保留字
	}

	for _, c := range cases {
		got := quoteSQLServerIdentifier(c.input)
		if got != c.expected {
			t.Errorf("quoteSQLServerIdentifier(%q): expected %q, got %q", c.input, c.expected, got)
		}
	}
}

func TestSQLServerMergeBuilder_Build_BasicUpsert(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	sql, err := features.MergeInto("dbo.users").
		Using("(SELECT 1 AS id, N'Alice' AS name) AS s").
		On("t.[id] = s.[id]").
		WhenMatchedUpdate("t.[name] = s.[name]").
		WhenNotMatchedInsert([]string{"id", "name"}, "s.[id], s.[name]").
		Build()

	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}
	if !strings.Contains(sql, "MERGE INTO [dbo].[users]") {
		t.Errorf("expected MERGE target in SQL, got:\n%s", sql)
	}
	if !strings.Contains(sql, "USING (SELECT 1 AS id") {
		t.Errorf("expected USING source in SQL, got:\n%s", sql)
	}
	if !strings.Contains(sql, "WHEN MATCHED THEN") || !strings.Contains(sql, "UPDATE SET") {
		t.Errorf("expected WHEN MATCHED UPDATE in SQL, got:\n%s", sql)
	}
	if !strings.Contains(sql, "WHEN NOT MATCHED BY TARGET THEN") || !strings.Contains(sql, "INSERT ([id], [name])") {
		t.Errorf("expected WHEN NOT MATCHED INSERT in SQL, got:\n%s", sql)
	}
	if !strings.HasSuffix(strings.TrimSpace(sql), ";") {
		t.Errorf("expected MERGE statement ending with ';', got:\n%s", sql)
	}
}

func TestSQLServerMergeBuilder_Validate_NoAction(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	_, err := features.MergeInto("dbo.t1").
		Using("(SELECT 1 AS id) AS s").
		On("t.[id] = s.[id]").
		Build()

	if err == nil {
		t.Fatal("expected error when no merge action is provided")
	}
	if !strings.Contains(err.Error(), "at least one action") {
		t.Errorf("expected action validation error, got: %v", err)
	}
}

func TestSQLServerTempTableBuilder_BuildCreateWithColumns(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	sql, err := features.TempTable("session_items").
		Column("id", "INT", false).
		Column("name", "NVARCHAR(100)", true).
		BuildCreate()

	if err != nil {
		t.Fatalf("BuildCreate() returned error: %v", err)
	}
	if !strings.Contains(sql, "DROP TABLE #session_items") {
		t.Errorf("expected drop-if-exists clause, got:\n%s", sql)
	}
	if !strings.Contains(sql, "CREATE TABLE #session_items") {
		t.Errorf("expected create temp table clause, got:\n%s", sql)
	}
	if !strings.Contains(sql, "[id] INT NOT NULL") || !strings.Contains(sql, "[name] NVARCHAR(100) NULL") {
		t.Errorf("expected columns in SQL, got:\n%s", sql)
	}
}

func TestSQLServerTempTableBuilder_BuildCreateAsSelectGlobal(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	sql, err := features.TempTable("snapshot_users").
		Global().
		AsSelect("SELECT id, name FROM dbo.users WHERE active = 1").
		BuildCreate()

	if err != nil {
		t.Fatalf("BuildCreate() returned error: %v", err)
	}
	if !strings.Contains(sql, "DROP TABLE ##snapshot_users") {
		t.Errorf("expected global temp table drop clause, got:\n%s", sql)
	}
	if !strings.Contains(sql, "SELECT * INTO ##snapshot_users") {
		t.Errorf("expected SELECT INTO global temp table, got:\n%s", sql)
	}
}

func TestSQLServerViewBuilder_BuildCreate_DefaultCreateOrAlter(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	sql, err := features.View("dbo.v_active_users").
		As("SELECT id, name FROM [dbo].[users] WHERE [active] = 1").
		BuildCreate()

	if err != nil {
		t.Fatalf("BuildCreate() returned error: %v", err)
	}
	if !strings.Contains(sql, "CREATE OR ALTER VIEW [dbo].[v_active_users] AS") {
		t.Errorf("expected CREATE OR ALTER VIEW clause, got:\n%s", sql)
	}
	if !strings.Contains(sql, "SELECT id, name FROM [dbo].[users]") {
		t.Errorf("expected SELECT definition in SQL, got:\n%s", sql)
	}
	if !strings.HasSuffix(strings.TrimSpace(sql), ";") {
		t.Errorf("expected VIEW statement ending with ';', got:\n%s", sql)
	}
}

func TestSQLServerViewBuilder_BuildCreate_CreateOnlyWithCheckOption(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	sql, err := features.View("v_adults").
		CreateOnly().
		As("SELECT id FROM [dbo].[users] WHERE [age] >= 18").
		WithCheckOption().
		BuildCreate()

	if err != nil {
		t.Fatalf("BuildCreate() returned error: %v", err)
	}
	if !strings.Contains(sql, "CREATE VIEW [v_adults] AS") {
		t.Errorf("expected CREATE VIEW clause, got:\n%s", sql)
	}
	if !strings.Contains(sql, "WITH CHECK OPTION") {
		t.Errorf("expected WITH CHECK OPTION clause, got:\n%s", sql)
	}
}

func TestSQLServerViewBuilder_BuildDrop(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	sql, err := features.View("dbo.v_test").BuildDrop()
	if err != nil {
		t.Fatalf("BuildDrop() returned error: %v", err)
	}
	if sql != "DROP VIEW IF EXISTS [dbo].[v_test];" {
		t.Errorf("unexpected drop sql: %s", sql)
	}

	strictSQL, err := features.View("dbo.v_test").DropStrict().BuildDrop()
	if err != nil {
		t.Fatalf("BuildDrop() strict returned error: %v", err)
	}
	if strictSQL != "DROP VIEW [dbo].[v_test];" {
		t.Errorf("unexpected strict drop sql: %s", strictSQL)
	}
}

func TestSQLServerViewBuilder_Validate(t *testing.T) {
	features, _ := GetSQLServerFeatures(&SQLServerAdapter{})

	_, err := features.View("").As("SELECT 1").BuildCreate()
	if err == nil {
		t.Fatal("expected error for empty view name")
	}

	_, err = features.View("dbo.v_x").As("").BuildCreate()
	if err == nil {
		t.Fatal("expected error for empty view definition")
	}
}
