package adapter_tests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	db "github.com/eit-cms/eit-db"
)

// buildDepartmentsSchema 构建 departments 层级表（经典递归 CTE 演示场景）。
// id 使用普通整数 PK（非 IDENTITY），以便测试时显式控制父子关系 ID。
func buildDepartmentsSchema() db.Schema {
	schema := db.NewBaseSchema("departments")
	schema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true})
	schema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
	schema.AddField(&db.Field{Name: "parent_id", Type: db.TypeInteger, Null: true})
	return schema
}

func buildMergeUsersSchema() db.Schema {
	schema := db.NewBaseSchema("merge_users")
	schema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true})
	schema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
	schema.AddField(&db.Field{Name: "age", Type: db.TypeInteger, Null: false})
	return schema
}

func buildViewUsersSchema() db.Schema {
	schema := db.NewBaseSchema("view_users")
	schema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true})
	schema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
	schema.AddField(&db.Field{Name: "age", Type: db.TypeInteger, Null: false})
	return schema
}

// insertDepartmentRows 插入组织树测试数据：
//
//	1: 公司 (root)
//	  2: 技术部
//	    4: 后端组
//	    5: 前端组
//	  3: 市场部
//	    6: 品牌组
func insertDepartmentRows(ctx context.Context, repo *db.Repository) error {
	rows := []struct {
		id       int
		name     string
		parentID interface{}
	}{
		{1, "公司", nil},
		{2, "技术部", 1},
		{3, "市场部", 1},
		{4, "后端组", 2},
		{5, "前端组", 2},
		{6, "品牌组", 3},
	}
	for _, r := range rows {
		var err error
		if r.parentID == nil {
			_, err = repo.Exec(ctx,
				"INSERT INTO [dbo].[departments] (id, name, parent_id) VALUES (@p1, @p2, NULL)",
				r.id, r.name)
		} else {
			_, err = repo.Exec(ctx,
				"INSERT INTO [dbo].[departments] (id, name, parent_id) VALUES (@p1, @p2, @p3)",
				r.id, r.name, r.parentID)
		}
		if err != nil {
			return fmt.Errorf("insert %q: %w", r.name, err)
		}
	}
	return nil
}

// TestSQLServerRecursiveQuery_OrgTree E2E：递归 CTE 查询组织树层级
func TestSQLServerRecursiveQuery_OrgTree(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter, ok := repo.GetAdapter().(*db.SQLServerAdapter)
	if !ok {
		t.Skip("无法获取 SQLServerAdapter")
	}

	ctx := context.Background()

	// 建表
	schema := buildDepartmentsSchema()
	mig := db.NewSchemaMigration("test_ss_feat_depts", "create_departments").CreateTable(schema)
	if err := mig.Up(ctx, repo); err != nil {
		t.Fatalf("创建 departments 表失败: %v", err)
	}
	defer func() { _ = mig.Down(ctx, repo) }()

	// 插入数据
	if err := insertDepartmentRows(ctx, repo); err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	// 获取 SQLServerFeatures
	features, ok := db.GetSQLServerFeatures(adapter)
	if !ok {
		t.Fatal("GetSQLServerFeatures 返回 ok=false")
	}

	// 执行递归 CTE：从根节点（公司）向下展开全树，附带层级深度
	results, err := features.RecursiveQuery("org_tree").
		Columns("id", "name", "parent_id", "depth").
		Anchor(`SELECT id, name, parent_id, 0 AS depth
			FROM [dbo].[departments]
			WHERE parent_id IS NULL`).
		Recursive(`SELECT d.id, d.name, d.parent_id, t.depth + 1
			FROM [dbo].[departments] d
			INNER JOIN org_tree t ON d.parent_id = t.id`).
		Select("id, name, parent_id, depth").
		OrderBy("depth ASC, id ASC").
		MaxRecursion(10).
		ScanRows(ctx)

	if err != nil {
		t.Fatalf("递归 CTE 查询失败: %v", err)
	}

	// 应该返回全部 6 行
	if len(results) != 6 {
		t.Fatalf("期望 6 行，得到 %d 行。结果: %v", len(results), results)
	}

	// 验证层级深度分布
	depthCount := map[int64]int{}
	for _, row := range results {
		// SQL Server 返回的数值可能是 int64
		if d, ok := row["depth"].(int64); ok {
			depthCount[d]++
		}
	}
	if depthCount[0] != 1 {
		t.Errorf("期望 depth=0 有 1 行（根节点），得到 %d", depthCount[0])
	}
	if depthCount[1] != 2 {
		t.Errorf("期望 depth=1 有 2 行（技术部/市场部），得到 %d", depthCount[1])
	}
	if depthCount[2] != 3 {
		t.Errorf("期望 depth=2 有 3 行（后端组/前端组/品牌组），得到 %d", depthCount[2])
	}

	t.Logf("✓ 递归 CTE 返回 %d 行，层级分布: %v", len(results), depthCount)
}

// TestSQLServerRecursiveQuery_MaxRecursionLimit E2E：MAXRECURSION 限制保护无限递归
func TestSQLServerRecursiveQuery_MaxRecursionLimit(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter, ok := repo.GetAdapter().(*db.SQLServerAdapter)
	if !ok {
		t.Skip("无法获取 SQLServerAdapter")
	}

	ctx := context.Background()

	// 建表
	schema := buildDepartmentsSchema()
	mig := db.NewSchemaMigration("test_ss_maxrec_depts", "create_depts_maxrec").CreateTable(schema)
	if err := mig.Up(ctx, repo); err != nil {
		t.Fatalf("创建 departments 表失败: %v", err)
	}
	defer func() { _ = mig.Down(ctx, repo) }()

	if err := insertDepartmentRows(ctx, repo); err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	features, _ := db.GetSQLServerFeatures(adapter)

	// MAXRECURSION 1：只展开 1 层，应该能看到 root + depth=1（不出错，因为数据只有 2 层在此限制内）
	results, err := features.RecursiveQuery("shallow_tree").
		Columns("id", "name", "parent_id", "depth").
		Anchor(`SELECT id, name, parent_id, 0 AS depth FROM [dbo].[departments] WHERE parent_id IS NULL`).
		Recursive(`SELECT d.id, d.name, d.parent_id, t.depth + 1
			FROM [dbo].[departments] d
			INNER JOIN shallow_tree t ON d.parent_id = t.id
			WHERE t.depth < 1`).
		Select("id, name, depth").
		MaxRecursion(5).
		ScanRows(ctx)

	if err != nil {
		t.Fatalf("受限递归查询失败: %v", err)
	}
	// depth < 1 的条件使递归在 depth=1 停止，应返回根节点 + 2 个子节点 = 3 行
	if len(results) != 3 {
		t.Fatalf("期望 3 行（根节点 + 2 个直属部门），得到 %d 行。结果: %v", len(results), results)
	}

	t.Logf("✓ 受限递归（depth < 1 condition）返回 %d 行", len(results))
}

// TestSQLServerRecursiveQuery_BuildOnly 验证 Build() 方法生成正确 T-SQL
func TestSQLServerRecursiveQuery_BuildOnly(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter, ok := repo.GetAdapter().(*db.SQLServerAdapter)
	if !ok {
		t.Skip("无法获取 SQLServerAdapter")
	}

	features, _ := db.GetSQLServerFeatures(adapter)

	query, err := features.RecursiveQuery("path_cte").
		Anchor("SELECT id, name, 0 AS level FROM nodes WHERE parent_id IS NULL").
		Recursive("SELECT n.id, n.name, t.level + 1 FROM nodes n JOIN path_cte t ON n.parent_id = t.id").
		SelectAll().
		MaxRecursion(32).
		Build()

	if err != nil {
		t.Fatalf("Build() 失败: %v", err)
	}

	expectedSnippets := []string{
		"WITH [path_cte]",
		"UNION ALL",
		"FROM [path_cte]",
		"OPTION (MAXRECURSION 32)",
	}
	for _, snippet := range expectedSnippets {
		if !strings.Contains(query, snippet) {
			t.Errorf("期望 T-SQL 包含 %q，实际:\n%s", snippet, query)
		}
	}

	t.Logf("✓ Build() 生成 T-SQL:\n%s", query)
}

// TestSQLServerMergeFeature_Upsert E2E：通过 MERGE 完成更新+插入
func TestSQLServerMergeFeature_Upsert(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter, ok := repo.GetAdapter().(*db.SQLServerAdapter)
	if !ok {
		t.Skip("无法获取 SQLServerAdapter")
	}

	ctx := context.Background()

	mig := db.NewSchemaMigration("test_ss_merge_users", "create_merge_users").CreateTable(buildMergeUsersSchema())
	if err := mig.Up(ctx, repo); err != nil {
		t.Fatalf("创建 merge_users 表失败: %v", err)
	}
	defer func() { _ = mig.Down(ctx, repo) }()

	_, err := repo.Exec(ctx,
		"INSERT INTO [dbo].[merge_users] (id, name, age) VALUES (@p1, @p2, @p3), (@p4, @p5, @p6)",
		1, "Alice", 30,
		2, "Bob", 25,
	)
	if err != nil {
		t.Fatalf("插入初始数据失败: %v", err)
	}

	features, _ := db.GetSQLServerFeatures(adapter)

	if _, err := features.MergeInto("dbo.merge_users").
		Using("(VALUES (1, N'Alice Updated', 31), (3, N'Carol', 22)) AS s(id, name, age)").
		On("t.[id] = s.[id]").
		WhenMatchedUpdate("t.[name] = s.[name], t.[age] = s.[age]").
		WhenNotMatchedInsert([]string{"id", "name", "age"}, "s.[id], s.[name], s.[age]").
		Execute(ctx); err != nil {
		t.Fatalf("执行 MERGE 失败: %v", err)
	}

	rows, err := repo.Query(ctx,
		"SELECT id, name, age FROM [dbo].[merge_users] ORDER BY id ASC",
	)
	if err != nil {
		t.Fatalf("查询 merge_users 失败: %v", err)
	}
	defer rows.Close()

	type userRow struct {
		id   int
		name string
		age  int
	}
	var got []userRow
	for rows.Next() {
		var r userRow
		if scanErr := rows.Scan(&r.id, &r.name, &r.age); scanErr != nil {
			t.Fatalf("扫描结果失败: %v", scanErr)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("读取查询结果失败: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("期望 3 行（更新 1 行 + 插入 1 行 + 保留 1 行），得到 %d 行: %+v", len(got), got)
	}

	if got[0].id != 1 || got[0].name != "Alice Updated" || got[0].age != 31 {
		t.Fatalf("id=1 行不符合预期: %+v", got[0])
	}
	if got[1].id != 2 || got[1].name != "Bob" || got[1].age != 25 {
		t.Fatalf("id=2 行不符合预期: %+v", got[1])
	}
	if got[2].id != 3 || got[2].name != "Carol" || got[2].age != 22 {
		t.Fatalf("id=3 行不符合预期: %+v", got[2])
	}
}

// TestSQLServerTempTableFeature_Global E2E：创建全局临时表并查询，再删除
func TestSQLServerTempTableFeature_Global(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter, ok := repo.GetAdapter().(*db.SQLServerAdapter)
	if !ok {
		t.Skip("无法获取 SQLServerAdapter")
	}

	ctx := context.Background()
	features, _ := db.GetSQLServerFeatures(adapter)

	tempName := "eit_global_temp_feature_test"
	builder := features.TempTable(tempName).
		Global().
		AsSelect("SELECT CAST(1 AS INT) AS id, N'alpha' AS name")
	createSQL, err := builder.BuildCreate()
	if err != nil {
		t.Fatalf("构建全局临时表 SQL 失败: %v", err)
	}

	// 在同一批次中创建并查询，避免连接池切换导致的临时表会话可见性问题。
	rows, err := repo.Query(ctx, createSQL+"\nSELECT COUNT(*) FROM ##eit_global_temp_feature_test;")
	if err != nil {
		t.Fatalf("创建并查询全局临时表失败: %v", err)
	}
	var cnt int
	if rows.Next() {
		if scanErr := rows.Scan(&cnt); scanErr != nil {
			rows.Close()
			t.Fatalf("扫描 count 失败: %v", scanErr)
		}
	}
	rows.Close()

	if cnt != 1 {
		t.Fatalf("期望全局临时表有 1 行，实际 %d 行", cnt)
	}

	if err := builder.Drop(ctx); err != nil {
		t.Fatalf("删除全局临时表失败: %v", err)
	}
}

// TestSQLServerViewFeature_CreateOrAlter E2E：创建并更新 VIEW，然后验证查询结果。
func TestSQLServerViewFeature_CreateOrAlter(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter, ok := repo.GetAdapter().(*db.SQLServerAdapter)
	if !ok {
		t.Skip("无法获取 SQLServerAdapter")
	}

	ctx := context.Background()
	features, _ := db.GetSQLServerFeatures(adapter)

	mig := db.NewSchemaMigration("test_ss_view_users", "create_view_users").CreateTable(buildViewUsersSchema())
	if err := mig.Up(ctx, repo); err != nil {
		t.Fatalf("创建 view_users 表失败: %v", err)
	}
	defer func() { _ = mig.Down(ctx, repo) }()

	_, err := repo.Exec(ctx,
		"INSERT INTO [dbo].[view_users] (id, name, age) VALUES (@p1, @p2, @p3), (@p4, @p5, @p6), (@p7, @p8, @p9)",
		1, "Amy", 17,
		2, "Bob", 21,
		3, "Carl", 35,
	)
	if err != nil {
		t.Fatalf("插入 view_users 测试数据失败: %v", err)
	}

	builder := features.View("dbo.v_adult_users").
		As("SELECT id, name, age FROM [dbo].[view_users] WHERE [age] >= 18")

	if err := builder.ExecuteCreate(ctx); err != nil {
		t.Fatalf("创建视图失败: %v", err)
	}
	defer func() { _ = builder.Drop(ctx) }()

	var cnt int
	rows, err := repo.Query(ctx, "SELECT COUNT(*) FROM [dbo].[v_adult_users]")
	if err != nil {
		t.Fatalf("查询视图失败: %v", err)
	}
	if rows.Next() {
		if scanErr := rows.Scan(&cnt); scanErr != nil {
			rows.Close()
			t.Fatalf("扫描 count 失败: %v", scanErr)
		}
	}
	rows.Close()

	if cnt != 2 {
		t.Fatalf("期望成年人视图 2 行，实际 %d 行", cnt)
	}

	if err := builder.As("SELECT id, name, age FROM [dbo].[view_users] WHERE [age] >= 30").ExecuteCreate(ctx); err != nil {
		t.Fatalf("更新视图定义失败: %v", err)
	}

	rows, err = repo.Query(ctx, "SELECT COUNT(*) FROM [dbo].[v_adult_users]")
	if err != nil {
		t.Fatalf("查询更新后的视图失败: %v", err)
	}
	if rows.Next() {
		if scanErr := rows.Scan(&cnt); scanErr != nil {
			rows.Close()
			t.Fatalf("扫描更新后 count 失败: %v", scanErr)
		}
	}
	rows.Close()

	if cnt != 1 {
		t.Fatalf("期望更新后视图 1 行，实际 %d 行", cnt)
	}
}
