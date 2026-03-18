package adapter_tests

import (
	"context"
	"strings"
	"testing"

	db "github.com/eit-cms/eit-db"
)

// buildSQLServerProjectsSchema 构建 projects 父表 Schema（作为动态表触发源）
func buildSQLServerProjectsSchema() db.Schema {
	schema := db.NewBaseSchema("projects")
	schema.AddField(&db.Field{Name: "id", Type: db.TypeString, Primary: true})
	schema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
	return schema
}

// buildSQLServerEventsDynamicTableConfig 构建事件动态表配置
func buildSQLServerEventsDynamicTableConfig(strategy string) *db.DynamicTableConfig {
	return &db.DynamicTableConfig{
		TableName:   "events",
		ParentTable: "projects",
		Strategy:    strategy,
		Fields: []*db.DynamicTableField{
			{Name: "id", Type: db.TypeInteger, Primary: true, Autoinc: true},
			{Name: "event_type", Type: db.TypeString, Null: false},
			{Name: "payload", Type: db.TypeString},
		},
	}
}

// getSQLServerAdapter 从 Repository 提取底层 SQLServerAdapter（利用 GetUnderlyingAdapter 接口）
func getSQLServerAdapter(t *testing.T, repo *db.Repository) *db.SQLServerAdapter {
	t.Helper()
	adapter, ok := repo.GetAdapter().(*db.SQLServerAdapter)
	if !ok {
		t.Skip("无法获取 SQLServerAdapter，跳过动态表测试")
		return nil
	}
	return adapter
}

// TestSQLServerDynamicTable_ManualStrategy 手动策略：手动调用 CreateDynamicTable 创建子表
func TestSQLServerDynamicTable_ManualStrategy(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter := getSQLServerAdapter(t, repo)
	if adapter == nil {
		return
	}

	ctx := context.Background()

	// 清理可能残留的动态表（防止上次测试失败时未清理）
	_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS [dbo].[events_proj001]")

	// 创建父表
	projectsSchema := buildSQLServerProjectsSchema()
	migration := db.NewSchemaMigration("test_ss_dyn_projects_manual", "create_projects_manual").
		CreateTable(projectsSchema)
	if err := migration.Up(ctx, repo); err != nil {
		t.Fatalf("创建 projects 表失败: %v", err)
	}
	defer func() { _ = migration.Down(ctx, repo) }()

	// 初始化 Hook（手动策略，不创建触发器）
	hook := db.NewSQLServerDynamicTableHook(adapter)
	config := buildSQLServerEventsDynamicTableConfig("manual")

	if err := hook.RegisterDynamicTable(ctx, config); err != nil {
		t.Fatalf("RegisterDynamicTable 失败: %v", err)
	}
	defer func() { _ = hook.UnregisterDynamicTable(ctx, config.TableName) }()

	// 列出注册信息
	configs, err := hook.ListDynamicTableConfigs(ctx)
	if err != nil {
		t.Fatalf("ListDynamicTableConfigs 失败: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("期望 1 个配置，得到 %d", len(configs))
	}

	// 手动创建动态表（entityID = "proj001"）
	tableName, err := hook.CreateDynamicTable(ctx, "events", map[string]interface{}{"id": "proj001"})
	if err != nil {
		t.Fatalf("CreateDynamicTable 失败: %v", err)
	}
	if tableName != "events_proj001" {
		t.Fatalf("期望表名 events_proj001，得到 %s", tableName)
	}

	// 验证动态表已存在
	tables, err := hook.ListCreatedDynamicTables(ctx, "events")
	if err != nil {
		t.Fatalf("ListCreatedDynamicTables 失败: %v", err)
	}
	found := false
	for _, tbl := range tables {
		if tbl == "events_proj001" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("未找到动态表 events_proj001，实际列表: %v", tables)
	}

	// 向动态表插入一行
	_, err = repo.Exec(ctx, "INSERT INTO [dbo].[events_proj001] (event_type, payload) VALUES ('login', 'test payload')")
	if err != nil {
		t.Fatalf("向动态表插入数据失败: %v", err)
	}

	// 清理动态表
	_, err = repo.Exec(ctx, "DROP TABLE IF EXISTS [dbo].[events_proj001]")
	if err != nil {
		t.Fatalf("清理动态表失败: %v", err)
	}
}

// TestSQLServerDynamicTable_AutoStrategy 自动策略：插入父表行时自动创建子表（触发器）
func TestSQLServerDynamicTable_AutoStrategy(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter := getSQLServerAdapter(t, repo)
	if adapter == nil {
		return
	}

	ctx := context.Background()

	// 创建父表
	projectsSchema := buildSQLServerProjectsSchema()
	migration := db.NewSchemaMigration("test_ss_dyn_projects_auto", "create_projects_auto").
		CreateTable(projectsSchema)
	if err := migration.Up(ctx, repo); err != nil {
		t.Fatalf("创建 projects 表失败: %v", err)
	}
	defer func() { _ = migration.Down(ctx, repo) }()

	// 注册 auto 策略（创建 Procedure + Trigger）
	hook := db.NewSQLServerDynamicTableHook(adapter)
	config := buildSQLServerEventsDynamicTableConfig("auto")

	if err := hook.RegisterDynamicTable(ctx, config); err != nil {
		t.Fatalf("RegisterDynamicTable（auto）失败: %v", err)
	}
	defer func() { _ = hook.UnregisterDynamicTable(ctx, config.TableName) }()

	// 验证 Procedure 已存在于 sys.procedures
	var procCount int
	rows, err := repo.Query(ctx,
		"SELECT COUNT(*) FROM sys.procedures WHERE name = @p1 AND schema_id = SCHEMA_ID('dbo')",
		"sp_create_events_table",
	)
	if err != nil {
		t.Fatalf("查询 sys.procedures 失败: %v", err)
	}
	if rows.Next() {
		_ = rows.Scan(&procCount)
	}
	rows.Close()
	if procCount == 0 {
		t.Fatal("存储过程 sp_create_events_table 未创建")
	}

	// 验证 Trigger 已存在于 sys.triggers
	var trigCount int
	rows, err = repo.Query(ctx,
		"SELECT COUNT(*) FROM sys.triggers WHERE name = @p1",
		"trg_auto_events",
	)
	if err != nil {
		t.Fatalf("查询 sys.triggers 失败: %v", err)
	}
	if rows.Next() {
		_ = rows.Scan(&trigCount)
	}
	rows.Close()
	if trigCount == 0 {
		t.Fatal("触发器 trg_auto_events 未创建")
	}

	// 向父表插入一行，触发器应自动创建子表 events_p42
	_, err = repo.Exec(ctx, "INSERT INTO [dbo].[projects] (id, name) VALUES ('p42', 'Test Project')")
	if err != nil {
		t.Fatalf("插入 projects 行失败（触发器可能报错）: %v", err)
	}

	// 验证动态表 events_p42 被触发器自动创建
	var tableCount int
	rows, err = repo.Query(ctx,
		"SELECT COUNT(*) FROM sys.tables t INNER JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo' AND t.name=@p1",
		"events_p42",
	)
	if err != nil {
		t.Fatalf("查询 sys.tables 失败: %v", err)
	}
	if rows.Next() {
		_ = rows.Scan(&tableCount)
	}
	rows.Close()
	if tableCount == 0 {
		t.Fatal("触发器未自动创建 events_p42 表")
	}

	// 通过 ListCreatedDynamicTables 验证
	tables, err := hook.ListCreatedDynamicTables(ctx, "events")
	if err != nil {
		t.Fatalf("ListCreatedDynamicTables 失败: %v", err)
	}
	foundP42 := false
	for _, tbl := range tables {
		if tbl == "events_p42" {
			foundP42 = true
			break
		}
	}
	if !foundP42 {
		t.Fatalf("ListCreatedDynamicTables 未返回 events_p42，实际: %v", tables)
	}

	// 清理动态表
	_, _ = repo.Exec(ctx, "DROP TABLE IF EXISTS [dbo].[events_p42]")
}

// TestSQLServerDynamicTable_UnregisterDropsTriggerAndProcedure 注销后触发器和存储过程应被删除
func TestSQLServerDynamicTable_UnregisterDropsTriggerAndProcedure(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter := getSQLServerAdapter(t, repo)
	if adapter == nil {
		return
	}

	ctx := context.Background()

	// 创建父表
	projectsSchema := buildSQLServerProjectsSchema()
	migration := db.NewSchemaMigration("test_ss_dyn_unregister", "create_projects_unreg").
		CreateTable(projectsSchema)
	if err := migration.Up(ctx, repo); err != nil {
		t.Fatalf("创建 projects 表失败: %v", err)
	}
	defer func() { _ = migration.Down(ctx, repo) }()

	hook := db.NewSQLServerDynamicTableHook(adapter)
	config := buildSQLServerEventsDynamicTableConfig("auto")

	if err := hook.RegisterDynamicTable(ctx, config); err != nil {
		t.Fatalf("RegisterDynamicTable 失败: %v", err)
	}

	// 注销
	if err := hook.UnregisterDynamicTable(ctx, "events"); err != nil {
		t.Fatalf("UnregisterDynamicTable 失败: %v", err)
	}

	// 验证触发器已删除
	var trigCount int
	rows, err := repo.Query(ctx,
		"SELECT COUNT(*) FROM sys.triggers WHERE name = @p1",
		"trg_auto_events",
	)
	if err != nil {
		t.Fatalf("查询 sys.triggers 失败: %v", err)
	}
	if rows.Next() {
		_ = rows.Scan(&trigCount)
	}
	rows.Close()
	if trigCount != 0 {
		t.Fatal("UnregisterDynamicTable 后触发器仍存在")
	}

	// 验证存储过程已删除
	var procCount int
	rows, err = repo.Query(ctx,
		"SELECT COUNT(*) FROM sys.procedures WHERE name = @p1 AND schema_id = SCHEMA_ID('dbo')",
		"sp_create_events_table",
	)
	if err != nil {
		t.Fatalf("查询 sys.procedures 失败: %v", err)
	}
	if rows.Next() {
		_ = rows.Scan(&procCount)
	}
	rows.Close()
	if procCount != 0 {
		t.Fatal("UnregisterDynamicTable 后存储过程仍存在")
	}

	// 确认 registry 已清空
	configs, err := hook.ListDynamicTableConfigs(ctx)
	if err != nil {
		t.Fatalf("ListDynamicTableConfigs 失败: %v", err)
	}
	if len(configs) != 0 {
		t.Fatalf("注销后期望 0 个配置，得到 %d", len(configs))
	}
}

// TestSQLServerDynamicTable_GetConfig 验证 GetDynamicTableConfig 返回正确信息
func TestSQLServerDynamicTable_GetConfig(t *testing.T) {
	repo, cleanup := setupSQLServerRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	adapter := getSQLServerAdapter(t, repo)
	if adapter == nil {
		return
	}

	ctx := context.Background()

	hook := db.NewSQLServerDynamicTableHook(adapter)
	config := buildSQLServerEventsDynamicTableConfig("manual")

	if err := hook.RegisterDynamicTable(ctx, config); err != nil {
		t.Fatalf("RegisterDynamicTable 失败: %v", err)
	}
	defer func() { _ = hook.UnregisterDynamicTable(ctx, config.TableName) }()

	got, err := hook.GetDynamicTableConfig(ctx, "events")
	if err != nil {
		t.Fatalf("GetDynamicTableConfig 失败: %v", err)
	}
	if got.TableName != "events" {
		t.Fatalf("TableName 不匹配: %s", got.TableName)
	}
	if got.Strategy != "manual" {
		t.Fatalf("Strategy 不匹配: %s", got.Strategy)
	}
	if !strings.HasPrefix(got.TableName, "events") {
		t.Fatalf("TableName 前缀不匹配: %s", got.TableName)
	}
}
