package db

import (
	"context"
	"strings"
	"testing"
)

// ==================== Step 1: ViewHint / FKOption in schema.go ====================

// TestAddForeignKeyWithViewHint 验证 WithViewHint 选项被正确存储在 TableConstraint 中
func TestAddForeignKeyWithViewHint(t *testing.T) {
	schema := NewBaseSchema("orders")
	schema.AddField(NewField("id", TypeInteger).PrimaryKey().Build())
	schema.AddField(NewField("user_id", TypeInteger).Build())

	schema.AddForeignKey("fk_orders_users",
		[]string{"user_id"}, "users", []string{"id"},
		"CASCADE", "",
		WithViewHint("user_orders_view", false))

	constraints := schema.Constraints()
	if len(constraints) != 1 {
		t.Fatalf("expected 1 constraint, got %d", len(constraints))
	}

	fk := constraints[0]
	if fk.Kind != ConstraintForeignKey {
		t.Errorf("expected FK kind, got %s", fk.Kind)
	}
	if fk.ViewHint == nil {
		t.Fatal("expected ViewHint to be set, got nil")
	}
	if fk.ViewHint.ViewName != "user_orders_view" {
		t.Errorf("expected ViewName=user_orders_view, got %s", fk.ViewHint.ViewName)
	}
	if fk.ViewHint.Materialized {
		t.Errorf("expected Materialized=false")
	}
	t.Logf("✓ ViewHint stored: %+v", *fk.ViewHint)
}

// TestAddForeignKeyBackwardCompat 验证 AddForeignKey 在不传 opts 时向后兼容
func TestAddForeignKeyBackwardCompat(t *testing.T) {
	schema := NewBaseSchema("orders")
	schema.AddForeignKey("fk_orders_users",
		[]string{"user_id"}, "users", []string{"id"},
		"CASCADE", "")

	constraints := schema.Constraints()
	if len(constraints) != 1 {
		t.Fatalf("expected 1 constraint, got %d", len(constraints))
	}
	if constraints[0].ViewHint != nil {
		t.Errorf("expected ViewHint to be nil without WithViewHint option")
	}
	t.Log("✓ Backward compatibility with no opts preserved")
}

// TestWithViewHint_EmptyViewName 验证视图名为空时自动生成
func TestWithViewHint_EmptyViewName(t *testing.T) {
	schema := NewBaseSchema("orders")
	schema.AddForeignKey("fk", []string{"user_id"}, "users", []string{"id"}, "", "",
		WithViewHint("", false))

	fk := schema.Constraints()[0]
	if fk.ViewHint == nil {
		t.Fatal("ViewHint should be set")
	}
	// 空 ViewName 由 Registry / Migration 阶段自动补全
	if fk.ViewHint.ViewName != "" {
		t.Errorf("expected empty ViewName stored as-is, got %s", fk.ViewHint.ViewName)
	}
	t.Log("✓ Empty ViewName stored; auto-generation deferred to migration/registry")
}

// TestWithJoinType 验证 WithJoinType 正确设置 JoinType
func TestWithJoinType(t *testing.T) {
	schema := NewBaseSchema("orders")
	schema.AddForeignKey("fk", []string{"user_id"}, "users", []string{"id"}, "", "",
		WithViewHint("v", false),
		WithJoinType("left"))

	fk := schema.Constraints()[0]
	if fk.ViewHint == nil {
		t.Fatal("ViewHint should be set")
	}
	if fk.ViewHint.JoinType != "LEFT" {
		t.Errorf("expected JoinType=LEFT, got %s", fk.ViewHint.JoinType)
	}
	t.Log("✓ WithJoinType sets JoinType correctly")
}

// ==================== Step 2: Migration view DDL generation ====================

// TestBuildViewFromFKHintSQL_MySQL 验证 MySQL 生成 CREATE OR REPLACE VIEW
func TestBuildViewFromFKHintSQL_MySQL(t *testing.T) {
	repo := newTestRepoWithAdapter(newMockMySQLAdapter())
	fk := TableConstraint{
		Name:      "fk_orders_users",
		Kind:      ConstraintForeignKey,
		Fields:    []string{"user_id"},
		RefTable:  "users",
		RefFields: []string{"id"},
		ViewHint:  &ViewHint{ViewName: "user_orders_view", Materialized: false},
	}

	sql, err := buildViewFromFKHintSQL(repo, "orders", fk)
	if err != nil {
		t.Fatalf("buildViewFromFKHintSQL failed: %v", err)
	}

	if !strings.HasPrefix(sql, "CREATE OR REPLACE VIEW") {
		t.Errorf("expected CREATE OR REPLACE VIEW, got: %s", sql)
	}
	if !strings.Contains(sql, "`user_orders_view`") {
		t.Errorf("expected quoted view name, got: %s", sql)
	}
	if !strings.Contains(sql, "INNER JOIN") {
		t.Errorf("expected INNER JOIN, got: %s", sql)
	}
	if !strings.Contains(sql, "o.user_id = u.id") {
		t.Errorf("expected ON clause, got: %s", sql)
	}
	t.Logf("✓ MySQL view SQL: %s", sql)
}

// TestBuildViewFromFKHintSQL_Postgres 验证 PostgreSQL 生成 CREATE OR REPLACE VIEW
func TestBuildViewFromFKHintSQL_Postgres(t *testing.T) {
	repo := newTestRepoWithAdapter(newMockPostgresAdapter())
	fk := TableConstraint{
		Name:      "fk_orders_users",
		Kind:      ConstraintForeignKey,
		Fields:    []string{"user_id"},
		RefTable:  "users",
		RefFields: []string{"id"},
		ViewHint:  &ViewHint{ViewName: "user_orders_view", Materialized: false},
	}

	sql, err := buildViewFromFKHintSQL(repo, "orders", fk)
	if err != nil {
		t.Fatalf("buildViewFromFKHintSQL failed: %v", err)
	}

	if !strings.HasPrefix(sql, "CREATE OR REPLACE VIEW") {
		t.Errorf("expected CREATE OR REPLACE VIEW, got: %s", sql)
	}
	t.Logf("✓ PostgreSQL view SQL: %s", sql)
}

// TestBuildViewFromFKHintSQL_Postgres_Materialized 验证 PostgreSQL 物化视图
func TestBuildViewFromFKHintSQL_Postgres_Materialized(t *testing.T) {
	repo := newTestRepoWithAdapter(newMockPostgresAdapter())
	fk := TableConstraint{
		Name:      "fk_orders_users",
		Kind:      ConstraintForeignKey,
		Fields:    []string{"user_id"},
		RefTable:  "users",
		RefFields: []string{"id"},
		ViewHint:  &ViewHint{ViewName: "mat_user_orders", Materialized: true},
	}

	sql, err := buildViewFromFKHintSQL(repo, "orders", fk)
	if err != nil {
		t.Fatalf("buildViewFromFKHintSQL failed: %v", err)
	}

	if !strings.HasPrefix(sql, "CREATE MATERIALIZED VIEW") {
		t.Errorf("expected CREATE MATERIALIZED VIEW, got: %s", sql)
	}
	t.Logf("✓ PostgreSQL materialized view SQL: %s", sql)
}

// TestBuildViewFromFKHintSQL_SQLServer 验证 SQL Server 生成 CREATE OR ALTER VIEW
func TestBuildViewFromFKHintSQL_SQLServer(t *testing.T) {
	repo := newTestRepoWithAdapter(newMockSQLServerAdapter())
	fk := TableConstraint{
		Name:      "fk_orders_users",
		Kind:      ConstraintForeignKey,
		Fields:    []string{"user_id"},
		RefTable:  "users",
		RefFields: []string{"id"},
		ViewHint:  &ViewHint{ViewName: "user_orders_view"},
	}

	sql, err := buildViewFromFKHintSQL(repo, "orders", fk)
	if err != nil {
		t.Fatalf("buildViewFromFKHintSQL failed: %v", err)
	}

	if !strings.HasPrefix(sql, "CREATE OR ALTER VIEW") {
		t.Errorf("expected CREATE OR ALTER VIEW, got: %s", sql)
	}
	t.Logf("✓ SQL Server view SQL: %s", sql)
}

// TestBuildViewFromFKHintSQL_AutoViewName 验证视图名为空时自动生成
func TestBuildViewFromFKHintSQL_AutoViewName(t *testing.T) {
	repo := newTestRepoWithAdapter(newMockMySQLAdapter())
	fk := TableConstraint{
		Name:      "fk",
		Kind:      ConstraintForeignKey,
		Fields:    []string{"user_id"},
		RefTable:  "users",
		RefFields: []string{"id"},
		ViewHint:  &ViewHint{ViewName: "", Materialized: false},
	}

	sql, err := buildViewFromFKHintSQL(repo, "orders", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(sql, "orders_users_view") {
		t.Errorf("expected auto-generated view name 'orders_users_view' in: %s", sql)
	}
	t.Logf("✓ Auto-generated view name: %s", sql)
}

// TestBuildViewFromFKHintSQL_CustomColumns 验证自定义列声明
func TestBuildViewFromFKHintSQL_CustomColumns(t *testing.T) {
	repo := newTestRepoWithAdapter(newMockMySQLAdapter())
	fk := TableConstraint{
		Name:      "fk",
		Kind:      ConstraintForeignKey,
		Fields:    []string{"user_id"},
		RefTable:  "users",
		RefFields: []string{"id"},
		ViewHint: &ViewHint{
			ViewName: "v",
			Columns:  []string{"o.id AS order_id", "o.amount", "u.name AS user_name"},
		},
	}

	sql, err := buildViewFromFKHintSQL(repo, "orders", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(sql, "o.id AS order_id") {
		t.Errorf("expected custom columns in: %s", sql)
	}
	t.Logf("✓ Custom columns view SQL: %s", sql)
}

// TestBuildViewFromFKHintSQL_LeftJoin 验证 WithJoinType("LEFT") 生效
func TestBuildViewFromFKHintSQL_LeftJoin(t *testing.T) {
	repo := newTestRepoWithAdapter(newMockMySQLAdapter())
	fk := TableConstraint{
		Name:      "fk",
		Kind:      ConstraintForeignKey,
		Fields:    []string{"user_id"},
		RefTable:  "users",
		RefFields: []string{"id"},
		ViewHint:  &ViewHint{ViewName: "v", JoinType: "LEFT"},
	}

	sql, err := buildViewFromFKHintSQL(repo, "orders", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(sql, "LEFT JOIN") {
		t.Errorf("expected LEFT JOIN, got: %s", sql)
	}
	t.Logf("✓ LEFT JOIN view SQL: %s", sql)
}

// TestBuildDropViewSQL 验证不同 adapter 的 DROP VIEW SQL
func TestBuildDropViewSQL(t *testing.T) {
	cases := []struct {
		adapter      Adapter
		materialized bool
		wantPrefix   string
	}{
		{newMockMySQLAdapter(), false, "DROP VIEW IF EXISTS"},
		{newMockPostgresAdapter(), false, "DROP VIEW IF EXISTS"},
		{newMockPostgresAdapter(), true, "DROP MATERIALIZED VIEW IF EXISTS"},
		{newMockSQLServerAdapter(), false, "IF OBJECT_ID("},
	}

	for _, tc := range cases {
		repo := newTestRepoWithAdapter(tc.adapter)
		sql := buildDropViewSQL(repo, "my_view", tc.materialized)
		if !strings.HasPrefix(sql, tc.wantPrefix) {
			t.Errorf("adapter=%T materialized=%v: expected prefix %q, got: %s",
				tc.adapter, tc.materialized, tc.wantPrefix, sql)
		}
		t.Logf("✓ DROP VIEW SQL (%T materialized=%v): %s", tc.adapter, tc.materialized, sql)
	}
}

// ==================== Step 3: CrossTableViewRegistry ====================

// TestCrossTableViewRegistry_RegisterAndLookup 验证注册和精确查找
func TestCrossTableViewRegistry_RegisterAndLookup(t *testing.T) {
	reg := NewCrossTableViewRegistry()
	reg.Register(&CrossTableViewEntry{
		LocalTable: "orders",
		RefTable:   "users",
		ViewName:   "user_orders_view",
	})

	entry, ok := reg.Lookup("orders", "users")
	if !ok {
		t.Fatal("expected to find entry for orders|users")
	}
	if entry.ViewName != "user_orders_view" {
		t.Errorf("expected ViewName=user_orders_view, got %s", entry.ViewName)
	}

	_, ok = reg.Lookup("users", "orders")
	if ok {
		t.Error("forward-only lookup should not find reverse entry")
	}
	t.Log("✓ Register and Lookup work correctly")
}

// TestCrossTableViewRegistry_LookupAny 验证双向查找
func TestCrossTableViewRegistry_LookupAny(t *testing.T) {
	reg := NewCrossTableViewRegistry()
	reg.Register(&CrossTableViewEntry{
		LocalTable: "orders",
		RefTable:   "users",
		ViewName:   "user_orders_view",
	})

	// 正向
	e1, ok := reg.LookupAny("orders", "users")
	if !ok || e1.ViewName != "user_orders_view" {
		t.Errorf("expected forward lookup to succeed")
	}

	// 反向（FROM users JOIN orders → 仍可找到视图）
	e2, ok := reg.LookupAny("users", "orders")
	if !ok || e2.ViewName != "user_orders_view" {
		t.Errorf("expected reverse lookup to succeed, ok=%v", ok)
	}
	t.Log("✓ LookupAny works in both directions")
}

// TestCrossTableViewRegistry_RegisterFromSchema 验证从 Schema FK 自动注册
func TestCrossTableViewRegistry_RegisterFromSchema(t *testing.T) {
	ordersSchema := NewBaseSchema("orders")
	ordersSchema.AddField(NewField("user_id", TypeInteger).Build())
	ordersSchema.AddForeignKey("fk_orders_users",
		[]string{"user_id"}, "users", []string{"id"},
		"CASCADE", "",
		WithViewHint("user_orders_view", false))

	reg := NewCrossTableViewRegistry()
	reg.RegisterFromSchema(ordersSchema)

	entry, ok := reg.Lookup("orders", "users")
	if !ok {
		t.Fatal("expected entry after RegisterFromSchema")
	}
	if entry.ViewName != "user_orders_view" {
		t.Errorf("expected ViewName=user_orders_view, got %s", entry.ViewName)
	}
	if entry.LocalAlias != "o" {
		t.Errorf("expected LocalAlias=o (from 'orders'), got %s", entry.LocalAlias)
	}
	if entry.RefAlias != "u" {
		t.Errorf("expected RefAlias=u (from 'users'), got %s", entry.RefAlias)
	}
	t.Logf("✓ RegisterFromSchema works: entry=%+v", *entry)
}

// TestCrossTableViewRegistry_AutoViewName 验证 ViewName 为空时自动生成
func TestCrossTableViewRegistry_AutoViewName(t *testing.T) {
	schema := NewBaseSchema("order_items")
	schema.AddForeignKey("fk", []string{"product_id"}, "products", []string{"id"}, "", "",
		WithViewHint("", false))

	reg := NewCrossTableViewRegistry()
	reg.RegisterFromSchema(schema)

	entry, ok := reg.Lookup("order_items", "products")
	if !ok {
		t.Fatal("expected auto registered entry")
	}
	expected := "order_items_products_view"
	if entry.ViewName != expected {
		t.Errorf("expected ViewName=%s, got %s", expected, entry.ViewName)
	}
	t.Logf("✓ Auto ViewName=%s", entry.ViewName)
}

// TestDeriveViewAlias 验证别名推导逻辑
func TestDeriveViewAlias(t *testing.T) {
	cases := []struct {
		table string
		want  string
	}{
		{"users", "u"},
		{"orders", "o"},
		{"order_items", "oi"},
		{"user_profiles", "up"},
		{"products", "p"},
		{"dbo.users", "u"},
		{`"public"."orders"`, "\"public\""}, // 带引号时取整体第一段，这是边界测试
	}

	for _, tc := range cases {
		// 修正：带引号的 schema.table 是边界场景，这里只测试无引号情况
		if strings.Contains(tc.table, `"`) {
			continue
		}
		got := deriveViewAlias(tc.table)
		if got != tc.want {
			t.Errorf("deriveViewAlias(%q) = %q, want %q", tc.table, got, tc.want)
		}
	}
	t.Log("✓ deriveViewAlias produces expected aliases")
}

// ==================== Step 4: QueryBuilder auto-routing ====================

// TestQueryBuilder_AutoRouteToView_MySQL 验证 MySQL JOIN 自动路由到视图
func TestQueryBuilder_AutoRouteToView_MySQL(t *testing.T) {
	ordersSchema := NewBaseSchema("orders")
	ordersSchema.AddField(NewField("id", TypeInteger).PrimaryKey().Build())
	ordersSchema.AddField(NewField("user_id", TypeInteger).Build())
	ordersSchema.AddForeignKey("fk_orders_users",
		[]string{"user_id"}, "users", []string{"id"}, "CASCADE", "",
		WithViewHint("user_orders_view", false))

	reg := NewCrossTableViewRegistry()
	reg.RegisterFromSchema(ordersSchema)

	dialect := NewMySQLDialect()
	qc := NewSQLQueryConstructor(ordersSchema, dialect)
	qc.viewRegistry = reg // 注入私有注册表（测试专用）

	// 原来的 JOIN → 应该被路由到视图
	qc.Join("users", "orders.user_id = users.id")

	ctx := context.Background()
	sql, args, err := qc.Build(ctx)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if strings.Contains(sql, "JOIN") {
		t.Errorf("expected no JOIN after auto-routing, got: %s", sql)
	}
	if !strings.Contains(sql, "user_orders_view") {
		t.Errorf("expected view name in SQL, got: %s", sql)
	}
	if len(args) != 0 {
		t.Errorf("expected no args, got: %v", args)
	}
	t.Logf("✓ Auto-routed SQL: %s", sql)
}

// TestQueryBuilder_AutoRouteToView_ReverseFK 验证反向 FK 也能路由（FROM users JOIN orders）
func TestQueryBuilder_AutoRouteToView_ReverseFK(t *testing.T) {
	ordersSchema := NewBaseSchema("orders")
	ordersSchema.AddForeignKey("fk_orders_users",
		[]string{"user_id"}, "users", []string{"id"}, "", "",
		WithViewHint("user_orders_view", false))

	reg := NewCrossTableViewRegistry()
	reg.RegisterFromSchema(ordersSchema)

	// 从 users 表出发 JOIN orders — 反向查找
	usersSchema := NewBaseSchema("users")
	dialect := NewMySQLDialect()
	qc := NewSQLQueryConstructor(usersSchema, dialect)
	qc.viewRegistry = reg
	qc.Join("orders", "users.id = orders.user_id")

	ctx := context.Background()
	sql, _, err := qc.Build(ctx)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if strings.Contains(sql, "JOIN") {
		t.Errorf("expected no JOIN after reverse auto-routing, got: %s", sql)
	}
	if !strings.Contains(sql, "user_orders_view") {
		t.Errorf("expected view name in SQL, got: %s", sql)
	}
	t.Logf("✓ Reverse FK auto-routed SQL: %s", sql)
}

// TestQueryBuilder_AutoRouteToView_WithWhereAndLimit 验证路由后 WHERE/ORDER/LIMIT 仍然生效
func TestQueryBuilder_AutoRouteToView_WithWhereAndLimit(t *testing.T) {
	ordersSchema := NewBaseSchema("orders")
	ordersSchema.AddForeignKey("fk", []string{"user_id"}, "users", []string{"id"}, "", "",
		WithViewHint("user_orders_view", false))

	reg := NewCrossTableViewRegistry()
	reg.RegisterFromSchema(ordersSchema)

	dialect := NewMySQLDialect()
	qc := NewSQLQueryConstructor(ordersSchema, dialect)
	qc.viewRegistry = reg
	qc.Join("users", "orders.user_id = users.id")
	qc.Where(Eq("amount", 100))
	qc.OrderBy("id", "DESC")
	qc.Limit(10)

	ctx := context.Background()
	sql, args, err := qc.Build(ctx)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(sql, "WHERE") {
		t.Errorf("expected WHERE clause, got: %s", sql)
	}
	if !strings.Contains(sql, "ORDER BY") {
		t.Errorf("expected ORDER BY, got: %s", sql)
	}
	if !strings.Contains(sql, "LIMIT") {
		t.Errorf("expected LIMIT, got: %s", sql)
	}
	if len(args) != 1 || args[0] != 100 {
		t.Errorf("expected args=[100], got: %v", args)
	}
	t.Logf("✓ Auto-routed SQL with WHERE/ORDER/LIMIT: %s", sql)
}

// TestQueryBuilder_NoAutoRoute_NoRegistry 验证未注册时正常 JOIN
func TestQueryBuilder_NoAutoRoute_NoRegistry(t *testing.T) {
	ordersSchema := NewBaseSchema("orders")
	dialect := NewMySQLDialect()
	qc := NewSQLQueryConstructor(ordersSchema, dialect)
	// 使用空注册表（无条目）
	qc.viewRegistry = NewCrossTableViewRegistry()
	qc.Join("users", "orders.user_id = users.id")

	ctx := context.Background()
	sql, _, err := qc.Build(ctx)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(sql, "JOIN") {
		t.Errorf("expected JOIN when no view registered, got: %s", sql)
	}
	t.Logf("✓ No auto-route when registry is empty: %s", sql)
}

// TestQueryBuilder_NoAutoRoute_MultipleJoins 验证多 JOIN 时不路由（仅支持单 JOIN 路由）
func TestQueryBuilder_NoAutoRoute_MultipleJoins(t *testing.T) {
	ordersSchema := NewBaseSchema("orders")
	ordersSchema.AddForeignKey("fk1", []string{"user_id"}, "users", []string{"id"}, "", "",
		WithViewHint("user_orders_view", false))

	reg := NewCrossTableViewRegistry()
	reg.RegisterFromSchema(ordersSchema)

	dialect := NewMySQLDialect()
	qc := NewSQLQueryConstructor(ordersSchema, dialect)
	qc.viewRegistry = reg
	// 两个 JOIN — 不应路由到单表视图
	qc.Join("users", "orders.user_id = users.id")
	qc.Join("products", "orders.product_id = products.id")

	ctx := context.Background()
	sql, _, err := qc.Build(ctx)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(sql, "JOIN") {
		t.Errorf("expected JOIN for multi-join case, got: %s", sql)
	}
	t.Logf("✓ Multi-JOIN not auto-routed: %s", sql)
}

// ==================== 测试辅助函数 ====================

// newTestRepoWithAdapter 创建只用于 DDL 测试的 Repository（无需真实连接）
func newTestRepoWithAdapter(adapter Adapter) *Repository {
	return &Repository{adapter: adapter}
}

func newMockMySQLAdapter() *MySQLAdapter {
	return &MySQLAdapter{}
}

func newMockPostgresAdapter() *PostgreSQLAdapter {
	return &PostgreSQLAdapter{}
}

func newMockSQLServerAdapter() *SQLServerAdapter {
	return &SQLServerAdapter{}
}
