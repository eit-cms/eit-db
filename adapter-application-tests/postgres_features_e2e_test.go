package adapter_tests

import (
	"context"
	"strings"
	"testing"

	db "github.com/eit-cms/eit-db"
)

// getPgFeatures 从 repo 中提取 PostgreSQLFeatures，不可用则 skip。
func getPgFeatures(t *testing.T, repo *db.Repository) *db.PostgreSQLFeatures {
	t.Helper()
	features, ok := db.GetPostgreSQLFeatures(repo.GetAdapter())
	if !ok {
		t.Skip("无法获取 PostgreSQLFeatures，跳过测试")
		return nil
	}
	return features
}

// ==================== ENUM 类型 E2E ====================

// TestPostgresEnumType_CreateAndDrop 验证 ENUM 类型的完整生命周期
func TestPostgresEnumType_CreateAndDrop(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	features := getPgFeatures(t, repo)
	ctx := context.Background()

	typeName := "test_order_status"
	enumBuilder := features.EnumType(typeName).
		Values("pending", "processing", "shipped", "delivered", "cancelled").
		IfNotExists()

	// 清理旧残留
	_ = enumBuilder.Drop(ctx, true)

	// 创建
	if err := enumBuilder.Create(ctx); err != nil {
		t.Fatalf("Create ENUM 失败: %v", err)
	}
	defer func() { _ = enumBuilder.Drop(ctx, true) }()

	// 验证存在
	exists, err := enumBuilder.Exists(ctx)
	if err != nil {
		t.Fatalf("Exists() 失败: %v", err)
	}
	if !exists {
		t.Fatal("ENUM 类型创建后 Exists() 应返回 true")
	}

	// IfNotExists 幂等：二次调用不报错
	if err := enumBuilder.Create(ctx); err != nil {
		t.Fatalf("IfNotExists 时二次 Create 不应报错: %v", err)
	}

	// 使用 ENUM 类型创建表，并插入数据
	_, err = repo.Exec(ctx, `CREATE TEMP TABLE test_orders (
		id SERIAL PRIMARY KEY,
		status test_order_status NOT NULL
	)`)
	if err != nil {
		t.Fatalf("创建使用 ENUM 的表失败: %v", err)
	}

	_, err = repo.Exec(ctx, `INSERT INTO test_orders (status) VALUES ('pending')`)
	if err != nil {
		t.Fatalf("插入合法 ENUM 值失败: %v", err)
	}

	// 插入非法 ENUM 值应报错
	_, err = repo.Exec(ctx, `INSERT INTO test_orders (status) VALUES ('invalid_status')`)
	if err == nil {
		t.Fatal("插入非法 ENUM 值应该报错，但没有")
	}

	// DROP
	if err := enumBuilder.Drop(ctx, true); err != nil {
		t.Fatalf("Drop ENUM 失败: %v", err)
	}
	exists, _ = enumBuilder.Exists(ctx)
	if exists {
		t.Fatal("Drop 后 Exists() 应返回 false")
	}

	t.Logf("✓ ENUM 类型 %q 完整生命周期测试通过", typeName)
}

// TestPostgresEnumType_BuildOnlyOutput 验证 ENUM DDL 字符串内容
func TestPostgresEnumType_BuildOnlyOutput(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	features := getPgFeatures(t, repo)

	ddl, err := features.EnumType("mood").Values("happy", "sad", "ok").Build()
	if err != nil {
		t.Fatalf("Build() 失败: %v", err)
	}

	expected := []string{`CREATE TYPE "mood" AS ENUM`, `'happy'`, `'sad'`, `'ok'`}
	for _, s := range expected {
		if !strings.Contains(ddl, s) {
			t.Errorf("期望 DDL 包含 %q，实际:\n%s", s, ddl)
		}
	}
	t.Logf("✓ ENUM DDL:\n%s", ddl)
}

// ==================== DOMAIN 类型 E2E ====================

// TestPostgresDomainType_CreateAndDrop 验证 DOMAIN 类型完整生命周期
func TestPostgresDomainType_CreateAndDrop(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	features := getPgFeatures(t, repo)
	ctx := context.Background()

	domainBuilder := features.DomainType("test_positive_int").
		BaseType("INTEGER").
		NotNull().
		CheckNamed("must_be_positive", "VALUE > 0").
		IfNotExists()

	// 清理旧残留
	_ = domainBuilder.Drop(ctx, true)

	if err := domainBuilder.Create(ctx); err != nil {
		t.Fatalf("Create DOMAIN 失败: %v", err)
	}
	defer func() { _ = domainBuilder.Drop(ctx, true) }()

	// 验证存在
	exists, err := domainBuilder.Exists(ctx)
	if err != nil {
		t.Fatalf("Exists() 失败: %v", err)
	}
	if !exists {
		t.Fatal("DOMAIN 创建后 Exists() 应返回 true")
	}

	// 用 DOMAIN 创建表
	_, err = repo.Exec(ctx, `CREATE TEMP TABLE test_ratings (
		id    SERIAL PRIMARY KEY,
		score test_positive_int
	)`)
	if err != nil {
		t.Fatalf("创建使用 DOMAIN 的表失败: %v", err)
	}

	// 插入合法值
	_, err = repo.Exec(ctx, `INSERT INTO test_ratings (score) VALUES (95)`)
	if err != nil {
		t.Fatalf("插入合法 DOMAIN 值失败: %v", err)
	}

	// 插入违反约束的值应失败
	_, err = repo.Exec(ctx, `INSERT INTO test_ratings (score) VALUES (-1)`)
	if err == nil {
		t.Fatal("插入负数到 positive_int domain 应该报错")
	}

	// 插入 NULL 到 NOT NULL domain 应失败
	_, err = repo.Exec(ctx, `INSERT INTO test_ratings (score) VALUES (NULL)`)
	if err == nil {
		t.Fatal("插入 NULL 到 NOT NULL domain 应该报错")
	}

	t.Logf("✓ DOMAIN 类型 test_positive_int 完整生命周期测试通过")
}

// TestPostgresDomainType_EmailValidation 验证 email DOMAIN 的正则约束
func TestPostgresDomainType_EmailValidation(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	features := getPgFeatures(t, repo)
	ctx := context.Background()

	emailDomain := features.DomainType("test_email_addr").
		BaseType("TEXT").
		NotNull().
		Check(`VALUE ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$'`).
		IfNotExists()

	_ = emailDomain.Drop(ctx, true)
	if err := emailDomain.Create(ctx); err != nil {
		t.Fatalf("Create email DOMAIN 失败: %v", err)
	}
	defer func() { _ = emailDomain.Drop(ctx, true) }()

	_, err := repo.Exec(ctx, `CREATE TEMP TABLE test_contacts (
		id    SERIAL PRIMARY KEY,
		email test_email_addr
	)`)
	if err != nil {
		t.Fatalf("创建 contacts 表失败: %v", err)
	}

	// 合法 email
	_, err = repo.Exec(ctx, `INSERT INTO test_contacts (email) VALUES ('user@example.com')`)
	if err != nil {
		t.Fatalf("插入合法 email 失败: %v", err)
	}

	// 非法 email（无@）
	_, err = repo.Exec(ctx, `INSERT INTO test_contacts (email) VALUES ('not-an-email')`)
	if err == nil {
		t.Fatal("插入非法 email 应该报错")
	}

	t.Logf("✓ email DOMAIN 验证通过")
}

// ==================== COMPOSITE 类型 E2E ====================

// TestPostgresCompositeType_CreateAndDrop 验证 COMPOSITE 类型完整生命周期
func TestPostgresCompositeType_CreateAndDrop(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	features := getPgFeatures(t, repo)
	ctx := context.Background()

	compositeBuilder := features.CompositeType("test_address").
		Field("street", "TEXT").
		Field("city", "VARCHAR(100)").
		Field("zip", "CHAR(6)").
		IfNotExists()

	_ = compositeBuilder.Drop(ctx, true)
	if err := compositeBuilder.Create(ctx); err != nil {
		t.Fatalf("Create COMPOSITE 类型失败: %v", err)
	}
	defer func() { _ = compositeBuilder.Drop(ctx, true) }()

	exists, err := compositeBuilder.Exists(ctx)
	if err != nil {
		t.Fatalf("Exists() 失败: %v", err)
	}
	if !exists {
		t.Fatal("COMPOSITE 类型创建后 Exists() 应返回 true")
	}

	// 验证 pg_type 中确实是 composite 类型（typtype = 'c'）
	var typtype string
	row := repo.QueryRow(ctx,
		`SELECT t.typtype FROM pg_type t
		 JOIN pg_namespace n ON n.oid = t.typnamespace
		 WHERE t.typname = $1 AND n.nspname = 'public'`, "test_address")
	if err := row.Scan(&typtype); err != nil {
		t.Fatalf("查询 pg_type 失败: %v", err)
	}
	if typtype != "c" {
		t.Fatalf("期望 typtype='c'（composite），得到 %q", typtype)
	}

	// 使用 COMPOSITE 类型的表
	_, err = repo.Exec(ctx, `CREATE TEMP TABLE test_locations (
		id      SERIAL PRIMARY KEY,
		addr    test_address
	)`)
	if err != nil {
		t.Fatalf("创建使用 COMPOSITE 类型的表失败: %v", err)
	}

	// 插入结构体字面量
	_, err = repo.Exec(ctx, `INSERT INTO test_locations (addr) VALUES (ROW('123 Main St', 'Springfield', '62701'))`)
	if err != nil {
		t.Fatalf("插入 COMPOSITE 值失败: %v", err)
	}

	// 读取并验证字段
	rows, err := repo.Query(ctx, `SELECT (addr).street, (addr).city FROM test_locations`)
	if err != nil {
		t.Fatalf("查询 COMPOSITE 字段失败: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var street, city string
		if err := rows.Scan(&street, &city); err != nil {
			t.Fatalf("Scan 失败: %v", err)
		}
		if street != "123 Main St" {
			t.Errorf("期望 street='123 Main St'，得到 %q", street)
		}
		if city != "Springfield" {
			t.Errorf("期望 city='Springfield'，得到 %q", city)
		}
	} else {
		t.Fatal("期望至少一行结果")
	}

	t.Logf("✓ COMPOSITE 类型 test_address 完整生命周期测试通过")
}

// TestPostgresCompositeType_AddField 验证 ALTER TYPE ... ADD ATTRIBUTE
func TestPostgresCompositeType_AddField(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	features := getPgFeatures(t, repo)
	ctx := context.Background()

	compositeBuilder := features.CompositeType("test_point2d").
		Field("x", "DOUBLE PRECISION").
		Field("y", "DOUBLE PRECISION").
		IfNotExists()

	_ = compositeBuilder.Drop(ctx, true)
	if err := compositeBuilder.Create(ctx); err != nil {
		t.Fatalf("Create COMPOSITE 失败: %v", err)
	}
	defer func() { _ = compositeBuilder.Drop(ctx, true) }()

	// 动态追加字段 z（升维为 3D）
	if err := compositeBuilder.AddField(ctx, "z", "DOUBLE PRECISION"); err != nil {
		t.Fatalf("AddField z 失败: %v", err)
	}

	// 验证 z 属性已存在（查 pg_attribute）
	var attCount int
	row := repo.QueryRow(ctx,
		`SELECT count(*) FROM pg_attribute a
		 JOIN pg_type t ON t.typrelid = a.attrelid
		 JOIN pg_namespace n ON n.oid = t.typnamespace
		 WHERE t.typname = 'test_point2d' AND n.nspname = 'public' AND a.attname = 'z' AND a.attnum > 0`)
	if err := row.Scan(&attCount); err != nil {
		t.Fatalf("查询 pg_attribute 失败: %v", err)
	}
	if attCount == 0 {
		t.Fatal("AddField 后 z 属性应出现在 pg_attribute 中")
	}

	t.Logf("✓ COMPOSITE 动态 AddField 测试通过")
}

// ==================== VIEW / MATERIALIZED VIEW E2E ====================

func TestPostgresView_CreateOrReplace(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	features := getPgFeatures(t, repo)
	ctx := context.Background()

	_, err := repo.Exec(ctx, `CREATE TEMP TABLE test_view_users (
		id INT PRIMARY KEY,
		name TEXT NOT NULL,
		active BOOLEAN NOT NULL
	)`)
	if err != nil {
		t.Fatalf("创建临时表失败: %v", err)
	}

	_, err = repo.Exec(ctx, `INSERT INTO test_view_users (id, name, active) VALUES
		(1, 'Alice', true),
		(2, 'Bob', false),
		(3, 'Carol', true)`)
	if err != nil {
		t.Fatalf("插入视图测试数据失败: %v", err)
	}

	v := features.View("test_v_active_users").
		As("SELECT id, name FROM test_view_users WHERE active = true")

	_ = v.Drop(ctx)
	if err := v.Create(ctx); err != nil {
		t.Fatalf("创建普通视图失败: %v", err)
	}
	defer func() { _ = v.Drop(ctx) }()

	var cnt int
	row := repo.QueryRow(ctx, `SELECT COUNT(*) FROM test_v_active_users`)
	if err := row.Scan(&cnt); err != nil {
		t.Fatalf("查询视图失败: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("期望 active 用户 2 行，得到 %d", cnt)
	}

	// CreateOrReplace 更新定义后，结果应改变。
	if err := v.As("SELECT id, name FROM test_view_users WHERE active = false").Create(ctx); err != nil {
		t.Fatalf("更新视图定义失败: %v", err)
	}
	row = repo.QueryRow(ctx, `SELECT COUNT(*) FROM test_v_active_users`)
	if err := row.Scan(&cnt); err != nil {
		t.Fatalf("查询更新后的视图失败: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("期望 inactive 用户 1 行，得到 %d", cnt)
	}
}

func TestPostgresMaterializedView_CreateAndRefresh(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	features := getPgFeatures(t, repo)
	ctx := context.Background()

	_, _ = repo.Exec(ctx, `DROP MATERIALIZED VIEW IF EXISTS test_mv_status_count`)
	_, _ = repo.Exec(ctx, `DROP TABLE IF EXISTS test_mv_orders`)

	_, err := repo.Exec(ctx, `CREATE TABLE test_mv_orders (
		id BIGINT PRIMARY KEY,
		status TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("创建订单表失败: %v", err)
	}
	defer func() { _, _ = repo.Exec(ctx, `DROP TABLE IF EXISTS test_mv_orders`) }()

	_, err = repo.Exec(ctx, `INSERT INTO test_mv_orders (id, status) VALUES
		(1, 'paid'),
		(2, 'pending')`)
	if err != nil {
		t.Fatalf("插入订单数据失败: %v", err)
	}

	mv := features.MaterializedView("test_mv_status_count").
		As("SELECT status, COUNT(*) AS cnt FROM test_mv_orders GROUP BY status")

	_ = mv.Drop(ctx)
	if err := mv.Create(ctx); err != nil {
		t.Fatalf("创建物化视图失败: %v", err)
	}
	defer func() { _ = mv.Drop(ctx) }()

	var paidCnt int
	row := repo.QueryRow(ctx, `SELECT cnt FROM test_mv_status_count WHERE status = 'paid'`)
	if err := row.Scan(&paidCnt); err != nil {
		t.Fatalf("查询物化视图失败: %v", err)
	}
	if paidCnt != 1 {
		t.Fatalf("期望 paid=1，得到 %d", paidCnt)
	}

	// 插入新数据后，物化视图不会自动变化，需要 Refresh。
	_, err = repo.Exec(ctx, `INSERT INTO test_mv_orders (id, status) VALUES (3, 'paid')`)
	if err != nil {
		t.Fatalf("插入增量订单失败: %v", err)
	}

	row = repo.QueryRow(ctx, `SELECT cnt FROM test_mv_status_count WHERE status = 'paid'`)
	if err := row.Scan(&paidCnt); err != nil {
		t.Fatalf("刷新前查询物化视图失败: %v", err)
	}
	if paidCnt != 1 {
		t.Fatalf("刷新前期望 paid=1，得到 %d", paidCnt)
	}

	if err := mv.Refresh(ctx); err != nil {
		t.Fatalf("刷新物化视图失败: %v", err)
	}

	row = repo.QueryRow(ctx, `SELECT cnt FROM test_mv_status_count WHERE status = 'paid'`)
	if err := row.Scan(&paidCnt); err != nil {
		t.Fatalf("刷新后查询物化视图失败: %v", err)
	}
	if paidCnt != 2 {
		t.Fatalf("刷新后期望 paid=2，得到 %d", paidCnt)
	}
}
