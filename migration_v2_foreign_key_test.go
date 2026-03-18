package db

import (
	"strings"
	"testing"
)

// --------------- 辅助 schema ---------------

func newOrderItemsSchema() *BaseSchema {
	s := NewBaseSchema("order_items")
	s.AddField(&Field{Name: "order_id", Type: TypeInteger, Null: false})
	s.AddField(&Field{Name: "product_id", Type: TypeInteger, Null: false})
	s.AddField(&Field{Name: "quantity", Type: TypeInteger, Null: false})
	s.AddPrimaryKey("order_id", "product_id")

	// 单列外键
	s.AddForeignKey("fk_order", []string{"order_id"}, "orders", []string{"id"}, "CASCADE", "")
	// 复合外键
	s.AddForeignKey("fk_product", []string{"product_id", "order_id"}, "products", []string{"id", "order_ref"}, "RESTRICT", "CASCADE")
	return s
}

// --------------- 测试 ---------------

// TestForeignKeyDDLPostgres 验证 PostgreSQL 生成带 CONSTRAINT 的 FK DDL
func TestForeignKeyDDLPostgres(t *testing.T) {
	adapter := &PostgreSQLAdapter{}
	repo := &Repository{adapter: adapter}
	schema := newOrderItemsSchema()

	sql := buildCreateTableSQL(repo, schema)

	// 单列 FK
	if !strings.Contains(sql, `CONSTRAINT "fk_order" FOREIGN KEY ("order_id") REFERENCES "orders" ("id") ON DELETE CASCADE`) {
		t.Errorf("expected single-column FK DDL, got:\n%s", sql)
	}

	// 复合 FK
	if !strings.Contains(sql, `CONSTRAINT "fk_product" FOREIGN KEY ("product_id", "order_id") REFERENCES "products" ("id", "order_ref") ON DELETE RESTRICT ON UPDATE CASCADE`) {
		t.Errorf("expected composite FK DDL, got:\n%s", sql)
	}

	// 确保 PostgreSQL 语法：CREATE TABLE IF NOT EXISTS
	if !strings.HasPrefix(sql, `CREATE TABLE IF NOT EXISTS`) {
		t.Errorf("expected PostgreSQL CREATE TABLE prefix, got:\n%s", sql)
	}
}

// TestForeignKeyDDLMySQL 验证 MySQL 生成带反引号的 FK DDL
func TestForeignKeyDDLMySQL(t *testing.T) {
	adapter := &MySQLAdapter{}
	repo := &Repository{adapter: adapter}
	schema := newOrderItemsSchema()

	sql := buildCreateTableSQL(repo, schema)

	// 单列 FK 使用反引号
	if !strings.Contains(sql, "CONSTRAINT `fk_order` FOREIGN KEY (`order_id`) REFERENCES `orders` (`id`) ON DELETE CASCADE") {
		t.Errorf("expected MySQL single-column FK DDL, got:\n%s", sql)
	}

	// 复合 FK
	if !strings.Contains(sql, "CONSTRAINT `fk_product` FOREIGN KEY (`product_id`, `order_id`) REFERENCES `products` (`id`, `order_ref`) ON DELETE RESTRICT ON UPDATE CASCADE") {
		t.Errorf("expected MySQL composite FK DDL, got:\n%s", sql)
	}
}

// TestForeignKeyDDLSQLite 验证 SQLite 生成 FK DDL（SQLite 支持 FK 语法）
func TestForeignKeyDDLSQLite(t *testing.T) {
	adapter := &SQLiteAdapter{}
	repo := &Repository{adapter: adapter}
	s := NewBaseSchema("order_items")
	s.AddField(&Field{Name: "order_id", Type: TypeInteger, Null: false, Primary: true})
	s.AddForeignKey("fk_order", []string{"order_id"}, "orders", []string{"id"}, "CASCADE", "")

	sql := buildCreateTableSQL(repo, s)

	if !strings.Contains(sql, "CONSTRAINT `fk_order` FOREIGN KEY (`order_id`) REFERENCES `orders` (`id`) ON DELETE CASCADE") {
		t.Errorf("expected SQLite FK DDL, got:\n%s", sql)
	}
}

// TestForeignKeyDDLSQLServer 验证 SQL Server 生成带方括号的 FK DDL
func TestForeignKeyDDLSQLServer(t *testing.T) {
	adapter := &SQLServerAdapter{}
	repo := &Repository{adapter: adapter}
	s := NewBaseSchema("order_items")
	s.AddField(&Field{Name: "order_id", Type: TypeInteger, Null: false, Primary: true})
	s.AddForeignKey("fk_order", []string{"order_id"}, "orders", []string{"id"}, "NO ACTION", "")

	sql := buildCreateTableSQL(repo, s)

	if !strings.Contains(sql, "CONSTRAINT [fk_order] FOREIGN KEY ([order_id]) REFERENCES [orders] ([id]) ON DELETE NO ACTION") {
		t.Errorf("expected SQL Server FK DDL, got:\n%s", sql)
	}
}

// TestForeignKeyDDLMongoSkipped 验证 MongoDB 不生成任何 FK DDL
func TestForeignKeyDDLMongoSkipped(t *testing.T) {
	adapter := &MongoAdapter{}
	repo := &Repository{adapter: adapter}
	schema := newOrderItemsSchema()

	sql := buildCreateTableSQL(repo, schema)

	if strings.Contains(sql, "FOREIGN KEY") {
		t.Errorf("expected no FK DDL for MongoDB, got:\n%s", sql)
	}
	if strings.Contains(sql, "REFERENCES") {
		t.Errorf("expected no REFERENCES clause for MongoDB, got:\n%s", sql)
	}
}

// TestForeignKeyFallbackToSingleColumn 验证不支持复合外键时降级为单列 FK
func TestForeignKeyFallbackToSingleColumn(t *testing.T) {
	// 构造一个 SupportsCompositeForeignKeys=false 的适配器
	adapter := &mockNoCompositeFKAdapter{}
	repo := &Repository{adapter: adapter}

	s := NewBaseSchema("items")
	s.AddField(&Field{Name: "a", Type: TypeInteger, Null: false})
	s.AddField(&Field{Name: "b", Type: TypeInteger, Null: false})
	// 声明复合外键 (a, b) → other_table (x, y)
	s.AddForeignKey("fk_ab", []string{"a", "b"}, "other_table", []string{"x", "y"}, "", "")

	sql := buildCreateTableSQL(repo, s)

	if !strings.Contains(sql, "FOREIGN KEY") {
		t.Errorf("expected FK DDL after downgrade, got:\n%s", sql)
	}
	// 降级后只保留第一列
	// 降级后 FOREIGN KEY 子句只保留第一列 a，不包含 b
	if !strings.Contains(sql, "FOREIGN KEY (`a`) REFERENCES `other_table` (`x`)") {
		t.Errorf("expected FK downgraded to single column `a`, got:\n%s", sql)
	}
	// 完整复合外键 (a, b) 不应出现
	if strings.Contains(sql, "FOREIGN KEY (`a`, `b`)") {
		t.Errorf("expected composite FK to be absent after downgrade, got:\n%s", sql)
	}
}

// mockNoCompositeFKAdapter 模拟 SupportsForeignKeys=true 但 SupportsCompositeForeignKeys=false
type mockNoCompositeFKAdapter struct {
	MySQLAdapter
}

func (a *mockNoCompositeFKAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	f := a.MySQLAdapter.GetDatabaseFeatures()
	f.SupportsCompositeForeignKeys = false
	return f
}

// TestForeignKeyNoOnDeleteOrUpdate 验证不设置 ON DELETE/UPDATE 时不生成对应子句
func TestForeignKeyNoOnDeleteOrUpdate(t *testing.T) {
	adapter := &PostgreSQLAdapter{}
	repo := &Repository{adapter: adapter}
	s := NewBaseSchema("t")
	s.AddField(&Field{Name: "ref_id", Type: TypeInteger, Null: false})
	s.AddForeignKey("fk_ref", []string{"ref_id"}, "other", []string{"id"}, "", "")

	sql := buildCreateTableSQL(repo, s)

	if strings.Contains(sql, "ON DELETE") || strings.Contains(sql, "ON UPDATE") {
		t.Errorf("expected no ON DELETE/ON UPDATE when not specified, got:\n%s", sql)
	}
	if !strings.Contains(sql, `CONSTRAINT "fk_ref" FOREIGN KEY ("ref_id") REFERENCES "other" ("id")`) {
		t.Errorf("expected basic FK DDL without actions, got:\n%s", sql)
	}
}

// TestSupportsForeignKeysFlag 验证能力标志正确设置
func TestSupportsForeignKeysFlag(t *testing.T) {
	sqlAdapters := []struct {
		name    string
		adapter Adapter
	}{
		{"postgres", &PostgreSQLAdapter{}},
		{"mysql", &MySQLAdapter{}},
		{"sqlite", &SQLiteAdapter{}},
		{"sqlserver", &SQLServerAdapter{}},
	}

	for _, tc := range sqlAdapters {
		t.Run(tc.name, func(t *testing.T) {
			features := tc.adapter.GetDatabaseFeatures()
			if !features.SupportsForeignKeys {
				t.Errorf("%s: expected SupportsForeignKeys=true", tc.name)
			}
			if !features.SupportsCompositeForeignKeys {
				t.Errorf("%s: expected SupportsCompositeForeignKeys=true", tc.name)
			}
		})
	}

	// MongoDB 不支持外键
	mongoFeatures := NewMongoDatabaseFeatures()
	if mongoFeatures.SupportsForeignKeys {
		t.Error("expected MongoDB SupportsForeignKeys=false")
	}
	if mongoFeatures.SupportsCompositeForeignKeys {
		t.Error("expected MongoDB SupportsCompositeForeignKeys=false")
	}
}
