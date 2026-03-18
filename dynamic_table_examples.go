package db

import (
	"context"
	"fmt"
)

// 此文件包含动态建表功能的使用示例

// ExamplePostgreSQLDynamicTable PostgreSQL 动态建表示例
// 场景：在 CMS 系统中，当创建自定义字段时，自动为该自定义字段创建专属表存储字段数据
func ExamplePostgreSQLDynamicTable() {
	/*
		// 1. 创建动态表配置
		// 每个项目创建时自动创建一个专属表来存储该项目的自定义字段值
		customFieldConfig := NewDynamicTableConfig("project_custom_fields").
			WithDescription("动态存储项目的自定义字段值").
			WithParentTable("projects", "type = 'advanced'"). // 只有高级项目才创建
			WithStrategy("auto").
			AddField(
				NewDynamicTableField("id", TypeInteger).
					AsPrimaryKey().
					WithAutoinc(),
			).
			AddField(
				NewDynamicTableField("field_name", TypeString).AsNotNull(),
			).
			AddField(
				NewDynamicTableField("field_value", TypeJSON).AsNotNull(),
			).
			AddField(
				NewDynamicTableField("created_at", TypeTime).AsNotNull(),
			)

		// 2. 初始化仓储和动态表 hook
		repo, err := NewRepository(ctx, &Config{
			Adapter: "postgres",
			Host:    "localhost",
			Port:    5432,
			Username: "postgres",
			Password: "password",
			Database: "myapp",
		})
		if err != nil {
			panic(err)
		}
		defer repo.Close()

		// 3. 为 PostgreSQL 创建和配置动态表 hook
		pgAdapter := repo.adapter.(*PostgreSQLAdapter)
		hook := NewPostgreSQLDynamicTableHook(pgAdapter)

		// 4. 注册动态表配置
		// 这将创建触发器和存储函数
		if err := hook.RegisterDynamicTable(ctx, customFieldConfig); err != nil {
			panic(err)
		}

		// 5. 之后，每当向 projects 表插入 type='advanced' 的记录时，
		// 触发器会自动执行函数创建对应的表：
		// - project_custom_fields_1
		// - project_custom_fields_2
		// 等等

		// 6. 如需手动创建（strategy = manual），可以使用：
		tableName, err := hook.CreateDynamicTable(ctx, "project_custom_fields", map[string]interface{}{
			"id": 999,
		})
		if err != nil {
			panic(err)
		}
		// 返回的 tableName = "project_custom_fields_999"

		// 7. 列出已创建的动态表
		tables, err := hook.ListCreatedDynamicTables(ctx, "project_custom_fields")
		if err != nil {
			panic(err)
		}
		// tables = ["project_custom_fields_1", "project_custom_fields_2", ...]

		// 8. 注销配置（删除触发器）
		if err := hook.UnregisterDynamicTable(ctx, "project_custom_fields"); err != nil {
			panic(err)
		}
	*/
}

// ExampleMySQLDynamicTable MySQL 动态建表示例
// 场景：在电商系统中，为每个店铺创建独立的订单表来分离数据
func ExampleMySQLDynamicTable() {
	/*
		// 1. 定义店铺订单表的配置
		shopOrderConfig := NewDynamicTableConfig("shop_orders").
			WithDescription("为每个店铺创建独立的订单表").
			WithParentTable("shops", "status = 'active'"). // 只有活跃店铺创建
			WithStrategy("auto").
			AddField(
				NewDynamicTableField("id", TypeInteger).
					AsPrimaryKey().
					WithAutoinc(),
			).
			AddField(
				NewDynamicTableField("order_id", TypeString).
					AsNotNull().
					WithIndex(),
			).
			AddField(
				NewDynamicTableField("customer_name", TypeString).AsNotNull(),
			).
			AddField(
				NewDynamicTableField("total_amount", TypeDecimal).AsNotNull(),
			).
			AddField(
				NewDynamicTableField("status", TypeString).AsNotNull().
					WithDefault("pending"),
			).
			AddField(
				NewDynamicTableField("order_data", TypeJSON),
			).
			AddField(
				NewDynamicTableField("created_at", TypeTime).AsNotNull(),
			)

		// 2. 初始化 MySQL 仓储
		repo, err := NewRepository(ctx, &Config{
			Adapter: "mysql",
			Host:    "localhost",
			Port:    3306,
			Username: "root",
			Password: "password",
			Database: "myshop",
		})
		if err != nil {
			panic(err)
		}
		defer repo.Close()

		// 3. 创建 MySQL 动态表 hook
		mysqlAdapter := repo.adapter.(*MySQLAdapter)
		hook := NewMySQLDynamicTableHook(mysqlAdapter)

		// 4. 注册配置
		if err := hook.RegisterDynamicTable(ctx, shopOrderConfig); err != nil {
			panic(err)
		}

		// 5. 通过 GORM hook 自动触发
		// 当向 shops 表插入新店铺时，hook 会自动在 AfterCreate 中创建对应的表：
		// - shop_orders_1
		// - shop_orders_2
		// ...

		// 6. 列出店铺 1 的所有订单表
		tables, err := hook.ListCreatedDynamicTables(ctx, "shop_orders")
		if err != nil {
			panic(err)
		}
		// tables = ["shop_orders_1", "shop_orders_2", ...]
	*/
}

// ExampleSQLiteDynamicTable SQLite 动态建表示例
// 场景：在日志系统中，为每个应用创建独立的日志表
func ExampleSQLiteDynamicTable() {
	/*
		// 1. 定义日志表配置
		logConfig := NewDynamicTableConfig("app_logs").
			WithDescription("为每个应用创建独立的日志表").
			WithParentTable("applications", "log_enabled = 1").
			WithStrategy("auto").
			AddField(
				NewDynamicTableField("id", TypeInteger).
					AsPrimaryKey().
					WithAutoinc(),
			).
			AddField(
				NewDynamicTableField("level", TypeString).AsNotNull(),
			).
			AddField(
				NewDynamicTableField("message", TypeString).AsNotNull(),
			).
			AddField(
				NewDynamicTableField("context", TypeJSON),
			).
			AddField(
				NewDynamicTableField("created_at", TypeTime).AsNotNull(),
			)

		// 2. 初始化 SQLite 仓储
		repo, err := NewRepository(ctx, &Config{
			Adapter:  "sqlite",
			Database: "./data/app.db",
		})
		if err != nil {
			panic(err)
		}
		defer repo.Close()

		// 3. 创建 SQLite 动态表 hook
		sqliteAdapter := repo.adapter.(*SQLiteAdapter)
		hook := NewSQLiteDynamicTableHook(sqliteAdapter)

		// 4. 注册配置
		if err := hook.RegisterDynamicTable(ctx, logConfig); err != nil {
			panic(err)
		}

		// 5. 自动触发创建表
		// 当向 applications 表插入 log_enabled = 1 的记录时：
		// - app_logs_1
		// - app_logs_2
		// ...

		// 6. 手动创建特定应用的日志表
		tableName, err := hook.CreateDynamicTable(ctx, "app_logs", map[string]interface{}{
			"id": 100,
		})
		if err != nil {
			panic(err)
		}
		// tableName = "app_logs_100"
	*/
}

// ExampleSQLServerDynamicTable SQL Server 动态建表示例
// 场景：在企业系统中，为每个业务单元创建独立的审计日志表
func ExampleSQLServerDynamicTable() {
	/*
		// 1. 定义审计日志表配置
		auditConfig := NewDynamicTableConfig("biz_audit_logs").
			WithDescription("为每个业务单元创建独立审计日志表").
			WithParentTable("business_units", "enabled = 1").
			WithStrategy("auto").
			AddField(
				NewDynamicTableField("id", TypeInteger).
					AsPrimaryKey().
					WithAutoinc(),
			).
			AddField(
				NewDynamicTableField("event_type", TypeString).AsNotNull(),
			).
			AddField(
				NewDynamicTableField("payload", TypeJSON).AsNotNull(),
			).
			AddField(
				NewDynamicTableField("location", TypeLocation),
			).
			AddField(
				NewDynamicTableField("created_at", TypeTime).AsNotNull(),
			)

		// 2. 初始化 SQL Server 仓储
		repo, err := NewRepository(ctx, &Config{
			Adapter:  "sqlserver",
			Host:     "localhost",
			Port:     1433,
			Username: "sa",
			Password: "password",
			Database: "enterprise",
		})
		if err != nil {
			panic(err)
		}
		defer repo.Close()

		// 3. 创建 SQL Server 动态表 hook（触发器 + T-SQL 原生方案）
		sqlServerAdapter := repo.adapter.(*SQLServerAdapter)
		hook := NewSQLServerDynamicTableHook(sqlServerAdapter)

		// 4. 注册配置
		if err := hook.RegisterDynamicTable(ctx, auditConfig); err != nil {
			panic(err)
		}

		_ = auditConfig
	*/
}

// 实际使用示例

// RealWorldExample 真实业务场景示例
// 场景：SaaS CMS 系统 - 为每个客户项目创建独立的内容表
func RealWorldExample(repo *Repository, ctx context.Context) error {
	// 假设我们有 projects 表，当创建项目时，需要为该项目创建一个内容表

	// 第1步：定义内容表配置
	contentTableConfig := NewDynamicTableConfig("project_contents").
		WithDescription("为每个项目存储内容数据").
		WithParentTable("projects", "").
		WithStrategy("auto").
		AddField(
			NewDynamicTableField("id", TypeInteger).
				AsPrimaryKey().
				WithAutoinc(),
		).
		AddField(
			NewDynamicTableField("title", TypeString).
				AsNotNull().
				WithIndex(),
		).
		AddField(
			NewDynamicTableField("slug", TypeString).
				AsNotNull().
				WithUnique(),
		).
		AddField(
			NewDynamicTableField("content", TypeString).AsNotNull(),
		).
		AddField(
			NewDynamicTableField("meta", TypeJSON),
		).
		AddField(
			NewDynamicTableField("published", TypeBoolean).
				WithDefault(false),
		).
		AddField(
			NewDynamicTableField("created_at", TypeTime).AsNotNull(),
		).
		AddField(
			NewDynamicTableField("updated_at", TypeTime).AsNotNull(),
		)

	// 第2步：获取适配器类型并创建对应的 hook
	var hook DynamicTableHook

	switch adapter := repo.adapter.(type) {
	case *PostgreSQLAdapter:
		hook = NewPostgreSQLDynamicTableHook(adapter)
	case *MySQLAdapter:
		hook = NewMySQLDynamicTableHook(adapter)
	case *SQLiteAdapter:
		hook = NewSQLiteDynamicTableHook(adapter)
	case *SQLServerAdapter:
		hook = NewSQLServerDynamicTableHook(adapter)
	default:
		return fmt.Errorf("unsupported adapter type: %T", repo.adapter)
	}

	// 第3步：注册配置
	if err := hook.RegisterDynamicTable(ctx, contentTableConfig); err != nil {
		return fmt.Errorf("failed to register dynamic table: %w", err)
	}

	// 第4步：现在每当插入新项目时，对应的表会自动创建
	// project_contents_1（项目ID为1）
	// project_contents_2（项目ID为2）
	// ...

	// 第5步：查询已创建的表
	_, _ = hook.ListCreatedDynamicTables(ctx, "project_contents")

	// 第6步：向动态表中插入内容
	// 使用 Repository.Exec 执行标准 SQL，而不是直接暴露 ORM 连接
	if _, err := repo.Exec(ctx,
		"INSERT INTO project_contents_1 (title, slug, content, published, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		"Welcome to Project 1",
		"welcome-to-project-1",
		"This is the welcome content...",
		true,
		"2024-01-01 00:00:00",
		"2024-01-01 00:00:00",
	); err != nil {
		return err
	}

	return nil
}
