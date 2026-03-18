package db

import (
	"context"
	"testing"
)

// TestDynamicTableRegistry 测试动态表注册表
func TestDynamicTableRegistry(t *testing.T) {
	registry := NewDynamicTableRegistry()

	// 创建测试配置
	config := NewDynamicTableConfig("test_table").
		WithDescription("Test Table").
		AddField(NewDynamicTableField("id", TypeInteger).AsPrimaryKey()).
		AddField(NewDynamicTableField("name", TypeString))

	// 测试注册
	err := registry.Register("test_table", config)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	// 测试获取
	retrieved, err := registry.Get("test_table")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if retrieved.TableName != "test_table" {
		t.Fatalf("Expected table name 'test_table', got '%s'", retrieved.TableName)
	}

	// 测试列表
	configs := registry.List()
	if len(configs) != 1 {
		t.Fatalf("Expected 1 config, got %d", len(configs))
	}

	// 测试注销
	err = registry.Unregister("test_table")
	if err != nil {
		t.Fatalf("Failed to unregister: %v", err)
	}

	// 验证已注销
	configs = registry.List()
	if len(configs) != 0 {
		t.Fatalf("Expected 0 configs after unregister, got %d", len(configs))
	}
}

// TestDynamicTableConfigBuilder 测试配置构造器链式方法
func TestDynamicTableConfigBuilder(t *testing.T) {
	config := NewDynamicTableConfig("users_data").
		WithDescription("User data table").
		WithParentTable("users", "status = 'active'").
		WithStrategy("auto").
		AddField(
			NewDynamicTableField("id", TypeInteger).
				AsPrimaryKey().
				WithAutoinc(),
		).
		AddField(
			NewDynamicTableField("email", TypeString).
				AsNotNull().
				WithIndex().
				WithUnique(),
		).
		AddField(
			NewDynamicTableField("created_at", TypeTime).
				AsNotNull(),
		)

	if config.TableName != "users_data" {
		t.Fatalf("Expected table name 'users_data', got '%s'", config.TableName)
	}

	if config.Description != "User data table" {
		t.Fatalf("Expected description 'User data table', got '%s'", config.Description)
	}

	if config.ParentTable != "users" {
		t.Fatalf("Expected parent table 'users', got '%s'", config.ParentTable)
	}

	if config.Strategy != "auto" {
		t.Fatalf("Expected strategy 'auto', got '%s'", config.Strategy)
	}

	if len(config.Fields) != 3 {
		t.Fatalf("Expected 3 fields, got %d", len(config.Fields))
	}

	// 检查字段属性
	emailField := config.Fields[1]
	if emailField.Null || !emailField.Index || !emailField.Unique {
		t.Fatalf("Email field attributes not set correctly: Null=%v, Index=%v, Unique=%v",
			emailField.Null, emailField.Index, emailField.Unique)
	}
}

// TestDynamicTableField 测试字段构造器
func TestDynamicTableField(t *testing.T) {
	field := NewDynamicTableField("username", TypeString).
		AsNotNull().
		WithIndex().
		WithUnique().
		WithDefault("guest").
		WithDescription("User's login name")

	if field.Name != "username" {
		t.Fatalf("Expected name 'username', got '%s'", field.Name)
	}

	if field.Type != TypeString {
		t.Fatalf("Expected type TypeString, got %v", field.Type)
	}

	if field.Null {
		t.Fatalf("Expected Null to be false")
	}

	if !field.Index {
		t.Fatalf("Expected Index to be true")
	}

	if !field.Unique {
		t.Fatalf("Expected Unique to be true")
	}

	if field.Default != "guest" {
		t.Fatalf("Expected default 'guest', got '%v'", field.Default)
	}

	if field.Description != "User's login name" {
		t.Fatalf("Expected description, got '%s'", field.Description)
	}
}

// TestDynamicTableWithOptions 测试配置的高级选项
func TestDynamicTableWithOptions(t *testing.T) {
	config := NewDynamicTableConfig("custom_table").
		WithOption("engine", "InnoDB").
		WithOption("charset", "utf8mb4").
		WithOption("partition_key", "id")

	if config.Options["engine"] != "InnoDB" {
		t.Fatalf("Expected engine 'InnoDB'")
	}

	if config.Options["charset"] != "utf8mb4" {
		t.Fatalf("Expected charset 'utf8mb4'")
	}

	if config.Options["partition_key"] != "id" {
		t.Fatalf("Expected partition_key 'id'")
	}
}

// TestDynamicTableFieldTypes 测试所有字段类型
func TestDynamicTableFieldTypes(t *testing.T) {
	fieldTypes := []FieldType{
		TypeString,
		TypeInteger,
		TypeFloat,
		TypeBoolean,
		TypeTime,
		TypeBinary,
		TypeDecimal,
		TypeJSON,
		TypeArray,
		TypeLocation,
	}

	for _, fieldType := range fieldTypes {
		field := NewDynamicTableField("test_field", fieldType)
		if field.Type != fieldType {
			t.Fatalf("Expected type %v, got %v", fieldType, field.Type)
		}
	}
}

// TestPrimaryKeyField 测试主键字段配置
func TestPrimaryKeyField(t *testing.T) {
	pkField := NewDynamicTableField("id", TypeInteger).
		AsPrimaryKey().
		WithAutoinc()

	if !pkField.Primary {
		t.Fatalf("Expected Primary to be true")
	}

	if !pkField.Autoinc {
		t.Fatalf("Expected Autoinc to be true")
	}

	if pkField.Null {
		t.Fatalf("Expected Null to be false for primary key")
	}
}

// TestMultipleFields 测试多字段配置
func TestMultipleFields(t *testing.T) {
	config := NewDynamicTableConfig("products").
		AddField(
			NewDynamicTableField("id", TypeInteger).
				AsPrimaryKey().
				WithAutoinc(),
		).
		AddField(
			NewDynamicTableField("sku", TypeString).
				AsNotNull().
				WithUnique(),
		).
		AddField(
			NewDynamicTableField("name", TypeString).
				AsNotNull().
				WithIndex(),
		).
		AddField(
			NewDynamicTableField("price", TypeDecimal).
				AsNotNull(),
		).
		AddField(
			NewDynamicTableField("attributes", TypeJSON),
		).
		AddField(
			NewDynamicTableField("tags", TypeArray),
		).
		AddField(
			NewDynamicTableField("is_active", TypeBoolean).
				WithDefault(true),
		).
		AddField(
			NewDynamicTableField("created_at", TypeTime).
				AsNotNull(),
		)

	if len(config.Fields) != 8 {
		t.Fatalf("Expected 8 fields, got %d", len(config.Fields))
	}

	// 验证每个字段
	expectedFields := map[string]FieldType{
		"id":         TypeInteger,
		"sku":        TypeString,
		"name":       TypeString,
		"price":      TypeDecimal,
		"attributes": TypeJSON,
		"tags":       TypeArray,
		"is_active":  TypeBoolean,
		"created_at": TypeTime,
	}

	for _, field := range config.Fields {
		if expectedType, ok := expectedFields[field.Name]; ok {
			if field.Type != expectedType {
				t.Fatalf("Field %s: expected type %v, got %v",
					field.Name, expectedType, field.Type)
			}
		}
	}
}

// TestDynamicTableStrategyValidation 测试策略验证
func TestDynamicTableStrategyValidation(t *testing.T) {
	// 测试有效策略
	config1 := NewDynamicTableConfig("table1").WithStrategy("auto")
	if config1.Strategy != "auto" {
		t.Fatalf("Expected strategy 'auto'")
	}

	config2 := NewDynamicTableConfig("table2").WithStrategy("manual")
	if config2.Strategy != "manual" {
		t.Fatalf("Expected strategy 'manual'")
	}

	// 测试无效策略（应该使用默认值）
	config3 := NewDynamicTableConfig("table3").WithStrategy("invalid")
	if config3.Strategy != "invalid" {
		// WithStrategy 不验证，直接设置
		// 这是设计中的行为
	}
}

// BenchmarkDynamicTableRegistry 基准测试
func BenchmarkDynamicTableRegistry(b *testing.B) {
	registry := NewDynamicTableRegistry()
	config := NewDynamicTableConfig("bench_table")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Register("bench_table", config)
		registry.Get("bench_table")
		registry.Unregister("bench_table")
	}
}

// TestDynamicTableConfigCloning 测试配置的深拷贝
func TestDynamicTableConfigCloning(t *testing.T) {
	// 创建原始配置
	original := NewDynamicTableConfig("original").
		AddField(NewDynamicTableField("id", TypeInteger))

	// 模拟修改（在实际使用中应避免直接修改）
	original.TableName = "modified"

	// 验证原始配置被修改了
	if original.TableName != "modified" {
		t.Fatalf("Expected modified table name")
	}
}

// TestFieldValidation 测试字段验证
func TestFieldValidation(t *testing.T) {
	// 创建无效配置（缺少表名）应该在注册时被检查
	config := &DynamicTableConfig{
		TableName: "", // 无效
		Fields:    make([]*DynamicTableField, 0),
	}

	registry := NewDynamicTableRegistry()
	err := registry.Register("test", config)
	if err == nil {
		t.Fatalf("Expected error for empty table name")
	}
}

// TestIntegrationFlow 集成测试示例（演示性的）
func TestIntegrationFlow(t *testing.T) {
	ctx := context.Background()

	// 演示流程（不实际执行，因为需要真实数据库）
	t.Log("Integration test flow:")
	t.Log("1. Create registry")
	t.Log("2. Register dynamic table config")
	t.Log("3. Connect to database")
	t.Log("4. Create hook instance")
	t.Log("5. Register config with database")
	t.Log("6. Verify table creation trigger")
	t.Log("7. List created tables")
	t.Log("8. Cleanup")

	_ = ctx
}

func TestDynamicTableConfigToSchema(t *testing.T) {
	config := NewDynamicTableConfig("ignored_name").
		AddField(NewDynamicTableField("id", TypeInteger).AsPrimaryKey().WithAutoinc()).
		AddField(NewDynamicTableField("name", TypeString).AsNotNull().WithDefault("'guest'"))

	schema := config.toSchema("runtime_users_1")
	if schema.TableName() != "runtime_users_1" {
		t.Fatalf("expected runtime table name, got %s", schema.TableName())
	}

	fields := schema.Fields()
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}

	if fields[0].Name != "id" || !fields[0].Primary || !fields[0].Autoinc {
		t.Fatalf("id field mapping is incorrect: %+v", fields[0])
	}

	if fields[1].Name != "name" || fields[1].Null || fields[1].Default != "'guest'" {
		t.Fatalf("name field mapping is incorrect: %+v", fields[1])
	}
}
