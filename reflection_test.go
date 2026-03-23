package db

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"
)

// TestStruct 测试用的结构体
type TestUser struct {
	ID        int    `eit_db:"id,primary_key,auto_increment"`
	Username  string `eit_db:"username,not_null,unique"`
	Email     string `eit_db:"email,not_null,unique"`
	Age       *int   `eit_db:"age"` // 指针表示可空
	IsActive  bool   `eit_db:"is_active,not_null"`
	CreatedAt string `eit_db:"created_at"`
}

type LegacyTaggedUser struct {
	ID   int    `db:"id,primary_key"`
	Name string `db:"name,not_null"`
}

type GormAndEITUser struct {
	ID      int    `gorm:"column:id;primaryKey;autoIncrement" eit_db:"id,primary_key,auto_increment"`
	Status  string `gorm:"column:status;not null;default:active" eit_db:"status,not_null,default=enabled"`
	Profile string `gorm:"column:profile" eit_db:"profile,type=json"`
}

// TestInferSchema 测试从结构体推导 Schema
func TestInferSchema(t *testing.T) {
	schema, err := InferSchema(TestUser{})
	if err != nil {
		t.Fatalf("InferSchema failed: %v", err)
	}

	// 验证表名
	if schema.TableName() != "test_user" {
		t.Errorf("Expected table name 'test_user', got '%s'", schema.TableName())
	}

	// 验证字段数量
	fields := schema.Fields()
	if len(fields) != 6 {
		t.Errorf("Expected 6 fields, got %d", len(fields))
	}

	// 验证 ID 字段
	idField := schema.GetField("id")
	if idField == nil {
		t.Fatal("ID field not found")
	}
	if !idField.Primary {
		t.Error("ID field should be primary key")
	}
	if !idField.Autoinc {
		t.Error("ID field should be auto increment")
	}
	if idField.Type != TypeInteger {
		t.Errorf("ID field should be integer, got %s", idField.Type)
	}

	// 验证 Username 字段
	usernameField := schema.GetField("username")
	if usernameField == nil {
		t.Fatal("Username field not found")
	}
	if usernameField.Null {
		t.Error("Username field should be not null")
	}
	if !usernameField.Unique {
		t.Error("Username field should be unique")
	}
	if usernameField.Type != TypeString {
		t.Errorf("Username field should be string, got %s", usernameField.Type)
	}

	// 验证 Age 字段（可空）
	ageField := schema.GetField("age")
	if ageField == nil {
		t.Fatal("Age field not found")
	}
	if !ageField.Null {
		t.Error("Age field should be nullable")
	}

	t.Log("✓ InferSchema test passed")
}

// TestToSnakeCase 测试驼峰转蛇形
func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"TestUser", "test_user"},
		{"UserProfile", "user_profile"},
		{"ID", "i_d"},
		{"HTTPRequest", "h_t_t_p_request"},
		{"simple", "simple"},
	}

	for _, tt := range tests {
		result := toSnakeCase(tt.input)
		if result != tt.expected {
			t.Errorf("toSnakeCase(%s) = %s, expected %s", tt.input, result, tt.expected)
		}
	}

	t.Log("✓ toSnakeCase test passed")
}

// TestGetStructFields 测试获取结构体字段
func TestGetStructFields(t *testing.T) {
	fields := GetStructFields(TestUser{})

	expected := []string{"id", "username", "email", "age", "is_active", "created_at"}
	if len(fields) != len(expected) {
		t.Fatalf("Expected %d fields, got %d", len(expected), len(fields))
	}

	for i, field := range fields {
		if field != expected[i] {
			t.Errorf("Field %d: expected '%s', got '%s'", i, expected[i], field)
		}
	}

	t.Log("✓ GetStructFields test passed")
}

// TestGetStructValues 测试获取结构体值
func TestGetStructValues(t *testing.T) {
	age := 30
	user := TestUser{
		ID:        1,
		Username:  "testuser",
		Email:     "test@example.com",
		Age:       &age,
		IsActive:  true,
		CreatedAt: "2024-01-01",
	}

	values := GetStructValues(user)
	if len(values) != 6 {
		t.Fatalf("Expected 6 values, got %d", len(values))
	}

	// 验证值
	if values[0].(int) != 1 {
		t.Errorf("ID value mismatch: expected 1, got %v", values[0])
	}
	if values[1].(string) != "testuser" {
		t.Errorf("Username value mismatch: expected 'testuser', got %v", values[1])
	}

	t.Log("✓ GetStructValues test passed")
}

// TestSQLiteReflectionIntegration 测试 SQLite 与反射的集成
func TestSQLiteReflectionIntegration(t *testing.T) {
	// 创建 SQLite 内存数据库
	config := &Config{
		Adapter:  "sqlite",
		Database: ":memory:",
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// 从结构体推导 Schema
	schema, err := InferSchema(TestUser{})
	if err != nil {
		t.Fatalf("Failed to infer schema: %v", err)
	}

	// 创建表
	createTableSQL := `
		CREATE TABLE test_user (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL UNIQUE,
			email TEXT NOT NULL UNIQUE,
			age INTEGER,
			is_active INTEGER NOT NULL,
			created_at TEXT
		)
	`
	if _, err := repo.Exec(ctx, createTableSQL); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 插入测试数据
	age := 30
	insertSQL := `INSERT INTO test_user (username, email, age, is_active, created_at) VALUES (?, ?, ?, ?, ?)`
	if _, err := repo.Exec(ctx, insertSQL, "testuser", "test@example.com", age, 1, "2024-01-01"); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// 测试 QueryStruct
	var user TestUser
	if err := repo.QueryStruct(ctx, &user, "SELECT * FROM test_user WHERE username = ?", "testuser"); err != nil {
		t.Fatalf("QueryStruct failed: %v", err)
	}

	if user.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got '%s'", user.Username)
	}
	if user.Email != "test@example.com" {
		t.Errorf("Expected email 'test@example.com', got '%s'", user.Email)
	}

	// 插入更多数据
	if _, err := repo.Exec(ctx, insertSQL, "user2", "user2@example.com", 25, 1, "2024-01-02"); err != nil {
		t.Fatalf("Failed to insert second user: %v", err)
	}

	// 测试 QueryStructs
	var users []TestUser
	if err := repo.QueryStructs(ctx, &users, "SELECT * FROM test_user ORDER BY id"); err != nil {
		t.Fatalf("QueryStructs failed: %v", err)
	}

	if len(users) != 2 {
		t.Errorf("Expected 2 users, got %d", len(users))
	}

	if users[0].Username != "testuser" {
		t.Errorf("First user: expected username 'testuser', got '%s'", users[0].Username)
	}
	if users[1].Username != "user2" {
		t.Errorf("Second user: expected username 'user2', got '%s'", users[1].Username)
	}

	// 验证 Schema 对象被正确创建
	if schema.TableName() != "test_user" {
		t.Errorf("Schema table name mismatch: expected 'test_user', got '%s'", schema.TableName())
	}

	t.Log("✓ SQLite reflection integration test passed")
}

func TestInferSchemaLegacyDBTagCompatibility(t *testing.T) {
	schema, err := InferSchema(LegacyTaggedUser{})
	if err != nil {
		t.Fatalf("InferSchema failed: %v", err)
	}

	id := schema.GetField("id")
	if id == nil || !id.Primary {
		t.Fatalf("expected id primary key from legacy db tag")
	}

	name := schema.GetField("name")
	if name == nil || name.Null {
		t.Fatalf("expected name not null from legacy db tag")
	}
}

func TestInferSchemaMergeGormAndEITTag(t *testing.T) {
	schema, err := InferSchema(GormAndEITUser{})
	if err != nil {
		t.Fatalf("InferSchema failed: %v", err)
	}

	id := schema.GetField("id")
	if id == nil || !id.Primary || !id.Autoinc {
		t.Fatalf("expected gorm/eit merged primary autoinc id field, got: %+v", id)
	}

	status := schema.GetField("status")
	if status == nil {
		t.Fatalf("status field not found")
	}
	if status.Null {
		t.Fatalf("expected status not null from merged tags")
	}
	if status.Default != "enabled" {
		t.Fatalf("expected eit_db default override 'enabled', got: %#v", status.Default)
	}

	profile := schema.GetField("profile")
	if profile == nil || profile.Type != TypeJSON {
		t.Fatalf("expected profile type override to json, got: %+v", profile)
	}
}

func TestParseFieldTypeAliasAndInferFieldType(t *testing.T) {
	aliasCases := []struct {
		input    string
		expected FieldType
		ok       bool
	}{
		{input: "varchar", expected: TypeString, ok: true},
		{input: "integer", expected: TypeInteger, ok: true},
		{input: "double", expected: TypeFloat, ok: true},
		{input: "boolean", expected: TypeBoolean, ok: true},
		{input: "timestamp", expected: TypeTime, ok: true},
		{input: "blob", expected: TypeBinary, ok: true},
		{input: "numeric", expected: TypeDecimal, ok: true},
		{input: "map", expected: TypeMap, ok: true},
		{input: "array", expected: TypeArray, ok: true},
		{input: "json", expected: TypeJSON, ok: true},
		{input: "geo", expected: TypeLocation, ok: true},
		{input: "unknown", expected: "", ok: false},
	}

	for _, tc := range aliasCases {
		got, ok := parseFieldTypeAlias(tc.input)
		if ok != tc.ok || got != tc.expected {
			t.Fatalf("parseFieldTypeAlias(%q) = (%q, %v), want (%q, %v)", tc.input, got, ok, tc.expected, tc.ok)
		}
	}

	inferCases := []struct {
		name     string
		typeOf   reflect.Type
		expected FieldType
	}{
		{name: "string", typeOf: reflect.TypeOf(""), expected: TypeString},
		{name: "int", typeOf: reflect.TypeOf(int64(1)), expected: TypeInteger},
		{name: "float", typeOf: reflect.TypeOf(float32(1)), expected: TypeFloat},
		{name: "bool", typeOf: reflect.TypeOf(true), expected: TypeBoolean},
		{name: "time", typeOf: reflect.TypeOf(time.Time{}), expected: TypeTime},
		{name: "null string", typeOf: reflect.TypeOf(sql.NullString{}), expected: TypeString},
		{name: "bytes", typeOf: reflect.TypeOf([]byte{}), expected: TypeBinary},
		{name: "slice", typeOf: reflect.TypeOf([]string{}), expected: TypeArray},
		{name: "map", typeOf: reflect.TypeOf(map[string]any{}), expected: TypeMap},
		{name: "struct", typeOf: reflect.TypeOf(struct{ Name string }{}), expected: TypeJSON},
		{name: "pointer", typeOf: reflect.TypeOf(&time.Time{}), expected: TypeTime},
	}

	for _, tc := range inferCases {
		if got := inferFieldType(tc.typeOf); got != tc.expected {
			t.Fatalf("%s: inferFieldType() = %q, want %q", tc.name, got, tc.expected)
		}
	}
}
