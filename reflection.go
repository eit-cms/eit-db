package db

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// InferSchema 从 Go struct 推导 Schema
// 支持 struct tag: `db:"column_name,primary_key,not_null,unique,index,auto_increment"`
func InferSchema(v interface{}) (*BaseSchema, error) {
	val := reflect.ValueOf(v)
	typ := val.Type()

	// 处理指针类型
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		val = val.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("InferSchema: expected struct, got %v", typ.Kind())
	}

	// 推导表名：使用类型名的 snake_case 形式
	tableName := toSnakeCase(typ.Name())
	schema := NewBaseSchema(tableName)

	// 遍历字段
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// 跳过非导出字段
		if !field.IsExported() {
			continue
		}

		// 解析 db tag
		dbTag := field.Tag.Get("db")
		if dbTag == "-" {
			continue // 忽略此字段
		}

		columnName, options := parseDBTag(dbTag, field.Name)
		fieldType := inferFieldType(field.Type)

		// 构建字段
		fb := NewField(columnName, fieldType)

		// 处理选项
		if options.notNull {
			fb.Null(false)
		} else {
			fb.Null(true)
		}

		if options.primaryKey {
			fb.PrimaryKey()
		}

		if options.unique {
			fb.Unique()
		}

		if options.index {
			fb.Index()
		}

		if options.autoIncrement {
			fb.field.Autoinc = true
		}

		// 添加到 schema
		schema.AddField(fb.Build())
	}

	return schema, nil
}

// tagOptions 解析后的 tag 选项
type tagOptions struct {
	notNull       bool
	primaryKey    bool
	unique        bool
	index         bool
	autoIncrement bool
}

// parseDBTag 解析 db tag
// 格式: "column_name,primary_key,not_null,unique,index,auto_increment"
func parseDBTag(tag, fieldName string) (string, tagOptions) {
	if tag == "" {
		return toSnakeCase(fieldName), tagOptions{}
	}

	parts := strings.Split(tag, ",")
	columnName := strings.TrimSpace(parts[0])
	if columnName == "" {
		columnName = toSnakeCase(fieldName)
	}

	opts := tagOptions{}
	for i := 1; i < len(parts); i++ {
		opt := strings.TrimSpace(parts[i])
		switch opt {
		case "primary_key", "primarykey", "pk":
			opts.primaryKey = true
		case "not_null", "notnull":
			opts.notNull = true
		case "unique":
			opts.unique = true
		case "index":
			opts.index = true
		case "auto_increment", "autoincrement":
			opts.autoIncrement = true
		}
	}

	return columnName, opts
}

// inferFieldType 从 Go 类型推导 FieldType
func inferFieldType(t reflect.Type) FieldType {
	// 处理指针类型
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// 处理 sql.Null* 类型
	switch t {
	case reflect.TypeOf(sql.NullString{}):
		return TypeString
	case reflect.TypeOf(sql.NullInt64{}):
		return TypeInteger
	case reflect.TypeOf(sql.NullFloat64{}):
		return TypeFloat
	case reflect.TypeOf(sql.NullBool{}):
		return TypeBoolean
	case reflect.TypeOf(sql.NullTime{}):
		return TypeTime
	}

	// 根据 Kind 推导
	switch t.Kind() {
	case reflect.String:
		return TypeString
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return TypeInteger
	case reflect.Float32, reflect.Float64:
		return TypeFloat
	case reflect.Bool:
		return TypeBoolean
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return TypeTime
		}
		return TypeJSON // 默认作为 JSON
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return TypeBinary // []byte
		}
		return TypeArray
	case reflect.Map:
		return TypeMap
	default:
		return TypeString // 默认类型
	}
}

// toSnakeCase 将驼峰命名转换为蛇形命名
func toSnakeCase(s string) string {
	var result []rune
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result = append(result, '_')
		}
		result = append(result, r)
	}
	return strings.ToLower(string(result))
}

// ScanStruct 从 sql.Row 扫描单个结构体
func ScanStruct(row *sql.Row, dest interface{}) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("ScanStruct: dest must be a pointer")
	}

	elem := val.Elem()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("ScanStruct: dest must be a pointer to struct")
	}

	// 收集扫描目标
	scanDest := make([]interface{}, 0, elem.NumField())
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		if !field.CanSet() {
			continue
		}
		scanDest = append(scanDest, field.Addr().Interface())
	}

	return row.Scan(scanDest...)
}

// ScanStructs 从 sql.Rows 扫描多个结构体
func ScanStructs(rows *sql.Rows, dest interface{}) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return fmt.Errorf("ScanStructs: dest must be a pointer")
	}

	sliceVal := destVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("ScanStructs: dest must be a pointer to slice")
	}

	elemType := sliceVal.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	if isPtr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("ScanStructs: slice element must be struct or pointer to struct")
	}

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ScanStructs: failed to get columns: %w", err)
	}

	// 构建字段名到索引的映射
	fieldMap := make(map[string]int)
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		if !field.IsExported() {
			continue
		}

		dbTag := field.Tag.Get("db")
		columnName, _ := parseDBTag(dbTag, field.Name)
		fieldMap[columnName] = i
	}

	// 遍历行
	for rows.Next() {
		// 创建新元素
		elemVal := reflect.New(elemType).Elem()

		// 准备扫描目标
		scanDest := make([]interface{}, len(columns))
		for i, colName := range columns {
			if fieldIdx, ok := fieldMap[colName]; ok {
				field := elemVal.Field(fieldIdx)
				if field.CanSet() {
					scanDest[i] = field.Addr().Interface()
					continue
				}
			}
			// 未映射的列使用占位符
			var placeholder interface{}
			scanDest[i] = &placeholder
		}

		// 扫描行
		if err := rows.Scan(scanDest...); err != nil {
			return fmt.Errorf("ScanStructs: failed to scan row: %w", err)
		}

		// 添加到切片
		if isPtr {
			sliceVal.Set(reflect.Append(sliceVal, elemVal.Addr()))
		} else {
			sliceVal.Set(reflect.Append(sliceVal, elemVal))
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("ScanStructs: rows error: %w", err)
	}

	return nil
}

// GetStructFields 获取结构体的字段名列表（按 db tag 顺序）
func GetStructFields(v interface{}) []string {
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil
	}

	fields := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		dbTag := field.Tag.Get("db")
		if dbTag == "-" {
			continue
		}

		columnName, _ := parseDBTag(dbTag, field.Name)
		fields = append(fields, columnName)
	}

	return fields
}

// GetStructValues 获取结构体的字段值列表
func GetStructValues(v interface{}) []interface{} {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil
	}

	typ := val.Type()
	values := make([]interface{}, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		dbTag := field.Tag.Get("db")
		if dbTag == "-" {
			continue
		}

		fieldVal := val.Field(i)
		values = append(values, fieldVal.Interface())
	}

	return values
}
