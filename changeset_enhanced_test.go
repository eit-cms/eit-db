package db

import (
	"context"
	"slices"
	"testing"
)

// TestValidateRequired 测试必填字段验证
func TestValidateRequired(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "name", Type: TypeString})
	schema.AddField(&Field{Name: "email", Type: TypeString})
	schema.AddField(&Field{Name: "age", Type: TypeInteger})

	tests := []struct {
		name      string
		data      map[string]interface{}
		required  []string
		wantValid bool
		wantError string
	}{
		{
			name:      "all required fields present",
			data:      map[string]interface{}{"name": "Alice", "email": "alice@example.com"},
			required:  []string{"name", "email"},
			wantValid: true,
		},
		{
			name:      "missing required field",
			data:      map[string]interface{}{"name": "Alice"},
			required:  []string{"name", "email"},
			wantValid: false,
			wantError: "email",
		},
		{
			name:      "empty string for required field",
			data:      map[string]interface{}{"name": "", "email": "alice@example.com"},
			required:  []string{"name", "email"},
			wantValid: false,
			wantError: "name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := FromMap(schema, tt.data)
			cs.ValidateRequired(tt.required)

			if cs.IsValid() != tt.wantValid {
				t.Errorf("IsValid() = %v, want %v, errors: %v", cs.IsValid(), tt.wantValid, cs.Errors())
			}

			if !tt.wantValid && tt.wantError != "" {
				if _, ok := cs.Errors()[tt.wantError]; !ok {
					t.Errorf("Expected error for field %s, got errors: %v", tt.wantError, cs.Errors())
				}
			}
		})
	}
}

// TestValidateLength 测试字符串长度验证
func TestValidateLength(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "username", Type: TypeString})
	schema.AddField(&Field{Name: "password", Type: TypeString})

	tests := []struct {
		name      string
		data      map[string]interface{}
		field     string
		min       int
		max       int
		wantValid bool
	}{
		{
			name:      "valid length",
			data:      map[string]interface{}{"username": "alice"},
			field:     "username",
			min:       3,
			max:       10,
			wantValid: true,
		},
		{
			name:      "too short",
			data:      map[string]interface{}{"username": "ab"},
			field:     "username",
			min:       3,
			max:       10,
			wantValid: false,
		},
		{
			name:      "too long",
			data:      map[string]interface{}{"username": "verylongusername"},
			field:     "username",
			min:       3,
			max:       10,
			wantValid: false,
		},
		{
			name:      "no min limit",
			data:      map[string]interface{}{"username": "a"},
			field:     "username",
			min:       0,
			max:       10,
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := FromMap(schema, tt.data)
			cs.ValidateLength(tt.field, tt.min, tt.max)

			if cs.IsValid() != tt.wantValid {
				t.Errorf("IsValid() = %v, want %v, errors: %v", cs.IsValid(), tt.wantValid, cs.Errors())
			}
		})
	}
}

// TestValidateFormat 测试格式验证
func TestValidateFormat(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "email", Type: TypeString})
	schema.AddField(&Field{Name: "phone", Type: TypeString})

	tests := []struct {
		name      string
		data      map[string]interface{}
		field     string
		pattern   string
		wantValid bool
	}{
		{
			name:      "valid email",
			data:      map[string]interface{}{"email": "alice@example.com"},
			field:     "email",
			pattern:   `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`,
			wantValid: true,
		},
		{
			name:      "invalid email",
			data:      map[string]interface{}{"email": "not-an-email"},
			field:     "email",
			pattern:   `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`,
			wantValid: false,
		},
		{
			name:      "valid phone",
			data:      map[string]interface{}{"phone": "1234567890"},
			field:     "phone",
			pattern:   `^\d{10}$`,
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := FromMap(schema, tt.data)
			cs.ValidateFormat(tt.field, tt.pattern)

			if cs.IsValid() != tt.wantValid {
				t.Errorf("IsValid() = %v, want %v, errors: %v", cs.IsValid(), tt.wantValid, cs.Errors())
			}
		})
	}
}

// TestValidateInclusion 测试包含验证
func TestValidateInclusion(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "role", Type: TypeString})
	schema.AddField(&Field{Name: "status", Type: TypeString})

	tests := []struct {
		name      string
		data      map[string]interface{}
		field     string
		list      []interface{}
		wantValid bool
	}{
		{
			name:      "value in list",
			data:      map[string]interface{}{"role": "admin"},
			field:     "role",
			list:      []interface{}{"admin", "user", "guest"},
			wantValid: true,
		},
		{
			name:      "value not in list",
			data:      map[string]interface{}{"role": "superadmin"},
			field:     "role",
			list:      []interface{}{"admin", "user", "guest"},
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := FromMap(schema, tt.data)
			cs.ValidateInclusion(tt.field, tt.list)

			if cs.IsValid() != tt.wantValid {
				t.Errorf("IsValid() = %v, want %v, errors: %v", cs.IsValid(), tt.wantValid, cs.Errors())
			}
		})
	}
}

// TestValidateExclusion 测试排除验证
func TestValidateExclusion(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "username", Type: TypeString})

	tests := []struct {
		name      string
		data      map[string]interface{}
		field     string
		list      []interface{}
		wantValid bool
	}{
		{
			name:      "value not in exclusion list",
			data:      map[string]interface{}{"username": "alice"},
			field:     "username",
			list:      []interface{}{"admin", "root", "system"},
			wantValid: true,
		},
		{
			name:      "value in exclusion list",
			data:      map[string]interface{}{"username": "admin"},
			field:     "username",
			list:      []interface{}{"admin", "root", "system"},
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := FromMap(schema, tt.data)
			cs.ValidateExclusion(tt.field, tt.list)

			if cs.IsValid() != tt.wantValid {
				t.Errorf("IsValid() = %v, want %v, errors: %v", cs.IsValid(), tt.wantValid, cs.Errors())
			}
		})
	}
}

// TestValidateNumber 测试数字范围验证
func TestValidateNumber(t *testing.T) {
	schema := NewBaseSchema("products")
	schema.AddField(&Field{Name: "price", Type: TypeFloat})
	schema.AddField(&Field{Name: "quantity", Type: TypeInteger})
	schema.AddField(&Field{Name: "rating", Type: TypeFloat})

	tests := []struct {
		name      string
		data      map[string]interface{}
		field     string
		opts      map[string]interface{}
		wantValid bool
	}{
		{
			name:      "greater than",
			data:      map[string]interface{}{"price": 10.0},
			field:     "price",
			opts:      map[string]interface{}{"greater_than": 0.0},
			wantValid: true,
		},
		{
			name:      "not greater than",
			data:      map[string]interface{}{"price": 0.0},
			field:     "price",
			opts:      map[string]interface{}{"greater_than": 0.0},
			wantValid: false,
		},
		{
			name:      "greater than or equal",
			data:      map[string]interface{}{"quantity": 10},
			field:     "quantity",
			opts:      map[string]interface{}{"greater_than_or_equal_to": 10.0},
			wantValid: true,
		},
		{
			name:      "less than",
			data:      map[string]interface{}{"rating": 4.5},
			field:     "rating",
			opts:      map[string]interface{}{"less_than": 5.0},
			wantValid: true,
		},
		{
			name:      "range validation",
			data:      map[string]interface{}{"rating": 3.5},
			field:     "rating",
			opts:      map[string]interface{}{"greater_than_or_equal_to": 0.0, "less_than_or_equal_to": 5.0},
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := FromMap(schema, tt.data)
			cs.ValidateNumber(tt.field, tt.opts)

			if cs.IsValid() != tt.wantValid {
				t.Errorf("IsValid() = %v, want %v, errors: %v", cs.IsValid(), tt.wantValid, cs.Errors())
			}
		})
	}
}

// TestGetChange 测试 GetChange 便捷方法
func TestGetChange(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "name", Type: TypeString})
	schema.AddField(&Field{Name: "email", Type: TypeString})

	cs := NewChangeset(schema)
	cs.Cast(map[string]interface{}{
		"name":  "Alice",
		"email": "alice@example.com",
	})

	// GetChange 应该返回变更的值
	if name := cs.GetChange("name"); name != "Alice" {
		t.Errorf("GetChange(name) = %v, want Alice", name)
	}

	if email := cs.GetChange("email"); email != "alice@example.com" {
		t.Errorf("GetChange(email) = %v, want alice@example.com", email)
	}

	// 未变更的字段应该返回 nil
	if age := cs.GetChange("age"); age != nil {
		t.Errorf("GetChange(age) = %v, want nil", age)
	}
}

// TestCombinedValidations 测试组合验证
func TestCombinedValidations(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "username", Type: TypeString})
	schema.AddField(&Field{Name: "email", Type: TypeString})
	schema.AddField(&Field{Name: "age", Type: TypeInteger})
	schema.AddField(&Field{Name: "role", Type: TypeString})

	data := map[string]interface{}{
		"username": "alice",
		"email":    "alice@example.com",
		"age":      25,
		"role":     "admin",
	}

	cs := FromMap(schema, data)
	cs.ValidateRequired([]string{"username", "email"}).
		ValidateLength("username", 3, 20).
		ValidateFormat("email", `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`).
		ValidateNumber("age", map[string]interface{}{"greater_than_or_equal_to": 18.0}).
		ValidateInclusion("role", []interface{}{"admin", "user", "guest"})

	if !cs.IsValid() {
		t.Errorf("Combined validations failed, errors: %v", cs.Errors())
	}
}

func TestChangesetAccessorsAndMutations(t *testing.T) {
	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "name", Type: TypeString})
	schema.AddField(&Field{Name: "phone", Type: TypeString})

	cs := FromMap(schema, map[string]interface{}{"name": "alice", "phone": "+1 (415) 555-2671"})
	cs.PutChange("name", "bob")
	cs.PutChange("unknown", "ignored")

	if got := cs.Get("name"); got != "bob" {
		t.Fatalf("expected updated name, got %v", got)
	}
	if prev := cs.GetPrevious("name"); prev != "alice" {
		t.Fatalf("expected previous value alice, got %v", prev)
	}
	if !cs.HasChanged("name") {
		t.Fatalf("expected name marked as changed")
	}
	changedVal, ok := cs.GetChanged("name")
	if !ok || changedVal != "bob" {
		t.Fatalf("expected changed name bob, got %v, %v", changedVal, ok)
	}
	if cs.GetChange("name") != "bob" {
		t.Fatalf("expected GetChange to return bob")
	}

	data := cs.Data()
	changes := cs.Changes()
	data["name"] = "mutated"
	changes["name"] = "mutated"
	if cs.Get("name") != "bob" || cs.GetChange("name") != "bob" {
		t.Fatalf("expected Data/Changes to return defensive copies")
	}

	fields := cs.GetChangedFields()
	if !slices.Contains(fields, "name") {
		t.Fatalf("expected changed fields to contain name, got %v", fields)
	}
	if cs.ToMap()["name"] != "bob" {
		t.Fatalf("expected ToMap to reflect changed values")
	}
	if cs.Action() != ActionInsert {
		t.Fatalf("expected default action to be insert")
	}
	cs.SetAction(ActionUpdate)
	if cs.Action() != ActionUpdate {
		t.Fatalf("expected SetAction to change action to update")
	}
	if cs.ApplyAction(ActionDelete) != cs {
		t.Fatalf("expected ApplyAction to return receiver")
	}
	if cs.Action() != ActionDelete {
		t.Fatalf("expected ApplyAction to persist action")
	}
}

func TestValidateChangeHelpers(t *testing.T) {
	prevLocale := GetValidationLocale()
	defer func() {
		_ = SetValidationLocale(prevLocale)
	}()

	schema := NewBaseSchema("users")
	schema.AddField(&Field{Name: "name", Type: TypeString})
	schema.AddField(&Field{Name: "phone", Type: TypeString})

	cs := NewChangeset(schema)
	cs.PutChange("name", "bob")
	cs.ValidateChange("name", &RegexValidator{Pattern: `^[A-Z]+$`, Code: "regex", InvalidType: "must be string", InvalidValue: "invalid format"})
	if cs.IsValid() {
		t.Fatalf("expected ValidateChange to mark changeset invalid")
	}
	if len(cs.GetError("name")) == 0 {
		t.Fatalf("expected name validation error")
	}
	if cs.ErrorString() == "" {
		t.Fatalf("expected ErrorString to include validation errors")
	}
	cs.ClearError("name")
	if !cs.IsValid() {
		t.Fatalf("expected ClearError to restore valid state")
	}

	if err := SetValidationLocale("zh-CN"); err != nil {
		t.Fatalf("failed to set locale: %v", err)
	}
	cs.PutChange("phone", "+1 (415) 555-2671")
	cs.ValidateChangeWithLocale("phone", &PhoneNumberValidator{}, "en-US")
	if !cs.IsValid() {
		t.Fatalf("expected ValidateChangeWithLocale to accept en-US phone, errors=%v", cs.Errors())
	}

	cs2 := NewChangeset(schema)
	cs2.PutChange("phone", "+1 (415) 555-2671")
	ctx := WithValidationLocale(context.Background(), "en-US")
	cs2.ValidateChangeWithContext(ctx, "phone", &PhoneNumberValidator{})
	if !cs2.IsValid() {
		t.Fatalf("expected ValidateChangeWithContext to accept context locale, errors=%v", cs2.Errors())
	}
	if NewChangeset(schema).ValidateChange("missing", &PhoneNumberValidator{}) == nil {
		t.Fatalf("expected ValidateChange to return receiver even when field missing")
	}
}

func TestValidateWithLocaleForLocaleAwareValidators(t *testing.T) {
	prev := GetValidationLocale()
	defer func() {
		_ = SetValidationLocale(prev)
	}()

	schema := NewBaseSchema("profiles")
	schema.AddField(NewField("id_card", TypeString).Validate(&IDCardValidator{}).Build())

	csUS := NewChangeset(schema).Cast(map[string]interface{}{"id_card": "123-45-6789"})
	csUS.ValidateWithLocale("en-US")
	if !csUS.IsValid() {
		t.Fatalf("expected en-US id format to pass with ValidateWithLocale, errors: %v", csUS.Errors())
	}

	csZH := NewChangeset(schema).Cast(map[string]interface{}{"id_card": "123-45-6789"})
	csZH.ValidateWithLocale("zh-CN")
	if csZH.IsValid() {
		t.Fatalf("expected en-US id format to fail under zh-CN locale")
	}
}

func TestValidateWithContextPropagatesLocale(t *testing.T) {
	prev := GetValidationLocale()
	defer func() {
		_ = SetValidationLocale(prev)
	}()

	schema := NewBaseSchema("profiles")
	schema.AddField(NewField("postal_code", TypeString).Validate(&PostalCodeValidator{}).Build())

	ctxUS := WithValidationLocale(context.Background(), "en-US")
	csUS := NewChangeset(schema).Cast(map[string]interface{}{"postal_code": "94105-1234"})
	csUS.ValidateWithContext(ctxUS)
	if !csUS.IsValid() {
		t.Fatalf("expected US ZIP to pass with context locale, errors: %v", csUS.Errors())
	}

	ctxZH := WithValidationLocale(context.Background(), "zh-CN")
	csZH := NewChangeset(schema).Cast(map[string]interface{}{"postal_code": "94105-1234"})
	csZH.ValidateWithContext(ctxZH)
	if csZH.IsValid() {
		t.Fatalf("expected US ZIP to fail under zh-CN context locale")
	}
}

func TestValidateForUpdateOnlyChecksChangedFields(t *testing.T) {
	schema := NewBaseSchema("pages")
	schema.AddField(NewField("id", TypeInteger).Null(false).Build())
	schema.AddField(NewField("slug", TypeString).Null(false).Build())
	schema.AddField(NewField("title", TypeString).Null(false).Build())
	schema.AddField(NewField("published", TypeBoolean).Null(false).Build())

	patch := map[string]interface{}{
		"published": true,
	}

	cs := NewChangeset(schema).Cast(patch).ValidateForUpdate()
	if !cs.IsValid() {
		t.Fatalf("expected update patch to pass ValidateForUpdate, errors: %v", cs.Errors())
	}

	fullValidate := NewChangeset(schema).Cast(patch).Validate()
	if fullValidate.IsValid() {
		t.Fatalf("expected Validate to keep full required semantics")
	}
	if _, ok := fullValidate.Errors()["title"]; !ok {
		t.Fatalf("expected Validate to report missing required field 'title', errors: %v", fullValidate.Errors())
	}
}

func TestValidateForUpdateRequiredOnChangedField(t *testing.T) {
	schema := NewBaseSchema("pages")
	schema.AddField(NewField("title", TypeString).Null(false).Build())
	schema.AddField(NewField("published", TypeBoolean).Null(false).Build())

	cs := NewChangeset(schema).Cast(map[string]interface{}{
		"title": "",
	}).ValidateForUpdate()

	if cs.IsValid() {
		t.Fatalf("expected changed required field to fail ValidateForUpdate")
	}
	if _, ok := cs.Errors()["title"]; !ok {
		t.Fatalf("expected ValidateForUpdate required error on title, errors: %v", cs.Errors())
	}
}

func TestActionDrivenValidationSemantics(t *testing.T) {
	schema := NewBaseSchema("pages")
	schema.AddField(NewField("id", TypeInteger).Null(false).Build())
	schema.AddField(NewField("title", TypeString).Null(false).Build())
	schema.AddField(NewField("published", TypeBoolean).Null(false).Build())

	patch := map[string]interface{}{
		"published": true,
	}

	insertLike := NewChangeset(schema).Cast(patch).SetAction(ActionInsert).Validate()
	if insertLike.IsValid() {
		t.Fatalf("expected insert action to fail when required fields are missing")
	}

	updateLike := NewChangeset(schema).Cast(patch).SetAction(ActionUpdate).Validate()
	if !updateLike.IsValid() {
		t.Fatalf("expected update action to validate changed fields only, errors: %v", updateLike.Errors())
	}

	upsertWithoutBaseline := NewChangeset(schema).Cast(patch).SetAction(ActionUpsert).Validate()
	if upsertWithoutBaseline.IsValid() {
		t.Fatalf("expected upsert action to fail when required fields are absent in both data and changes")
	}

	upsertWithBaseline := FromMap(schema, map[string]interface{}{
		"id":        1,
		"title":     "hello",
		"published": false,
	}).PutChange("published", true).SetAction(ActionUpsert).Validate()
	if !upsertWithBaseline.IsValid() {
		t.Fatalf("expected upsert action to pass when required baseline exists, errors: %v", upsertWithBaseline.Errors())
	}
}
