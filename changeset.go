package db

import (
	"context"
	"fmt"
	"regexp"
	"sync"
)

// Changeset 代表对数据的变更（参考 Ecto.Changeset）
type Changeset struct {
	// 原始数据
	data map[string]interface{}

	// 变更的数据
	changes map[string]interface{}

	// 验证错误
	errors map[string][]string

	// 关联的模式
	schema Schema

	// 是否有效
	valid bool

	// 变更前的值（用于追踪）
	previousValues map[string]interface{}

	// 当前 changeset 的操作语义（insert/update/upsert）
	action Action

	// 锁
	mu sync.RWMutex
}

// NewChangeset 创建新的 Changeset
func NewChangeset(schema Schema) *Changeset {
	return &Changeset{
		data:           make(map[string]interface{}),
		changes:        make(map[string]interface{}),
		errors:         make(map[string][]string),
		schema:         schema,
		valid:          true,
		previousValues: make(map[string]interface{}),
		action:         ActionInsert,
	}
}

// FromMap 从 map 创建 Changeset
func FromMap(schema Schema, dataMap map[string]interface{}) *Changeset {
	cs := NewChangeset(schema)
	for k, v := range dataMap {
		cs.data[k] = v
	}
	return cs
}

// Cast 设置字段值（类似 Ecto 的 cast）
func (cs *Changeset) Cast(data map[string]interface{}) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for key, value := range data {
		field := cs.schema.GetField(key)
		if field == nil {
			continue // 忽略未定义的字段
		}

		// 保存原始值
		if oldValue, exists := cs.data[key]; exists {
			cs.previousValues[key] = oldValue
		}

		// 应用转换器
		transformedValue := value
		for _, transformer := range field.Transformers {
			transformed, err := transformer.Transform(transformedValue)
			if err != nil {
				cs.addError(key, fmt.Sprintf("转换器错误: %v", err))
				continue
			}
			transformedValue = transformed
		}

		// 类型转换
		convertedValue, err := ConvertValue(transformedValue, field.Type)
		if err != nil {
			cs.addError(key, fmt.Sprintf("类型转换失败: %v", err))
			continue
		}

		cs.changes[key] = convertedValue
		cs.data[key] = convertedValue
	}

	return cs
}

// Validate 验证 Changeset
func (cs *Changeset) Validate() *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.validateLocked(GetValidationLocale(), cs.action)
}

// ValidateWithLocale 使用指定 locale 执行验证。
func (cs *Changeset) ValidateWithLocale(locale string) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.validateLocked(locale, cs.action)
}

// ValidateWithContext 从上下文读取 locale 执行验证。
func (cs *Changeset) ValidateWithContext(ctx context.Context) *Changeset {
	return cs.ValidateWithLocale(ValidationLocaleFromContext(ctx))
}

// ValidateForInsert 验证插入场景（全字段 required 语义）。
func (cs *Changeset) ValidateForInsert() *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.validateLocked(GetValidationLocale(), ActionInsert)
}

// ValidateForInsertWithLocale 使用指定 locale 验证插入场景。
func (cs *Changeset) ValidateForInsertWithLocale(locale string) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.validateLocked(locale, ActionInsert)
}

// ValidateForInsertWithContext 从上下文读取 locale 验证插入场景。
func (cs *Changeset) ValidateForInsertWithContext(ctx context.Context) *Changeset {
	return cs.ValidateForInsertWithLocale(ValidationLocaleFromContext(ctx))
}

// ValidateForUpdate 验证更新场景（仅校验变更字段）。
func (cs *Changeset) ValidateForUpdate() *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.validateLocked(GetValidationLocale(), ActionUpdate)
}

// ValidateForUpdateWithLocale 使用指定 locale 验证更新场景（仅校验变更字段）。
func (cs *Changeset) ValidateForUpdateWithLocale(locale string) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.validateLocked(locale, ActionUpdate)
}

// ValidateForUpdateWithContext 从上下文读取 locale 验证更新场景（仅校验变更字段）。
func (cs *Changeset) ValidateForUpdateWithContext(ctx context.Context) *Changeset {
	return cs.ValidateForUpdateWithLocale(ValidationLocaleFromContext(ctx))
}

// ValidateForUpsert 验证 upsert 场景。
// 语义：
// 1. 变更字段按 required + validator 校验。
// 2. required 字段若在 data/changes 中都不存在，则判定为缺失。
// 3. 未变更但已存在于 data 的字段不重复执行 validator。
func (cs *Changeset) ValidateForUpsert() *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.validateLocked(GetValidationLocale(), ActionUpsert)
}

// ValidateForUpsertWithLocale 使用指定 locale 验证 upsert 场景。
func (cs *Changeset) ValidateForUpsertWithLocale(locale string) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.validateLocked(locale, ActionUpsert)
}

// ValidateForUpsertWithContext 从上下文读取 locale 验证 upsert 场景。
func (cs *Changeset) ValidateForUpsertWithContext(ctx context.Context) *Changeset {
	return cs.ValidateForUpsertWithLocale(ValidationLocaleFromContext(ctx))
}

func (cs *Changeset) validateLocked(locale string, action Action) *Changeset {
	cs.errors = make(map[string][]string) // 清空之前的错误
	cs.valid = true

	if locale == "" {
		locale = GetValidationLocale()
	}

	action = normalizeAction(action)

	for _, field := range cs.schema.Fields() {
		var (
			value              interface{}
			exists             bool
			shouldCheckRequired bool
			shouldRunValidators bool
		)

		switch action {
		case ActionUpdate:
			value, exists = cs.changes[field.Name]
			if !exists {
				continue
			}
			shouldCheckRequired = true
			shouldRunValidators = true
		case ActionUpsert:
			if changedValue, changed := cs.changes[field.Name]; changed {
				value = changedValue
				exists = true
				shouldCheckRequired = true
				shouldRunValidators = true
			} else {
				value, exists = cs.data[field.Name]
				shouldCheckRequired = !exists
				shouldRunValidators = false
			}
		default: // ActionInsert
			value, exists = cs.data[field.Name]
			shouldCheckRequired = true
			shouldRunValidators = exists && value != nil
		}

		// 检查必填字段
		if shouldCheckRequired && !field.Null && (!exists || value == nil || value == "") {
			cs.addError(field.Name, "字段为必填项")
			cs.valid = false
			continue
		}

		// 应用验证器
		if shouldRunValidators {
			for _, validator := range field.Validators {
				var err error
				if localeAware, ok := validator.(LocaleAwareValidator); ok {
					err = localeAware.ValidateWithLocale(value, locale)
				} else {
					err = validator.Validate(value)
				}

				if err != nil {
					cs.addError(field.Name, err.Error())
					cs.valid = false
				}
			}
		}
	}

	return cs
}

// ValidateChange 验证特定字段的变更
func (cs *Changeset) ValidateChange(fieldName string, validator Validator) *Changeset {
	return cs.ValidateChangeWithLocale(fieldName, validator, GetValidationLocale())
}

// ValidateChangeWithLocale 使用指定 locale 验证指定字段。
func (cs *Changeset) ValidateChangeWithLocale(fieldName string, validator Validator, locale string) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	value, exists := cs.changes[fieldName]
	if !exists {
		return cs
	}

	if locale == "" {
		locale = GetValidationLocale()
	}

	var err error
	if localeAware, ok := validator.(LocaleAwareValidator); ok {
		err = localeAware.ValidateWithLocale(value, locale)
	} else {
		err = validator.Validate(value)
	}

	if err != nil {
		cs.addError(fieldName, err.Error())
		cs.valid = false
	}

	return cs
}

// ValidateChangeWithContext 从上下文读取 locale 验证指定字段。
func (cs *Changeset) ValidateChangeWithContext(ctx context.Context, fieldName string, validator Validator) *Changeset {
	return cs.ValidateChangeWithLocale(fieldName, validator, ValidationLocaleFromContext(ctx))
}

// IsValid 检查 Changeset 是否有效
func (cs *Changeset) IsValid() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.valid && len(cs.errors) == 0
}

// Errors 获取所有错误
func (cs *Changeset) Errors() map[string][]string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.errors
}

// GetError 获取字段的错误
func (cs *Changeset) GetError(fieldName string) []string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.errors[fieldName]
}

// Data 获取所有数据
func (cs *Changeset) Data() map[string]interface{} {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make(map[string]interface{})
	for k, v := range cs.data {
		result[k] = v
	}
	return result
}

// Changes 获取变更的数据
func (cs *Changeset) Changes() map[string]interface{} {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make(map[string]interface{})
	for k, v := range cs.changes {
		result[k] = v
	}
	return result
}

// Get 获取字段值
func (cs *Changeset) Get(fieldName string) interface{} {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.data[fieldName]
}

// GetChanged 获取变更的字段值
func (cs *Changeset) GetChanged(fieldName string) (interface{}, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	val, ok := cs.changes[fieldName]
	return val, ok
}

// GetPrevious 获取变更前的值
func (cs *Changeset) GetPrevious(fieldName string) interface{} {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.previousValues[fieldName]
}

// HasChanged 检查字段是否被修改
func (cs *Changeset) HasChanged(fieldName string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	_, ok := cs.changes[fieldName]
	return ok
}

// addError 添加验证错误
func (cs *Changeset) addError(fieldName string, message string) {
	if _, ok := cs.errors[fieldName]; !ok {
		cs.errors[fieldName] = make([]string, 0)
	}
	cs.errors[fieldName] = append(cs.errors[fieldName], message)
}

// PutChange 手动添加变更
func (cs *Changeset) PutChange(fieldName string, value interface{}) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	field := cs.schema.GetField(fieldName)
	if field == nil {
		return cs
	}

	// 保存原始值
	if oldValue, exists := cs.data[fieldName]; exists {
		cs.previousValues[fieldName] = oldValue
	}

	cs.changes[fieldName] = value
	cs.data[fieldName] = value

	return cs
}

// ClearError 清除错误
func (cs *Changeset) ClearError(fieldName string) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	delete(cs.errors, fieldName)
	if len(cs.errors) == 0 {
		cs.valid = true
	}

	return cs
}

// ForceChanges 强制所有字段为变更状态（用于插入操作）
func (cs *Changeset) ForceChanges() *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for k, v := range cs.data {
		cs.changes[k] = v
	}

	return cs
}

// GetChangedFields 获取所有被修改的字段名列表
func (cs *Changeset) GetChangedFields() []string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	fields := make([]string, 0, len(cs.changes))
	for k := range cs.changes {
		fields = append(fields, k)
	}
	return fields
}

// ErrorString 返回格式化的错误字符串
func (cs *Changeset) ErrorString() string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if len(cs.errors) == 0 {
		return ""
	}

	errorStr := ""
	for field, messages := range cs.errors {
		errorStr += field + ": " + fmt.Sprintf("%v", messages) + "; "
	}
	return errorStr
}

// ToMap 转换为 map（用于数据库操作）
func (cs *Changeset) ToMap() map[string]interface{} {
	return cs.Changes()
}

// ValidateRequired 验证必填字段
func (cs *Changeset) ValidateRequired(fields []string) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, fieldName := range fields {
		value, exists := cs.data[fieldName]
		if !exists || value == nil || value == "" {
			cs.addError(fieldName, fmt.Sprintf("%s is required", fieldName))
			cs.valid = false
		}
	}

	return cs
}

// ValidateLength 验证字符串长度
func (cs *Changeset) ValidateLength(fieldName string, min, max int) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	value, exists := cs.data[fieldName]
	if !exists || value == nil {
		return cs
	}

	str, ok := value.(string)
	if !ok {
		cs.addError(fieldName, fmt.Sprintf("%s must be a string", fieldName))
		cs.valid = false
		return cs
	}

	length := len(str)
	if min > 0 && length < min {
		cs.addError(fieldName, fmt.Sprintf("%s is too short (minimum is %d characters)", fieldName, min))
		cs.valid = false
	}
	if max > 0 && length > max {
		cs.addError(fieldName, fmt.Sprintf("%s is too long (maximum is %d characters)", fieldName, max))
		cs.valid = false
	}

	return cs
}

// ValidateFormat 验证字段格式（使用正则表达式）
func (cs *Changeset) ValidateFormat(fieldName string, pattern string, message ...string) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	value, exists := cs.data[fieldName]
	if !exists || value == nil {
		return cs
	}

	str, ok := value.(string)
	if !ok {
		cs.addError(fieldName, fmt.Sprintf("%s must be a string", fieldName))
		cs.valid = false
		return cs
	}

	// 使用 regexp 验证
	re, err := regexp.Compile(pattern)
	if err != nil {
		cs.addError(fieldName, fmt.Sprintf("invalid pattern: %v", err))
		cs.valid = false
		return cs
	}

	if !re.MatchString(str) {
		errMsg := fmt.Sprintf("%s has invalid format", fieldName)
		if len(message) > 0 {
			errMsg = message[0]
		}
		cs.addError(fieldName, errMsg)
		cs.valid = false
	}

	return cs
}

// ValidateInclusion 验证字段值在指定列表中
func (cs *Changeset) ValidateInclusion(fieldName string, list []interface{}) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	value, exists := cs.data[fieldName]
	if !exists || value == nil {
		return cs
	}

	found := false
	for _, item := range list {
		if value == item {
			found = true
			break
		}
	}

	if !found {
		cs.addError(fieldName, fmt.Sprintf("%s is not included in the list", fieldName))
		cs.valid = false
	}

	return cs
}

// ValidateExclusion 验证字段值不在指定列表中
func (cs *Changeset) ValidateExclusion(fieldName string, list []interface{}) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	value, exists := cs.data[fieldName]
	if !exists || value == nil {
		return cs
	}

	for _, item := range list {
		if value == item {
			cs.addError(fieldName, fmt.Sprintf("%s is reserved", fieldName))
			cs.valid = false
			break
		}
	}

	return cs
}

// ValidateNumber 验证数字范围
func (cs *Changeset) ValidateNumber(fieldName string, opts map[string]interface{}) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	value, exists := cs.data[fieldName]
	if !exists || value == nil {
		return cs
	}

	// 转换为 float64 进行比较
	var num float64
	switch v := value.(type) {
	case int:
		num = float64(v)
	case int64:
		num = float64(v)
	case float32:
		num = float64(v)
	case float64:
		num = v
	default:
		cs.addError(fieldName, fmt.Sprintf("%s must be a number", fieldName))
		cs.valid = false
		return cs
	}

	if minVal, ok := opts["greater_than"].(float64); ok {
		if num <= minVal {
			cs.addError(fieldName, fmt.Sprintf("%s must be greater than %v", fieldName, minVal))
			cs.valid = false
		}
	}

	if minVal, ok := opts["greater_than_or_equal_to"].(float64); ok {
		if num < minVal {
			cs.addError(fieldName, fmt.Sprintf("%s must be greater than or equal to %v", fieldName, minVal))
			cs.valid = false
		}
	}

	if maxVal, ok := opts["less_than"].(float64); ok {
		if num >= maxVal {
			cs.addError(fieldName, fmt.Sprintf("%s must be less than %v", fieldName, maxVal))
			cs.valid = false
		}
	}

	if maxVal, ok := opts["less_than_or_equal_to"].(float64); ok {
		if num > maxVal {
			cs.addError(fieldName, fmt.Sprintf("%s must be less than or equal to %v", fieldName, maxVal))
			cs.valid = false
		}
	}

	if equalTo, ok := opts["equal_to"].(float64); ok {
		if num != equalTo {
			cs.addError(fieldName, fmt.Sprintf("%s must be equal to %v", fieldName, equalTo))
			cs.valid = false
		}
	}

	return cs
}

// GetChange 获取变更的字段值（便捷方法）
func (cs *Changeset) GetChange(fieldName string) interface{} {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.changes[fieldName]
}

// Action 表示 Changeset 的操作类型
type Action string

const (
	ActionInsert Action = "insert"
	ActionUpdate Action = "update"
	ActionUpsert Action = "upsert"
	ActionDelete Action = "delete"
)

func normalizeAction(action Action) Action {
	switch action {
	case ActionUpdate, ActionUpsert, ActionDelete:
		return action
	default:
		return ActionInsert
	}
}

// SetAction 设置当前 changeset 的操作语义。
func (cs *Changeset) SetAction(action Action) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.action = normalizeAction(action)
	return cs
}

// Action 返回当前 changeset 的操作语义。
func (cs *Changeset) Action() Action {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.action
}

// ApplyAction 根据操作类型应用不同的验证逻辑
func (cs *Changeset) ApplyAction(action Action) *Changeset {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	action = normalizeAction(action)
	cs.action = action
	return cs.validateLocked(GetValidationLocale(), action)
}
