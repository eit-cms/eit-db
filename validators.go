package db

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"sync"
)

// ==================== 常用验证器 ====================

var (
	defaultEmailPattern = `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
	// 支持国际区号与常见分隔符（空格、-、()）。
	defaultPhonePattern = `^\+?[0-9][0-9\-\s()]{5,19}$`
	defaultURLPattern   = `^https?://[\w.-]+(?:\:[0-9]+)?(?:/[\w\-./?%&=+#]*)?$`
	// 中国大陆邮政编码（6 位数字）。
	defaultPostalCodePattern = `^[0-9]{6}$`
	// 中国大陆身份证号（15位或18位，最后一位可为 X/x）。
	defaultIDCardPattern = `^(?:\d{15}|\d{17}[0-9Xx])$`
)

// ValidationMessages 校验消息模板。
type ValidationMessages struct {
	EmailInvalidType   string
	EmailInvalidValue  string
	PhoneInvalidType   string
	PhoneInvalidValue  string
	URLInvalidType     string
	URLInvalidValue    string
	PostalInvalidType  string
	PostalInvalidValue string
	IDCardInvalidType  string
	IDCardInvalidValue string
}

// ValidationProfile 区域化校验规则配置。
type ValidationProfile struct {
	Locale            string
	EmailPattern      string
	PhonePattern      string
	URLPattern        string
	PostalCodePattern string
	IDCardPattern     string
	Messages          ValidationMessages
}

var (
	validationMu sync.RWMutex

	validationProfiles = map[string]ValidationProfile{
		"zh-CN": {
			Locale:            "zh-CN",
			EmailPattern:      defaultEmailPattern,
			PhonePattern:      defaultPhonePattern,
			URLPattern:        defaultURLPattern,
			PostalCodePattern: defaultPostalCodePattern,
			IDCardPattern:     defaultIDCardPattern,
			Messages: ValidationMessages{
				EmailInvalidType:   "邮箱必须是字符串",
				EmailInvalidValue:  "邮箱格式不正确",
				PhoneInvalidType:   "电话号码必须是字符串",
				PhoneInvalidValue:  "电话号码格式不正确",
				URLInvalidType:     "URL 必须是字符串",
				URLInvalidValue:    "URL 格式不正确",
				PostalInvalidType:  "邮政编码必须是字符串",
				PostalInvalidValue: "邮政编码格式不正确",
				IDCardInvalidType:  "身份证号必须是字符串",
				IDCardInvalidValue: "身份证号格式不正确",
			},
		},
		"en-US": {
			Locale:            "en-US",
			EmailPattern:      defaultEmailPattern,
			PhonePattern:      `^(?:\+1[\s-]?)?(?:\([2-9][0-9]{2}\)|[2-9][0-9]{2})[\s-]?[0-9]{3}[\s-]?[0-9]{4}$`,
			URLPattern:        defaultURLPattern,
			PostalCodePattern: `^[0-9]{5}(?:-[0-9]{4})?$`,
			IDCardPattern:     `^[0-9]{3}-?[0-9]{2}-?[0-9]{4}$`,
			Messages: ValidationMessages{
				EmailInvalidType:   "email must be a string",
				EmailInvalidValue:  "invalid email format",
				PhoneInvalidType:   "phone number must be a string",
				PhoneInvalidValue:  "invalid phone number format",
				URLInvalidType:     "URL must be a string",
				URLInvalidValue:    "invalid URL format",
				PostalInvalidType:  "postal code must be a string",
				PostalInvalidValue: "invalid postal code format",
				IDCardInvalidType:  "id card must be a string",
				IDCardInvalidValue: "invalid id card format",
			},
		},
	}

	currentValidationLocale  = "zh-CN"
	enabledValidationLocales = map[string]struct{}{
		"zh-CN": {},
		"en-US": {},
	}
)

// ValidationLocaleExists 检查 locale 是否存在对应 profile。
func ValidationLocaleExists(locale string) bool {
	validationMu.RLock()
	defer validationMu.RUnlock()
	_, ok := validationProfiles[locale]
	return ok
}

// ConfigureValidationLocales 配置默认 locale 与启用的 locale 集合。
// enabledLocales 为空时默认启用所有已注册 profile。
func ConfigureValidationLocales(defaultLocale string, enabledLocales []string) error {
	validationMu.Lock()
	defer validationMu.Unlock()

	nextEnabled := make(map[string]struct{})
	if len(enabledLocales) == 0 {
		for locale := range validationProfiles {
			nextEnabled[locale] = struct{}{}
		}
	} else {
		for _, locale := range enabledLocales {
			if _, ok := validationProfiles[locale]; !ok {
				return fmt.Errorf("validation locale not found: %s", locale)
			}
			nextEnabled[locale] = struct{}{}
		}
	}

	if len(nextEnabled) == 0 {
		return fmt.Errorf("enabled validation locales cannot be empty")
	}

	if defaultLocale == "" {
		if _, ok := nextEnabled[currentValidationLocale]; ok {
			defaultLocale = currentValidationLocale
		} else {
			for locale := range nextEnabled {
				defaultLocale = locale
				break
			}
		}
	}

	if _, ok := validationProfiles[defaultLocale]; !ok {
		return fmt.Errorf("validation locale not found: %s", defaultLocale)
	}
	if _, ok := nextEnabled[defaultLocale]; !ok {
		return fmt.Errorf("default validation locale is not enabled: %s", defaultLocale)
	}

	enabledValidationLocales = nextEnabled
	currentValidationLocale = defaultLocale
	return nil
}

// GetEnabledValidationLocales 获取当前启用的 locale 列表。
func GetEnabledValidationLocales() []string {
	validationMu.RLock()
	defer validationMu.RUnlock()

	locales := make([]string, 0, len(enabledValidationLocales))
	for locale := range enabledValidationLocales {
		locales = append(locales, locale)
	}
	sort.Strings(locales)
	return locales
}

// SetValidationLocale 设置当前校验规则 locale。
func SetValidationLocale(locale string) error {
	validationMu.Lock()
	defer validationMu.Unlock()

	if _, ok := validationProfiles[locale]; !ok {
		return fmt.Errorf("validation locale not found: %s", locale)
	}
	if _, ok := enabledValidationLocales[locale]; !ok {
		return fmt.Errorf("validation locale is not enabled: %s", locale)
	}

	currentValidationLocale = locale
	return nil
}

// GetValidationLocale 获取当前校验规则 locale。
func GetValidationLocale() string {
	validationMu.RLock()
	defer validationMu.RUnlock()
	return currentValidationLocale
}

// RegisterValidationProfile 注册自定义校验规则配置。
func RegisterValidationProfile(profile ValidationProfile) error {
	if profile.Locale == "" {
		return fmt.Errorf("validation profile locale cannot be empty")
	}
	if profile.EmailPattern == "" || profile.PhonePattern == "" || profile.URLPattern == "" || profile.PostalCodePattern == "" || profile.IDCardPattern == "" {
		return fmt.Errorf("validation profile patterns cannot be empty")
	}

	validationMu.Lock()
	defer validationMu.Unlock()
	validationProfiles[profile.Locale] = profile
	enabledValidationLocales[profile.Locale] = struct{}{}
	return nil
}

func currentValidationProfile() ValidationProfile {
	validationMu.RLock()
	defer validationMu.RUnlock()

	profile, ok := validationProfiles[currentValidationLocale]
	if ok {
		return profile
	}

	return validationProfiles["zh-CN"]
}

func validationProfileForLocale(locale string) ValidationProfile {
	if locale == "" {
		return currentValidationProfile()
	}

	validationMu.RLock()
	defer validationMu.RUnlock()

	if _, enabled := enabledValidationLocales[locale]; !enabled {
		if current, ok := validationProfiles[currentValidationLocale]; ok {
			return current
		}
	}

	if profile, ok := validationProfiles[locale]; ok {
		return profile
	}

	if fallback, ok := validationProfiles["zh-CN"]; ok {
		return fallback
	}

	return ValidationProfile{}
}

type validationLocaleContextKey struct{}

// WithValidationLocale 将 locale 写入上下文，供 Changeset/Validator 联动使用。
func WithValidationLocale(ctx context.Context, locale string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, validationLocaleContextKey{}, locale)
}

// ValidationLocaleFromContext 从上下文中获取 locale；未设置时回退全局 locale。
func ValidationLocaleFromContext(ctx context.Context) string {
	if ctx == nil {
		return GetValidationLocale()
	}
	if locale, ok := ctx.Value(validationLocaleContextKey{}).(string); ok && locale != "" {
		validationMu.RLock()
		_, enabled := enabledValidationLocales[locale]
		validationMu.RUnlock()
		if !enabled {
			return GetValidationLocale()
		}
		return locale
	}
	return GetValidationLocale()
}

// LocaleAwareValidator 可感知 locale 的验证器接口。
type LocaleAwareValidator interface {
	ValidateWithLocale(value interface{}, locale string) error
}

// RegexValidator 通用正则验证器
type RegexValidator struct {
	Pattern      string
	Code         string
	InvalidType  string
	InvalidValue string
}

func (v *RegexValidator) Validate(value interface{}) error {
	str, ok := value.(string)
	if !ok {
		code := v.Code
		if code == "" {
			code = "regex"
		}
		msg := v.InvalidType
		if msg == "" {
			msg = "字段必须是字符串"
		}
		return NewValidationError(code, msg)
	}

	if v.Pattern == "" {
		return NewValidationError("regex", "正则表达式不能为空")
	}

	re, err := regexp.Compile(v.Pattern)
	if err != nil {
		return NewValidationError("regex", fmt.Sprintf("无效的正则表达式: %v", err))
	}

	if !re.MatchString(str) {
		code := v.Code
		if code == "" {
			code = "regex"
		}
		msg := v.InvalidValue
		if msg == "" {
			msg = "字段格式不正确"
		}
		return NewValidationError(code, msg)
	}

	return nil
}

// EmailValidator 邮箱格式验证器
type EmailValidator struct {
	Locale string
}

func NewEmailValidatorForLocale(locale string) *EmailValidator {
	return &EmailValidator{Locale: locale}
}

func (v *EmailValidator) Validate(value interface{}) error {
	return v.ValidateWithLocale(value, "")
}

func (v *EmailValidator) ValidateWithLocale(value interface{}, locale string) error {
	effectiveLocale := locale
	if effectiveLocale == "" {
		effectiveLocale = v.Locale
	}

	profile := validationProfileForLocale(effectiveLocale)
	return (&RegexValidator{
		Pattern:      profile.EmailPattern,
		Code:         "email",
		InvalidType:  profile.Messages.EmailInvalidType,
		InvalidValue: profile.Messages.EmailInvalidValue,
	}).Validate(value)
}

// PhoneNumberValidator 手机号/电话号码格式验证器
// 它是 RegexValidator 的特化封装。
type PhoneNumberValidator struct {
	Locale string
}

func NewPhoneNumberValidatorForLocale(locale string) *PhoneNumberValidator {
	return &PhoneNumberValidator{Locale: locale}
}

func (v *PhoneNumberValidator) Validate(value interface{}) error {
	return v.ValidateWithLocale(value, "")
}

func (v *PhoneNumberValidator) ValidateWithLocale(value interface{}, locale string) error {
	effectiveLocale := locale
	if effectiveLocale == "" {
		effectiveLocale = v.Locale
	}

	profile := validationProfileForLocale(effectiveLocale)
	return (&RegexValidator{
		Pattern:      profile.PhonePattern,
		Code:         "phone",
		InvalidType:  profile.Messages.PhoneInvalidType,
		InvalidValue: profile.Messages.PhoneInvalidValue,
	}).Validate(value)
}

// URLValidator URL 格式验证器（http/https）
// 它是 RegexValidator 的特化封装。
type URLValidator struct {
	Locale string
}

func NewURLValidatorForLocale(locale string) *URLValidator {
	return &URLValidator{Locale: locale}
}

func (v *URLValidator) Validate(value interface{}) error {
	return v.ValidateWithLocale(value, "")
}

func (v *URLValidator) ValidateWithLocale(value interface{}, locale string) error {
	effectiveLocale := locale
	if effectiveLocale == "" {
		effectiveLocale = v.Locale
	}

	profile := validationProfileForLocale(effectiveLocale)
	return (&RegexValidator{
		Pattern:      profile.URLPattern,
		Code:         "url",
		InvalidType:  profile.Messages.URLInvalidType,
		InvalidValue: profile.Messages.URLInvalidValue,
	}).Validate(value)
}

// PostalCodeValidator 邮政编码验证器（中国大陆 6 位）
// 它是 RegexValidator 的特化封装。
type PostalCodeValidator struct {
	Locale string
}

func NewPostalCodeValidatorForLocale(locale string) *PostalCodeValidator {
	return &PostalCodeValidator{Locale: locale}
}

func (v *PostalCodeValidator) Validate(value interface{}) error {
	return v.ValidateWithLocale(value, "")
}

func (v *PostalCodeValidator) ValidateWithLocale(value interface{}, locale string) error {
	effectiveLocale := locale
	if effectiveLocale == "" {
		effectiveLocale = v.Locale
	}

	profile := validationProfileForLocale(effectiveLocale)
	return (&RegexValidator{
		Pattern:      profile.PostalCodePattern,
		Code:         "postal_code",
		InvalidType:  profile.Messages.PostalInvalidType,
		InvalidValue: profile.Messages.PostalInvalidValue,
	}).Validate(value)
}

// IDCardValidator 身份证号验证器（中国大陆 15/18 位）
// 它是 RegexValidator 的特化封装。
type IDCardValidator struct {
	Locale string
}

func NewIDCardValidatorForLocale(locale string) *IDCardValidator {
	return &IDCardValidator{Locale: locale}
}

func (v *IDCardValidator) Validate(value interface{}) error {
	return v.ValidateWithLocale(value, "")
}

func (v *IDCardValidator) ValidateWithLocale(value interface{}, locale string) error {
	effectiveLocale := locale
	if effectiveLocale == "" {
		effectiveLocale = v.Locale
	}

	profile := validationProfileForLocale(effectiveLocale)
	return (&RegexValidator{
		Pattern:      profile.IDCardPattern,
		Code:         "id_card",
		InvalidType:  profile.Messages.IDCardInvalidType,
		InvalidValue: profile.Messages.IDCardInvalidValue,
	}).Validate(value)
}

// RangeValidator 数值范围验证器（闭区间）
type RangeValidator struct {
	Min *float64
	Max *float64
}

func NewRangeValidator(min, max float64) *RangeValidator {
	return &RangeValidator{Min: &min, Max: &max}
}

func NewMinRangeValidator(min float64) *RangeValidator {
	return &RangeValidator{Min: &min}
}

func NewMaxRangeValidator(max float64) *RangeValidator {
	return &RangeValidator{Max: &max}
}

func (v *RangeValidator) Validate(value interface{}) error {
	number, ok := asFloat64(value)
	if !ok {
		return NewValidationError("range", "字段必须是数字")
	}

	if v.Min != nil && number < *v.Min {
		return NewValidationError("range", fmt.Sprintf("字段值不能小于 %v", *v.Min))
	}

	if v.Max != nil && number > *v.Max {
		return NewValidationError("range", fmt.Sprintf("字段值不能大于 %v", *v.Max))
	}

	return nil
}

func asFloat64(value interface{}) (float64, bool) {
	switch n := value.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// MinLengthValidator 最小长度验证器
type MinLengthValidator struct {
	Length int
}

func (v *MinLengthValidator) Validate(value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return NewValidationError("min_length", "字段必须是字符串")
	}

	if len(str) < v.Length {
		return NewValidationError("min_length", fmt.Sprintf("字段长度不能小于 %d", v.Length))
	}
	return nil
}

// MaxLengthValidator 最大长度验证器
type MaxLengthValidator struct {
	Length int
}

func (v *MaxLengthValidator) Validate(value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return NewValidationError("max_length", "字段必须是字符串")
	}

	if len(str) > v.Length {
		return NewValidationError("max_length", fmt.Sprintf("字段长度不能大于 %d", v.Length))
	}
	return nil
}
