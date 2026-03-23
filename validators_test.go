package db

import (
	"context"
	"slices"
	"testing"
)

func TestRegexValidator(t *testing.T) {
	validator := &RegexValidator{
		Pattern:      `^[a-z]+$`,
		Code:         "regex",
		InvalidType:  "must be string",
		InvalidValue: "invalid format",
	}

	if err := validator.Validate("abc"); err != nil {
		t.Fatalf("expected valid value, got error: %v", err)
	}

	if err := validator.Validate("abc123"); err == nil {
		t.Fatalf("expected format error for abc123")
	}

	if err := validator.Validate(123); err == nil {
		t.Fatalf("expected type error for non-string input")
	}
}

func TestRangeValidator(t *testing.T) {
	validator := NewRangeValidator(18, 60)

	if err := validator.Validate(18); err != nil {
		t.Fatalf("expected boundary min to pass, got: %v", err)
	}
	if err := validator.Validate(60); err != nil {
		t.Fatalf("expected boundary max to pass, got: %v", err)
	}
	if err := validator.Validate(35.5); err != nil {
		t.Fatalf("expected in-range float to pass, got: %v", err)
	}

	if err := validator.Validate(17); err == nil {
		t.Fatalf("expected lower-bound violation")
	}
	if err := validator.Validate(61); err == nil {
		t.Fatalf("expected upper-bound violation")
	}
	if err := validator.Validate("20"); err == nil {
		t.Fatalf("expected type error for non-numeric input")
	}
}

func TestValidationLocaleHelpersAndRangeBoundaries(t *testing.T) {
	if !ValidationLocaleExists("zh-CN") {
		t.Fatalf("expected zh-CN validation profile to exist")
	}
	if ValidationLocaleExists("missing-locale") {
		t.Fatalf("expected missing locale to be absent")
	}

	if err := NewMinRangeValidator(10).Validate(9); err == nil {
		t.Fatalf("expected min-only range validator to reject 9")
	}
	if err := NewMinRangeValidator(10).Validate(10); err != nil {
		t.Fatalf("expected min-only range validator to accept 10, got %v", err)
	}
	if err := NewMaxRangeValidator(20).Validate(21); err == nil {
		t.Fatalf("expected max-only range validator to reject 21")
	}
	if err := NewMaxRangeValidator(20).Validate(uint32(20)); err != nil {
		t.Fatalf("expected max-only range validator to accept uint32, got %v", err)
	}

	if err := (&MinLengthValidator{Length: 3}).Validate("ab"); err == nil {
		t.Fatalf("expected min length validator to reject short string")
	}
	if err := (&MaxLengthValidator{Length: 3}).Validate("abcd"); err == nil {
		t.Fatalf("expected max length validator to reject long string")
	}
	if err := (&MinLengthValidator{Length: 3}).Validate("abcd"); err != nil {
		t.Fatalf("expected min length validator to accept long enough string, got %v", err)
	}
	if err := (&MaxLengthValidator{Length: 3}).Validate("abc"); err != nil {
		t.Fatalf("expected max length validator to accept boundary string, got %v", err)
	}

	if err := NewEmailValidatorForLocale("en-US").Validate("user@example.com"); err != nil {
		t.Fatalf("expected locale-specific email validator to work, got %v", err)
	}
	if err := NewPostalCodeValidatorForLocale("en-US").Validate("94105"); err != nil {
		t.Fatalf("expected locale-specific postal validator to work, got %v", err)
	}
	if err := NewIDCardValidatorForLocale("en-US").Validate("123-45-6789"); err != nil {
		t.Fatalf("expected locale-specific id validator to work, got %v", err)
	}
}

func TestPhoneAndEmailValidatorsAsRegexSpecialCases(t *testing.T) {
	emailValidator := &EmailValidator{}
	if err := emailValidator.Validate("user@example.com"); err != nil {
		t.Fatalf("expected valid email, got: %v", err)
	}
	if err := emailValidator.Validate("bad-email"); err == nil {
		t.Fatalf("expected invalid email to fail")
	}

	phoneValidator := &PhoneNumberValidator{}
	validPhones := []string{"13800138000", "+86 138-0013-8000", "+1 (415) 555-2671"}
	for _, phone := range validPhones {
		if err := phoneValidator.Validate(phone); err != nil {
			t.Fatalf("expected valid phone %q, got: %v", phone, err)
		}
	}

	invalidPhones := []string{"abc", "12", "++86"}
	for _, phone := range invalidPhones {
		if err := phoneValidator.Validate(phone); err == nil {
			t.Fatalf("expected invalid phone %q to fail", phone)
		}
	}
}

func TestURLValidatorAsRegexSpecialCase(t *testing.T) {
	validator := &URLValidator{}

	validURLs := []string{
		"https://example.com",
		"http://localhost:8080/api/v1/users?id=1",
		"https://sub.domain.example/path/to/page?x=1&y=2#anchor",
	}
	for _, url := range validURLs {
		if err := validator.Validate(url); err != nil {
			t.Fatalf("expected valid URL %q, got: %v", url, err)
		}
	}

	invalidURLs := []string{"ftp://example.com", "example.com", "http://"}
	for _, url := range invalidURLs {
		if err := validator.Validate(url); err == nil {
			t.Fatalf("expected invalid URL %q to fail", url)
		}
	}
}

func TestPostalCodeValidatorAsRegexSpecialCase(t *testing.T) {
	validator := &PostalCodeValidator{}

	if err := validator.Validate("100000"); err != nil {
		t.Fatalf("expected valid postal code, got: %v", err)
	}

	invalidPostalCodes := []string{"ABCDE", "10000", "1000000"}
	for _, code := range invalidPostalCodes {
		if err := validator.Validate(code); err == nil {
			t.Fatalf("expected invalid postal code %q to fail", code)
		}
	}
}

func TestIDCardValidatorAsRegexSpecialCase(t *testing.T) {
	validator := &IDCardValidator{}

	validIDs := []string{"11010519491231002X", "110105194912310021", "130503670401001"}
	for _, id := range validIDs {
		if err := validator.Validate(id); err != nil {
			t.Fatalf("expected valid id card %q, got: %v", id, err)
		}
	}

	invalidIDs := []string{"123", "11010519491231002A", "ABCDEFGHIJKLMNO"}
	for _, id := range invalidIDs {
		if err := validator.Validate(id); err == nil {
			t.Fatalf("expected invalid id card %q to fail", id)
		}
	}
}

func TestValidationLocaleSwitchAffectsRules(t *testing.T) {
	prev := GetValidationLocale()
	defer func() {
		_ = SetValidationLocale(prev)
	}()

	phoneValidator := &PhoneNumberValidator{}
	postalValidator := &PostalCodeValidator{}
	idValidator := &IDCardValidator{}

	if err := SetValidationLocale("zh-CN"); err != nil {
		t.Fatalf("failed to set zh-CN locale: %v", err)
	}
	if err := phoneValidator.Validate("13800138000"); err != nil {
		t.Fatalf("expected zh-CN phone to pass, got: %v", err)
	}
	if err := postalValidator.Validate("100000"); err != nil {
		t.Fatalf("expected zh-CN postal code to pass, got: %v", err)
	}
	if err := idValidator.Validate("11010519491231002X"); err != nil {
		t.Fatalf("expected zh-CN id card to pass, got: %v", err)
	}

	if err := SetValidationLocale("en-US"); err != nil {
		t.Fatalf("failed to set en-US locale: %v", err)
	}
	if err := phoneValidator.Validate("+1 (415) 555-2671"); err != nil {
		t.Fatalf("expected en-US phone to pass, got: %v", err)
	}
	if err := postalValidator.Validate("94105-1234"); err != nil {
		t.Fatalf("expected en-US ZIP to pass, got: %v", err)
	}
	if err := idValidator.Validate("123-45-6789"); err != nil {
		t.Fatalf("expected en-US id format to pass, got: %v", err)
	}

	if err := idValidator.Validate("11010519491231002X"); err == nil {
		t.Fatalf("expected zh-CN id card to fail under en-US locale")
	}
}

func TestSetValidationLocaleRejectsUnknownLocale(t *testing.T) {
	prev := GetValidationLocale()
	defer func() {
		_ = SetValidationLocale(prev)
	}()

	if err := SetValidationLocale("unknown-locale"); err == nil {
		t.Fatalf("expected unknown locale to fail")
	}
}

func TestRegisterValidationProfile(t *testing.T) {
	prev := GetValidationLocale()
	defer func() {
		_ = SetValidationLocale(prev)
	}()

	profile := ValidationProfile{
		Locale:            "test-locale",
		EmailPattern:      `^[a-z]+@[a-z]+\.[a-z]+$`,
		PhonePattern:      `^T-[0-9]{4}$`,
		URLPattern:        `^https://t\.[a-z]+$`,
		PostalCodePattern: `^P-[0-9]{3}$`,
		IDCardPattern:     `^I-[A-Z]{2}$`,
		Messages: ValidationMessages{
			EmailInvalidType:   "email must be string",
			EmailInvalidValue:  "invalid email",
			PhoneInvalidType:   "phone must be string",
			PhoneInvalidValue:  "invalid phone",
			URLInvalidType:     "url must be string",
			URLInvalidValue:    "invalid url",
			PostalInvalidType:  "postal must be string",
			PostalInvalidValue: "invalid postal",
			IDCardInvalidType:  "id must be string",
			IDCardInvalidValue: "invalid id",
		},
	}

	if err := RegisterValidationProfile(profile); err != nil {
		t.Fatalf("failed to register custom profile: %v", err)
	}
	if err := SetValidationLocale("test-locale"); err != nil {
		t.Fatalf("failed to switch to custom locale: %v", err)
	}

	phoneValidator := &PhoneNumberValidator{}
	postalValidator := &PostalCodeValidator{}
	idValidator := &IDCardValidator{}

	if err := phoneValidator.Validate("T-1234"); err != nil {
		t.Fatalf("expected custom phone to pass, got: %v", err)
	}
	if err := postalValidator.Validate("P-123"); err != nil {
		t.Fatalf("expected custom postal code to pass, got: %v", err)
	}
	if err := idValidator.Validate("I-AB"); err != nil {
		t.Fatalf("expected custom id to pass, got: %v", err)
	}
}

func TestPerValidatorLocaleOverride(t *testing.T) {
	prev := GetValidationLocale()
	defer func() {
		_ = SetValidationLocale(prev)
	}()

	if err := SetValidationLocale("zh-CN"); err != nil {
		t.Fatalf("failed to set global zh-CN locale: %v", err)
	}

	usPhoneValidator := NewPhoneNumberValidatorForLocale("en-US")
	if err := usPhoneValidator.Validate("+1 (415) 555-2671"); err != nil {
		t.Fatalf("expected en-US phone validator to pass under zh-CN global locale, got: %v", err)
	}

	if err := usPhoneValidator.Validate("13800138000"); err == nil {
		t.Fatalf("expected zh-CN phone to fail for en-US scoped validator")
	}

	usURLValidator := NewURLValidatorForLocale("en-US")
	if err := usURLValidator.Validate("https://example.com"); err != nil {
		t.Fatalf("expected en-US URL validator to pass, got: %v", err)
	}
}

func TestConfigureValidationLocales(t *testing.T) {
	prevLocale := GetValidationLocale()
	prevEnabled := GetEnabledValidationLocales()
	defer func() {
		_ = ConfigureValidationLocales(prevLocale, prevEnabled)
	}()

	if err := ConfigureValidationLocales("zh-CN", []string{"zh-CN", "en-US"}); err != nil {
		t.Fatalf("expected valid locale config, got: %v", err)
	}

	enabled := GetEnabledValidationLocales()
	if !slices.Contains(enabled, "zh-CN") || !slices.Contains(enabled, "en-US") {
		t.Fatalf("expected zh-CN and en-US enabled, got: %v", enabled)
	}

	if err := ConfigureValidationLocales("en-US", []string{"zh-CN"}); err == nil {
		t.Fatalf("expected default locale not in enabled set to fail")
	}

	if err := ConfigureValidationLocales("zh-CN", []string{"unknown-locale"}); err == nil {
		t.Fatalf("expected unknown enabled locale to fail")
	}
}

func TestValidationLocaleFromContextFallsBackWhenDisabled(t *testing.T) {
	prevLocale := GetValidationLocale()
	prevEnabled := GetEnabledValidationLocales()
	defer func() {
		_ = ConfigureValidationLocales(prevLocale, prevEnabled)
	}()

	if err := ConfigureValidationLocales("zh-CN", []string{"zh-CN"}); err != nil {
		t.Fatalf("failed to configure locales: %v", err)
	}

	ctx := WithValidationLocale(context.Background(), "en-US")
	locale := ValidationLocaleFromContext(ctx)
	if locale != "zh-CN" {
		t.Fatalf("expected fallback locale zh-CN, got: %s", locale)
	}
}
