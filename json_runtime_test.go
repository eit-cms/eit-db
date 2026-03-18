package db

import "testing"

func TestNormalizePostgresJSONType(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{name: "json lowercase", input: "json", expected: "JSON"},
		{name: "jsonb lowercase", input: "jsonb", expected: "JSONB"},
		{name: "json uppercase", input: "JSON", expected: "JSON"},
		{name: "jsonb mixed", input: "JsonB", expected: "JSONB"},
		{name: "invalid", input: "text", expected: ""},
		{name: "non-string", input: 1, expected: ""},
	}

	for _, tc := range testCases {
		got := normalizePostgresJSONType(tc.input)
		if got != tc.expected {
			t.Fatalf("%s: expected %q, got %q", tc.name, tc.expected, got)
		}
	}
}

func TestPostgresJSONTypeFromConfigOptions(t *testing.T) {
	adapter := &PostgreSQLAdapter{config: &Config{Options: map[string]interface{}{}}}
	if got := adapter.PostgresJSONType(); got != "JSONB" {
		t.Fatalf("expected default JSONB, got %s", got)
	}

	adapter.config.Options["postgres_json_type"] = "json"
	if got := adapter.PostgresJSONType(); got != "JSON" {
		t.Fatalf("expected JSON from postgres_json_type option, got %s", got)
	}

	adapter.config.Options = map[string]interface{}{"json_type": "jsonb"}
	if got := adapter.PostgresJSONType(); got != "JSONB" {
		t.Fatalf("expected JSONB from json_type option, got %s", got)
	}
}

func TestMapPostgresTypeRespectsAdapterJSONType(t *testing.T) {
	pgJSON := &PostgreSQLAdapter{config: &Config{Options: map[string]interface{}{"postgres_json_type": "json"}}}
	if got := mapPostgresType(TypeJSON, pgJSON); got != "JSON" {
		t.Fatalf("expected JSON when configured, got %s", got)
	}

	pgJSONB := &PostgreSQLAdapter{config: &Config{Options: map[string]interface{}{"postgres_json_type": "jsonb"}}}
	if got := mapPostgresType(TypeJSON, pgJSONB); got != "JSONB" {
		t.Fatalf("expected JSONB when configured, got %s", got)
	}

	if got := mapPostgresType(TypeJSON, nil); got != "JSONB" {
		t.Fatalf("expected JSONB fallback without adapter, got %s", got)
	}
}
