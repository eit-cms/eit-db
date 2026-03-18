package db

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestSQLiteCustomFunction(t *testing.T) {
	err := RegisterCustomSQLiteDriver("sqlite3_test_custom", map[string]interface{}{
		"UPPER_GO": func(s string) string { return strings.ToUpper(s) },
		"STR_LEN":  func(s string) int { return len(s) },
	})
	if err != nil {
		t.Fatalf("Failed to register custom driver: %v", err)
	}

	db, _ := sql.Open("sqlite3_test_custom", ":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("INSERT INTO test_users (name) VALUES ('hello'), ('world')")

	rows, _ := db.Query("SELECT UPPER_GO(name) as upper_name FROM test_users ORDER BY id")
	defer rows.Close()

	expected := []string{"HELLO", "WORLD"}
	i := 0
	for rows.Next() {
		var upperName string
		rows.Scan(&upperName)
		if upperName != expected[i] {
			t.Errorf("Expected %s, got %s", expected[i], upperName)
		}
		i++
	}
}

func TestSQLiteCustomFunctionInWhere(t *testing.T) {
	RegisterCustomSQLiteDriver("sqlite3_test_where", map[string]interface{}{
		"STR_LEN": func(s string) int { return len(s) },
	})

	db, _ := sql.Open("sqlite3_test_where", ":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	for _, name := range []string{"a", "ab", "abc", "abcd", "abcde"} {
		db.Exec("INSERT INTO users (name) VALUES (?)", name)
	}

	rows, _ := db.Query("SELECT name FROM users WHERE STR_LEN(name) > 3")
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

func TestSQLiteJSONFallbackFunction(t *testing.T) {
	err := RegisterSQLiteJSONFallbackDriver("sqlite3_test_json_fallback", nil)
	if err != nil {
		t.Fatalf("Failed to register JSON fallback driver: %v", err)
	}

	db, err := sql.Open("sqlite3_test_json_fallback", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE docs (id INTEGER PRIMARY KEY, payload TEXT)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	jsonPayload := `{"user":{"name":"alice","roles":["admin","editor"]}}`
	if _, err := db.Exec("INSERT INTO docs (payload) VALUES (?)", jsonPayload); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	var name string
	if err := db.QueryRow("SELECT JSON_EXTRACT_GO(payload, '$.user.name') FROM docs WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("query name failed: %v", err)
	}
	if name != "alice" {
		t.Fatalf("expected alice, got %s", name)
	}

	var role string
	if err := db.QueryRow("SELECT JSON_EXTRACT_GO(payload, '$.user.roles[1]') FROM docs WHERE id = 1").Scan(&role); err != nil {
		t.Fatalf("query role failed: %v", err)
	}
	if role != "editor" {
		t.Fatalf("expected editor, got %s", role)
	}
}
