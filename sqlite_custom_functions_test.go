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
