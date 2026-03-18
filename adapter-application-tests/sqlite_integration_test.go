package adapter_tests

import (
	"database/sql"
	"os"
	"testing"

	db "github.com/eit-cms/eit-db"
)

type User struct {
	ID    int64
	Name  string
	Email string
	Age   int
}

type Post struct {
	ID     int64
	Title  string
	UserID int64
}

func setupTestDB(t *testing.T) (*sql.DB, func()) {
	tmpFile := "./test.db"
	os.Remove(tmpFile)

	config := &db.Config{
		Database: tmpFile,
	}

	adapter, err := db.NewSQLiteAdapter(config)
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}

	rawConn := adapter.GetRawConn()
	sqlDB, ok := rawConn.(*sql.DB)
	if !ok || sqlDB == nil {
		t.Fatalf("expected *sql.DB from GetRawConn, got %T", rawConn)
	}

	if _, err := sqlDB.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT NOT NULL UNIQUE,
			age INTEGER NOT NULL
		)
	`); err != nil {
		t.Fatalf("create users table failed: %v", err)
	}

	if _, err := sqlDB.Exec(`
		CREATE TABLE IF NOT EXISTS posts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			user_id INTEGER NOT NULL
		)
	`); err != nil {
		t.Fatalf("create posts table failed: %v", err)
	}

	return sqlDB, func() {
		adapter.Close()
		os.Remove(tmpFile)
	}
}

func TestBasicCRUD(t *testing.T) {
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	res, err := sqlDB.Exec("INSERT INTO users(name, email, age) VALUES(?, ?, ?)", "Alice", "alice@test.com", 25)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	userID, _ := res.LastInsertId()

	var r User
	if err := sqlDB.QueryRow("SELECT id, name, email, age FROM users WHERE id = ?", userID).Scan(&r.ID, &r.Name, &r.Email, &r.Age); err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if r.Name != "Alice" {
		t.Errorf("Expected Alice, got %s", r.Name)
	}

	if _, err := sqlDB.Exec("UPDATE users SET age = ? WHERE id = ?", 26, userID); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if _, err := sqlDB.Exec("DELETE FROM users WHERE id = ?", userID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func TestQueryWhere(t *testing.T) {
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	if _, err := sqlDB.Exec("INSERT INTO users(name, email, age) VALUES(?, ?, ?), (?, ?, ?)",
		"Alice", "alice@x.com", 25,
		"Bob", "bob@x.com", 30,
	); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	var count int
	if err := sqlDB.QueryRow("SELECT COUNT(*) FROM users WHERE age > ?", 27).Scan(&count); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1, got %d", count)
	}
}

func TestWhereIN(t *testing.T) {
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	if _, err := sqlDB.Exec("INSERT INTO users(name, email, age) VALUES(?, ?, ?), (?, ?, ?), (?, ?, ?)",
		"A", "a@test.com", 20,
		"B", "b@test.com", 21,
		"C", "c@test.com", 22,
	); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	var count int
	if err := sqlDB.QueryRow("SELECT COUNT(*) FROM users WHERE name IN (?, ?)", "A", "B").Scan(&count); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2, got %d", count)
	}
}

func TestBetween(t *testing.T) {
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	if _, err := sqlDB.Exec("INSERT INTO users(name, email, age) VALUES(?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)",
		"U0", "u0@test.com", 20,
		"U1", "u1@test.com", 25,
		"U2", "u2@test.com", 30,
		"U3", "u3@test.com", 35,
		"U4", "u4@test.com", 40,
	); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	var count int
	if err := sqlDB.QueryRow("SELECT COUNT(*) FROM users WHERE age BETWEEN ? AND ?", 25, 35).Scan(&count); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3, got %d", count)
	}
}

func TestDistinct(t *testing.T) {
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	if _, err := sqlDB.Exec("INSERT INTO users(name, email, age) VALUES(?, ?, ?), (?, ?, ?), (?, ?, ?)",
		"A1", "a1@x.com", 25,
		"A2", "a2@x.com", 25,
		"B1", "b1@x.com", 30,
	); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	rows, err := sqlDB.Query("SELECT DISTINCT age FROM users")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 distinct ages, got %d", count)
	}
}

func TestWindowFunction(t *testing.T) {
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	res, err := sqlDB.Exec("INSERT INTO users(name, email, age) VALUES(?, ?, ?)", "Alice", "w@test.com", 25)
	if err != nil {
		t.Fatalf("insert user failed: %v", err)
	}
	userID, _ := res.LastInsertId()

	if _, err := sqlDB.Exec("INSERT INTO posts(title, user_id) VALUES(?, ?), (?, ?), (?, ?)",
		"P0", userID,
		"P1", userID,
		"P2", userID,
	); err != nil {
		t.Fatalf("insert posts failed: %v", err)
	}

	type R struct {
		Title  string
		RowNum int
	}
	rows, err := sqlDB.Query("SELECT title, ROW_NUMBER() OVER (ORDER BY id) as row_num FROM posts")
	if err != nil {
		t.Fatalf("window query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var r R
		if err := rows.Scan(&r.Title, &r.RowNum); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		count++
	}

	if count != 3 {
		t.Errorf("Expected 3, got %d", count)
	}
}

func TestCTE(t *testing.T) {
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	if _, err := sqlDB.Exec("INSERT INTO users(name, email, age) VALUES(?, ?, ?), (?, ?, ?)",
		"X", "x1@c.com", 25,
		"Y", "y1@c.com", 35,
	); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	query := "WITH adults AS (SELECT name, age FROM users WHERE age > 30) SELECT * FROM adults"
	rows, err := sqlDB.Query(query)
	if err != nil {
		t.Fatalf("cte query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1, got %d", count)
	}
}

func TestUpsert(t *testing.T) {
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	res, err := sqlDB.Exec("INSERT INTO users(name, email, age) VALUES(?, ?, ?)", "Test", "upsert@test.com", 20)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	userID, _ := res.LastInsertId()

	query := "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?) ON CONFLICT(email) DO UPDATE SET name = excluded.name, age = excluded.age"
	if _, err := sqlDB.Exec(query, userID, "Updated", "upsert@test.com", 25); err != nil {
		t.Fatalf("upsert failed: %v", err)
	}

	var u User
	if err := sqlDB.QueryRow("SELECT id, name, email, age FROM users WHERE id = ?", userID).Scan(&u.ID, &u.Name, &u.Email, &u.Age); err != nil {
		t.Fatalf("select failed: %v", err)
	}

	if u.Name != "Updated" || u.Age != 25 {
		t.Errorf("Upsert failed: %v %d", u.Name, u.Age)
	}
}

func TestTransaction(t *testing.T) {
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO users(name, email, age) VALUES(?, ?, ?)", "TX", "tx@test.com", 30); err != nil {
		t.Fatalf("insert in tx failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	var found User
	if err := sqlDB.QueryRow("SELECT id, name, email, age FROM users WHERE email = ?", "tx@test.com").Scan(&found.ID, &found.Name, &found.Email, &found.Age); err != nil {
		t.Fatalf("query committed row failed: %v", err)
	}
	if found.Name != "TX" {
		t.Error("Commit failed")
	}

	tx, err = sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO users(name, email, age) VALUES(?, ?, ?)", "RB", "rb@test.com", 30); err != nil {
		t.Fatalf("insert in rollback tx failed: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	var count int
	if err := sqlDB.QueryRow("SELECT COUNT(*) FROM users WHERE email = ?", "rb@test.com").Scan(&count); err != nil {
		t.Fatalf("query rollback row failed: %v", err)
	}
	if count != 0 {
		t.Error("Rollback failed")
	}
}
