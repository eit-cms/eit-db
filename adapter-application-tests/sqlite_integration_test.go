package adapter_tests

import (
	"os"
	"testing"

	db "github.com/eit-cms/eit-db"
	"gorm.io/gorm"
)

type User struct {
	ID    uint   `gorm:"primaryKey"`
	Name  string
	Email string `gorm:"uniqueIndex"`
	Age   int
}

type Post struct {
	ID     uint
	Title  string
	UserID uint
}

func setupTestDB(t *testing.T) (*gorm.DB, func()) {
	tmpFile := "./test.db"
	os.Remove(tmpFile)

	config := &db.Config{
		Database: tmpFile,
	}

	adapter, err := db.NewSQLiteAdapter(config)
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}

	gormDB := adapter.GetRawConn().(*gorm.DB)
	if err := gormDB.AutoMigrate(&User{}, &Post{}); err != nil {
		t.Fatalf("Migrate failed: %v", err)
	}

	return gormDB, func() {
		adapter.Close()
		os.Remove(tmpFile)
	}
}

func TestBasicCRUD(t *testing.T) {
	gormDB, cleanup := setupTestDB(t)
	defer cleanup()

	user := &User{Name: "Alice", Email: "alice@test.com", Age: 25}

	if err := gormDB.Create(user).Error; err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	var r User
	if err := gormDB.First(&r, user.ID).Error; err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if r.Name != "Alice" {
		t.Errorf("Expected Alice, got %s", r.Name)
	}

	r.Age = 26
	if err := gormDB.Save(&r).Error; err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if err := gormDB.Delete(&r).Error; err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func TestQueryWhere(t *testing.T) {
	gormDB, cleanup := setupTestDB(t)
	defer cleanup()

	users := []User{
		{Name: "Alice", Email: "alice@x.com", Age: 25},
		{Name: "Bob", Email: "bob@x.com", Age: 30},
	}
	for _, u := range users {
		gormDB.Create(&u)
	}

	var results []User
	gormDB.Where("age > ?", 27).Find(&results)
	if len(results) != 1 {
		t.Errorf("Expected 1, got %d", len(results))
	}
}

func TestWhereIN(t *testing.T) {
	gormDB, cleanup := setupTestDB(t)
	defer cleanup()

	for i, name := range []string{"A", "B", "C"} {
		gormDB.Create(&User{Name: name, Email: "e" + string(rune(i)), Age: 20 + i})
	}

	var results []User
	gormDB.Where("name IN ?", []string{"A", "B"}).Find(&results)
	if len(results) != 2 {
		t.Errorf("Expected 2, got %d", len(results))
	}
}

func TestBetween(t *testing.T) {
	gormDB, cleanup := setupTestDB(t)
	defer cleanup()

	for i := 0; i < 5; i++ {
		gormDB.Create(&User{Name: "U" + string(rune(48+i)), Email: "b" + string(rune(48+i)), Age: 20 + i*5})
	}

	var results []User
	gormDB.Where("age BETWEEN ? AND ?", 25, 35).Find(&results)
	if len(results) < 1 {
		t.Errorf("Expected at least 1, got %d", len(results))
	}
}

func TestDistinct(t *testing.T) {
	gormDB, cleanup := setupTestDB(t)
	defer cleanup()

	gormDB.Create(&User{Name: "A1", Email: "a1@x.com", Age: 25})
	gormDB.Create(&User{Name: "A2", Email: "a2@x.com", Age: 25})
	gormDB.Create(&User{Name: "B1", Email: "b1@x.com", Age: 30})

	var ages []int
	gormDB.Model(&User{}).Distinct("age").Pluck("age", &ages)

	if len(ages) != 2 {
		t.Errorf("Expected 2 distinct ages, got %d", len(ages))
	}
}

func TestWindowFunction(t *testing.T) {
	gormDB, cleanup := setupTestDB(t)
	defer cleanup()

	u := &User{Name: "Alice", Email: "w@test.com", Age: 25}
	gormDB.Create(u)

	for i := 0; i < 3; i++ {
		gormDB.Create(&Post{Title: "P" + string(rune(48+i)), UserID: u.ID})
	}

	type R struct {
		Title  string
		RowNum int
	}
	var results []R
	gormDB.Raw("SELECT title, ROW_NUMBER() OVER (ORDER BY id) as row_num FROM posts").Scan(&results)

	if len(results) != 3 {
		t.Errorf("Expected 3, got %d", len(results))
	}
}

func TestCTE(t *testing.T) {
	gormDB, cleanup := setupTestDB(t)
	defer cleanup()

	users := []User{
		{Name: "X", Email: "x1@c.com", Age: 25},
		{Name: "Y", Email: "y1@c.com", Age: 35},
	}
	for _, u := range users {
		gormDB.Create(&u)
	}

	type R struct {
		Name string
		Age  int
	}
	var results []R

	query := "WITH adults AS (SELECT name, age FROM users WHERE age > 30) SELECT * FROM adults"
	gormDB.Raw(query).Scan(&results)

	if len(results) != 1 {
		t.Errorf("Expected 1, got %d", len(results))
	}
}

func TestUpsert(t *testing.T) {
	gormDB, cleanup := setupTestDB(t)
	defer cleanup()

	user := &User{Name: "Test", Email: "upsert@test.com", Age: 20}
	gormDB.Create(user)

	query := "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?) ON CONFLICT(email) DO UPDATE SET name = excluded.name, age = excluded.age"
	gormDB.Exec(query, user.ID, "Updated", "upsert@test.com", 25)

	var u User
	gormDB.First(&u, user.ID)

	if u.Name != "Updated" || u.Age != 25 {
		t.Errorf("Upsert failed: %v %d", u.Name, u.Age)
	}
}

func TestTransaction(t *testing.T) {
	gormDB, cleanup := setupTestDB(t)
	defer cleanup()

	tx := gormDB.Begin()
	u := &User{Name: "TX", Email: "tx@test.com", Age: 30}
	tx.Create(u)
	tx.Commit()

	var found User
	gormDB.Where("email = ?", "tx@test.com").First(&found)
	if found.Name != "TX" {
		t.Error("Commit failed")
	}

	tx = gormDB.Begin()
	u2 := &User{Name: "RB", Email: "rb@test.com", Age: 30}
	tx.Create(u2)
	tx.Rollback()

	var notFound User
	err := gormDB.Where("email = ?", "rb@test.com").First(&notFound).Error
	if err == nil {
		t.Error("Rollback failed")
	}
}

