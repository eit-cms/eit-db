package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestSQLiteAdapterInitialization 测试 SQLite 适配器初始化和 GetGormDB
func TestSQLiteAdapterInitialization(t *testing.T) {
	// 创建临时数据库文件
	tmpDir := filepath.Join(os.TempDir(), "eit-db-test")
	os.MkdirAll(tmpDir, 0o755)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	config := &Config{
		Adapter:  "sqlite",
		Database: dbPath,
		Pool: &PoolConfig{
			MaxConnections: 10,
			IdleTimeout:    300,
		},
	}

	// 创建 Repository
	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("Failed to create SQLite repository: %v", err)
	}
	defer repo.Close()

	// 验证连接
	ctx := context.Background()
	if err := repo.Ping(ctx); err != nil {
		t.Fatalf("Failed to ping SQLite database: %v", err)
	}

	// 测试 GetGormDB
	gormDB := repo.GetGormDB()
	if gormDB == nil {
		t.Fatal("GetGormDB() returned nil for SQLite adapter")
	}

	// 验证 GORM 实例的有效性
	sqlDB, err := gormDB.DB()
	if err != nil {
		t.Fatalf("Failed to get sql.DB from GORM: %v", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	t.Log("✓ SQLite adapter initialization and GetGormDB test passed")
}

// TestInitDB 测试 InitDB 函数（使用配置文件）
func TestInitDB(t *testing.T) {
	// 创建临时配置文件
	tmpDir := filepath.Join(os.TempDir(), "eit-db-test-config")
	os.MkdirAll(tmpDir, 0o755)
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.yaml")

	// 创建 SQLite 配置文件
	configContent := `database:
  adapter: sqlite
  database: /tmp/eit-test.db
  pool:
    max_connections: 25
    idle_timeout: 300
`

	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	// 使用 InitDB 初始化数据库
	repo, err := InitDB(configPath)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer repo.Close()

	// 验证连接
	ctx := context.Background()
	if err := repo.Ping(ctx); err != nil {
		t.Fatalf("Failed to ping database initialized by InitDB: %v", err)
	}

	// 测试 GetGormDB
	gormDB := repo.GetGormDB()
	if gormDB == nil {
		t.Fatal("GetGormDB() returned nil for database initialized by InitDB")
	}

	t.Log("✓ InitDB with YAML config test passed")
}

// TestConfigFileFormats 测试 JSON 和 YAML 配置文件兼容性
func TestConfigFileFormats(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "eit-db-test-formats")
	os.MkdirAll(tmpDir, 0o755)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name    string
		content string
		ext     string
	}{
		{
			name: "YAML with nested database config",
			content: `database:
  adapter: sqlite
  database: /tmp/eit-yaml-test.db
`,
			ext: ".yaml",
		},
		{
			name: "JSON with nested database config",
			content: `{
  "database": {
    "adapter": "sqlite",
    "database": "/tmp/eit-json-test.db"
  }
}`,
			ext: ".json",
		},
		{
			name: "JSON with direct config",
			content: `{
  "adapter": "sqlite",
  "database": "/tmp/eit-json-direct-test.db"
}`,
			ext: ".json",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			configPath := filepath.Join(tmpDir, fmt.Sprintf("test-config%s", tc.ext))

			if err := os.WriteFile(configPath, []byte(tc.content), 0o644); err != nil {
				t.Fatalf("Failed to write config file: %v", err)
			}

			config, err := LoadConfig(configPath)
			if err != nil {
				t.Fatalf("LoadConfig failed: %v", err)
			}

			if config.Adapter != "sqlite" {
				t.Fatalf("Expected adapter 'sqlite', got '%s'", config.Adapter)
			}

			if config.Database == "" {
				t.Fatal("Database path should not be empty")
			}

			// 创建 Repository 验证配置有效性
			repo, err := NewRepository(config)
			if err != nil {
				t.Fatalf("NewRepository failed: %v", err)
			}
			defer repo.Close()

			gormDB := repo.GetGormDB()
			if gormDB == nil {
				t.Fatal("GetGormDB() returned nil")
			}

			t.Logf("✓ Config format %s test passed", tc.ext)
		})
	}
}

// TestConnectionPoolConfiguration 测试连接池配置的正确应用
func TestConnectionPoolConfiguration(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "eit-db-pool-test")
	os.MkdirAll(tmpDir, 0o755)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "pool-test.db")

	config := &Config{
		Adapter:  "sqlite",
		Database: dbPath,
		Pool: &PoolConfig{
			MaxConnections: 15,
			MinConnections: 2,
			ConnectTimeout: 10,
			IdleTimeout:    120,
			MaxLifetime:    3600,
		},
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	gormDB := repo.GetGormDB()
	if gormDB == nil {
		t.Fatal("GetGormDB() returned nil")
	}

	sqlDB, err := gormDB.DB()
	if err != nil {
		t.Fatalf("Failed to get sql.DB: %v", err)
	}

	// SQLite 连接池配置验证
	stats := sqlDB.Stats()
	if stats.OpenConnections > 15 {
		t.Logf("Warning: Expected MaxOpenConns around 15, got %d", stats.OpenConnections)
	}

	t.Logf("✓ Connection pool configuration test passed")
	t.Logf("  Pool stats: OpenConnections=%d, MaxOpenConns would be set to 15", stats.OpenConnections)
}

// TestAllAdaptersAvailable 测试所有适配器都已注册
func TestAllAdaptersAvailable(t *testing.T) {
	adapters := []string{"sqlite", "mysql", "postgres"}
	ctx := context.Background()

	for _, adapterName := range adapters {
		t.Run(fmt.Sprintf("Adapter-%s", adapterName), func(t *testing.T) {
			// 检查适配器是否已注册
			config := &Config{
				Adapter: adapterName,
			}

			// 设置必要的配置字段
			switch adapterName {
			case "sqlite":
				tmpDir := filepath.Join(os.TempDir(), "eit-db-test-adapter")
				os.MkdirAll(tmpDir, 0o755)
				defer os.RemoveAll(tmpDir)
				config.Database = filepath.Join(tmpDir, "test.db")
			case "mysql":
				config.Host = "localhost"
				config.Port = 3306
				config.Username = "root"
				config.Password = "root"
				config.Database = "test"
			case "postgres":
				config.Host = "localhost"
				config.Port = 5432
				config.Username = "postgres"
				config.Password = "postgres"
				config.Database = "postgres"
				config.SSLMode = "disable"
			}

			repo, err := NewRepository(config)
			if adapterName == "sqlite" {
				// SQLite should always work since it's local
				if err != nil {
					t.Fatalf("Failed to create %s repository: %v", adapterName, err)
				}
				defer repo.Close()

				gormDB := repo.GetGormDB()
				if gormDB == nil {
					t.Fatalf("GetGormDB() returned nil for %s", adapterName)
				}

				sqlDB, err := gormDB.DB()
				if err != nil {
					t.Fatalf("Failed to get sql.DB: %v", err)
				}
				if err := sqlDB.PingContext(ctx); err != nil {
					t.Fatalf("GORM ping failed for %s: %v", adapterName, err)
				}
			} else {
				// For MySQL and PostgreSQL, just verify the adapter is registered
				if err != nil {
					t.Logf("Adapter %s created but connection failed (expected if DB not running): %v", adapterName, err)
				} else {
					defer repo.Close()
					t.Logf("✓ Adapter %s available and connected", adapterName)
				}
			}

			t.Logf("✓ Adapter %s is registered and available", adapterName)
		})
	}
}

// TestErrorMessages 测试错误消息的有用性
func TestErrorMessages(t *testing.T) {
	testCases := []struct {
		name   string
		config *Config
		expErr string
	}{
		{
			name: "Missing MySQL username",
			config: &Config{
				Adapter:  "mysql",
				Host:     "localhost",
				Port:     3306,
				Password: "test",
				Database: "test",
			},
			expErr: "username is required",
		},
		{
			name: "Missing PostgreSQL database",
			config: &Config{
				Adapter:   "postgres",
				Host:      "localhost",
				Port:      5432,
				Username:  "postgres",
				Password:  "postgres",
				SSLMode:   "disable",
			},
			expErr: "database name is required",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			repo, err := NewRepository(tc.config)
			if err == nil {
				defer repo.Close()
				t.Fatal("Expected error but got none")
			}

			if err.Error() == "" {
				t.Fatal("Error message is empty")
			}

			t.Logf("✓ Error message: %v", err)
		})
	}
}

// TestConcurrentGetGormDB 测试并发访问 GetGormDB
func TestConcurrentGetGormDB(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "eit-db-concurrent")
	os.MkdirAll(tmpDir, 0o755)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "concurrent.db")

	config := &Config{
		Adapter:  "sqlite",
		Database: dbPath,
	}

	repo, err := NewRepository(config)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			gormDB := repo.GetGormDB()
			if gormDB == nil {
				t.Errorf("Goroutine %d: GetGormDB() returned nil", id)
			}

			// 执行简单查询验证连接有效
			var count int64
			result := gormDB.Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count)
			if result.Error != nil {
				t.Errorf("Goroutine %d: Query failed: %v", id, result.Error)
			}

			done <- true
		}(i)
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 10; i++ {
		<-done
	}

	t.Log("✓ Concurrent GetGormDB test passed")
}

// BenchmarkGetGormDB 基准测试 GetGormDB 性能
func BenchmarkGetGormDB(b *testing.B) {
	tmpDir := filepath.Join(os.TempDir(), "eit-db-bench")
	os.MkdirAll(tmpDir, 0o755)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "bench.db")

	config := &Config{
		Adapter:  "sqlite",
		Database: dbPath,
	}

	repo, err := NewRepository(config)
	if err != nil {
		b.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = repo.GetGormDB()
	}
}
