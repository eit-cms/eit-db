package db

import (
	"context"
	"fmt"
	"log"
)

// InitDB 使用配置文件初始化数据库
// 这是推荐的使用方式，遵循 Ecto 的依赖注入模式
func InitDB(configPath string) (*Repository, error) {
	// 从配置文件加载配置
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// 验证配置
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// 创建 Repository (Ecto 中类似 Repo)
	repo, err := NewRepository(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}

	// 连接数据库
	if err := repo.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect database: %w", err)
	}

	// 测试连接
	if err := repo.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("database connection failed: %w", err)
	}

	log.Printf("Database connected: %s", config.Adapter)
	return repo, nil
}

// InitDBWithDefaults 使用默认配置初始化数据库
func InitDBWithDefaults(adapterType string) (*Repository, error) {
	config := DefaultConfig(adapterType)
	
	repo, err := NewRepository(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}

	if err := repo.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect database: %w", err)
	}

	if err := repo.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("database connection failed: %w", err)
	}

	log.Printf("Database connected: %s", adapterType)
	return repo, nil
}

// InitDBFromAdapterRegistry 从多 Adapter 配置文件初始化指定 adapter
// configPath: YAML/JSON 配置文件路径
// adapterName: 在 adapters 中注册的名称
func InitDBFromAdapterRegistry(configPath, adapterName string) (*Repository, error) {
	registry, err := LoadAdapterRegistry(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load adapter registry: %w", err)
	}
	if err := RegisterAdapterConfigs(registry); err != nil {
		return nil, fmt.Errorf("failed to register adapter configs: %w", err)
	}

	repo, err := NewRepositoryFromAdapterConfig(adapterName)
	if err != nil {
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}

	if err := repo.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect database: %w", err)
	}

	if err := repo.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("database connection failed: %w", err)
	}

	log.Printf("Database connected: %s", adapterName)
	return repo, nil
}

// InitDBFromEnv 使用环境变量初始化数据库
// 环境变量:
//   DB_ADAPTER: 适配器类型 (sqlite/postgres/mysql)
//   DB_PATH: SQLite 数据库文件路径
//   DB_HOST: 数据库主机
//   DB_PORT: 数据库端口
//   DB_USER: 数据库用户名
//   DB_PASSWORD: 数据库密码
//   DB_NAME: 数据库名称
//   DB_SSL_MODE: PostgreSQL SSL 模式
func InitDBFromEnv() (*Repository, error) {
	// 这个函数的具体实现取决于环境变量的使用
	// 这里只是一个框架示例
	return nil, fmt.Errorf("not implemented")
}
