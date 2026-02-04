package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConfigFile 配置文件结构 (从 YAML 或 JSON 加载)
type ConfigFile struct {
	Database *Config `yaml:"database" json:"database"`
}

// AdapterRegistryFile 多 Adapter 配置文件结构
type AdapterRegistryFile struct {
	Adapters map[string]*Config `yaml:"adapters" json:"adapters"`
}

// LoadConfig 从文件加载数据库配置（支持 JSON 和 YAML 格式）
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(filename))
	var config *Config

	switch ext {
	case ".json":
		// 先尝试解析为直接的 Config 对象
		if err := json.Unmarshal(data, &config); err == nil && config != nil && config.Adapter != "" {
			// 成功解析为直接的 Config 对象
		} else {
			// 尝试解析为 ConfigFile 包装结构
			var cf ConfigFile
			if err := json.Unmarshal(data, &cf); err != nil {
				return nil, fmt.Errorf("failed to parse JSON config: %w", err)
			}
			if cf.Database == nil {
				return nil, fmt.Errorf("database configuration not found in JSON config file")
			}
			config = cf.Database
		}

	case ".yaml", ".yml":
		// 使用 YAML 解析器
		var cf ConfigFile
		if err := yaml.Unmarshal(data, &cf); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
		if cf.Database == nil {
			return nil, fmt.Errorf("database configuration not found in YAML config file")
		}
		config = cf.Database

	default:
		// 默认使用 YAML 解析器
		var cf ConfigFile
		if err := yaml.Unmarshal(data, &cf); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
		if cf.Database == nil {
			return nil, fmt.Errorf("database configuration not found in config file")
		}
		config = cf.Database
	}

	// 验证配置
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid database configuration: %w", err)
	}

	return config, nil
}

// LoadAdapterRegistry 从文件加载多 Adapter 配置（支持 JSON 和 YAML）
func LoadAdapterRegistry(filename string) (map[string]*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(filename))
	var registry AdapterRegistryFile

	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &registry); err != nil {
			return nil, fmt.Errorf("failed to parse JSON adapter registry: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &registry); err != nil {
			return nil, fmt.Errorf("failed to parse YAML adapter registry: %w", err)
		}
	default:
		if err := yaml.Unmarshal(data, &registry); err != nil {
			return nil, fmt.Errorf("failed to parse adapter registry: %w", err)
		}
	}

	if len(registry.Adapters) == 0 {
		return nil, fmt.Errorf("no adapters found in config file")
	}

	// 验证每个 adapter 配置
	for name, cfg := range registry.Adapters {
		if name == "" {
			return nil, fmt.Errorf("adapter name cannot be empty")
		}
		if cfg == nil {
			return nil, fmt.Errorf("adapter '%s' config cannot be nil", name)
		}
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid adapter '%s' config: %w", name, err)
		}
	}

	return registry.Adapters, nil
}

// LoadConfigWithDefaults 从文件加载配置并应用默认值
func LoadConfigWithDefaults(filename string, defaults *Config) (*Config, error) {
	config, err := LoadConfig(filename)
	if err != nil {
		return nil, err
	}

	// 应用默认值
	if defaults != nil {
		if config.Adapter == "" {
			config.Adapter = defaults.Adapter
		}
		if config.Port == 0 && defaults.Port != 0 {
			config.Port = defaults.Port
		}
		if config.Pool == nil && defaults.Pool != nil {
			config.Pool = defaults.Pool
		}
		if config.SSLMode == "" && defaults.SSLMode != "" {
			config.SSLMode = defaults.SSLMode
		}
	}

	return config, nil
}

// SaveConfig 保存配置到文件
func SaveConfig(filename string, config *Config) error {
	cf := ConfigFile{Database: config}
	
	data, err := yaml.Marshal(cf)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(filename, data, 0o644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Validate 验证配置有效性
func (c *Config) Validate() error {
	if c.Adapter == "" {
		return fmt.Errorf("adapter must be specified")
	}

	switch c.Adapter {
	case "sqlite":
		if c.Database == "" {
			return fmt.Errorf("sqlite: database path must be specified")
		}

	case "postgres", "mysql":
		if c.Host == "" {
			return fmt.Errorf("%s: host must be specified", c.Adapter)
		}
		if c.Port == 0 {
			if c.Adapter == "postgres" {
				c.Port = 5432
			} else if c.Adapter == "mysql" {
				c.Port = 3306
			}
		}
		if c.Username == "" {
			return fmt.Errorf("%s: username must be specified", c.Adapter)
		}
		if c.Database == "" {
			return fmt.Errorf("%s: database name must be specified", c.Adapter)
		}

	case "mongodb":
		if c.Database == "" {
			return fmt.Errorf("mongodb: database name must be specified")
		}
		if c.Options == nil {
			return fmt.Errorf("mongodb: options must include uri")
		}
		uri, _ := c.Options["uri"].(string)
		if uri == "" {
			return fmt.Errorf("mongodb: options.uri must be specified")
		}

	default:
		return fmt.Errorf("unsupported adapter: %s", c.Adapter)
	}

	// 验证连接池配置
	if c.Pool != nil {
		if c.Pool.MaxConnections <= 0 {
			c.Pool.MaxConnections = 25
		}
		if c.Pool.MinConnections < 0 {
			c.Pool.MinConnections = 0
		}
		if c.Pool.ConnectTimeout <= 0 {
			c.Pool.ConnectTimeout = 30
		}
		if c.Pool.IdleTimeout <= 0 {
			c.Pool.IdleTimeout = 300
		}
	} else {
		// 使用默认连接池配置
		c.Pool = &PoolConfig{
			MaxConnections: 25,
			MinConnections: 0,
			ConnectTimeout: 30,
			IdleTimeout:    300,
		}
	}

	// PostgreSQL 特定验证
	if c.Adapter == "postgres" {
		if c.SSLMode == "" {
			c.SSLMode = "disable"
		}
	}

	return nil
}

// DefaultConfig 返回默认配置
func DefaultConfig(adapterType string) *Config {
	config := &Config{
		Adapter: adapterType,
		Pool: &PoolConfig{
			MaxConnections: 25,
			MinConnections: 0,
			ConnectTimeout: 30,
			IdleTimeout:    300,
		},
	}

	switch adapterType {
	case "sqlite":
		config.Database = "./eit.db"

	case "postgres":
		config.Host = "localhost"
		config.Port = 5432
		config.Database = "eit"
		config.Username = "postgres"
		config.Password = "postgres"
		config.SSLMode = "disable"

	case "mysql":
		config.Host = "localhost"
		config.Port = 3306
		config.Database = "eit"
		config.Username = "root"
		config.Password = "root"
	}

	return config
}
