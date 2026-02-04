package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoAdapter MongoDB 适配器（最小可用版本：连接/健康检查）
type MongoAdapter struct {
	client   *mongo.Client
	database string
	uri      string
}

// NewMongoAdapter 创建 MongoAdapter（不建立连接）
func NewMongoAdapter(config *Config) (*MongoAdapter, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	uri, _ := config.Options["uri"].(string)
	return &MongoAdapter{
		database: config.Database,
		uri:      uri,
	}, nil
}

// Connect 建立 MongoDB 连接
func (a *MongoAdapter) Connect(ctx context.Context, config *Config) error {
	if a.client != nil {
		return nil
	}

	uri := a.uri
	if config != nil {
		if err := config.Validate(); err != nil {
			return err
		}
		uri, _ = config.Options["uri"].(string)
		if config.Database != "" {
			a.database = config.Database
		}
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}

	// 短连接测试
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return err
	}

	a.client = client
	return nil
}

// Close 关闭连接
func (a *MongoAdapter) Close() error {
	if a.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return a.client.Disconnect(ctx)
}

// Ping 测试连接
func (a *MongoAdapter) Ping(ctx context.Context) error {
	if a.client == nil {
		return fmt.Errorf("mongodb client not connected")
	}
	return a.client.Ping(ctx, nil)
}

// Begin MongoDB 不支持 SQL 事务接口
func (a *MongoAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, fmt.Errorf("mongodb: transactions are not supported in SQL interface")
}

// Query MongoDB 不支持 SQL Query
func (a *MongoAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("mongodb: sql query not supported")
}

// QueryRow MongoDB 不支持 SQL QueryRow
func (a *MongoAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}

// Exec MongoDB 不支持 SQL Exec
func (a *MongoAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("mongodb: sql exec not supported")
}

// GetRawConn 返回 mongo.Client
func (a *MongoAdapter) GetRawConn() interface{} {
	return a.client
}

// RegisterScheduledTask MongoDB 暂不支持定时任务
func (a *MongoAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return fmt.Errorf("mongodb: scheduled task not supported")
}

// UnregisterScheduledTask MongoDB 暂不支持定时任务
func (a *MongoAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return fmt.Errorf("mongodb: scheduled task not supported")
}

// ListScheduledTasks MongoDB 暂不支持定时任务
func (a *MongoAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, fmt.Errorf("mongodb: scheduled task not supported")
}

// GetQueryBuilderProvider MongoDB 不提供 SQL Query Builder
func (a *MongoAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return nil
}

// GetDatabaseFeatures MongoDB 特性声明（最小实现）
func (a *MongoAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return NewMongoDatabaseFeatures()
}

// GetQueryFeatures MongoDB 查询特性声明（最小实现）
func (a *MongoAdapter) GetQueryFeatures() *QueryFeatures {
	return NewMongoQueryFeatures()
}

// MongoFactory AdapterFactory 实现
type MongoFactory struct{}

func (f *MongoFactory) Name() string { return "mongodb" }

func (f *MongoFactory) Create(config *Config) (Adapter, error) {
	return NewMongoAdapter(config)
}

func init() {
	RegisterAdapter(&MongoFactory{})
}
