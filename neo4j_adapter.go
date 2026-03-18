package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Neo4jAdapter Neo4j 适配器（最小可用版本：连接/健康检查/能力声明）。
type Neo4jAdapter struct {
	driver   neo4j.DriverWithContext
	uri      string
	username string
	password string
	database string
}

// CypherWriteSummary 表示 Neo4j 写入执行摘要。
type CypherWriteSummary struct {
	NodesCreated         int
	NodesDeleted         int
	RelationshipsCreated int
	RelationshipsDeleted int
	PropertiesSet        int
	LabelsAdded          int
	LabelsRemoved        int
}

// NewNeo4jAdapter 创建 Neo4jAdapter（不建立连接）。
func NewNeo4jAdapter(config *Config) (*Neo4jAdapter, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	resolved := config.ResolvedNeo4jConfig()
	return &Neo4jAdapter{
		uri:      resolved.URI,
		username: resolved.Username,
		password: resolved.Password,
		database: resolved.Database,
	}, nil
}

// Connect 建立 Neo4j 连接。
func (a *Neo4jAdapter) Connect(ctx context.Context, config *Config) error {
	if a.driver != nil {
		return nil
	}

	uri := a.uri
	username := a.username
	password := a.password
	database := a.database

	if config != nil {
		if err := config.Validate(); err != nil {
			return err
		}
		resolved := config.ResolvedNeo4jConfig()
		uri = resolved.URI
		username = resolved.Username
		password = resolved.Password
		database = resolved.Database
	}

	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return fmt.Errorf("failed to connect to neo4j: %w", err)
	}

	verifyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := driver.VerifyConnectivity(verifyCtx); err != nil {
		_ = driver.Close(ctx)
		return fmt.Errorf("neo4j connectivity check failed: %w", err)
	}

	a.driver = driver
	a.uri = uri
	a.username = username
	a.password = password
	a.database = database
	return nil
}

// Close 关闭 Neo4j 连接。
func (a *Neo4jAdapter) Close() error {
	if a.driver == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := a.driver.Close(ctx)
	a.driver = nil
	return err
}

// Ping 检查 Neo4j 连接可用性。
func (a *Neo4jAdapter) Ping(ctx context.Context) error {
	if a.driver == nil {
		return fmt.Errorf("neo4j driver not connected")
	}
	return a.driver.VerifyConnectivity(ctx)
}

// Begin Neo4j 不支持 SQL 事务接口。
func (a *Neo4jAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, fmt.Errorf("neo4j: SQL transaction interface is not supported")
}

// Query Neo4j 不支持 SQL Query 接口。
func (a *Neo4jAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("neo4j: sql query not supported; use custom feature or native driver")
}

// QueryRow Neo4j 不支持 SQL QueryRow 接口。
func (a *Neo4jAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}

// Exec Neo4j 不支持 SQL Exec 接口。
func (a *Neo4jAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("neo4j: sql exec not supported; use custom feature or native driver")
}

// GetRawConn 返回底层 Neo4j Driver。
func (a *Neo4jAdapter) GetRawConn() interface{} {
	return a.driver
}

// RegisterScheduledTask Neo4j 暂不支持定时任务接口。
func (a *Neo4jAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return fmt.Errorf("neo4j: scheduled task not supported")
}

// UnregisterScheduledTask Neo4j 暂不支持定时任务接口。
func (a *Neo4jAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return fmt.Errorf("neo4j: scheduled task not supported")
}

// ListScheduledTasks Neo4j 暂不支持定时任务接口。
func (a *Neo4jAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, fmt.Errorf("neo4j: scheduled task not supported")
}

// QueryCypher 执行读类型 Cypher，并返回记录列表。
func (a *Neo4jAdapter) QueryCypher(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	if a.driver == nil {
		return nil, fmt.Errorf("neo4j driver not connected")
	}
	if strings.TrimSpace(cypher) == "" {
		return nil, fmt.Errorf("cypher cannot be empty")
	}
	if params == nil {
		params = map[string]interface{}{}
	}

	session := a.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: a.database,
		AccessMode:   neo4j.AccessModeRead,
	})
	defer func() { _ = session.Close(ctx) }()

	out, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, runErr := tx.Run(ctx, cypher, params)
		if runErr != nil {
			return nil, runErr
		}
		rows := make([]map[string]interface{}, 0)
		for result.Next(ctx) {
			record := result.Record()
			rows = append(rows, record.AsMap())
		}
		if consumeErr := result.Err(); consumeErr != nil {
			return nil, consumeErr
		}
		return rows, nil
	})
	if err != nil {
		return nil, err
	}

	rows, ok := out.([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("neo4j query returned unexpected result type: %T", out)
	}
	return rows, nil
}

// ExecCypher 执行写类型 Cypher，并返回写入摘要。
func (a *Neo4jAdapter) ExecCypher(ctx context.Context, cypher string, params map[string]interface{}) (*CypherWriteSummary, error) {
	if a.driver == nil {
		return nil, fmt.Errorf("neo4j driver not connected")
	}
	if strings.TrimSpace(cypher) == "" {
		return nil, fmt.Errorf("cypher cannot be empty")
	}
	if params == nil {
		params = map[string]interface{}{}
	}

	session := a.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: a.database,
		AccessMode:   neo4j.AccessModeWrite,
	})
	defer func() { _ = session.Close(ctx) }()

	out, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, runErr := tx.Run(ctx, cypher, params)
		if runErr != nil {
			return nil, runErr
		}
		summary, consumeErr := result.Consume(ctx)
		if consumeErr != nil {
			return nil, consumeErr
		}
		c := summary.Counters()
		return &CypherWriteSummary{
			NodesCreated:         c.NodesCreated(),
			NodesDeleted:         c.NodesDeleted(),
			RelationshipsCreated: c.RelationshipsCreated(),
			RelationshipsDeleted: c.RelationshipsDeleted(),
			PropertiesSet:        c.PropertiesSet(),
			LabelsAdded:          c.LabelsAdded(),
			LabelsRemoved:        c.LabelsRemoved(),
		}, nil
	})
	if err != nil {
		return nil, err
	}

	summary, ok := out.(*CypherWriteSummary)
	if !ok {
		return nil, fmt.Errorf("neo4j exec returned unexpected summary type: %T", out)
	}
	return summary, nil
}

// QueryCypher 在 Repository 上执行 Neo4j 读查询。
func (r *Repository) QueryCypher(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	r.mu.RLock()
	adapter := r.adapter
	r.mu.RUnlock()
	if adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}
	neo, ok := adapter.(*Neo4jAdapter)
	if !ok {
		return nil, fmt.Errorf("query cypher requires neo4j adapter, got %T", adapter)
	}
	return neo.QueryCypher(ctx, cypher, params)
}

// ExecCypher 在 Repository 上执行 Neo4j 写查询。
func (r *Repository) ExecCypher(ctx context.Context, cypher string, params map[string]interface{}) (*CypherWriteSummary, error) {
	r.mu.RLock()
	adapter := r.adapter
	r.mu.RUnlock()
	if adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}
	neo, ok := adapter.(*Neo4jAdapter)
	if !ok {
		return nil, fmt.Errorf("exec cypher requires neo4j adapter, got %T", adapter)
	}
	return neo.ExecCypher(ctx, cypher, params)
}

// GetQueryBuilderProvider 返回 Neo4j Cypher Query Builder Provider。
func (a *Neo4jAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return NewNeo4jQueryConstructorProvider()
}

// GetDatabaseFeatures 返回 Neo4j 数据库特性声明。
func (a *Neo4jAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return NewNeo4jDatabaseFeatures()
}

// GetQueryFeatures 返回 Neo4j 查询特性声明。
func (a *Neo4jAdapter) GetQueryFeatures() *QueryFeatures {
	return NewNeo4jQueryFeatures()
}

// InspectFullTextRuntime 探测 Neo4j 全文能力（需要全文索引）。
func (a *Neo4jAdapter) InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error) {
	available := a.driver != nil
	notes := "Neo4j supports full-text via fulltext indexes and procedures"
	if !available {
		notes = "Neo4j driver not connected; full-text capability assumed but runtime verification skipped"
	}
	return &FullTextRuntimeCapability{
		NativeSupported:       true,
		PluginChecked:         available,
		PluginAvailable:       available,
		PluginName:            "neo4j_fulltext_index",
		TokenizationSupported: true,
		TokenizationMode:      "native",
		Notes:                 notes,
	}, nil
}

// HasCustomFeatureImplementation 声明 Neo4j 可用的自定义能力。
func (a *Neo4jAdapter) HasCustomFeatureImplementation(feature string) bool {
	switch feature {
	case "graph_traversal", "document_join", "recursive_query", "full_text_search", "tokenized_full_text_search", "relationship_association_query", "relationship_with_payload", "bidirectional_relationship_semantics":
		return true
	default:
		return false
	}
}

// ExecuteCustomFeature 执行 Neo4j 自定义能力（返回可执行 Cypher/说明）。
func (a *Neo4jAdapter) ExecuteCustomFeature(ctx context.Context, feature string, input map[string]interface{}) (interface{}, error) {
	switch feature {
	case "graph_traversal", "recursive_query":
		startLabel, _ := input["start_label"].(string)
		relType, _ := input["relationship"].(string)
		depth, _ := input["max_depth"].(int)
		if depth <= 0 {
			depth = 3
		}
		if strings.TrimSpace(startLabel) == "" || strings.TrimSpace(relType) == "" {
			return nil, fmt.Errorf("neo4j graph_traversal requires start_label and relationship")
		}
		cypher := fmt.Sprintf("MATCH p=(n:%s)-[:%s*1..%d]->(m) RETURN p", startLabel, relType, depth)
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "native_graph_traversal",
			"cypher":   cypher,
		}, nil
	case "document_join":
		leftLabel, _ := input["left_label"].(string)
		rightLabel, _ := input["right_label"].(string)
		relType, _ := input["relationship"].(string)
		if leftLabel == "" || rightLabel == "" || relType == "" {
			return nil, fmt.Errorf("neo4j document_join requires left_label, right_label and relationship")
		}
		cypher := fmt.Sprintf("MATCH (a:%s)-[r:%s]->(b:%s) RETURN a, r, b", leftLabel, relType, rightLabel)
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "pattern_match_join",
			"cypher":   cypher,
		}, nil
	case "full_text_search", "tokenized_full_text_search":
		indexName, _ := input["index"].(string)
		query, _ := input["query"].(string)
		if strings.TrimSpace(indexName) == "" || strings.TrimSpace(query) == "" {
			return nil, fmt.Errorf("neo4j full_text_search requires index and query")
		}
		cypher := "CALL db.index.fulltext.queryNodes($index, $query) YIELD node, score RETURN node, score"
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "native_fulltext",
			"cypher":   cypher,
			"params": map[string]interface{}{
				"index": indexName,
				"query": query,
			},
		}, nil
	case "relationship_association_query":
		fromLabel, _ := input["from_label"].(string)
		toLabel, _ := input["to_label"].(string)
		relType, _ := input["relationship"].(string)
		direction, _ := input["direction"].(string)
		if strings.TrimSpace(fromLabel) == "" || strings.TrimSpace(toLabel) == "" || strings.TrimSpace(relType) == "" {
			return nil, fmt.Errorf("neo4j relationship_association_query requires from_label, to_label and relationship")
		}
		relPattern := "-[r:" + relType + "]->"
		switch strings.ToLower(strings.TrimSpace(direction)) {
		case "in":
			relPattern = "<-[r:" + relType + "]-"
		case "both", "bidirectional", "undirected":
			relPattern = "-[r:" + relType + "]-"
		}
		cypher := fmt.Sprintf("MATCH (a:%s)%s(b:%s) RETURN a, r, b", fromLabel, relPattern, toLabel)
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "relationship_association_query",
			"cypher":   cypher,
		}, nil
	case "relationship_with_payload":
		fromLabel, _ := input["from_label"].(string)
		toLabel, _ := input["to_label"].(string)
		relType, _ := input["relationship"].(string)
		if strings.TrimSpace(fromLabel) == "" || strings.TrimSpace(toLabel) == "" || strings.TrimSpace(relType) == "" {
			return nil, fmt.Errorf("neo4j relationship_with_payload requires from_label, to_label and relationship")
		}
		cypher := fmt.Sprintf("MATCH (a:%s {id: $from_id}), (b:%s {id: $to_id}) MERGE (a)-[r:%s]->(b) SET r += $payload RETURN a, r, b", fromLabel, toLabel, relType)
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "relationship_with_payload",
			"cypher":   cypher,
			"params": map[string]interface{}{
				"from_id": input["from_id"],
				"to_id":   input["to_id"],
				"payload": input["payload"],
			},
		}, nil
	case "bidirectional_relationship_semantics":
		userLabel, _ := input["user_label"].(string)
		requestType, _ := input["request_relationship"].(string)
		friendType, _ := input["friend_relationship"].(string)
		if strings.TrimSpace(userLabel) == "" {
			userLabel = "User"
		}
		if strings.TrimSpace(requestType) == "" {
			requestType = "KNOWS_REQUEST"
		}
		if strings.TrimSpace(friendType) == "" {
			friendType = "KNOWS"
		}
		cypher := fmt.Sprintf("MATCH (a:%s {id: $from_id})-[req:%s]->(b:%s {id: $to_id}) DELETE req MERGE (a)-[f1:%s]->(b) MERGE (b)-[f2:%s]->(a) SET f1.accepted_at = datetime(), f2.accepted_at = datetime() RETURN a, f1, b, f2", userLabel, requestType, userLabel, friendType, friendType)
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "bidirectional_relationship_semantics",
			"cypher":   cypher,
			"params": map[string]interface{}{
				"from_id": input["from_id"],
				"to_id":   input["to_id"],
			},
			"notes": "directional request to bidirectional friendship transition",
		}, nil
	default:
		return nil, fmt.Errorf("neo4j custom feature not supported: %s", feature)
	}
}

// Neo4jFactory AdapterFactory 实现。
type Neo4jFactory struct{}

func (f *Neo4jFactory) Name() string { return "neo4j" }

func (f *Neo4jFactory) Create(config *Config) (Adapter, error) {
	return NewNeo4jAdapter(config)
}

// init 自动注册 Neo4j 适配器。
func init() {
	RegisterAdapter(&Neo4jFactory{})
}
