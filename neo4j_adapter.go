package db

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
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
	case "graph_traversal", "document_join", "recursive_query", "full_text_search", "tokenized_full_text_search", "relationship_association_query", "relationship_with_payload", "bidirectional_relationship_semantics", "social_network_preset_model", "social_model_bidirectional_follow", "social_model_friendship", "social_model_forum_post", "social_model_one_to_one_chat", "social_model_group_chat_room", "social_model_chat_receipt", "social_model_chat_moderation", "social_model_message_emoji", "social_model_executor":
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
	case "social_model_bidirectional_follow":
		return buildNeo4jSocialPresetModel("bidirectional_follow")
	case "social_model_friendship":
		return buildNeo4jSocialPresetModel("friendship")
	case "social_model_forum_post":
		return buildNeo4jSocialPresetModel("forum_post")
	case "social_model_one_to_one_chat":
		return buildNeo4jSocialPresetModel("one_to_one_chat")
	case "social_model_group_chat_room":
		return buildNeo4jSocialPresetModel("group_chat_room")
	case "social_model_chat_receipt":
		return buildNeo4jSocialPresetModel("chat_receipt")
	case "social_model_chat_moderation":
		return buildNeo4jSocialPresetModel("chat_moderation")
	case "social_model_message_emoji":
		return buildNeo4jSocialPresetModel("message_emoji")
	case "social_network_preset_model":
		preset, _ := input["preset"].(string)
		if strings.TrimSpace(preset) == "" {
			preset = "bidirectional_follow"
		}
		return buildNeo4jSocialPresetModel(preset)
	case "social_model_executor":
		preset, _ := input["preset"].(string)
		if strings.TrimSpace(preset) == "" {
			preset = "bidirectional_follow"
		}
		execute, _ := input["execute"].(bool)
		if execute && a.driver == nil {
			return nil, fmt.Errorf("neo4j driver not connected")
		}
		return buildNeo4jSocialModelExecutor(ctx, a.driver, a.database, preset, execute)
	default:
		return nil, fmt.Errorf("neo4j custom feature not supported: %s", feature)
	}
}

func buildNeo4jSocialPresetModel(preset string) (map[string]interface{}, error) {
	switch strings.ToLower(strings.TrimSpace(preset)) {
	case "bidirectional_follow", "mutual_follow":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "bidirectional_follow",
			"constraints": []string{
				"CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
			},
			"queries": map[string]string{
				"create_user":      "MERGE (u:User {id: $user_id}) SET u.name = coalesce($name, u.name), u.created_at = coalesce(u.created_at, datetime()) RETURN u",
				"follow":           "MATCH (a:User {id: $from_id}), (b:User {id: $to_id}) MERGE (a)-[f:FOLLOWS]->(b) SET f.created_at = coalesce(f.created_at, datetime()) RETURN a, f, b",
				"unfollow":         "MATCH (a:User {id: $from_id})-[f:FOLLOWS]->(b:User {id: $to_id}) DELETE f",
				"mutual_followers": "MATCH (a:User {id: $user_id})-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(a) RETURN b",
				"suggestions":      "MATCH (a:User {id: $user_id})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(candidate:User) WHERE candidate.id <> $user_id AND NOT (a)-[:FOLLOWS]->(candidate) RETURN candidate, count(*) AS score ORDER BY score DESC LIMIT $limit",
			},
			"notes": "Directed follow graph with built-in mutual-follow query for social discovery",
		}, nil
	case "friendship", "friend":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "friendship",
			"constraints": []string{
				"CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
			},
			"queries": map[string]string{
				"send_request":   "MATCH (a:User {id: $from_id}), (b:User {id: $to_id}) MERGE (a)-[r:FRIEND_REQUEST]->(b) SET r.created_at = coalesce(r.created_at, datetime()), r.message = $message RETURN a, r, b",
				"accept_request": "MATCH (a:User {id: $from_id})-[r:FRIEND_REQUEST]->(b:User {id: $to_id}) DELETE r MERGE (a)-[f1:FRIEND]->(b) MERGE (b)-[f2:FRIEND]->(a) SET f1.since = datetime(), f2.since = datetime() RETURN a, f1, b, f2",
				"reject_request": "MATCH (:User {id: $from_id})-[r:FRIEND_REQUEST]->(:User {id: $to_id}) DELETE r",
				"list_friends":   "MATCH (u:User {id: $user_id})-[:FRIEND]->(f:User) RETURN f",
				"mutual_friends": "MATCH (u1:User {id: $user_a})-[:FRIEND]->(m:User)<-[:FRIEND]-(u2:User {id: $user_b}) RETURN m",
			},
			"notes": "Two-way friend model with explicit request/accept lifecycle",
		}, nil
	case "forum_post", "forum", "post":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "forum_post",
			"constraints": []string{
				"CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
				"CREATE CONSTRAINT forum_id_unique IF NOT EXISTS FOR (f:Forum) REQUIRE f.id IS UNIQUE",
				"CREATE CONSTRAINT post_id_unique IF NOT EXISTS FOR (p:Post) REQUIRE p.id IS UNIQUE",
			},
			"queries": map[string]string{
				"create_forum":   "MERGE (f:Forum {id: $forum_id}) SET f.name = coalesce($name, f.name), f.created_at = coalesce(f.created_at, datetime()) RETURN f",
				"join_forum":     "MATCH (u:User {id: $user_id}), (f:Forum {id: $forum_id}) MERGE (u)-[r:MEMBER_OF]->(f) SET r.joined_at = coalesce(r.joined_at, datetime()) RETURN u, r, f",
				"create_post":    "MATCH (u:User {id: $author_id}), (f:Forum {id: $forum_id}) MERGE (p:Post {id: $post_id}) SET p.title = $title, p.content = $content, p.created_at = datetime() MERGE (u)-[:AUTHORED]->(p) MERGE (p)-[:POSTED_IN]->(f) RETURN p",
				"reply_post":     "MATCH (u:User {id: $author_id}), (p:Post {id: $post_id}) MERGE (c:Comment {id: $comment_id}) SET c.content = $content, c.created_at = datetime() MERGE (u)-[:AUTHORED]->(c) MERGE (c)-[:REPLY_TO]->(p) RETURN c",
				"like_post":      "MATCH (u:User {id: $user_id}), (p:Post {id: $post_id}) MERGE (u)-[l:LIKES_POST]->(p) SET l.created_at = coalesce(l.created_at, datetime()) RETURN u, l, p",
				"hot_posts":      "MATCH (p:Post)-[:POSTED_IN]->(f:Forum {id: $forum_id}) OPTIONAL MATCH (:User)-[l:LIKES_POST]->(p) WITH p, count(l) AS likes RETURN p, likes ORDER BY likes DESC, p.created_at DESC LIMIT $limit",
				"forum_timeline": "MATCH (u:User {id: $user_id})-[:MEMBER_OF]->(f:Forum)<-[:POSTED_IN]-(p:Post) RETURN p, f ORDER BY p.created_at DESC LIMIT $limit",
			},
			"notes": "Forum domain model with post/comment/like relationships for community products",
		}, nil
	case "one_to_one_chat", "direct_chat", "dm":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "one_to_one_chat",
			"constraints": []string{
				"CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
				"CREATE CONSTRAINT chat_message_id_unique IF NOT EXISTS FOR (m:ChatMessage) REQUIRE m.id IS UNIQUE",
				"CREATE FULLTEXT INDEX chat_message_fulltext IF NOT EXISTS FOR (m:ChatMessage) ON EACH [m.content]",
			},
			"queries": map[string]string{
				"send_direct_message": "MATCH (a:User {id: $from_id}), (b:User {id: $to_id}) WHERE ((a)-[:FRIEND]->(b) AND (b)-[:FRIEND]->(a)) OR ((a)-[:FOLLOWS]->(b) AND (b)-[:FOLLOWS]->(a)) CREATE (m:ChatMessage {id: $message_id, content: $content, created_at: datetime()}) MERGE (a)-[:SENT]->(m) MERGE (m)-[:TO]->(b) RETURN m",
				"list_direct_messages": "MATCH (s:User)-[:SENT]->(m:ChatMessage)-[:TO]->(r:User) WHERE (s.id = $user_a AND r.id = $user_b) OR (s.id = $user_b AND r.id = $user_a) RETURN m, s, r ORDER BY m.created_at DESC LIMIT $limit",
				"search_direct_messages": "CALL db.index.fulltext.queryNodes('chat_message_fulltext', $query) YIELD node, score MATCH (s:User)-[:SENT]->(node:ChatMessage)-[:TO]->(r:User) WHERE (s.id = $user_a AND r.id = $user_b) OR (s.id = $user_b AND r.id = $user_a) RETURN node AS m, s, r, score ORDER BY score DESC, m.created_at DESC LIMIT $limit",
				"search_direct_messages_advanced": "CALL db.index.fulltext.queryNodes('chat_message_fulltext', $query) YIELD node, score MATCH (s:User)-[:SENT]->(node:ChatMessage)-[:TO]->(r:User) WHERE ((s.id = $user_a AND r.id = $user_b) OR (s.id = $user_b AND r.id = $user_a)) AND node.deleted_at IS NULL AND ($start_at = '' OR node.created_at >= datetime($start_at)) AND ($end_at = '' OR node.created_at <= datetime($end_at)) OPTIONAL MATCH (node)-[:AT]->(mu:User {id: $mentioned_user_id}) WITH node AS m, s, r, score, CASE WHEN $mentioned_user_id = '' THEN 0.0 WHEN mu IS NULL THEN 0.0 ELSE 0.5 END AS mention_boost RETURN m, s, r, score, mention_boost, score + mention_boost AS rank_score ORDER BY rank_score DESC, m.created_at DESC LIMIT $limit",
				"can_chat_check":      "MATCH (a:User {id: $from_id}), (b:User {id: $to_id}) RETURN ((a)-[:FRIEND]->(b) AND (b)-[:FRIEND]->(a)) OR ((a)-[:FOLLOWS]->(b) AND (b)-[:FOLLOWS]->(a)) AS can_chat",
				"reference_message":   "MATCH (m:ChatMessage {id: $message_id}), (ref:ChatMessage {id: $ref_message_id}) MERGE (m)-[:REF]->(ref) RETURN m, ref",
			},
			"notes": "Direct chat is allowed only when users have bidirectional FRIEND or mutual FOLLOWS; message is a middle node connecting sender and receiver",
		}, nil
	case "group_chat_room", "group_chat", "chat_room":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "group_chat_room",
			"constraints": []string{
				"CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
				"CREATE CONSTRAINT chat_room_id_unique IF NOT EXISTS FOR (r:ChatRoom) REQUIRE r.id IS UNIQUE",
				"CREATE CONSTRAINT chat_message_id_unique IF NOT EXISTS FOR (m:ChatMessage) REQUIRE m.id IS UNIQUE",
				"CREATE FULLTEXT INDEX chat_message_fulltext IF NOT EXISTS FOR (m:ChatMessage) ON EACH [m.content]",
			},
			"queries": map[string]string{
				"create_room":         "MATCH (creator:User {id: $creator_id}) MERGE (room:ChatRoom {id: $room_id}) SET room.name = coalesce($name, coalesce(room.name, 'room')), room.created_at = coalesce(room.created_at, datetime()) MERGE (creator)-[:CREATED]->(room) MERGE (creator)-[:IN]->(room) MERGE (room)-[:IN]->(creator) RETURN room, creator",
				"request_join_room":   "MATCH (u:User {id: $user_id}), (room:ChatRoom {id: $room_id}) MERGE (u)-[req:IN]->(room) SET req.status = coalesce(req.status, 'requested'), req.created_at = coalesce(req.created_at, datetime()) RETURN u, req, room",
				"approve_join_room":   "MATCH (u:User {id: $user_id})-[req:IN]->(room:ChatRoom {id: $room_id}) SET req.status = 'approved', req.approved_at = datetime() MERGE (room)-[inback:IN]->(u) SET inback.status = 'approved', inback.approved_at = coalesce(inback.approved_at, datetime()) RETURN u, room",
				"send_room_message":   "MATCH (u:User {id: $user_id})-[in1:IN]->(room:ChatRoom {id: $room_id})-[in2:IN]->(u) WHERE coalesce(in1.status, 'approved') <> 'requested' AND coalesce(in2.status, 'approved') <> 'requested' CREATE (m:ChatMessage {id: $message_id, content: $content, created_at: datetime()}) MERGE (u)-[:SENT]->(m) MERGE (m)-[:IN_ROOM]->(room) RETURN m, room",
				"at_user":            "MATCH (m:ChatMessage {id: $message_id}), (u:User {id: $mentioned_user_id}) MERGE (m)-[:AT]->(u) RETURN m, u",
				"ref_message":        "MATCH (m:ChatMessage {id: $message_id}), (ref:ChatMessage {id: $ref_message_id}) MERGE (m)-[:REF]->(ref) RETURN m, ref",
				"list_room_messages": "MATCH (room:ChatRoom {id: $room_id})<-[:IN_ROOM]-(m:ChatMessage)<-[:SENT]-(u:User) RETURN m, u ORDER BY m.created_at DESC LIMIT $limit",
				"search_room_messages": "CALL db.index.fulltext.queryNodes('chat_message_fulltext', $query) YIELD node, score MATCH (room:ChatRoom {id: $room_id})<-[:IN_ROOM]-(node:ChatMessage)<-[:SENT]-(u:User) RETURN node AS m, u, room, score ORDER BY score DESC, m.created_at DESC LIMIT $limit",
				"search_room_messages_advanced": "CALL db.index.fulltext.queryNodes('chat_message_fulltext', $query) YIELD node, score MATCH (room:ChatRoom {id: $room_id})<-[:IN_ROOM]-(node:ChatMessage)<-[:SENT]-(u:User) WHERE node.deleted_at IS NULL AND ($start_at = '' OR node.created_at >= datetime($start_at)) AND ($end_at = '' OR node.created_at <= datetime($end_at)) OPTIONAL MATCH (node)-[:AT]->(mu:User {id: $mentioned_user_id}) WITH node AS m, u, room, score, CASE WHEN $mentioned_user_id = '' THEN 0.0 WHEN mu IS NULL THEN 0.0 ELSE 0.5 END AS mention_boost RETURN m, u, room, score, mention_boost, score + mention_boost AS rank_score ORDER BY rank_score DESC, m.created_at DESC LIMIT $limit",
			},
			"notes": "Group chat room uses ChatRoom unit with creator relation; bidirectional IN means active member, one-way IN means join request. @ and ref are modeled as AT and REF relations.",
		}, nil
	case "chat_receipt", "receipt", "message_receipt":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "chat_receipt",
			"constraints": []string{
				"CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
				"CREATE CONSTRAINT chat_message_id_unique IF NOT EXISTS FOR (m:ChatMessage) REQUIRE m.id IS UNIQUE",
				"CREATE FULLTEXT INDEX chat_message_fulltext IF NOT EXISTS FOR (m:ChatMessage) ON EACH [m.content]",
			},
			"queries": map[string]string{
				"mark_direct_message_read": "MATCH (u:User {id: $user_id}), (m:ChatMessage {id: $message_id})-[:TO]->(u) MERGE (u)-[r:READ_BY]->(m) SET r.read_at = coalesce(r.read_at, datetime()) RETURN u, m, r",
				"mark_room_message_read":   "MATCH (u:User {id: $user_id})-[in1:IN]->(room:ChatRoom)<-[in2:IN]-(u), (m:ChatMessage {id: $message_id})-[:IN_ROOM]->(room) WHERE coalesce(in1.status, 'approved') <> 'requested' AND coalesce(in2.status, 'approved') <> 'requested' MERGE (u)-[r:READ_BY]->(m) SET r.read_at = coalesce(r.read_at, datetime()) RETURN u, m, r",
				"list_direct_unread":       "MATCH (sender:User {id: $peer_id})-[:SENT]->(m:ChatMessage)-[:TO]->(u:User {id: $user_id}) WHERE NOT (u)-[:READ_BY]->(m) RETURN m, sender ORDER BY m.created_at DESC LIMIT $limit",
				"list_room_unread":         "MATCH (u:User {id: $user_id})-[in1:IN]->(room:ChatRoom)<-[in2:IN]-(u), (sender:User)-[:SENT]->(m:ChatMessage)-[:IN_ROOM]->(room) WHERE coalesce(in1.status, 'approved') <> 'requested' AND coalesce(in2.status, 'approved') <> 'requested' AND NOT (u)-[:READ_BY]->(m) RETURN m, sender, room ORDER BY m.created_at DESC LIMIT $limit",
			},
			"notes": "Read receipt preset models message acknowledgment using READ_BY relation for both direct chat and room chat",
		}, nil
	case "chat_moderation", "moderation", "room_moderation":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "chat_moderation",
			"constraints": []string{
				"CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
				"CREATE CONSTRAINT chat_room_id_unique IF NOT EXISTS FOR (r:ChatRoom) REQUIRE r.id IS UNIQUE",
			},
			"queries": map[string]string{
				"mute_member":            "MATCH (operator:User {id: $operator_id})-[:CREATED]->(room:ChatRoom {id: $room_id}), (target:User {id: $target_id}) MERGE (target)-[mu:MUTED_IN]->(room) SET mu.reason = $reason, mu.until_at = $until_at, mu.updated_at = datetime() RETURN target, room, mu",
				"unmute_member":          "MATCH (operator:User {id: $operator_id})-[:CREATED]->(room:ChatRoom {id: $room_id}) MATCH (target:User {id: $target_id})-[mu:MUTED_IN]->(room) DELETE mu RETURN target, room",
				"ban_member":             "MATCH (operator:User {id: $operator_id})-[:CREATED]->(room:ChatRoom {id: $room_id}), (target:User {id: $target_id}) MERGE (target)-[ban:BANNED_IN]->(room) SET ban.reason = $reason, ban.created_at = coalesce(ban.created_at, datetime()) WITH target, room OPTIONAL MATCH (target)-[in1:IN]->(room) DELETE in1 WITH target, room OPTIONAL MATCH (room)-[in2:IN]->(target) DELETE in2 RETURN target, room, ban",
				"unban_member":           "MATCH (operator:User {id: $operator_id})-[:CREATED]->(room:ChatRoom {id: $room_id}) MATCH (target:User {id: $target_id})-[ban:BANNED_IN]->(room) DELETE ban RETURN target, room",
				"can_send_room_message":  "MATCH (u:User {id: $user_id}), (room:ChatRoom {id: $room_id}) OPTIONAL MATCH (u)-[in1:IN]->(room) OPTIONAL MATCH (room)-[in2:IN]->(u) OPTIONAL MATCH (u)-[ban:BANNED_IN]->(room) OPTIONAL MATCH (u)-[mu:MUTED_IN]->(room) RETURN in1 IS NOT NULL AND in2 IS NOT NULL AND coalesce(in1.status, 'approved') <> 'requested' AND coalesce(in2.status, 'approved') <> 'requested' AND ban IS NULL AND (mu IS NULL OR mu.until_at IS NULL OR datetime(mu.until_at) < datetime()) AS can_send",
				"list_moderation_actions": "MATCH (u:User)-[r:MUTED_IN|BANNED_IN]->(room:ChatRoom {id: $room_id}) RETURN u, type(r) AS action, r ORDER BY coalesce(r.updated_at, r.created_at) DESC LIMIT $limit",
			},
			"notes": "Moderation preset models mute/ban policies inside graph relations and exposes safe can_send check for API gating",
		}, nil
	case "message_emoji", "chat_emoji", "emoji_placeholder":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "message_emoji",
			"constraints": []string{
				"CREATE CONSTRAINT chat_message_id_unique IF NOT EXISTS FOR (m:ChatMessage) REQUIRE m.id IS UNIQUE",
				"CREATE CONSTRAINT emoji_id_unique IF NOT EXISTS FOR (e:Emoji) REQUIRE e.id IS UNIQUE",
				"CREATE INDEX emoji_symbol_index IF NOT EXISTS FOR (e:Emoji) ON (e.symbol)",
			},
			"queries": map[string]string{
				"upsert_emoji":                "MERGE (e:Emoji {id: $emoji_id}) SET e.symbol = coalesce($symbol, e.symbol), e.name = coalesce($name, e.name), e.updated_at = datetime() RETURN e",
				"attach_emoji_to_message":     "MATCH (m:ChatMessage {id: $message_id}), (e:Emoji {id: $emoji_id}) MERGE (e)-[r:INCLUDED_BY {index: $index}]->(m) SET r.updated_at = datetime() RETURN e, r, m",
				"detach_emoji_from_message":   "MATCH (e:Emoji {id: $emoji_id})-[r:INCLUDED_BY {index: $index}]->(m:ChatMessage {id: $message_id}) DELETE r RETURN e, m",
				"list_message_emojis":         "MATCH (e:Emoji)-[r:INCLUDED_BY]->(m:ChatMessage {id: $message_id}) RETURN r.index AS index, e.id AS emoji_id, e.symbol AS symbol, e.name AS name ORDER BY index ASC",
				"render_message_emoji_payload": "MATCH (m:ChatMessage {id: $message_id}) OPTIONAL MATCH (e:Emoji)-[r:INCLUDED_BY]->(m) RETURN m.id AS message_id, m.content AS template_content, collect({index: r.index, emoji_id: e.id, symbol: e.symbol, name: e.name}) AS emojis",
			},
			"notes": "Message can embed emoji placeholders like {{0}}. INCLUDED_BY relation points from static Emoji node to ChatMessage, with index binding placeholder -> emoji.",
		}, nil
	default:
		return nil, fmt.Errorf("neo4j social preset model not supported: %s", preset)
	}
}

func buildNeo4jSocialModelExecutor(ctx context.Context, driver neo4j.DriverWithContext, database string, preset string, execute bool) (interface{}, error) {
	// 获取预设模型定义
	modelDef, err := buildNeo4jSocialPresetModel(preset)
	if err != nil {
		return nil, fmt.Errorf("failed to load preset model: %w", err)
	}

	defMap := modelDef
	constraints, _ := defMap["constraints"].([]string)
	queries, _ := defMap["queries"].(map[string]string)

	result := map[string]interface{}{
		"engine":       "neo4j",
		"strategy":     "social_model_executor",
		"preset":       preset,
		"constraints":  constraints,
		"sample_rules": queries,
	}

	if !execute {
		return result, nil
	}

	// 执行constraints和sample queries
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer session.Close(ctx)

	executionResults := make(map[string]interface{})
	
	// 执行constraints
	constraintResults := make([]map[string]interface{}, 0, len(constraints))
	for _, constraint := range constraints {
		_, err := session.Run(ctx, constraint, nil)
		if err != nil {
			constraintResults = append(constraintResults, map[string]interface{}{
				"constraint": constraint,
				"status":     "error",
				"message":    err.Error(),
			})
		} else {
			constraintResults = append(constraintResults, map[string]interface{}{
				"constraint": constraint,
				"status":     "success",
			})
		}
	}
	executionResults["constraints"] = constraintResults

	// 执行sample queries（获取元数据，不执行数据修改）
	sampleQueryResults := make(map[string]interface{})
	for qName, qCypher := range queries {
		sampleQueryResults[qName] = map[string]interface{}{
			"cypher":      qCypher,
			"description": fmt.Sprintf("Sample query for %s operation in %s model", qName, preset),
			"parameters":  extractCypherParameters(qCypher),
		}
	}
	executionResults["sample_queries"] = sampleQueryResults

	result["execution_results"] = executionResults
	result["status"] = "executed"

	return result, nil
}

func extractCypherParameters(cypher string) []string {
	params := make(map[string]bool)
	reg := regexp.MustCompile(`\$(\w+)`)
	matches := reg.FindAllStringSubmatch(cypher, -1)
	for _, match := range matches {
		if len(match) > 1 {
			params[match[1]] = true
		}
	}
	result := make([]string, 0, len(params))
	for p := range params {
		result = append(result, p)
	}
	sort.Strings(result)
	return result
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
