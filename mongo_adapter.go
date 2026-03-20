package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoAdapter MongoDB 适配器（最小可用版本：连接/健康检查）
type MongoAdapter struct {
	client               *mongo.Client
	database             string
	uri                  string
	relationJoinStrategy string
	hideThroughArtifacts bool
}

// NewMongoAdapter 创建 MongoAdapter（不建立连接）
func NewMongoAdapter(config *Config) (*MongoAdapter, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	resolved := config.ResolvedMongoConfig()
	return &MongoAdapter{
		database:             resolved.Database,
		uri:                  resolved.URI,
		relationJoinStrategy: resolved.RelationJoinStrategy,
		hideThroughArtifacts: mongoHideThroughArtifactsEnabled(resolved),
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
		resolved := config.ResolvedMongoConfig()
		uri = resolved.URI
		a.database = resolved.Database
		a.relationJoinStrategy = resolved.RelationJoinStrategy
		a.hideThroughArtifacts = mongoHideThroughArtifactsEnabled(resolved)
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

// GetQueryBuilderProvider 返回 MongoDB BSON Query Builder Provider。
func (a *MongoAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return NewMongoQueryConstructorProvider()
}

// ExecuteCompiledFindPlan 执行由 MongoQueryConstructor 生成的 Find 计划。
func (a *MongoAdapter) ExecuteCompiledFindPlan(ctx context.Context, query string) ([]map[string]interface{}, error) {
	if a.client == nil {
		return nil, fmt.Errorf("mongodb client not connected")
	}
	if !strings.HasPrefix(query, mongoCompiledQueryPrefix) {
		return nil, fmt.Errorf("invalid mongodb compiled query prefix")
	}

	payload := strings.TrimPrefix(query, mongoCompiledQueryPrefix)
	var plan MongoCompiledFindPlan
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		return nil, fmt.Errorf("failed to parse mongodb compiled query: %w", err)
	}

	collection := strings.TrimSpace(plan.Collection)
	if collection == "" {
		return nil, fmt.Errorf("mongodb compiled query requires collection")
	}

	coll := a.client.Database(a.database).Collection(collection)

	filter := bson.M{}
	if plan.Filter != nil {
		filter = plan.Filter
	}

	// 含 lookup 阶段时改用 aggregate pipeline，以支持跨集合关联。
	if len(plan.Lookups) > 0 {
		strategy := normalizeMongoRelationJoinStrategy(a.relationJoinStrategy)
		pipeline := make([]bson.M, 0, 2+len(plan.Lookups)+3)
		if len(filter) > 0 {
			pipeline = append(pipeline, bson.M{"$match": filter})
		}

		for _, lk := range plan.Lookups {
			stage, ok := buildMongoLookupStage(lk, strategy)
			if !ok {
				continue
			}
			pipeline = append(pipeline, stage)

			as := strings.TrimSpace(lk.As)
			if lk.Semantic == JoinSemanticRequired {
				pipeline = append(pipeline, bson.M{"$match": bson.M{as + ".0": bson.M{"$exists": true}}})
			}
		}

		if a.hideThroughArtifacts {
			throughFields := collectThroughArtifactFields(plan.Lookups)
			if len(throughFields) > 0 {
				pipeline = append(pipeline, bson.M{"$unset": throughFields})
			}
		}

		if len(plan.Sort) > 0 {
			sortSpec := bson.D{}
			for _, item := range plan.Sort {
				field := strings.TrimSpace(item.Field)
				if field == "" {
					continue
				}
				direction := 1
				if item.Direction < 0 {
					direction = -1
				}
				sortSpec = append(sortSpec, bson.E{Key: field, Value: direction})
			}
			if len(sortSpec) > 0 {
				pipeline = append(pipeline, bson.M{"$sort": sortSpec})
			}
		}
		if plan.Offset != nil {
			pipeline = append(pipeline, bson.M{"$skip": int64(*plan.Offset)})
		}
		if plan.Limit != nil {
			pipeline = append(pipeline, bson.M{"$limit": int64(*plan.Limit)})
		}
		if len(plan.Projection) > 0 {
			projection := bson.M{}
			for _, field := range plan.Projection {
				trimmed := strings.TrimSpace(field)
				if trimmed == "" {
					continue
				}
				projection[trimmed] = 1
			}
			if len(projection) > 0 {
				pipeline = append(pipeline, bson.M{"$project": projection})
			}
		}

		cursor, err := coll.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, err
		}
		defer cursor.Close(ctx)

		rows := make([]map[string]interface{}, 0)
		for cursor.Next(ctx) {
			entry := map[string]interface{}{}
			if err := cursor.Decode(&entry); err != nil {
				return nil, err
			}
			rows = append(rows, entry)
		}
		if err := cursor.Err(); err != nil {
			return nil, err
		}
		return rows, nil
	}

	findOpts := options.Find()
	if len(plan.Projection) > 0 {
		projection := bson.M{}
		for _, field := range plan.Projection {
			trimmed := strings.TrimSpace(field)
			if trimmed == "" {
				continue
			}
			projection[trimmed] = 1
		}
		if len(projection) > 0 {
			findOpts.SetProjection(projection)
		}
	}
	if len(plan.Sort) > 0 {
		sortSpec := bson.D{}
		for _, item := range plan.Sort {
			field := strings.TrimSpace(item.Field)
			if field == "" {
				continue
			}
			direction := 1
			if item.Direction < 0 {
				direction = -1
			}
			sortSpec = append(sortSpec, bson.E{Key: field, Value: direction})
		}
		if len(sortSpec) > 0 {
			findOpts.SetSort(sortSpec)
		}
	}
	if plan.Limit != nil {
		findOpts.SetLimit(int64(*plan.Limit))
	}
	if plan.Offset != nil {
		findOpts.SetSkip(int64(*plan.Offset))
	}

	cursor, err := coll.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	rows := make([]map[string]interface{}, 0)
	for cursor.Next(ctx) {
		entry := map[string]interface{}{}
		if err := cursor.Decode(&entry); err != nil {
			return nil, err
		}
		rows = append(rows, entry)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return rows, nil
}

func normalizeMongoRelationJoinStrategy(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "pipeline" {
		return "pipeline"
	}
	return "lookup"
}

func mongoHideThroughArtifactsEnabled(cfg *MongoConnectionConfig) bool {
	if cfg == nil || cfg.HideThroughArtifacts == nil {
		return true
	}
	return *cfg.HideThroughArtifacts
}

func collectThroughArtifactFields(lookups []MongoLookupStage) []string {
	fields := make([]string, 0)
	seen := map[string]bool{}
	for _, lk := range lookups {
		as := strings.TrimSpace(lk.As)
		if as == "" {
			continue
		}
		if !lk.ThroughArtifact && !strings.HasSuffix(as, "_through") {
			continue
		}
		if !seen[as] {
			fields = append(fields, as)
			seen[as] = true
		}
	}
	return fields
}

func buildMongoLookupStage(lk MongoLookupStage, strategy string) (bson.M, bool) {
	from := strings.TrimSpace(lk.From)
	localField := strings.TrimSpace(lk.LocalField)
	foreignField := strings.TrimSpace(lk.ForeignField)
	as := strings.TrimSpace(lk.As)
	if from == "" || localField == "" || foreignField == "" || as == "" {
		return nil, false
	}

	if normalizeMongoRelationJoinStrategy(strategy) == "pipeline" {
		localRef := "$$local_join_value"
		arrayExpr := bson.M{"$cond": bson.A{
			bson.M{"$isArray": localRef},
			localRef,
			bson.A{localRef},
		}}
		return bson.M{"$lookup": bson.M{
			"from": from,
			"let": bson.M{"local_join_value": "$" + localField},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$in": bson.A{"$" + foreignField, arrayExpr}}}},
			},
			"as": as,
		}}, true
	}

	return bson.M{"$lookup": bson.M{
		"from":         from,
		"localField":   localField,
		"foreignField": foreignField,
		"as":           as,
	}}, true
}

// ExecuteCompiledWritePlan 执行由 MongoQueryConstructor 生成的写入计划。
func (a *MongoAdapter) ExecuteCompiledWritePlan(ctx context.Context, query string) (*QueryConstructorExecSummary, error) {
	if a.client == nil {
		return nil, fmt.Errorf("mongodb client not connected")
	}
	if !strings.HasPrefix(query, mongoCompiledWritePrefix) {
		return nil, fmt.Errorf("invalid mongodb compiled write prefix")
	}

	payload := strings.TrimPrefix(query, mongoCompiledWritePrefix)
	var plan MongoCompiledWritePlan
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		return nil, fmt.Errorf("failed to parse mongodb compiled write: %w", err)
	}

	collection := strings.TrimSpace(plan.Collection)
	if collection == "" {
		return nil, fmt.Errorf("mongodb compiled write requires collection")
	}
	coll := a.client.Database(a.database).Collection(collection)

	summary := &QueryConstructorExecSummary{Counters: map[string]int{}}
	if plan.ReturnWriteDetail || plan.ReturnInsertedID {
		summary.Details = map[string]interface{}{}
	}

	switch strings.ToLower(strings.TrimSpace(plan.Operation)) {
	case "insert_one":
		if len(plan.Document) == 0 {
			return nil, fmt.Errorf("mongodb insert_one requires document")
		}
		res, err := coll.InsertOne(ctx, plan.Document)
		if err != nil {
			return nil, err
		}
		summary.RowsAffected = 1
		summary.Counters["inserted"] = 1
		if res != nil && res.InsertedID != nil {
			summary.Counters["inserted_id_present"] = 1
			if plan.ReturnInsertedID && summary.Details != nil {
				summary.Details["inserted_id"] = res.InsertedID
			}
		}
		if plan.ReturnWriteDetail && summary.Details != nil {
			summary.Details["inserted"] = 1
		}
		return summary, nil
	case "insert_many":
		if len(plan.Documents) == 0 {
			return nil, fmt.Errorf("mongodb insert_many requires documents")
		}
		docs := make([]interface{}, 0, len(plan.Documents))
		for _, d := range plan.Documents {
			docs = append(docs, d)
		}
		res, err := coll.InsertMany(ctx, docs)
		if err != nil {
			return nil, err
		}
		summary.RowsAffected = int64(len(plan.Documents))
		summary.Counters["inserted"] = len(plan.Documents)
		if res != nil {
			summary.Counters["inserted_ids"] = len(res.InsertedIDs)
			if plan.ReturnInsertedID && summary.Details != nil {
				summary.Details["inserted_ids"] = res.InsertedIDs
			}
			if plan.ReturnWriteDetail && summary.Details != nil {
				summary.Details["inserted_count"] = len(res.InsertedIDs)
			}
		}
		return summary, nil
	case "update_many":
		if len(plan.Update) == 0 {
			return nil, fmt.Errorf("mongodb update_many requires update document")
		}
		filter := bson.M{}
		if plan.Filter != nil {
			filter = plan.Filter
		}
		res, err := coll.UpdateMany(ctx, filter, plan.Update, options.Update().SetUpsert(plan.Upsert))
		if err != nil {
			return nil, err
		}
		if res != nil {
			summary.RowsAffected = int64(res.ModifiedCount)
			summary.Counters["matched"] = int(res.MatchedCount)
			summary.Counters["modified"] = int(res.ModifiedCount)
			summary.Counters["upserted"] = int(res.UpsertedCount)
			if plan.ReturnWriteDetail && summary.Details != nil {
				summary.Details["matched_count"] = res.MatchedCount
				summary.Details["modified_count"] = res.ModifiedCount
				summary.Details["upserted_count"] = res.UpsertedCount
				summary.Details["upserted_id"] = res.UpsertedID
			}
		}
		return summary, nil
	case "delete_many":
		filter := bson.M{}
		if plan.Filter != nil {
			filter = plan.Filter
		}
		res, err := coll.DeleteMany(ctx, filter)
		if err != nil {
			return nil, err
		}
		if res != nil {
			summary.RowsAffected = int64(res.DeletedCount)
			summary.Counters["deleted"] = int(res.DeletedCount)
			if plan.ReturnWriteDetail && summary.Details != nil {
				summary.Details["deleted_count"] = res.DeletedCount
			}
		}
		return summary, nil
	default:
		return nil, fmt.Errorf("mongodb compiled write operation not supported: %s", plan.Operation)
	}
}

// GetDatabaseFeatures MongoDB 特性声明（最小实现）
func (a *MongoAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return NewMongoDatabaseFeatures()
}

// GetQueryFeatures MongoDB 查询特性声明（最小实现）
func (a *MongoAdapter) GetQueryFeatures() *QueryFeatures {
	return NewMongoQueryFeatures()
}

// InspectFullTextRuntime MongoDB 运行时全文能力说明。
// 这里不依赖插件检测，默认给出应用层分词可用的能力声明。
func (a *MongoAdapter) InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error) {
	return &FullTextRuntimeCapability{
		NativeSupported:       true,
		PluginChecked:         false,
		PluginAvailable:       false,
		PluginName:            "",
		TokenizationSupported: true,
		TokenizationMode:      "application",
		Notes:                 "MongoDB can use text index; app-layer tokenization is supported for fallback/boost",
	}, nil
}

// HasCustomFeatureImplementation 声明 MongoDB 可用的自定义能力
func (a *MongoAdapter) HasCustomFeatureImplementation(feature string) bool {
	switch feature {
	case "foreign_keys", "composite_foreign_keys", "custom_joiner", "document_join", "full_text_search", "tokenized_full_text_search":
		return true
	default:
		return false
	}
}

// ExecuteCustomFeature 执行 MongoDB 的自定义能力（当前提供 document join 方案）
func (a *MongoAdapter) ExecuteCustomFeature(ctx context.Context, feature string, input map[string]interface{}) (interface{}, error) {
	switch feature {
	case "foreign_keys", "composite_foreign_keys", "custom_joiner", "document_join":
		localCollection, _ := input["local_collection"].(string)
		foreignCollection, _ := input["foreign_collection"].(string)
		asField, _ := input["as"].(string)

		localFields, _ := input["local_fields"].([]string)
		foreignFields, _ := input["foreign_fields"].([]string)

		if localCollection == "" || foreignCollection == "" || len(localFields) == 0 || len(foreignFields) == 0 {
			return nil, fmt.Errorf("mongodb custom_joiner requires local_collection, foreign_collection, local_fields, foreign_fields")
		}

		if len(localFields) != len(foreignFields) {
			return nil, fmt.Errorf("mongodb custom_joiner requires same number of local_fields and foreign_fields")
		}

		if asField == "" {
			asField = "joined_docs"
		}

		andConditions := make([]map[string]interface{}, 0, len(localFields))
		for i := range localFields {
			andConditions = append(andConditions, map[string]interface{}{
				"$eq": []interface{}{"$$local_" + localFields[i], "$" + foreignFields[i]},
			})
		}

		letVars := make(map[string]interface{}, len(localFields))
		for _, localField := range localFields {
			letVars["local_"+localField] = "$" + localField
		}

		pipeline := []map[string]interface{}{
			{
				"$lookup": map[string]interface{}{
					"from": foreignCollection,
					"let":  letVars,
					"pipeline": []map[string]interface{}{
						{
							"$match": map[string]interface{}{
								"$expr": map[string]interface{}{"$and": andConditions},
							},
						},
					},
					"as": asField,
				},
			},
		}

		return map[string]interface{}{
			"engine":             "mongodb",
			"strategy":           "aggregation_lookup",
			"local_collection":   localCollection,
			"foreign_collection": foreignCollection,
			"pipeline":           pipeline,
		}, nil
	case "full_text_search", "tokenized_full_text_search":
		collection, _ := input["collection"].(string)
		query, _ := input["query"].(string)
		fields, _ := input["fields"].([]string)
		if collection == "" || strings.TrimSpace(query) == "" {
			return nil, fmt.Errorf("mongodb text search requires collection and query")
		}
		if len(fields) == 0 {
			fields = []string{"content"}
		}

		tokens := tokenizeSearchTerms(query)
		if len(tokens) == 0 {
			tokens = []string{query}
		}

		// 生成应用层分词增强的 $regex 查询计划（可由上层替换成 Atlas Search / $text）
		andConditions := make([]map[string]interface{}, 0, len(tokens))
		for _, token := range tokens {
			orGroup := make([]map[string]interface{}, 0, len(fields))
			for _, field := range fields {
				orGroup = append(orGroup, map[string]interface{}{
					field: map[string]interface{}{
						"$regex":   token,
						"$options": "i",
					},
				})
			}
			andConditions = append(andConditions, map[string]interface{}{"$or": orGroup})
		}

		pipeline := []map[string]interface{}{
			{"$match": map[string]interface{}{"$and": andConditions}},
		}

		return map[string]interface{}{
			"engine":     "mongodb",
			"strategy":   "tokenized_regex_pipeline",
			"collection": collection,
			"fields":     fields,
			"tokens":     tokens,
			"pipeline":   pipeline,
		}, nil
	default:
		return nil, fmt.Errorf("mongodb custom feature not supported: %s", feature)
	}
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
