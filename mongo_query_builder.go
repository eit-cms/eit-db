package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

const mongoCompiledQueryPrefix = "MONGO_FIND::"
const mongoCompiledWritePrefix = "MONGO_WRITE::"

// mongoJoinClause MongoDB 跨集合关联信息（来自 JoinWith）。
type mongoJoinClause struct {
	semantic JoinSemantic // 已解析的语义意图
	schema   Schema       // 目标集合 Schema
	alias    string       // 输出字段名（as 字段）
	filters  []Condition  // 对连接目标集合的额外过滤条件
}

// MongoQueryConstructor MongoDB BSON 查询构造器。
// 目标是复用统一 QueryConstructor 接口，在 Mongo 下编译为 BSON Find 计划。
type MongoQueryConstructor struct {
	schema       Schema
	selectedCols []string
	countExpr    *string
	conditions   []Condition
	orderBys     []OrderBy
	limitVal     *int
	offsetVal    *int
	writePlan    *MongoCompiledWritePlan
	customMode   bool
	joins        []mongoJoinClause // 跨集合关联（通过关系注册表解析为 $lookup）
}

// MongoLookupStage 描述一个 MongoDB $lookup 聚合阶段的参数。
// 对于 required 语义（BelongsTo）：调用方应在 $lookup 后追加 $unwind{preserveNullAndEmptyArrays:false}
// 以过滤掉无匹配文档（等价于 INNER JOIN）。
// 对于 optional 语义（HasMany/HasOne）：无需额外处理（等价于 LEFT JOIN/OPTIONAL MATCH）。
type MongoLookupStage struct {
	From            string       `json:"from"`                      // 目标集合名
	LocalField      string       `json:"localField"`                // 本集合中的连接字段
	ForeignField    string       `json:"foreignField"`              // 目标集合中的连接字段
	As              string       `json:"as"`                        // 输出数组字段名（alias）
	Semantic        JoinSemantic `json:"semantic"`                  // optional → 保留空数组；required → 需过滤
	ThroughArtifact bool         `json:"throughArtifact,omitempty"` // true 表示中间关系临时字段
}

// MongoCompiledFindPlan 是 Mongo QueryConstructor 编译后的可执行计划。
type MongoCompiledFindPlan struct {
	Collection string                 `json:"collection"`
	Filter     map[string]interface{} `json:"filter"`
	Projection []string               `json:"projection,omitempty"`
	Sort       []MongoSortField       `json:"sort,omitempty"`
	Limit      *int                   `json:"limit,omitempty"`
	Offset     *int                   `json:"offset,omitempty"`
	Lookups    []MongoLookupStage     `json:"lookups,omitempty"` // $lookup 聚合阶段参数列表
}

// MongoSortField 表示 Mongo 排序字段。
type MongoSortField struct {
	Field     string `json:"field"`
	Direction int    `json:"direction"` // 1=ASC, -1=DESC
}

// MongoCompiledWritePlan 是 Mongo 写入计划。
// Operation 支持 insert_one / insert_many / update_many / delete_many。
type MongoCompiledWritePlan struct {
	Operation         string                   `json:"operation"`
	Collection        string                   `json:"collection"`
	Filter            map[string]interface{}   `json:"filter,omitempty"`
	Update            map[string]interface{}   `json:"update,omitempty"`
	Document          map[string]interface{}   `json:"document,omitempty"`
	Documents         []map[string]interface{} `json:"documents,omitempty"`
	Upsert            bool                     `json:"upsert,omitempty"`
	ReturnInsertedID  bool                     `json:"return_inserted_id,omitempty"`
	ReturnWriteDetail bool                     `json:"return_write_detail,omitempty"`
}

func NewMongoQueryConstructor(schema Schema) *MongoQueryConstructor {
	return &MongoQueryConstructor{
		schema:       schema,
		joins:        make([]mongoJoinClause, 0),
		selectedCols: make([]string, 0),
		conditions:   make([]Condition, 0),
		orderBys:     make([]OrderBy, 0),
	}
}

func (qb *MongoQueryConstructor) Where(condition Condition) QueryConstructor {
	if condition != nil {
		qb.conditions = append(qb.conditions, condition)
	}
	return qb
}

func (qb *MongoQueryConstructor) WhereWith(builder *WhereBuilder) QueryConstructor {
	if builder == nil {
		return qb
	}
	return qb.Where(builder.Build())
}

func (qb *MongoQueryConstructor) WhereAll(conditions ...Condition) QueryConstructor {
	if len(conditions) > 0 {
		qb.conditions = append(qb.conditions, And(conditions...))
	}
	return qb
}

func (qb *MongoQueryConstructor) WhereAny(conditions ...Condition) QueryConstructor {
	if len(conditions) > 0 {
		qb.conditions = append(qb.conditions, Or(conditions...))
	}
	return qb
}

func (qb *MongoQueryConstructor) Select(fields ...string) QueryConstructor {
	qb.countExpr = nil
	qb.selectedCols = append(qb.selectedCols, fields...)
	return qb
}

func (qb *MongoQueryConstructor) Count(fieldName ...string) QueryConstructor {
	expr := "COUNT(*)"
	if len(fieldName) > 0 && strings.TrimSpace(fieldName[0]) != "" && strings.TrimSpace(fieldName[0]) != "*" {
		expr = "COUNT(" + strings.TrimSpace(fieldName[0]) + ")"
	}
	qb.countExpr = &expr
	qb.selectedCols = nil
	qb.limitVal = nil
	qb.offsetVal = nil
	qb.orderBys = nil
	return qb
}

func (qb *MongoQueryConstructor) CountWith(builder *CountBuilder) QueryConstructor {
	if builder == nil {
		return qb.Count()
	}
	field := strings.TrimSpace(builder.field)
	if field == "" {
		field = "*"
	}
	expr := "COUNT(*)"
	if field != "*" {
		if builder.distinct {
			expr = "COUNT(DISTINCT " + field + ")"
		} else {
			expr = "COUNT(" + field + ")"
		}
	}
	if alias := strings.TrimSpace(builder.alias); alias != "" {
		expr += " AS " + alias
	}
	qb.countExpr = &expr
	qb.selectedCols = nil
	qb.limitVal = nil
	qb.offsetVal = nil
	qb.orderBys = nil
	return qb
}

func (qb *MongoQueryConstructor) OrderBy(field string, direction string) QueryConstructor {
	direction = strings.ToUpper(strings.TrimSpace(direction))
	if direction != "DESC" {
		direction = "ASC"
	}
	qb.orderBys = append(qb.orderBys, OrderBy{Field: strings.TrimSpace(field), Direction: direction})
	return qb
}

func (qb *MongoQueryConstructor) Limit(count int) QueryConstructor {
	qb.limitVal = &count
	return qb
}

func (qb *MongoQueryConstructor) Offset(count int) QueryConstructor {
	qb.offsetVal = &count
	return qb
}

func (qb *MongoQueryConstructor) Page(page int, pageSize int) QueryConstructor {
	_, normalizedPageSize, offset := normalizePaginationParams(page, pageSize)
	qb.limitVal = &normalizedPageSize
	if offset <= 0 {
		qb.offsetVal = nil
		return qb
	}
	qb.offsetVal = &offset
	return qb
}

func (qb *MongoQueryConstructor) Paginate(builder *PaginationBuilder) QueryConstructor {
	if builder == nil {
		return qb.Page(1, defaultQueryPageSize)
	}

	mode := builder.Mode
	if mode == "" {
		mode = PaginationModeAuto
	}

	if mode == PaginationModeCursor || (mode == PaginationModeAuto && strings.TrimSpace(builder.CursorField) != "") {
		field := strings.TrimSpace(builder.CursorField)
		if field != "" {
			direction := normalizeOrderDirection(builder.CursorDirection)
			pkField := primaryKeyFieldNameOrDefault(qb.schema, "id")
			qOrders := buildStableCursorOrders(field, direction, pkField)
			qb.orderBys = mergeOrderBysIfMissing(qb.orderBys, qOrders)

			cursorCond, err := buildStableCursorCondition(field, direction, builder.CursorValue, builder.CursorPrimaryValue, pkField, false)
			if err != nil {
				return qb
			}
			if cursorCond != nil {
				qb.Where(cursorCond)
			}
		}
		return qb.Page(1, builder.PageSize)
	}

	return qb.Page(builder.Page, builder.PageSize)
}

func (qb *MongoQueryConstructor) FromAlias(alias string) QueryConstructor {
	// Mongo 查询不使用 SQL 表别名，保留接口兼容。
	return qb
}

func (qb *MongoQueryConstructor) Join(table, onClause string, alias ...string) QueryConstructor {
	// JOIN 在 Mongo 中依赖聚合管道或应用层策略，不在基础 Find 构造器中支持。
	return qb
}

func (qb *MongoQueryConstructor) LeftJoin(table, onClause string, alias ...string) QueryConstructor {
	return qb.Join(table, onClause, alias...)
}

func (qb *MongoQueryConstructor) RightJoin(table, onClause string, alias ...string) QueryConstructor {
	return qb.Join(table, onClause, alias...)
}

func (qb *MongoQueryConstructor) CrossJoin(table string, alias ...string) QueryConstructor {
	return qb.Join(table, "", alias...)
}

func (qb *MongoQueryConstructor) JoinWith(builder *JoinBuilder) QueryConstructor {
	if builder == nil || builder.schema == nil {
		return qb
	}
	resolved := resolveJoinSemantic(builder.semantic, qb.schema, builder.schema)
	alias := strings.TrimSpace(builder.alias)
	if alias == "" {
		alias = strings.TrimSpace(builder.schema.TableName())
	}
	qb.joins = append(qb.joins, mongoJoinClause{
		semantic: resolved,
		schema:   builder.schema,
		alias:    alias,
		filters:  append([]Condition(nil), builder.filters...),
	})
	return qb
}

func (qb *MongoQueryConstructor) CrossTableStrategy(strategy CrossTableStrategy) QueryConstructor {
	// Mongo 不使用 SQL 跨表策略，保留接口兼容。
	return qb
}

func (qb *MongoQueryConstructor) CustomMode() QueryConstructor {
	qb.customMode = true
	return qb
}

func (qb *MongoQueryConstructor) Build(ctx context.Context) (string, []interface{}, error) {
	_ = ctx
	if qb.writePlan != nil {
		plan := *qb.writePlan
		if strings.TrimSpace(plan.Collection) == "" && qb.schema != nil {
			plan.Collection = strings.TrimSpace(qb.schema.TableName())
		}
		if strings.TrimSpace(plan.Collection) == "" {
			return "", nil, fmt.Errorf("mongo collection name is required")
		}
		if (plan.Operation == "update_many" || plan.Operation == "delete_many") && len(plan.Filter) == 0 {
			filter, err := qb.buildFilter()
			if err != nil {
				return "", nil, err
			}
			plan.Filter = filter
		}
		payload, err := json.Marshal(plan)
		if err != nil {
			return "", nil, fmt.Errorf("failed to marshal mongo write plan: %w", err)
		}
		return mongoCompiledWritePrefix + string(payload), nil, nil
	}

	if qb.countExpr != nil {
		return "", nil, fmt.Errorf("mongo query constructor does not support SQL-style Count build; use Mongo aggregation pipeline")
	}

	plan, err := qb.BuildFindPlan()
	if err != nil {
		return "", nil, err
	}

	payload, err := json.Marshal(plan)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal mongo find plan: %w", err)
	}

	return mongoCompiledQueryPrefix + string(payload), nil, nil
}

func (qb *MongoQueryConstructor) SelectCount(ctx context.Context, repo *Repository) (int64, error) {
	return 0, fmt.Errorf("mongo query constructor does not support SelectCount via SQL-style API")
}

func (qb *MongoQueryConstructor) Upsert(ctx context.Context, repo *Repository, cs *Changeset, conflictColumns ...string) (sql.Result, error) {
	return nil, fmt.Errorf("mongo query constructor does not support SQL-style Upsert; use native Mongo write plan")
}

// InsertOne 设置写入计划为单文档插入。
func (qb *MongoQueryConstructor) InsertOne(document map[string]interface{}) *MongoQueryConstructor {
	qb.writePlan = &MongoCompiledWritePlan{Operation: "insert_one", Document: document}
	return qb
}

// InsertMany 设置写入计划为批量插入。
func (qb *MongoQueryConstructor) InsertMany(documents []map[string]interface{}) *MongoQueryConstructor {
	qb.writePlan = &MongoCompiledWritePlan{Operation: "insert_many", Documents: documents}
	return qb
}

// UpdateMany 设置写入计划为批量更新。
// filter 默认取 Where 条件生成的 BSON 过滤器。
func (qb *MongoQueryConstructor) UpdateMany(setFields map[string]interface{}, upsert bool) *MongoQueryConstructor {
	qb.writePlan = &MongoCompiledWritePlan{
		Operation: "update_many",
		Update:    map[string]interface{}{"$set": setFields},
		Upsert:    upsert,
	}
	return qb
}

// DeleteMany 设置写入计划为批量删除。
// filter 默认取 Where 条件生成的 BSON 过滤器。
func (qb *MongoQueryConstructor) DeleteMany() *MongoQueryConstructor {
	qb.writePlan = &MongoCompiledWritePlan{Operation: "delete_many"}
	return qb
}

// ReturnInsertedID 请求在写入结果中附加 inserted_id / inserted_ids（仅插入操作有效）。
func (qb *MongoQueryConstructor) ReturnInsertedID() *MongoQueryConstructor {
	if qb.writePlan == nil {
		qb.writePlan = &MongoCompiledWritePlan{}
	}
	qb.writePlan.ReturnInsertedID = true
	return qb
}

// ReturnWriteDetail 请求在写入结果中附加详细信息（matched/modified/upserted/deleted 等）。
func (qb *MongoQueryConstructor) ReturnWriteDetail() *MongoQueryConstructor {
	if qb.writePlan == nil {
		qb.writePlan = &MongoCompiledWritePlan{}
	}
	qb.writePlan.ReturnWriteDetail = true
	return qb
}

func (qb *MongoQueryConstructor) buildFilter() (map[string]interface{}, error) {
	filter := map[string]interface{}{}
	if len(qb.conditions) == 0 {
		return filter, nil
	}

	andConditions := make([]map[string]interface{}, 0, len(qb.conditions))
	for _, cond := range qb.conditions {
		translated, err := translateMongoCondition(cond)
		if err != nil {
			return nil, err
		}
		if len(translated) > 0 {
			andConditions = append(andConditions, translated)
		}
	}
	if len(andConditions) == 1 {
		return andConditions[0], nil
	}
	if len(andConditions) > 1 {
		return map[string]interface{}{"$and": andConditions}, nil
	}
	return filter, nil
}

func (qb *MongoQueryConstructor) BuildFindPlan() (*MongoCompiledFindPlan, error) {
	if qb == nil || qb.schema == nil {
		return nil, fmt.Errorf("mongo query constructor schema is required")
	}
	collection := strings.TrimSpace(qb.schema.TableName())
	if collection == "" {
		return nil, fmt.Errorf("mongo collection name is required")
	}

	filter, err := qb.buildFilter()
	if err != nil {
		return nil, err
	}

	sortFields := make([]MongoSortField, 0, len(qb.orderBys))
	for _, order := range qb.orderBys {
		field := strings.TrimSpace(order.Field)
		if field == "" {
			continue
		}
		direction := 1
		if strings.EqualFold(strings.TrimSpace(order.Direction), "DESC") {
			direction = -1
		}
		sortFields = append(sortFields, MongoSortField{Field: field, Direction: direction})
	}

	projection := make([]string, 0, len(qb.selectedCols))
	for _, col := range qb.selectedCols {
		trimmed := strings.TrimSpace(col)
		if trimmed == "" {
			continue
		}
		if trimmed == "*" {
			projection = projection[:0]
			break
		}
		projection = append(projection, trimmed)
	}

	// 解析 $lookup 阶段（来自 JoinWith 的关系注册表）
	lookups := make([]MongoLookupStage, 0, len(qb.joins))
	for _, join := range qb.joins {
		stages := resolveMongoLookups(qb.schema, join)
		if len(stages) > 0 {
			lookups = append(lookups, stages...)
		}
	}

	return &MongoCompiledFindPlan{
		Collection: collection,
		Filter:     filter,
		Projection: projection,
		Sort:       sortFields,
		Limit:      qb.limitVal,
		Offset:     qb.offsetVal,
		Lookups:    lookups,
	}, nil
}

// resolveMongoLookups 从关系注册表或 FK 约束推断 $lookup 阶段参数。
// 推断优先级：① source→join 直接关系声明 → ② join→source 反向关系声明 → ③ FK 约束。
// 若无法确定关联字段则返回 nil（跳过该连接）。
func resolveMongoLookups(sourceSchema Schema, join mongoJoinClause) []MongoLookupStage {
	if join.schema == nil {
		return nil
	}
	targetTable := strings.TrimSpace(join.schema.TableName())
	if targetTable == "" {
		return nil
	}
	alias := join.alias
	if alias == "" {
		alias = targetTable
	}

	// many-to-many + through：编译为两段 lookup（source -> through -> target）
	if rel := buildQueryJoinRelationIR(sourceSchema, join.schema); rel != nil &&
		rel.Type == RelationManyToMany && rel.Through != nil && strings.TrimSpace(rel.Through.Table) != "" {
		sourcePK := primaryKeyFieldNameOrDefault(sourceSchema, "id")
		targetPK := primaryKeyFieldNameOrDefault(join.schema, "id")
		throughAlias := alias + "_through"

		return []MongoLookupStage{
			{
				From:            strings.TrimSpace(rel.Through.Table),
				LocalField:      sourcePK,
				ForeignField:    strings.TrimSpace(rel.Through.SourceKey),
				As:              throughAlias,
				Semantic:        JoinSemanticOptional,
				ThroughArtifact: true,
			},
			{
				From:         targetTable,
				LocalField:   throughAlias + "." + strings.TrimSpace(rel.Through.TargetKey),
				ForeignField: targetPK,
				As:           alias,
				Semantic:     join.semantic,
			},
		}
	}

	var localField, foreignField string

	// ① source → join 直接声明
	if rs, ok := sourceSchema.(RelationalSchema); ok {
		if rel := rs.FindRelation(targetTable); rel != nil && rel.ForeignKey != "" && rel.OriginKey != "" {
			switch rel.Type {
			case RelationHasMany, RelationHasOne:
				// FK 在 target 侧；本侧是 OriginKey（通常是 PK）
				localField = rel.OriginKey
				foreignField = rel.ForeignKey
			case RelationBelongsTo:
				// FK 在本侧
				localField = rel.ForeignKey
				foreignField = rel.OriginKey
			}
		}
	}

	// ② join → source 反向声明
	if localField == "" {
		if rj, ok := join.schema.(RelationalSchema); ok {
			if rel := rj.FindRelation(sourceSchema.TableName()); rel != nil && rel.ForeignKey != "" && rel.OriginKey != "" {
				switch rel.Type {
				case RelationBelongsTo:
					// join 持有 FK 指向 source；source 是 OriginKey，join 是 ForeignKey
					localField = rel.OriginKey
					foreignField = rel.ForeignKey
				case RelationHasMany, RelationHasOne:
					// source 持有 FK 指向 join（不常见但合法）
					localField = rel.ForeignKey
					foreignField = rel.OriginKey
				}
			}
		}
	}

	// ③ FK 约束回退
	if localField == "" {
		localField, foreignField = resolveMongoFieldsFromFK(sourceSchema, join.schema)
	}

	if localField == "" || foreignField == "" {
		return nil // 无法确定关联字段，跳过
	}

	return []MongoLookupStage{{
		From:         targetTable,
		LocalField:   localField,
		ForeignField: foreignField,
		As:           alias,
		Semantic:     join.semantic,
	}}
}

// resolveMongoFieldsFromFK 从 FK 约束推断 localField / foreignField（兜底方案）。
func resolveMongoFieldsFromFK(sourceSchema, joinSchema Schema) (localField, foreignField string) {
	// join Schema 持有 FK 指向 source（HasMany 场景）
	if cs, ok := joinSchema.(ConstrainedSchema); ok {
		for _, tc := range cs.Constraints() {
			if tc.Kind != ConstraintForeignKey {
				continue
			}
			if !strings.EqualFold(tc.RefTable, sourceSchema.TableName()) {
				continue
			}
			if len(tc.Fields) > 0 && len(tc.RefFields) > 0 {
				return tc.RefFields[0], tc.Fields[0] // local=source PK, foreign=join FK
			}
		}
	}
	// source Schema 持有 FK 指向 join（BelongsTo 场景）
	if cs, ok := sourceSchema.(ConstrainedSchema); ok {
		for _, tc := range cs.Constraints() {
			if tc.Kind != ConstraintForeignKey {
				continue
			}
			if !strings.EqualFold(tc.RefTable, joinSchema.TableName()) {
				continue
			}
			if len(tc.Fields) > 0 && len(tc.RefFields) > 0 {
				return tc.Fields[0], tc.RefFields[0] // local=source FK, foreign=join PK
			}
		}
	}
	return "", ""
}

func (qb *MongoQueryConstructor) GetNativeBuilder() interface{} {
	return qb
}

func translateMongoCondition(condition Condition) (map[string]interface{}, error) {
	switch c := condition.(type) {
	case *SimpleCondition:
		field := strings.TrimSpace(c.Field)
		if field == "" {
			return nil, fmt.Errorf("mongo condition field cannot be empty")
		}
		switch strings.ToLower(strings.TrimSpace(c.Operator)) {
		case "eq":
			return map[string]interface{}{field: c.Value}, nil
		case "ne":
			return map[string]interface{}{field: map[string]interface{}{"$ne": c.Value}}, nil
		case "gt":
			return map[string]interface{}{field: map[string]interface{}{"$gt": c.Value}}, nil
		case "lt":
			return map[string]interface{}{field: map[string]interface{}{"$lt": c.Value}}, nil
		case "gte":
			return map[string]interface{}{field: map[string]interface{}{"$gte": c.Value}}, nil
		case "lte":
			return map[string]interface{}{field: map[string]interface{}{"$lte": c.Value}}, nil
		case "like":
			pattern := strings.TrimSpace(fmt.Sprintf("%v", c.Value))
			if pattern == "" {
				return nil, fmt.Errorf("mongo like condition requires non-empty pattern")
			}
			return map[string]interface{}{field: map[string]interface{}{"$regex": pattern, "$options": "i"}}, nil
		case "in":
			values, ok := c.Value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("mongo in condition requires []interface{} value")
			}
			return map[string]interface{}{field: map[string]interface{}{"$in": values}}, nil
		case "between":
			rangeVals, ok := c.Value.([]interface{})
			if !ok || len(rangeVals) != 2 {
				return nil, fmt.Errorf("mongo between condition requires [min,max] values")
			}
			return map[string]interface{}{field: map[string]interface{}{"$gte": rangeVals[0], "$lte": rangeVals[1]}}, nil
		default:
			return nil, fmt.Errorf("mongo condition operator not supported: %s", c.Operator)
		}
	case *CompositeCondition:
		operator := strings.ToLower(strings.TrimSpace(c.Operator))
		items := make([]map[string]interface{}, 0, len(c.Conditions))
		for _, inner := range c.Conditions {
			translated, err := translateMongoCondition(inner)
			if err != nil {
				return nil, err
			}
			if len(translated) > 0 {
				items = append(items, translated)
			}
		}
		if len(items) == 0 {
			return map[string]interface{}{}, nil
		}
		switch operator {
		case "and":
			if len(items) == 1 {
				return items[0], nil
			}
			return map[string]interface{}{"$and": items}, nil
		case "or":
			return map[string]interface{}{"$or": items}, nil
		default:
			return nil, fmt.Errorf("mongo composite operator not supported: %s", c.Operator)
		}
	case *NotCondition:
		if c.Condition == nil {
			return nil, fmt.Errorf("mongo not condition requires inner condition")
		}
		translated, err := translateMongoCondition(c.Condition)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"$nor": []map[string]interface{}{translated}}, nil
	default:
		return nil, fmt.Errorf("mongo condition type not supported: %T", condition)
	}
}

// MongoQueryConstructorProvider MongoDB 查询构造器提供者。
type MongoQueryConstructorProvider struct{}

func NewMongoQueryConstructorProvider() *MongoQueryConstructorProvider {
	return &MongoQueryConstructorProvider{}
}

func (p *MongoQueryConstructorProvider) NewQueryConstructor(schema Schema) QueryConstructor {
	return NewMongoQueryConstructor(schema)
}

func (p *MongoQueryConstructorProvider) GetCapabilities() *QueryBuilderCapabilities {
	caps := DefaultQueryBuilderCapabilities()
	caps.SupportsJoin = false
	caps.SupportsSubquery = false
	caps.SupportsQueryPlan = false
	caps.SupportsIndex = true
	caps.SupportsNativeQuery = true
	caps.NativeQueryLang = "bson"
	caps.Description = "MongoDB BSON Query Constructor"
	return caps
}
