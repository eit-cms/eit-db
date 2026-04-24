package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	redisCompiledCommandPrefix  = "REDIS_CMD::"
	redisCompiledPipelinePrefix = "REDIS_PIPE::"
)

// RedisCompiledCommandPlan Redis 原生命令编译计划。
type RedisCompiledCommandPlan struct {
	Command  string        `json:"command"`
	Args     []interface{} `json:"args,omitempty"`
	ReadOnly bool          `json:"read_only,omitempty"`
}

// RedisCompiledPipelinePlan Redis Pipeline 编译计划。
type RedisCompiledPipelinePlan struct {
	Commands []RedisCompiledCommandPlan `json:"commands"`
	ReadOnly bool                       `json:"read_only,omitempty"`
}

// RedisQueryConstructorProvider Redis 原生查询构造器提供者。
type RedisQueryConstructorProvider struct {
	capabilities *QueryBuilderCapabilities
}

func NewRedisQueryConstructorProvider() *RedisQueryConstructorProvider {
	caps := DefaultQueryBuilderCapabilities()
	caps.SupportsEq = false
	caps.SupportsNe = false
	caps.SupportsGt = false
	caps.SupportsLt = false
	caps.SupportsGte = false
	caps.SupportsLte = false
	caps.SupportsIn = false
	caps.SupportsBetween = false
	caps.SupportsLike = false
	caps.SupportsAnd = false
	caps.SupportsOr = false
	caps.SupportsNot = false
	caps.SupportsSelect = false
	caps.SupportsOrderBy = false
	caps.SupportsLimit = false
	caps.SupportsOffset = false
	caps.SupportsJoin = false
	caps.SupportsSubquery = false
	caps.SupportsQueryPlan = false
	caps.SupportsIndex = false
	caps.SupportsNativeQuery = true
	caps.NativeQueryLang = "redis"
	caps.Description = "Redis Native Command Query Builder"

	return &RedisQueryConstructorProvider{capabilities: caps}
}

func (p *RedisQueryConstructorProvider) NewQueryConstructor(schema Schema) QueryConstructor {
	return NewRedisQueryConstructor(schema)
}

func (p *RedisQueryConstructorProvider) GetCapabilities() *QueryBuilderCapabilities {
	return p.capabilities
}

// RedisQueryConstructor Redis 原生查询构造器。
// 通过 GetNativeBuilder() 暴露命令式 API，不复用 SQL 风格条件语义。
type RedisQueryConstructor struct {
	schema       Schema
	commandPlan  *RedisCompiledCommandPlan
	pipelinePlan []RedisCompiledCommandPlan
	customMode   bool
}

func NewRedisQueryConstructor(schema Schema) *RedisQueryConstructor {
	return &RedisQueryConstructor{schema: schema}
}

func (qb *RedisQueryConstructor) Command(name string, args ...interface{}) *RedisQueryConstructor {
	qb.commandPlan = &RedisCompiledCommandPlan{
		Command:  strings.ToUpper(strings.TrimSpace(name)),
		Args:     append([]interface{}(nil), args...),
		ReadOnly: redisCommandIsReadOnly(name),
	}
	qb.pipelinePlan = nil
	return qb
}

func (qb *RedisQueryConstructor) Pipeline(commands ...RedisCompiledCommandPlan) *RedisQueryConstructor {
	qb.commandPlan = nil
	qb.pipelinePlan = make([]RedisCompiledCommandPlan, 0, len(commands))
	readOnly := true
	for _, cmd := range commands {
		cmd.Command = strings.ToUpper(strings.TrimSpace(cmd.Command))
		if !cmd.ReadOnly {
			cmd.ReadOnly = redisCommandIsReadOnly(cmd.Command)
		}
		if !cmd.ReadOnly {
			readOnly = false
		}
		qb.pipelinePlan = append(qb.pipelinePlan, cmd)
	}
	if readOnly {
		for i := range qb.pipelinePlan {
			qb.pipelinePlan[i].ReadOnly = true
		}
	}
	return qb
}

func (qb *RedisQueryConstructor) AddPipelineCommand(name string, args ...interface{}) *RedisQueryConstructor {
	qb.commandPlan = nil
	qb.pipelinePlan = append(qb.pipelinePlan, RedisCompiledCommandPlan{
		Command:  strings.ToUpper(strings.TrimSpace(name)),
		Args:     append([]interface{}(nil), args...),
		ReadOnly: redisCommandIsReadOnly(name),
	})
	return qb
}

func (qb *RedisQueryConstructor) Where(condition Condition) QueryConstructor                     { return qb }
func (qb *RedisQueryConstructor) WhereWith(builder *WhereBuilder) QueryConstructor              { return qb }
func (qb *RedisQueryConstructor) WhereAll(conditions ...Condition) QueryConstructor             { return qb }
func (qb *RedisQueryConstructor) WhereAny(conditions ...Condition) QueryConstructor             { return qb }
func (qb *RedisQueryConstructor) Select(fields ...string) QueryConstructor                      { return qb }
func (qb *RedisQueryConstructor) Count(fieldName ...string) QueryConstructor                    { return qb }
func (qb *RedisQueryConstructor) CountWith(builder *CountBuilder) QueryConstructor              { return qb }
func (qb *RedisQueryConstructor) OrderBy(field string, direction string) QueryConstructor       { return qb }
func (qb *RedisQueryConstructor) Limit(count int) QueryConstructor                              { return qb }
func (qb *RedisQueryConstructor) Offset(count int) QueryConstructor                             { return qb }
func (qb *RedisQueryConstructor) Page(page int, pageSize int) QueryConstructor                  { return qb }
func (qb *RedisQueryConstructor) Paginate(builder *PaginationBuilder) QueryConstructor          { return qb }
func (qb *RedisQueryConstructor) FromAlias(alias string) QueryConstructor                       { return qb }
func (qb *RedisQueryConstructor) Join(table, onClause string, alias ...string) QueryConstructor { return qb }
func (qb *RedisQueryConstructor) LeftJoin(table, onClause string, alias ...string) QueryConstructor {
	return qb
}
func (qb *RedisQueryConstructor) RightJoin(table, onClause string, alias ...string) QueryConstructor {
	return qb
}
func (qb *RedisQueryConstructor) CrossJoin(table string, alias ...string) QueryConstructor { return qb }
func (qb *RedisQueryConstructor) JoinWith(builder *JoinBuilder) QueryConstructor            { return qb }
func (qb *RedisQueryConstructor) CrossTableStrategy(strategy CrossTableStrategy) QueryConstructor {
	return qb
}

func (qb *RedisQueryConstructor) CustomMode() QueryConstructor {
	qb.customMode = true
	return qb
}

func (qb *RedisQueryConstructor) Build(ctx context.Context) (string, []interface{}, error) {
	_ = ctx
	if qb.commandPlan != nil {
		payload, err := json.Marshal(qb.commandPlan)
		if err != nil {
			return "", nil, err
		}
		return redisCompiledCommandPrefix + string(payload), nil, nil
	}
	if len(qb.pipelinePlan) > 0 {
		readOnly := true
		for _, cmd := range qb.pipelinePlan {
			if !cmd.ReadOnly {
				readOnly = false
				break
			}
		}
		payload, err := json.Marshal(RedisCompiledPipelinePlan{Commands: qb.pipelinePlan, ReadOnly: readOnly})
		if err != nil {
			return "", nil, err
		}
		return redisCompiledPipelinePrefix + string(payload), nil, nil
	}
	return "", nil, fmt.Errorf("redis query constructor requires Command() or Pipeline() via GetNativeBuilder()")
}

func (qb *RedisQueryConstructor) SelectCount(ctx context.Context, repo *Repository) (int64, error) {
	return 0, fmt.Errorf("redis query constructor does not implement SelectCount")
}

func (qb *RedisQueryConstructor) Upsert(ctx context.Context, repo *Repository, cs *Changeset, conflictColumns ...string) (sql.Result, error) {
	return nil, fmt.Errorf("redis query constructor does not implement Upsert")
}

func (qb *RedisQueryConstructor) GetNativeBuilder() interface{} {
	return qb
}

func redisCommandIsReadOnly(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "GET", "MGET", "HGET", "HGETALL", "HEXISTS", "LRANGE", "LLEN", "SMEMBERS", "SISMEMBER", "SCARD", "ZRANGE", "ZRANGEBYSCORE", "ZSCORE", "ZCARD", "TTL", "EXISTS", "PING", "INFO", "TYPE":
		return true
	default:
		return false
	}
}