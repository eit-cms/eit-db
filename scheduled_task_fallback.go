package db

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson"
)

const defaultMonthlyCronSpec = "0 0 1 * *"

type scheduledTaskRuntime struct {
	config *ScheduledTaskConfig
	entry  cron.EntryID
	status *ScheduledTaskStatus
}

type inProcessScheduledTaskManager struct {
	mu        sync.RWMutex
	repo      *Repository
	scheduler *cron.Cron
	tasks     map[string]*scheduledTaskRuntime
	executor  ScheduledTaskExecutor
}

func newInProcessScheduledTaskManager(repo *Repository, executor ScheduledTaskExecutor) *inProcessScheduledTaskManager {
	return &inProcessScheduledTaskManager{
		repo:      repo,
		scheduler: cron.New(),
		tasks:     make(map[string]*scheduledTaskRuntime),
		executor:  executor,
	}
}

func (m *inProcessScheduledTaskManager) start() {
	m.scheduler.Start()
}

func (m *inProcessScheduledTaskManager) stop() {
	ctx := m.scheduler.Stop()
	<-ctx.Done()
}

func (m *inProcessScheduledTaskManager) register(ctx context.Context, task *ScheduledTaskConfig) error {
	if task == nil {
		return fmt.Errorf("task cannot be nil")
	}
	if err := task.Validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tasks[task.Name]; exists {
		return fmt.Errorf("scheduled task already exists: %s", task.Name)
	}

	spec := strings.TrimSpace(task.CronExpression)
	if spec == "" {
		spec = defaultMonthlyCronSpec
	}

	cfgCopy := cloneScheduledTaskConfig(task)
	entry, err := m.scheduler.AddFunc(spec, func() {
		m.executeTask(cfgCopy)
	})
	if err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", spec, err)
	}

	status := &ScheduledTaskStatus{
		Name:      cfgCopy.Name,
		Type:      cfgCopy.Type,
		Running:   false,
		CreatedAt: time.Now().Unix(),
		Info: map[string]interface{}{
			"mode": "in_process_fallback",
			"cron": spec,
		},
	}
	m.tasks[cfgCopy.Name] = &scheduledTaskRuntime{config: cfgCopy, entry: entry, status: status}
	m.refreshNextExecutionLocked(cfgCopy.Name)
	return nil
}

func (m *inProcessScheduledTaskManager) unregister(ctx context.Context, taskName string) error {
	_ = ctx
	name := strings.TrimSpace(taskName)
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	runtime, exists := m.tasks[name]
	if !exists {
		return fmt.Errorf("scheduled task not found: %s", name)
	}

	m.scheduler.Remove(runtime.entry)
	delete(m.tasks, name)
	return nil
}

func (m *inProcessScheduledTaskManager) list(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]*ScheduledTaskStatus, 0, len(m.tasks))
	for name, runtime := range m.tasks {
		_ = name
		cloned := *runtime.status
		if runtime.status.Info != nil {
			cloned.Info = make(map[string]interface{}, len(runtime.status.Info))
			for k, v := range runtime.status.Info {
				cloned.Info[k] = v
			}
		}
		if entry := m.scheduler.Entry(runtime.entry); !entry.Next.IsZero() {
			cloned.NextExecutedAt = entry.Next.Unix()
		}
		out = append(out, &cloned)
	}
	return out, nil
}

func (m *inProcessScheduledTaskManager) executeTask(task *ScheduledTaskConfig) {
	if task == nil {
		return
	}

	m.mu.Lock()
	runtime, exists := m.tasks[task.Name]
	if !exists {
		m.mu.Unlock()
		return
	}
	runtime.status.Running = true
	runtime.status.LastExecutedAt = time.Now().Unix()
	m.mu.Unlock()

	err := m.executor.Execute(context.Background(), task.Name, task)

	m.mu.Lock()
	defer m.mu.Unlock()
	runtime, exists = m.tasks[task.Name]
	if !exists {
		return
	}
	runtime.status.Running = false
	if runtime.status.Info == nil {
		runtime.status.Info = make(map[string]interface{})
	}
	if err != nil {
		runtime.status.Info["last_error"] = err.Error()
	} else {
		delete(runtime.status.Info, "last_error")
	}
	if entry := m.scheduler.Entry(runtime.entry); !entry.Next.IsZero() {
		runtime.status.NextExecutedAt = entry.Next.Unix()
	}
}

func (m *inProcessScheduledTaskManager) refreshNextExecutionLocked(taskName string) {
	runtime, exists := m.tasks[taskName]
	if !exists {
		return
	}
	if entry := m.scheduler.Entry(runtime.entry); !entry.Next.IsZero() {
		runtime.status.NextExecutedAt = entry.Next.Unix()
	}
}

type repositoryScheduledTaskExecutor struct {
	repo *Repository
}

func (e *repositoryScheduledTaskExecutor) Execute(ctx context.Context, taskName string, config *ScheduledTaskConfig) error {
	if config == nil {
		return fmt.Errorf("task config cannot be nil")
	}

	adapter := e.repo.getAdapterUnsafe()
	if adapter == nil {
		return fmt.Errorf("adapter is not initialized")
	}

	switch config.Type {
	case TaskTypeMonthlyTableCreation:
		return e.executeMonthlyTask(ctx, adapter, config)
	default:
		return fmt.Errorf("unsupported scheduled task type: %s", config.Type)
	}
}

func (e *repositoryScheduledTaskExecutor) executeMonthlyTask(ctx context.Context, adapter Adapter, task *ScheduledTaskConfig) error {
	cfg := task.GetMonthlyTableConfig()
	tableBase, _ := cfg["tableName"].(string)
	tableBase = sanitizeTaskObjectName(tableBase)
	if tableBase == "" {
		return fmt.Errorf("tableName is required")
	}
	monthFormat, _ := cfg["monthFormat"].(string)
	if strings.TrimSpace(monthFormat) == "" {
		monthFormat = "2006_01"
	}
	bucketName := tableBase + "_" + time.Now().AddDate(0, 1, 0).Format(monthFormat)

	switch a := adapter.(type) {
	case *MongoAdapter:
		if a.client == nil {
			return fmt.Errorf("mongodb client not connected")
		}
		db := a.client.Database(a.database)
		names, err := db.ListCollectionNames(ctx, bson.M{"name": bucketName})
		if err != nil {
			return err
		}
		if len(names) == 0 {
			if err := db.CreateCollection(ctx, bucketName); err != nil {
				return err
			}
		}
		return nil
	case *Neo4jAdapter:
		_, err := a.ExecCypher(ctx, "MERGE (b:ScheduledBucket {name: $name}) ON CREATE SET b.created_at = datetime()", map[string]interface{}{"name": bucketName})
		return err
	default:
		fieldDefs, _ := cfg["fieldDefinitions"].(string)
		if strings.TrimSpace(fieldDefs) == "" {
			fieldDefs = defaultSQLFieldDefinitionsForAdapter(adapter)
		}
		stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", bucketName, fieldDefs)
		_, err := adapter.Exec(ctx, stmt)
		return err
	}
}

func defaultSQLFieldDefinitionsForAdapter(adapter Adapter) string {
	switch adapter.(type) {
	case *MySQLAdapter:
		return "id BIGINT AUTO_INCREMENT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
	case *SQLiteAdapter:
		return "id INTEGER PRIMARY KEY AUTOINCREMENT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP"
	case *SQLServerAdapter:
		return "id BIGINT IDENTITY(1,1) PRIMARY KEY, created_at DATETIME2 DEFAULT SYSUTCDATETIME()"
	default:
		return "id BIGSERIAL PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
	}
}

func sanitizeTaskObjectName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return strings.Trim(b.String(), "_")
}

func cloneScheduledTaskConfig(task *ScheduledTaskConfig) *ScheduledTaskConfig {
	if task == nil {
		return nil
	}
	clone := &ScheduledTaskConfig{
		Name:           task.Name,
		Type:           task.Type,
		CronExpression: task.CronExpression,
		Description:    task.Description,
		Enabled:        task.Enabled,
	}
	if task.Config != nil {
		clone.Config = make(map[string]interface{}, len(task.Config))
		for k, v := range task.Config {
			clone.Config[k] = v
		}
	}
	return clone
}

func shouldUseScheduledTaskFallback(err error) bool {
	return IsScheduledTaskFallbackError(err)
}
