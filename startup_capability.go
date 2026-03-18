package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"
)

const (
	// StartupCapabilityJSONRuntime JSON 运行时能力。
	StartupCapabilityJSONRuntime = "json_runtime"
	// StartupCapabilityFullTextRuntime 全文运行时能力。
	StartupCapabilityFullTextRuntime = "full_text_runtime"
)

const (
	startupCapabilityModeStrict  = "strict"
	startupCapabilityModeLenient = "lenient"
)

var supportedStartupCapabilities = []string{
	StartupCapabilityJSONRuntime,
	StartupCapabilityFullTextRuntime,
}

// StartupCapabilityConfig 启动期能力体检配置。
type StartupCapabilityConfig struct {
	// 模式：strict | lenient（默认 lenient）
	Mode string `json:"mode" yaml:"mode"`

	// 按能力覆盖模式（strict | lenient）。
	// 可用于“全局 lenient，但对 json_runtime 强制 strict”。
	CapabilityModes map[string]string `json:"capability_modes,omitempty" yaml:"capability_modes,omitempty"`

	// 需要检查的能力名；为空时默认检查 json_runtime 和 full_text_runtime。
	Inspect []string `json:"inspect,omitempty" yaml:"inspect,omitempty"`

	// 必须满足的能力名（strict 模式下不满足将失败）。
	Required []string `json:"required,omitempty" yaml:"required,omitempty"`
}

// StartupCapabilityReport 启动体检报告。
type StartupCapabilityReport struct {
	Adapter     string                   `json:"adapter"`
	Mode        string                   `json:"mode"`
	GeneratedAt string                   `json:"generated_at"`
	Checks      []StartupCapabilityCheck `json:"checks"`
}

// StartupCapabilityCheck 单项能力检查结果。
type StartupCapabilityCheck struct {
	Name               string `json:"name"`
	EffectiveMode      string `json:"effective_mode"`
	Required           bool   `json:"required"`
	Supported          bool   `json:"supported"`
	InspectorAvailable bool   `json:"inspector_available"`
	Status             string `json:"status"`
	Notes              string `json:"notes,omitempty"`
	Error              string `json:"error,omitempty"`
}

func validateStartupCapabilityConfig(cfg *StartupCapabilityConfig) error {
	if cfg == nil {
		return nil
	}

	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == "" {
		mode = startupCapabilityModeLenient
	}
	if mode != startupCapabilityModeLenient && mode != startupCapabilityModeStrict {
		return fmt.Errorf("startup_capabilities.mode must be strict or lenient")
	}

	for _, name := range append(append([]string{}, cfg.Inspect...), cfg.Required...) {
		if !slices.Contains(supportedStartupCapabilities, strings.ToLower(strings.TrimSpace(name))) {
			return fmt.Errorf("startup_capabilities contains unsupported capability: %s", name)
		}
	}

	for capability, capabilityMode := range cfg.CapabilityModes {
		capabilityName := strings.ToLower(strings.TrimSpace(capability))
		if !slices.Contains(supportedStartupCapabilities, capabilityName) {
			return fmt.Errorf("startup_capabilities.capability_modes contains unsupported capability: %s", capability)
		}

		modeValue := strings.ToLower(strings.TrimSpace(capabilityMode))
		if modeValue != startupCapabilityModeLenient && modeValue != startupCapabilityModeStrict {
			return fmt.Errorf("startup_capabilities.capability_modes[%s] must be strict or lenient", capability)
		}
	}

	return nil
}

// RunStartupCapabilityCheck 执行启动期能力体检并根据 strict/lenient 决策。
func (r *Repository) RunStartupCapabilityCheck(ctx context.Context, cfg *StartupCapabilityConfig) (*StartupCapabilityReport, error) {
	if r == nil || r.adapter == nil || cfg == nil {
		return nil, nil
	}

	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == "" {
		mode = startupCapabilityModeLenient
	}

	inspectList := uniqueCapabilityNames(cfg.Inspect)
	requiredList := uniqueCapabilityNames(cfg.Required)
	if len(inspectList) == 0 {
		inspectList = append(inspectList, StartupCapabilityJSONRuntime, StartupCapabilityFullTextRuntime)
	}
	for _, req := range requiredList {
		if !slices.Contains(inspectList, req) {
			inspectList = append(inspectList, req)
		}
	}

	normalizedCapabilityModes := normalizeCapabilityModes(cfg.CapabilityModes)
	for capability := range normalizedCapabilityModes {
		if !slices.Contains(inspectList, capability) {
			inspectList = append(inspectList, capability)
		}
	}

	report := &StartupCapabilityReport{
		Adapter:     inferAdapterName(r.adapter),
		Mode:        mode,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Checks:      make([]StartupCapabilityCheck, 0, len(inspectList)),
	}

	strictFailures := make([]string, 0)
	for _, capability := range inspectList {
		effectiveMode := mode
		overrideMode, hasOverride := normalizedCapabilityModes[capability]
		if hasOverride {
			effectiveMode = overrideMode
		}
		enforced := hasOverride && overrideMode == startupCapabilityModeStrict
		if !enforced {
			enforced = slices.Contains(requiredList, capability) && effectiveMode == startupCapabilityModeStrict
		}

		check := StartupCapabilityCheck{
			Name:          capability,
			EffectiveMode: effectiveMode,
			Required:      slices.Contains(requiredList, capability),
		}

		switch capability {
		case StartupCapabilityJSONRuntime:
			inspector, ok := r.adapter.(JSONRuntimeInspector)
			check.InspectorAvailable = ok
			if !ok {
				check.Status = "unavailable"
				check.Supported = false
				check.Notes = "adapter does not implement JSONRuntimeInspector"
				if enforced {
					strictFailures = append(strictFailures, capability+": inspector unavailable")
				}
				break
			}

			runtime, err := inspector.InspectJSONRuntime(ctx)
			if err != nil {
				check.Status = "error"
				check.Supported = false
				check.Error = err.Error()
				if enforced {
					strictFailures = append(strictFailures, capability+": inspect error")
				}
				break
			}

			check.Supported = runtime != nil && runtime.NativeSupported
			if runtime != nil {
				check.Notes = runtime.Notes
			}
			if check.Supported {
				check.Status = "ok"
			} else {
				check.Status = "degraded"
				if enforced {
					strictFailures = append(strictFailures, capability+": not supported")
				}
			}

		case StartupCapabilityFullTextRuntime:
			inspector, ok := r.adapter.(FullTextRuntimeInspector)
			check.InspectorAvailable = ok
			if !ok {
				check.Status = "unavailable"
				check.Supported = false
				check.Notes = "adapter does not implement FullTextRuntimeInspector"
				if enforced {
					strictFailures = append(strictFailures, capability+": inspector unavailable")
				}
				break
			}

			runtime, err := inspector.InspectFullTextRuntime(ctx)
			if err != nil {
				check.Status = "error"
				check.Supported = false
				check.Error = err.Error()
				if enforced {
					strictFailures = append(strictFailures, capability+": inspect error")
				}
				break
			}

			check.Supported = runtime != nil && (runtime.NativeSupported || runtime.PluginAvailable)
			if runtime != nil {
				check.Notes = runtime.Notes
			}
			if check.Supported {
				check.Status = "ok"
			} else {
				check.Status = "degraded"
				if enforced {
					strictFailures = append(strictFailures, capability+": not supported")
				}
			}
		}

		report.Checks = append(report.Checks, check)
	}

	if payload, err := json.Marshal(report); err == nil {
		log.Printf("[eit-db] startup capability report: %s", payload)
	}

	if len(strictFailures) > 0 {
		return report, fmt.Errorf("strict startup capability check failed: %s", strings.Join(strictFailures, "; "))
	}
	return report, nil
}

func normalizeCapabilityModes(input map[string]string) map[string]string {
	if len(input) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(input))
	for capability, mode := range input {
		capabilityName := strings.ToLower(strings.TrimSpace(capability))
		modeValue := strings.ToLower(strings.TrimSpace(mode))
		if capabilityName == "" || modeValue == "" {
			continue
		}
		out[capabilityName] = modeValue
	}
	return out
}

func uniqueCapabilityNames(input []string) []string {
	out := make([]string, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, raw := range input {
		name := strings.ToLower(strings.TrimSpace(raw))
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func inferAdapterName(adapter Adapter) string {
	switch adapter.(type) {
	case *PostgreSQLAdapter:
		return "postgres"
	case *MySQLAdapter:
		return "mysql"
	case *SQLiteAdapter:
		return "sqlite"
	case *SQLServerAdapter:
		return "sqlserver"
	case *MongoAdapter:
		return "mongodb"
	case *Neo4jAdapter:
		return "neo4j"
	default:
		return "unknown"
	}
}
