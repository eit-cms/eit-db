package db

import "context"

// JSONRuntimeCapability JSON 能力运行时探测结果。
type JSONRuntimeCapability struct {
	// JSON 函数能力是否可用（如 ISJSON/JSON_VALUE 或 json/jsonb 操作能力）。
	NativeSupported bool

	// 是否支持原生 JSON 数据类型（而非文本承载）。
	NativeJSONTypeSupported bool

	// 运行时检测到的引擎版本信息。
	Version string

	// 是否执行了插件检测（某些数据库 JSON 为内建能力，该值可为 false）。
	PluginChecked bool

	// 是否检测到插件（若插件不适用则为 false）。
	PluginAvailable bool

	// 检测到的插件名或说明。
	PluginName string

	// 额外说明（升级建议、降级建议等）。
	Notes string
}

// JSONRuntimeInspector 适配器可选接口：用于运行时探测 JSON 能力。
type JSONRuntimeInspector interface {
	InspectJSONRuntime(ctx context.Context) (*JSONRuntimeCapability, error)
}
