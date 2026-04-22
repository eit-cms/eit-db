package db

import (
	"errors"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

const scheduledTaskMetadataTable = "eit_scheduled_tasks"

var ErrScheduledTaskFallbackRequired = errors.New("scheduled task requires application-layer cron fallback")

type ScheduledTaskFallbackReason string

const (
	ScheduledTaskFallbackReasonUnknown                   ScheduledTaskFallbackReason = "unknown"
	ScheduledTaskFallbackReasonAdapterUnsupported        ScheduledTaskFallbackReason = "adapter_unsupported"
	ScheduledTaskFallbackReasonNativeCapabilityMissing   ScheduledTaskFallbackReason = "native_capability_missing"
	ScheduledTaskFallbackReasonCronExpressionUnsupported ScheduledTaskFallbackReason = "cron_expression_unsupported"
)

type ScheduledTaskFallbackError struct {
	AdapterName string
	Reason      ScheduledTaskFallbackReason
	Detail      string
}

func (e *ScheduledTaskFallbackError) Error() string {
	if e == nil {
		return ErrScheduledTaskFallbackRequired.Error()
	}

	prefix := "scheduled task requires application-layer cron fallback"
	parts := make([]string, 0, 3)
	if strings.TrimSpace(e.AdapterName) != "" {
		parts = append(parts, strings.TrimSpace(e.AdapterName))
	}
	if strings.TrimSpace(string(e.Reason)) != "" {
		parts = append(parts, "reason="+strings.TrimSpace(string(e.Reason)))
	}
	if strings.TrimSpace(e.Detail) != "" {
		parts = append(parts, strings.TrimSpace(e.Detail))
	}
	if len(parts) == 0 {
		return prefix
	}
	return prefix + " (" + strings.Join(parts, "; ") + ")"
}

func (e *ScheduledTaskFallbackError) Unwrap() error {
	return ErrScheduledTaskFallbackRequired
}

func NewScheduledTaskFallbackErrorWithReason(adapterName string, reason ScheduledTaskFallbackReason, detail string) error {
	normalizedReason := strings.TrimSpace(string(reason))
	if normalizedReason == "" {
		normalizedReason = string(ScheduledTaskFallbackReasonUnknown)
	}

	return &ScheduledTaskFallbackError{
		AdapterName: strings.TrimSpace(adapterName),
		Reason:      ScheduledTaskFallbackReason(normalizedReason),
		Detail:      strings.TrimSpace(detail),
	}
}

func NewScheduledTaskFallbackError(adapterName, reason string) error {
	return NewScheduledTaskFallbackErrorWithReason(adapterName, ScheduledTaskFallbackReasonUnknown, reason)
}

func IsScheduledTaskFallbackError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrScheduledTaskFallbackRequired) {
		return true
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "not supported") {
		return true
	}
	if strings.Contains(msg, "not implemented") {
		return true
	}
	if strings.Contains(msg, "not yet implemented") {
		return true
	}
	if strings.Contains(msg, "does not support") {
		return true
	}
	return false
}

func ScheduledTaskFallbackReasonOf(err error) (ScheduledTaskFallbackReason, bool) {
	if err == nil {
		return "", false
	}

	var fallbackErr *ScheduledTaskFallbackError
	if errors.As(err, &fallbackErr) {
		if fallbackErr == nil {
			return ScheduledTaskFallbackReasonUnknown, true
		}
		if strings.TrimSpace(string(fallbackErr.Reason)) == "" {
			return ScheduledTaskFallbackReasonUnknown, true
		}
		return fallbackErr.Reason, true
	}

	if IsScheduledTaskFallbackError(err) {
		return ScheduledTaskFallbackReasonUnknown, true
	}

	return "", false
}

func scheduledTaskCreateTableRoutineName(taskName string) string {
	sanitized := sanitizeTaskObjectName(taskName)
	if sanitized == "" {
		sanitized = "scheduled_task"
	}
	return sanitized + "_create_table"
}

func scheduledTaskAgentJobName(taskName string) string {
	sanitized := sanitizeTaskObjectName(taskName)
	if sanitized == "" {
		sanitized = "scheduled_task"
	}
	return "eit_task_" + sanitized
}

func scheduledTaskEventName(taskName string) string {
	sanitized := sanitizeTaskObjectName(taskName)
	if sanitized == "" {
		sanitized = "scheduled_task"
	}
	return "evt_" + sanitized
}

func computeNextScheduledRun(spec string, now time.Time) (time.Time, error) {
	parsed, err := cron.ParseStandard(strings.TrimSpace(spec))
	if err != nil {
		return time.Time{}, err
	}
	return parsed.Next(now), nil
}
