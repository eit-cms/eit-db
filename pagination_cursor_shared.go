package db

import "strings"

func primaryKeyFieldNameOrDefault(schema Schema, fallback string) string {
	if schema != nil {
		if pk := schema.PrimaryKeyField(); pk != nil {
			if name := strings.TrimSpace(pk.Name); name != "" {
				return name
			}
		}
	}
	return strings.TrimSpace(fallback)
}

func buildStableCursorOrders(field string, direction string, primaryKeyField string) []OrderBy {
	field = strings.TrimSpace(field)
	if field == "" {
		return nil
	}

	direction = normalizeOrderDirection(direction)
	orders := []OrderBy{{Field: field, Direction: direction}}

	pk := strings.TrimSpace(primaryKeyField)
	if pk == "" {
		return orders
	}
	if normalizeOrderFieldName(field) != normalizeOrderFieldName(pk) {
		orders = append(orders, OrderBy{Field: pk, Direction: direction})
	}
	return orders
}

func ensureStableOffsetOrders(existing []OrderBy, primaryKeyField string) []OrderBy {
	pk := strings.TrimSpace(primaryKeyField)
	if pk == "" {
		return existing
	}

	if len(existing) == 0 {
		return mergeOrderBysIfMissing(existing, []OrderBy{{Field: pk, Direction: "ASC"}})
	}

	lastDirection := normalizeOrderDirection(existing[len(existing)-1].Direction)
	return mergeOrderBysIfMissing(existing, []OrderBy{{Field: pk, Direction: lastDirection}})
}

func buildStableCursorCondition(field string, direction string, cursorValue interface{}, cursorPrimaryValue interface{}, primaryKeyField string, requirePrimaryTieBreaker bool) (Condition, error) {
	field = strings.TrimSpace(field)
	if field == "" || cursorValue == nil {
		return nil, nil
	}

	direction = normalizeOrderDirection(direction)
	pk := strings.TrimSpace(primaryKeyField)
	if pk == "" || normalizeOrderFieldName(field) == normalizeOrderFieldName(pk) {
		return cursorComparisonCondition(field, direction, cursorValue), nil
	}

	if cursorPrimaryValue == nil {
		if requirePrimaryTieBreaker {
			return nil, ErrCursorPrimaryValueRequired(field)
		}
		return cursorComparisonCondition(field, direction, cursorValue), nil
	}

	return Or(
		cursorComparisonCondition(field, direction, cursorValue),
		And(
			Eq(field, cursorValue),
			cursorComparisonCondition(pk, direction, cursorPrimaryValue),
		),
	), nil
}

func mergeOrderBysIfMissing(base []OrderBy, extras []OrderBy) []OrderBy {
	if len(extras) == 0 {
		return base
	}
	out := append([]OrderBy(nil), base...)
	for _, extra := range extras {
		normalizedField := normalizeOrderFieldName(extra.Field)
		if normalizedField == "" {
			continue
		}
		exists := false
		for _, order := range out {
			if normalizeOrderFieldName(order.Field) == normalizedField {
				exists = true
				break
			}
		}
		if exists {
			continue
		}
		out = append(out, OrderBy{Field: strings.TrimSpace(extra.Field), Direction: normalizeOrderDirection(extra.Direction)})
	}
	return out
}

func ErrCursorPrimaryValueRequired(field string) error {
	return &cursorPrimaryValueRequiredError{field: field}
}

type cursorPrimaryValueRequiredError struct {
	field string
}

func (e *cursorPrimaryValueRequiredError) Error() string {
	return "cursor pagination on non-primary field \"" + e.field + "\" requires cursor primary key value"
}
