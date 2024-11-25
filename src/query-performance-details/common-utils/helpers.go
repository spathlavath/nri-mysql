package common_utils

import (
	"database/sql"
	"fmt"
	"github.com/newrelic/infra-integrations-sdk/v3/data/attribute"
	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	"strconv"
	"strings"
)

func GetStringValue(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func CreateMetricSet(e *integration.Entity, sampleName string, args arguments.ArgumentList) *metric.Set {
	return metricSet(
		e,
		sampleName,
		args.Hostname,
		args.Port,
		args.RemoteMonitoring,
	)
}

func metricSet(e *integration.Entity, eventType, hostname string, port int, remoteMonitoring bool) *metric.Set {
	if remoteMonitoring {
		return e.NewMetricSet(
			eventType,
			attribute.Attr("hostname", hostname),
			attribute.Attr("port", strconv.Itoa(port)),
		)
	}

	return e.NewMetricSet(
		eventType,
		attribute.Attr("port", strconv.Itoa(port)),
	)
}

func GetString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok {
		if strVal, ok := val.(string); ok {
			return strVal
		}
		// Log unexpected types
		log.Error("Unexpected type for %q: %T", key, val)
	}
	return "" // Default to empty string if nil or type doesn't match
}

func GetFloat64(m map[string]interface{}, key string) float64 {
	if val, ok := m[key]; ok {
		switch v := val.(type) {
		case float64:
			return v
		case string:
			parsedVal, err := parseSpecialFloat(v)
			if err == nil {
				return parsedVal
			}
			log.Error("Failed to parse string to float64 for key %q: %v", key, err)
		default:
			log.Error("Unhandled type for key %q: %T", key, val)
		}
	}
	return 0.0 // Default to 0.0 if nil or type doesn't match
}

func GetInt64(m map[string]interface{}, key string) int64 {
	if val, ok := m[key]; ok {
		switch v := val.(type) {
		case float64:
			return int64(v)
		case string:
			parsedVal, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				return parsedVal
			}
			log.Error("Failed to parse string to int64 for key %q: %v", key, err)
		default:
			log.Error("Unhandled type for key %q: %T", key, val)
		}
	}
	return 0 // Default to 0 if nil or type doesn't match
}

func parseSpecialFloat(value string) (float64, error) {
	multipliers := map[string]float64{
		"K": 1e3,
		"M": 1e6,
		"G": 1e9,
		"T": 1e12,
	}

	for suffix, multiplier := range multipliers {
		if strings.HasSuffix(value, suffix) {
			baseValue := strings.TrimSuffix(value, suffix)
			parsedVal, err := strconv.ParseFloat(baseValue, 64)
			if err != nil {
				return 0, err
			}
			return parsedVal * multiplier, nil
		}
	}

	return strconv.ParseFloat(value, 64)
}

func ConvertToStringArray(arr []interface{}) string {
	parts := make([]string, len(arr))
	for i, v := range arr {
		if str, ok := v.(string); ok {
			parts[i] = str
		} else {
			log.Error("Unexpected type in array at index %d: %T", i, v)
		}
	}
	return strings.Join(parts, ", ")
}

func GetStringValueSafe(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case sql.NullString:
		if v.Valid {
			return v.String
		}
		return ""
	default:
		log.Error("Unexpected type for value: %T", value)
		return ""
	}
}

func GetFloat64ValueSafe(value interface{}) float64 {
	if value == nil {
		return 0.0
	}
	switch v := value.(type) {
	case float64:
		return v
	case string:
		parsedVal, err := parseSpecialFloat(v)
		if err == nil {
			return parsedVal
		}
		log.Error("Failed to parse string to float64: %v", err)
	case sql.NullString:
		if v.Valid {
			parsedVal, err := parseSpecialFloat(v.String)
			if err == nil {
				return parsedVal
			}
			log.Error("Failed to parse sql.NullString to float64: %v", err)
		}
	default:
		log.Error("Unexpected type for value: %T", value)
	}
	return 0.0
}

func GetInt64ValueSafe(value interface{}) int64 {
	if value == nil {
		return 0
	}
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		parsedVal, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return parsedVal
		}
		log.Error("Failed to parse string to int64: %v", err)
	case sql.NullString:
		if v.Valid {
			parsedVal, err := strconv.ParseInt(v.String, 10, 64)
			if err == nil {
				return parsedVal
			}
			log.Error("Failed to parse sql.NullString to int64: %v", err)
		}
	default:
		log.Error("Unexpected type for value: %T", value)
	}
	return 0
}

func PrintMetricSet(ms *metric.Set) {
	fmt.Println("Metric Set Contents:")
	for name, metric := range ms.Metrics {
		fmt.Printf("Name: %s, Value: %v, Type: %v\n", name, metric, "unknown")
	}
}

func GetInt64Value(ni sql.NullInt64) int64 {
	if ni.Valid {
		return ni.Int64
	}
	return 0
}

func FatalIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
