package query_details

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
)

const (
	explainQueryFormat  = "EXPLAIN FORMAT=JSON %s"
	supportedStatements = "SELECT INSERT UPDATE DELETE WITH"
)

// PopulateExecutionPlans populates execution plans for the given queries.
func PopulateExecutionPlans(db performance_database.DataSource, queryGroups []performance_data_model.QueryGroup, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]performance_data_model.QueryPlanMetrics, error) {
	var events []performance_data_model.QueryPlanMetrics

	for _, group := range queryGroups {
		dsn := performance_database.GenerateDSN(args, group.Database)
		// Open the DB connection
		db, err := performance_database.OpenDB(dsn)
		common_utils.FatalIfErr(err)
		defer db.Close()

		for _, query := range group.Queries {
			tableIngestionDataList := processExecutionPlanMetrics(db, query)
			events = append(events, tableIngestionDataList...)
		}
	}

	if len(events) == 0 {
		return make([]performance_data_model.QueryPlanMetrics, 0), nil
	}

	err := SetExecutionPlanMetrics(i, args, events)
	if err != nil {
		log.Error("Error publishing execution plan metrics: %v", err)
		return nil, err
	}

	return events, nil
}

// processExecutionPlanMetrics processes the execution plan metrics for a given query.
func processExecutionPlanMetrics(db performance_database.DataSource, query performance_data_model.IndividualQueryMetrics) []performance_data_model.QueryPlanMetrics {
	// supportedStatements := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "WITH": true}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if *query.QueryText == "" {
		log.Warn("Query text is empty, skipping.")
		return nil
	}
	queryText := strings.TrimSpace(*query.QueryText)
	upperQueryText := strings.ToUpper(queryText)

	// Check if the query is a supported statement
	if !isSupportedStatement(upperQueryText) {
		log.Warn("Skipping unsupported query for EXPLAIN: %s", queryText)
		return nil
	}

	// Skip queries with placeholders
	if strings.Contains(queryText, "?") {
		log.Warn("Skipping query with placeholders for EXPLAIN: %s", queryText)
		return nil
	}

	// Execute the EXPLAIN query
	execPlanQuery := fmt.Sprintf(explainQueryFormat, queryText)
	rows, err := db.QueryxContext(ctx, execPlanQuery)
	if err != nil {
		log.Error("Error executing EXPLAIN for query '%s': %v", queryText, err)
		return nil
	}
	defer rows.Close()

	var execPlanJSON string
	if rows.Next() {
		err := rows.Scan(&execPlanJSON)
		if err != nil {
			log.Error("Failed to scan execution plan: %v", err)
			return nil
		}
	} else {
		log.Error("No rows returned from EXPLAIN for query '%s'", queryText)
		return nil
	}

	// Extract metrics from the JSON string
	dbPerformanceEvents, err := extractMetricsFromJSONString(execPlanJSON, *query.EventID)
	if err != nil {
		log.Error("Error extracting metrics from JSON: %v", err)
		return nil
	}

	return dbPerformanceEvents
}

// extractMetricsFromJSONString extracts metrics from a JSON string.
func extractMetricsFromJSONString(jsonString string, eventID uint64) ([]performance_data_model.QueryPlanMetrics, error) {
	js, err := simplejson.NewJson([]byte(jsonString))
	if err != nil {
		log.Error("Error creating simplejson from byte slice: %v", err)
		return nil, err
	}

	memo := performance_data_model.Memo{QueryCost: ""}
	stepID := 0
	dbPerformanceEvents := make([]performance_data_model.QueryPlanMetrics, 0)
	dbPerformanceEvents = extractMetrics(js, dbPerformanceEvents, eventID, memo, &stepID)

	return dbPerformanceEvents, nil
}

// extractMetrics recursively extracts metrics from a simplejson.Json object.
func extractMetrics(js *simplejson.Json, dbPerformanceEvents []performance_data_model.QueryPlanMetrics, eventID uint64, memo performance_data_model.Memo, stepID *int) []performance_data_model.QueryPlanMetrics {
	tableName, _ := js.Get("table_name").String()
	queryCost, _ := js.Get("cost_info").Get("query_cost").String()
	accessType, _ := js.Get("access_type").String()
	rowsExaminedPerScan, _ := js.Get("rows_examined_per_scan").Int64()
	rowsProducedPerJoin, _ := js.Get("rows_produced_per_join").Int64()
	filtered, _ := js.Get("filtered").String()
	readCost, _ := js.Get("cost_info").Get("read_cost").String()
	evalCost, _ := js.Get("cost_info").Get("eval_cost").String()
	possibleKeysArray, _ := js.Get("possible_keys").StringArray()
	key, _ := js.Get("key").String()
	usedKeyPartsArray, _ := js.Get("used_key_parts").StringArray()
	refArray, _ := js.Get("ref").StringArray()
	attachedCondition, _ := js.Get("attached_condition").String()

	possibleKeys := strings.Join(possibleKeysArray, ",")
	usedKeyParts := strings.Join(usedKeyPartsArray, ",")
	ref := strings.Join(refArray, ",")

	if queryCost != "" {
		memo.QueryCost = queryCost
	}

	if tableName != "" || accessType != "" || rowsExaminedPerScan != 0 || rowsProducedPerJoin != 0 || filtered != "" || readCost != "" || evalCost != "" {
		dbPerformanceEvents = append(dbPerformanceEvents, performance_data_model.QueryPlanMetrics{
			EventID:             eventID,
			QueryCost:           memo.QueryCost,
			StepID:              *stepID,
			TableName:           tableName,
			AccessType:          accessType,
			RowsExaminedPerScan: rowsExaminedPerScan,
			RowsProducedPerJoin: rowsProducedPerJoin,
			Filtered:            filtered,
			ReadCost:            readCost,
			EvalCost:            evalCost,
			PossibleKeys:        possibleKeys,
			Key:                 key,
			UsedKeyParts:        usedKeyParts,
			Ref:                 ref,
			AttachedCondition:   attachedCondition,
		})
		*stepID++
	}

	if jsMap, _ := js.Map(); jsMap != nil {
		for _, value := range jsMap {
			if value != nil {
				t := reflect.TypeOf(value)
				if t.Kind() == reflect.Map {
					if t.Key().Kind() == reflect.String && t.Elem().Kind() == reflect.Interface {
						jsBytes, err := json.Marshal(value)
						if err != nil {
							log.Error("Error marshalling map: %v", err)
						}

						convertedSimpleJson, err := simplejson.NewJson(jsBytes)
						if err != nil {
							log.Error("Error creating simplejson from byte slice: %v", err)
						}

						dbPerformanceEvents = extractMetrics(convertedSimpleJson, dbPerformanceEvents, eventID, memo, stepID)
					}
				} else if t.Kind() == reflect.Slice {
					for _, element := range value.([]interface{}) {
						if elementJson, ok := element.(map[string]interface{}); ok {
							jsBytes, err := json.Marshal(elementJson)
							if err != nil {
								log.Error("Error marshalling map: %v", err)
							}

							convertedSimpleJson, err := simplejson.NewJson(jsBytes)
							if err != nil {
								log.Error("Error creating simplejson from byte slice: %v", err)
							}

							dbPerformanceEvents = extractMetrics(convertedSimpleJson, dbPerformanceEvents, eventID, memo, stepID)
						}
					}
				}
			}
		}
	}

	return dbPerformanceEvents
}

// SetExecutionPlanMetrics sets the execution plan metrics.
func SetExecutionPlanMetrics(i *integration.Integration, args arguments.ArgumentList, metrics []performance_data_model.QueryPlanMetrics) error {
	var metricList []interface{}
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	common_utils.IngestMetric(metricList, "MysqlQueryExecutionSample", i, args)

	return nil
}

// isSupportedStatement checks if the given query is a supported statement.
func isSupportedStatement(query string) bool {
	for _, stmt := range strings.Split(supportedStatements, " ") {
		if strings.HasPrefix(query, stmt) {
			return true
		}
	}
	return false
}
