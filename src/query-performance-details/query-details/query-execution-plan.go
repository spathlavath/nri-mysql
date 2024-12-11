package query_details

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
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
func PopulateExecutionPlans(db performance_database.DataSource, queries []performance_data_model.IndividualQueryMetrics, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]performance_data_model.QueryPlanMetrics, error) {
	var events []performance_data_model.QueryPlanMetrics

	for _, query := range queries {
		tableIngestionDataList := processExecutionPlanMetrics(db, query)
		events = append(events, tableIngestionDataList...)
	}

	if len(events) == 0 {
		return make([]performance_data_model.QueryPlanMetrics, 0), nil
	}

	err := SetExecutionPlanMetrics(i, args, events)
	if err != nil {
		log.Error("Error setting execution plan metrics: %v", err)
		return nil, err
	}

	return events, nil
}

// processExecutionPlanMetrics processes the execution plan metrics for a given query.
func processExecutionPlanMetrics(db performance_database.DataSource, query performance_data_model.IndividualQueryMetrics) []performance_data_model.QueryPlanMetrics {
	// supportedStatements := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "WITH": true}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if query.QueryText == "" {
		log.Warn("Query text is empty, skipping.")
		return nil
	}
	queryText := strings.TrimSpace(query.QueryText)
	upperQueryText := strings.ToUpper(queryText)

	if !isSupportedStatement(upperQueryText) {
		log.Warn("Skipping unsupported query for EXPLAIN: %s", queryText)
		return nil
	}

	if strings.Contains(queryText, "?") {
		log.Warn("Skipping query with placeholders for EXPLAIN: %s", queryText)
		return nil
	}

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

	dbPerformanceEvents, err := extractMetricsFromJSONString(execPlanJSON, query.EventID)
	if err != nil {
		log.Error("Error extracting metrics from JSON: %v", err)
		return nil
	}

	return dbPerformanceEvents
}

func extractMetricsFromJSONString(jsonString string, eventID uint64) ([]performance_data_model.QueryPlanMetrics, error) {
	js, err := simplejson.NewJson([]byte(jsonString))
	if err != nil {
		log.Error("Error creating simplejson from byte slice: %v", err)
		return nil, err
	}

	memo := performance_data_model.Memo{QueryCost: ""}
	dbPerformanceEvents := make([]performance_data_model.QueryPlanMetrics, 0)
	dbPerformanceEvents = extractMetrics(js, dbPerformanceEvents, eventID, memo)

	return dbPerformanceEvents, nil
}

func SetExecutionPlanMetrics(i *integration.Integration, args arguments.ArgumentList, metrics []performance_data_model.QueryPlanMetrics) error {
	e, err := common_utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
	common_utils.FatalIfErr(err)
	count := 0
	for _, metricObject := range metrics {
		ms := common_utils.CreateMetricSet(e, "MysqlQueryExecutionSample", args)

		publishQueryPerformanceMetrics(metricObject, ms)

		count++
		if count >= common_utils.MetricSetLimit {
			common_utils.FatalIfErr(i.Publish())

			e, err = common_utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
			common_utils.FatalIfErr(err)
			count = 0
		}
	}

	if count > 0 {
		common_utils.FatalIfErr(i.Publish())
	}
	return nil
}

func publishQueryPerformanceMetrics(metricObject performance_data_model.QueryPlanMetrics, ms *metric.Set) {
	metricsMap := map[string]struct {
		Value      interface{}
		MetricType metric.SourceType
	}{
		"event_id":      {metricObject.EventID, metric.GAUGE},
		"table_name":    {metricObject.TableName, metric.ATTRIBUTE},
		"query_cost":    {metricObject.QueryCost, metric.GAUGE},
		"access_type":   {metricObject.AccessType, metric.ATTRIBUTE},
		"rows_examined": {metricObject.RowsExaminedPerScan, metric.GAUGE},
		"rows_produced": {metricObject.RowsProducedPerJoin, metric.GAUGE},
		"filtered":      {metricObject.Filtered, metric.GAUGE},
		"read_cost":     {metricObject.ReadCost, metric.GAUGE},
		"eval_cost":     {metricObject.EvalCost, metric.GAUGE},
	}

	for metricName, metricData := range metricsMap {
		err := ms.SetMetric(metricName, metricData.Value, metricData.MetricType)
		if err != nil {
			log.Error("Error setting metric %s: %v", metricName, err)
		}
	}
}

func extractMetrics(js *simplejson.Json, dbPerformanceEvents []performance_data_model.QueryPlanMetrics, eventID uint64, memo performance_data_model.Memo) []performance_data_model.QueryPlanMetrics {
	tableName, _ := js.Get("table_name").String()
	queryCost, _ := js.Get("cost_info").Get("query_cost").String()
	accessType, _ := js.Get("access_type").String()
	rowsExaminedPerScan, _ := js.Get("rows_examined_per_scan").Int64()
	rowsProducedPerJoin, _ := js.Get("rows_produced_per_join").Int64()
	filtered, _ := js.Get("filtered").String()
	readCost, _ := js.Get("cost_info").Get("read_cost").String()
	evalCost, _ := js.Get("cost_info").Get("eval_cost").String()

	if queryCost != "" {
		memo.QueryCost = queryCost
	}

	if tableName != "" || accessType != "" || rowsExaminedPerScan != 0 || rowsProducedPerJoin != 0 || filtered != "" || readCost != "" || evalCost != "" {
		dbPerformanceEvents = append(dbPerformanceEvents, performance_data_model.QueryPlanMetrics{
			EventID:             eventID,
			QueryCost:           memo.QueryCost,
			TableName:           tableName,
			AccessType:          accessType,
			RowsExaminedPerScan: rowsExaminedPerScan,
			RowsProducedPerJoin: rowsProducedPerJoin,
			Filtered:            filtered,
			ReadCost:            readCost,
			EvalCost:            evalCost,
		})
		return dbPerformanceEvents
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

						dbPerformanceEvents = extractMetrics(convertedSimpleJson, dbPerformanceEvents, eventID, memo)
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

							dbPerformanceEvents = extractMetrics(convertedSimpleJson, dbPerformanceEvents, eventID, memo)
						}
					}
				}
			}
		}
	}

	return dbPerformanceEvents
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
