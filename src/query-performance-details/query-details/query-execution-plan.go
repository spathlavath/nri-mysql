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
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
)

type DBPerformanceEvent struct {
	QueryID             string  `json:"query_id"`
	EventID             uint64  `json:"event_id"`
	QueryCost           float64 `json:"query_cost"`
	TableName           string  `json:"table_name"`
	AccessType          string  `json:"access_type"`
	RowsExaminedPerScan int64   `json:"rows_examined_per_scan"`
	RowsProducedPerJoin int64   `json:"rows_produced_per_join"`
	Filtered            float64 `json:"filtered"`
	ReadCost            float64 `json:"read_cost"`
	EvalCost            float64 `json:"eval_cost"`
}

func PopulateExecutionPlans(db performance_database.DataSource, queries []performance_data_model.QueryPlanMetrics, e *integration.Entity, args arguments.ArgumentList) ([]DBPerformanceEvent, error) {
	var events []DBPerformanceEvent

	for _, query := range queries {
		fmt.Printf("Query: %v\n", query)
		tableIngestionDataList := processExecutionPlanMetrics(db, query)
		events = append(events, tableIngestionDataList...)
	}

	fmt.Printf("Total events collected: %d\n", len(events))

	if len(events) == 0 {
		return make([]DBPerformanceEvent, 0), nil
	}

	err := SetExecutionPlanMetrics(e, args, events)
	if err != nil {
		log.Error("Error setting execution plan metrics: %v", err)
		return nil, err
	}

	return events, nil
}

func processExecutionPlanMetrics(db performance_database.DataSource, query performance_data_model.QueryPlanMetrics) []DBPerformanceEvent {
	supportedStatements := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "WITH": true}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if query.QueryText == "" {
		fmt.Println("Query text is empty, skipping.")
		return nil
	}
	queryText := strings.TrimSpace(query.QueryText)
	upperQueryText := strings.ToUpper(queryText)

	if !supportedStatements[strings.Split(upperQueryText, " ")[0]] {
		log.Debug("Skipping unsupported query for EXPLAIN: %s", queryText)
		return nil
	}

	if strings.Contains(queryText, "?") {
		log.Debug("Skipping query with placeholders for EXPLAIN: %s", queryText)
		return nil
	}

	execPlanQuery := fmt.Sprintf("EXPLAIN FORMAT=JSON %s", queryText)
	fmt.Println("Executing EXPLAIN query:", execPlanQuery)

	rows, err := db.QueryxContext(ctx, execPlanQuery)
	if err != nil {
		log.Error("Error executing EXPLAIN for query '%s': %v", queryText, err)
		return nil
	}

	var execPlanJSON string
	if rows.Next() {
		err := rows.Scan(&execPlanJSON)

		if err != nil {
			log.Error("Failed to scan execution plan: %v", err)
			rows.Close()
			return nil
		}
	} else {
		log.Error("No rows returned from EXPLAIN for query '%s'", queryText)
		rows.Close()
		return nil
	}
	rows.Close()

	fmt.Println("Execution Plan JSON:")
	fmt.Println(execPlanJSON)

	dbPerformanceEvents, err := extractMetricsFromJSONString(execPlanJSON, query.QueryID, query.EventID)
	if err != nil {
		log.Error("Error extracting metrics from JSON: %v", err)
		return nil
	}

	return dbPerformanceEvents
}

func extractMetricsFromJSONString(jsonString, queryID string, eventID uint64) ([]DBPerformanceEvent, error) {
	js, err := simplejson.NewJson([]byte(jsonString))
	if err != nil {
		log.Error("Error creating simplejson from byte slice: %v", err)
		return nil, err
	}

	dbPerformanceEvents := make([]DBPerformanceEvent, 0)
	dbPerformanceEvents = extractMetrics(js, dbPerformanceEvents, queryID, eventID)

	return dbPerformanceEvents, nil
}

func SetExecutionPlanMetrics(e *integration.Entity, args arguments.ArgumentList, metrics []DBPerformanceEvent) error {
	ms := e.NewMetricSet("MysqlQueryExecutionPlan")
	ms.SetMetric("query_id", "aaaaa", metric.ATTRIBUTE)

	// for _, metricObject := range metrics {

	// 	fmt.Println("Metric Object ---> ", metricObject)
	// 	fmt.Println("Metric Object Contents and Types:")
	// 	fmt.Printf("%+v\n", metricObject)

	// 	publishQueryPerformanceMetrics(metricObject, ms)

	// 	common_utils.PrintMetricSet(ms)
	// }
	return nil
}

func publishQueryPerformanceMetrics(metricObject DBPerformanceEvent, ms *metric.Set) {
	metricsMap := map[string]struct {
		Value      interface{}
		MetricType metric.SourceType
	}{
		"query_id": {metricObject.QueryID, metric.ATTRIBUTE},
		// "query_text":    {metricObject.QueryText, metric.ATTRIBUTE},
		"event_id":      {metricObject.EventID, metric.GAUGE},
		"query_cost":    {metricObject.QueryCost, metric.GAUGE},
		"access_type":   {metricObject.AccessType, metric.ATTRIBUTE},
		"rows_examined": {metricObject.RowsExaminedPerScan, metric.GAUGE},
		"rows_produced": {metricObject.RowsProducedPerJoin, metric.GAUGE},
		"filtered":      {metricObject.Filtered, metric.GAUGE},
		"read_cost":     {metricObject.ReadCost, metric.GAUGE},
		"eval_cost":     {metricObject.EvalCost, metric.GAUGE},
	}

	for metricName, metricData := range metricsMap {
		fmt.Println("Setting metric:", metricName, "with value:", metricData.Value)
		err := ms.SetMetric(metricName, metricData.Value, metricData.MetricType)
		if err != nil {
			log.Error("Error setting metric %s: %v", metricName, err)
		}
	}
}

func extractMetrics(js *simplejson.Json, dbPerformanceEvents []DBPerformanceEvent, queryID string, eventID uint64) []DBPerformanceEvent {
	queryCost, _ := js.Get("cost_info").Get("query_cost").Float64()
	tableName, _ := js.Get("table_name").String()
	accessType, _ := js.Get("access_type").String()
	rowsExaminedPerScan, _ := js.Get("rows_examined_per_scan").Int64()
	rowsProducedPerJoin, _ := js.Get("rows_produced_per_join").Int64()
	filtered, _ := js.Get("filtered").Float64()
	readCost, _ := js.Get("cost_info").Get("read_cost").Float64()
	evalCost, _ := js.Get("cost_info").Get("eval_cost").Float64()

	if tableName != "" || accessType != "" || rowsExaminedPerScan != 0 || rowsProducedPerJoin != 0 || filtered != 0 || readCost != 0 || evalCost != 0 {
		dbPerformanceEvents = append(dbPerformanceEvents, DBPerformanceEvent{
			QueryID:             queryID,
			EventID:             eventID,
			QueryCost:           queryCost,
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

						dbPerformanceEvents = extractMetrics(convertedSimpleJson, dbPerformanceEvents, queryID, eventID)
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

							dbPerformanceEvents = extractMetrics(convertedSimpleJson, dbPerformanceEvents, queryID, eventID)
						}
					}
				}
			}
		}
	}

	return dbPerformanceEvents
}
