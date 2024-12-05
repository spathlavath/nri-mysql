package query_details

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
)

func PopulateExecutionPlans(db performance_database.DataSource, queries []performance_data_model.QueryPlanMetrics, e *integration.Entity, args arguments.ArgumentList) ([]performance_data_model.Event, error) {
	var events []performance_data_model.Event
	ms := e.NewMetricSet("MysqlTest")
	ms.Metrics["name"] = "p1"
	for _, query := range queries {
		ms1 := e.NewMetricSet("MysqlTest1")
		ms1.Metrics["name"] = "p2"
		fmt.Printf("Query: %v\n", query)
		tableIngestionDataList := processExecutionPlanMetrics(e, args, db, query)
		events = append(events, tableIngestionDataList...)
	}

	// Debugging: Log the number of events collected
	fmt.Printf("Total events collected: %d\n", len(events))

	if len(events) == 0 {
		return make([]performance_data_model.Event, 0), nil
	}

	// Create and set metrics for MysqlTest2
	ms2 := e.NewMetricSet("MysqlTest2")
	ms2.Metrics["name"] = "p3"
	ms2.Metrics["total_events"] = len(events)

	// Set execution plan metrics
	err := SetExecutionPlanMetrics(e, args, events)
	if err != nil {
		log.Error("Error setting execution plan metrics: %v", err)
		return nil, err
	}

	return events, nil
}

func processExecutionPlanMetrics(e *integration.Entity, args arguments.ArgumentList, db performance_database.DataSource, query performance_data_model.QueryPlanMetrics) []performance_data_model.Event {
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

	// Debugging: Print the execution plan JSON
	fmt.Println("Execution Plan JSON:")
	fmt.Println(execPlanJSON)

	var execPlan map[string]interface{}
	err = json.Unmarshal([]byte(execPlanJSON), &execPlan)
	if err != nil {
		log.Error("Failed to unmarshal execution plan: %v", err)
		return nil
	}

	// Debugging: Print the unmarshaled execution plan
	fmt.Println("Unmarshaled Execution Plan:")
	fmt.Printf("%+v\n", execPlan)

	metrics := extractMetricsFromPlan(execPlan, query.QueryID, query.AnonymizedQueryText, query.EventID)

	return metrics
}

func extractMetricsFromPlan(plan map[string]interface{}, queryID, queryText string, eventID uint64) []performance_data_model.Event {
	var metrics []performance_data_model.Event
	stepID := 0

	// Debugging: Log the entire plan
	fmt.Printf("Extracting metrics from plan: %+v\n", plan)

	if queryBlock, exists := plan["query_block"].(map[string]interface{}); exists {
		extractMetricsFromQueryBlock(queryBlock, &metrics, &stepID, queryID, queryText, eventID, 0)
	} else {
		fmt.Println("No 'query_block' found in plan.")
	}

	return metrics
}

func extractMetricsFromQueryBlock(queryBlock map[string]interface{}, metrics *[]performance_data_model.Event, stepID *int, queryID, queryText string, eventID uint64, parentCost float64) {
	// Debugging: Log the queryBlock
	fmt.Printf("Processing Query Block: %+v\n", queryBlock)

	var totalCost float64
	if costInfo, exists := queryBlock["cost_info"].(map[string]interface{}); exists {
		totalCost = getCostSafely(costInfo, "query_cost")
	}

	// Process tables directly in the query block
	if table, exists := queryBlock["table"].(map[string]interface{}); exists {
		tableMetrics, newStepID := extractTableMetrics(map[string]interface{}{"table": table}, *stepID, queryID, queryText, eventID, totalCost)
		*metrics = append(*metrics, tableMetrics...)
		*stepID = newStepID
	}

	// Process nested loops
	if nestedLoop, exists := queryBlock["nested_loop"].([]interface{}); exists {
		for _, nested := range nestedLoop {
			if nestedMap, ok := nested.(map[string]interface{}); ok {
				extractMetricsFromQueryBlock(nestedMap, metrics, stepID, queryID, queryText, eventID, totalCost)
			}
		}
	}

	// Process grouping operations
	if groupingOp, exists := queryBlock["grouping_operation"].(map[string]interface{}); exists {
		extractMetricsFromQueryBlock(groupingOp, metrics, stepID, queryID, queryText, eventID, totalCost)
	}

	// Process ordering operations
	if orderingOp, exists := queryBlock["ordering_operation"].(map[string]interface{}); exists {
		if table, exists := orderingOp["table"].(map[string]interface{}); exists {
			tableMetrics, newStepID := extractTableMetrics(map[string]interface{}{"table": table}, *stepID, queryID, queryText, eventID, totalCost)
			*metrics = append(*metrics, tableMetrics...)
			*stepID = newStepID
		}

		if groupingOp, exists := orderingOp["grouping_operation"].(map[string]interface{}); exists {
			extractMetricsFromQueryBlock(groupingOp, metrics, stepID, queryID, queryText, eventID, totalCost)
		}

		// Process select list subqueries
		if subqueries, exists := orderingOp["select_list_subqueries"].([]interface{}); exists {
			for _, subquery := range subqueries {
				if subqueryMap, ok := subquery.(map[string]interface{}); ok {
					if subQueryBlock, exists := subqueryMap["query_block"].(map[string]interface{}); exists {
						extractMetricsFromQueryBlock(subQueryBlock, metrics, stepID, queryID, queryText, eventID, totalCost)
					}
				}
			}
		}
	}

	// Process windowing operations
	if windowing, exists := queryBlock["windowing"].(map[string]interface{}); exists {
		if bufferResult, exists := windowing["buffer_result"].(map[string]interface{}); exists {
			extractMetricsFromQueryBlock(bufferResult, metrics, stepID, queryID, queryText, eventID, totalCost)
		}
	}

	// Process select list subqueries
	if subqueries, exists := queryBlock["select_list_subqueries"].([]interface{}); exists {
		for _, subquery := range subqueries {
			if subqueryMap, ok := subquery.(map[string]interface{}); ok {
				if subQueryBlock, exists := subqueryMap["query_block"].(map[string]interface{}); exists {
					extractMetricsFromQueryBlock(subQueryBlock, metrics, stepID, queryID, queryText, eventID, totalCost)
				}
			}
		}
	}

	// Process materialized subqueries
	if materializedSubquery, exists := queryBlock["materialized_from_subquery"].(map[string]interface{}); exists {
		if subQueryBlock, exists := materializedSubquery["query_block"].(map[string]interface{}); exists {
			extractMetricsFromQueryBlock(subQueryBlock, metrics, stepID, queryID, queryText, eventID, totalCost)
		}
	}

	// Process union results
	if unionResult, exists := queryBlock["union_result"].(map[string]interface{}); exists {
		if querySpecifications, exists := unionResult["query_specifications"].([]interface{}); exists {
			for _, querySpec := range querySpecifications {
				if querySpecMap, ok := querySpec.(map[string]interface{}); ok {
					if subQueryBlock, exists := querySpecMap["query_block"].(map[string]interface{}); exists {
						extractMetricsFromQueryBlock(subQueryBlock, metrics, stepID, queryID, queryText, eventID, totalCost)
					}
				}
			}
		}
	}
}

func extractTableMetrics(tableInfo map[string]interface{}, stepID int, queryID, queryText string, eventID uint64, parentCost float64) ([]performance_data_model.Event, int) {
	fmt.Printf("Processing Table Info: %+v\n", tableInfo)

	var tableMetrics []performance_data_model.Event
	stepID++

	if table, exists := tableInfo["table"].(map[string]interface{}); exists {
		metrics := performance_data_model.Event{
			QueryID:       queryID,
			QueryText:     queryText,
			EventID:       eventID,
			TotalCost:     parentCost,
			StepID:        stepID,
			ExecutionStep: common_utils.GetString(table, "table_name"),
			AccessType:    common_utils.GetString(table, "access_type"),
			RowsExamined:  common_utils.GetInt64(table, "rows_examined_per_scan"),
			RowsProduced:  common_utils.GetInt64(table, "rows_produced_per_join"),
			Filtered:      common_utils.GetFloat64(table, "filtered"),
		}

		if costInfo, ok := table["cost_info"].(map[string]interface{}); ok {
			metrics.ReadCost = common_utils.GetFloat64(costInfo, "read_cost")
			metrics.EvalCost = common_utils.GetFloat64(costInfo, "eval_cost")
			metrics.DataRead = common_utils.GetFloat64(costInfo, "data_read_per_join")
		}

		if usedKeyParts, ok := table["used_key_parts"].([]interface{}); ok {
			metrics.ExtraInfo = common_utils.ConvertToStringArray(usedKeyParts)
		}

		// Debugging: Print the extracted table metrics
		fmt.Printf("Extracted Table Metrics: %+v\n", metrics)

		tableMetrics = append(tableMetrics, metrics)
	}

	// Handle nested loops within the table
	if nestedLoop, exists := tableInfo["nested_loop"].([]interface{}); exists {
		for _, nested := range nestedLoop {
			if nestedMap, ok := nested.(map[string]interface{}); ok {
				nestedMetrics, newStepID := extractTableMetrics(nestedMap, stepID, queryID, queryText, eventID, parentCost)
				tableMetrics = append(tableMetrics, nestedMetrics...)
				stepID = newStepID
			}
		}
	}

	// Handle attached subqueries within the table
	if attachedSubqueries, exists := tableInfo["attached_subqueries"].([]interface{}); exists {
		for _, subquery := range attachedSubqueries {
			if subqueryMap, ok := subquery.(map[string]interface{}); ok {
				if subQueryBlock, exists := subqueryMap["query_block"].(map[string]interface{}); exists {
					subMetrics := extractMetricsFromPlan(map[string]interface{}{"query_block": subQueryBlock}, queryID, queryText, eventID)
					tableMetrics = append(tableMetrics, subMetrics...)
				}
			}
		}
	}

	// Handle materialized subqueries within the table
	if materializedSubquery, exists := tableInfo["materialized_from_subquery"].(map[string]interface{}); exists {
		if subQueryBlock, exists := materializedSubquery["query_block"].(map[string]interface{}); exists {
			subMetrics := extractMetricsFromPlan(map[string]interface{}{"query_block": subQueryBlock}, queryID, queryText, eventID)
			tableMetrics = append(tableMetrics, subMetrics...)
		}
	}

	// Handle union results within the table
	if unionResult, exists := tableInfo["union_result"].(map[string]interface{}); exists {
		if querySpecifications, exists := unionResult["query_specifications"].([]interface{}); exists {
			for _, querySpec := range querySpecifications {
				if querySpecMap, ok := querySpec.(map[string]interface{}); ok {
					if subQueryBlock, exists := querySpecMap["query_block"].(map[string]interface{}); exists {
						subMetrics := extractMetricsFromPlan(map[string]interface{}{"query_block": subQueryBlock}, queryID, queryText, eventID)
						tableMetrics = append(tableMetrics, subMetrics...)
					}
				}
			}
		}
	}

	return tableMetrics, stepID
}

func SetExecutionPlanMetrics(e *integration.Entity, args arguments.ArgumentList, metrics []performance_data_model.Event) error {
	// Debugging: Log the number of metrics to process
	fmt.Printf("Setting execution plan metrics for %d metrics\n", len(metrics))
	for _, metricObject := range metrics {
		// Create a new metric set for each metricObject
		ms := common_utils.CreateMetricSet(e, "MysqlQueryExecution", args)

		// Debugging: Print the contents of metricObject
		fmt.Println("Metric Object ---> ", metricObject)

		// Print the contents and types of metricObject
		fmt.Println("Metric Object Contents and Types:")
		fmt.Printf("%+v\n", metricObject)

		ms.SetMetric("query_id", metricObject.QueryID, metric.ATTRIBUTE)
		ms.SetMetric("query_text", metricObject.QueryText, metric.ATTRIBUTE)
		ms.SetMetric("event_id", metricObject.EventID, metric.GAUGE)
		ms.SetMetric("total_cost", metricObject.TotalCost, metric.GAUGE)
		ms.SetMetric("step_id", metricObject.StepID, metric.GAUGE)
		ms.SetMetric("execution_step", metricObject.ExecutionStep, metric.ATTRIBUTE)
		ms.SetMetric("access_type", metricObject.AccessType, metric.ATTRIBUTE)
		ms.SetMetric("rows_examined", metricObject.RowsExamined, metric.GAUGE)
		ms.SetMetric("rows_produced", metricObject.RowsProduced, metric.GAUGE)
		ms.SetMetric("filtered", metricObject.Filtered, metric.GAUGE)
		ms.SetMetric("read_cost", metricObject.ReadCost, metric.GAUGE)
		ms.SetMetric("eval_cost", metricObject.EvalCost, metric.GAUGE)
		ms.SetMetric("data_read", metricObject.DataRead, metric.GAUGE)
		ms.SetMetric("extra_info", metricObject.ExtraInfo, metric.ATTRIBUTE)

		// Print the metric set for debugging
		common_utils.PrintMetricSet(ms)
	}

	return nil
}

func getCostSafely(costInfo map[string]interface{}, key string) float64 {
	if costValue, ok := costInfo[key]; ok {
		switch v := costValue.(type) {
		case float64:
			return v
		case string:
			parsedVal, err := strconv.ParseFloat(v, 64)
			if err == nil {
				return parsedVal
			}
			log.Error("Failed to parse string to float64 for key %q: %v", key, err)
		default:
			log.Error("Unhandled type for key %q: %T", key, costValue)
		}
	}
	return 0.0 // Default to 0.0 if key doesn't exist or type doesn't match
}
