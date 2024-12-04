package query_details

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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

func PopulateExecutionPlans(db performance_database.DataSource, queries []performance_data_model.QueryPlanMetrics, e *integration.Entity, args arguments.ArgumentList) ([]map[string]interface{}, error) {
	var events []map[string]interface{}

	for _, query := range queries {
		tableIngestionDataList := processExecutionPlanMetrics(e, args, db, query)
		events = append(events, tableIngestionDataList...)
	}

	// Debugging: Log the number of events collected
	fmt.Printf("Total events collected: %d\n", len(events))

	if len(events) == 0 {
		return []map[string]interface{}{}, nil
	}

	// Set execution plan metrics
	err := SetExecutionPlanMetrics(e, args, events)
	if err != nil {
		log.Error("Error setting execution plan metrics: %v", err)
		return nil, err
	}

	return events, nil
}

func processExecutionPlanMetrics(e *integration.Entity, args arguments.ArgumentList, db performance_database.DataSource, query performance_data_model.QueryPlanMetrics) []map[string]interface{} {
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

	metrics := extractMetricsFromPlan(execPlan)

	var tableIngestionDataList []map[string]interface{}
	for _, metric := range metrics.TableMetrics {
		tableIngestionData := make(map[string]interface{})
		tableIngestionData["query_id"] = query.QueryID
		tableIngestionData["query_text"] = query.AnonymizedQueryText
		tableIngestionData["event_id"] = query.EventID
		tableIngestionData["total_cost"] = metrics.TotalCost
		tableIngestionData["step_id"] = int64(metric.StepID)
		tableIngestionData["execution_step"] = metric.ExecutionStep
		tableIngestionData["access_type"] = metric.AccessType
		tableIngestionData["rows_examined"] = int64(metric.RowsExamined)
		tableIngestionData["rows_produced"] = int64(metric.RowsProduced)
		tableIngestionData["filtered"] = float64(metric.Filtered)
		tableIngestionData["read_cost"] = float64(metric.ReadCost)
		tableIngestionData["eval_cost"] = float64(metric.EvalCost)
		tableIngestionData["data_read"] = float64(metric.DataRead)
		tableIngestionData["extra_info"] = metric.ExtraInfo

		// Debugging: Print the table ingestion data
		fmt.Println("tableIngestionData:", tableIngestionData)

		tableIngestionDataList = append(tableIngestionDataList, tableIngestionData)
	}

	return tableIngestionDataList
}

func SetExecutionPlanMetrics(e *integration.Entity, args arguments.ArgumentList, metrics []map[string]interface{}) error {
	// Debugging: Log the number of metrics to process
	fmt.Printf("Setting execution plan metrics for %d metrics\n", len(metrics))

	for _, metricObject := range metrics {
		// Create a new metric set for each metricObject
		ms := common_utils.CreateMetricSet(e, "MysqlQueryExecution", args)

		// Debugging: Print the contents of metricObject
		fmt.Println("Metric Object ---> ", metricObject)

		// Print the contents and types of metricObject
		fmt.Println("Metric Object Contents and Types:")
		for k, v := range metricObject {
			fmt.Printf("Key: %s, Value: %v, Type: %T\n", k, v, v)
		}

		// Proceed to set metrics as before
		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{
			"query_id":       {common_utils.GetStringValueSafe(metricObject["query_id"]), metric.ATTRIBUTE},
			"query_text":     {common_utils.GetStringValueSafe(metricObject["query_text"]), metric.ATTRIBUTE},
			"event_id":       {common_utils.GetInt64ValueSafe(metricObject["event_id"]), metric.GAUGE},
			"total_cost":     {common_utils.GetFloat64ValueSafe(metricObject["total_cost"]), metric.GAUGE},
			"step_id":        {common_utils.GetInt64ValueSafe(metricObject["step_id"]), metric.GAUGE},
			"execution_step": {common_utils.GetStringValueSafe(metricObject["execution_step"]), metric.ATTRIBUTE},
			"access_type":    {common_utils.GetStringValueSafe(metricObject["access_type"]), metric.ATTRIBUTE},
			"rows_examined":  {common_utils.GetInt64ValueSafe(metricObject["rows_examined"]), metric.GAUGE},
			"rows_produced":  {common_utils.GetInt64ValueSafe(metricObject["rows_produced"]), metric.GAUGE},
			"filtered":       {common_utils.GetFloat64ValueSafe(metricObject["filtered"]), metric.GAUGE},
			"read_cost":      {common_utils.GetFloat64ValueSafe(metricObject["read_cost"]), metric.GAUGE},
			"eval_cost":      {common_utils.GetFloat64ValueSafe(metricObject["eval_cost"]), metric.GAUGE},
			"data_read":      {common_utils.GetFloat64ValueSafe(metricObject["data_read"]), metric.GAUGE},
			"extra_info":     {common_utils.GetStringValueSafe(metricObject["extra_info"]), metric.ATTRIBUTE},
		}

		for name, metricData := range metricsMap {
			fmt.Println("name:", name)
			fmt.Println("metricData:", metricData.Value)
			fmt.Printf("Type of metricData.Value: %T\n", metricData.Value)

			// Convert uint64 to int64 or float64 if necessary
			if val, ok := metricData.Value.(uint64); ok {
				if val <= uint64(math.MaxInt64) {
					metricData.Value = int64(val)
				} else {
					// If it doesn't fit, convert to float64
					metricData.Value = float64(val)
				}
			}

			err := ms.SetMetric(name, metricData.Value, metricData.MetricType)
			if err != nil {
				log.Error("Error setting value for %s: %v", name, err)
				continue
			}
		}

		// Print the metric set for debugging
		fmt.Println("Metric Set:", ms)
	}

	return nil
}

// extractMetricsFromPlan processes the top-level query block and recursively extracts metrics.
func extractMetricsFromPlan(plan map[string]interface{}) performance_data_model.ExecutionPlan {
	var metrics performance_data_model.ExecutionPlan
	stepID := 0

	// Debugging: Log the entire plan
	fmt.Printf("Extracting metrics from plan: %+v\n", plan)

	if queryBlock, exists := plan["query_block"].(map[string]interface{}); exists {
		extractMetricsFromQueryBlock(queryBlock, &metrics, &stepID)
	} else {
		fmt.Println("No 'query_block' found in plan.")
	}

	return metrics
}

// extractMetricsFromQueryBlock processes a query block and extracts metrics, handling nested structures.
func extractMetricsFromQueryBlock(queryBlock map[string]interface{}, metrics *performance_data_model.ExecutionPlan, stepID *int) {
	// Debugging: Log the queryBlock
	fmt.Printf("Processing Query Block: %+v\n", queryBlock)

	if costInfo, exists := queryBlock["cost_info"].(map[string]interface{}); exists {
		metrics.TotalCost += getCostSafely(costInfo, "query_cost")
	}

	// Process tables directly in the query block
	if table, exists := queryBlock["table"].(map[string]interface{}); exists {
		tableMetrics, newStepID := extractTableMetrics(map[string]interface{}{"table": table}, *stepID)
		metrics.TableMetrics = append(metrics.TableMetrics, tableMetrics...)
		*stepID = newStepID
	}

	// Process nested loops
	if nestedLoop, exists := queryBlock["nested_loop"].([]interface{}); exists {
		for _, nested := range nestedLoop {
			if nestedMap, ok := nested.(map[string]interface{}); ok {
				extractMetricsFromQueryBlock(nestedMap, metrics, stepID)
			}
		}
	}
	// Additional processing for other operations (grouping, ordering, etc.) can be added here with similar debug logs
}

// extractTableMetrics extracts metrics from a table structure.
func extractTableMetrics(tableInfo map[string]interface{}, stepID int) ([]performance_data_model.TableMetrics, int) {
	fmt.Printf("Processing Table Info: %+v\n", tableInfo)

	var tableMetrics []performance_data_model.TableMetrics
	stepID++

	if table, exists := tableInfo["table"].(map[string]interface{}); exists {
		metrics := performance_data_model.TableMetrics{
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
				nestedMetrics, newStepID := extractTableMetrics(nestedMap, stepID)
				tableMetrics = append(tableMetrics, nestedMetrics...)
				stepID = newStepID
			}
		}
	}

	// Additional nested structures can be processed similarly

	return tableMetrics, stepID
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
