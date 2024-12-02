package query_details

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	"github.com/olekukonko/tablewriter"
)

func PopulateExecutionPlans(db performance_database.DataSource, queries []performance_data_model.QueryPlanMetrics, e *integration.Entity, args arguments.ArgumentList) ([]map[string]interface{}, error) {
	supportedStatements := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "WITH": true}
	var events []map[string]interface{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, query := range queries {
		queryText := strings.TrimSpace(query.QueryText)
		upperQueryText := strings.ToUpper(queryText)

		if !supportedStatements[strings.Split(upperQueryText, " ")[0]] {
			fmt.Printf("Skipping unsupported query for EXPLAIN: %s\n", queryText)
			continue
		}

		if strings.Contains(queryText, "?") {
			fmt.Printf("Skipping query with placeholders for EXPLAIN: %s\n", queryText)
			continue
		}

		execPlanQuery := fmt.Sprintf("EXPLAIN FORMAT=JSON %s", queryText)
		rows, err := db.QueryxContext(ctx, execPlanQuery)
		if err != nil {
			log.Error("Error executing EXPLAIN for query '%s': %v\n", queryText, err)
			continue
		}
		defer rows.Close()

		var execPlanJSON string
		if rows.Next() {
			err := rows.Scan(&execPlanJSON)
			if err != nil {
				log.Error("Failed to scan execution plan: %v", err)
				continue
			}
		}

		var execPlan map[string]interface{}
		err = json.Unmarshal([]byte(execPlanJSON), &execPlan)
		if err != nil {
			log.Error("Failed to unmarshal execution plan: %v", err)
			continue
		}
		// fmt.Println("Query execPlan------", execPlan)
		metrics := extractMetricsFromPlan(execPlan)

		baseIngestionData := map[string]interface{}{
			"query_id":   query.QueryID,
			"query_text": query.AnonymizedQueryText,
			"total_cost": metrics.TotalCost,
			"step_id":    0,
		}

		events = append(events, baseIngestionData)
		// formatAsTable(metrics.TableMetrics)

		for _, metric := range metrics.TableMetrics {
			tableIngestionData := make(map[string]interface{})
			for k, v := range baseIngestionData {
				tableIngestionData[k] = v
			}
			tableIngestionData["step_id"] = metric.StepID
			tableIngestionData["Execution Step"] = metric.ExecutionStep
			tableIngestionData["access_type"] = metric.AccessType
			tableIngestionData["rows_examined"] = metric.RowsExamined
			tableIngestionData["rows_produced"] = metric.RowsProduced
			tableIngestionData["filtered (%)"] = metric.Filtered
			tableIngestionData["read_cost"] = metric.ReadCost
			tableIngestionData["eval_cost"] = metric.EvalCost
			tableIngestionData["data_read"] = metric.DataRead
			tableIngestionData["extra_info"] = metric.ExtraInfo

			events = append(events, tableIngestionData)
		}
	}

	if len(events) == 0 {
		return []map[string]interface{}{}, nil
	}
	planErr := setExecutionPlanMetrics(e, args, events)
	if planErr != nil {
		fmt.Println("Error setting execution plan metrics: ", planErr)
		log.Error("Error setting value for: %v", planErr)
	}
	return events, nil
}

func setExecutionPlanMetrics(e *integration.Entity, args arguments.ArgumentList, metrics []map[string]interface{}) error {
	//ms := common_utils.CreateMetricSet(e, "MysqlQueryExecutionPlan", args)
	for _, metricObject := range metrics {
		// Create a new metric set for each row
		ms := common_utils.CreateMetricSet(e, "MysqlQueryExecutionPlan", args)
		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{
			"query_id":       {common_utils.GetStringValueSafe(metricObject["query_id"]), metric.ATTRIBUTE},
			"query_text":     {common_utils.GetStringValueSafe(metricObject["query_text"]), metric.ATTRIBUTE},
			"total_cost":     {common_utils.GetFloat64ValueSafe(metricObject["total_cost"]), metric.GAUGE},
			"step_id":        {common_utils.GetInt64ValueSafe(metricObject["step_id"]), metric.GAUGE},
			"Execution Step": {common_utils.GetStringValueSafe(metricObject["Execution Step"]), metric.ATTRIBUTE},
			"access_type":    {common_utils.GetStringValueSafe(metricObject["access_type"]), metric.ATTRIBUTE},
			"rows_examined":  {common_utils.GetInt64ValueSafe(metricObject["rows_examined"]), metric.GAUGE},
			"rows_produced":  {common_utils.GetInt64ValueSafe(metricObject["rows_produced"]), metric.GAUGE},
			"filtered (%)":   {common_utils.GetFloat64ValueSafe(metricObject["filtered (%)"]), metric.GAUGE},
			"read_cost":      {common_utils.GetFloat64ValueSafe(metricObject["read_cost"]), metric.GAUGE},
			"eval_cost":      {common_utils.GetFloat64ValueSafe(metricObject["eval_cost"]), metric.GAUGE},
			"data_read":      {common_utils.GetFloat64ValueSafe(metricObject["data_read"]), metric.GAUGE},
			"extra_info":     {common_utils.GetStringValueSafe(metricObject["extra_info"]), metric.ATTRIBUTE},
		}

		for name, metricData := range metricsMap {
			err := ms.SetMetric(name, metricData.Value, metricData.MetricType)
			if err != nil {
				log.Error("Error setting value for %s: %v", name, err)
				continue
			}
		}

		// Print the metric set for debugging
		// common_utils.PrintMetricSet(ms)
	}

	return nil
}

func extractMetricsFromPlan(plan map[string]interface{}) performance_data_model.ExecutionPlan {
	var metrics performance_data_model.ExecutionPlan
	queryBlock, _ := plan["query_block"].(map[string]interface{})
	stepID := 0
	if costInfo, exists := queryBlock["cost_info"].(map[string]interface{}); exists {
		metrics.TotalCost = getCostSafely(costInfo, "query_cost")
	}

	if nestedLoop, exists := queryBlock["nested_loop"].([]interface{}); exists {
		for _, nested := range nestedLoop {
			if nestedMap, ok := nested.(map[string]interface{}); ok {
				nestedMetrics, newStepID := extractTableMetrics(nestedMap, stepID)
				metrics.TableMetrics = append(metrics.TableMetrics, nestedMetrics...)
				stepID = newStepID
			} else {
				log.Error("Unexpected type for nested element: %T", nested)
			}
		}
	}

	if orderingOp, exists := queryBlock["ordering_operation"].(map[string]interface{}); exists {
		if groupingOp, exists := orderingOp["grouping_operation"].(map[string]interface{}); exists {
			if nestedLoop, exists := groupingOp["nested_loop"].([]interface{}); exists {
				for _, nested := range nestedLoop {
					if nestedMap, ok := nested.(map[string]interface{}); ok {
						nestedMetrics, newStepID := extractTableMetrics(nestedMap, stepID)
						metrics.TableMetrics = append(metrics.TableMetrics, nestedMetrics...)
						stepID = newStepID
					} else {
						log.Error("Unexpected type for nested element in grouping_operation: %T", nested)
					}
				}
			}
		}
	}

	if table, exists := queryBlock["table"].(map[string]interface{}); exists {
		metricsTable, _ := extractTableMetrics(map[string]interface{}{"table": table}, stepID)
		metrics.TableMetrics = append(metrics.TableMetrics, metricsTable...)
	}

	return metrics
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

func extractTableMetrics(tableInfo map[string]interface{}, stepID int) ([]performance_data_model.TableMetrics, int) {
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

		tableMetrics = append(tableMetrics, metrics)
	}

	if nestedLoop, exists := tableInfo["nested_loop"].([]interface{}); exists {
		for _, nested := range nestedLoop {
			if nestedMap, ok := nested.(map[string]interface{}); ok {
				metrics, newStepID := extractTableMetrics(nestedMap, stepID)
				tableMetrics = append(tableMetrics, metrics...)
				stepID = newStepID
			} else {
				log.Error("Unexpected type for nested element: %T", nested)
			}
		}
	}

	return tableMetrics, stepID
}

func formatAsTable(metrics []performance_data_model.TableMetrics) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"step_id", "Execution Step", "access_type", "rows_examined", "rows_produced", "filtered (%)", "read_cost", "eval_cost", "data_read", "extra_info"})

	for _, metric := range metrics {
		row := []string{
			fmt.Sprintf("%d", metric.StepID),
			metric.ExecutionStep,
			metric.AccessType,
			fmt.Sprintf("%d", metric.RowsExamined),
			fmt.Sprintf("%d", metric.RowsProduced),
			fmt.Sprintf("%.2f", metric.Filtered),
			fmt.Sprintf("%.2f", metric.ReadCost),
			fmt.Sprintf("%.2f", metric.EvalCost),
			fmt.Sprintf("%.2f", metric.DataRead),
			metric.ExtraInfo,
		}
		table.Append(row)
	}

	table.Render()
}
