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

func PopulateExecutionPlans(db performance_database.DataSource, queries []performance_data_model.QueryPlanMetrics, e *integration.Entity, args arguments.ArgumentList) ([]map[string]interface{}, error) {
	var events []map[string]interface{}

	// fmt.Println("queries", queries)
	// mm := common_utils.CreateMetricSet(e, "outsideLoop", args)
	// mm.SetMetric("query_id", "aaaaa", metric.ATTRIBUTE)
	for _, query := range queries {
		mm := common_utils.CreateMetricSet(e, "insideLoop", args)
		mm.SetMetric("query_id", "aaaaa", metric.ATTRIBUTE)
		tableIngestionData := processExecutionPlanMetrics(e, args, db, query)
		// events = append(events, baseIngestionData)
		events = append(events, tableIngestionData)
	}

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

func processExecutionPlanMetrics(e *integration.Entity, args arguments.ArgumentList, db performance_database.DataSource, query performance_data_model.QueryPlanMetrics) map[string]interface{} {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	supportedStatements := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "WITH": true}

	if query.QueryText == "" {
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
	}
	rows.Close()

	mm := common_utils.CreateMetricSet(e, "InsideLoop1", args)
	mm.SetMetric("query_id", "aaaaa", metric.ATTRIBUTE)

	var execPlan map[string]interface{}
	err = json.Unmarshal([]byte(execPlanJSON), &execPlan)
	if err != nil {
		log.Error("Failed to unmarshal execution plan: %v", err)
		return nil
	}

	metrics := extractMetricsFromPlan(execPlan)

	baseIngestionData := map[string]interface{}{
		"query_id":   query.QueryID,
		"query_text": query.AnonymizedQueryText,
		"event_id":   query.EventID,
		"total_cost": metrics.TotalCost,
	}

	tableIngestionData := make(map[string]interface{})
	for _, metric := range metrics.TableMetrics {
		for k, v := range baseIngestionData {
			tableIngestionData[k] = v
		}
		tableIngestionData["step_id"] = metric.StepID
		tableIngestionData["execution_step"] = metric.ExecutionStep
		tableIngestionData["access_type"] = metric.AccessType
		tableIngestionData["rows_examined"] = metric.RowsExamined
		tableIngestionData["rows_produced"] = metric.RowsProduced
		tableIngestionData["filtered (%)"] = metric.Filtered
		tableIngestionData["read_cost"] = metric.ReadCost
		tableIngestionData["eval_cost"] = metric.EvalCost
		tableIngestionData["data_read"] = metric.DataRead
		tableIngestionData["extra_info"] = metric.ExtraInfo

	}
	fmt.Println("tableIngestionData", tableIngestionData)
	fmt.Print("baseIngestionData", baseIngestionData)
	return tableIngestionData
}

func SetExecutionPlanMetrics(e *integration.Entity, args arguments.ArgumentList, metrics []map[string]interface{}) error {
	// mm:=common_utils.CreateMetricSet(e, "outsideLoopMet", args)
	// mm.SetMetric("query_id","aaaaa" , metric.ATTRIBUTE)
	ms1 := common_utils.CreateMetricSet(e, "MysqlQueryExecutionVcc1", args)
	ms1.SetMetric("query_id", "testtingweds", metric.ATTRIBUTE)
	for _, metricObject := range metrics {
		processExecutionMetricsIngestion(e, args, metricObject)

	}

	return nil
}

func processExecutionMetricsIngestion(e *integration.Entity, args arguments.ArgumentList, metricObject map[string]interface{}) {
	ms := common_utils.CreateMetricSet(e, "MysqlQueryExecutionV2", args)
	// Debugging: Print the contents of metricObject
	fmt.Println("Metric Object ---> ", metricObject)

	// Access and print the value before passing to GetStringValueSafe
	queryId := metricObject["query_id"]
	fmt.Println("query_id before GetStringValueSafe:", queryId)

	// Debugging: Print the value after passing to GetStringValueSafe
	queryIdSafe := common_utils.GetStringValueSafe(queryId)
	fmt.Println("query_id after GetStringValueSafe:", queryIdSafe)

	metricsMap := map[string]struct {
		Value      interface{}
		MetricType metric.SourceType
	}{
		"query_id":       {common_utils.GetStringValueSafe(metricObject["query_id"]), metric.ATTRIBUTE},
		"query_text":     {common_utils.GetStringValueSafe(metricObject["query_text"]), metric.ATTRIBUTE},
		"total_cost":     {common_utils.GetFloat64ValueSafe(metricObject["total_cost"]), metric.GAUGE},
		"step_id":        {common_utils.GetInt64ValueSafe(metricObject["step_id"]), metric.GAUGE},
		"execution_step": {common_utils.GetStringValueSafe(metricObject["execution_step"]), metric.ATTRIBUTE},
		"access_type":    {common_utils.GetStringValueSafe(metricObject["access_type"]), metric.ATTRIBUTE},
		"rows_examined":  {common_utils.GetInt64ValueSafe(metricObject["rows_examined"]), metric.GAUGE},
		"rows_produced":  {common_utils.GetInt64ValueSafe(metricObject["rows_produced"]), metric.GAUGE},
		"filtered (%)":   {common_utils.GetFloat64ValueSafe(metricObject["filtered (%)"]), metric.GAUGE},
		"read_cost":      {common_utils.GetFloat64ValueSafe(metricObject["read_cost"]), metric.GAUGE},
		"eval_cost":      {common_utils.GetFloat64ValueSafe(metricObject["eval_cost"]), metric.GAUGE},
		"data_read":      {common_utils.GetFloat64ValueSafe(metricObject["data_read"]), metric.GAUGE},
		"extra_info":     {common_utils.GetStringValueSafe(metricObject["extra_info"]), metric.ATTRIBUTE},
	}
	fmt.Println("metricsMap", metricsMap)

	for name, metricData := range metricsMap {
		fmt.Println("name", name)
		fmt.Println("metricData", metricData.Value)

		// switch v := metricData.Value.(type) {
		// case int, int32, int64:
		// 	ms.SetMetric(name, v, metricData.MetricType)
		// case float32, float64:
		// 	ms.SetMetric(name, v, metricData.MetricType)
		// case string:
		// 	// if v == "" {
		// 	// 	return fmt.Errorf("value for %s is an empty string", v)
		// 	// }
		// 	ms.SetMetric(name, v, metricData.MetricType)
		// default:
		// 	fmt.Println("unexpected type for value:", v)
		// 	// return fmt.Errorf("unexpected type for value %s:", v)
		// }
		err := ms.SetMetric(name, metricData.Value, metricData.MetricType)
		if err != nil {
			log.Error("Error setting value for %s: %v", name, err)
			continue
		}
	}
}

// extractMetricsFromPlan processes the top-level query block and recursively extracts metrics.
func extractMetricsFromPlan(plan map[string]interface{}) performance_data_model.ExecutionPlan {

	// var planResult map[string]interface{}
	// data, err := json.Marshal(plan)

	// if err != nil {
	// 	return performance_data_model.ExecutionPlan{}
	// }

	// err = json.Unmarshal(data, &planResult)

	// if err != nil {
	// 	return performance_data_model.ExecutionPlan{}
	// }

	var metrics performance_data_model.ExecutionPlan
	stepID := 0

	if queryBlock, exists := plan["query_block"].(map[string]interface{}); exists {
		extractMetricsFromQueryBlock(queryBlock, &metrics, &stepID)
	}

	return metrics
}

// extractMetricsFromQueryBlock processes a query block and extracts metrics, handling nested structures.
func extractMetricsFromQueryBlock(queryBlock map[string]interface{}, metrics *performance_data_model.ExecutionPlan, stepID *int) {
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
	if groupingOp, exists := queryBlock["grouping_operation"].(map[string]interface{}); exists {
		extractMetricsFromQueryBlock(groupingOp, metrics, stepID)
	}
	// Process ordering operations
	if orderingOp, exists := queryBlock["ordering_operation"].(map[string]interface{}); exists {
		if table, exists := orderingOp["table"].(map[string]interface{}); exists {
			tableMetrics, newStepID := extractTableMetrics(map[string]interface{}{"table": table}, *stepID)
			metrics.TableMetrics = append(metrics.TableMetrics, tableMetrics...)
			*stepID = newStepID
		}

		if groupingOp, exists := orderingOp["grouping_operation"].(map[string]interface{}); exists {
			extractMetricsFromQueryBlock(groupingOp, metrics, stepID)
		}

		// Process select list subqueries
		if subqueries, exists := orderingOp["select_list_subqueries"].([]interface{}); exists {
			for _, subquery := range subqueries {
				if subqueryMap, ok := subquery.(map[string]interface{}); ok {
					if subQueryBlock, exists := subqueryMap["query_block"].(map[string]interface{}); exists {
						extractMetricsFromQueryBlock(subQueryBlock, metrics, stepID)
					}
				}
			}
		}
	}

	// Process windowing operations
	if windowing, exists := queryBlock["windowing"].(map[string]interface{}); exists {
		if bufferResult, exists := windowing["buffer_result"].(map[string]interface{}); exists {
			extractMetricsFromQueryBlock(bufferResult, metrics, stepID)
		}
	}

	// Process select list subqueries
	if subqueries, exists := queryBlock["select_list_subqueries"].([]interface{}); exists {
		for _, subquery := range subqueries {
			if subqueryMap, ok := subquery.(map[string]interface{}); ok {
				if subQueryBlock, exists := subqueryMap["query_block"].(map[string]interface{}); exists {
					extractMetricsFromQueryBlock(subQueryBlock, metrics, stepID)
				}
			}
		}
	}

	// Process materialized subqueries
	if materializedSubquery, exists := queryBlock["materialized_from_subquery"].(map[string]interface{}); exists {
		if subQueryBlock, exists := materializedSubquery["query_block"].(map[string]interface{}); exists {
			extractMetricsFromQueryBlock(subQueryBlock, metrics, stepID)
		}
	}

	// Process union results
	if unionResult, exists := queryBlock["union_result"].(map[string]interface{}); exists {
		if querySpecifications, exists := unionResult["query_specifications"].([]interface{}); exists {
			for _, querySpec := range querySpecifications {
				if querySpecMap, ok := querySpec.(map[string]interface{}); ok {
					if subQueryBlock, exists := querySpecMap["query_block"].(map[string]interface{}); exists {
						extractMetricsFromQueryBlock(subQueryBlock, metrics, stepID)
					}
				}
			}
		}
	}
}

// extractTableMetrics extracts metrics from a table structure.
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

	// Handle attached subqueries within the table
	if attachedSubqueries, exists := tableInfo["attached_subqueries"].([]interface{}); exists {
		for _, subquery := range attachedSubqueries {
			if subqueryMap, ok := subquery.(map[string]interface{}); ok {
				if subQueryBlock, exists := subqueryMap["query_block"].(map[string]interface{}); exists {
					subMetrics := extractMetricsFromPlan(map[string]interface{}{"query_block": subQueryBlock})
					tableMetrics = append(tableMetrics, subMetrics.TableMetrics...)
				}
			}
		}
	}

	// Handle materialized subqueries within the table
	if materializedSubquery, exists := tableInfo["materialized_from_subquery"].(map[string]interface{}); exists {
		if subQueryBlock, exists := materializedSubquery["query_block"].(map[string]interface{}); exists {
			subMetrics := extractMetricsFromPlan(map[string]interface{}{"query_block": subQueryBlock})
			tableMetrics = append(tableMetrics, subMetrics.TableMetrics...)
		}
	}

	// Handle union results within the table
	if unionResult, exists := tableInfo["union_result"].(map[string]interface{}); exists {
		if querySpecifications, exists := unionResult["query_specifications"].([]interface{}); exists {
			for _, querySpec := range querySpecifications {
				if querySpecMap, ok := querySpec.(map[string]interface{}); ok {
					if subQueryBlock, exists := querySpecMap["query_block"].(map[string]interface{}); exists {
						subMetrics := extractMetricsFromPlan(map[string]interface{}{"query_block": subQueryBlock})
						tableMetrics = append(tableMetrics, subMetrics.TableMetrics...)
					}
				}
			}
		}
	}

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
