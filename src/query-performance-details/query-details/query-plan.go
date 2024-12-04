package query_details

import (
	"context"
	"fmt"

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

func PopulateExecutionPlan(db performance_database.DataSource, queries []performance_data_model.QueryPlanMetrics, e *integration.Entity, args arguments.ArgumentList) ([]map[string]interface{}, error) {
	var events []map[string]interface{}

	for _, query := range queries {
		tableIngestionDataList := processExecutionPlan(db, query)
		events = append(events, tableIngestionDataList...)
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

func processExecutionPlan(db performance_database.DataSource, query performance_data_model.QueryPlanMetrics) []map[string]interface{} {
	supportedStatements := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "WITH": true}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	execPlanQuery := fmt.Sprintf("EXPLAIN %s", queryText)
	rows, err := db.QueryxContext(ctx, execPlanQuery)
	if err != nil {
		log.Error("Error executing EXPLAIN for query '%s': %v", queryText, err)
		return nil
	}

	// Here we process each row of the EXPLAIN result set
	var tableIngestionDataList []map[string]interface{}
	for rows.Next() {
		var stepID, rowsExamined int64
		var selectType, table, partitions, typeField, possibleKeys, key, keyLen, ref, extra string
		var filtered float64

		err := rows.Scan(&stepID, &selectType, &table, &partitions, &typeField, &possibleKeys, &key, &keyLen, &ref, &rowsExamined, &filtered, &extra)
		if err != nil {
			log.Error("Failed to scan execution plan: %v", err)
			rows.Close()
			return nil
		}

		tableIngestionData := map[string]interface{}{
			"query_id":       query.QueryID,
			"query_text":     query.AnonymizedQueryText,
			"event_id":       query.EventID,
			"step_id":        stepID,
			"execution_step": table,
			"access_type":    typeField,
			"possible_keys":  possibleKeys,
			"ref":            ref,
			"rows_examined":  rowsExamined,
			"filtered":       filtered,
			"extra_info":     extra,
		}

		tableIngestionDataList = append(tableIngestionDataList, tableIngestionData)
	}

	rows.Close()
	return tableIngestionDataList
}

func SetExecutionPlan(e *integration.Entity, args arguments.ArgumentList, metrics []map[string]interface{}) error {
	ms1 := common_utils.CreateMetricSet(e, "MysqlQueryExecutionV11", args)
	ms1.SetMetric("query_id", "testtingweds", metric.ATTRIBUTE)
	for _, metricObject := range metrics {
		processExecutionMetrics(e, args, metricObject)
	}

	return nil
}

func processExecutionMetrics(e *integration.Entity, args arguments.ArgumentList, metricObject map[string]interface{}) {
	ms := common_utils.CreateMetricSet(e, "MysqlQueryExecutionPlan", args)
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
		"event_id":       {common_utils.GetInt64ValueSafe(metricObject["event_id"]), metric.GAUGE},
		"step_id":        {common_utils.GetInt64ValueSafe(metricObject["step_id"]), metric.GAUGE},
		"execution_step": {common_utils.GetStringValueSafe(metricObject["execution_step"]), metric.ATTRIBUTE},
		"access_type":    {common_utils.GetStringValueSafe(metricObject["access_type"]), metric.ATTRIBUTE},
		"possible_keys":  {common_utils.GetStringValueSafe(metricObject["possible_keys"]), metric.ATTRIBUTE},
		"ref":            {common_utils.GetStringValueSafe(metricObject["ref"]), metric.ATTRIBUTE},
		"rows_examined":  {common_utils.GetInt64ValueSafe(metricObject["rows_examined"]), metric.GAUGE},
		"filtered":       {common_utils.GetFloat64ValueSafe(metricObject["filtered"]), metric.GAUGE},
		"extra_info":     {common_utils.GetStringValueSafe(metricObject["extra_info"]), metric.ATTRIBUTE},
	}

	for name, metricData := range metricsMap {
		fmt.Println("name:", name)
		fmt.Println("metricData:", metricData.Value)

		var err error

		switch v := metricData.Value.(type) {
		case int, int32, int64, uint64, float32, float64:
			err = ms.SetMetric(name, v, metricData.MetricType)
		case string:
			if v == "" {
				err = fmt.Errorf("value for %s is an empty string", name)
			} else {
				err = ms.SetMetric(name, v, metricData.MetricType)
			}
		default:
			err = fmt.Errorf("unexpected type for value %s: %T", name, metricData.Value)
		}

		if err != nil {
			log.Error("Error setting value for %s: %v", name, err)
			continue
		}
	}

	// Print the metric set for debugging
	common_utils.PrintMetricSet(ms)
}
