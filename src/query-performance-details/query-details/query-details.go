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
	performancedatamodel "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	performancedatabase "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
	queries "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
)

func PopulateSlowQueryMetrics(e *integration.Entity, db performancedatabase.DataSource, args arguments.ArgumentList) []string {
	rawMetrics, queryIdList, err := collectPerformanceSchemaMetrics(db, args.SlowQueryInterval)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil
	}
	setSlowQueryMetrics(e, rawMetrics, args)
	return queryIdList
}

func collectPerformanceSchemaMetrics(db performancedatabase.DataSource, slowQueryInterval int) ([]performancedatamodel.QueryMetrics, []string, error) {
	query := queries.SlowQueries
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryxContext(ctx, query, slowQueryInterval)
	if err != nil {
		log.Error("Failed to collect query metrics from Performance Schema: %v", err)
		return nil, []string{}, err
	}
	defer rows.Close()

	var metrics []performancedatamodel.QueryMetrics
	var qIdList []string
	for rows.Next() {
		var metric performancedatamodel.QueryMetrics
		var qId string
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan query metrics row: %v", err)
			return nil, []string{}, err
		}
		qId = metric.QueryID
		qIdList = append(qIdList, qId)
		metrics = append(metrics, metric)
	}
	fmt.Println("Query Id List: ", qIdList)
	if err := rows.Err(); err != nil {
		log.Error("Error iterating over query metrics rows: %v", err)
		return nil, []string{}, err
	}
	return metrics, qIdList, nil
}

func setSlowQueryMetrics(e *integration.Entity, metrics []performancedatamodel.QueryMetrics, args arguments.ArgumentList) error {
	for _, metricObject := range metrics {
		ms := common_utils.CreateMetricSet(e, "MysqlSlowQueriesSampleV1", args)
		if ms == nil {
			return fmt.Errorf("failed to create metric set")
		}
		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{
			"query_id":             {metricObject.QueryID, metric.ATTRIBUTE},
			"query_text":           {common_utils.GetStringValue(metricObject.QueryText), metric.ATTRIBUTE},
			"database_name":        {common_utils.GetStringValue(metricObject.DatabaseName), metric.ATTRIBUTE},
			"schema_name":          {metricObject.SchemaName, metric.ATTRIBUTE},
			"execution_count":      {metricObject.ExecutionCount, metric.GAUGE},
			"avg_cpu_time_ms":      {metricObject.AvgCPUTimeMs, metric.GAUGE},
			"avg_elapsed_time_ms":  {metricObject.AvgElapsedTimeMs, metric.GAUGE},
			"avg_disk_reads":       {metricObject.AvgDiskReads, metric.GAUGE},
			"avg_disk_writes":      {metricObject.AvgDiskWrites, metric.GAUGE},
			"has_full_table_scan":  {metricObject.HasFullTableScan, metric.ATTRIBUTE},
			"statement_type":       {metricObject.StatementType, metric.ATTRIBUTE},
			"collection_timestamp": {metricObject.CollectionTimestamp, metric.ATTRIBUTE},
		}
		for name, metric := range metricsMap {
			err := ms.SetMetric(name, metric.Value, metric.MetricType)
			if err != nil {
				log.Warn("Error setting value:  %s", err)
				continue
			}
		}
	}
	return nil
}

func PopulateIndividualQueryDetails(db performancedatabase.DataSource, queryIdList []string, e *integration.Entity, args arguments.ArgumentList) ([]performancedatamodel.QueryPlanMetrics, error) {
	currentQueryMetrics, currentQueryMetricsErr := currentQueryMetrics(db, queryIdList, args.IndividualQueryThreshold)
	if currentQueryMetricsErr != nil {
		log.Error("Failed to collect current query metrics: %v", currentQueryMetricsErr)
		return nil, currentQueryMetricsErr
	}

	recentQueryList, recentQueryErr := recentQueryMetrics(db, queryIdList, args.IndividualQueryThreshold)
	if recentQueryErr != nil {
		log.Error("Failed to collect recent query metrics: %v", recentQueryErr)
		return nil, recentQueryErr
	}

	extensiveQueryList, extensiveQueryErr := extensiveQueryMetrics(db, queryIdList, args.IndividualQueryThreshold)
	if extensiveQueryErr != nil {
		log.Error("Failed to collect extensive query metrics: %v", extensiveQueryErr)
		return nil, extensiveQueryErr
	}

	queryList := append(append(currentQueryMetrics, recentQueryList...), extensiveQueryList...)
	filteredQueryList := getUniqueQueryList(queryList)
	fmt.Println("Filtered Query List: ", filteredQueryList)

	setIndividualQueryMetrics(e, args, filteredQueryList)
	return filteredQueryList, nil
}

func getUniqueQueryList(queryList []performancedatamodel.QueryPlanMetrics) []performancedatamodel.QueryPlanMetrics {
	uniqueEvents := make(map[uint64]bool)
	var uniqueQueryList []performancedatamodel.QueryPlanMetrics

	for _, query := range queryList {
		if _, exists := uniqueEvents[query.EventID]; !exists {
			uniqueEvents[query.EventID] = true
			uniqueQueryList = append(uniqueQueryList, query)
		}
	}

	return uniqueQueryList
}

func setIndividualQueryMetrics(e *integration.Entity, args arguments.ArgumentList, metrics []performancedatamodel.QueryPlanMetrics) error {
	for _, metricObject := range metrics {

		// Create a new metric set for each row
		ms := common_utils.CreateMetricSet(e, "MysqlIndividualQueriesV1", args)

		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{

			"query_id":      {metricObject.QueryID, metric.ATTRIBUTE},
			"query_text":    {metricObject.AnonymizedQueryText, metric.ATTRIBUTE},
			"event_id":      {metricObject.EventID, metric.GAUGE},
			"thread_id":     {metricObject.ThreadID, metric.GAUGE},
			"timer_wait":    {metricObject.TimerWait, metric.GAUGE},
			"rows_sent":     {metricObject.RowsSent, metric.GAUGE},
			"rows_examined": {metricObject.RowsExamined, metric.GAUGE},
		}

		for name, metric := range metricsMap {
			err := ms.SetMetric(name, metric.Value, metric.MetricType)
			if err != nil {
				log.Warn("Error setting value:  %s", err)
				continue
			}
		}
	}
	return nil
}

func currentQueryMetrics(db performancedatabase.DataSource, QueryIDList []string, individualQueryThreshold int) ([]performancedatamodel.QueryPlanMetrics, error) {
	// Check Performance Schema availability
	metrics, err := collectCurrentQueryMetrics(db, QueryIDList, individualQueryThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

func recentQueryMetrics(db performancedatabase.DataSource, QueryIDList []string, individualQueryThreshold int) ([]performancedatamodel.QueryPlanMetrics, error) {
	// Check Performance Schema availability
	metrics, err := collectRecentQueryMetrics(db, QueryIDList, individualQueryThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

func extensiveQueryMetrics(db performancedatabase.DataSource, QueryIDList []string, individualQueryThreshold int) ([]performancedatamodel.QueryPlanMetrics, error) {
	// Check Performance Schema availability
	metrics, err := collectExtensiveQueryMetrics(db, QueryIDList, individualQueryThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

func collectCurrentQueryMetrics(db performancedatabase.DataSource, queryIDList []string, individualQueryThreshold int) ([]performancedatamodel.QueryPlanMetrics, error) {
	if len(queryIDList) == 0 {
		log.Warn("queryIDList is empty")
		return nil, nil
	}
	// Building the placeholder string for the IN clause
	placeholders := make([]string, len(queryIDList))
	for i := range queryIDList {
		placeholders[i] = "?"
	}

	// Joining the placeholders to form the IN clause
	inClause := strings.Join(placeholders, ", ")
	query := fmt.Sprintf(queries.CurrentRunningQueriesSearch, inClause)
	args := make([]interface{}, len(queryIDList))
	for i, id := range queryIDList {
		args[i] = id
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	updatedArgs := append(args, individualQueryThreshold)
	rows, err := db.QueryxContext(ctx, query, updatedArgs...)
	if err != nil {
		log.Error("Failed to collect query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()
	var metrics []performancedatamodel.QueryPlanMetrics
	for rows.Next() {
		var metric performancedatamodel.QueryPlanMetrics
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan query metrics row: %v", err)
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	if err := rows.Err(); err != nil {
		log.Error("Error iterating over query metrics rows: %v", err)
		return nil, err
	}

	return metrics, nil
}

func collectRecentQueryMetrics(db performancedatabase.DataSource, queryIDList []string, individualQueryThreshold int) ([]performancedatamodel.QueryPlanMetrics, error) {
	if len(queryIDList) == 0 {
		log.Warn("queryIDList is empty")
		return nil, nil
	}
	placeholders := make([]string, len(queryIDList))
	for i := range queryIDList {
		placeholders[i] = "?"
	}
	inClause := strings.Join(placeholders, ", ")
	query := fmt.Sprintf(queries.RecentQueriesSearch, inClause)
	args := make([]interface{}, len(queryIDList))
	for i, id := range queryIDList {
		args[i] = id
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	updatedArgs := append(args, individualQueryThreshold)
	rows, err := db.QueryxContext(ctx, query, updatedArgs...)
	if err != nil {
		log.Error("Failed to collect query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()
	var metrics []performancedatamodel.QueryPlanMetrics
	for rows.Next() {
		var metric performancedatamodel.QueryPlanMetrics
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan query metrics row: %v", err)
			return nil, err
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		log.Error("Error iterating over query metrics rows: %v", err)
		return nil, err
	}

	return metrics, nil
}

func collectExtensiveQueryMetrics(db performancedatabase.DataSource, queryIDList []string, individualQueryThreshold int) ([]performancedatamodel.QueryPlanMetrics, error) {
	if len(queryIDList) == 0 {
		log.Warn("queryIDList is empty")
		return nil, nil
	}
	// Building the placeholder string for the IN clause
	placeholders := make([]string, len(queryIDList))
	for i := range queryIDList {
		placeholders[i] = "?"
	}

	// Joining the placeholders to form the IN clause
	inClause := strings.Join(placeholders, ", ")

	query := fmt.Sprintf(queries.PastQueriesSearch, inClause)
	args := make([]interface{}, len(queryIDList))
	for i, id := range queryIDList {
		args[i] = id
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	updatedArgs := append(args, individualQueryThreshold)
	rows, err := db.QueryxContext(ctx, query, updatedArgs...)
	if err != nil {
		log.Error("Failed to collect query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()
	var metrics []performancedatamodel.QueryPlanMetrics
	for rows.Next() {
		var metric performancedatamodel.QueryPlanMetrics
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan query metrics row: %v", err)
			return nil, err
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		log.Error("Error iterating over query metrics rows: %v", err)
		return nil, err
	}

	return metrics, nil
}
