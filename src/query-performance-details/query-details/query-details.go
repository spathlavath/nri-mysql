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

// PopulateSlowQueryMetrics collects and sets slow query metrics
func PopulateSlowQueryMetrics(i *integration.Integration, e *integration.Entity, db performancedatabase.DataSource, args arguments.ArgumentList) []string {
	rawMetrics, queryIdList, err := collectGroupedSlowQueryMetrics(db, args.FetchInterval, args.QueryCountThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil
	}
	setSlowQueryMetrics(i, rawMetrics, args)
	return queryIdList
}

// collectGroupedSlowQueryMetrics collects metrics from the performance schema database
func collectGroupedSlowQueryMetrics(db performancedatabase.DataSource, fetchInterval int, queryCountThreshold int) ([]performancedatamodel.SlowQueryMetrics, []string, error) {
	query := queries.SlowQueries
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryxContext(ctx, query, fetchInterval, queryCountThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics from Performance Schema: %v", err)
		return nil, []string{}, err
	}
	defer rows.Close()

	var metrics []performancedatamodel.SlowQueryMetrics
	var qIdList []string
	for rows.Next() {
		var metric performancedatamodel.SlowQueryMetrics
		var qId string
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan query metrics row: %v", err)
			return nil, []string{}, err
		}
		qId = metric.QueryID
		qIdList = append(qIdList, qId)
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		log.Error("Error iterating over query metrics rows: %v", err)
		return nil, []string{}, err
	}
	return metrics, qIdList, nil
}

// setSlowQueryMetrics sets the collected slow query metrics to the integration
func setSlowQueryMetrics(i *integration.Integration, metrics []performancedatamodel.SlowQueryMetrics, args arguments.ArgumentList) error {
	e, err := common_utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
	common_utils.FatalIfErr(err)
	count := 0
	for _, metricObject := range metrics {
		ms := common_utils.CreateMetricSet(e, "MysqlSlowQueriesSample", args)
		if ms == nil {
			return fmt.Errorf("failed to create metric set")
		}
		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{
			"query_id":                 {metricObject.QueryID, metric.ATTRIBUTE},
			"query_text":               {common_utils.GetStringValue(metricObject.QueryText), metric.ATTRIBUTE},
			"database_name":            {common_utils.GetStringValue(metricObject.DatabaseName), metric.ATTRIBUTE},
			"schema_name":              {metricObject.SchemaName, metric.ATTRIBUTE},
			"execution_count":          {metricObject.ExecutionCount, metric.GAUGE},
			"avg_cpu_time_ms":          {metricObject.AvgCPUTimeMs, metric.GAUGE},
			"avg_elapsed_time_ms":      {metricObject.AvgElapsedTimeMs, metric.GAUGE},
			"avg_disk_reads":           {metricObject.AvgDiskReads, metric.GAUGE},
			"avg_disk_writes":          {metricObject.AvgDiskWrites, metric.GAUGE},
			"has_full_table_scan":      {metricObject.HasFullTableScan, metric.ATTRIBUTE},
			"statement_type":           {metricObject.StatementType, metric.ATTRIBUTE},
			"last_execution_timestamp": {metricObject.LastExecutionTimestamp, metric.ATTRIBUTE},
			"collection_timestamp":     {metricObject.CollectionTimestamp, metric.ATTRIBUTE},
		}
		for name, metric := range metricsMap {
			err := ms.SetMetric(name, metric.Value, metric.MetricType)
			if err != nil {
				log.Warn("Error setting value:  %s", err)
				continue
			}
		}

		count++
		// Publish the metrics if the count reaches the limit
		if count >= common_utils.MetricSetLimit {
			common_utils.FatalIfErr(i.Publish())

			// Create a new node entity for the next batch of metrics
			e, err = common_utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
			common_utils.FatalIfErr(err)
			count = 0
		}
	}

	// Publish any remaining metrics
	if count > 0 {
		common_utils.FatalIfErr(i.Publish())
	}
	return nil
}

// PopulateIndividualQueryDetails collects and sets individual query details
func PopulateIndividualQueryDetails(db performancedatabase.DataSource, queryIdList []string, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]performancedatamodel.QueryGroup, error) {
	currentQueryMetrics, currentQueryMetricsErr := currentQueryMetrics(db, queryIdList, args.QueryResponseTimeThreshold, args.QueryCountThreshold)
	if currentQueryMetricsErr != nil {
		log.Error("Failed to collect current query metrics: %v", currentQueryMetricsErr)
		return nil, currentQueryMetricsErr
	}

	recentQueryList, recentQueryErr := recentQueryMetrics(db, queryIdList, args.QueryResponseTimeThreshold, args.QueryCountThreshold)
	if recentQueryErr != nil {
		log.Error("Failed to collect recent query metrics: %v", recentQueryErr)
		return nil, recentQueryErr
	}

	extensiveQueryList, extensiveQueryErr := extensiveQueryMetrics(db, queryIdList, args.QueryResponseTimeThreshold, args.QueryCountThreshold)
	if extensiveQueryErr != nil {
		log.Error("Failed to collect extensive query metrics: %v", extensiveQueryErr)
		return nil, extensiveQueryErr
	}

	queryList := append(append(currentQueryMetrics, recentQueryList...), extensiveQueryList...)
	filteredQueryList := getUniqueQueryList(queryList)
	setIndividualQueryMetrics(i, args, filteredQueryList)
	groupQueriesByDatabase := groupQueriesByDatabase(filteredQueryList)
	return groupQueriesByDatabase, nil
}

// getUniqueQueryList filters out duplicate queries from the list
func getUniqueQueryList(queryList []performancedatamodel.IndividualQueryMetrics) []performancedatamodel.IndividualQueryMetrics {
	uniqueEvents := make(map[uint64]bool)
	var uniqueQueryList []performancedatamodel.IndividualQueryMetrics

	for _, query := range queryList {
		if _, exists := uniqueEvents[query.EventID]; !exists {
			uniqueEvents[query.EventID] = true
			uniqueQueryList = append(uniqueQueryList, query)
		}
	}

	return uniqueQueryList
}

// groupQueriesByDatabase groups queries by their database name
func groupQueriesByDatabase(filteredList []performancedatamodel.IndividualQueryMetrics) []performancedatamodel.QueryGroup {
	groupMap := make(map[string][]performancedatamodel.IndividualQueryMetrics)

	for _, query := range filteredList {
		groupMap[query.DatabaseName] = append(groupMap[query.DatabaseName], query)
	}

	var groupedQueries []performancedatamodel.QueryGroup
	for dbName, queries := range groupMap {
		groupedQueries = append(groupedQueries, performancedatamodel.QueryGroup{
			Database: dbName,
			Queries:  queries,
		})
	}

	return groupedQueries
}

// setIndividualQueryMetrics sets the collected individual query metrics to the integration
func setIndividualQueryMetrics(i *integration.Integration, args arguments.ArgumentList, metrics []performancedatamodel.IndividualQueryMetrics) error {
	e, err := common_utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
	common_utils.FatalIfErr(err)
	count := 0
	for _, metricObject := range metrics {

		// Create a new metric set for each row
		ms := common_utils.CreateMetricSet(e, "MysqlIndividualQueriesSample", args)

		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{

			"query_id":          {metricObject.QueryID, metric.ATTRIBUTE},
			"query_text":        {metricObject.AnonymizedQueryText, metric.ATTRIBUTE},
			"event_id":          {metricObject.EventID, metric.GAUGE},
			"thread_id":         {metricObject.ThreadID, metric.GAUGE},
			"execution_time_ms": {metricObject.ExecutionTimeMs, metric.GAUGE},
			"rows_sent":         {metricObject.RowsSent, metric.GAUGE},
			"rows_examined":     {metricObject.RowsExamined, metric.GAUGE},
			"database_name":     {metricObject.DatabaseName, metric.ATTRIBUTE},
		}

		for name, metric := range metricsMap {
			err := ms.SetMetric(name, metric.Value, metric.MetricType)
			if err != nil {
				log.Warn("Error setting value:  %s", err)
				continue
			}
		}

		count++
		// Publish the metrics if the count reaches the limit
		if count >= common_utils.MetricSetLimit {
			common_utils.FatalIfErr(i.Publish())

			// Create a new node entity for the next batch of metrics
			e, err = common_utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
			common_utils.FatalIfErr(err)
			count = 0
		}
	}

	// Publish any remaining metrics
	if count > 0 {
		common_utils.FatalIfErr(i.Publish())
	}
	return nil
}

// currentQueryMetrics collects current query metrics from the performance schema database for the given query IDs
func currentQueryMetrics(db performancedatabase.DataSource, QueryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int) ([]performancedatamodel.IndividualQueryMetrics, error) {
	metrics, err := collectCurrentQueryMetrics(db, QueryIDList, queryResponseTimeThreshold, queryCountThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// recentQueryMetrics collects recent query metrics	from the performance schema	database for the given query IDs
func recentQueryMetrics(db performancedatabase.DataSource, QueryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int) ([]performancedatamodel.IndividualQueryMetrics, error) {
	metrics, err := collectRecentQueryMetrics(db, QueryIDList, queryResponseTimeThreshold, queryCountThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// extensiveQueryMetrics collects extensive query metrics from the performance schema database for the given query IDs
func extensiveQueryMetrics(db performancedatabase.DataSource, QueryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int) ([]performancedatamodel.IndividualQueryMetrics, error) {
	metrics, err := collectExtensiveQueryMetrics(db, QueryIDList, queryResponseTimeThreshold, queryCountThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// collectCurrentQueryMetrics collects current query metrics from the performance schema database for the given query IDs
func collectCurrentQueryMetrics(db performancedatabase.DataSource, queryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int) ([]performancedatamodel.IndividualQueryMetrics, error) {
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
	updatedArgs := append(args, queryResponseTimeThreshold, queryCountThreshold)
	rows, err := db.QueryxContext(ctx, query, updatedArgs...)
	if err != nil {
		log.Error("Failed to collect query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()
	var metrics []performancedatamodel.IndividualQueryMetrics
	for rows.Next() {
		var metric performancedatamodel.IndividualQueryMetrics
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

// collectRecentQueryMetrics collects recent query metrics from the performance schema database for the given query IDs
func collectRecentQueryMetrics(db performancedatabase.DataSource, queryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int) ([]performancedatamodel.IndividualQueryMetrics, error) {
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
	updatedArgs := append(args, queryResponseTimeThreshold, queryCountThreshold)
	rows, err := db.QueryxContext(ctx, query, updatedArgs...)
	if err != nil {
		log.Error("Failed to collect query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()
	var metrics []performancedatamodel.IndividualQueryMetrics
	for rows.Next() {
		var metric performancedatamodel.IndividualQueryMetrics
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

// collectExtensiveQueryMetrics collects extensive query metrics from the performance schema database for the given query IDs
func collectExtensiveQueryMetrics(db performancedatabase.DataSource, queryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int) ([]performancedatamodel.IndividualQueryMetrics, error) {
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
	updatedArgs := append(args, queryResponseTimeThreshold, queryCountThreshold)
	rows, err := db.QueryxContext(ctx, query, updatedArgs...)
	if err != nil {
		log.Error("Failed to collect query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()
	var metrics []performancedatamodel.IndividualQueryMetrics
	for rows.Next() {
		var metric performancedatamodel.IndividualQueryMetrics
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
