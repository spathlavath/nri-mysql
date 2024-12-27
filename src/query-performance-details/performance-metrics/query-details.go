package performancemetrics

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	commonutils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	dbconnection "github.com/newrelic/nri-mysql/src/query-performance-details/connection"
	datamodels "github.com/newrelic/nri-mysql/src/query-performance-details/data-models"
	queries "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
)

// PopulateSlowQueryMetrics collects and sets slow query metrics
func PopulateSlowQueryMetrics(i *integration.Integration, e *integration.Entity, db dbconnection.DataSource, args arguments.ArgumentList) []string {
	rawMetrics, queryIDList, err := collectGroupedSlowQueryMetrics(db, args.SlowQueryFetchInterval, args.QueryCountThreshold, args.ExcludedDatabases)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil
	}
	setSlowQueryMetrics(i, rawMetrics, args)
	return queryIDList
}

// collectGroupedSlowQueryMetrics collects metrics from the performance schema database for slow queries
func collectGroupedSlowQueryMetrics(db dbconnection.DataSource, slowQueryfetchInterval int, queryCountThreshold int, excludedDatabasesList string) ([]datamodels.SlowQueryMetrics, []string, error) {
	// Get the list of unique excluded databases
	excludedDatabases, err := commonutils.GetExcludedDatabases(excludedDatabasesList)
	if err != nil {
		log.Error("Error unmarshaling JSON: %v", err)
		return nil, []string{}, err
	}

	// Prepare the SQL query with the provided parameters
	query, args, err := sqlx.In(queries.SlowQueries, slowQueryfetchInterval, excludedDatabases, queryCountThreshold)
	if err != nil {
		log.Error("Failed to prepare slow query: %v", err)
		return nil, []string{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), commonutils.TimeoutDuration)
	defer cancel()
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error("Failed to collect slow query metrics from Performance Schema: %v", err)
		return nil, []string{}, err
	}
	defer rows.Close()

	var metrics []datamodels.SlowQueryMetrics
	var qIDList []string
	for rows.Next() {
		var metric datamodels.SlowQueryMetrics
		var qID string
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan slow query metrics row: %v", err)
			return nil, []string{}, err
		}
		qID = *metric.QueryID
		qIDList = append(qIDList, qID)
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		log.Error("Error encountered while iterating over slow query metric rows: %v", err)
		return nil, []string{}, err
	}

	return metrics, qIDList, nil
}

// setSlowQueryMetrics sets the collected slow query metrics to the integration
func setSlowQueryMetrics(i *integration.Integration, metrics []datamodels.SlowQueryMetrics, args arguments.ArgumentList) {
	metricList := make([]interface{}, 0, len(metrics))
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	commonutils.IngestMetric(metricList, "MysqlSlowQueriesSample", i, args)
}

// PopulateIndividualQueryDetails collects and sets individual query details
func PopulateIndividualQueryDetails(db dbconnection.DataSource, queryIDList []string, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]datamodels.QueryGroup, error) {
	currentQueryMetrics, currentQueryMetricsErr := currentQueryMetrics(db, queryIDList, args.QueryResponseTimeThreshold, args.QueryCountThreshold, args.ExcludedDatabases)
	if currentQueryMetricsErr != nil {
		log.Error("Failed to collect current query metrics: %v", currentQueryMetricsErr)
		return nil, currentQueryMetricsErr
	}

	recentQueryList, recentQueryErr := recentQueryMetrics(db, queryIDList, args.QueryResponseTimeThreshold, args.QueryCountThreshold, args.ExcludedDatabases)
	if recentQueryErr != nil {
		log.Error("Failed to collect recent query metrics: %v", recentQueryErr)
		return nil, recentQueryErr
	}

	extensiveQueryList, extensiveQueryErr := extensiveQueryMetrics(db, queryIDList, args.QueryResponseTimeThreshold, args.QueryCountThreshold, args.ExcludedDatabases)
	if extensiveQueryErr != nil {
		log.Error("Failed to collect history query metrics: %v", extensiveQueryErr)
		return nil, extensiveQueryErr
	}

	queryList := append(append(currentQueryMetrics, recentQueryList...), extensiveQueryList...)
	filteredQueryList := getUniqueQueryList(queryList)
	newMetricsList := make([]datamodels.IndividualQueryMetrics, len(filteredQueryList))
	copy(newMetricsList, filteredQueryList)
	metricList := make([]interface{}, 0, len(newMetricsList))
	for i := range newMetricsList {
		newMetricsList[i].QueryText = nil
		metricList = append(metricList, newMetricsList[i])
	}

	commonutils.IngestMetric(metricList, "MysqlIndividualQueriesSample", i, args)
	groupQueriesByDatabase := groupQueriesByDatabase(filteredQueryList)

	return groupQueriesByDatabase, nil
}

// getUniqueQueryList filters out duplicate queries from the list
func getUniqueQueryList(queryList []datamodels.IndividualQueryMetrics) []datamodels.IndividualQueryMetrics {
	uniqueEvents := make(map[uint64]bool)
	var uniqueQueryList []datamodels.IndividualQueryMetrics

	for _, query := range queryList {
		if _, exists := uniqueEvents[*query.EventID]; !exists {
			uniqueEvents[*query.EventID] = true
			uniqueQueryList = append(uniqueQueryList, query)
		}
	}
	return uniqueQueryList
}

// groupQueriesByDatabase groups queries by their database name
func groupQueriesByDatabase(filteredList []datamodels.IndividualQueryMetrics) []datamodels.QueryGroup {
	groupMap := make(map[string][]datamodels.IndividualQueryMetrics)

	for _, query := range filteredList {
		groupMap[*query.DatabaseName] = append(groupMap[*query.DatabaseName], query)
	}

	// Pre-allocate the slice with the length of the groupMap
	groupedQueries := make([]datamodels.QueryGroup, 0, len(groupMap))
	for dbName, queries := range groupMap {
		groupedQueries = append(groupedQueries, datamodels.QueryGroup{
			Database: dbName,
			Queries:  queries,
		})
	}

	return groupedQueries
}

// currentQueryMetrics collects current query metrics from the performance schema database for the given query IDs
func currentQueryMetrics(db dbconnection.DataSource, queryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int, excludedDatabasesList string) ([]datamodels.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, queryIDList, queries.CurrentRunningQueriesSearch, queryResponseTimeThreshold, queryCountThreshold, excludedDatabasesList)
	if err != nil {
		log.Error("Failed to collect current query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// recentQueryMetrics collects recent query metrics	from the performance schema	database for the given query IDs
func recentQueryMetrics(db dbconnection.DataSource, queryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int, excludedDatabasesList string) ([]datamodels.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, queryIDList, queries.RecentQueriesSearch, queryResponseTimeThreshold, queryCountThreshold, excludedDatabasesList)
	if err != nil {
		log.Error("Failed to collect recent query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// extensiveQueryMetrics collects extensive query metrics from the performance schema database for the given query IDs
func extensiveQueryMetrics(db dbconnection.DataSource, queryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int, excludedDatabasesList string) ([]datamodels.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, queryIDList, queries.PastQueriesSearch, queryResponseTimeThreshold, queryCountThreshold, excludedDatabasesList)
	if err != nil {
		log.Error("Failed to collect history query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// collectIndividualQueryMetrics collects current query metrics from the performance schema database for the given query IDs
func collectIndividualQueryMetrics(db dbconnection.DataSource, queryIDList []string, queryString string, queryResponseTimeThreshold int, queryCountThreshold int, excludedDatabasesList string) ([]datamodels.IndividualQueryMetrics, error) {
	// Early exit if queryIDList is empty
	if len(queryIDList) == 0 {
		log.Warn("queryIDList is empty")
		return nil, nil
	}

	// Get the list of unique excluded databases
	excludedDatabases, err := commonutils.GetExcludedDatabases(excludedDatabasesList)
	if err != nil {
		log.Error("Error unmarshaling JSON: %v", err)
		return nil, err
	}

	// Build the placeholder string for the IN clause
	placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"

	// Form the query with the IN clause
	query := fmt.Sprintf(queryString, placeholders)

	// Combine queryIDList and excludedDatabases with thresholds into args
	args := append(
		commonutils.ConvertToInterfaceSlice(queryIDList),
		commonutils.ConvertToInterfaceSlice(excludedDatabases),
	)
	args = append(args, queryResponseTimeThreshold, queryCountThreshold)

	// Use sqlx.In to safely include the slices in the query
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		log.Error("Failed to prepare individual query: %v", err)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), commonutils.TimeoutDuration)
	defer cancel()

	// Execute the query
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error("Failed to collect individual query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()

	// Process query results
	var metrics []datamodels.IndividualQueryMetrics
	for rows.Next() {
		var metric datamodels.IndividualQueryMetrics
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan individual query metrics row: %v", err)
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	// Handle any errors encountered during iteration
	if err := rows.Err(); err != nil {
		log.Error("Error encountered while iterating over individual query metric rows: %v", err)
		return nil, err
	}

	return metrics, nil
}
