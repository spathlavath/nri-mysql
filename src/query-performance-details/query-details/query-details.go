package query_details

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
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
	rawMetrics, queryIDList, err := collectGroupedSlowQueryMetrics(db, args.SlowQueryFetchInterval, args.QueryCountThreshold, args.ExcludedDatabases)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil
	}
	setSlowQueryMetrics(i, rawMetrics, args)
	return queryIDList
}

// collectGroupedSlowQueryMetrics collects metrics from the performance schema database for slow queries
func collectGroupedSlowQueryMetrics(db performancedatabase.DataSource, slowQueryfetchInterval int, queryCountThreshold int, excludedDatabasesList string) ([]performancedatamodel.SlowQueryMetrics, []string, error) {
	// Get the list of unique excluded databases
	excludedDatabases, err := common_utils.GetExcludedDatabases(excludedDatabasesList)
	if err != nil {
		log.Error("Error unmarshaling JSON: %v\n", err)
		return nil, []string{}, err
	}

	// Prepare the SQL query with the provided parameters
	query, args, err := sqlx.In(queries.SlowQueries, slowQueryfetchInterval, excludedDatabases, queryCountThreshold)
	if err != nil {
		log.Error("Failed to prepare slow query: %v", err)
		return nil, []string{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), common_utils.TimeoutDuration)
	defer cancel()
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error("Failed to collect slow query metrics from Performance Schema: %v", err)
		return nil, []string{}, err
	}
	defer rows.Close()

	var metrics []performancedatamodel.SlowQueryMetrics
	var qIdList []string
	for rows.Next() {
		var metric performancedatamodel.SlowQueryMetrics
		var qId string
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan slow query metrics row: %v", err)
			return nil, []string{}, err
		}
		qId = *metric.QueryID
		qIdList = append(qIdList, qId)
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		log.Error("Error encountered while iterating over slow query metric rows: %v", err)
		return nil, []string{}, err
	}

	return metrics, qIdList, nil
}

// setSlowQueryMetrics sets the collected slow query metrics to the integration
func setSlowQueryMetrics(i *integration.Integration, metrics []performancedatamodel.SlowQueryMetrics, args arguments.ArgumentList) {
	var metricList []interface{}
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	common_utils.IngestMetric(metricList, "MysqlSlowQueriesSample", i, args)
}

// PopulateIndividualQueryDetails collects and sets individual query details
func PopulateIndividualQueryDetails(db performancedatabase.DataSource, queryIDList []string, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]performancedatamodel.QueryGroup, error) {
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
	var metricList []interface{}
	newMetricsList := make([]performancedatamodel.IndividualQueryMetrics, len(filteredQueryList))
	copy(newMetricsList, filteredQueryList)
	for i := range newMetricsList {
		newMetricsList[i].QueryText = nil
		metricList = append(metricList, newMetricsList[i])
	}

	common_utils.IngestMetric(metricList, "MysqlIndividualQueriesSample", i, args)
	groupQueriesByDatabase := groupQueriesByDatabase(filteredQueryList)

	return groupQueriesByDatabase, nil
}

// getUniqueQueryList filters out duplicate queries from the list
func getUniqueQueryList(queryList []performancedatamodel.IndividualQueryMetrics) []performancedatamodel.IndividualQueryMetrics {
	uniqueEvents := make(map[uint64]bool)
	var uniqueQueryList []performancedatamodel.IndividualQueryMetrics

	for _, query := range queryList {
		if _, exists := uniqueEvents[*query.EventID]; !exists {
			uniqueEvents[*query.EventID] = true
			uniqueQueryList = append(uniqueQueryList, query)
		}
	}
	return uniqueQueryList
}

// groupQueriesByDatabase groups queries by their database name
func groupQueriesByDatabase(filteredList []performancedatamodel.IndividualQueryMetrics) []performancedatamodel.QueryGroup {
	groupMap := make(map[string][]performancedatamodel.IndividualQueryMetrics)

	for _, query := range filteredList {
		groupMap[*query.DatabaseName] = append(groupMap[*query.DatabaseName], query)
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

// currentQueryMetrics collects current query metrics from the performance schema database for the given query IDs
func currentQueryMetrics(db performancedatabase.DataSource, queryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int, excludedDatabasesList string) ([]performancedatamodel.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, queryIDList, queries.CurrentRunningQueriesSearch, queryResponseTimeThreshold, queryCountThreshold, excludedDatabasesList)
	if err != nil {
		log.Error("Failed to collect current query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// recentQueryMetrics collects recent query metrics	from the performance schema	database for the given query IDs
func recentQueryMetrics(db performancedatabase.DataSource, queryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int, excludedDatabasesList string) ([]performancedatamodel.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, queryIDList, queries.RecentQueriesSearch, queryResponseTimeThreshold, queryCountThreshold, excludedDatabasesList)
	if err != nil {
		log.Error("Failed to collect recent query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// extensiveQueryMetrics collects extensive query metrics from the performance schema database for the given query IDs
func extensiveQueryMetrics(db performancedatabase.DataSource, queryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int, excludedDatabasesList string) ([]performancedatamodel.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, queryIDList, queries.PastQueriesSearch, queryResponseTimeThreshold, queryCountThreshold, excludedDatabasesList)
	if err != nil {
		log.Error("Failed to collect history query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// collectIndividualQueryMetrics collects current query metrics from the performance schema database for the given query IDs
func collectIndividualQueryMetrics(db performancedatabase.DataSource, queryIDList []string, queryString string, queryResponseTimeThreshold int, queryCountThreshold int, excludedDatabasesList string) ([]performancedatamodel.IndividualQueryMetrics, error) {
	// Early exit if queryIDList is empty
	if len(queryIDList) == 0 {
		log.Warn("queryIDList is empty")
		return nil, nil
	}

	// Get the list of unique excluded databases
	excludedDatabases, err := common_utils.GetExcludedDatabases(excludedDatabasesList)
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
		common_utils.ConvertToInterfaceSlice(queryIDList),
		common_utils.ConvertToInterfaceSlice(excludedDatabases),
	)
	args = append(args, queryResponseTimeThreshold, queryCountThreshold)

	// Use sqlx.In to safely include the slices in the query
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		log.Error("Failed to prepare individual query: %v", err)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), common_utils.TimeoutDuration)
	defer cancel()

	// Execute the query
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error("Failed to collect individual query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()

	// Process query results
	var metrics []performancedatamodel.IndividualQueryMetrics
	for rows.Next() {
		var metric performancedatamodel.IndividualQueryMetrics
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
