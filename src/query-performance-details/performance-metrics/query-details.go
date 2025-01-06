package performancemetrics

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	commonutils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	dbconnection "github.com/newrelic/nri-mysql/src/query-performance-details/connection"
	datamodels "github.com/newrelic/nri-mysql/src/query-performance-details/data-models"
	queries "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
)

// PopulateSlowQueryMetrics collects and sets slow query metrics and returns the list of query IDs
func PopulateSlowQueryMetrics(i *integration.Integration, e *integration.Entity, db dbconnection.DataSource, args arguments.ArgumentList, excludedDatabases []string) []string {
	rawMetrics, queryIDList, err := collectGroupedSlowQueryMetrics(db, args.SlowQueryFetchInterval, args.QueryCountThreshold, excludedDatabases)
	if err != nil {
		log.Error("Failed to collect slow query metrics: %v", err)
		return nil
	}
	setSlowQueryMetrics(i, rawMetrics, args)
	return queryIDList
}

// collectGroupedSlowQueryMetrics collects metrics from the performance schema database for slow queries
func collectGroupedSlowQueryMetrics(db dbconnection.DataSource, slowQueryfetchInterval int, queryCountThreshold int, excludedDatabases []string) ([]datamodels.SlowQueryMetrics, []string, error) {
	// Prepare the SQL query with the provided parameters
	query, args, err := sqlx.In(queries.SlowQueries, slowQueryfetchInterval, excludedDatabases, min(queryCountThreshold, commonutils.MaxQueryCountThreshold))
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
	currentQueryMetrics, currentQueryMetricsErr := currentQueryMetrics(db, queryIDList, args.QueryResponseTimeThreshold)
	if currentQueryMetricsErr != nil {
		log.Error("Failed to collect current query metrics: %v", currentQueryMetricsErr)
		return nil, currentQueryMetricsErr
	}

	recentQueryList, recentQueryErr := recentQueryMetrics(db, queryIDList, args.QueryResponseTimeThreshold)
	if recentQueryErr != nil {
		log.Error("Failed to collect recent query metrics: %v", recentQueryErr)
		return nil, recentQueryErr
	}

	extensiveQueryList, extensiveQueryErr := extensiveQueryMetrics(db, queryIDList, args.QueryResponseTimeThreshold)
	if extensiveQueryErr != nil {
		log.Error("Failed to collect history query metrics: %v", extensiveQueryErr)
		return nil, extensiveQueryErr
	}

	queryList := append(append(currentQueryMetrics, recentQueryList...), extensiveQueryList...)
	newMetricsList := make([]datamodels.IndividualQueryMetrics, len(queryList))
	copy(newMetricsList, queryList)
	metricList := make([]interface{}, 0, len(newMetricsList))
	for i := range newMetricsList {
		newMetricsList[i].QueryText = nil
		metricList = append(metricList, newMetricsList[i])
	}

	commonutils.IngestMetric(metricList, "MysqlIndividualQueriesSample", i, args)
	groupQueriesByDatabase := groupQueriesByDatabase(queryList)

	return groupQueriesByDatabase, nil
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
func currentQueryMetrics(db dbconnection.DataSource, queryIDList []string, queryResponseTimeThreshold int) ([]datamodels.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, queryIDList, queries.CurrentRunningQueriesSearch, queryResponseTimeThreshold)
	if err != nil {
		log.Error("Failed to collect current query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// recentQueryMetrics collects recent query metrics	from the performance schema	database for the given query IDs
func recentQueryMetrics(db dbconnection.DataSource, queryIDList []string, queryResponseTimeThreshold int) ([]datamodels.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, queryIDList, queries.RecentQueriesSearch, queryResponseTimeThreshold)
	if err != nil {
		log.Error("Failed to collect recent query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// extensiveQueryMetrics collects extensive query metrics from the performance schema database for the given query IDs
func extensiveQueryMetrics(db dbconnection.DataSource, queryIDList []string, queryResponseTimeThreshold int) ([]datamodels.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, queryIDList, queries.PastQueriesSearch, queryResponseTimeThreshold)
	if err != nil {
		log.Error("Failed to collect history query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// collectIndividualQueryMetrics collects current query metrics from the performance schema database for the given query IDs
func collectIndividualQueryMetrics(db dbconnection.DataSource, queryIDList []string, queryString string, queryResponseTimeThreshold int) ([]datamodels.IndividualQueryMetrics, error) {
	// Early exit if queryIDList is empty
	if len(queryIDList) == 0 {
		log.Warn("queryIDList is empty")
		return nil, nil
	}

	var metricsList []datamodels.IndividualQueryMetrics

	for _, queryID := range queryIDList {
		// Combine queryID and thresholds into args
		args := []interface{}{queryID, queryResponseTimeThreshold, min(commonutils.IndividualQueryCountThreshold, commonutils.MaxQueryCountThreshold)}

		// Use sqlx.In to safely include the slices in the query
		query, args, err := sqlx.In(queryString, args...)
		if err != nil {
			log.Error("Failed to prepare individual query: %v", err)
			return nil, err
		}

		// Collect the individual query metrics
		metrics, err := dbconnection.CollectMetrics[datamodels.IndividualQueryMetrics](db, query, args...)
		if err != nil {
			log.Error("Error collecting wait event metrics for queryID %s: %v", queryID, err)
			return nil, err
		}

		metricsList = append(metricsList, metrics...)
	}

	return metricsList, nil
}
