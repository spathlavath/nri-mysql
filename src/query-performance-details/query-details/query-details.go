package query_details

import (
	"context"
	"fmt"
	"strings"
	"time"

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
	rawMetrics, queryIdList, err := collectGroupedSlowQueryMetrics(db, args.SlowQueryFetchInterval, args.QueryCountThreshold, args.ExcludedDatabases)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil
	}
	fmt.Println("rawMetrics---->", rawMetrics)
	// setSlowQueryMetrics(i, rawMetrics, args)
	return queryIdList
}

// collectGroupedSlowQueryMetrics collects metrics from the performance schema database
func collectGroupedSlowQueryMetrics(db performancedatabase.DataSource, slowQueryfetchInterval int, queryCountThreshold int, excludedDatabasesList string) ([]performancedatamodel.SlowQueryMetrics, []string, error) {
	// query := queries.SlowQueries
	parsedDBList, err := common_utils.ParseIgnoreList(excludedDatabasesList)
	if err != nil {
		log.Error("Error parsing excludedDbList:", err)
		return nil, []string{}, err
	}
	// Get the list of unique excluded databases
	excludedDatabases := common_utils.GetUniqueExcludedDatabases(parsedDBList)
	fmt.Println("excludedDatabases---->", excludedDatabases)

	// Use sqlx.In to safely include the slice in the query
	query, args, err := sqlx.In(queries.SlowQueries, slowQueryfetchInterval, excludedDatabases, queryCountThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics from Performance Schema: %v", err)
		return nil, []string{}, err
	}
	fmt.Println("query---->", query)
	// Rebind the query for the specific database driver
	query = db.RebindX(query)
	fmt.Println("rebind---->", query)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryxContext(ctx, query, args...)
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
		qId = *metric.QueryID
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
	var metricList []interface{}
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	common_utils.IngestMetric(metricList, "MysqlSlowQueriesSample", i, args)

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
func currentQueryMetrics(db performancedatabase.DataSource, QueryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int) ([]performancedatamodel.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, QueryIDList, queries.CurrentRunningQueriesSearch, queryResponseTimeThreshold, queryCountThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// recentQueryMetrics collects recent query metrics	from the performance schema	database for the given query IDs
func recentQueryMetrics(db performancedatabase.DataSource, QueryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int) ([]performancedatamodel.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, QueryIDList, queries.RecentQueriesSearch, queryResponseTimeThreshold, queryCountThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// extensiveQueryMetrics collects extensive query metrics from the performance schema database for the given query IDs
func extensiveQueryMetrics(db performancedatabase.DataSource, QueryIDList []string, queryResponseTimeThreshold int, queryCountThreshold int) ([]performancedatamodel.IndividualQueryMetrics, error) {
	metrics, err := collectIndividualQueryMetrics(db, QueryIDList, queries.PastQueriesSearch, queryResponseTimeThreshold, queryCountThreshold)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, err
	}

	return metrics, nil
}

// collectIndividualQueryMetrics collects current query metrics from the performance schema database for the given query IDs
func collectIndividualQueryMetrics(db performancedatabase.DataSource, queryIDList []string, queryString string, queryResponseTimeThreshold int, queryCountThreshold int) ([]performancedatamodel.IndividualQueryMetrics, error) {
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
	query := fmt.Sprintf(queryString, inClause)
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
