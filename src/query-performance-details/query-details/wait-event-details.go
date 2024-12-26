package query_details

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
	queries "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
)

// PopulateWaitEventMetrics retrieves wait event metrics from the database and sets them in the integration.
func PopulateWaitEventMetrics(db performance_database.DataSource, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]performance_data_model.WaitEventQueryMetrics, error) {
	// Get the list of unique excluded databases
	excludedDatabases, err := common_utils.GetExcludedDatabases(args.ExcludedDatabases)
	if err != nil {
		log.Error("Error unmarshaling JSON: %v\n", err)
		return nil, err
	}

	// Prepare the arguments for the query
	excludedDatabasesArgs := []interface{}{excludedDatabases, excludedDatabases, excludedDatabases, args.QueryCountThreshold}

	// Prepare the SQL query with the provided parameters
	preparedQuery, preparedArgs, err := sqlx.In(queries.WaitEventsQuery, excludedDatabasesArgs...)
	if err != nil {
		log.Error("Failed to prepare wait event query: %v", err)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryxContext(ctx, preparedQuery, preparedArgs...)
	if err != nil {
		log.Error("Failed to collect wait event query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()

	var metrics []performance_data_model.WaitEventQueryMetrics
	for rows.Next() {
		var metric performance_data_model.WaitEventQueryMetrics
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan wait event query metrics row: %v", err)
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	if err := rows.Err(); err != nil {
		log.Error("Error encountered while iterating over wait event query metric rows: %v", err)
		return nil, err
	}

	// Set the retrieved metrics in the integration
	setWaitEventMetrics(i, args, metrics)
	return metrics, nil
}

// setWaitEventMetrics sets the wait event metrics in the integration.
func setWaitEventMetrics(i *integration.Integration, args arguments.ArgumentList, metrics []performance_data_model.WaitEventQueryMetrics) {
	var metricList []interface{}
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	common_utils.IngestMetric(metricList, "MysqlWaitEventsSample", i, args)
}
