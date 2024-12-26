package query_details

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
	queries "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
)

// PopulateBlockingSessionMetrics retrieves blocking session metrics from the database and populates them into the integration entity.
func PopulateBlockingSessionMetrics(db performance_database.DataSource, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]performance_data_model.BlockingSessionMetrics, error) {
	// Get the list of unique excluded databases
	excludedDatabases, err := common_utils.GetExcludedDatabases(args.ExcludedDatabases)
	if err != nil {
		log.Error("Error unmarshaling JSON: %v\n", err)
		return nil, err
	}

	// Prepare the SQL query with the provided parameters
	query, inputArgs, err := sqlx.In(queries.BlockingSessionsQuery, excludedDatabases, args.QueryCountThreshold)
	if err != nil {
		log.Error("Failed to prepare blocking sessions query: %v", err)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), common_utils.TimeoutDuration)
	defer cancel()
	rows, err := db.QueryxContext(ctx, query, inputArgs...)
	if err != nil {
		log.Error("Failed to collect blocking session query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()

	var metrics []performance_data_model.BlockingSessionMetrics
	for rows.Next() {
		var metric performance_data_model.BlockingSessionMetrics
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan blocking session query metrics: %v", err)
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	if err := rows.Err(); err != nil {
		log.Error("Error encountered while iterating over blocking session metric rows: %v", err)
		return nil, err
	}

	// Set the blocking query metrics in the integration entity
	setBlockingQueryMetrics(metrics, i, args)
	return metrics, nil
}

// setBlockingQueryMetrics sets the blocking session metrics into the integration entity.
func setBlockingQueryMetrics(metrics []performance_data_model.BlockingSessionMetrics, i *integration.Integration, args arguments.ArgumentList) {
	var metricList []interface{}
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	common_utils.IngestMetric(metricList, "MysqlBlockingSessionSample", i, args)
}
