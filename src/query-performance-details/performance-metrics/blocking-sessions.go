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

// PopulateBlockingSessionMetrics retrieves blocking session metrics from the database and populates them into the integration entity.
func PopulateBlockingSessionMetrics(db dbconnection.DataSource, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]datamodels.BlockingSessionMetrics, error) {
	// Get the list of unique excluded databases
	excludedDatabases, err := commonutils.GetExcludedDatabases(args.ExcludedDatabases)
	if err != nil {
		log.Error("Error unmarshaling JSON: %v", err)
		return nil, err
	}

	// Prepare the SQL query with the provided parameters
	query, inputArgs, err := sqlx.In(queries.BlockingSessionsQuery, excludedDatabases, args.QueryCountThreshold)
	if err != nil {
		log.Error("Failed to prepare blocking sessions query: %v", err)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), commonutils.TimeoutDuration)
	defer cancel()
	rows, err := db.QueryxContext(ctx, query, inputArgs...)
	if err != nil {
		log.Error("Failed to collect blocking session query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()

	var metrics []datamodels.BlockingSessionMetrics
	for rows.Next() {
		var metric datamodels.BlockingSessionMetrics
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
func setBlockingQueryMetrics(metrics []datamodels.BlockingSessionMetrics, i *integration.Integration, args arguments.ArgumentList) {
	metricList := make([]interface{}, 0, len(metrics))
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	commonutils.IngestMetric(metricList, "MysqlBlockingSessionSample", i, args)
}