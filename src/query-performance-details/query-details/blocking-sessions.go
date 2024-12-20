package query_details

import (
	"context"

	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
	query_performance_details "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
)

// PopulateBlockingSessionMetrics retrieves blocking session metrics from the database and populates them into the integration entity.
func PopulateBlockingSessionMetrics(db performance_database.DataSource, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]performance_data_model.BlockingSessionMetrics, error) {
	query := query_performance_details.BlockingSessionsQuery
	rows, err := db.QueryxContext(context.Background(), query, args.QueryCountThreshold)
	if err != nil {
		log.Error("Failed to execute blocking session query: %v", err)
		return nil, err
	}
	defer rows.Close()

	var metrics []performance_data_model.BlockingSessionMetrics
	for rows.Next() {
		var metric performance_data_model.BlockingSessionMetrics
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan blocking session metrics: %v", err)
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	if err := rows.Err(); err != nil {
		log.Error("Error iterating over blocking session metrics rows: %v", err)
		return nil, err
	}

	// Set the blocking query metrics in the integration entity
	setBlockingQueryMetrics(metrics, i, args)
	return metrics, nil
}

// setBlockingQueryMetrics sets the blocking session metrics into the integration entity.
func setBlockingQueryMetrics(metrics []performance_data_model.BlockingSessionMetrics, i *integration.Integration, args arguments.ArgumentList) error {
	var metricList []interface{}
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	common_utils.IngestMetric(metricList, "MysqlBlockingSessionSample", i, args)

	return nil
}
