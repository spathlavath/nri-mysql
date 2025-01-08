package performancemetricscollectors

import (
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	utils "github.com/newrelic/nri-mysql/src/query-performance-monitoring/utils"
)

// PopulateBlockingSessionMetrics retrieves blocking session metrics from the database and populates them into the integration entity.
func PopulateBlockingSessionMetrics(db utils.DataSource, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList, excludedDatabases []string) error {
	// Prepare the SQL query with the provided parameters
	query, inputArgs, err := sqlx.In(utils.BlockingSessionsQuery, excludedDatabases, min(args.QueryCountThreshold, utils.MaxQueryCountThreshold))
	if err != nil {
		log.Error("Failed to prepare blocking sessions query: %v", err)
		return err
	}

	// Collect the blocking session metrics
	metrics, err := utils.CollectMetrics[utils.BlockingSessionMetrics](db, query, inputArgs...)
	if err != nil {
		log.Error("Error collecting blocking session metrics: %v", err)
		return err
	}

	// Set the blocking query metrics in the integration entity
	err = setBlockingQueryMetrics(metrics, i, args)
	if err != nil {
		log.Error("Error setting blocking session metrics: %v", err)
		return err
	}
	return nil
}

// setBlockingQueryMetrics sets the blocking session metrics into the integration entity.
func setBlockingQueryMetrics(metrics []utils.BlockingSessionMetrics, i *integration.Integration, args arguments.ArgumentList) error {
	metricList := make([]interface{}, 0, len(metrics))
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	err := utils.IngestMetric(metricList, "MysqlBlockingSessionSample", i, args)
	if err != nil {
		log.Error("Error ingesting blocking session metrics: %v", err)
		return err
	}
	return nil
}
