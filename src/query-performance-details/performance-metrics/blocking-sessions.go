package performancemetrics

import (
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
func PopulateBlockingSessionMetrics(db dbconnection.DataSource, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList, excludedDatabases []string) error {
	// Prepare the SQL query with the provided parameters
	query, inputArgs, err := sqlx.In(queries.BlockingSessionsQuery, excludedDatabases, min(args.QueryCountThreshold, commonutils.MaxQueryCountThreshold))
	if err != nil {
		log.Error("Failed to prepare blocking sessions query: %v", err)
		return err
	}

	// Collect the blocking session metrics
	metrics, err := dbconnection.CollectMetrics[datamodels.BlockingSessionMetrics](db, query, inputArgs...)
	if err != nil {
		log.Error("Error collecting blocking session metrics: %v", err)
		return err
	}

	// Set the blocking query metrics in the integration entity
	setBlockingQueryMetrics(metrics, i, args)
	return nil
}

// setBlockingQueryMetrics sets the blocking session metrics into the integration entity.
func setBlockingQueryMetrics(metrics []datamodels.BlockingSessionMetrics, i *integration.Integration, args arguments.ArgumentList) {
	metricList := make([]interface{}, 0, len(metrics))
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	commonutils.IngestMetric(metricList, "MysqlBlockingSessionSample", i, args)
}
