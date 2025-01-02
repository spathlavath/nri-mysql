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

// PopulateWaitEventMetrics retrieves wait event metrics from the database and sets them in the integration.
func PopulateWaitEventMetrics(db dbconnection.DataSource, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList, excludedDatabases []string) error {
	// Prepare the arguments for the query
	excludedDatabasesArgs := []interface{}{excludedDatabases, excludedDatabases, excludedDatabases, args.QueryCountThreshold}

	// Prepare the SQL query with the provided parameters
	preparedQuery, preparedArgs, err := sqlx.In(queries.WaitEventsQuery, excludedDatabasesArgs...)
	if err != nil {
		log.Error("Failed to prepare wait event query: %v", err)
		return err
	}

	// Collect the wait event metrics
	metrics, err := dbconnection.CollectMetrics[datamodels.WaitEventQueryMetrics](db, preparedQuery, preparedArgs...)
	if err != nil {
		log.Error("Error collecting wait event metrics: %v", err)
		return err
	}

	// Set the retrieved metrics in the integration
	setWaitEventMetrics(i, args, metrics)
	return nil
}

// setWaitEventMetrics sets the wait event metrics in the integration.
func setWaitEventMetrics(i *integration.Integration, args arguments.ArgumentList, metrics []datamodels.WaitEventQueryMetrics) {
	metricList := make([]interface{}, 0, len(metrics))
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	commonutils.IngestMetric(metricList, "MysqlWaitEventsSample", i, args)
}
