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

// PopulateWaitEventMetrics retrieves wait event metrics from the database and sets them in the integration.
func PopulateWaitEventMetrics(db dbconnection.DataSource, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]datamodels.WaitEventQueryMetrics, error) {
	// Get the list of unique excluded databases
	excludedDatabases, err := commonutils.GetExcludedDatabases(args.ExcludedDatabases)
	if err != nil {
		log.Error("Error unmarshaling JSON: %v", err)
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

	ctx, cancel := context.WithTimeout(context.Background(), commonutils.TimeoutDuration)
	defer cancel()
	rows, err := db.QueryxContext(ctx, preparedQuery, preparedArgs...)
	if err != nil {
		log.Error("Failed to collect wait event query metrics from Performance Schema: %v", err)
		return nil, err
	}
	defer rows.Close()

	var metrics []datamodels.WaitEventQueryMetrics
	for rows.Next() {
		var metric datamodels.WaitEventQueryMetrics
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
func setWaitEventMetrics(i *integration.Integration, args arguments.ArgumentList, metrics []datamodels.WaitEventQueryMetrics) {
	var metricList []interface{}
	for _, metricData := range metrics {
		metricList = append(metricList, metricData)
	}

	commonutils.IngestMetric(metricList, "MysqlWaitEventsSample", i, args)
}
