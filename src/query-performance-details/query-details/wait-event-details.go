package query_details

import (
	"context"

	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
	query_performance_details "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
)

// PopulateWaitEventMetrics retrieves wait event metrics from the database and sets them in the integration.
func PopulateWaitEventMetrics(db performance_database.DataSource, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]performance_data_model.WaitEventQueryMetrics, error) {
	query := query_performance_details.WaitEventsQuery

	rows, err := db.QueryxContext(context.Background(), query)
	if err != nil {
		log.Error("Failed to execute query: %v", err)
		return nil, err
	}
	defer rows.Close()

	var metrics []performance_data_model.WaitEventQueryMetrics
	for rows.Next() {
		var metric performance_data_model.WaitEventQueryMetrics
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan row: %v", err)
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	if err := rows.Err(); err != nil {
		log.Error("Error iterating over query metrics rows: %v", err)
		return nil, err
	}

	// Set the retrieved metrics in the integration
	setWaitEventMetrics(i, args, metrics)
	return metrics, nil
}

// setWaitEventMetrics sets the wait event metrics in the integration.
func setWaitEventMetrics(i *integration.Integration, args arguments.ArgumentList, metrics []performance_data_model.WaitEventQueryMetrics) error {
	e, err := common_utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
	common_utils.FatalIfErr(err)
	count := 0
	for _, metricData := range metrics {
		// Create a new metric set for each row
		ms := common_utils.CreateMetricSet(e, "MysqlWaitEventsSample", args)
		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{
			"query_id":             {common_utils.GetStringValue(metricData.QueryID), metric.ATTRIBUTE},
			"query_text":           {common_utils.GetStringValue(metricData.QueryText), metric.ATTRIBUTE},
			"database_name":        {common_utils.GetStringValue(metricData.DatabaseName), metric.ATTRIBUTE},
			"wait_category":        {metricData.WaitCategory, metric.ATTRIBUTE},
			"collection_timestamp": {metricData.CollectionTimestamp, metric.ATTRIBUTE},
			"instance_id":          {metricData.InstanceID, metric.ATTRIBUTE},
			"wait_event_name":      {metricData.WaitEventName, metric.ATTRIBUTE},
			"wait_event_count":     {int(metricData.WaitEventCount), metric.GAUGE},
			"avg_wait_time_ms":     {metricData.AvgWaitTimeMs, metric.GAUGE},
			"total_wait_time_ms":   {metricData.TotalWaitTimeMs, metric.GAUGE},
		}
		for name, metric := range metricsMap {
			err := ms.SetMetric(name, metric.Value, metric.MetricType)
			if err != nil {
				log.Warn("Error setting value:  %s", err)
				continue
			}
		}

		count++
		// Publish the metrics if the count reaches the limit
		if count >= common_utils.MetricSetLimit {
			common_utils.FatalIfErr(i.Publish())

			// Create a new node entity for the next batch of metrics
			e, err = common_utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
			common_utils.FatalIfErr(err)
			count = 0
		}
	}

	// Publish any remaining metrics
	if count > 0 {
		common_utils.FatalIfErr(i.Publish())
	}
	return nil
}
