package query_details

import (
	"context"

	"fmt"

	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
	query_performance_details "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
)

func PopulateBlockingSessionMetrics(db performance_database.DataSource, i *integration.Integration, e *integration.Entity, args arguments.ArgumentList) ([]performance_data_model.BlockingSessionMetrics, error) {
	query := query_performance_details.BlockingSessionsQuery
	rows, err := db.QueryxContext(context.Background(), query)
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

	setBlockingQueryMetrics(metrics, i, args)
	return metrics, nil
}

func setBlockingQueryMetrics(metrics []performance_data_model.BlockingSessionMetrics, i *integration.Integration, args arguments.ArgumentList) error {
	if e == nil {
		return fmt.Errorf("entity is nil")
	}
	e, err := common_utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
	common_utils.FatalIfErr(err)
	count := 0
	if e == nil {
		return fmt.Errorf("entity is nil")
	}
	for _, metricData := range metrics {
		// Create a new metric set for each row
		ms := common_utils.CreateMetricSet(e, "MysqlBlockingSessionSample", args)
		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{
			"blocked_txn_id":     {common_utils.GetStringValue(metricData.BlockedTxnID), metric.ATTRIBUTE},
			"blocked_pid":        {common_utils.GetStringValue(metricData.BlockedPID), metric.ATTRIBUTE},
			"blocked_thread_id":  {common_utils.GetInt64Value(metricData.BlockedThreadID), metric.GAUGE},
			"blocked_query_id":   {common_utils.GetStringValue(metricData.BlockedQueryID), metric.ATTRIBUTE},
			"blocked_query":      {common_utils.GetStringValue(metricData.BlockedQuery), metric.ATTRIBUTE},
			"blocked_status":     {common_utils.GetStringValue(metricData.BlockedQuery), metric.ATTRIBUTE},
			"blocked_user":       {common_utils.GetStringValue(metricData.BlockedUser), metric.ATTRIBUTE},
			"blocked_host":       {common_utils.GetStringValue(metricData.BlockedHost), metric.ATTRIBUTE},
			"database_name":      {common_utils.GetStringValue(metricData.BlockedDB), metric.ATTRIBUTE},
			"blocking_txn_id":    {common_utils.GetStringValue(metricData.BlockingTxnID), metric.ATTRIBUTE},
			"blocking_pid":       {common_utils.GetStringValue(metricData.BlockingPID), metric.ATTRIBUTE},
			"blocking_thread_id": {common_utils.GetInt64Value(metricData.BlockingThreadID), metric.GAUGE},
			"blocking_user":      {common_utils.GetStringValue(metricData.BlockingUser), metric.ATTRIBUTE},
			"blocking_host":      {common_utils.GetStringValue(metricData.BlockingHost), metric.ATTRIBUTE},
			"blocking_query_id":  {common_utils.GetStringValue(metricData.BlockingQueryID), metric.ATTRIBUTE},
			"blocking_query":     {common_utils.GetStringValue(metricData.BlockingQuery), metric.ATTRIBUTE},
			"blocking_status":    {common_utils.GetStringValue(metricData.BlockingQuery), metric.ATTRIBUTE},
		}

		for metricName, data := range metricsMap {
			err := ms.SetMetric(metricName, data.Value, data.MetricType)
			if err != nil {
				log.Warn("Error setting value:  %s", err)
				continue
			}
		}

		count++
		if count >= common_utils.MetricSetLimit {
			common_utils.FatalIfErr(i.Publish())

			e, err = common_utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
			common_utils.FatalIfErr(err)
			count = 0
		}
	}

	if count > 0 {
		common_utils.FatalIfErr(i.Publish())
	}
	return nil
}
