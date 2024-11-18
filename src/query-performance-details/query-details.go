package query_performance_details

import (
	"database/sql"
	"fmt"

	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
)

type QueryMetrics struct {
	DBQueryID           string         `json:"db_query_id" db:"db_query_id"`
	QueryID             string         `json:"query_id" db:"query_id"`
	QueryText           string         `json:"query_text" db:"query_text"`
	DatabaseName        sql.NullString `json:"database_name" db:"database_name"`
	SchemaName          string         `json:"schema_name" db:"schema_name"`
	ExecutionCount      uint64         `json:"execution_count" db:"execution_count"`
	AvgCPUTimeMs        float64        `json:"avg_cpu_time_ms" db:"avg_cpu_time_ms"`
	AvgElapsedTimeMs    float64        `json:"avg_elapsed_time_ms" db:"avg_elapsed_time_ms"`
	AvgDiskReads        float64        `json:"avg_disk_reads" db:"avg_disk_reads"`
	AvgDiskWrites       float64        `json:"avg_disk_writes" db:"avg_disk_writes"`
	HasFullTableScan    string         `json:"has_full_table_scan" db:"has_full_table_scan"`
	StatementType       string         `json:"statement_type" db:"statement_type"`
	CollectionTimestamp string         `json:"collection_timestamp" db:"collection_timestamp"`
}

func collectQueryMetrics(db dataSource) ([]QueryMetrics, error) {
	return collectPerformanceSchemaMetrics(db)
}

func populateMetrics(ms *metric.Set, metrics []QueryMetrics) error {
	for _, metricObject := range metrics {
		if ms == nil {
			return fmt.Errorf("failed to create metric set")
		}

		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{
			"db_query_id":          {metricObject.DBQueryID, metric.ATTRIBUTE},
			"query_id":             {metricObject.QueryID, metric.ATTRIBUTE},
			"query_text":           {metricObject.QueryText, metric.ATTRIBUTE},
			"database_name":        {getStringValue(metricObject.DatabaseName), metric.ATTRIBUTE},
			"schema_name":          {metricObject.SchemaName, metric.ATTRIBUTE},
			"execution_count":      {metricObject.ExecutionCount, metric.GAUGE},
			"avg_cpu_time_ms":      {metricObject.AvgCPUTimeMs, metric.GAUGE},
			"avg_elapsed_time_ms":  {metricObject.AvgElapsedTimeMs, metric.GAUGE},
			"avg_disk_reads":       {metricObject.AvgDiskReads, metric.GAUGE},
			"avg_disk_writes":      {metricObject.AvgDiskWrites, metric.GAUGE},
			"has_full_table_scan":  {metricObject.HasFullTableScan, metric.ATTRIBUTE},
			"statement_type":       {metricObject.StatementType, metric.ATTRIBUTE},
			"collection_timestamp": {metricObject.CollectionTimestamp, metric.ATTRIBUTE},
		}

		for name, metric := range metricsMap {
			err := ms.SetMetric(name, metric.Value, metric.MetricType)
			if err != nil {
				log.Warn("Error setting value:  %s", err)
				continue
			}
		}
	}
	return nil
}

func getStringValue(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}
