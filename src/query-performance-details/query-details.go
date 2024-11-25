package query_performance_details

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
)

type QueryMetrics struct {
	QueryID             string         `json:"query_id" db:"query_id"`
	QueryText           sql.NullString `json:"query_text" db:"query_text"`
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

func collectSlowQueryMetrics(db dataSource) ([]QueryMetrics, []string, error) {
	// Check Performance Schema availability
	metrics, queryIdString, err := collectPerformanceSchemaMetrics(db)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, []string{}, err
	}

	return metrics, queryIdString, nil
}

func collectPerformanceSchemaMetrics(db dataSource) ([]QueryMetrics, []string, error) {
	query := `
        SELECT
            DIGEST AS query_id,
            DIGEST_TEXT AS query_text,
            SCHEMA_NAME AS database_name,
            'N/A' AS schema_name,
            COUNT_STAR AS execution_count,
            ROUND((SUM_CPU_TIME / COUNT_STAR) / 1000000000000, 3) AS avg_cpu_time_ms,
            ROUND((SUM_TIMER_WAIT / COUNT_STAR) / 1000000000000, 3) AS avg_elapsed_time_ms,
            SUM_ROWS_EXAMINED / COUNT_STAR AS avg_disk_reads,
            SUM_ROWS_AFFECTED / COUNT_STAR AS avg_disk_writes,
            CASE
                WHEN SUM_NO_INDEX_USED > 0 THEN 'Yes'
                ELSE 'No'
            END AS has_full_table_scan,
            CASE
                WHEN DIGEST_TEXT LIKE 'SELECT%' THEN 'SELECT'
                WHEN DIGEST_TEXT LIKE 'INSERT%' THEN 'INSERT'
                WHEN DIGEST_TEXT LIKE 'UPDATE%' THEN 'UPDATE'
                WHEN DIGEST_TEXT LIKE 'DELETE%' THEN 'DELETE'
                ELSE 'OTHER'
            END AS statement_type,
            DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%sZ') AS collection_timestamp
        FROM performance_schema.events_statements_summary_by_digest
        WHERE LAST_SEEN >= UTC_TIMESTAMP() - INTERVAL 30 SECOND
            AND SCHEMA_NAME NOT IN ('', 'mysql', 'performance_schema', 'information_schema', 'sys')
            AND QUERY_SAMPLE_TEXT NOT LIKE '%SET %'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%SHOW %'
			AND QUERY_SAMPLE_TEXT NOT LIKE '%COMMIT %'
			AND QUERY_SAMPLE_TEXT NOT LIKE '%START %'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%INFORMATION_SCHEMA%'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%PERFORMANCE_SCHEMA%'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%mysql%'
            AND QUERY_SAMPLE_TEXT NOT LIKE 'EXPLAIN %'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%PERFORMANCE_SCHEMA%'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%INFORMATION_SCHEMA%'
        ORDER BY avg_elapsed_time_ms DESC
		LIMIT 10;
    `

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryxContext(ctx, query)
	if err != nil {
		log.Error("Failed to collect query metrics from Performance Schema: %v", err)
		return nil, []string{}, err
	}
	defer rows.Close()

	var metrics []QueryMetrics
	var qIdList []string
	for rows.Next() {
		var metric QueryMetrics
		var qId string
		if err := rows.StructScan(&metric); err != nil {
			log.Error("Failed to scan query metrics row: %v", err)
			return nil, []string{}, err
		}
		qId = metric.QueryID
		qIdList = append(qIdList, qId)
		metrics = append(metrics, metric)
	}
	//qIdString := strings.Join(qIdList, ",")
	fmt.Println("Query Id List: ", qIdList)
	if err := rows.Err(); err != nil {
		log.Error("Error iterating over query metrics rows: %v", err)
		return nil, []string{}, err
	}

	return metrics, qIdList, nil
}

func collectIndividualQueryDetails(db dataSource, queryIdList []string) ([]QueryPlanMetrics, error) {
	metrics, err := currentQueryMetrics(db, queryIdList)
	if err != nil {
		log.Error("Failed to collect query metrics: %v", err)
		return nil, err
	}

	metrics1, err1 := recentQueryMetrics(db, queryIdList)
	if err1 != nil {
		log.Error("Failed to collect query metrics: %v", err1)
		return nil, err1
	}

	metrics2, err2 := extensiveQueryMetrics(db, queryIdList)
	if err2 != nil {
		log.Error("Failed to collect query metrics: %v", err2)
		return nil, err2
	}

	queryList := append(append(metrics, metrics1...), metrics2...)
	return queryList, nil
}

func populateMetrics(e *integration.Entity, args arguments.ArgumentList, metrics []QueryMetrics) error {
	for _, metricObject := range metrics {
		ms := createMetricSet(e, "MysqlSlowQueriesSample", args)

		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{
			"query_id":             {metricObject.QueryID, metric.ATTRIBUTE},
			"query_text":           {getStringValue(metricObject.QueryText), metric.ATTRIBUTE},
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
