package query_performance_details

import (
	"context"
	"database/sql"

	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
)

type WaitEventQueryMetrics struct {
	TotalWaitTimeMs     float64        `json:"total_wait_time_ms" db:"total_wait_time_ms"`
	QueryID             sql.NullString `json:"query_id" db:"query_id"`
	ThreadID            sql.NullInt64  `json:"thread_id" db:"thread_id"`
	QueryText           sql.NullString `json:"query_text" db:"query_text"`
	DatabaseName        sql.NullString `json:"database_name" db:"database_name"`
	WaitCategory        string         `json:"wait_category" db:"wait_category"`
	CollectionTimestamp string         `json:"collection_timestamp" db:"collection_timestamp"`
	InstanceID          string         `json:"instance_id" db:"instance_id"`
	WaitEventName       string         `json:"wait_event_name" db:"wait_event_name"`
	WaitingTasksCount   uint64         `json:"waiting_tasks_count" db:"waiting_tasks_count"`
}
type WaitEvent struct {
	QueryID  string
	ThreadID int64
	// other fields...
}

func correlateWaitEvents(queries []QueryMetrics, waitEvents []WaitEvent) map[string][]WaitEvent {
	correlatedEvents := make(map[string][]WaitEvent)

	if len(queries) == 0 || len(waitEvents) == 0 {
		log.Warn("Correlation cannot be performed: queries or waitEvents is empty.")
		return correlatedEvents
	}

	// Create a map for fast lookups of queries by query_id
	queryMap := make(map[string]QueryMetrics)
	for _, query := range queries {
		queryMap[query.QueryID] = query
	}

	// Correlate wait events by query_id
	for _, event := range waitEvents {
		if event.QueryID != "" {
			if _, exists := queryMap[event.QueryID]; exists {
				correlatedEvents[event.QueryID] = append(correlatedEvents[event.QueryID], event)
				continue
			}
		}

		// Fallback to thread_id if query_id is unavailable
		if event.ThreadID != 0 {
			for _, query := range queries {
				if query.ThreadID == event.ThreadID {
					correlatedEvents[query.QueryID] = append(correlatedEvents[query.QueryID], event)
					log.Warn("Warning: Correlation based on thread_id for event with ThreadID: %d", event.ThreadID)
					break
				}
			}
		}
	}

	return correlatedEvents
}

func collectWaitEventQueryMetrics(db dataSource) ([]WaitEventQueryMetrics, error) {
	metrics, err := collectWaitEventMetrics(db)
	if err != nil {
		log.Error("Failed to collect wait event metrics: %v", err)
		return nil, err
	}
	return metrics, nil
}

func collectWaitEventMetrics(db dataSource) ([]WaitEventQueryMetrics, error) {
	query := `
		SELECT
			DIGEST AS query_id,
			wait_data.instance_id,
			schema_data.database_name,
			wait_data.wait_event_name,
			CASE
				WHEN wait_data.wait_event_name LIKE 'wait/io/file/innodb/%' THEN 'InnoDB File IO'
				WHEN wait_data.wait_event_name LIKE 'wait/io/file/sql/%' THEN 'SQL File IO'
				WHEN wait_data.wait_event_name LIKE 'wait/io/socket/%' THEN 'Network IO'
				WHEN wait_data.wait_event_name LIKE 'wait/synch/cond/%' THEN 'Condition Wait'
				WHEN wait_data.wait_event_name LIKE 'wait/synch/mutex/%' THEN 'Mutex'
				WHEN wait_data.wait_event_name LIKE 'wait/lock/table/%' THEN 'Table Lock'
				WHEN wait_data.wait_event_name LIKE 'wait/lock/metadata/%' THEN 'Metadata Lock'
				WHEN wait_data.wait_event_name LIKE 'wait/lock/transaction/%' THEN 'Transaction Lock'
				ELSE 'Other'
			END AS wait_category,
			ROUND(SUM(wait_data.TIMER_WAIT) / 1000000000000, 3) AS total_wait_time_ms,
			SUM(wait_data.COUNT_STAR) AS waiting_tasks_count,
			schema_data.query_text,
			DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%sZ') AS collection_timestamp
		FROM (
			-- Historical wait data
			SELECT 
				THREAD_ID,
				OBJECT_INSTANCE_BEGIN AS instance_id,
				EVENT_NAME AS wait_event_name,
				TIMER_WAIT,
				1 AS COUNT_STAR
			FROM performance_schema.events_waits_history_long
			UNION ALL
			-- Current wait data
			SELECT 
				THREAD_ID,
				OBJECT_INSTANCE_BEGIN AS instance_id,
				EVENT_NAME AS wait_event_name,
				TIMER_WAIT,
				1 AS COUNT_STAR
			FROM performance_schema.events_waits_current
		) AS wait_data
		JOIN (
			-- Historical queries
			SELECT 
				THREAD_ID,
				DIGEST,
				CURRENT_SCHEMA AS database_name,
				SQL_TEXT AS query_text
			FROM performance_schema.events_statements_history_long
			UNION ALL
			-- Current queries
			SELECT 
				THREAD_ID,
				DIGEST,
				CURRENT_SCHEMA AS database_name,
				SQL_TEXT AS query_text
			FROM performance_schema.events_statements_current
		) AS schema_data
		ON wait_data.THREAD_ID = schema_data.THREAD_ID
		GROUP BY
			query_id,
			wait_data.instance_id,
			wait_data.wait_event_name,
			wait_category,
			schema_data.database_name,
			schema_data.query_text
		ORDER BY 
			total_wait_time_ms DESC
		LIMIT 10;
	`

	rows, err := db.QueryxContext(context.Background(), query)
	if err != nil {
		log.Error("Failed to execute query: %v", err)
		return nil, err
	}
	defer rows.Close()

	var metrics []WaitEventQueryMetrics
	for rows.Next() {
		var metric WaitEventQueryMetrics
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

	return metrics, nil
}

// populateWaitEventMetrics populates the metric set with the wait event metrics.
func populateWaitEventMetrics(e *integration.Entity, args arguments.ArgumentList, metrics []WaitEventQueryMetrics) error {
	for _, metricData := range metrics {
		// Create a new metric set for each row
		ms := createMetricSet(e, "MysqlWaitEventSample", args)
		metricsMap := map[string]struct {
			Value      interface{}
			MetricType metric.SourceType
		}{

			"total_wait_time_ms":   {metricData.TotalWaitTimeMs, metric.GAUGE},
			"query_id":             {getStringValue(metricData.QueryID), metric.ATTRIBUTE},
			"query_text":           {getStringValue(metricData.QueryText), metric.ATTRIBUTE},
			"database_name":        {getStringValue(metricData.DatabaseName), metric.ATTRIBUTE},
			"wait_category":        {metricData.WaitCategory, metric.ATTRIBUTE},
			"collection_timestamp": {metricData.CollectionTimestamp, metric.ATTRIBUTE},
			"instance_id":          {metricData.InstanceID, metric.ATTRIBUTE},
			"wait_event_name":      {metricData.WaitEventName, metric.ATTRIBUTE},
			"waiting_tasks_count":  {int(metricData.WaitingTasksCount), metric.GAUGE},
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
