package query_performance_details

func collectPerformanceSchemaMetrics(db dataSource) ([]QueryMetrics, error) {
	query := `
        SELECT
            DIGEST AS db_query_id,
            LEFT(UPPER(SHA2(DIGEST_TEXT, 256)), 16) AS query_id,
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
            AND DIGEST_TEXT NOT LIKE '%SET %'
            AND DIGEST_TEXT NOT LIKE '%SHOW %'
            AND DIGEST_TEXT NOT LIKE '%INFORMATION_SCHEMA%'
            AND DIGEST_TEXT NOT LIKE '%PERFORMANCE_SCHEMA%'
            AND DIGEST_TEXT NOT LIKE '%mysql%'
            AND DIGEST_TEXT NOT LIKE 'EXPLAIN %'
        ORDER BY avg_elapsed_time_ms DESC;
    `

	return executeQuery(db, query, &[]QueryMetrics{})
}
