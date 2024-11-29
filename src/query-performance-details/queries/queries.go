package queries

const (
	Slow_queries = `
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
        WHERE LAST_SEEN >= UTC_TIMESTAMP() - INTERVAL ? SECOND
			AND SCHEMA_NAME NOT IN ('', 'mysql', 'performance_schema', 'information_schema', 'sys')
            AND QUERY_SAMPLE_TEXT NOT LIKE '%SET %'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%SHOW %'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%INFORMATION_SCHEMA%'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%PERFORMANCE_SCHEMA%'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%mysql%'
            AND QUERY_SAMPLE_TEXT NOT LIKE 'EXPLAIN %'
			AND QUERY_SAMPLE_TEXT NOT LIKE '%DIGEST%'
			AND QUERY_SAMPLE_TEXT NOT LIKE '%DIGEST_TEXT%'
			AND QUERY_SAMPLE_TEXT NOT LIKE 'EXPLAIN %'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%PERFORMANCE_SCHEMA%'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%INFORMATION_SCHEMA%'
        ORDER BY avg_elapsed_time_ms DESC;
    `
	CurrentRunningQueriesSearch = `
		SELECT
			DIGEST AS query_id,
			DIGEST_TEXT AS query_text,
			SQL_TEXT AS query_sample_text
		FROM performance_schema.events_statements_current
		WHERE DIGEST IN (%s)
			AND CURRENT_SCHEMA NOT IN ('', 'mysql', 'performance_schema', 'information_schema', 'sys')
            AND SQL_TEXT NOT LIKE '%%SET %%'
            AND SQL_TEXT NOT LIKE '%%SHOW %%'
            AND SQL_TEXT NOT LIKE '%%INFORMATION_SCHEMA%%'
            AND SQL_TEXT NOT LIKE '%%PERFORMANCE_SCHEMA%%'
            AND SQL_TEXT NOT LIKE '%%mysql%%'
			AND SQL_TEXT NOT LIKE '%%DIGEST%%'
			AND SQL_TEXT NOT LIKE '%%DIGEST_TEXT%%'
            AND SQL_TEXT NOT LIKE 'EXPLAIN %%'
            AND SQL_TEXT NOT LIKE '%%PERFORMANCE_SCHEMA%%'
            AND SQL_TEXT NOT LIKE '%%INFORMATION_SCHEMA%%'
			AND TIMER_WAIT / 1000000 > ?
		ORDER BY TIMER_WAIT DESC;
	`
	RecentQueriesSearch = `
		SELECT
			DIGEST AS query_id,
			DIGEST_TEXT AS query_text,
			SQL_TEXT AS query_sample_text
		FROM performance_schema.events_statements_history
		WHERE DIGEST IN (%s)
			AND CURRENT_SCHEMA NOT IN ('', 'mysql', 'performance_schema', 'information_schema', 'sys')
            AND SQL_TEXT NOT LIKE '%%SET %%'
            AND SQL_TEXT NOT LIKE '%%SHOW %%'
            AND SQL_TEXT NOT LIKE '%%INFORMATION_SCHEMA%%'
            AND SQL_TEXT NOT LIKE '%%PERFORMANCE_SCHEMA%%'
            AND SQL_TEXT NOT LIKE '%%mysql%%'
			AND SQL_TEXT NOT LIKE '%%DIGEST%%'
			AND SQL_TEXT NOT LIKE '%%DIGEST_TEXT%%'
            AND SQL_TEXT NOT LIKE 'EXPLAIN %%'
            AND SQL_TEXT NOT LIKE '%%PERFORMANCE_SCHEMA%%'
            AND SQL_TEXT NOT LIKE '%%INFORMATION_SCHEMA%%'
			AND TIMER_WAIT / 1000000 > ?
		ORDER BY TIMER_WAIT DESC;
	`
	PastQueriesQuery = `
		SELECT
			DIGEST AS query_id,
			DIGEST_TEXT AS query_text,
			SQL_TEXT AS query_sample_text
		FROM performance_schema.events_statements_history_long
		WHERE DIGEST IN (%s)
			AND CURRENT_SCHEMA NOT IN ('', 'mysql', 'performance_schema', 'information_schema', 'sys')
            AND SQL_TEXT NOT LIKE '%%SET %%'
            AND SQL_TEXT NOT LIKE '%%SHOW %%'
            AND SQL_TEXT NOT LIKE '%%INFORMATION_SCHEMA%%'
            AND SQL_TEXT NOT LIKE '%%PERFORMANCE_SCHEMA%%'
            AND SQL_TEXT NOT LIKE '%%mysql%%'
			AND SQL_TEXT NOT LIKE '%%DIGEST%%'
			AND SQL_TEXT NOT LIKE '%%DIGEST_TEXT%%'
            AND SQL_TEXT NOT LIKE 'EXPLAIN %%'
            AND SQL_TEXT NOT LIKE '%%PERFORMANCE_SCHEMA%%'
            AND SQL_TEXT NOT LIKE '%%INFORMATION_SCHEMA%%'
			AND TIMER_WAIT / 1000000 > ?
		ORDER BY TIMER_WAIT DESC;
	`
	Wait_event_query = `
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
			SUM(wait_data.COUNT_STAR) AS wait_event_count,
			ROUND((SUM(wait_data.TIMER_WAIT) / 1000000000000) / SUM(wait_data.COUNT_STAR), 3) AS avg_wait_time_ms,
			schema_data.query_text,
			DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%sZ') AS collection_timestamp
		FROM (
			SELECT 
				THREAD_ID,
				OBJECT_INSTANCE_BEGIN AS instance_id,
				EVENT_NAME AS wait_event_name,
				TIMER_WAIT,
				1 AS COUNT_STAR
			FROM performance_schema.events_waits_history_long
			UNION ALL
			SELECT 
				THREAD_ID,
				OBJECT_INSTANCE_BEGIN AS instance_id,
				EVENT_NAME AS wait_event_name,
				TIMER_WAIT,
				1 AS COUNT_STAR
			FROM performance_schema.events_waits_current
		) AS wait_data
		JOIN (
			SELECT 
				THREAD_ID,
				DIGEST,
				CURRENT_SCHEMA AS database_name,
				SQL_TEXT AS query_text
			FROM performance_schema.events_statements_history_long
			UNION ALL
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
			total_wait_time_ms DESC;
	`
	Blocking_session_query = `
		SELECT 
                      r.trx_id AS blocked_txn_id,
                      r.trx_mysql_thread_id AS blocked_thread_id,
					  wt.PROCESSLIST_ID AS blocked_pid,
                      wt.PROCESSLIST_USER AS blocked_user,
                      wt.PROCESSLIST_HOST AS blocked_host,
                      wt.PROCESSLIST_DB AS database_name,
					  wt.PROCESSLIST_STATE AS blocked_status,
                      b.trx_id AS blocking_txn_id,
                      b.trx_mysql_thread_id AS blocking_thread_id,
					  bt.PROCESSLIST_ID AS blocking_pid,
                      bt.PROCESSLIST_USER AS blocking_user,
                      bt.PROCESSLIST_HOST AS blocking_host,
                      bt.PROCESSLIST_DB AS blocking_db,
                      es_waiting.DIGEST_TEXT AS blocked_query,
                      es_blocking.DIGEST_TEXT AS blocking_query,
					  es_waiting.DIGEST AS blocked_query_id,
                      es_blocking.DIGEST AS blocking_query_id,
    				  bt.PROCESSLIST_STATE AS blocking_status
                  FROM 
                      performance_schema.data_lock_waits w
                  JOIN 
                      performance_schema.threads wt ON wt.THREAD_ID = w.REQUESTING_THREAD_ID
                  JOIN 
                      information_schema.innodb_trx r ON r.trx_mysql_thread_id = wt.PROCESSLIST_ID
                  JOIN 
                      performance_schema.threads bt ON bt.THREAD_ID = w.BLOCKING_THREAD_ID
                  JOIN 
                      information_schema.innodb_trx b ON b.trx_mysql_thread_id = bt.PROCESSLIST_ID
                  JOIN 
                      performance_schema.events_statements_current esc_waiting ON esc_waiting.THREAD_ID = wt.THREAD_ID
                  JOIN 
                      performance_schema.events_statements_summary_by_digest es_waiting 
                      ON esc_waiting.DIGEST = es_waiting.DIGEST
                  JOIN 
                      performance_schema.events_statements_current esc_blocking ON esc_blocking.THREAD_ID = bt.THREAD_ID
                  JOIN 
                      performance_schema.events_statements_summary_by_digest es_blocking 
                      ON esc_blocking.DIGEST = es_blocking.DIGEST;
	`
)