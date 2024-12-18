package performance_data_models

type SlowQueryMetrics struct {
	QueryID                string  `json:"query_id" db:"query_id"`
	QueryText              string  `json:"query_text" db:"query_text"`
	DatabaseName           string  `json:"database_name" db:"database_name"`
	SchemaName             string  `json:"schema_name" db:"schema_name"`
	ExecutionCount         uint64  `json:"execution_count" db:"execution_count"`
	AvgCPUTimeMs           float64 `json:"avg_cpu_time_ms" db:"avg_cpu_time_ms"`
	AvgElapsedTimeMs       float64 `json:"avg_elapsed_time_ms" db:"avg_elapsed_time_ms"`
	AvgDiskReads           float64 `json:"avg_disk_reads" db:"avg_disk_reads"`
	AvgDiskWrites          float64 `json:"avg_disk_writes" db:"avg_disk_writes"`
	HasFullTableScan       string  `json:"has_full_table_scan" db:"has_full_table_scan"`
	StatementType          string  `json:"statement_type" db:"statement_type"`
	LastExecutionTimestamp string  `json:"last_execution_timestamp" db:"last_execution_timestamp"`
	CollectionTimestamp    string  `json:"collection_timestamp" db:"collection_timestamp"`
}

type IndividualQueryMetrics struct {
	QueryID             string  `json:"query_id" db:"query_id"`
	AnonymizedQueryText string  `json:"query_text" db:"query_text"`
	QueryText           string  `json:"query_sample_text" db:"query_sample_text"`
	EventID             uint64  `json:"event_id" db:"event_id"`
	ThreadID            uint64  `json:"thread_id" db:"thread_id"`
	ExecutionTimeMs     float64 `json:"execution_time_ms" db:"execution_time_ms"`
	RowsSent            int64   `json:"rows_sent" db:"rows_sent"`
	RowsExamined        int64   `json:"rows_examined" db:"rows_examined"`
	DatabaseName        string  `json:"database_name" db:"database_name"`
}

type QueryGroup struct {
	Database string
	Queries  []IndividualQueryMetrics
}

type QueryPlanMetrics struct {
	EventID             uint64 `json:"event_id"`
	QueryCost           string `json:"query_cost"`
	TableName           string `json:"table_name"`
	AccessType          string `json:"access_type"`
	RowsExaminedPerScan int64  `json:"rows_examined_per_scan"`
	RowsProducedPerJoin int64  `json:"rows_produced_per_join"`
	Filtered            string `json:"filtered"`
	ReadCost            string `json:"read_cost"`
	EvalCost            string `json:"eval_cost"`
}

type Memo struct {
	QueryCost string `json:"query_cost"`
}

type WaitEventQueryMetrics struct {
	TotalWaitTimeMs     float64 `json:"total_wait_time_ms" db:"total_wait_time_ms"`
	QueryID             string  `json:"query_id" db:"query_id"`
	QueryText           string  `json:"query_text" db:"query_text"`
	DatabaseName        string  `json:"database_name" db:"database_name"`
	WaitCategory        string  `json:"wait_category" db:"wait_category"`
	CollectionTimestamp string  `json:"collection_timestamp" db:"collection_timestamp"`
	InstanceID          string  `json:"instance_id" db:"instance_id"`
	WaitEventName       string  `json:"wait_event_name" db:"wait_event_name"`
	WaitEventCount      uint64  `json:"wait_event_count" db:"wait_event_count"`
	AvgWaitTimeMs       string  `json:"avg_wait_time_ms" db:"avg_wait_time_ms"`
}

type BlockingSessionMetrics struct {
	BlockedTxnID     string `json:"blocked_txn_id" db:"blocked_txn_id"`
	BlockedPID       string `json:"blocked_pid" db:"blocked_pid"`
	BlockedThreadID  int64  `json:"blocked_thread_id" db:"blocked_thread_id"`
	BlockedQueryID   string `json:"blocked_query_id" db:"blocked_query_id"`
	BlockedQuery     string `json:"blocked_query" db:"blocked_query"`
	BlockedStatus    string `json:"blocked_status" db:"blocked_status"`
	BlockedUser      string `json:"blocked_user" db:"blocked_user"`
	BlockedHost      string `json:"blocked_host" db:"blocked_host"`
	BlockedDB        string `json:"database_name" db:"database_name"`
	BlockingTxnID    string `json:"blocking_txn_id" db:"blocking_txn_id"`
	BlockingPID      string `json:"blocking_pid" db:"blocking_pid"`
	BlockingThreadID int64  `json:"blocking_thread_id" db:"blocking_thread_id"`
	BlockingUser     string `json:"blocking_user" db:"blocking_user"`
	BlockingHost     string `json:"blocking_host" db:"blocking_host"`
	BlockingQueryID  string `json:"blocking_query_id" db:"blocking_query_id"`
	BlockingQuery    string `json:"blocking_query" db:"blocking_query"`
	BlockingStatus   string `json:"blocking_status" db:"blocking_status"`
}
