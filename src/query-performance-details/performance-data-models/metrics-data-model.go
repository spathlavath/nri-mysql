package performance_data_models

import "database/sql"

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

type QueryPlanMetrics struct {
	QueryID             string `json:"query_id" db:"query_id"`
	AnonymizedQueryText string `json:"query_text" db:"query_text"`
	QueryText           string `json:"query_sample_text" db:"query_sample_text"`
}

type ExecutionPlan struct {
	TableMetrics []TableMetrics `json:"table_metrics"`
	TotalCost    float64        `json:"total_cost"`
}

type TableMetrics struct {
	StepID        int     `json:"step_id"`
	ExecutionStep string  `json:"Execution Step"`
	AccessType    string  `json:"access_type"`
	RowsExamined  int64   `json:"rows_examined"`
	RowsProduced  int64   `json:"rows_produced"`
	Filtered      float64 `json:"filtered (%)"`
	ReadCost      float64 `json:"read_cost"`
	EvalCost      float64 `json:"eval_cost"`
	DataRead      float64 `json:"data_read"`
	ExtraInfo     string  `json:"extra_info"`
}

type WaitEventQueryMetrics struct {
	TotalWaitTimeMs     float64        `json:"total_wait_time_ms" db:"total_wait_time_ms"`
	QueryID             sql.NullString `json:"query_id" db:"query_id"`
	QueryText           sql.NullString `json:"query_text" db:"query_text"`
	DatabaseName        sql.NullString `json:"database_name" db:"database_name"`
	WaitCategory        string         `json:"wait_category" db:"wait_category"`
	CollectionTimestamp string         `json:"collection_timestamp" db:"collection_timestamp"`
	InstanceID          string         `json:"instance_id" db:"instance_id"`
	WaitEventName       string         `json:"wait_event_name" db:"wait_event_name"`
	WaitingTasksCount   uint64         `json:"waiting_tasks_count" db:"waiting_tasks_count"`
}

type BlockingSessionMetrics struct {
	BlockedTxnID     sql.NullString `json:"blocked_txn_id" db:"blocked_txn_id"`
	BlockedThreadID  sql.NullInt64  `json:"blocked_thread_id" db:"blocked_thread_id"`
	BlockedUser      sql.NullString `json:"blocked_user" db:"blocked_user"`
	BlockedHost      sql.NullString `json:"blocked_host" db:"blocked_host"`
	BlockedDB        sql.NullString `json:"blocked_db" db:"blocked_db"`
	BlockingTxnID    sql.NullString `json:"blocking_txn_id" db:"blocking_txn_id"`
	BlockingThreadID sql.NullInt64  `json:"blocking_thread_id" db:"blocking_thread_id"`
	BlockingUser     sql.NullString `json:"blocking_user" db:"blocking_user"`
	BlockingHost     sql.NullString `json:"blocking_host" db:"blocking_host"`
	BlockingDB       sql.NullString `json:"blocking_db" db:"blocking_db"`
	BlockedQuery     sql.NullString `json:"blocked_query" db:"blocked_query"`
	BlockingQuery    sql.NullString `json:"blocking_query" db:"blocking_query"`
}
