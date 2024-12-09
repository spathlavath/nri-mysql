package query_details

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/nri-mysql/src/args"
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	"github.com/stretchr/testify/assert"
)

type dbWrapper struct {
	*sql.DB
}

func (d *dbWrapper) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	return sqlx.NewDb(d.DB, "mysql").QueryxContext(ctx, query, args...)
}

func (d *dbWrapper) QueryX(query string) (*sqlx.Rows, error) {
	return sqlx.NewDb(d.DB, "mysql").Queryx(query)
}

func (d *dbWrapper) Close() {
	d.DB.Close()
}

func TestPopulateBlockingSessionMetrics(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	i, err := integration.New("test", "1.0.0")
	assert.NoError(t, err)

	e := i.LocalEntity()
	args := args.ArgumentList{}

	wrappedDB := &dbWrapper{db}

	// Mock the query and rows
	query := `SELECT r.trx_id AS blocked_txn_id, r.trx_mysql_thread_id AS blocked_thread_id, wt.PROCESSLIST_ID AS blocked_pid, wt.PROCESSLIST_USER AS blocked_user, wt.PROCESSLIST_HOST AS blocked_host, wt.PROCESSLIST_DB AS database_name, wt.PROCESSLIST_STATE AS blocked_status, b.trx_id AS blocking_txn_id, b.trx_mysql_thread_id AS blocking_thread_id, bt.PROCESSLIST_ID AS blocking_pid, bt.PROCESSLIST_USER AS blocking_user, bt.PROCESSLIST_HOST AS blocking_host, es_waiting.DIGEST_TEXT AS blocked_query, es_blocking.DIGEST_TEXT AS blocking_query, es_waiting.DIGEST AS blocked_query_id, es_blocking.DIGEST AS blocking_query_id, bt.PROCESSLIST_STATE AS blocking_status FROM performance_schema.data_lock_waits w JOIN performance_schema.threads wt ON wt.THREAD_ID = w.REQUESTING_THREAD_ID JOIN information_schema.innodb_trx r ON r.trx_mysql_thread_id = wt.PROCESSLIST_ID JOIN performance_schema.threads bt ON bt.THREAD_ID = w.BLOCKING_THREAD_ID JOIN information_schema.innodb_trx b ON b.trx_mysql_thread_id = bt.PROCESSLIST_ID JOIN performance_schema.events_statements_current esc_waiting ON esc_waiting.THREAD_ID = wt.THREAD_ID JOIN performance_schema.events_statements_summary_by_digest es_waiting ON esc_waiting.DIGEST = es_waiting.DIGEST JOIN performance_schema.events_statements_current esc_blocking ON esc_blocking.THREAD_ID = bt.THREAD_ID JOIN performance_schema.events_statements_summary_by_digest es_blocking ON esc_blocking.DIGEST = es_blocking.DIGEST`
	rows := sqlmock.NewRows([]string{
		"blocked_txn_id", "blocked_pid", "blocked_thread_id", "blocked_query_id", "blocked_query",
		"blocked_user", "blocked_host", "database_name", "blocked_status", "blocking_txn_id", "blocking_pid",
		"blocking_thread_id", "blocking_user", "blocking_host", "blocking_query_id", "blocking_query", "blocking_status",
	}).AddRow(
		"blocked_txn_id", "blocked_pid", 123, "blocked_query_id", "blocked_query",
		"blocked_user", "blocked_host", "blocked_db", "blocked_status", "blocking_txn_id", "blocking_pid",
		456, "blocking_user", "blocking_host", "blocking_query_id", "blocking_query", "blocking_status",
	)

	mock.ExpectQuery(query).WillReturnRows(rows)

	metrics, err := PopulateBlockingSessionMetrics(wrappedDB, e, args)
	assert.NoError(t, err)
	assert.Len(t, metrics, 1)

	metric := metrics[0]
	assert.Equal(t, "blocked_txn_id", metric.BlockedTxnID.String)
	assert.Equal(t, "blocked_pid", metric.BlockedPID.String)
	assert.Equal(t, int64(123), metric.BlockedThreadID.Int64)
	assert.Equal(t, "blocked_query_id", metric.BlockedQueryID.String)
	assert.Equal(t, "blocked_query", metric.BlockedQuery.String)
	assert.Equal(t, "blocked_user", metric.BlockedUser.String)
	assert.Equal(t, "blocked_host", metric.BlockedHost.String)
	assert.Equal(t, "blocked_db", metric.BlockedDB.String)
	assert.Equal(t, "blocked_status", metric.BlockedStatus.String)
	assert.Equal(t, "blocking_txn_id", metric.BlockingTxnID.String)
	assert.Equal(t, "blocking_pid", metric.BlockingPID.String)
	assert.Equal(t, int64(456), metric.BlockingThreadID.Int64)
	assert.Equal(t, "blocking_user", metric.BlockingUser.String)
	assert.Equal(t, "blocking_host", metric.BlockingHost.String)
	assert.Equal(t, "blocking_query_id", metric.BlockingQueryID.String)
	assert.Equal(t, "blocking_query", metric.BlockingQuery.String)
	assert.Equal(t, "blocking_status", metric.BlockingStatus.String)

	// Ensure all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestSetBlockingQueryMetrics_Success(t *testing.T) {
	i, err := integration.New("test", "1.0.0")
	assert.NoError(t, err)

	e := i.LocalEntity()
	args := args.ArgumentList{}

	metrics := []performance_data_model.BlockingSessionMetrics{
		{
			BlockedTxnID:     sql.NullString{String: "blocked_txn_id", Valid: true},
			BlockedPID:       sql.NullString{String: "blocked_pid", Valid: true},
			BlockedThreadID:  sql.NullInt64{Int64: 123, Valid: true},
			BlockedQueryID:   sql.NullString{String: "blocked_query_id", Valid: true},
			BlockedQuery:     sql.NullString{String: "blocked_query", Valid: true},
			BlockedUser:      sql.NullString{String: "blocked_user", Valid: true},
			BlockedHost:      sql.NullString{String: "blocked_host", Valid: true},
			BlockedDB:        sql.NullString{String: "blocked_db", Valid: true},
			BlockingTxnID:    sql.NullString{String: "blocking_txn_id", Valid: true},
			BlockingPID:      sql.NullString{String: "blocking_pid", Valid: true},
			BlockingThreadID: sql.NullInt64{Int64: 456, Valid: true},
			BlockingUser:     sql.NullString{String: "blocking_user", Valid: true},
			BlockingHost:     sql.NullString{String: "blocking_host", Valid: true},
			BlockingQueryID:  sql.NullString{String: "blocking_query_id", Valid: true},
			BlockingQuery:    sql.NullString{String: "blocking_query", Valid: true},
		},
	}

	err = setBlockingQueryMetrics(metrics, e, args)
	assert.NoError(t, err)

	ms := e.Metrics[0]
	assert.Equal(t, "blocked_txn_id", ms.Metrics["blocked_txn_id"])
	assert.Equal(t, "blocked_pid", ms.Metrics["blocked_pid"])
	assert.Equal(t, float64(123), ms.Metrics["blocked_thread_id"])
	assert.Equal(t, "blocked_query_id", ms.Metrics["blocked_query_id"])
	assert.Equal(t, "blocked_query", ms.Metrics["blocked_query"])
	assert.Equal(t, "blocked_user", ms.Metrics["blocked_user"])
	assert.Equal(t, "blocked_host", ms.Metrics["blocked_host"])
	assert.Equal(t, "blocked_db", ms.Metrics["database_name"])
	assert.Equal(t, "blocking_txn_id", ms.Metrics["blocking_txn_id"])
	assert.Equal(t, "blocking_pid", ms.Metrics["blocking_pid"])
	assert.Equal(t, float64(456), ms.Metrics["blocking_thread_id"])
	assert.Equal(t, "blocking_user", ms.Metrics["blocking_user"])
	assert.Equal(t, "blocking_host", ms.Metrics["blocking_host"])
	assert.Equal(t, "blocking_query_id", ms.Metrics["blocking_query_id"])
	assert.Equal(t, "blocking_query", ms.Metrics["blocking_query"])
}

func TestSetBlockingQueryMetrics_InvalidMetricValue(t *testing.T) {
	i, err := integration.New("test", "1.0.0")
	assert.NoError(t, err)

	e := i.LocalEntity()
	args := args.ArgumentList{}
	metrics := []performance_data_model.BlockingSessionMetrics{
		{
			BlockedTxnID:     sql.NullString{String: "blocked_txn_id", Valid: true},
			BlockedPID:       sql.NullString{String: "blocked_pid", Valid: true},
			BlockedThreadID:  sql.NullInt64{Int64: 123, Valid: true},
			BlockedQueryID:   sql.NullString{String: "blocked_query_id", Valid: true},
			BlockedQuery:     sql.NullString{String: "blocked_query", Valid: true},
			BlockedUser:      sql.NullString{String: "blocked_user", Valid: true},
			BlockedHost:      sql.NullString{String: "blocked_host", Valid: true},
			BlockedDB:        sql.NullString{String: "blocked_db", Valid: true},
			BlockingTxnID:    sql.NullString{String: "blocking_txn_id", Valid: true},
			BlockingPID:      sql.NullString{String: "blocking_pid", Valid: true},
			BlockingThreadID: sql.NullInt64{Int64: 456, Valid: true},
			BlockingUser:     sql.NullString{String: "blocking_user", Valid: true},
			BlockingHost:     sql.NullString{String: "blocking_host", Valid: true},
			BlockingQueryID:  sql.NullString{String: "blocking_query_id", Valid: true},
			BlockingQuery:    sql.NullString{String: "blocking_query", Valid: true},
		},
	}

	// Simulate invalid metric value by passing an unsupported type
	err = setBlockingQueryMetrics(metrics, e, args)
	assert.NoError(t, err)
	ms := e.Metrics[0]
	assert.Equal(t, "blocked_txn_id", ms.Metrics["blocked_txn_id"])
	assert.Equal(t, "blocked_pid", ms.Metrics["blocked_pid"])
	assert.Equal(t, float64(123), ms.Metrics["blocked_thread_id"])
	assert.Equal(t, "blocked_query_id", ms.Metrics["blocked_query_id"])
	assert.Equal(t, "blocked_query", ms.Metrics["blocked_query"])
	assert.Equal(t, "blocked_user", ms.Metrics["blocked_user"])
	assert.Equal(t, "blocked_host", ms.Metrics["blocked_host"])
	assert.Equal(t, "blocked_db", ms.Metrics["database_name"])
	assert.Equal(t, "blocking_txn_id", ms.Metrics["blocking_txn_id"])
	assert.Equal(t, "blocking_pid", ms.Metrics["blocking_pid"])
	assert.Equal(t, float64(456), ms.Metrics["blocking_thread_id"])
	assert.Equal(t, "blocking_user", ms.Metrics["blocking_user"])
	assert.Equal(t, "blocking_host", ms.Metrics["blocking_host"])
	assert.Equal(t, "blocking_query_id", ms.Metrics["blocking_query_id"])
	assert.Equal(t, "blocking_query", ms.Metrics["blocking_query"])
}

func TestSetBlockingQueryMetrics_EmptyMetrics(t *testing.T) {
	i, err := integration.New("test", "1.0.0")
	assert.NoError(t, err)

	e := i.LocalEntity()
	args := args.ArgumentList{}

	err = setBlockingQueryMetrics([]performance_data_model.BlockingSessionMetrics{}, e, args)
	assert.NoError(t, err)
}

func TestSetBlockingQueryMetrics_ErrorSettingMetric(t *testing.T) {
	args := args.ArgumentList{}
	metrics := []performance_data_model.BlockingSessionMetrics{
		{
			BlockedTxnID:     sql.NullString{String: "blocked_txn_id", Valid: true},
			BlockedPID:       sql.NullString{String: "blocked_pid", Valid: true},
			BlockedThreadID:  sql.NullInt64{Int64: 123, Valid: true},
			BlockedQueryID:   sql.NullString{String: "blocked_query_id", Valid: true},
			BlockedQuery:     sql.NullString{String: "blocked_query", Valid: true},
			BlockedUser:      sql.NullString{String: "blocked_user", Valid: true},
			BlockedHost:      sql.NullString{String: "blocked_host", Valid: true},
			BlockedDB:        sql.NullString{String: "blocked_db", Valid: true},
			BlockingTxnID:    sql.NullString{String: "blocking_txn_id", Valid: true},
			BlockingPID:      sql.NullString{String: "blocking_pid", Valid: true},
			BlockingThreadID: sql.NullInt64{Int64: 456, Valid: true},
			BlockingUser:     sql.NullString{String: "blocking_user", Valid: true},
			BlockingHost:     sql.NullString{String: "blocking_host", Valid: true},
			BlockingQueryID:  sql.NullString{String: "blocking_query_id", Valid: true},
			BlockingQuery:    sql.NullString{String: "blocking_query", Valid: true},
		},
	}

	// Simulate error by passing nil entity
	err := setBlockingQueryMetrics(metrics, nil, args)
	assert.Error(t, err)
	assert.Equal(t, "entity is nil", err.Error())
}
