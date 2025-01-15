package performancemetricscollectors

import (
	"context"
	"database/sql"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/nri-mysql/src/args"
	arguments "github.com/newrelic/nri-mysql/src/args"
	utils "github.com/newrelic/nri-mysql/src/query-performance-monitoring/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func convertNullString(ns sql.NullString) *string {
	if ns.Valid {
		return &ns.String
	}
	return nil
}

func convertNullInt64(ni sql.NullInt64) *int64 {
	if ni.Valid {
		return &ni.Int64
	}
	return nil
}

// Mocking utils.IngestMetric function
type MockUtilsIngest struct {
	mock.Mock
}

func (m *MockUtilsIngest) IngestMetric(metricList []interface{}, sampleName string, i *integration.Integration, args arguments.ArgumentList) error {
	callArgs := m.Called(metricList, sampleName, i, args)
	return callArgs.Error(0)
}

func Float64Ptr(f float64) *float64 {
	return &f
}

type dbWrapper struct {
	DB *sqlx.DB
}

func (d *dbWrapper) Close() {
	d.DB.Close()
}

func (d *dbWrapper) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	return d.DB.QueryxContext(ctx, query, args...)
}

func (d *dbWrapper) QueryX(query string) (*sqlx.Rows, error) {
	return d.DB.Queryx(query)
}

func TestPopulateBlockingSessionMetrics(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockRows := sqlmock.NewRows([]string{})

	query := `SELECT r.trx_id AS blocked_txn_id, r.trx_mysql_thread_id AS blocked_thread_id, wt.PROCESSLIST_ID AS blocked_pid, wt.PROCESSLIST_HOST AS blocked_host, wt.PROCESSLIST_DB AS database_name, wt.PROCESSLIST_STATE AS blocked_status, b.trx_id AS blocking_txn_id, b.trx_mysql_thread_id AS blocking_thread_id, bt.PROCESSLIST_ID AS blocking_pid, bt.PROCESSLIST_HOST AS blocking_host, es_waiting.DIGEST_TEXT AS blocked_query, es_blocking.DIGEST_TEXT AS blocking_query, es_waiting.DIGEST AS blocked_query_id, es_blocking.DIGEST AS blocking_query_id, bt.PROCESSLIST_STATE AS blocking_status FROM performance_schema.data_lock_waits w JOIN performance_schema.threads wt ON wt.THREAD_ID = w.REQUESTING_THREAD_ID JOIN information_schema.innodb_trx r ON r.trx_mysql_thread_id = wt.PROCESSLIST_ID JOIN performance_schema.threads bt ON bt.THREAD_ID = w.BLOCKING_THREAD_ID JOIN information_schema.innodb_trx b ON b.trx_mysql_thread_id = bt.PROCESSLIST_ID JOIN performance_schema.events_statements_current esc_waiting ON esc_waiting.THREAD_ID = wt.THREAD_ID JOIN performance_schema.events_statements_summary_by_digest es_waiting ON esc_waiting.DIGEST = es_waiting.DIGEST JOIN performance_schema.events_statements_current esc_blocking ON esc_blocking.THREAD_ID = bt.THREAD_ID JOIN performance_schema.events_statements_summary_by_digest es_blocking ON esc_blocking.DIGEST = es_blocking.DIGEST WHERE wt.PROCESSLIST_DB IS NOT NULL AND wt.PROCESSLIST_DB NOT IN (?, ?, ?, ?, ?) LIMIT ?;`

	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockRows)

	dataSource := &dbWrapper{DB: sqlx.NewDb(db, "sqlmock")}
	i, _ := integration.New("test", "1.0.0")
	e := i.LocalEntity()
	args := args.ArgumentList{ExcludedDatabases: `["", " mysql", "information_schema", "performance_schema", "sys"]`, QueryCountThreshold: 10}

	PopulateBlockingSessionMetrics(dataSource, i, e, args, []string{})
	assert.NoError(t, err)
}

func TestSetBlockingQueryMetrics(t *testing.T) {
	i, err := integration.New("test", "1.0.0")
	assert.NoError(t, err)
	e := i.LocalEntity()
	args := args.ArgumentList{}
	metrics := []utils.BlockingSessionMetrics{
		{
			BlockedTxnID:     convertNullString(sql.NullString{String: "blocked_txn_id", Valid: true}),
			BlockedPID:       convertNullString(sql.NullString{String: "blocked_pid", Valid: true}),
			BlockedThreadID:  convertNullInt64(sql.NullInt64{Int64: 123, Valid: true}),
			BlockedQueryID:   convertNullString(sql.NullString{String: "blocked_query_id", Valid: true}),
			BlockedQuery:     convertNullString(sql.NullString{String: "blocked_query", Valid: true}),
			BlockedStatus:    convertNullString(sql.NullString{String: "blocked_status", Valid: true}),
			BlockedHost:      convertNullString(sql.NullString{String: "blocked_host", Valid: true}),
			BlockedDB:        convertNullString(sql.NullString{String: "blocked_db", Valid: true}),
			BlockingTxnID:    convertNullString(sql.NullString{String: "blocking_txn_id", Valid: true}),
			BlockingPID:      convertNullString(sql.NullString{String: "blocking_pid", Valid: true}),
			BlockingThreadID: convertNullInt64(sql.NullInt64{Int64: 456, Valid: true}),
			BlockingStatus:   convertNullString(sql.NullString{String: "blocking_status", Valid: true}),
			BlockingHost:     convertNullString(sql.NullString{String: "blocking_host", Valid: true}),
			BlockingQueryID:  convertNullString(sql.NullString{String: "blocking_query_id", Valid: true}),
			BlockingQuery:    convertNullString(sql.NullString{String: "blocking_query", Valid: true}),
		},
	}
	setBlockingQueryMetrics(metrics, i, args)
	assert.NoError(t, err)
	ms := e.Metrics[0]
	assert.Equal(t, "blocked_txn_id", ms.Metrics["blocked_txn_id"])
	assert.Equal(t, "blocked_pid", ms.Metrics["blocked_pid"])
	assert.Equal(t, float64(123), ms.Metrics["blocked_thread_id"])
	assert.Equal(t, "blocked_query_id", ms.Metrics["blocked_query_id"])
	assert.Equal(t, "blocked_query", ms.Metrics["blocked_query"])
	assert.Equal(t, "blocked_host", ms.Metrics["blocked_host"])
	assert.Equal(t, "blocked_db", ms.Metrics["database_name"])
	assert.Equal(t, "blocking_txn_id", ms.Metrics["blocking_txn_id"])
	assert.Equal(t, "blocking_pid", ms.Metrics["blocking_pid"])
	assert.Equal(t, float64(456), ms.Metrics["blocking_thread_id"])
	assert.Equal(t, "blocking_host", ms.Metrics["blocking_host"])
	assert.Equal(t, "blocking_query_id", ms.Metrics["blocking_query_id"])
	assert.Equal(t, "blocking_query", ms.Metrics["blocking_query"])
}
