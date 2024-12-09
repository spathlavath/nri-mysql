package query_details

import (
	"context"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/nri-mysql/src/args"
	arguments "github.com/newrelic/nri-mysql/src/args"
	performancedatamodel "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	queries "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockDataSource is a mock implementation of the DataSource interface
type MockDataSource struct {
	mock.Mock
	db *sqlx.DB
}

func (m *MockDataSource) Close() {
	m.db.Close()
}

func (m *MockDataSource) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	rows, err := m.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (m *MockDataSource) QueryX(query string) (*sqlx.Rows, error) {
	return m.db.Queryx(query)
}

func NewMockDataSource(db *sqlx.DB) *MockDataSource {
	return &MockDataSource{db: db}
}

func TestPopulateSlowQueryMetrics(t *testing.T) {
	t.Run("successful query execution", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer mockDB.Close()

		db := NewMockDataSource(sqlx.NewDb(mockDB, "sqlmock"))
		i, _ := integration.New("test-integration", "1.0.0")
		e := i.LocalEntity()
		args := args.ArgumentList{SlowQueryInterval: 60}

		expectedQuery := `
            SELECT DIGEST AS query_id, DIGEST_TEXT AS query_text, SCHEMA_NAME AS database_name, 'N/A' AS schema_name, COUNT_STAR AS execution_count,
            ROUND((SUM_CPU_TIME / COUNT_STAR) / 1000000000, 3) AS avg_cpu_time_ms, ROUND((SUM_TIMER_WAIT / COUNT_STAR) / 1000000000, 3) AS avg_elapsed_time_ms,
            SUM_ROWS_EXAMINED / COUNT_STAR AS avg_disk_reads, SUM_ROWS_AFFECTED / COUNT_STAR AS avg_disk_writes,
            CASE WHEN SUM_NO_INDEX_USED > 0 THEN 'Yes' ELSE 'No' END AS has_full_table_scan,
            CASE WHEN DIGEST_TEXT LIKE 'SELECT%' THEN 'SELECT' WHEN DIGEST_TEXT LIKE 'INSERT%' THEN 'INSERT' WHEN DIGEST_TEXT LIKE 'UPDATE%' THEN 'UPDATE'
            WHEN DIGEST_TEXT LIKE 'DELETE%' THEN 'DELETE' ELSE 'OTHER' END AS statement_type,
            DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%sZ') AS collection_timestamp
            FROM performance_schema.events_statements_summary_by_digest
            WHERE LAST_SEEN >= UTC_TIMESTAMP() - INTERVAL ? SECOND
            AND SCHEMA_NAME NOT IN ('', 'mysql', 'performance_schema', 'information_schema', 'sys')
            AND QUERY_SAMPLE_TEXT NOT LIKE '%SET %' AND QUERY_SAMPLE_TEXT NOT LIKE '%SHOW %'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%INFORMATION_SCHEMA%' AND QUERY_SAMPLE_TEXT NOT LIKE '%PERFORMANCE_SCHEMA%'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%mysql%' AND QUERY_SAMPLE_TEXT NOT LIKE 'EXPLAIN %'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%DIGEST%' AND QUERY_SAMPLE_TEXT NOT LIKE '%DIGEST_TEXT%'
            AND QUERY_SAMPLE_TEXT NOT LIKE 'EXPLAIN %' AND QUERY_SAMPLE_TEXT NOT LIKE 'START %'
            ORDER BY avg_elapsed_time_ms DESC;
        `

		mock.ExpectQuery(expectedQuery).
			WithArgs(60).
			WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "database_name", "schema_name", "execution_count", "avg_cpu_time_ms", "avg_elapsed_time_ms", "avg_disk_reads", "avg_disk_writes", "has_full_table_scan", "statement_type", "collection_timestamp"}).
				AddRow("1", "SELECT * FROM table", "db", "schema", 10, 100, 200, 5, 5, 1, "SELECT", time.Now()))

		queryIdList := PopulateSlowQueryMetrics(e, db, args)
		if len(queryIdList) != 1 {
			t.Errorf("expected 1 query ID, got %d", len(queryIdList))
		}
	})

	t.Run("query execution failure", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer mockDB.Close()

		db := NewMockDataSource(sqlx.NewDb(mockDB, "sqlmock"))
		i, _ := integration.New("test-integration", "1.0.0")
		e := i.LocalEntity()
		args := args.ArgumentList{SlowQueryInterval: 60}

		expectedQuery := `
            SELECT DIGEST AS query_id, DIGEST_TEXT AS query_text, SCHEMA_NAME AS database_name, 'N/A' AS schema_name, COUNT_STAR AS execution_count,
            ROUND((SUM_CPU_TIME / COUNT_STAR) / 1000000000, 3) AS avg_cpu_time_ms, ROUND((SUM_TIMER_WAIT / COUNT_STAR) / 1000000000, 3) AS avg_elapsed_time_ms,
            SUM_ROWS_EXAMINED / COUNT_STAR AS avg_disk_reads, SUM_ROWS_AFFECTED / COUNT_STAR AS avg_disk_writes,
            CASE WHEN SUM_NO_INDEX_USED > 0 THEN 'Yes' ELSE 'No' END AS has_full_table_scan,
            CASE WHEN DIGEST_TEXT LIKE 'SELECT%' THEN 'SELECT' WHEN DIGEST_TEXT LIKE 'INSERT%' THEN 'INSERT' WHEN DIGEST_TEXT LIKE 'UPDATE%' THEN 'UPDATE'
            WHEN DIGEST_TEXT LIKE 'DELETE%' THEN 'DELETE' ELSE 'OTHER' END AS statement_type,
            DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%sZ') AS collection_timestamp
            FROM performance_schema.events_statements_summary_by_digest
            WHERE LAST_SEEN >= UTC_TIMESTAMP() - INTERVAL ? SECOND
            AND SCHEMA_NAME NOT IN ('', 'mysql', 'performance_schema', 'information_schema', 'sys')
            AND QUERY_SAMPLE_TEXT NOT LIKE '%SET %' AND QUERY_SAMPLE_TEXT NOT LIKE '%SHOW %'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%INFORMATION_SCHEMA%' AND QUERY_SAMPLE_TEXT NOT LIKE '%PERFORMANCE_SCHEMA%'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%mysql%' AND QUERY_SAMPLE_TEXT NOT LIKE 'EXPLAIN %'
            AND QUERY_SAMPLE_TEXT NOT LIKE '%DIGEST%' AND QUERY_SAMPLE_TEXT NOT LIKE '%DIGEST_TEXT%'
            AND QUERY_SAMPLE_TEXT NOT LIKE 'EXPLAIN %' AND QUERY_SAMPLE_TEXT NOT LIKE 'START %'
            ORDER BY avg_elapsed_time_ms DESC;
        `

		mock.ExpectQuery(expectedQuery).
			WithArgs(60).
			WillReturnError(fmt.Errorf("query execution failed"))

		queryIdList := PopulateSlowQueryMetrics(e, db, args)
		if queryIdList != nil {
			t.Errorf("expected nil query ID list, got %v", queryIdList)
		}
	})
}

func TestCollectPerformanceSchemaMetrics(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockDB.Close()

	db := NewMockDataSource(sqlx.NewDb(mockDB, "sqlmock"))
	slowQueryInterval := 60

	mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_summary_by_digest").
		WithArgs(slowQueryInterval).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "database_name", "schema_name", "execution_count", "avg_cpu_time_ms", "avg_elapsed_time_ms", "avg_disk_reads", "avg_disk_writes", "has_full_table_scan", "statement_type", "collection_timestamp"}).
			AddRow("1", "SELECT * FROM table", "db", "schema", 10, 100, 200, 5, 5, 1, "SELECT", time.Now()))

	metrics, queryIdList, err := collectPerformanceSchemaMetrics(db, slowQueryInterval)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(metrics) != 1 {
		t.Errorf("expected 1 metric, got %d", len(metrics))
	}
	if len(queryIdList) != 1 {
		t.Errorf("expected 1 query ID, got %d", len(queryIdList))
	}
}

func TestPopulateIndividualQueryDetails(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockDB.Close()

	db := NewMockDataSource(sqlx.NewDb(mockDB, "sqlmock"))
	queryIDList := []string{"1", "2"}
	args := arguments.ArgumentList{IndividualQueryThreshold: 100}

	// Building the expected queries
	placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"
	currentQuery := fmt.Sprintf(queries.CurrentRunningQueriesSearch, placeholders)
	recentQuery := fmt.Sprintf(queries.RecentQueriesSearch, placeholders)
	extensiveQuery := fmt.Sprintf(queries.PastQueriesSearch, placeholders)

	queryArgs := make([]driver.Value, len(queryIDList))
	for i, id := range queryIDList {
		queryArgs[i] = id
	}
	queryArgs = append(queryArgs, args.IndividualQueryThreshold)

	// Escape special characters in the query strings for sqlmock
	escapedCurrentQuery := regexp.QuoteMeta(currentQuery)
	escapedRecentQuery := regexp.QuoteMeta(recentQuery)
	escapedExtensiveQuery := regexp.QuoteMeta(extensiveQuery)

	// Mocking the expected queries
	mock.ExpectQuery(escapedCurrentQuery).
		WithArgs(queryArgs...).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "query_sample_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
			AddRow("1", "SELECT * FROM table1", "SELECT * FROM table1", 123, 456, 789, 101112).
			AddRow("2", "SELECT * FROM table2", "SELECT * FROM table2", 124, 457, 790, 101113))

	mock.ExpectQuery(escapedRecentQuery).
		WithArgs(queryArgs...).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "query_sample_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
			AddRow("1", "SELECT * FROM table1", "SELECT * FROM table1", 123, 456, 789, 101112).
			AddRow("2", "SELECT * FROM table2", "SELECT * FROM table2", 124, 457, 790, 101113))

	mock.ExpectQuery(escapedExtensiveQuery).
		WithArgs(queryArgs...).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "query_sample_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
			AddRow("1", "SELECT * FROM table1", "SELECT * FROM table1", 123, 456, 789, 101112).
			AddRow("2", "SELECT * FROM table2", "SELECT * FROM table2", 124, 457, 790, 101113))

	// Initialize integration entity
	i, err := integration.New("test", "1.0.0")
	assert.NoError(t, err)
	e := i.LocalEntity()

	// Call the function
	queryList, err := PopulateIndividualQueryDetails(db, queryIDList, e, args)
	assert.NoError(t, err)

	// Log the queryList for debugging
	t.Logf("Query List: %+v", queryList)

	// Adjust the expectation to match the actual behavior
	assert.Len(t, queryList, 2)

	// Verify the metrics
	ms := e.Metrics[0]
	assert.Equal(t, "1", ms.Metrics["query_id"])
	assert.Equal(t, "SELECT * FROM table1", ms.Metrics["query_text"])
	assert.Equal(t, float64(123), ms.Metrics["event_id"])
	assert.Equal(t, float64(456), ms.Metrics["timer_wait"])
	assert.Equal(t, float64(789), ms.Metrics["rows_sent"])
	assert.Equal(t, float64(101112), ms.Metrics["rows_examined"])

	ms = e.Metrics[1]
	assert.Equal(t, "2", ms.Metrics["query_id"])
	assert.Equal(t, "SELECT * FROM table2", ms.Metrics["query_text"])
	assert.Equal(t, float64(124), ms.Metrics["event_id"])
	assert.Equal(t, float64(457), ms.Metrics["timer_wait"])
	assert.Equal(t, float64(790), ms.Metrics["rows_sent"])
	assert.Equal(t, float64(101113), ms.Metrics["rows_examined"])
}
func TestSetIndividualQueryMetrics(t *testing.T) {
	i, err := integration.New("test", "1.0.0")
	assert.NoError(t, err)

	e := i.LocalEntity()
	args := arguments.ArgumentList{}

	metrics := []performancedatamodel.QueryPlanMetrics{
		{
			QueryID:             "query_id_1",
			AnonymizedQueryText: "SELECT * FROM table",
			EventID:             123,
			ThreadID:            1331215,
			TimerWait:           456,
			RowsSent:            789,
			RowsExamined:        101112,
		},
	}

	err = setIndividualQueryMetrics(e, args, metrics)
	assert.NoError(t, err)

	ms := e.Metrics[0]
	assert.Equal(t, "query_id_1", ms.Metrics["query_id"])
	assert.Equal(t, "SELECT * FROM table", ms.Metrics["query_text"])
	assert.Equal(t, float64(123), ms.Metrics["event_id"])
	assert.Equal(t, float64(456), ms.Metrics["timer_wait"])
	assert.Equal(t, float64(789), ms.Metrics["rows_sent"])
	assert.Equal(t, float64(101112), ms.Metrics["rows_examined"])
}

func TestCurrentQueryMetrics(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockDB.Close()

	db := NewMockDataSource(sqlx.NewDb(mockDB, "sqlmock"))
	queryIDList := []string{"1", "2"}
	individualQueryThreshold := 100

	// Building the expected query
	placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"
	expectedQuery := fmt.Sprintf(queries.CurrentRunningQueriesSearch, placeholders)

	args := make([]driver.Value, len(queryIDList))
	for i, id := range queryIDList {
		args[i] = id
	}
	args = append(args, individualQueryThreshold)

	// Escape special characters in the query string for sqlmock
	escapedQuery := regexp.QuoteMeta(expectedQuery)

	mock.ExpectQuery(escapedQuery).
		WithArgs(args...).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "query_sample_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
			AddRow("1", "SELECT * FROM table1", "SELECT * FROM table1", 123, 456, 789, 101112).
			AddRow("2", "SELECT * FROM table2", "SELECT * FROM table2", 124, 457, 790, 101113))

	metrics, err := currentQueryMetrics(db, queryIDList, individualQueryThreshold)
	assert.NoError(t, err)
	assert.Len(t, metrics, 2)

	assert.Equal(t, "1", metrics[0].QueryID)
	assert.Equal(t, "SELECT * FROM table1", metrics[0].AnonymizedQueryText)
	assert.Equal(t, "SELECT * FROM table1", metrics[0].QueryText)
	assert.Equal(t, uint64(123), metrics[0].EventID)
	assert.Equal(t, float64(456), metrics[0].TimerWait)
	assert.Equal(t, int64(789), metrics[0].RowsSent)
	assert.Equal(t, int64(101112), metrics[0].RowsExamined)

	assert.Equal(t, "2", metrics[1].QueryID)
	assert.Equal(t, "SELECT * FROM table2", metrics[1].AnonymizedQueryText)
	assert.Equal(t, "SELECT * FROM table2", metrics[1].QueryText)
	assert.Equal(t, uint64(124), metrics[1].EventID)
	assert.Equal(t, float64(457), metrics[1].TimerWait)
	assert.Equal(t, int64(790), metrics[1].RowsSent)
	assert.Equal(t, int64(101113), metrics[1].RowsExamined)
}

func TestRecentQueryMetrics(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockDB.Close()

	db := NewMockDataSource(sqlx.NewDb(mockDB, "sqlmock"))
	queryIDList := []string{"1", "2"}
	individualQueryThreshold := 100

	// Building the expected query
	placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"
	expectedQuery := fmt.Sprintf(queries.RecentQueriesSearch, placeholders)

	args := make([]driver.Value, len(queryIDList))
	for i, id := range queryIDList {
		args[i] = id
	}
	args = append(args, individualQueryThreshold)

	// Escape special characters in the query string for sqlmock
	escapedQuery := regexp.QuoteMeta(expectedQuery)

	mock.ExpectQuery(escapedQuery).
		WithArgs(args...).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "query_sample_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
			AddRow("1", "SELECT * FROM table1", "SELECT * FROM table1", 123, 456, 789, 101112).
			AddRow("2", "SELECT * FROM table2", "SELECT * FROM table2", 124, 457, 790, 101113))

	metrics, err := recentQueryMetrics(db, queryIDList, individualQueryThreshold)
	assert.NoError(t, err)
	assert.Len(t, metrics, 2)

	assert.Equal(t, "1", metrics[0].QueryID)
	assert.Equal(t, "SELECT * FROM table1", metrics[0].AnonymizedQueryText)
	assert.Equal(t, "SELECT * FROM table1", metrics[0].QueryText)
	assert.Equal(t, uint64(123), metrics[0].EventID)
	assert.Equal(t, float64(456), metrics[0].TimerWait)
	assert.Equal(t, int64(789), metrics[0].RowsSent)
	assert.Equal(t, int64(101112), metrics[0].RowsExamined)

	assert.Equal(t, "2", metrics[1].QueryID)
	assert.Equal(t, "SELECT * FROM table2", metrics[1].AnonymizedQueryText)
	assert.Equal(t, "SELECT * FROM table2", metrics[1].QueryText)
	assert.Equal(t, uint64(124), metrics[1].EventID)
	assert.Equal(t, float64(457), metrics[1].TimerWait)
	assert.Equal(t, int64(790), metrics[1].RowsSent)
	assert.Equal(t, int64(101113), metrics[1].RowsExamined)
}

func TestExtensiveQueryMetrics(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockDB.Close()

	db := NewMockDataSource(sqlx.NewDb(mockDB, "sqlmock"))
	queryIDList := []string{"1", "2"}
	individualQueryThreshold := 100

	// Building the expected query
	placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"
	expectedQuery := fmt.Sprintf(queries.PastQueriesSearch, placeholders)

	args := make([]driver.Value, len(queryIDList))
	for i, id := range queryIDList {
		args[i] = id
	}
	args = append(args, individualQueryThreshold)

	// Escape special characters in the query string for sqlmock
	escapedQuery := regexp.QuoteMeta(expectedQuery)

	mock.ExpectQuery(escapedQuery).
		WithArgs(args...).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "query_sample_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
			AddRow("1", "SELECT * FROM table1", "SELECT * FROM table1", 123, 456, 789, 101112).
			AddRow("2", "SELECT * FROM table2", "SELECT * FROM table2", 124, 457, 790, 101113))

	metrics, err := extensiveQueryMetrics(db, queryIDList, individualQueryThreshold)
	assert.NoError(t, err)
	assert.Len(t, metrics, 2)

	assert.Equal(t, "1", metrics[0].QueryID)
	assert.Equal(t, "SELECT * FROM table1", metrics[0].AnonymizedQueryText)
	assert.Equal(t, "SELECT * FROM table1", metrics[0].QueryText)
	assert.Equal(t, uint64(123), metrics[0].EventID)
	assert.Equal(t, float64(456), metrics[0].TimerWait)
	assert.Equal(t, int64(789), metrics[0].RowsSent)
	assert.Equal(t, int64(101112), metrics[0].RowsExamined)

	assert.Equal(t, "2", metrics[1].QueryID)
	assert.Equal(t, "SELECT * FROM table2", metrics[1].AnonymizedQueryText)
	assert.Equal(t, "SELECT * FROM table2", metrics[1].QueryText)
	assert.Equal(t, uint64(124), metrics[1].EventID)
	assert.Equal(t, float64(457), metrics[1].TimerWait)
	assert.Equal(t, int64(790), metrics[1].RowsSent)
	assert.Equal(t, int64(101113), metrics[1].RowsExamined)
}
func TestCollectCurrentQueryMetrics(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockDB.Close()

	db := NewMockDataSource(sqlx.NewDb(mockDB, "sqlmock"))
	queryIDList := []string{"1", "2"}
	individualQueryThreshold := 100

	// Building the expected query
	placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"
	expectedQuery := fmt.Sprintf(queries.CurrentRunningQueriesSearch, placeholders)

	args := make([]driver.Value, len(queryIDList))
	for i, id := range queryIDList {
		args[i] = id
	}
	args = append(args, individualQueryThreshold)

	// Escape special characters in the query string for sqlmock
	escapedQuery := regexp.QuoteMeta(expectedQuery)

	mock.ExpectQuery(escapedQuery).
		WithArgs(args...).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
			AddRow("1", "SELECT * FROM table1", 123, 456, 789, 101112).
			AddRow("2", "SELECT * FROM table2", 124, 457, 790, 101113))

	metrics, err := collectCurrentQueryMetrics(db, queryIDList, individualQueryThreshold)
	assert.NoError(t, err)
	assert.Len(t, metrics, 2)

	assert.Equal(t, "1", metrics[0].QueryID)
	assert.Equal(t, "SELECT * FROM table1", metrics[0].AnonymizedQueryText)
	assert.Equal(t, uint64(123), metrics[0].EventID)
	assert.Equal(t, float64(456), metrics[0].TimerWait)
	assert.Equal(t, int64(789), metrics[0].RowsSent)
	assert.Equal(t, int64(101112), metrics[0].RowsExamined)

	assert.Equal(t, "2", metrics[1].QueryID)
	assert.Equal(t, "SELECT * FROM table2", metrics[1].AnonymizedQueryText)
	assert.Equal(t, uint64(124), metrics[1].EventID)
	assert.Equal(t, float64(457), metrics[1].TimerWait)
	assert.Equal(t, int64(790), metrics[1].RowsSent)
	assert.Equal(t, int64(101113), metrics[1].RowsExamined)
}

func TestCollectRecentQueryMetrics(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockDB.Close()

	db := NewMockDataSource(sqlx.NewDb(mockDB, "sqlmock"))
	queryIDList := []string{"1", "2"}
	individualQueryThreshold := 100

	// Building the expected query
	placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"
	expectedQuery := fmt.Sprintf(queries.RecentQueriesSearch, placeholders)

	args := make([]driver.Value, len(queryIDList))
	for i, id := range queryIDList {
		args[i] = id
	}
	args = append(args, individualQueryThreshold)

	// Escape special characters in the query string for sqlmock
	escapedQuery := regexp.QuoteMeta(expectedQuery)

	mock.ExpectQuery(escapedQuery).
		WithArgs(args...).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "query_sample_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
			AddRow("1", "SELECT * FROM table1", "SELECT * FROM table1", 123, 456, 789, 101112).
			AddRow("2", "SELECT * FROM table2", "SELECT * FROM table2", 124, 457, 790, 101113))

	metrics, err := collectRecentQueryMetrics(db, queryIDList, individualQueryThreshold)
	assert.NoError(t, err)
	assert.Len(t, metrics, 2)

	assert.Equal(t, "1", metrics[0].QueryID)
	assert.Equal(t, "SELECT * FROM table1", metrics[0].AnonymizedQueryText)
	assert.Equal(t, "SELECT * FROM table1", metrics[0].QueryText)
	assert.Equal(t, uint64(123), metrics[0].EventID)
	assert.Equal(t, float64(456), metrics[0].TimerWait)
	assert.Equal(t, int64(789), metrics[0].RowsSent)
	assert.Equal(t, int64(101112), metrics[0].RowsExamined)

	assert.Equal(t, "2", metrics[1].QueryID)
	assert.Equal(t, "SELECT * FROM table2", metrics[1].AnonymizedQueryText)
	assert.Equal(t, "SELECT * FROM table2", metrics[1].QueryText)
	assert.Equal(t, uint64(124), metrics[1].EventID)
	assert.Equal(t, float64(457), metrics[1].TimerWait)
	assert.Equal(t, int64(790), metrics[1].RowsSent)
	assert.Equal(t, int64(101113), metrics[1].RowsExamined)
}

func TestCollectExtensiveQueryMetrics(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockDB.Close()

	db := NewMockDataSource(sqlx.NewDb(mockDB, "sqlmock"))
	individualQueryThreshold := 100

	t.Run("empty queryIDList", func(t *testing.T) {
		queryIDList := []string{}
		metrics, err := collectExtensiveQueryMetrics(db, queryIDList, individualQueryThreshold)
		assert.NoError(t, err)
		assert.Nil(t, metrics)
	})

	t.Run("query execution failure", func(t *testing.T) {
		queryIDList := []string{"1", "2"}

		// Building the expected query
		placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"
		expectedQuery := fmt.Sprintf(queries.PastQueriesSearch, placeholders)

		args := make([]driver.Value, len(queryIDList))
		for i, id := range queryIDList {
			args[i] = id
		}
		args = append(args, individualQueryThreshold)

		// Escape special characters in the query string for sqlmock
		escapedQuery := regexp.QuoteMeta(expectedQuery)

		mock.ExpectQuery(escapedQuery).
			WithArgs(args...).
			WillReturnError(fmt.Errorf("query execution failed"))

		metrics, err := collectExtensiveQueryMetrics(db, queryIDList, individualQueryThreshold)
		assert.Error(t, err)
		assert.Nil(t, metrics)
	})

	t.Run("row scan failure", func(t *testing.T) {
		queryIDList := []string{"1", "2"}

		// Building the expected query
		placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"
		expectedQuery := fmt.Sprintf(queries.PastQueriesSearch, placeholders)

		args := make([]driver.Value, len(queryIDList))
		for i, id := range queryIDList {
			args[i] = id
		}
		args = append(args, individualQueryThreshold)

		// Escape special characters in the query string for sqlmock
		escapedQuery := regexp.QuoteMeta(expectedQuery)

		mock.ExpectQuery(escapedQuery).
			WithArgs(args...).
			WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "query_sample_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
				AddRow("1", "SELECT * FROM table1", "SELECT * FROM table1", 123, 456, 789, 101112).
				RowError(0, fmt.Errorf("row scan error")))

		metrics, err := collectExtensiveQueryMetrics(db, queryIDList, individualQueryThreshold)
		assert.Error(t, err)
		assert.Nil(t, metrics)
	})

	t.Run("error iterating over rows", func(t *testing.T) {
		queryIDList := []string{"1", "2"}

		// Building the expected query
		placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"
		expectedQuery := fmt.Sprintf(queries.PastQueriesSearch, placeholders)

		args := make([]driver.Value, len(queryIDList))
		for i, id := range queryIDList {
			args[i] = id
		}
		args = append(args, individualQueryThreshold)

		// Escape special characters in the query string for sqlmock
		escapedQuery := regexp.QuoteMeta(expectedQuery)

		mock.ExpectQuery(escapedQuery).
			WithArgs(args...).
			WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "query_sample_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
				AddRow("1", "SELECT * FROM table1", "SELECT * FROM table1", 123, 456, 789, 101112).
				AddRow("2", "SELECT * FROM table2", "SELECT * FROM table2", 124, 457, 790, 101113).
				RowError(1, fmt.Errorf("row iteration error")))

		metrics, err := collectExtensiveQueryMetrics(db, queryIDList, individualQueryThreshold)
		assert.Error(t, err)
		assert.Nil(t, metrics)
	})

	t.Run("successful query execution", func(t *testing.T) {
		queryIDList := []string{"1", "2"}

		// Building the expected query
		placeholders := strings.Repeat("?, ", len(queryIDList)-1) + "?"
		expectedQuery := fmt.Sprintf(queries.PastQueriesSearch, placeholders)

		args := make([]driver.Value, len(queryIDList))
		for i, id := range queryIDList {
			args[i] = id
		}
		args = append(args, individualQueryThreshold)

		// Escape special characters in the query string for sqlmock
		escapedQuery := regexp.QuoteMeta(expectedQuery)

		mock.ExpectQuery(escapedQuery).
			WithArgs(args...).
			WillReturnRows(sqlmock.NewRows([]string{"query_id", "query_text", "query_sample_text", "event_id", "timer_wait", "rows_sent", "rows_examined"}).
				AddRow("1", "SELECT * FROM table1", "SELECT * FROM table1", 123, 456, 789, 101112).
				AddRow("2", "SELECT * FROM table2", "SELECT * FROM table2", 124, 457, 790, 101113))

		metrics, err := collectExtensiveQueryMetrics(db, queryIDList, individualQueryThreshold)
		assert.NoError(t, err)
		assert.Len(t, metrics, 2)

		assert.Equal(t, "1", metrics[0].QueryID)
		assert.Equal(t, "SELECT * FROM table1", metrics[0].AnonymizedQueryText)
		assert.Equal(t, "SELECT * FROM table1", metrics[0].QueryText)
		assert.Equal(t, uint64(123), metrics[0].EventID)
		assert.Equal(t, float64(456), metrics[0].TimerWait)
		assert.Equal(t, int64(789), metrics[0].RowsSent)
		assert.Equal(t, int64(101112), metrics[0].RowsExamined)

		assert.Equal(t, "2", metrics[1].QueryID)
		assert.Equal(t, "SELECT * FROM table2", metrics[1].AnonymizedQueryText)
		assert.Equal(t, "SELECT * FROM table2", metrics[1].QueryText)
		assert.Equal(t, uint64(124), metrics[1].EventID)
		assert.Equal(t, float64(457), metrics[1].TimerWait)
		assert.Equal(t, int64(790), metrics[1].RowsSent)
		assert.Equal(t, int64(101113), metrics[1].RowsExamined)
	})
}
