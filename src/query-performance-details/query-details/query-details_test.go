package query_details

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/nri-mysql/src/args"
	arguments "github.com/newrelic/nri-mysql/src/args"

	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	performancedatamodel "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockDataSource is a mock implementation of the DataSource interface
type MockDataSource struct {
	mock.Mock
	db                          *sqlx.DB
	SetExecutionPlanMetricsFunc func(i *integration.Integration, args arguments.ArgumentList, metrics []performance_data_model.QueryPlanMetrics) error
}

// MockCommonUtils is a mock implementation of the CommonUtils interface
type MockCommonUtils struct {
	mock.Mock
}

func (m *MockCommonUtils) IngestMetric(metrics []interface{}, metricName string, i *integration.Integration, args args.ArgumentList) error {
	m.Called(metrics, metricName, i, args)
	return nil
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

func stringPtr(s string) *string {
	return &s
}
func uint64Ptr(i uint64) *uint64 {
	return &i
}

func float64Ptr(f float64) *float64 {
	return &f
}

func TestCollectGroupedSlowQueryMetrics(t *testing.T) {
	tests := []struct {
		name                string
		fetchInterval       int
		queryCountThreshold int
		mockSetup           func(mock sqlmock.Sqlmock)
		expectedMetrics     []performancedatamodel.SlowQueryMetrics
		expectedQueryIDList []string
		expectedError       error
	}{
		{
			name:                "Successful collection of slow query metrics",
			fetchInterval:       60,
			queryCountThreshold: 10,
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"query_id", "query_text"}).
					AddRow("1", "SELECT * FROM table1").
					AddRow("2", "SELECT * FROM table2")
				mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_summary_by_digest WHERE .*").
					WithArgs(60, 10).
					WillReturnRows(rows)
			},
			expectedMetrics: []performancedatamodel.SlowQueryMetrics{
				{QueryID: stringPtr("1"), QueryText: stringPtr("SELECT * FROM table1")},
				{QueryID: stringPtr("2"), QueryText: stringPtr("SELECT * FROM table2")},
			},
			expectedQueryIDList: []string{"1", "2"},
			expectedError:       nil,
		},
		{
			name:                "Failure to execute the query",
			fetchInterval:       60,
			queryCountThreshold: 10,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_summary_by_digest WHERE .*").
					WithArgs(60, 10).
					WillReturnError(errors.New("query failed"))
			},
			expectedMetrics:     nil,
			expectedQueryIDList: []string{},
			expectedError:       errors.New("query failed"),
		},
		{
			name:                "Failure to scan a row",
			fetchInterval:       60,
			queryCountThreshold: 10,
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"query_id", "query_text"}).
					AddRow("1", "SELECT * FROM table1").
					AddRow("2", "SELECT * FROM table2").
					RowError(1, errors.New("scan error"))
				mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_summary_by_digest WHERE .*").
					WithArgs(60, 10).
					WillReturnRows(rows)
			},
			expectedMetrics:     nil,
			expectedQueryIDList: []string{},
			expectedError:       errors.New("scan error"),
		},
		{
			name:                "Error iterating over rows",
			fetchInterval:       60,
			queryCountThreshold: 10,
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"query_id", "query_text"}).
					AddRow("1", "SELECT * FROM table1").
					AddRow("2", "SELECT * FROM table2")
				mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_summary_by_digest WHERE .*").
					WithArgs(60, 10).
					WillReturnRows(rows).
					WillReturnError(errors.New("iteration error"))
			},
			expectedMetrics:     nil,
			expectedQueryIDList: []string{},
			expectedError:       errors.New("iteration error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			db := sqlx.NewDb(sqlDB, "sqlmock")
			defer db.Close()

			tt.mockSetup(mock)

			mockDataSource := NewMockDataSource(db)
			defer db.Close()
			metrics, queryIDList, err := collectGroupedSlowQueryMetrics(mockDataSource, tt.fetchInterval, tt.queryCountThreshold)
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expectedMetrics, metrics)
			assert.Equal(t, tt.expectedQueryIDList, queryIDList)
		})
	}
}

func TestCurrentQueryMetrics(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	db := sqlx.NewDb(sqlDB, "sqlmock")
	mockDataSource := NewMockDataSource(db)

	queryIDList := []string{"1", "2", "3"}
	queryResponseTimeThreshold := 100
	queryCountThreshold := 10
	expectedMetrics := []performancedatamodel.IndividualQueryMetrics{
		{QueryID: stringPtr("1"), ExecutionTimeMs: float64Ptr(50)},
		{QueryID: stringPtr("2"), ExecutionTimeMs: float64Ptr(60)},
	}

	query := `SELECT DIGEST AS query_id, CASE WHEN CHAR_LENGTH(DIGEST_TEXT) > 4000 THEN CONCAT(LEFT(DIGEST_TEXT, 3997), '...') ELSE DIGEST_TEXT END AS query_text, SQL_TEXT AS query_sample_text, EVENT_ID AS event_id, THREAD_ID AS thread_id, ROUND(TIMER_WAIT / 1000000000, 3) AS execution_time_ms, ROWS_SENT AS rows_sent, ROWS_EXAMINED AS rows_examined, CURRENT_SCHEMA AS database_name FROM performance_schema.events_statements_current WHERE DIGEST IN (?, ?, ?) AND CURRENT_SCHEMA IS NOT NULL AND CURRENT_SCHEMA NOT IN ('', 'mysql', 'performance_schema', 'information_schema', 'sys') AND SQL_TEXT RLIKE '^(SELECT|INSERT|UPDATE|DELETE|WITH)' AND SQL_TEXT NOT LIKE '%DIGEST_TEXT%' AND TIMER_WAIT / 1000000000 > ? ORDER BY TIMER_WAIT DESC LIMIT ?;`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], queryResponseTimeThreshold, queryCountThreshold).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "execution_time_ms"}).
			AddRow("1", 50).
			AddRow("2", 60))

	metrics, err := currentQueryMetrics(mockDataSource, queryIDList, queryResponseTimeThreshold, queryCountThreshold)
	assert.NoError(t, err)
	assert.Equal(t, expectedMetrics, metrics)
}

func TestRecentQueryMetrics(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	db := sqlx.NewDb(sqlDB, "sqlmock")
	mockDataSource := NewMockDataSource(db)

	queryIDList := []string{"1", "2", "3"}
	queryResponseTimeThreshold := 100
	queryCountThreshold := 10
	expectedMetrics := []performancedatamodel.IndividualQueryMetrics{
		{QueryID: stringPtr("1"), ExecutionTimeMs: float64Ptr(50)},
		{QueryID: stringPtr("2"), ExecutionTimeMs: float64Ptr(60)},
	}

	query := `SELECT DIGEST AS query_id, CASE WHEN CHAR_LENGTH(DIGEST_TEXT) > 4000 THEN CONCAT(LEFT(DIGEST_TEXT, 3997), '...') ELSE DIGEST_TEXT END AS query_text, SQL_TEXT AS query_sample_text, EVENT_ID AS event_id, THREAD_ID AS thread_id, ROUND(TIMER_WAIT / 1000000000, 3) AS execution_time_ms, ROWS_SENT AS rows_sent, ROWS_EXAMINED AS rows_examined, CURRENT_SCHEMA AS database_name FROM performance_schema.events_statements_history WHERE DIGEST IN (?, ?, ?) AND CURRENT_SCHEMA IS NOT NULL AND CURRENT_SCHEMA NOT IN ('', 'mysql', 'performance_schema', 'information_schema', 'sys') AND SQL_TEXT RLIKE '^(SELECT|INSERT|UPDATE|DELETE|WITH)' AND SQL_TEXT NOT LIKE '%DIGEST_TEXT%' AND TIMER_WAIT / 1000000000 > ? ORDER BY TIMER_WAIT DESC LIMIT ?;`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], queryResponseTimeThreshold, queryCountThreshold).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "execution_time_ms"}).
			AddRow("1", 50).
			AddRow("2", 60))

	metrics, err := recentQueryMetrics(mockDataSource, queryIDList, queryResponseTimeThreshold, queryCountThreshold)
	assert.NoError(t, err)
	assert.Equal(t, expectedMetrics, metrics)
}

func TestExtensiveQueryMetrics(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	db := sqlx.NewDb(sqlDB, "sqlmock")
	mockDataSource := NewMockDataSource(db)

	queryIDList := []string{"1", "2", "3"}
	queryResponseTimeThreshold := 100
	queryCountThreshold := 10
	expectedMetrics := []performancedatamodel.IndividualQueryMetrics{
		{QueryID: stringPtr("1"), ExecutionTimeMs: float64Ptr(50)},
		{QueryID: stringPtr("2"), ExecutionTimeMs: float64Ptr(60)},
	}

	query := `SELECT DIGEST AS query_id, CASE WHEN CHAR_LENGTH(DIGEST_TEXT) > 4000 THEN CONCAT(LEFT(DIGEST_TEXT, 3997), '...') ELSE DIGEST_TEXT END AS query_text, SQL_TEXT AS query_sample_text, EVENT_ID AS event_id, THREAD_ID AS thread_id, ROUND(TIMER_WAIT / 1000000000, 3) AS execution_time_ms, ROWS_SENT AS rows_sent, ROWS_EXAMINED AS rows_examined, CURRENT_SCHEMA AS database_name FROM performance_schema.events_statements_history_long WHERE DIGEST IN (?, ?, ?) AND CURRENT_SCHEMA IS NOT NULL AND CURRENT_SCHEMA NOT IN ('', 'mysql', 'performance_schema', 'information_schema', 'sys') AND SQL_TEXT RLIKE '^(SELECT|INSERT|UPDATE|DELETE|WITH)' AND SQL_TEXT NOT LIKE '%DIGEST_TEXT%' AND TIMER_WAIT / 1000000000 > ? ORDER BY TIMER_WAIT DESC LIMIT ?;`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], queryResponseTimeThreshold, queryCountThreshold).
		WillReturnRows(sqlmock.NewRows([]string{"query_id", "execution_time_ms"}).
			AddRow("1", 50).
			AddRow("2", 60))

	metrics, err := extensiveQueryMetrics(mockDataSource, queryIDList, queryResponseTimeThreshold, queryCountThreshold)
	assert.NoError(t, err)
	assert.Equal(t, expectedMetrics, metrics)
}

func TestQueryMetrics_Error(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	db := sqlx.NewDb(sqlDB, "sqlmock")
	mockDataSource := NewMockDataSource(db)

	queryIDList := []string{"1", "2", "3"}
	queryResponseTimeThreshold := 100
	queryCountThreshold := 10
	expectedError := errors.New("some error")

	mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_current WHERE .*").
		WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], queryResponseTimeThreshold, queryCountThreshold).
		WillReturnError(expectedError)

	_, err = currentQueryMetrics(mockDataSource, queryIDList, queryResponseTimeThreshold, queryCountThreshold)
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)

	mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_history WHERE .*").
		WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], queryResponseTimeThreshold, queryCountThreshold).
		WillReturnError(expectedError)

	_, err = recentQueryMetrics(mockDataSource, queryIDList, queryResponseTimeThreshold, queryCountThreshold)
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)

	mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_history_long WHERE .*").
		WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], queryResponseTimeThreshold, queryCountThreshold).
		WillReturnError(expectedError)

	_, err = extensiveQueryMetrics(mockDataSource, queryIDList, queryResponseTimeThreshold, queryCountThreshold)
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
}

func TestPopulateIndividualQueryDetails(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	db := sqlx.NewDb(sqlDB, "sqlmock")
	mockDataSource := NewMockDataSource(db)

	queryIDList := []string{"1", "2", "3"}
	args := arguments.ArgumentList{
		QueryResponseTimeThreshold: 100,
		QueryCountThreshold:        10,
	}
	i, err := integration.New("test", "1.0.0")
	require.NoError(t, err)
	e, err := i.Entity("testEntity", "testType")
	require.NoError(t, err)

	t.Run("Successful execution", func(t *testing.T) {
		mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_current WHERE .*").
			WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], args.QueryResponseTimeThreshold, args.QueryCountThreshold).
			WillReturnRows(sqlmock.NewRows([]string{"query_id", "execution_time_ms", "event_id", "database_name"}).
				AddRow("1", 50, uint64(1), "db1").
				AddRow("2", 60, uint64(2), "db2"))

		mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_history WHERE .*").
			WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], args.QueryResponseTimeThreshold, args.QueryCountThreshold).
			WillReturnRows(sqlmock.NewRows([]string{"query_id", "execution_time_ms", "event_id", "database_name"}).
				AddRow("1", 50, uint64(1), "db1").
				AddRow("2", 60, uint64(2), "db2"))

		mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_history_long WHERE .*").
			WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], args.QueryResponseTimeThreshold, args.QueryCountThreshold).
			WillReturnRows(sqlmock.NewRows([]string{"query_id", "execution_time_ms", "event_id", "database_name"}).
				AddRow("1", 50, uint64(1), "db1").
				AddRow("2", 60, uint64(2), "db2"))

		groupedQueries, err := PopulateIndividualQueryDetails(mockDataSource, queryIDList, i, e, args)
		assert.NoError(t, err)
		assert.NotNil(t, groupedQueries)
	})

	t.Run("Error in currentQueryMetrics", func(t *testing.T) {
		expectedError := errors.New("current query metrics error")

		mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_current WHERE .*").
			WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], args.QueryResponseTimeThreshold, args.QueryCountThreshold).
			WillReturnError(expectedError)

		groupedQueries, err := PopulateIndividualQueryDetails(mockDataSource, queryIDList, i, e, args)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, groupedQueries)
	})

	t.Run("Error in recentQueryMetrics", func(t *testing.T) {
		expectedError := errors.New("recent query metrics error")

		mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_current WHERE .*").
			WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], args.QueryResponseTimeThreshold, args.QueryCountThreshold).
			WillReturnRows(sqlmock.NewRows([]string{"query_id", "execution_time_ms", "event_id", "database_name"}).
				AddRow("1", 50, uint64(1), "db1").
				AddRow("2", 60, uint64(2), "db2"))

		mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_history WHERE .*").
			WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], args.QueryResponseTimeThreshold, args.QueryCountThreshold).
			WillReturnError(expectedError)

		groupedQueries, err := PopulateIndividualQueryDetails(mockDataSource, queryIDList, i, e, args)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, groupedQueries)
	})

	t.Run("Error in extensiveQueryMetrics", func(t *testing.T) {
		expectedError := errors.New("extensive query metrics error")

		mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_current WHERE .*").
			WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], args.QueryResponseTimeThreshold, args.QueryCountThreshold).
			WillReturnRows(sqlmock.NewRows([]string{"query_id", "execution_time_ms", "event_id", "database_name"}).
				AddRow("1", 50, uint64(1), "db1").
				AddRow("2", 60, uint64(2), "db2"))

		mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_history WHERE .*").
			WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], args.QueryResponseTimeThreshold, args.QueryCountThreshold).
			WillReturnRows(sqlmock.NewRows([]string{"query_id", "execution_time_ms", "event_id", "database_name"}).
				AddRow("1", 50, uint64(1), "db1").
				AddRow("2", 60, uint64(2), "db2"))

		mock.ExpectQuery("SELECT .* FROM performance_schema.events_statements_history_long WHERE .*").
			WithArgs(queryIDList[0], queryIDList[1], queryIDList[2], args.QueryResponseTimeThreshold, args.QueryCountThreshold).
			WillReturnError(expectedError)

		groupedQueries, err := PopulateIndividualQueryDetails(mockDataSource, queryIDList, i, e, args)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, groupedQueries)
	})
}

func TestGetUniqueQueryList(t *testing.T) {
	tests := []struct {
		name         string
		queryList    []performancedatamodel.IndividualQueryMetrics
		expectedList []performancedatamodel.IndividualQueryMetrics
	}{
		{
			name:         "Empty list",
			queryList:    []performancedatamodel.IndividualQueryMetrics{},
			expectedList: []performancedatamodel.IndividualQueryMetrics{},
		},
		{
			name: "No duplicates",
			queryList: []performancedatamodel.IndividualQueryMetrics{
				{EventID: uint64Ptr(1)},
				{EventID: uint64Ptr(2)},
				{EventID: uint64Ptr(3)},
			},
			expectedList: []performancedatamodel.IndividualQueryMetrics{
				{EventID: uint64Ptr(1)},
				{EventID: uint64Ptr(2)},
				{EventID: uint64Ptr(3)},
			},
		},
		{
			name: "Some duplicates",
			queryList: []performancedatamodel.IndividualQueryMetrics{
				{EventID: uint64Ptr(1)},
				{EventID: uint64Ptr(2)},
				{EventID: uint64Ptr(1)},
				{EventID: uint64Ptr(3)},
				{EventID: uint64Ptr(2)},
			},
			expectedList: []performancedatamodel.IndividualQueryMetrics{
				{EventID: uint64Ptr(1)},
				{EventID: uint64Ptr(2)},
				{EventID: uint64Ptr(3)},
			},
		},
		{
			name: "All duplicates",
			queryList: []performancedatamodel.IndividualQueryMetrics{
				{EventID: uint64Ptr(1)},
				{EventID: uint64Ptr(1)},
				{EventID: uint64Ptr(1)},
			},
			expectedList: []performancedatamodel.IndividualQueryMetrics{
				{EventID: uint64Ptr(1)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getUniqueQueryList(tt.queryList)
			if result == nil {
				result = []performancedatamodel.IndividualQueryMetrics{}
			}
			assert.Equal(t, tt.expectedList, result)
		})
	}
}

func TestGroupQueriesByDatabase(t *testing.T) {
	tests := []struct {
		name         string
		filteredList []performancedatamodel.IndividualQueryMetrics
		expectedList []performancedatamodel.QueryGroup
	}{
		{
			name:         "Empty list",
			filteredList: []performancedatamodel.IndividualQueryMetrics{},
			expectedList: []performancedatamodel.QueryGroup{},
		},
		{
			name: "Single database",
			filteredList: []performancedatamodel.IndividualQueryMetrics{
				{DatabaseName: stringPtr("db1")},
				{DatabaseName: stringPtr("db1")},
				{DatabaseName: stringPtr("db1")},
			},
			expectedList: []performancedatamodel.QueryGroup{
				{
					Database: "db1",
					Queries: []performancedatamodel.IndividualQueryMetrics{
						{DatabaseName: stringPtr("db1")},
						{DatabaseName: stringPtr("db1")},
						{DatabaseName: stringPtr("db1")},
					},
				},
			},
		},
		{
			name: "Multiple databases",
			filteredList: []performancedatamodel.IndividualQueryMetrics{
				{DatabaseName: stringPtr("db1")},
				{DatabaseName: stringPtr("db2")},
				{DatabaseName: stringPtr("db1")},
				{DatabaseName: stringPtr("db2")},
				{DatabaseName: stringPtr("db1")},
			},
			expectedList: []performancedatamodel.QueryGroup{
				{
					Database: "db1",
					Queries: []performancedatamodel.IndividualQueryMetrics{
						{DatabaseName: stringPtr("db1")},
						{DatabaseName: stringPtr("db1")},
						{DatabaseName: stringPtr("db1")},
					},
				},
				{
					Database: "db2",
					Queries: []performancedatamodel.IndividualQueryMetrics{
						{DatabaseName: stringPtr("db2")},
						{DatabaseName: stringPtr("db2")},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := groupQueriesByDatabase(tt.filteredList)
			if result == nil {
				result = []performancedatamodel.QueryGroup{}
			}
			assert.Equal(t, tt.expectedList, result)
		})
	}
}
func TestCollectIndividualQueryMetrics(t *testing.T) {
	tests := []struct {
		name                       string
		queryIDList                []string
		queryString                string
		queryResponseTimeThreshold int
		queryCountThreshold        int
		mockSetup                  func(mock sqlmock.Sqlmock)
		expectedMetrics            []performancedatamodel.IndividualQueryMetrics
		expectedError              error
	}{
		{
			name:                       "Empty queryIDList",
			queryIDList:                []string{},
			queryString:                "SELECT * FROM performance_schema WHERE query_id IN (%s)",
			queryResponseTimeThreshold: 100,
			queryCountThreshold:        10,
			mockSetup:                  func(mock sqlmock.Sqlmock) {},
			expectedMetrics:            nil,
			expectedError:              nil,
		},
		{
			name:                       "Database query fails",
			queryIDList:                []string{"1", "2"},
			queryString:                "SELECT * FROM performance_schema WHERE query_id IN (%s)",
			queryResponseTimeThreshold: 100,
			queryCountThreshold:        10,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT \\* FROM performance_schema WHERE query_id IN \\(\\?, \\?\\)").
					WithArgs("1", "2", 100, 10).
					WillReturnError(errors.New("query failed"))
			},
			expectedMetrics: nil,
			expectedError:   errors.New("query failed"),
		},
		{
			name:                       "Successful query",
			queryIDList:                []string{"1", "2"},
			queryString:                "SELECT * FROM performance_schema WHERE query_id IN (%s)",
			queryResponseTimeThreshold: 100,
			queryCountThreshold:        10,
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"query_id", "query_text"}).
					AddRow("1", "metric1").
					AddRow("2", "metric2")
				mock.ExpectQuery("SELECT \\* FROM performance_schema WHERE query_id IN \\(\\?, \\?\\)").
					WithArgs("1", "2", 100, 10).
					WillReturnRows(rows)
			},
			expectedMetrics: []performancedatamodel.IndividualQueryMetrics{
				{QueryID: stringPtr("1"), AnonymizedQueryText: stringPtr("metric1"), QueryText: nil},
				{QueryID: stringPtr("2"), AnonymizedQueryText: stringPtr("metric2"), QueryText: nil},
			},
			expectedError: nil,
		},
		{
			name:                       "StructScan fails",
			queryIDList:                []string{"1", "2"},
			queryString:                "SELECT * FROM performance_schema WHERE query_id IN (%s)",
			queryResponseTimeThreshold: 100,
			queryCountThreshold:        10,
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"query_id", "query_text"}).
					AddRow("1", "metric1").
					AddRow("2", "metric2").
					RowError(0, errors.New("sql: Scan error on column index 0, name \"query_id\": converting driver.Value type string (\"metric1\") to a int: invalid syntax"))
				mock.ExpectQuery("SELECT \\* FROM performance_schema WHERE query_id IN \\(\\?, \\?\\)").
					WithArgs("1", "2", 100, 10).
					WillReturnRows(rows)
			},
			expectedMetrics: nil,
			expectedError:   errors.New("sql: Scan error on column index 0, name \"query_id\": converting driver.Value type string (\"metric1\") to a int: invalid syntax"),
		},
		{
			name:                       "Rows error",
			queryIDList:                []string{"1", "2"},
			queryString:                "SELECT * FROM performance_schema WHERE query_id IN (%s)",
			queryResponseTimeThreshold: 100,
			queryCountThreshold:        10,
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"query_id", "query_text"}).
					AddRow("1", "metric1").
					AddRow("2", "metric2").
					RowError(1, errors.New("row error"))
				mock.ExpectQuery("SELECT \\* FROM performance_schema WHERE query_id IN \\(\\?, \\?\\)").
					WithArgs("1", "2", 100, 10).
					WillReturnRows(rows)
			},
			expectedMetrics: nil,
			expectedError:   errors.New("row error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			db := sqlx.NewDb(sqlDB, "sqlmock")
			require.NoError(t, err)
			defer db.Close()

			tt.mockSetup(mock)

			mockDataSource := NewMockDataSource(db)
			metrics, err := collectIndividualQueryMetrics(mockDataSource, tt.queryIDList, tt.queryString, tt.queryResponseTimeThreshold, tt.queryCountThreshold)
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expectedMetrics, metrics)
		})
	}
}
