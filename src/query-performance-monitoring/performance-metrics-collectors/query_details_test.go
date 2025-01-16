package performancemetricscollectors

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	arguments "github.com/newrelic/nri-mysql/src/args"
	"github.com/newrelic/nri-mysql/src/query-performance-monitoring/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func NewMockDataSource(db *sqlx.DB) *MockDataSource {
	return &MockDataSource{db: db}
}

func (m *MockDataSource) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	arguments := m.Called(query, args)
	return arguments.Get(0).([]map[string]interface{}), arguments.Error(1)
}

// func float64Ptr(f float64) *float64 {
// 	return &f
// }

func stringPtr(s string) *string {
	return &s
}

var (
	mockCollectIndividualQueryMetrics func(db utils.DataSource, queryIDList []string, searchType string, args arguments.ArgumentList) ([]utils.IndividualQueryMetrics, error)
	// mockCollectMetrics                 func(db utils.DataSource, query string, args ...interface{}) ([]utils.IndividualQueryMetrics, error)
	mockCollectGroupedSlowQueryMetrics func(db utils.DataSource, fetchInterval int, queryCountThreshold int, excludedDatabases []string) ([]utils.IndividualQueryMetrics, []string, error)
	mockSetSlowQueryMetrics            func(i *integration.Integration, rawMetrics []map[string]interface{}, args arguments.ArgumentList) error
)
var (
	errEmptySlice = errors.New("empty slice passed to 'in' query")
)

func TestCollectGroupedSlowQueryMetrics(t *testing.T) {
	tests := []struct {
		name                string
		fetchInterval       int
		queryCountThreshold int
		mockSetup           func(mock sqlmock.Sqlmock)
		expectedMetrics     []utils.SlowQueryMetrics
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
			expectedMetrics: []utils.SlowQueryMetrics{
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
					WillReturnError(errEmptySlice)
			},
			expectedMetrics:     nil,
			expectedQueryIDList: []string{},
			expectedError:       errEmptySlice,
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
			metrics, queryIDList, err := collectGroupedSlowQueryMetrics(mockDataSource, tt.fetchInterval, tt.queryCountThreshold, []string{})
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
				assert.Nil(t, metrics)
				assert.NotNil(t, queryIDList)
			} else {
				assert.Error(t, err)
				assert.NotEqual(t, tt.expectedMetrics, metrics)
				assert.NotEqual(t, tt.expectedQueryIDList, queryIDList)
			}
		})
	}
}

func TestSetSlowQueryMetrics(t *testing.T) {
	mockIntegration := new(MockIntegration)
	mockIntegration.Integration, _ = integration.New("test", "1.0.0") // Properly initialize the Integration field
	mockArgs := arguments.ArgumentList{}

	t.Run("Successful Ingestion", func(t *testing.T) {
		metrics := []utils.SlowQueryMetrics{
			{QueryID: stringPtr("1"), QueryText: stringPtr("SELECT * FROM table1")},
		}
		mockIntegration.On("IngestMetric", mock.Anything, "MysqlSlowQuerySample", mockIntegration, mockArgs).Return(nil)

		err := setSlowQueryMetrics(mockIntegration.Integration, metrics, mockArgs)
		assert.NoError(t, err)
	})

	t.Run("Empty Metrics", func(t *testing.T) {
		metrics := []utils.SlowQueryMetrics{}
		err := setSlowQueryMetrics(mockIntegration.Integration, metrics, mockArgs)
		assert.NoError(t, err)
	})
}

func TestGroupQueriesByDatabase(t *testing.T) {
	tests := []struct {
		name           string
		filteredList   []utils.IndividualQueryMetrics
		expectedGroups []utils.QueryGroup
	}{
		{
			name: "Group queries by database",
			filteredList: []utils.IndividualQueryMetrics{
				{DatabaseName: stringPtr("db1"), QueryText: stringPtr("SELECT * FROM table1")},
				{DatabaseName: stringPtr("db1"), QueryText: stringPtr("SELECT * FROM table2")},
				{DatabaseName: stringPtr("db2"), QueryText: stringPtr("SELECT * FROM table3")},
			},
			expectedGroups: []utils.QueryGroup{
				{
					Database: "db1",
					Queries: []utils.IndividualQueryMetrics{
						{DatabaseName: stringPtr("db1"), QueryText: stringPtr("SELECT * FROM table1")},
						{DatabaseName: stringPtr("db1"), QueryText: stringPtr("SELECT * FROM table2")},
					},
				},
				{
					Database: "db2",
					Queries: []utils.IndividualQueryMetrics{
						{DatabaseName: stringPtr("db2"), QueryText: stringPtr("SELECT * FROM table3")},
					},
				},
			},
		},
		{
			name: "Handle nil database name",
			filteredList: []utils.IndividualQueryMetrics{
				{DatabaseName: nil, QueryText: stringPtr("SELECT * FROM table1")},
				{DatabaseName: stringPtr("db1"), QueryText: stringPtr("SELECT * FROM table2")},
			},
			expectedGroups: []utils.QueryGroup{
				{
					Database: "db1",
					Queries: []utils.IndividualQueryMetrics{
						{DatabaseName: stringPtr("db1"), QueryText: stringPtr("SELECT * FROM table2")},
					},
				},
			},
		},
		{
			name:           "Empty filtered list",
			filteredList:   []utils.IndividualQueryMetrics{},
			expectedGroups: []utils.QueryGroup{},
		},
		{
			name: "All nil database names",
			filteredList: []utils.IndividualQueryMetrics{
				{DatabaseName: nil, QueryText: stringPtr("SELECT * FROM table1")},
				{DatabaseName: nil, QueryText: stringPtr("SELECT * FROM table2")},
			},
			expectedGroups: []utils.QueryGroup{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualGroups := groupQueriesByDatabase(tt.filteredList)
			assert.ElementsMatch(t, tt.expectedGroups, actualGroups)
		})
	}
}

func TestCollectIndividualQueryMetrics(t *testing.T) {
	mockDB := new(MockDataSource)
	queryIDList := []string{"1", "2", "3"}
	queryString := "SELECT * FROM performance_schema.events_statements_current WHERE DIGEST = ? AND TIMER_WAIT / 1000000000 > ? ORDER BY TIMER_WAIT DESC LIMIT ?"
	args := arguments.ArgumentList{
		QueryResponseTimeThreshold: 1,
		QueryCountThreshold:        10,
	}
	t.Run("Error", func(t *testing.T) {
		mockCollectIndividualQueryMetrics = func(_ utils.DataSource, queryIDList []string, searchType string, args arguments.ArgumentList) ([]utils.IndividualQueryMetrics, error) {
			return nil, errors.New("some error")
		}

		rows := sqlx.Rows{}
		mockDB.On("QueryxContext", mock.Anything, mock.Anything, mock.Anything).Return(&rows, errors.New("some error"))

		actualMetrics, err := collectIndividualQueryMetrics(mockDB, queryIDList, "searchType", args)
		assert.Error(t, err)
		assert.NotNil(t, actualMetrics)
	})

	t.Run("EmptyQueryIDList", func(t *testing.T) {
		actualMetrics, err := collectIndividualQueryMetrics(mockDB, []string{}, queryString, args)
		assert.NoError(t, err)
		assert.Empty(t, actualMetrics)
	})
}

func TestExtensiveQueryMetrics(t *testing.T) {
	mockDB := new(MockDataSource)
	queryIDList := []string{"1", "2", "3"}
	args := arguments.ArgumentList{}

	t.Run("Error", func(t *testing.T) {
		mockCollectIndividualQueryMetrics = func(_ utils.DataSource, queryIDList []string, searchType string, args arguments.ArgumentList) ([]utils.IndividualQueryMetrics, error) {
			return nil, errors.New("some error")
		}

		rows := sqlx.Rows{}
		mockDB.On("QueryxContext", mock.Anything, mock.Anything, mock.Anything).Return(&rows, errors.New("some error"))

		actualMetrics, err := extensiveQueryMetrics(mockDB, queryIDList, args)
		assert.Error(t, err)
		assert.Nil(t, actualMetrics)
	})

	t.Run("EmptyQueryIDList", func(t *testing.T) {
		mockCollectIndividualQueryMetrics = func(_ utils.DataSource, _ []string, _ string, _ arguments.ArgumentList) ([]utils.IndividualQueryMetrics, error) {
			return []utils.IndividualQueryMetrics{}, nil
		}

		actualMetrics, err := extensiveQueryMetrics(mockDB, []string{}, args)
		assert.NoError(t, err)
		assert.Empty(t, actualMetrics)
	})
}
func TestCurrentQueryMetrics(t *testing.T) {
	mockDB := new(MockDataSource)
	queryIDList := []string{"1", "2", "3"}
	args := arguments.ArgumentList{}

	t.Run("Error", func(t *testing.T) {
		mockCollectIndividualQueryMetrics = func(_ utils.DataSource, queryIDList []string, searchType string, args arguments.ArgumentList) ([]utils.IndividualQueryMetrics, error) {
			return nil, errors.New("some error")
		}

		rows := sqlx.Rows{}
		mockDB.On("QueryxContext", mock.Anything, mock.Anything, mock.Anything).Return(&rows, errors.New("some error"))

		actualMetrics, err := currentQueryMetrics(mockDB, queryIDList, args)
		assert.Error(t, err)
		assert.Nil(t, actualMetrics)
	})

	t.Run("EmptyQueryIDList", func(t *testing.T) {
		mockCollectIndividualQueryMetrics = func(_ utils.DataSource, _ []string, _ string, _ arguments.ArgumentList) ([]utils.IndividualQueryMetrics, error) {
			return []utils.IndividualQueryMetrics{}, nil
		}

		actualMetrics, err := currentQueryMetrics(mockDB, []string{}, args)
		assert.NoError(t, err)
		assert.Empty(t, actualMetrics)
	})
}

func TestRecentQueryMetrics(t *testing.T) {
	mockDB := new(MockDataSource)
	queryIDList := []string{"1", "2", "3"}
	args := arguments.ArgumentList{}

	t.Run("Error", func(t *testing.T) {
		mockCollectIndividualQueryMetrics = func(_ utils.DataSource, queryIDList []string, searchType string, args arguments.ArgumentList) ([]utils.IndividualQueryMetrics, error) {
			return nil, errors.New("some error")
		}

		rows := sqlx.Rows{}
		mockDB.On("QueryxContext", mock.Anything, mock.Anything, mock.Anything).Return(&rows, errors.New("some error"))

		actualMetrics, err := recentQueryMetrics(mockDB, queryIDList, args)
		assert.Error(t, err)
		assert.Nil(t, actualMetrics)
	})

	t.Run("EmptyQueryIDList", func(t *testing.T) {
		mockCollectIndividualQueryMetrics = func(_ utils.DataSource, _ []string, _ string, _ arguments.ArgumentList) ([]utils.IndividualQueryMetrics, error) {
			return []utils.IndividualQueryMetrics{}, nil
		}

		actualMetrics, err := recentQueryMetrics(mockDB, []string{}, args)
		assert.NoError(t, err)
		assert.Empty(t, actualMetrics)
	})
}
func TestPopulateSlowQueryMetrics(t *testing.T) {
	sqlDB, _, err := sqlmock.New()
	require.NoError(t, err)
	db := sqlx.NewDb(sqlDB, "sqlmock")
	defer db.Close()

	mockDB := NewMockDataSource(db)
	mockIntegration := new(MockIntegration)
	mockIntegration.Integration, _ = integration.New("test", "1.0.0")
	args := arguments.ArgumentList{
		SlowQueryFetchInterval: 60,
		QueryCountThreshold:    10,
	}
	excludedDatabases := []string{}

	t.Run("Failure to collect slow query metrics", func(t *testing.T) {
		mockCollectGroupedSlowQueryMetrics = func(_ utils.DataSource, fetchInterval int, queryCountThreshold int, excludedDatabases []string) ([]utils.IndividualQueryMetrics, []string, error) {
			return nil, nil, errors.New("failed to collect metrics")
		}

		queryIDList := PopulateSlowQueryMetrics(mockIntegration.Integration, nil, mockDB, args, excludedDatabases)
		assert.Empty(t, queryIDList)
	})

	t.Run("No metrics collected", func(t *testing.T) {
		mockCollectGroupedSlowQueryMetrics = func(_ utils.DataSource, fetchInterval int, queryCountThreshold int, excludedDatabases []string) ([]utils.IndividualQueryMetrics, []string, error) {
			return []utils.IndividualQueryMetrics{}, []string{}, nil
		}

		queryIDList := PopulateSlowQueryMetrics(mockIntegration.Integration, nil, mockDB, args, excludedDatabases)
		assert.Empty(t, queryIDList)
	})

	t.Run("Failure to set slow query metrics", func(t *testing.T) {
		expectedMetrics := []map[string]interface{}{
			{"query_id": "1", "query_text": "SELECT * FROM table1"},
			{"query_id": "2", "query_text": "SELECT * FROM table2"},
		}
		expectedQueryIDList := []string{"1", "2"}

		mockCollectGroupedSlowQueryMetrics = func(_ utils.DataSource, _ int, _ int, _ []string) ([]utils.IndividualQueryMetrics, []string, error) {
			metrics := []utils.IndividualQueryMetrics{}
			for _, m := range expectedMetrics {
				metrics = append(metrics, utils.IndividualQueryMetrics{
					QueryID:   stringPtr(m["query_id"].(string)),
					QueryText: stringPtr(m["query_text"].(string)),
				})
			}
			return metrics, expectedQueryIDList, nil
		}

		mockSetSlowQueryMetrics = func(_ *integration.Integration, _ []map[string]interface{}, _ arguments.ArgumentList) error {
			return errors.New("failed to set metrics")
		}

		queryIDList := PopulateSlowQueryMetrics(mockIntegration.Integration, nil, mockDB, args, excludedDatabases)
		assert.Empty(t, queryIDList)
	})
}
