package performancemetrics

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	arguments "github.com/newrelic/nri-mysql/src/args"
	queries "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
	"github.com/stretchr/testify/assert"
)

func TestPopulateWaitEventMetrics(t *testing.T) {
	tests := []struct {
		name          string
		mockSetup     func(mock sqlmock.Sqlmock)
		expectedError bool
		expectedLen   int
	}{
		{
			name: "Successful execution",
			mockSetup: func(mock sqlmock.Sqlmock) {
				query := queries.WaitEventsQuery
				rows := sqlmock.NewRows([]string{"query_id", "query_text", "database_name", "wait_category", "collection_timestamp", "instance_id", "wait_event_name", "wait_event_count", "avg_wait_time_ms", "total_wait_time_ms"}).
					AddRow("1234", "SELECT * FROM table", "test_db", "wait_category", "2023-01-01T00:00:00Z", "1", "wait_event", 10, 1.23, 12.3)
				mock.ExpectQuery(query).WillReturnRows(rows)
			},
			expectedError: false,
			expectedLen:   1,
		},
		{
			name: "Query execution failure",
			mockSetup: func(mock sqlmock.Sqlmock) {
				query := queries.WaitEventsQuery
				mock.ExpectQuery(query).WillReturnError(fmt.Errorf("query execution error"))
			},
			expectedError: true,
			expectedLen:   0,
		},
		{
			name: "Row scanning failure",
			mockSetup: func(mock sqlmock.Sqlmock) {
				query := queries.WaitEventsQuery
				rows := sqlmock.NewRows([]string{"query_id", "query_text", "database_name", "wait_category", "collection_timestamp", "instance_id", "wait_event_name", "wait_event_count", "avg_wait_time_ms", "total_wait_time_ms"}).
					AddRow("1234", "SELECT * FROM table", "test_db", "wait_category", "2023-01-01T00:00:00Z", "1", "wait_event", "invalid", 1.23, 12.3)
				mock.ExpectQuery(query).WillReturnRows(rows)
			},
			expectedError: true,
			expectedLen:   0,
		},
		{
			name: "Row iteration error",
			mockSetup: func(mock sqlmock.Sqlmock) {
				query := queries.WaitEventsQuery
				rows := sqlmock.NewRows([]string{"query_id", "query_text", "database_name", "wait_category", "collection_timestamp", "instance_id", "wait_event_name", "wait_event_count", "avg_wait_time_ms", "total_wait_time_ms"}).
					AddRow("1234", "SELECT * FROM table", "test_db", "wait_category", "2023-01-01T00:00:00Z", "1", "wait_event", 10, 1.23, 12.3).
					RowError(0, fmt.Errorf("row iteration error"))
				mock.ExpectQuery(query).WillReturnRows(rows)
			},
			expectedError: true,
			expectedLen:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			assert.NoError(t, err)
			defer db.Close()

			sqlxDB := sqlx.NewDb(db, "sqlmock")
			mockDB := &MockDataSource{db: sqlxDB}
			tt.mockSetup(mock)

			i, err := integration.New("testIntegration", "1.0.0")
			assert.NoError(t, err)

			e := i.LocalEntity()
			args := arguments.ArgumentList{}

			metrics, err := PopulateWaitEventMetrics(mockDB, i, e, args)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Len(t, metrics, tt.expectedLen)
		})
	}
}
