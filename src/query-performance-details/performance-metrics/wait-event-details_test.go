package performancemetrics

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/nri-mysql/src/args"
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
				queryRegex := `SELECT\s+schema_data\.DIGEST\s+AS\s+query_id,.*FROM\s+\(\s*SELECT\s+.*FROM\s+performance_schema\.events_waits_history_long.*GROUP\s+BY\s+query_id.*ORDER\s+BY\s+total_wait_time_ms\s+DESC\s+LIMIT\s+\?;`

				rows := sqlmock.NewRows([]string{"query_id", "query_text", "database_name", "wait_category", "collection_timestamp", "instance_id", "wait_event_name", "wait_event_count", "avg_wait_time_ms", "total_wait_time_ms"}).
					AddRow("1234", "SELECT * FROM table", "test_db", "wait_category", "2023-01-01T00:00:00Z", "1", "wait_event", 10, 1.23, 12.3)
				
				mock.ExpectQuery(queryRegex).WillReturnRows(rows)
			},
			expectedError: false,
			expectedLen:   1,
		},
		{
			name: "Query execution failure",
			mockSetup: func(mock sqlmock.Sqlmock) {
				queryRegex := `SELECT\s+schema_data\.DIGEST\s+AS\s+query_id,.*FROM\s+\(\s*SELECT\s+.*FROM\s+performance_schema\.events_waits_history_long.*GROUP\s+BY\s+query_id.*ORDER\s+BY\s+total_wait_time_ms\s+DESC\s+LIMIT\s+\?;`
				mock.ExpectQuery(queryRegex).WillReturnError(fmt.Errorf("query execution error"))
			},
			expectedError: true,
			expectedLen:   0,
		},
		{
			name: "Row scanning failure",
			mockSetup: func(mock sqlmock.Sqlmock) {
				queryRegex := `SELECT\s+schema_data\.DIGEST\s+AS\s+query_id,.*FROM\s+\(\s*SELECT\s+.*FROM\s+performance_schema\.events_waits_history_long.*GROUP\s+BY\s+query_id.*ORDER\s+BY\s+total_wait_time_ms\s+DESC\s+LIMIT\s+\?;`
				rows := sqlmock.NewRows([]string{"query_id", "query_text", "database_name", "wait_category", "collection_timestamp", "instance_id", "wait_event_name", "wait_event_count", "avg_wait_time_ms", "total_wait_time_ms"}).
					AddRow("1234", "SELECT * FROM table", "test_db", "wait_category", "2023-01-01T00:00:00Z", "1", "wait_event", "invalid", 1.23, 12.3)
				mock.ExpectQuery(queryRegex).WillReturnRows(rows)
			},
			expectedError: true,
			expectedLen:   0,
		},
		{
			name: "Row iteration error",
			mockSetup: func(mock sqlmock.Sqlmock) {
				queryRegex := `SELECT\s+schema_data\.DIGEST\s+AS\s+query_id,.*FROM\s+\(\s*SELECT\s+.*FROM\s+performance_schema\.events_waits_history_long.*GROUP\s+BY\s+query_id.*ORDER\s+BY\s+total_wait_time_ms\s+DESC\s+LIMIT\s+\?;`
				rows := sqlmock.NewRows([]string{"query_id", "query_text", "database_name", "wait_category", "collection_timestamp", "instance_id", "wait_event_name", "wait_event_count", "avg_wait_time_ms", "total_wait_time_ms"}).
					AddRow("1234", "SELECT * FROM table", "test_db", "wait_category", "2023-01-01T00:00:00Z", "1", "wait_event", 10, 1.23, 12.3).
					RowError(0, fmt.Errorf("row iteration error"))
				mock.ExpectQuery(queryRegex).WillReturnRows(rows)
			},
			expectedError: true,
			expectedLen:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
			assert.NoError(t, err)
			defer db.Close()

			sqlxDB := sqlx.NewDb(db, "sqlmock")
			mockDB := &MockDataSource{db: sqlxDB}
			tt.mockSetup(mock)

			i, err := integration.New("testIntegration", "1.0.0")
			assert.NoError(t, err)

			e := i.LocalEntity()
			args := args.ArgumentList{ExcludedDatabases: `["", "mysql", "information_schema", "performance_schema", "sys"]`, QueryCountThreshold: 10}

			metrics, err := PopulateWaitEventMetrics(mockDB, i, e, args)
			if err != nil {
				fmt.Printf("Actual SQL Error: %v\n", err)
			}
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Len(t, metrics, tt.expectedLen)
		})
	}
}
