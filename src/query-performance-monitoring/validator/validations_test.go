package validator

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	constants "github.com/newrelic/nri-mysql/src/query-performance-monitoring/constants"
	"github.com/stretchr/testify/assert"
)

type mockDataSource struct {
	db *sqlx.DB
}

func (m *mockDataSource) Close() {
	m.db.Close()
}

func (m *mockDataSource) QueryX(query string) (*sqlx.Rows, error) {
	return m.db.Queryx(query)
}

func (m *mockDataSource) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	return m.db.QueryxContext(ctx, query, args...)
}

var errQueryFailed = errors.New("query failed")
var errQuery = errors.New("query error")
var errProcedure = errors.New("procedure error")

func TestValidatePreconditions_PerformanceSchemaDisabled(t *testing.T) {
	rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("performance_schema", "OFF")
	versionRows := sqlmock.NewRows([]string{"VERSION()"}).
		AddRow("8.0.23")
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mockDataSource := &mockDataSource{db: sqlxDB}

	// Set the correct order of mock expectations
	mock.ExpectQuery(versionQuery).WillReturnRows(versionRows)
	mock.ExpectQuery(performanceSchemaQuery).WillReturnRows(rows)

	err = ValidatePreconditions(mockDataSource)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "performance schema is not enabled")

	// Ensure all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestValidatePreconditions_EssentialChecksFailed(t *testing.T) {
	testCases := []struct {
		name            string
		expectQueryFunc func(mock sqlmock.Sqlmock)
		assertError     bool
	}{
		{
			name: "EssentialConsumersCheckFailed",
			expectQueryFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(buildConsumerStatusQuery()).WillReturnError(errQueryFailed)
			},
			assertError: false, // The function logs a warning but does not return an error
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			versionRows := sqlmock.NewRows([]string{"version"}).AddRow("8.0.23")
			performanceSchemaRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "ON")
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			assert.NoError(t, err, "an error was not expected when opening a stub database connection")
			defer db.Close()
			sqlxDB := sqlx.NewDb(db, "sqlmock")
			mockDataSource := &mockDataSource{db: sqlxDB}

			mock.ExpectQuery(versionQuery).WillReturnRows(versionRows)
			mock.ExpectQuery(performanceSchemaQuery).WillReturnRows(performanceSchemaRows)
			tc.expectQueryFunc(mock) // Dynamically call the query expectation function

			err = ValidatePreconditions(mockDataSource)
			if tc.assertError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIsPerformanceSchemaEnabled_NoRowsFound(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mockDataSource := &mockDataSource{db: sqlxDB}

	mock.ExpectQuery(performanceSchemaQuery).WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}))
	enabled, err := isPerformanceSchemaEnabled(mockDataSource)
	assert.Error(t, err)
	assert.Equal(t, ErrNoRowsFound, err)
	assert.False(t, enabled)
}

func TestCheckEssentialConsumers_ConsumerNotEnabled(t *testing.T) {
	rows := sqlmock.NewRows([]string{"NAME", "ENABLED"}).
		AddRow("events_waits_current", "NO")
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mockDataSource := &mockDataSource{db: sqlxDB}

	mock.ExpectQuery(buildConsumerStatusQuery()).WillReturnRows(rows)
	err = checkEssentialConsumers(mockDataSource)
	assert.Error(t, err)
}

func TestCheckEssentialStatus(t *testing.T) {
	tests := []struct {
		name           string
		setupMock      func(mock sqlmock.Sqlmock)
		testFunc       func(dataSource *mockDataSource) (interface{}, error)
		expectedResult interface{}
		expectError    bool
	}{
		{
			name: "CheckEssentialStatus_Success",
			setupMock: func(mock sqlmock.Sqlmock) {
				query := "SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);"
				rows := sqlmock.NewRows([]string{"NAME", "ENABLED"}).
					AddRow("events_waits_current", "YES").
					AddRow("events_statements_history", "YES")
				mock.ExpectQuery(query).WillReturnRows(rows)
			},
			testFunc: func(dataSource *mockDataSource) (interface{}, error) {
				query := "SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);"
				return checkEssentialStatus(dataSource, query)
			},
			expectedResult: 2,
			expectError:    false,
		},
		{
			name: "CheckEssentialStatus_Failure_QueryError",
			setupMock: func(mock sqlmock.Sqlmock) {
				query := "SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);"
				mock.ExpectQuery(query).WillReturnError(errQuery)
			},
			testFunc: func(dataSource *mockDataSource) (interface{}, error) {
				query := "SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);"
				return checkEssentialStatus(dataSource, query)
			},
			expectedResult: 0,
			expectError:    true,
		},
		{
			name: "CheckEssentialStatus_Failure_ScanError",
			setupMock: func(mock sqlmock.Sqlmock) {
				query := "SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);"
				rows := sqlmock.NewRows([]string{"NAME", "ENABLED"}).
					AddRow("events_waits_current", nil) // Simulate scan error
				mock.ExpectQuery(query).WillReturnRows(rows)
			},
			testFunc: func(dataSource *mockDataSource) (interface{}, error) {
				query := "SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);"
				return checkEssentialStatus(dataSource, query)
			},
			expectedResult: 0,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			assert.NoError(t, err)
			defer db.Close()

			sqlxDB := sqlx.NewDb(db, "sqlmock")
			mockDataSource := &mockDataSource{db: sqlxDB}

			tt.setupMock(mock)

			result, err := tt.testFunc(mockDataSource)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestEnableEssentialConsumersAndInstrumentsProcedure_Failure(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mockDataSource := &mockDataSource{db: sqlxDB}

	mock.ExpectQuery(enableEssentialConsumersAndInstrumentsProcedureQuery).WillReturnError(errProcedure)

	err = enableEssentialConsumersAndInstrumentsProcedure(mockDataSource)
	assert.Error(t, err)
}

func TestGetMySQLVersion(t *testing.T) {
	rows := sqlmock.NewRows([]string{"VERSION()"}).
		AddRow("8.0.23")
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mockDataSource := &mockDataSource{db: sqlxDB}

	mock.ExpectQuery(versionQuery).WillReturnRows(rows)
	version, err := getMySQLVersion(mockDataSource)
	assert.NoError(t, err)
	assert.Equal(t, "8.0.23", version)
}
func TestIsVersion8OrGreater(t *testing.T) {
	assert.True(t, isVersion8OrGreater("8.0.23"))
	assert.True(t, isVersion8OrGreater("8.4"))
	assert.False(t, isVersion8OrGreater("5.7.31"))
	assert.False(t, isVersion8OrGreater("5.6"))
	assert.False(t, isVersion8OrGreater("5"))
	assert.False(t, isVersion8OrGreater("invalid.version.string"))
	assert.False(t, isVersion8OrGreater(""))
}

func TestExtractMajorFromVersion(t *testing.T) {
	major, err := extractMajorFromVersion("8.0.23")
	assert.NoError(t, err)
	assert.Equal(t, 8, major)

	major, err = extractMajorFromVersion("5.7.31")
	assert.NoError(t, err)
	assert.Equal(t, 5, major)

	major, err = extractMajorFromVersion("5")
	assert.Error(t, err)
	assert.Equal(t, 0, major)

	major, err = extractMajorFromVersion("invalid.version")
	assert.Error(t, err)
	assert.Equal(t, 0, major)

	major, err = extractMajorFromVersion("")
	assert.Error(t, err)
	assert.Equal(t, 0, major)
}

func TestGetValidSlowQueryFetchIntervalThreshold(t *testing.T) {
	tests := []struct {
		name      string
		threshold int
		expected  int
	}{
		{"Negative threshold", -1, constants.DefaultSlowQueryFetchInterval},
		{"Zero threshold", 0, 0},
		{"Positive threshold", 100, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetValidSlowQueryFetchIntervalThreshold(tt.threshold)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetValidQueryResponseTimeThreshold(t *testing.T) {
	tests := []struct {
		name      string
		threshold int
		expected  int
	}{
		{"Negative threshold", -1, constants.DefaultQueryResponseTimeThreshold},
		{"Zero threshold", 0, 0},
		{"Positive threshold", 100, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetValidQueryResponseTimeThreshold(tt.threshold)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetValidQueryCountThreshold(t *testing.T) {
	tests := []struct {
		name      string
		threshold int
		expected  int
	}{
		{"Negative threshold", -1, constants.DefaultQueryCountThreshold},
		{"Zero threshold", 0, 0},
		{"Threshold greater than max", constants.MaxQueryCountThreshold + 1, constants.MaxQueryCountThreshold},
		{"Threshold equal to max", constants.MaxQueryCountThreshold, constants.MaxQueryCountThreshold},
		{"Positive threshold", constants.MaxQueryCountThreshold - 1, constants.MaxQueryCountThreshold - 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetValidQueryCountThreshold(tt.threshold)
			assert.Equal(t, tt.expected, result)
		})
	}
}
