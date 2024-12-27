package validator

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	dbconnection "github.com/newrelic/nri-mysql/src/query-performance-details/connection"
	"github.com/stretchr/testify/assert"
)

type MockDataSource struct {
	db *sqlx.DB
}

func (m *MockDataSource) Close() {
	m.db.Close()
}

func (m *MockDataSource) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	return m.db.QueryxContext(ctx, query, args...)
}

func (m *MockDataSource) QueryX(query string) (*sqlx.Rows, error) {
	return m.db.Queryx(query)
}

func getMockDataSource(db *sqlx.DB) dbconnection.DataSource {
	return &MockDataSource{db: db}
}

func TestValidatePreconditions_PerformanceSchemaDisabled(t *testing.T) {
	db, mock, err := sqlmock.New()
	sqlxDB := sqlx.NewDb(db, "sqlmock")
	assert.NoError(t, err)
	defer db.Close()

	// Mock Performance Schema disabled
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "OFF"))
	dataSource := getMockDataSource(sqlxDB)
	result := ValidatePreconditions(dataSource)
	assert.False(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestValidatePreconditions_ConsumerCheckError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	// Mock Performance Schema enabled
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "ON"))

	// Mock error on checking essential consumers
	mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers").
		WillReturnError(sql.ErrNoRows)

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	dataSource := getMockDataSource(sqlxDB)
	result := ValidatePreconditions(dataSource)
	assert.False(t, result)
	assert.NoError(t, mock.ExpectationsWereMet()) // Ensure all expected calls were made
}

func TestValidatePreconditions_InstrumentNotFullyEnabled(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	// Mock Performance Schema enabled
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "ON"))

	// Mock consumers check
	mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers").
		WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED"}).
			AddRow("events_statements_current", "YES").
			AddRow("events_statements_history", "YES"))

	// Mock essential instruments check
	mock.ExpectQuery("SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments").
		WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED", "TIMED"}).
			AddRow("wait/io/file/sql/handler", "YES", sql.NullString{String: "YES", Valid: true}).
			AddRow("statement/sql/select", "NO", sql.NullString{String: "YES", Valid: true}))

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	dataSource := getMockDataSource(sqlxDB)
	result := ValidatePreconditions(dataSource)
	assert.False(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestValidatePreconditions_InstrumentNotTimed(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	// Mock Performance Schema enabled
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "ON"))

	// Mock consumers check
	mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers").
		WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED"}).
			AddRow("events_statements_current", "YES").
			AddRow("events_statements_history", "YES"))

	// Mock essential instruments check
	mock.ExpectQuery("SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments").
		WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED", "TIMED"}).
			AddRow("wait/io/file/sql/handler", "YES", sql.NullString{String: "YES", Valid: true}).
			AddRow("statement/sql/select", "YES", sql.NullString{String: "NO", Valid: true}))

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	dataSource := getMockDataSource(sqlxDB)
	result := ValidatePreconditions(dataSource)
	assert.False(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestValidatePreconditions_InstrumentNotEnabled(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	// Mock Performance Schema enabled
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "ON"))

	// Mock consumers check
	mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers").
		WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED"}).
			AddRow("events_statements_current", "YES").
			AddRow("events_statements_history", "YES"))

	// Mock essential instruments check
	mock.ExpectQuery("SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments").
		WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED", "TIMED"}).
			AddRow("wait/io/file/sql/handler", "YES", sql.NullString{String: "YES", Valid: true}).
			AddRow("statement/sql/select", "NO", sql.NullString{String: "NO", Valid: true}))

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	dataSource := getMockDataSource(sqlxDB)
	result := ValidatePreconditions(dataSource)
	assert.False(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestValidatePreconditions_InstrumentCheckError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	// Mock Performance Schema enabled
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "ON"))

	// Mock consumers check
	mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers").
		WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED"}).
			AddRow("events_statements_current", "YES").
			AddRow("events_statements_history", "YES"))

	// Mock essential instruments check
	mock.ExpectQuery("SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments").
		WillReturnError(sql.ErrNoRows)

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	dataSource := getMockDataSource(sqlxDB)
	result := ValidatePreconditions(dataSource)
	assert.False(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLogEnablePerformanceSchemaInstructions(t *testing.T) {
	tests := []struct {
		version       string
		expectedLogs  []string
		expectedPrint []string
	}{
		{
			version: "8.0",
			expectedLogs: []string{
				"To enable the Performance Schema, add the following lines to your MySQL configuration file (my.cnf or my.ini) in the [mysqld] section and restart the MySQL server:",
				"performance_schema=ON",
				"For MySQL 8.0 and higher, you may also need to set the following variables:",
				"performance_schema_instrument='%=ON'",
				"performance_schema_consumer_events_statements_current=ON",
				"performance_schema_consumer_events_statements_history=ON",
				"performance_schema_consumer_events_statements_history_long=ON",
				"performance_schema_consumer_events_waits_current=ON",
				"performance_schema_consumer_events_waits_history=ON",
				"performance_schema_consumer_events_waits_history_long=ON",
			},
			expectedPrint: []string{
				"To enable the Performance Schema, add the following lines to your MySQL configuration file (my.cnf or my.ini) in the [mysqld] section and restart the MySQL server:",
				"performance_schema=ON",
				"For MySQL 8.0 and higher, you may also need to set the following variables:",
				"performance_schema_instrument='%=ON'",
				"performance_schema_consumer_events_statements_current=ON",
				"performance_schema_consumer_events_statements_history=ON",
				"performance_schema_consumer_events_statements_history_long=ON",
				"performance_schema_consumer_events_waits_current=ON",
				"performance_schema_consumer_events_waits_history=ON",
				"performance_schema_consumer_events_waits_history_long=ON",
			},
		},
		{
			version: "7.9",
			expectedLogs: []string{
				"MySQL version 7.9 is not supported. Only version 8.0+ is supported.",
			},
			expectedPrint: []string{
				"MySQL version 7.9 is not supported. Only version 8.0+ is supported.",
			},
		},
	}

	for _, test := range tests {
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		assert.NoError(t, err)
		defer db.Close()

		mock.ExpectQuery("SELECT VERSION();").
			WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow(test.version))

		sqlxDB := sqlx.NewDb(db, "sqlmock")
		dataSource := getMockDataSource(sqlxDB)

		var logBuffer bytes.Buffer
		log.SetOutput(&logBuffer)

		logEnablePerformanceSchemaInstructions(dataSource)

		for _, expectedLog := range test.expectedLogs {
			assert.Contains(t, logBuffer.String(), expectedLog)
		}

		assert.NoError(t, mock.ExpectationsWereMet())
	}
}

func TestIsVersion8OrGreater(t *testing.T) {
	tests := []struct {
		version  string
		expected bool
	}{
		{"8.0", true},
		{"8.1", true},
		{"9.0", true},
		{"7.9", false},
		{"8.0.1", true},
		{"8", false},
		{"8.", false},
		{"", false},
		{"invalid.version", false},
	}

	for _, test := range tests {
		result := IsVersion8OrGreater(test.version)
		assert.Equal(t, test.expected, result, "Version: %s", test.version)
	}
}

func TestParseVersion(t *testing.T) {
	tests := []struct {
		version       string
		expectedMajor int
		expectedMinor int
	}{
		{"8.0", 8, 0},
		{"8.1", 8, 1},
		{"9.0", 9, 0},
		{"7.9", 7, 9},
		{"8.0.1", 8, 0},
		{"8", 0, 0},
		{"8.", 0, 0},
		{"", 0, 0},
		{"invalid.version", 0, 0},
	}

	for _, test := range tests {
		major, minor := ParseVersion(test.version)
		assert.Equal(t, test.expectedMajor, major, "Version: %s", test.version)
		assert.Equal(t, test.expectedMinor, minor, "Version: %s", test.version)
	}
}
