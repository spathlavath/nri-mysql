package queryperformancedetails

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestNewMySQLCollector(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	collector := NewMySQLCollector(db)
	assert.NotNil(t, collector)
	assert.Equal(t, db, collector.db)
}

func TestMySQLCollector_Connect(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	collector := NewMySQLCollector(db)

	t.Run("Performance Schema Enabled", func(t *testing.T) {
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "ON"))

		mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);").
			WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED"}).
				AddRow("events_waits_current", "YES").
				AddRow("events_waits_history_long", "YES").
				AddRow("events_waits_history", "YES").
				AddRow("events_statements_history_long", "YES").
				AddRow("events_statements_history", "YES").
				AddRow("events_statements_current", "YES").
				AddRow("events_statements_cpu", "YES").
				AddRow("events_transactions_current", "YES").
				AddRow("events_stages_current", "YES"))

		mock.ExpectQuery("SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments WHERE .+;").
			WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED", "TIMED"}).
				AddRow("statement/sql/select", "YES", "YES").
				AddRow("wait/io/file/innodb/io_read", "YES", "YES").
				AddRow("wait/%", "YES", "YES").
				AddRow("statement/%", "YES", "YES").
				AddRow("%lock%", "YES", "YES"))

		enabled, err := collector.Connect()
		assert.NoError(t, err)
		assert.True(t, enabled)
	})

	t.Run("Performance Schema Disabled", func(t *testing.T) {
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "OFF"))

		mock.ExpectQuery("SELECT VERSION();").
			WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.33"))

		enabled, err := collector.Connect()
		assert.NoError(t, err)
		assert.False(t, enabled)
	})

	t.Run("Error Checking Performance Schema", func(t *testing.T) {
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
			WillReturnError(fmt.Errorf("query error"))

		enabled, err := collector.Connect()
		assert.Error(t, err)
		assert.False(t, enabled)
	})

	t.Run("Error Checking Essential Consumers", func(t *testing.T) {
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "ON"))

		mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);").
			WillReturnError(fmt.Errorf("query error"))

		enabled, err := collector.Connect()
		assert.Error(t, err)
		assert.False(t, enabled)
	})

	t.Run("Error Checking Essential Instruments", func(t *testing.T) {
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "ON"))

		mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);").
			WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED"}).
				AddRow("events_waits_current", "YES").
				AddRow("events_waits_history_long", "YES").
				AddRow("events_waits_history", "YES").
				AddRow("events_statements_history_long", "YES").
				AddRow("events_statements_history", "YES").
				AddRow("events_statements_current", "YES").
				AddRow("events_statements_cpu", "YES").
				AddRow("events_transactions_current", "YES").
				AddRow("events_stages_current", "YES"))

		mock.ExpectQuery("SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments WHERE .+;").
			WillReturnError(fmt.Errorf("query error"))

		enabled, err := collector.Connect()
		assert.Error(t, err)
		assert.False(t, enabled)
	})
}

func TestMySQLCollector_isPerformanceSchemaEnabled(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	collector := NewMySQLCollector(db)

	t.Run("Performance Schema Enabled", func(t *testing.T) {
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "ON"))

		enabled, err := collector.isPerformanceSchemaEnabled()
		assert.NoError(t, err)
		assert.True(t, enabled)
	})

	t.Run("Performance Schema Disabled", func(t *testing.T) {
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", "OFF"))

		enabled, err := collector.isPerformanceSchemaEnabled()
		assert.NoError(t, err)
		assert.False(t, enabled)
	})

	t.Run("Error Checking Performance Schema", func(t *testing.T) {
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").
			WillReturnError(fmt.Errorf("query error"))

		enabled, err := collector.isPerformanceSchemaEnabled()
		assert.Error(t, err)
		assert.False(t, enabled)
	})
}

func TestMySQLCollector_checkEssentialConsumers(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	collector := NewMySQLCollector(db)

	t.Run("All Consumers Enabled", func(t *testing.T) {
		mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);").
			WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED"}).
				AddRow("events_waits_current", "YES").
				AddRow("events_waits_history_long", "YES").
				AddRow("events_waits_history", "YES").
				AddRow("events_statements_history_long", "YES").
				AddRow("events_statements_history", "YES").
				AddRow("events_statements_current", "YES").
				AddRow("events_statements_cpu", "YES").
				AddRow("events_transactions_current", "YES").
				AddRow("events_stages_current", "YES"))

		err := collector.checkEssentialConsumers()
		assert.NoError(t, err)
	})

	t.Run("Some Consumers Disabled", func(t *testing.T) {
		mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);").
			WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED"}).
				AddRow("events_waits_current", "YES").
				AddRow("events_waits_history_long", "NO"))

		err := collector.checkEssentialConsumers()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "essential consumer events_waits_history_long is not enabled")
	})

	t.Run("Error Checking Consumers", func(t *testing.T) {
		mock.ExpectQuery("SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN (.+);").
			WillReturnError(fmt.Errorf("query error"))

		err := collector.checkEssentialConsumers()
		assert.Error(t, err)
	})
}

func TestMySQLCollector_checkEssentialInstruments(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	collector := NewMySQLCollector(db)

	t.Run("All Instruments Enabled", func(t *testing.T) {
		mock.ExpectQuery("SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments WHERE .+;").
			WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED", "TIMED"}).
				AddRow("statement/sql/select", "YES", "YES").
				AddRow("wait/io/file/innodb/io_read", "YES", "YES").
				AddRow("wait/%", "YES", "YES").
				AddRow("statement/%", "YES", "YES").
				AddRow("%lock%", "YES", "YES"))

		err := collector.checkEssentialInstruments()
		assert.NoError(t, err)
	})

	t.Run("Some Instruments Disabled", func(t *testing.T) {
		mock.ExpectQuery("SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments WHERE .+;").
			WillReturnRows(sqlmock.NewRows([]string{"NAME", "ENABLED", "TIMED"}).
				AddRow("statement/sql/select", "YES", "YES").
				AddRow("wait/io/file/innodb/io_read", "NO", "YES"))

		err := collector.checkEssentialInstruments()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "essential instrument wait/io/file/innodb/io_read is not fully enabled")
	})

	t.Run("Error Checking Instruments", func(t *testing.T) {
		mock.ExpectQuery("SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments WHERE .+;").
			WillReturnError(fmt.Errorf("query error"))

		err := collector.checkEssentialInstruments()
		assert.Error(t, err)
	})
}

func TestMySQLCollector_logEnablePerformanceSchemaInstructions(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	collector := NewMySQLCollector(db)

	t.Run("MySQL 5.6", func(t *testing.T) {
		mock.ExpectQuery("SELECT VERSION();").
			WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.6.50"))

		collector.logEnablePerformanceSchemaInstructions()
	})

	t.Run("MySQL 5.7", func(t *testing.T) {
		mock.ExpectQuery("SELECT VERSION();").
			WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.33"))

		collector.logEnablePerformanceSchemaInstructions()
	})

	t.Run("MySQL 8.0", func(t *testing.T) {
		mock.ExpectQuery("SELECT VERSION();").
			WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.23"))

		collector.logEnablePerformanceSchemaInstructions()
	})

	t.Run("Error Getting MySQL Version", func(t *testing.T) {
		mock.ExpectQuery("SELECT VERSION();").
			WillReturnError(fmt.Errorf("query error"))

		collector.logEnablePerformanceSchemaInstructions()
	})
}
