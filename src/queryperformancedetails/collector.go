package queryperformancedetails

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/newrelic/infra-integrations-sdk/v3/log"
)

// MySQLCollector manages the collection of MySQL metrics.
type MySQLCollector struct {
	db *sql.DB
}

// NewMySQLCollector creates a new MySQLCollector instance.
func NewMySQLCollector(db *sql.DB) *MySQLCollector {
	return &MySQLCollector{
		db: db,
	}
}

// Connect verifies the Performance Schema's overall status and the enablement of essential consumers and instruments.
func (mc *MySQLCollector) Connect() (bool, error) {
	// Check Performance Schema status
	performanceSchemaEnabled, err := mc.isPerformanceSchemaEnabled()
	if err != nil {
		log.Error("Failed to check Performance Schema status: %v", err)
		return false, err
	}

	if !performanceSchemaEnabled {
		log.Error("Performance Schema is not enabled. Skipping validation.")
		mc.logEnablePerformanceSchemaInstructions()
		return false, nil
	}

	// Check essential consumers
	if err := mc.checkEssentialConsumers(); err != nil {
		log.Error("Failed to check essential consumers: %v", err)
		return false, err
	}

	// Check essential instruments
	if err := mc.checkEssentialInstruments(); err != nil {
		log.Error("Failed to check essential instruments: %v", err)
		return false, err
	}

	return true, nil
}

func (mc *MySQLCollector) isPerformanceSchemaEnabled() (bool, error) {
	var variableName, performanceSchemaEnabled string
	err := mc.db.QueryRow("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").Scan(&variableName, &performanceSchemaEnabled)
	if err != nil {
		return false, fmt.Errorf("failed to check Performance Schema status: %w", err)
	}
	return performanceSchemaEnabled == "ON", nil
}

func (mc *MySQLCollector) checkEssentialConsumers() error {
	consumers := []string{
		"events_waits_current",
		"events_waits_history_long",
		"events_waits_history",
		"events_statements_history_long",
		"events_statements_history",
		"events_statements_current",
		"events_statements_cpu",
		"events_transactions_current",
		"events_stages_current",
		// Add other essential consumers here
	}

	query := "SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN ("
	for i, consumer := range consumers {
		if i > 0 {
			query += ", "
		}
		query += fmt.Sprintf("'%s'", consumer)
	}
	query += ");"

	rows, err := mc.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to check essential consumers: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Error("Failed to close rows: %v", err)
		}
	}()

	for rows.Next() {
		var name, enabled string
		if err := rows.Scan(&name, &enabled); err != nil {
			return fmt.Errorf("failed to scan consumer row: %w", err)
		}
		if enabled != "YES" {
			log.Error("Essential consumer %s is not enabled. To enable it, run: UPDATE performance_schema.setup_consumers SET ENABLED = 'YES' WHERE NAME = '%s';", name, name)
			return fmt.Errorf("essential consumer %s is not enabled", name)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}

	return nil
}

func (mc *MySQLCollector) checkEssentialInstruments() error {
	instruments := []string{
		"statement/sql/select",
		"wait/io/file/innodb/io_read",
		// Add other essential instruments here
		"wait/%",
		"statement/%",
		"%lock%",
	}

	var instrumentConditions []string
	for _, instrument := range instruments {
		instrumentConditions = append(instrumentConditions, fmt.Sprintf("NAME LIKE '%s'", instrument))
	}

	query := "SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments WHERE "
	query += strings.Join(instrumentConditions, " OR ")
	query += ";"

	rows, err := mc.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to check essential instruments: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Error("Failed to close rows: %v", err)
		}
	}()

	for rows.Next() {
		var name, enabled, timed string
		if err := rows.Scan(&name, &enabled, &timed); err != nil {
			return fmt.Errorf("failed to scan instrument row: %w", err)
		}
		if enabled != "YES" || timed != "YES" {
			log.Error("Essential instrument %s is not fully enabled. To enable it, run: UPDATE performance_schema.setup_instruments SET ENABLED = 'YES', TIMED = 'YES' WHERE NAME = '%s';", name, name)
			return fmt.Errorf("essential instrument %s is not fully enabled", name)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}

	return nil
}

func (mc *MySQLCollector) logEnablePerformanceSchemaInstructions() {
	version, err := mc.getMySQLVersion()
	if err != nil {
		log.Error("Failed to get MySQL version: %v", err)
		return
	}

	log.Error("To enable the Performance Schema, add the following line to your MySQL configuration file (my.cnf or my.ini) and restart the MySQL server:")
	log.Error("performance_schema=ON")

	if strings.HasPrefix(version, "5.6") {
		log.Error("For MySQL 5.6, you may also need to set the following variables:")
		log.Error("performance_schema_instrument='%=ON'")
		log.Error("performance_schema_consumer_events_statements_current=ON")
		log.Error("performance_schema_consumer_events_statements_history=ON")
		log.Error("performance_schema_consumer_events_statements_history_long=ON")
		log.Error("performance_schema_consumer_events_waits_current=ON")
		log.Error("performance_schema_consumer_events_waits_history=ON")
		log.Error("performance_schema_consumer_events_waits_history_long=ON")
	} else if strings.HasPrefix(version, "5.7") || strings.HasPrefix(version, "8.0") {
		log.Error("For MySQL 5.7 and 8.0, you may also need to set the following variables:")
		log.Error("performance_schema_instrument='%=ON'")
		log.Error("performance_schema_consumer_events_statements_current=ON")
		log.Error("performance_schema_consumer_events_statements_history=ON")
		log.Error("performance_schema_consumer_events_statements_history_long=ON")
		log.Error("performance_schema_consumer_events_waits_current=ON")
		log.Error("performance_schema_consumer_events_waits_history=ON")
		log.Error("performance_schema_consumer_events_waits_history_long=ON")
	}
}

func (mc *MySQLCollector) getMySQLVersion() (string, error) {
	var version string
	err := mc.db.QueryRow("SELECT VERSION();").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to get MySQL version: %w", err)
	}
	return version, nil
}
