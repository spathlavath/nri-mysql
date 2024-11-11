package queryperformancedetails

import (
	"database/sql"
	"fmt"
	"log"
)

// // MySQLCollector manages the collection of MySQL metrics.
type MySQLCollector struct {
	db *sql.DB
}

// // NewMySQLCollector creates a new MySQLCollector instance.
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
		return false, err
	}

	if !performanceSchemaEnabled {
		log.Println("Performance Schema is not enabled. Skipping validation.")
		return false, nil
	}

	// // Check essential consumers
	// if err := mc.checkEssentialConsumers(); err != nil {
	// 	return false, err
	// }

	// // Check essential instruments
	// if err := mc.checkEssentialInstruments(); err != nil {
	// 	return false, err
	// }

	return true, nil
}

func (mc *MySQLCollector) isPerformanceSchemaEnabled() (bool, error) {
	var performanceSchemaEnabled string
	err := mc.db.QueryRow("SHOW GLOBAL VARIABLES LIKE 'performance_schema';").Scan(&performanceSchemaEnabled)
	if err != nil {
		return false, fmt.Errorf("failed to check Performance Schema status: %w", err)
	}
	return performanceSchemaEnabled == "ON", nil
}

func (mc *MySQLCollector) checkEssentialConsumers() error {
	consumers := []string{
		"events_statements_current",
		"events_waits_current",
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
	defer rows.Close()

	for rows.Next() {
		var name, enabled string
		if err := rows.Scan(&name, &enabled); err != nil {
			return fmt.Errorf("failed to scan consumer row: %w", err)
		}
		if enabled != "YES" {
			return fmt.Errorf("essential consumer %s is not enabled", name)
		}
	}

	return nil
}

func (mc *MySQLCollector) checkEssentialInstruments() error {
	instruments := []string{
		"statement/sql/select",
		"wait/io/file/innodb/io_read",
		// Add other essential instruments here
	}

	query := "SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments WHERE NAME IN ("
	for i, instrument := range instruments {
		if i > 0 {
			query += ", "
		}
		query += fmt.Sprintf("'%s'", instrument)
	}
	query += ");"

	rows, err := mc.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to check essential instruments: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, enabled, timed string
		if err := rows.Scan(&name, &enabled, &timed); err != nil {
			return fmt.Errorf("failed to scan instrument row: %w", err)
		}
		if enabled != "YES" || timed != "YES" {
			return fmt.Errorf("essential instrument %s is not fully enabled", name)
		}
	}

	return nil
}
