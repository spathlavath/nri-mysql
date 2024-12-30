package validator

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	commonutils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	dbconnection "github.com/newrelic/nri-mysql/src/query-performance-details/connection"

	"github.com/newrelic/infra-integrations-sdk/v3/log"
)

// Define constants
const minVersionParts = 2

// ValidatePreconditions checks if the necessary preconditions are met for performance monitoring.
func ValidatePreconditions(db dbconnection.DataSource) bool {
	// Check if Performance Schema is enabled
	performanceSchemaEnabled, errPerformanceEnabled := isPerformanceSchemaEnabled(db)
	if errPerformanceEnabled != nil {
		log.Error("Failed to check Performance Schema status: %v", errPerformanceEnabled)
		return false
	}

	if !performanceSchemaEnabled {
		log.Error("Performance Schema is not enabled. Skipping validation.")
		logEnablePerformanceSchemaInstructions(db)
		return false
	}

	// Check if essential consumers are enabled
	errEssentialConsumers := checkEssentialConsumers(db)
	if errEssentialConsumers != nil {
		log.Error("Essential consumer check failed: %v", fmt.Errorf("%w", errEssentialConsumers))
		return false
	}

	// Check if essential instruments are enabled
	errEssentialInstruments := checkEssentialInstruments(db)
	if errEssentialInstruments != nil {
		log.Error("Essential instruments check failed: %v", fmt.Errorf("%w", errEssentialInstruments))
		return false
	}
	return true
}

// isPerformanceSchemaEnabled checks if the Performance Schema is enabled in the MySQL database.
func isPerformanceSchemaEnabled(db dbconnection.DataSource) (bool, error) {
	var variableName, performanceSchemaEnabled string
	rows, err := db.QueryX("SHOW GLOBAL VARIABLES LIKE 'performance_schema';")

	if !rows.Next() {
		log.Error("No rows found")
		return false, nil
	}

	if errScanning := rows.Scan(&variableName, &performanceSchemaEnabled); err != nil {
		commonutils.FatalIfErr(errScanning)
	}

	if err != nil {
		return false, fmt.Errorf("failed to check Performance Schema status: %w", err)
	}
	return performanceSchemaEnabled == "ON", nil
}

// checkEssentialConsumers checks if the essential consumers are enabled in the Performance Schema.
func checkEssentialConsumers(db dbconnection.DataSource) error {
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
	}

	// Build the query to check the status of essential consumers
	query := "SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN ("
	for i, consumer := range consumers {
		if i > 0 {
			query += ", "
		}
		query += fmt.Sprintf("'%s'", consumer)
	}
	query += ");"

	rows, err := db.QueryX(query)
	if err != nil {
		return fmt.Errorf("failed to check essential consumers: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Error("Failed to close rows: %v", err)
		}
	}()

	// Check if each essential consumer is enabled
	for rows.Next() {
		var name, enabled string
		if err := rows.Scan(&name, &enabled); err != nil {
			return fmt.Errorf("failed to scan consumer row: %w", err)
		}
		if enabled != "YES" {
			log.Error("Essential consumer %s is not enabled. To enable it, run: UPDATE performance_schema.setup_consumers SET ENABLED = 'YES' WHERE NAME = '%s';", name, name)
			return fmt.Errorf("%w: %s", commonutils.ErrEssentialConsumerNotEnabled, name)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}

	return nil
}

// checkEssentialInstruments checks if the essential instruments are enabled in the Performance Schema.
func checkEssentialInstruments(db dbconnection.DataSource) error {
	instruments := []string{
		// Add other essential instruments here
		"wait/%",
		"statement/%",
		"%lock%",
	}

	// Pre-allocate the slice with the expected length
	instrumentConditions := make([]string, 0, len(instruments))
	for _, instrument := range instruments {
		instrumentConditions = append(instrumentConditions, fmt.Sprintf("NAME LIKE '%s'", instrument))
	}

	// Build the query to check the status of essential instruments
	query := "SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments WHERE "
	query += strings.Join(instrumentConditions, " OR ")
	query += ";"

	rows, err := db.QueryX(query)
	if err != nil {
		return fmt.Errorf("failed to check essential instruments: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Error("Failed to close rows: %v", err)
		}
	}()

	// Check if each essential instrument is enabled and timed
	for rows.Next() {
		var name, enabled string
		var timed sql.NullString
		if err := rows.Scan(&name, &enabled, &timed); err != nil {
			return fmt.Errorf("failed to scan instrument row: %w", err)
		}
		if enabled != "YES" || (timed.Valid && timed.String != "YES") {
			log.Error("Essential instrument %s is not fully enabled. To enable it, run: UPDATE performance_schema.setup_instruments SET ENABLED = 'YES', TIMED = 'YES' WHERE NAME = '%s';", name, name)
			return fmt.Errorf("%w: %s", commonutils.ErrEssentialInstrumentNotEnabled, name)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}

	return nil
}

// logEnablePerformanceSchemaInstructions logs instructions to enable the Performance Schema.
func logEnablePerformanceSchemaInstructions(db dbconnection.DataSource) {
	version, err := getMySQLVersion(db)
	if err != nil {
		log.Error("Failed to get MySQL version: %v", err)
		return
	}

	if isVersion8OrGreater(version) {
		log.Debug("To enable the Performance Schema, add the following lines to your MySQL configuration file (my.cnf or my.ini) in the [mysqld] section and restart the MySQL server:")
		log.Debug("performance_schema=ON")

		log.Debug("For MySQL 8.0 and higher, you may also need to set the following variables:")
		log.Debug("performance_schema_instrument='%%=ON'")
		log.Debug("performance_schema_consumer_events_statements_current=ON")
		log.Debug("performance_schema_consumer_events_statements_history=ON")
		log.Debug("performance_schema_consumer_events_statements_history_long=ON")
		log.Debug("performance_schema_consumer_events_waits_current=ON")
		log.Debug("performance_schema_consumer_events_waits_history=ON")
		log.Debug("performance_schema_consumer_events_waits_history_long=ON")
	} else {
		log.Error("MySQL version %s is not supported. Only version 8.0+ is supported.", version)
	}
}

// getMySQLVersion retrieves the MySQL version from the database.
func getMySQLVersion(db dbconnection.DataSource) (string, error) {
	query := "SELECT VERSION();"
	rows, err := db.QueryX(query)
	if err != nil {
		return "", fmt.Errorf("failed to execute version query: %w", err)
	}
	defer rows.Close()

	var version string
	if rows.Next() {
		if err := rows.Scan(&version); err != nil {
			return "", fmt.Errorf("failed to scan version: %w", err)
		}
	}

	if version == "" {
		return "", commonutils.ErrMySQLVersion
	}

	return version, nil
}

// isVersion8OrGreater checks if the MySQL version is 8.0 or greater.
func isVersion8OrGreater(version string) bool {
	majorVersion, minorVersion := parseVersion(version)
	return (majorVersion > 8) || (majorVersion == 8 && minorVersion >= 0)
}

// parseVersion extracts the major and minor version numbers from the version string
func parseVersion(version string) (int, int) {
	parts := strings.Split(version, ".")
	if len(parts) < minVersionParts {
		return 0, 0 // Return 0 if the version string is improperly formatted
	}

	majorVersion, err := strconv.Atoi(parts[0])
	if err != nil {
		log.Error("Failed to parse major version: %v", err)
		return 0, 0
	}

	minorVersion, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Error("Failed to parse minor version: %v", err)
		return 0, 0
	}

	return majorVersion, minorVersion
}
