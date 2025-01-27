package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	constants "github.com/newrelic/nri-mysql/src/query-performance-monitoring/constants"
	mysqlapm "github.com/newrelic/nri-mysql/src/query-performance-monitoring/mysql-apm"
)

type dataSource interface {
	close()
	query(string) (map[string]interface{}, error)
}

type database struct {
	source *sql.DB
}

func openSQLDB(dsn string) (dataSource, error) {
	source, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error opening %s: %w", dsn, err)
	}

	db := database{
		source: source,
	}

	return &db, nil
}

func (db *database) close() {
	db.source.Close()
}

func (db *database) query(query string) (map[string]interface{}, error) {
	log.Debug("executing query: " + query)

	ctx, cancel := context.WithTimeout(context.Background(), constants.TimeoutDuration)
	defer cancel()

	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName(mysqlapm.ArgsAppName),
		newrelic.ConfigLicense(mysqlapm.ArgsKey),
		newrelic.ConfigDebugLogger(os.Stdout),
		newrelic.ConfigDatastoreRawQuery(true),
	)
	if err != nil {
		log.Error("Error creating new relic application: %s", err.Error())
		return nil, err
	}
	waitErr := app.WaitForConnection(5 * time.Second)
	if waitErr != nil {
		log.Error("Error waiting for connection: %s", waitErr.Error())
		return nil, waitErr
	}

	txn := app.StartTransaction("MysqlSample")
	defer txn.End()

	ctx = newrelic.NewContext(ctx, txn)
	s := newrelic.DatastoreSegment{
		StartTime:          txn.StartSegmentNow(),
		Product:            newrelic.DatastoreMySQL,
		Operation:          "SHOW",
		ParameterizedQuery: query,
	}
	defer s.End()

	rows, err := db.source.QueryContext(ctx, query)

	if err != nil {
		return nil, fmt.Errorf("error executing `%s`: %v", query, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Warn(fmt.Sprintf("error closing rows: %v", err))
		}
	}()

	rawData := make(map[string]interface{})

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting columns from query: %v", err)
	}

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	rowIndex := 0
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("error scanning rows[%d]: %v", rowIndex, err)
		}

		if len(values) == 2 {
			var val1, val2 interface{}
			if err := rows.Scan(&val1, &val2); err != nil {
				return nil, fmt.Errorf("error scanning rows[%d]: %v", rowIndex, err)
			}
			rawData[fmt.Sprintf("%v", val1)] = asValue(fmt.Sprintf("%v", val2))

		} else {
			if rowIndex != 0 {
				log.Debug("Cannot process query: %s, for query output with more than 2 columns only single row expected", query)
				break
			}
			for i := range values {
				var val interface{}
				if err := rows.Scan(&val); err != nil {
					return nil, fmt.Errorf("error scanning rows[%d]: %v", rowIndex, err)
				}
				rawData[columns[i]] = asValue(fmt.Sprintf("%v", val))
			}
			rowIndex++
		}
	}

	return rawData, nil
}
