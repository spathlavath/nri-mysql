package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	constants "github.com/newrelic/nri-mysql/src/query-performance-monitoring/constants"
	mysqlapm "github.com/newrelic/nri-mysql/src/query-performance-monitoring/mysql-apm"
)

type dataSource interface {
	close()
	query(*newrelic.Application, string) (map[string]interface{}, error)
}

type database struct {
	source *sql.DB
}

/*
openSQLDB function creates and returns a connection using the database/sql package for basic SQL database interactions.
It provides methods like Query, QueryRow, Exec, etc., that facilitate executing SQL queries and commands.
This package is well-suited for applications needing standard SQL database operations.
*/
func openSQLDB(dsn string) (dataSource, error) {
	source, err := sql.Open("nrmysql", dsn)
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

/*
query executes provided as an argument query and parses the output to the map structure.
It is only possible to parse two types of query:
1. output of the query consists of two columns. Names of the columns are ignored. Values from the first
column are used as keys, and from the second as corresponding values of the map. Number of rows can be greater than 1;
2. output of the query consists of multiple columns, but only single row.
In this case, each column name is a key, and corresponding value is a map value.
*/
func (db *database) query(app *newrelic.Application, query string) (map[string]interface{}, error) {
	log.Debug("executing query: " + query)
	// rows, err := db.source.Query(query)
	if app != nil {
		mysqlapm.NewrelicApp = *app
	}
	ctx, cancel := context.WithTimeout(context.Background(), constants.TimeoutDuration)
	defer cancel()

	ctx = newrelic.NewContext(ctx, mysqlapm.Txn)
	s := newrelic.DatastoreSegment{
		StartTime:          mysqlapm.Txn.StartSegmentNow(),
		Product:            newrelic.DatastoreMySQL,
		Operation:          "SELECT",
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
