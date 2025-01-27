package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/newrelic/go-agent/v3/integrations/nrmysql"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	constants "github.com/newrelic/nri-mysql/src/query-performance-monitoring/constants"
	mysqlapm "github.com/newrelic/nri-mysql/src/query-performance-monitoring/mysql-apm"
)

type DataSource interface {
	Close()
	QueryX(string) (*sqlx.Rows, error)
	QueryxContext(app *newrelic.Application, ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
}

type Database struct {
	source *sqlx.DB
}

func OpenSQLXDB(dsn string) (DataSource, error) {
	source, err := sqlx.Open("nrmysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error opening DSN: %w", err)
	}

	db := Database{
		source: source,
	}

	return &db, nil
}

func (db *Database) Close() {
	db.source.Close()
}

func (db *Database) QueryX(query string) (*sqlx.Rows, error) {
	rows, err := db.source.Queryx(query)
	return rows, err
}

// QueryxContext method implementation
func (db *Database) QueryxContext(app *newrelic.Application, ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {

	waitErr := mysqlapm.NewrelicApp.WaitForConnection(5 * time.Second)
	if waitErr != nil {
		log.Error("Error waiting for connection: %s", waitErr.Error())
		return nil, waitErr
	}

	ctx = newrelic.NewContext(ctx, mysqlapm.Txn)
	s := newrelic.DatastoreSegment{
		StartTime:          mysqlapm.Txn.StartSegmentNow(),
		Product:            newrelic.DatastoreMySQL,
		Operation:          "SELECT",
		ParameterizedQuery: query,
	}
	defer s.End()
	return db.source.QueryxContext(ctx, query, args...)
}

// collectMetrics collects metrics from the performance schema database
func CollectMetrics[T any](app *newrelic.Application, db DataSource, preparedQuery string, preparedArgs ...interface{}) ([]T, error) {
	ctx, cancel := context.WithTimeout(context.Background(), constants.TimeoutDuration)
	defer cancel()

	rows, err := db.QueryxContext(app, ctx, preparedQuery, preparedArgs...)
	if err != nil {
		return []T{}, err
	}
	defer rows.Close()

	var metrics []T
	for rows.Next() {
		var metric T
		if err := rows.StructScan(&metric); err != nil {
			return []T{}, err
		}
		metrics = append(metrics, metric)
	}
	if err := rows.Err(); err != nil {
		return []T{}, err
	}

	return metrics, nil
}
