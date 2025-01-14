package utils

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/newrelic/go-agent/v3/integrations/nrmysql"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	constants "github.com/newrelic/nri-mysql/src/query-performance-monitoring/constants"
	mysql_apm "github.com/newrelic/nri-mysql/src/query-performance-monitoring/mysql-apm"
)

type DataSource interface {
	Close()
	QueryX(string) (*sqlx.Rows, error)
	QueryxContext(app *newrelic.Application, ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
}

type Database struct {
	source *sqlx.DB
}

func OpenDB(dsn string) (DataSource, error) {
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
	fatalIfErr(err)
	return rows, err
}

func fatalIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// QueryxContext method implementation
func (db *Database) QueryxContext(app *newrelic.Application, ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	// Initialize New Relic application
	if app == nil {
		var err error
		app, err = newrelic.NewApplication(
			newrelic.ConfigAppName("nri-mysql-integration"),
			newrelic.ConfigLicense(mysql_apm.ArgsGlobal),
			newrelic.ConfigDebugLogger(os.Stdout),
			newrelic.ConfigDatastoreRawQuery(true),
		)
		if err != nil {
			log.Error("Error creating new relic application: %s", err.Error())
			return nil, err
		}
	}
	waitErr := app.WaitForConnection(5 * time.Second)
	if waitErr != nil {
		log.Error("Error waiting for connection: %s", waitErr.Error())
		return nil, waitErr
	}

	txn := app.StartTransaction("nrmysqlQuery")
	ctx = newrelic.NewContext(ctx, txn)
	s := newrelic.DatastoreSegment{
		StartTime: txn.StartSegmentNow(),
		Product:   newrelic.DatastoreMySQL,
		Operation: "SELECT",
		ParameterizedQuery: query,
	}
	defer s.End()
	return db.source.QueryxContext(ctx, query, args...)
}

func GenerateDSN(args arguments.ArgumentList, database string) string {
	query := url.Values{}
	if args.OldPasswords {
		query.Add("allowOldPasswords", "true")
	}
	if args.EnableTLS {
		query.Add("tls", "true")
	}
	if args.InsecureSkipVerify {
		query.Add("tls", "skip-verify")
	}
	extraArgsMap, err := url.ParseQuery(args.ExtraConnectionURLArgs)
	if err == nil {
		for k, v := range extraArgsMap {
			query.Add(k, v[0])
		}
	} else {
		log.Warn("Could not successfully parse ExtraConnectionURLArgs.", err.Error())
	}
	if args.Socket != "" {
		log.Debug("Socket parameter is defined, ignoring host and port parameters")
		return fmt.Sprintf("%s:%s@unix(%s)/%s?%s", args.Username, args.Password, args.Socket, determineDatabase(args, database), query.Encode())
	}

	// Convert hostname and port to DSN address format
	mysqlURL := net.JoinHostPort(args.Hostname, strconv.Itoa(args.Port))

	return fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", args.Username, args.Password, mysqlURL, determineDatabase(args, database), query.Encode())
}

// determineDatabase determines which database name to use for the DSN.
func determineDatabase(args arguments.ArgumentList, database string) string {
	if database != "" {
		return database
	}
	return args.Database
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
