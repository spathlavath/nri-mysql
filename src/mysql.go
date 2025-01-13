//go:generate goversioninfo
package main

import (
	"fmt"

	"github.com/newrelic/infra-integrations-sdk/v3/data/attribute"
	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"

	"net"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/newrelic/go-agent/v3/newrelic"
	arguments "github.com/newrelic/nri-mysql/src/args"
	queryperformancemonitoring "github.com/newrelic/nri-mysql/src/query-performance-monitoring"
	mysql_apm "github.com/newrelic/nri-mysql/src/query-performance-monitoring/mysql-apm"
)

const (
	integrationName = "com.newrelic.mysql"
	nodeEntityType  = "node"
)

func generateDSN(args arguments.ArgumentList) string {
	// Format query parameters
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
		log.Info("Socket parameter is defined, ignoring host and port parameters")
		return fmt.Sprintf("%s:%s@unix(%s)/%s?%s", args.Username, args.Password, args.Socket, args.Database, query.Encode())
	}

	// Convert hostname and port to DSN address format
	mysqlURL := net.JoinHostPort(args.Hostname, strconv.Itoa(args.Port))

	return fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", args.Username, args.Password, mysqlURL, args.Database, query.Encode())
}

var (
	args               arguments.ArgumentList
	integrationVersion = "0.0.0"
	gitCommit          = ""
	buildDate          = ""
)

func createNodeEntity(
	i *integration.Integration,
	remoteMonitoring bool,
	hostname string,
	port int,
) (*integration.Entity, error) {

	if remoteMonitoring {
		return i.Entity(fmt.Sprint(hostname, ":", port), nodeEntityType)
	}
	return i.LocalEntity(), nil
}

func main() {
	i, err := integration.New(integrationName, integrationVersion, integration.Args(&args))
	fatalIfErr(err)
	mysql_apm.ArgsGlobal = args.LicenseKey
	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName("nri-mysql-integration"),
		newrelic.ConfigLicense(args.LicenseKey),
		newrelic.ConfigAppLogForwardingEnabled(true),
		newrelic.ConfigDistributedTracerEnabled(true),
	)
	if err != nil {
		log.Error("Error creating new relic application: %s", err.Error())
	}

	mysql_apm.NewrelicApp = *app

	txn := app.StartTransaction("test_performance_monitoring")
	defer txn.End()
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	if args.ShowVersion {
		fmt.Printf(
			"New Relic %s integration Version: %s, Platform: %s, GoVersion: %s, GitCommit: %s, BuildDate: %s\n",
			strings.Title(strings.Replace(integrationName, "com.newrelic.", "", 1)),
			integrationVersion,
			fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
			runtime.Version(),
			gitCommit,
			buildDate)
		os.Exit(0)
	}

	log.SetupLogging(args.Verbose)

	e, err := createNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
	fatalIfErr(err)

	db, err := openDB(generateDSN(args))
	fatalIfErr(err)
	defer db.close()

	rawInventory, rawMetrics, err := getRawData(db)
	fatalIfErr(err)

	if args.HasInventory() {
		populateInventory(e.Inventory, rawInventory)
	}

	if args.HasMetrics() {
		ms := metricSet(
			e,
			"MysqlSample",
			args.Hostname,
			args.Port,
			args.RemoteMonitoring,
		)
		populateMetrics(ms, rawMetrics)
	}
	fatalIfErr(i.Publish())

	if args.EnableQueryPerformance {
		queryperformancemonitoring.PopulateQueryPerformanceMetrics(args, e, i, app)
	}
}

func metricSet(e *integration.Entity, eventType, hostname string, port int, remoteMonitoring bool) *metric.Set {
	if remoteMonitoring {
		return e.NewMetricSet(
			eventType,
			attribute.Attr("hostname", hostname),
			attribute.Attr("port", strconv.Itoa(port)),
		)
	}

	return e.NewMetricSet(
		eventType,
		attribute.Attr("port", strconv.Itoa(port)),
	)
}

func fatalIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
