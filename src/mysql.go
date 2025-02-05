//go:generate goversioninfo
package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	dbutils "github.com/newrelic/nri-mysql/src/dbutils"
	infrautils "github.com/newrelic/nri-mysql/src/infrautils"
	queryperformancemonitoring "github.com/newrelic/nri-mysql/src/query-performance-monitoring"
	constants "github.com/newrelic/nri-mysql/src/query-performance-monitoring/constants"
	mysqlapm "github.com/newrelic/nri-mysql/src/query-performance-monitoring/mysql-apm"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	args               arguments.ArgumentList
	integrationVersion = "0.0.0"
	gitCommit          = ""
	buildDate          = ""
)

func main() {
	i, err := integration.New(constants.IntegrationName, integrationVersion, integration.Args(&args))
	infrautils.FatalIfErr(err)

	mysqlapm.ArgsKey = args.LicenseKey
	mysqlapm.ArgsAppName = args.AppName
	mysqlapm.InitNewRelicApp()

	if mysqlapm.ArgsAppName != "" {
		defer mysqlapm.NewrelicApp.Shutdown(10 * time.Second)
	}

	if args.ShowVersion {
		fmt.Printf(
			"New Relic %s integration Version: %s, Platform: %s, GoVersion: %s, GitCommit: %s, BuildDate: %s\n",
			cases.Title(language.Und).String(strings.Replace(constants.IntegrationName, "com.newrelic.", "", 1)),
			integrationVersion,
			fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
			runtime.Version(),
			gitCommit,
			buildDate)
		os.Exit(0)
	}

	log.SetupLogging(args.Verbose)

	txn := mysqlapm.NewrelicApp.StartTransaction("MysqlSampleOld")
	if txn == nil {
		log.Error("Failed to start New Relic transaction for mysql sample old")
		return
	}

	mysqlapm.Txn = txn
	e, err := infrautils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
	infrautils.FatalIfErr(err)

	db, err := openSQLDB(dbutils.GenerateDSN(args, ""))
	infrautils.FatalIfErr(err)
	defer db.close()

	rawInventory, rawMetrics, dbVersion, err := getRawData(&mysqlapm.NewrelicApp, db)
	infrautils.FatalIfErr(err)

	if args.HasInventory() {
		populateInventory(e.Inventory, rawInventory)
	}

	if args.HasMetrics() {
		ms := infrautils.MetricSet(
			e,
			"MysqlSampleOld",
			args.Hostname,
			args.Port,
			args.RemoteMonitoring,
		)
		populateMetrics(ms, rawMetrics, dbVersion)
	}
	infrautils.FatalIfErr(i.Publish())

	if args.EnableQueryMonitoring {
		queryperformancemonitoring.PopulateQueryPerformanceMetrics(args, e, i)
	}
	defer txn.End()
}
