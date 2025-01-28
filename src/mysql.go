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
	queryperformancemonitoring "github.com/newrelic/nri-mysql/src/query-performance-monitoring"
	constants "github.com/newrelic/nri-mysql/src/query-performance-monitoring/constants"
	mysqlapm "github.com/newrelic/nri-mysql/src/query-performance-monitoring/mysql-apm"
	utils "github.com/newrelic/nri-mysql/src/query-performance-monitoring/utils"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	args      arguments.ArgumentList
	gitCommit = ""
	buildDate = ""
)

func main() {
	i, err := integration.New(constants.IntegrationName, constants.IntegrationVersion, integration.Args(&args))
	utils.FatalIfErr(err)

	mysqlapm.ArgsKey = args.LicenseKey
	mysqlapm.ArgsAppName = args.AppName
	mysqlapm.InitNewRelicApp()

	if args.ShowVersion {
		fmt.Printf(
			"New Relic %s integration Version: %s, Platform: %s, GoVersion: %s, GitCommit: %s, BuildDate: %s\n",
			cases.Title(language.Und).String(strings.Replace(constants.IntegrationName, "com.newrelic.", "", 1)),
			constants.IntegrationVersion,
			fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
			runtime.Version(),
			gitCommit,
			buildDate)
		os.Exit(0)
	}

	log.SetupLogging(args.Verbose)

	txn := mysqlapm.NewrelicApp.StartTransaction("MysqlSampleOld")
	defer txn.End()

	e, err := utils.CreateNodeEntity(i, args.RemoteMonitoring, args.Hostname, args.Port)
	utils.FatalIfErr(err)

	db, err := openSQLDB(utils.GenerateDSN(args, ""))
	utils.FatalIfErr(err)
	defer db.close()

	rawInventory, rawMetrics, dbVersion, err := getRawData(db)
	utils.FatalIfErr(err)

	if args.HasInventory() {
		populateInventory(e.Inventory, rawInventory)
	}

	if args.HasMetrics() {
		ms := utils.MetricSet(
			e,
			"MysqlSample",
			args.Hostname,
			args.Port,
			args.RemoteMonitoring,
		)
		populateMetrics(ms, rawMetrics, dbVersion)
	}
	utils.FatalIfErr(i.Publish())

	if args.EnableQueryMonitoring && args.HasMetrics() {
		queryperformancemonitoring.PopulateQueryPerformanceMetrics(args, e, i)
	}

	if mysqlapm.ArgsAppName != "" {
		defer mysqlapm.NewrelicApp.Shutdown(10 * time.Second)
	}
}
