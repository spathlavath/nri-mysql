package queryperformancemonitoring

import (
	"fmt"
	"os"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	mysql_apm "github.com/newrelic/nri-mysql/src/query-performance-monitoring/mysql-apm"
	performancemetricscollectors "github.com/newrelic/nri-mysql/src/query-performance-monitoring/performance-metrics-collectors"
	utils "github.com/newrelic/nri-mysql/src/query-performance-monitoring/utils"
	validator "github.com/newrelic/nri-mysql/src/query-performance-monitoring/validator"
)

// main
func PopulateQueryPerformanceMetrics(args arguments.ArgumentList, e *integration.Entity, i *integration.Integration) {
	var database string

	mysql_apm.ArgsGlobal = args.LicenseKey
	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName("nri-mysql-integration"),
		newrelic.ConfigLicense(args.LicenseKey),
		newrelic.ConfigDebugLogger(os.Stderr),
		newrelic.ConfigDatastoreRawQuery(true),
	)
	if err != nil {
		log.Error("Error creating new relic application: %s", err.Error())
	}

	mysql_apm.NewrelicApp = *app

	txn := app.StartTransaction("performance_monitoring")
	defer txn.End()
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
	// Generate Data Source Name (DSN) for database connection
	dsn := utils.GenerateDSN(args, database)

	// Open database connection
	db, err := utils.OpenDB(dsn)
	utils.FatalIfErr(err)
	defer db.Close()

	// Validate preconditions before proceeding
	preValidationErr := validator.ValidatePreconditions(db)
	if preValidationErr != nil {
		utils.FatalIfErr(fmt.Errorf("preconditions failed: %w", preValidationErr))
	}

	// Get the list of unique excluded databases
	excludedDatabases := utils.GetExcludedDatabases(args.ExcludedDatabases)

	// Populate metrics for slow queries
	start := time.Now()
	slowQueriesTxn := app.StartTransaction("MysqlSlowQueriesSample")
	log.Debug("Beginning to retrieve slow query metrics")
	queryIDList := performancemetricscollectors.PopulateSlowQueryMetrics(app, i, e, db, args, excludedDatabases)
	log.Debug("Completed fetching slow query metrics in %v", time.Since(start))
	defer slowQueriesTxn.End()

	if len(queryIDList) > 0 {
		// Populate metrics for individual queries
		start = time.Now()
		IndividualTxn := app.StartTransaction("MysqlIndividualQueriesSample")
		log.Debug("Beginning to retrieve individual query metrics")
		groupQueriesByDatabase := performancemetricscollectors.PopulateIndividualQueryDetails(app, db, queryIDList, i, e, args)
		log.Debug("Completed fetching individual query metrics in %v", time.Since(start))
		defer IndividualTxn.End()

		// Populate execution plan details
		start = time.Now()
		execPlanTxn := app.StartTransaction("MysqlQueryExecutionSample")
		log.Debug("Beginning to retrieve query execution plan metrics")
		performancemetricscollectors.PopulateExecutionPlans(app, db, groupQueriesByDatabase, i, e, args)
		log.Debug("Completed fetching query execution plan metrics in %v", time.Since(start))
		defer execPlanTxn.End()
	}

	// Populate wait event metrics
	start = time.Now()
	waitEventsTxn := app.StartTransaction("MysqlWaitEventsSample")
	log.Debug("Beginning to retrieve wait event metrics")
	performancemetricscollectors.PopulateWaitEventMetrics(app, db, i, e, args, excludedDatabases)
	log.Debug("Completed fetching wait event metrics in %v", time.Since(start))
	defer waitEventsTxn.End()

	// Populate blocking session metrics
	start = time.Now()
	blockingSessionsTxn := app.StartTransaction("MysqlBlockingSessionSample")
	log.Debug("Beginning to retrieve blocking session metrics")
	performancemetricscollectors.PopulateBlockingSessionMetrics(app, db, i, e, args, excludedDatabases)
	log.Debug("Completed fetching blocking session metrics in %v", time.Since(start))
	log.Debug("Query analysis completed.")
	defer blockingSessionsTxn.End()
}
