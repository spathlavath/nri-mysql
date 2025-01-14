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
		newrelic.ConfigDebugLogger(os.Stdout),
		newrelic.ConfigDatastoreRawQuery(true),
	)
	if err != nil {
		log.Error("Error creating new relic application: %s", err.Error())
	}
	defer app.Shutdown(10 * time.Second)

	// Ensure the application is connected
	if err := app.WaitForConnection(10 * time.Second); err != nil {
		log.Debug("New Relic Application did not connect:", err)
		return
	}

	// Log application connection status
	if app != nil {
		log.Debug("New Relic application initialized successfully")
		mysql_apm.NewrelicApp = *app
	} else {
		log.Error("New Relic application initialization failed")
	}

	// Generate Data Source Name (DSN) for database connection
	dsn := utils.GenerateDSN(args, database)

	// Open database connection
	db, err := utils.OpenDB(dsn)
	utils.FatalIfErr(err)
	defer db.Close()

	preCheckTxn := app.StartTransaction("MysqlSlowQueriesSample")
	// Validate preconditions before proceeding
	preValidationErr := validator.ValidatePreconditions(db)
	if preValidationErr != nil {
		utils.FatalIfErr(fmt.Errorf("preconditions failed: %w", preValidationErr))
	}
	preCheckTxn.End()

	// Get the list of unique excluded databases
	excludedDatabases := utils.GetExcludedDatabases(args.ExcludedDatabases)

	// Populate metrics for slow queries
	start := time.Now()
	slowQueriesTxn := app.StartTransaction("MysqlSlowQueriesSample")
	defer slowQueriesTxn.End()
	if slowQueriesTxn == nil {
		log.Error("Failed to start New Relic transaction for slow queries")
		return
	}
	log.Debug("Beginning to retrieve slow query metrics")
	queryIDList := performancemetricscollectors.PopulateSlowQueryMetrics(app, i, e, db, args, excludedDatabases)
	log.Debug("Completed fetching slow query metrics in %v", time.Since(start))

	if len(queryIDList) > 0 {
		// Populate metrics for individual queries
		start = time.Now()
		individualTxn := app.StartTransaction("MysqlIndividualQueriesSample")
		if individualTxn == nil {
			log.Error("Failed to start New Relic transaction for individual queries")
			return
		}
		log.Debug("Beginning to retrieve individual query metrics")
		groupQueriesByDatabase := performancemetricscollectors.PopulateIndividualQueryDetails(app, db, queryIDList, i, e, args)
		log.Debug("Completed fetching individual query metrics in %v", time.Since(start))
		defer individualTxn.End()

		// Populate execution plan details
		start = time.Now()
		execPlanTxn := app.StartTransaction("MysqlQueryExecutionSample")
		if execPlanTxn == nil {
			log.Error("Failed to start New Relic transaction for query execution plans")
			return
		}
		log.Debug("Beginning to retrieve query execution plan metrics")
		performancemetricscollectors.PopulateExecutionPlans(app, db, groupQueriesByDatabase, i, e, args)
		log.Debug("Completed fetching query execution plan metrics in %v", time.Since(start))
		defer execPlanTxn.End()
	}

	// Populate wait event metrics
	start = time.Now()
	waitEventsTxn := app.StartTransaction("MysqlWaitEventsSample")
	if waitEventsTxn == nil {
		log.Error("Failed to start New Relic transaction for wait events")
		return
	}
	log.Debug("Beginning to retrieve wait event metrics")
	performancemetricscollectors.PopulateWaitEventMetrics(app, db, i, e, args, excludedDatabases)
	log.Debug("Completed fetching wait event metrics in %v", time.Since(start))
	defer waitEventsTxn.End()

	// Populate blocking session metrics
	start = time.Now()
	blockingSessionsTxn := app.StartTransaction("MysqlBlockingSessionSample")
	if blockingSessionsTxn == nil {
		log.Error("Failed to start New Relic transaction for blocking sessions")
		return
	}
	log.Debug("Beginning to retrieve blocking session metrics")
	performancemetricscollectors.PopulateBlockingSessionMetrics(app, db, i, e, args, excludedDatabases)
	log.Debug("Completed fetching blocking session metrics in %v", time.Since(start))
	log.Debug("Query analysis completed.")
	defer blockingSessionsTxn.End()
}
