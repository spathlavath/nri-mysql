package queryperformancemonitoring

import (
	"context"
	"fmt"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	performancemetricscollectors "github.com/newrelic/nri-mysql/src/query-performance-monitoring/performance-metrics-collectors"
	utils "github.com/newrelic/nri-mysql/src/query-performance-monitoring/utils"
	validator "github.com/newrelic/nri-mysql/src/query-performance-monitoring/validator"
)

// main
func PopulateQueryPerformanceMetrics(args arguments.ArgumentList, e *integration.Entity, i *integration.Integration, app *newrelic.Application) {
	var database string

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
	ctx := context.Background()
	txn := app.StartTransaction("MysqlSlowQueriesSample")
	ctx = newrelic.NewContext(ctx, txn)
	log.Debug("Beginning to retrieve slow query metrics")
	performancemetricscollectors.PopulateSlowQueryMetrics(ctx, i, e, db, args, excludedDatabases)
	log.Debug("Completed fetching slow query metrics in %v", time.Since(start))
	defer txn.End()

	// if len(queryIDList) > 0 {
	// 	// Populate metrics for individual queries
	// 	start = time.Now()
	// 	IndividualTxn := app.StartTransaction("MysqlIndividualQueriesSample")
	// 	log.Debug("Beginning to retrieve individual query metrics")
	// 	groupQueriesByDatabase := performancemetricscollectors.PopulateIndividualQueryDetails(app, db, queryIDList, i, e, args)
	// 	log.Debug("Completed fetching individual query metrics in %v", time.Since(start))
	// 	defer IndividualTxn.End()

	// 	// Populate execution plan details
	// 	start = time.Now()
	// 	execPlanTxn := app.StartTransaction("MysqlQueryExecutionSample")
	// 	log.Debug("Beginning to retrieve query execution plan metrics")
	// 	performancemetricscollectors.PopulateExecutionPlans(db, groupQueriesByDatabase, i, e, args)
	// 	log.Debug("Completed fetching query execution plan metrics in %v", time.Since(start))
	// 	defer execPlanTxn.End()
	// }

	// // Populate wait event metrics
	// start = time.Now()
	// waitEventsTxn := app.StartTransaction("MysqlWaitEventsSample")
	// log.Debug("Beginning to retrieve wait event metrics")
	// performancemetricscollectors.PopulateWaitEventMetrics(app, db, i, e, args, excludedDatabases)
	// log.Debug("Completed fetching wait event metrics in %v", time.Since(start))
	// defer waitEventsTxn.End()

	// // Populate blocking session metrics
	// start = time.Now()
	// blockingSessionsTxn := app.StartTransaction("MysqlBlockingSessionSample")
	// log.Debug("Beginning to retrieve blocking session metrics")
	// performancemetricscollectors.PopulateBlockingSessionMetrics(app, db, i, e, args, excludedDatabases)
	// log.Debug("Completed fetching blocking session metrics in %v", time.Since(start))
	// log.Debug("Query analysis completed.")
	// defer blockingSessionsTxn.End()
}
