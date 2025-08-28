package queryperformancemonitoring

import (
	"fmt"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	dbutils "github.com/newrelic/nri-mysql/src/dbutils"
	infrautils "github.com/newrelic/nri-mysql/src/infrautils"
	mysqlapm "github.com/newrelic/nri-mysql/src/query-performance-monitoring/mysql-apm"
	performancemetricscollectors "github.com/newrelic/nri-mysql/src/query-performance-monitoring/performance-metrics-collectors"
	utils "github.com/newrelic/nri-mysql/src/query-performance-monitoring/utils"
	validator "github.com/newrelic/nri-mysql/src/query-performance-monitoring/validator"
)

// PopulateQueryPerformanceMetrics serves as the entry point for retrieving and populating query performance metrics, including slow queries, detailed query information, query execution plans, wait events, and blocking sessions.
func PopulateQueryPerformanceMetrics(app *newrelic.Application, args arguments.ArgumentList, e *integration.Entity, i *integration.Integration) {
	// Generate Data Source Name (DSN) for database connection
	dsn := dbutils.GenerateDSN(args, "")

	// Open database connection
	db, err := utils.OpenSQLXDB(dsn)
	infrautils.FatalIfErr(err)
	defer db.Close()

	// Validate preconditions before proceeding
	preValidationErr := validator.ValidatePreconditions(app, db)
	if preValidationErr != nil {
		infrautils.FatalIfErr(fmt.Errorf("preconditions failed: %w", preValidationErr))
	}

	// Get the list of unique excluded databases
	excludedDatabases := utils.GetExcludedDatabases(args.ExcludedPerformanceDatabases)

	// Populate metrics for slow queries
	start := time.Now()
	slowQueriesTxn := mysqlapm.NewrelicApp.StartTransaction("MysqlSlowQueriesSample")
	defer slowQueriesTxn.End()
	if slowQueriesTxn == nil {
		log.Error("Failed to start New Relic transaction for slow queries")
		return
	}
	mysqlapm.Txn = slowQueriesTxn
	log.Debug("Beginning to retrieve slow query metrics")
	queryIDList := performancemetricscollectors.PopulateSlowQueryMetrics(&mysqlapm.NewrelicApp, i, db, args, excludedDatabases)
	log.Debug("Completed fetching slow query metrics in %v", time.Since(start))

	if len(queryIDList) > 0 {
		// Populate metrics for individual queries
		start = time.Now()
		individualTxn := mysqlapm.NewrelicApp.StartTransaction("MysqlIndividualQueriesSample")
		if individualTxn == nil {
			log.Error("Failed to start New Relic transaction for individual queries")
			return
		}
		mysqlapm.Txn = individualTxn
		log.Debug("Beginning to retrieve individual query metrics")
		groupQueriesByDatabase, individualQueryDetailsErr := performancemetricscollectors.PopulateIndividualQueryDetails(&mysqlapm.NewrelicApp, db, queryIDList, i, args)
		if individualQueryDetailsErr != nil {
			log.Error("Error populating individual query details: %v", individualQueryDetailsErr)
		}
		log.Debug("Completed fetching individual query metrics in %v", time.Since(start))
		defer individualTxn.End()

		
		// Populate execution plan details
		start = time.Now()
		execPlanTxn := mysqlapm.NewrelicApp.StartTransaction("MysqlQueryExecutionSample")
		if execPlanTxn == nil {
			log.Error("Failed to start New Relic transaction for query execution plans")
			return
		}
		mysqlapm.Txn = execPlanTxn
		log.Debug("Beginning to retrieve query execution plan metrics")
		performancemetricscollectors.PopulateExecutionPlans(&mysqlapm.NewrelicApp, db, groupQueriesByDatabase, i, args)
		log.Debug("Completed fetching query execution plan metrics in %v", time.Since(start))
		defer execPlanTxn.End()
	}

	// Populate wait event metrics
	start = time.Now()
	waitEventsTxn := mysqlapm.NewrelicApp.StartTransaction("MysqlWaitEventsSample")
	if waitEventsTxn == nil {
		log.Error("Failed to start New Relic transaction for wait events")
		return
	}
	mysqlapm.Txn = waitEventsTxn
	log.Debug("Beginning to retrieve wait event metrics")
	performancemetricscollectors.PopulateWaitEventMetrics(&mysqlapm.NewrelicApp, db, i, args, excludedDatabases)
	log.Debug("Completed fetching wait event metrics in %v", time.Since(start))

	// Populate blocking session metrics
	start = time.Now()
	blockingSessionsTxn := mysqlapm.NewrelicApp.StartTransaction("MysqlBlockingSessionSample")
	if blockingSessionsTxn == nil {
		log.Error("Failed to start New Relic transaction for blocking sessions")
		return
	}
	mysqlapm.Txn = blockingSessionsTxn
	log.Debug("Beginning to retrieve blocking session metrics")
	performancemetricscollectors.PopulateBlockingSessionMetrics(&mysqlapm.NewrelicApp, db, i, args, excludedDatabases)
	log.Debug("Completed fetching blocking session metrics in %v", time.Since(start))
	log.Debug("Query analysis completed.")
	defer blockingSessionsTxn.End()
}
