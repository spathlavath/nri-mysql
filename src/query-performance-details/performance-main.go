package queryperformancedetails

import (
	"fmt"
	"time"

	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	commonutils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	dbconnection "github.com/newrelic/nri-mysql/src/query-performance-details/connection"
	performancemetrics "github.com/newrelic/nri-mysql/src/query-performance-details/performance-metrics"
	"github.com/newrelic/nri-mysql/src/query-performance-details/validator"
)

// main
func PopulateQueryPerformanceMetrics(args arguments.ArgumentList, e *integration.Entity, i *integration.Integration) {
	var database string

	// Generate Data Source Name (DSN) for database connection
	dsn := dbconnection.GenerateDSN(args, database)

	// Open database connection
	db, err := dbconnection.OpenDB(dsn)
	commonutils.FatalIfErr(err)
	defer db.Close()

	// Validate preconditions before proceeding
	preValidationErr := validator.ValidatePreconditions(db)
	if preValidationErr != nil {
		commonutils.FatalIfErr(fmt.Errorf("preconditions failed: %w", preValidationErr))
	}

	// Get the list of unique excluded databases
	excludedDatabases, err := commonutils.GetExcludedDatabases(args.ExcludedDatabases)
	if err != nil {
		commonutils.FatalIfErr(fmt.Errorf("error unmarshaling json: %w", err))
	}

	// Populate metrics for slow queries
	start := time.Now()
	log.Debug("Beginning to retrieve slow query metrics")
	queryIDList := performancemetrics.PopulateSlowQueryMetrics(i, e, db, args, excludedDatabases)
	log.Debug("Completed fetching slow query metrics in %v", time.Since(start))

	if len(queryIDList) > 0 {
		// Populate metrics for individual queries
		start = time.Now()
		log.Debug("Beginning to retrieve individual query metrics")
		groupQueriesByDatabase, individualQueryDetailsErr := performancemetrics.PopulateIndividualQueryDetails(db, queryIDList, i, e, args)
		log.Debug("Completed fetching individual query metrics in %v", time.Since(start))
		if individualQueryDetailsErr != nil {
			commonutils.FatalIfErr(fmt.Errorf("error populating individual query details: %w", individualQueryDetailsErr))
		}

		// Populate execution plan details
		start = time.Now()
		log.Debug("Beginning to retrieve query execution plan metrics")
		executionPlanMetricsErr := performancemetrics.PopulateExecutionPlans(db, groupQueriesByDatabase, i, e, args)
		log.Debug("Completed fetching query execution plan metrics in %v", time.Since(start))
		if executionPlanMetricsErr != nil {
			commonutils.FatalIfErr(fmt.Errorf("error populating execution plan details: %w", executionPlanMetricsErr))
		}
	}

	// Populate wait event metrics
	start = time.Now()
	log.Debug("Beginning to retrieve wait event metrics")
	waitEventError := performancemetrics.PopulateWaitEventMetrics(db, i, e, args, excludedDatabases)
	log.Debug("Completed fetching wait event metrics in %v", time.Since(start))
	if waitEventError != nil {
		commonutils.FatalIfErr(fmt.Errorf("error populating wait event metrics: %w", waitEventError))
	}

	// Populate blocking session metrics
	start = time.Now()
	log.Debug("Beginning to retrieve blocking session metrics")
	populateBlockingSessionMetricsError := performancemetrics.PopulateBlockingSessionMetrics(db, i, e, args, excludedDatabases)
	log.Debug("Completed fetching blocking session metrics in %v", time.Since(start))
	if populateBlockingSessionMetricsError != nil {
		commonutils.FatalIfErr(fmt.Errorf("error populating blocking session metrics: %w", populateBlockingSessionMetricsError))
	}
	log.Debug("Query analysis completed.")
}
