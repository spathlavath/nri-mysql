package queryperformancemonitoring

import (
	"fmt"
	"time"

	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	performancemetricscollectors "github.com/newrelic/nri-mysql/src/query-performance-monitoring/performance-metrics-collectors"
	utils "github.com/newrelic/nri-mysql/src/query-performance-monitoring/utils"
)

// main
func PopulateQueryPerformanceMetrics(args arguments.ArgumentList, e *integration.Entity, i *integration.Integration) {
	var database string

	// Generate Data Source Name (DSN) for database connection
	dsn := utils.GenerateDSN(args, database)

	// Open database connection
	db, err := utils.OpenDB(dsn)
	utils.FatalIfErr(err)
	defer db.Close()

	// Validate preconditions before proceeding
	preValidationErr := ValidatePreconditions(db)
	if preValidationErr != nil {
		utils.FatalIfErr(fmt.Errorf("preconditions failed: %w", preValidationErr))
	}

	// Get the list of unique excluded databases
	excludedDatabases, err := utils.GetExcludedDatabases(args.ExcludedDatabases)
	if err != nil {
		utils.FatalIfErr(fmt.Errorf("error unmarshaling json: %w", err))
	}

	// Populate metrics for slow queries
	start := time.Now()
	log.Debug("Beginning to retrieve slow query metrics")
	queryIDList := performancemetricscollectors.PopulateSlowQueryMetrics(i, e, db, args, excludedDatabases)
	log.Debug("Completed fetching slow query metrics in %v", time.Since(start))

	if len(queryIDList) > 0 {
		// Populate metrics for individual queries
		start = time.Now()
		log.Debug("Beginning to retrieve individual query metrics")
		groupQueriesByDatabase, individualQueryDetailsErr := performancemetricscollectors.PopulateIndividualQueryDetails(db, queryIDList, i, e, args)
		log.Debug("Completed fetching individual query metrics in %v", time.Since(start))
		if individualQueryDetailsErr != nil {
			utils.FatalIfErr(fmt.Errorf("error populating individual query details: %w", individualQueryDetailsErr))
		}

		// Populate execution plan details
		start = time.Now()
		log.Debug("Beginning to retrieve query execution plan metrics")
		executionPlanMetricsErr := performancemetricscollectors.PopulateExecutionPlans(db, groupQueriesByDatabase, i, e, args)
		log.Debug("Completed fetching query execution plan metrics in %v", time.Since(start))
		if executionPlanMetricsErr != nil {
			utils.FatalIfErr(fmt.Errorf("error populating execution plan details: %w", executionPlanMetricsErr))
		}
	}

	// Populate wait event metrics
	start = time.Now()
	log.Debug("Beginning to retrieve wait event metrics")
	waitEventError := performancemetricscollectors.PopulateWaitEventMetrics(db, i, e, args, excludedDatabases)
	log.Debug("Completed fetching wait event metrics in %v", time.Since(start))
	if waitEventError != nil {
		utils.FatalIfErr(fmt.Errorf("error populating wait event metrics: %w", waitEventError))
	}

	// Populate blocking session metrics
	start = time.Now()
	log.Debug("Beginning to retrieve blocking session metrics")
	populateBlockingSessionMetricsError := performancemetricscollectors.PopulateBlockingSessionMetrics(db, i, e, args, excludedDatabases)
	log.Debug("Completed fetching blocking session metrics in %v", time.Since(start))
	if populateBlockingSessionMetricsError != nil {
		utils.FatalIfErr(fmt.Errorf("error populating blocking session metrics: %w", populateBlockingSessionMetricsError))
	}
	log.Debug("Query analysis completed.")
}
