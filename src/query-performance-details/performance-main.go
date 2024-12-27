package queryperformancedetails

import (
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
	isPreConditionsPassed := validator.ValidatePreconditions(db)
	if !isPreConditionsPassed {
		log.Error("Preconditions failed. Exiting.")
		return
	}

	// Populate metrics for slow queries
	start := time.Now()
	log.Info("Beginning to retrieve slow query metrics")
	queryIDList := performancemetrics.PopulateSlowQueryMetrics(i, e, db, args)
	log.Info("Completed fetching slow query metrics in %v", time.Since(start))

	if len(queryIDList) > 0 {
		// Populate metrics for individual queries
		start = time.Now()
		log.Info("Beginning to retrieve individual query metrics")
		groupQueriesByDatabase, individualQueryDetailsErr := performancemetrics.PopulateIndividualQueryDetails(db, queryIDList, i, e, args)
		log.Info("Completed fetching individual query metrics in %v", time.Since(start))
		if individualQueryDetailsErr != nil {
			log.Error("Error populating individual query details: %v", individualQueryDetailsErr)
			return
		}

		// Populate execution plan details
		start = time.Now()
		log.Info("Beginning to retrieve query execution plan metrics")
		_, executionPlanMetricsErr := performancemetrics.PopulateExecutionPlans(db, groupQueriesByDatabase, i, e, args)
		log.Info("Completed fetching query execution plan metrics in %v", time.Since(start))
		if executionPlanMetricsErr != nil {
			log.Error("Error populating execution plan details: %v", executionPlanMetricsErr)
			return
		}
	}

	// Populate wait event metrics
	start = time.Now()
	log.Info("Beginning to retrieve wait event metrics")
	_, waitEventError := performancemetrics.PopulateWaitEventMetrics(db, i, e, args)
	log.Info("Completed fetching wait event metrics in %v", time.Since(start))
	if waitEventError != nil {
		log.Error("Error populating wait event metrics: %v", waitEventError)
		return
	}

	// Populate blocking session metrics
	start = time.Now()
	log.Info("Beginning to retrieve blocking session metrics")
	_, populateBlockingSessionMetricsError := performancemetrics.PopulateBlockingSessionMetrics(db, i, e, args)
	log.Info("Completed fetching blocking session metrics in %v", time.Since(start))
	if populateBlockingSessionMetricsError != nil {
		log.Error("Error populating blocking session metrics: %v", populateBlockingSessionMetricsError)
		return
	}
	log.Info("Query analysis completed.")
}
