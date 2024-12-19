package query_performance_details

import (
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
	query_details "github.com/newrelic/nri-mysql/src/query-performance-details/query-details"
	"github.com/newrelic/nri-mysql/src/query-performance-details/validator"
)

// main
func PopulateQueryPerformanceMetrics(args arguments.ArgumentList, e *integration.Entity, i *integration.Integration) {
	var database string

	// Generate Data Source Name (DSN) for database connection
	dsn := performance_database.GenerateDSN(args, database)

	// Open database connection
	db, err := performance_database.OpenDB(dsn)
	common_utils.FatalIfErr(err)
	defer db.Close()

	// Validate preconditions before proceeding
	isPreConditionsPassed := validator.ValidatePreconditions(db)
	if !isPreConditionsPassed {
		log.Error("Preconditions failed. Exiting.")
		return
	}

	// Populate metrics for slow queries
	queryIdList := query_details.PopulateSlowQueryMetrics(i, e, db, args)

	if len(queryIdList) > 0 {
		// Populate metrics for individual queries
		groupQueriesByDatabase, individualQueryDetailsErr := query_details.PopulateIndividualQueryDetails(db, queryIdList, i, e, args)
		if individualQueryDetailsErr != nil {
			log.Error("Error populating individual query details: %v", individualQueryDetailsErr)
			return
		}

		// Populate execution plan details
		_, executionPlanMetricsErr := query_details.PopulateExecutionPlans(db, groupQueriesByDatabase, i, e, args)
		if executionPlanMetricsErr != nil {
			log.Error("Error populating execution plan details: %v", executionPlanMetricsErr)
			return
		}
	}

	// Populate wait event metrics
	_, waitEventError := query_details.PopulateWaitEventMetrics(db, i, e, args)
	if waitEventError != nil {
		log.Error("Error populating wait event metrics: %v", waitEventError)
		return
	}

	// Populate blocking session metrics
	_, populateBlockingSessionMetricsError := query_details.PopulateBlockingSessionMetrics(db, i, e, args)
	if populateBlockingSessionMetricsError != nil {
		log.Error("Error populating blocking session metrics: %v", populateBlockingSessionMetricsError)
		return
	}
}
