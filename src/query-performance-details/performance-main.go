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
	dsn := performance_database.GenerateDSN(args)
	db, err := performance_database.OpenDB(dsn)
	common_utils.FatalIfErr(err)
	defer db.Close()
	isPreConditionsPassed := validator.ValidatePreconditions(db)
	if !isPreConditionsPassed {
		log.Error("Preconditions failed. Exiting.")
		return
	}
	// Slow Queries
	queryIdList := query_details.PopulateSlowQueryMetrics(i, e, db, args)

	// Individual Queries
	individualQueryDetails, individualQueryDetailsErr := query_details.PopulateIndividualQueryDetails(db, queryIdList, i, e, args)
	if individualQueryDetailsErr != nil {
		log.Error("Error populating individual query details: %v", individualQueryDetailsErr)
		return
	}

	// Execution Plan details
	_, executionPlanMetricsErr := query_details.PopulateExecutionPlans(db, individualQueryDetails, i, e, args)
	if executionPlanMetricsErr != nil {
		log.Error("Error populating execution plan details: %v", executionPlanMetricsErr)
		return
	}

	// Wait Events
	_, waitEventError := query_details.PopulateWaitEventMetrics(db, i, e, args)
	if waitEventError != nil {
		log.Error("Error populating wait event metrics: %v", waitEventError)
		return
	}

	// Blocking Sessions
	_, populateBlockingSessionMetricsError := query_details.PopulateBlockingSessionMetrics(db, i, e, args)
	if populateBlockingSessionMetricsError != nil {
		log.Error("Error populating blocking session metrics: %v", populateBlockingSessionMetricsError)
		return
	}
}
