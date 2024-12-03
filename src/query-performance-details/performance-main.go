package query_performance_details

import (
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
	query_details "github.com/newrelic/nri-mysql/src/query-performance-details/query-details"
	"github.com/newrelic/nri-mysql/src/query-performance-details/validator"
	log "github.com/sirupsen/logrus"
)

// main
func PopulateQueryPerformanceMetrics(args arguments.ArgumentList, e *integration.Entity) {
	dsn := performance_database.GenerateDSN(args)
	db, err := performance_database.OpenDB(dsn)
	common_utils.FatalIfErr(err)
	defer db.Close()
	isPreConditionsPassed := validator.ValidatePreconditions(db)
	if !isPreConditionsPassed {
		log.Error("Preconditions failed. Exiting.")
		return
	}

	queryIdList := query_details.PopulateSlowQueryMetrics(e, db, args)

	individualQueryDetails, individualQueryDetailsErr := query_details.PopulateIndividualQueryDetails(db, queryIdList, e, args)
	if individualQueryDetailsErr != nil {
		// log.Error("Error populating individual query details: %v", individualQueryDetailsErr)
		return
	}
	// fmt.Println("Individual Query details collected successfully.", individualQueryDetails)

	_, executionPlanMetricsErr := query_details.PopulateExecutionPlans(db, individualQueryDetails, e, args)
	if executionPlanMetricsErr != nil {
		// log.Error("Error populating execution plan details: %v", executionPlanMetricsErr)
		return
	}
	// fmt.Println("Execution Plan details collected successfully.", executionPlanMetrics)

	// waitEventMetrics, waitEventError := query_details.PopulateWaitEventMetrics(db, e, args)
	// if waitEventError != nil {
	// 	log.Error("Error populating wait event metrics: %v", waitEventError)
	// 	return
	// }
	// fmt.Println("Wait Event Metrics collected successfully.", waitEventMetrics)
	// blockingSessionMetrics, populateBlockingSessionMetricsError := query_details.PopulateBlockingSessionMetrics(db, e, args)
	// if populateBlockingSessionMetricsError != nil {
	// 	log.Error("Error populating blocking session metrics: %v", populateBlockingSessionMetricsError)
	// 	return
	// }
	// fmt.Println("Blocking Session Metrics collected successfully.", blockingSessionMetrics)
}
