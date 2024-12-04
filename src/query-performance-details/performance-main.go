package query_performance_details

import (
	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	performance_database "github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
	query_details "github.com/newrelic/nri-mysql/src/query-performance-details/query-details"
	"github.com/newrelic/nri-mysql/src/query-performance-details/validator"
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
	// Slow Queries
	queryIdList := query_details.PopulateSlowQueryMetrics(e, db, args)

	// Individual Queries
	individualQueryDetails, individualQueryDetailsErr := query_details.PopulateIndividualQueryDetails(db, queryIdList, e, args)
	if individualQueryDetailsErr != nil {
		log.Error("Error populating individual query details: %v", individualQueryDetailsErr)
		return
	}
	// fmt.Println("Individual Query details collected successfully.", individualQueryDetails)

	mm := common_utils.CreateMetricSet(e, "MysqlQueryExecutionaaaassdsdsdsfdaa", args)
	mm.SetMetric("query_id", "aaaaa", metric.ATTRIBUTE)

	_, executionPlanMetricsErr := query_details.PopulateExecutionPlan(db, individualQueryDetails, e, args)
	if executionPlanMetricsErr != nil {
		log.Error("Error populating execution plan details: %v", executionPlanMetricsErr)
		return
	}
	// _, executionPlanMetricsErr := query_details.PopulateExecutionPlans(db, individualQueryDetails, e, args)
	// if executionPlanMetricsErr != nil {
	// 	log.Error("Error populating execution plan details: %v", executionPlanMetricsErr)
	// 	return
	// }
	// fmt.Println("Execution Plan details collected successfully.", executionPlanMetrics)

	// Wait Events
	// waitEventMetrics, waitEventError := query_details.PopulateWaitEventMetrics(db, e, args)
	// if waitEventError != nil {
	// 	log.Error("Error populating wait event metrics: %v", waitEventError)
	// 	return
	// }
	// fmt.Println("Wait Event Metrics collected successfully.", waitEventMetrics)

	// // Blocking Sessions
	// blockingSessionMetrics, populateBlockingSessionMetricsError := query_details.PopulateBlockingSessionMetrics(db, e, args)
	// if populateBlockingSessionMetricsError != nil {
	// 	log.Error("Error populating blocking session metrics: %v", populateBlockingSessionMetricsError)
	// 	return
	// }
	// fmt.Println("Blocking Session Metrics collected successfully.", blockingSessionMetrics)
}
