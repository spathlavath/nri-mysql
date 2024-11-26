package query_performance_details

import (
	"fmt"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	arguments "github.com/newrelic/nri-mysql/src/args"
	common_utils "github.com/newrelic/nri-mysql/src/query-performance-details/common-utils"
	"github.com/newrelic/nri-mysql/src/query-performance-details/performance-database"
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
		fmt.Println("Preconditions failed. Exiting.")
		return
	}

	queryIdList := query_details.PopulateSlowQueryMetrics(e, db, args)

	individualQueryDetails, individualQueryDetailsErr := query_details.PopulateIndividualQueryDetails(db, queryIdList, e, args)
	if individualQueryDetailsErr != nil {
		fmt.Errorf("Error populating individual query details: %v", individualQueryDetailsErr)
		return
	}
	fmt.Println("Query Plan details collected successfully.", individualQueryDetails)

	executionPlanMetrics, executionPlanMetricsErr := query_details.PopulateExecutionPlans(db, individualQueryDetails, e, args)
	if executionPlanMetricsErr != nil {
		fmt.Errorf("Error populating execution plan details: %v", executionPlanMetricsErr)
		return
	}
	fmt.Println("Execution plan details collected successfully.", executionPlanMetrics)

	_, waitEventError := query_details.PopulateWaitEventMetrics(db, e, args)
	if waitEventError != nil {
		fmt.Errorf("Error populating wait event metrics: %v", waitEventError)
		return
	}

	_, populateBlockingSessionMetricsError := query_details.PopulateBlockingSessionMetrics(db, e, args)
	if populateBlockingSessionMetricsError != nil {
		fmt.Errorf("Error populating blocking session metrics: %v", populateBlockingSessionMetricsError)
		return
	}

}
