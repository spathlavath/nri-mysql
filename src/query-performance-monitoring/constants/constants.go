package constants

import "time"

const (
	IntegrationName = "com.newrelic.mysql"
	NodeEntityType  = "node"
	// MetricSetLimit defines the maximum number of metrics that can be collected in a single set.
	MetricSetLimit = 100
	// ExplainQueryFormat is the format string for generating EXPLAIN queries in JSON format.
	ExplainQueryFormat = "EXPLAIN FORMAT=JSON %s"
	// SupportedStatements lists the SQL statements that are supported by this integration
	// for fetching query execution plans.
	SupportedStatements = "SELECT INSERT UPDATE DELETE WITH"
	// QueryPlanTimeoutDuration defines the timeout duration for fetching query plans
	QueryPlanTimeoutDuration = 10 * time.Second
	// TimeoutDuration defines the timeout duration for fetching slow query metrics, individual query metrics, wait event metrics, and blocked session metrics
	TimeoutDuration = 5 * time.Second
	// MaxQueryCountThreshold specifies the upper limit for the number of collected queries, as customers might opt for a higher query count threshold, potentially leading to performance problems.
	MaxQueryCountThreshold = 30
	// IndividualQueryCountThreshold specifies the upper limit for the number of individual queries to be collected, as customers might choose a higher query count threshold, potentially leading to performance problems.
	IndividualQueryCountThreshold = 10
	// MinVersionParts defines the minimum number of version parts
	MinVersionParts = 2
)

// DefaultExcludedDatabases defines a list of database names that are excluded by default.
// These databases are typically system databases in MySQL that are used for internal purposes
// and typically do not require user interactions or modifications.
//
//   - "mysql": This database contains the system user accounts and privileges information.
//   - "information_schema": This database provides access to database metadata,
//     i.e., data about data. It is read-only and is used for querying about database objects.
//   - "performance_schema": This database provides performance-related data and metrics
//     about server execution and resource usage. It is mainly used for monitoring purposes.
//   - "sys": This database provides simplified views and functions for easier system
//     administration and performance tuning.
//   - "": The empty string is included because some queries may not be associated with
//     any specific database. Including "" ensures that these undetermined or global queries
//     are not incorrectly related to a specific user database.
//
// Excluding these databases by default helps to prevent accidental modifications
// and focuses system operations only on user-defined databases.
var DefaultExcludedDatabases = []string{"", "mysql", "information_schema", "performance_schema", "sys"}
