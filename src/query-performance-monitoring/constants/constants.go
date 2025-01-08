package constants

import "time"

const (
	IntegrationName = "com.newrelic.mysql"
	NodeEntityType  = "node"
	MetricSetLimit  = 100
	// TimeoutDuration defines the timeout duration for database queries
	TimeoutDuration               = 5 * time.Second
	MaxQueryCountThreshold        = 30
	IndividualQueryCountThreshold = 10
	MinVersionParts               = 2
)
