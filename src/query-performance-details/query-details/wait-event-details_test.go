package query_details

import (
	"testing"

	"database/sql"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/nri-mysql/src/args"
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	queries "github.com/newrelic/nri-mysql/src/query-performance-details/queries"
	"github.com/stretchr/testify/assert"
)

func TestPopulateWaitEventMetrics(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	sqlxDB := sqlx.NewDb(db, "sqlmock")
	assert.NoError(t, err)
	defer db.Close()
	mockDB := &MockDataSource{db: sqlxDB}
	// mockDB := &MockDataSource{db: db}

	query := queries.WaitEventsQuery
	rows := sqlmock.NewRows([]string{"query_id", "query_text", "database_name", "wait_category", "collection_timestamp", "instance_id", "wait_event_name", "wait_event_count", "avg_wait_time_ms", "total_wait_time_ms"}).
		AddRow("1234", "SELECT * FROM table", "test_db", "wait_category", "2023-01-01T00:00:00Z", "1", "wait_event", 10, 1.23, 12.3)
	mock.ExpectQuery(query).WillReturnRows(rows)

	i, err := integration.New("testIntegration", "1.0.0")
	assert.NoError(t, err)

	e := i.LocalEntity()
	args := args.ArgumentList{}

	metrics, err := PopulateWaitEventMetrics(mockDB, e, args)
	assert.NoError(t, err)
	assert.Len(t, metrics, 1)

	expectedMetric := performance_data_model.WaitEventQueryMetrics{
		QueryID:             sql.NullString{String: "1234", Valid: true},
		QueryText:           sql.NullString{String: "SELECT * FROM table", Valid: true},
		DatabaseName:        sql.NullString{String: "test_db", Valid: true},
		WaitCategory:        "wait_category",
		CollectionTimestamp: "2023-01-01T00:00:00Z",
		InstanceID:          "1",
		WaitEventName:       "wait_event",
		WaitEventCount:      10,
		AvgWaitTimeMs:       "1.23",
		TotalWaitTimeMs:     12.3,
	}

	assert.Equal(t, expectedMetric, metrics[0])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSetWaitEventMetrics_ValidMetrics(t *testing.T) {
	i, err := integration.New("testIntegration", "1.0.0")
	assert.NoError(t, err)

	e := i.LocalEntity()
	args := args.ArgumentList{}
	metrics := []performance_data_model.WaitEventQueryMetrics{
		{
			QueryID:             sql.NullString{String: "1234", Valid: true},
			QueryText:           sql.NullString{String: "SELECT * FROM table", Valid: true},
			DatabaseName:        sql.NullString{String: "test_db", Valid: true},
			WaitCategory:        "wait_category",
			CollectionTimestamp: "2023-01-01T00:00:00Z",
			InstanceID:          "1",
			WaitEventName:       "wait_event",
			WaitEventCount:      10,
			AvgWaitTimeMs:       "1.23",
			TotalWaitTimeMs:     12.3,
		},
	}

	err = setWaitEventMetrics(e, args, metrics)
	assert.NoError(t, err)

	ms := e.Metrics[0]
	assert.Equal(t, "1234", ms.Metrics["query_id"])
	assert.Equal(t, "SELECT * FROM table", ms.Metrics["query_text"])
	assert.Equal(t, "test_db", ms.Metrics["database_name"])
	assert.Equal(t, "wait_category", ms.Metrics["wait_category"])
	assert.Equal(t, "2023-01-01T00:00:00Z", ms.Metrics["collection_timestamp"])
	assert.Equal(t, "1", ms.Metrics["instance_id"])
	assert.Equal(t, "wait_event", ms.Metrics["wait_event_name"])
	assert.Equal(t, float64(10), ms.Metrics["wait_event_count"])
	assert.Equal(t, 1.23, ms.Metrics["avg_wait_time_ms"])
	assert.Equal(t, 12.3, ms.Metrics["total_wait_time_ms"])
}

func TestSetWaitEventMetrics_EmptyMetrics(t *testing.T) {
	i, err := integration.New("testIntegration", "1.0.0")
	assert.NoError(t, err)

	e := i.LocalEntity()
	args := args.ArgumentList{}
	metrics := []performance_data_model.WaitEventQueryMetrics{}

	err = setWaitEventMetrics(e, args, metrics)
	assert.NoError(t, err)
	assert.Empty(t, e.Metrics)
}

func TestSetWaitEventMetrics_NilEntity(t *testing.T) {
	args := args.ArgumentList{}
	metrics := []performance_data_model.WaitEventQueryMetrics{
		{
			QueryID:             sql.NullString{String: "1234", Valid: true},
			QueryText:           sql.NullString{String: "SELECT * FROM table", Valid: true},
			DatabaseName:        sql.NullString{String: "test_db", Valid: true},
			WaitCategory:        "wait_category",
			CollectionTimestamp: "2023-01-01T00:00:00Z",
			InstanceID:          "1",
			WaitEventName:       "wait_event",
			WaitEventCount:      10,
			AvgWaitTimeMs:       "1.23",
			TotalWaitTimeMs:     12.3,
		},
	}

	err := setWaitEventMetrics(nil, args, metrics)
	assert.Error(t, err)
}
