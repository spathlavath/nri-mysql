package query_details

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/nri-mysql/src/args"
	arguments "github.com/newrelic/nri-mysql/src/args"
	performance_data_model "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockEntity struct {
	integration.Entity
}

type MockIntegration struct {
	mock.Mock
	*integration.Integration
}

func TestPopulateExecutionPlans(t *testing.T) {
	t.Run("EmptyQueryList", func(t *testing.T) {
		db, _, err := sqlmock.New()
		assert.NoError(t, err)
		defer db.Close()

		sqlxDB := sqlx.NewDb(db, "sqlmock")
		dataSource := NewMockDataSource(sqlxDB)
		queries := []performance_data_model.QueryGroup{}
		i, _ := integration.New("test", "1.0.0")
		e := i.LocalEntity()
		args := args.ArgumentList{}

		metrics, err := PopulateExecutionPlans(dataSource, queries, i, e, args)
		assert.NoError(t, err)
		assert.Len(t, metrics, 0)
	})

	t.Run("UnsupportedQuery", func(t *testing.T) {
		db, _, err := sqlmock.New()
		assert.NoError(t, err)
		defer db.Close()

		sqlxDB := sqlx.NewDb(db, "sqlmock")
		dataSource := NewMockDataSource(sqlxDB)
		dropTableQuery := "DROP TABLE test"
		queries := []performance_data_model.QueryGroup{
			{Queries: []performance_data_model.IndividualQueryMetrics{{QueryText: &dropTableQuery}}},
		}
		i, _ := integration.New("test", "1.0.0")
		e := i.LocalEntity()
		args := args.ArgumentList{}

		metrics, err := PopulateExecutionPlans(dataSource, queries, i, e, args)
		assert.NoError(t, err)
		assert.Len(t, metrics, 0)
	})
}

func TestExtractMetricsFromJSONString(t *testing.T) {
	t.Run("Invalid JSON String", func(t *testing.T) {
		metrics, err := extractMetricsFromJSONString("invalid json", 1)
		assert.Error(t, err)
		assert.Nil(t, metrics)
	})

	t.Run("Valid JSON String", func(t *testing.T) {
		jsonString := `{"table_name": "test", "cost_info": {"query_cost": "10"}, "access_type": "ALL", "rows_examined_per_scan": 1, "rows_produced_per_join": 1, "filtered": "100", "read_cost": "5", "eval_cost": "5", "possible_keys": ["key1"], "key": "key1", "used_key_parts": ["part1"], "ref": ["ref1"], "attached_condition": "condition"}`
		metrics, err := extractMetricsFromJSONString(jsonString, 1)
		assert.NoError(t, err)
		assert.NotNil(t, metrics)
	})
}

func TestSetExecutionPlanMetrics(t *testing.T) {
	mockIntegration := new(MockIntegration)
	mockIntegration.Integration, _ = integration.New("test", "1.0.0") // Properly initialize the Integration field
	mockArgs := arguments.ArgumentList{}

	t.Run("Successful Ingestion", func(t *testing.T) {
		metrics := []performance_data_model.QueryPlanMetrics{
			{EventID: 1, QueryCost: "10", TableName: "test"},
		}
		mockIntegration.On("IngestMetric", mock.Anything, "MysqlQueryExecutionSample", mockIntegration, mockArgs).Return(nil)

		err := SetExecutionPlanMetrics(mockIntegration.Integration, mockArgs, metrics)
		assert.NoError(t, err)
	})

	t.Run("Empty Metrics", func(t *testing.T) {
		metrics := []performance_data_model.QueryPlanMetrics{}
		err := SetExecutionPlanMetrics(mockIntegration.Integration, mockArgs, metrics)
		assert.NoError(t, err)
	})
}

func TestIsSupportedStatement(t *testing.T) {
	t.Run("Supported Statement", func(t *testing.T) {
		assert.True(t, isSupportedStatement("SELECT * FROM test"))
		assert.True(t, isSupportedStatement("INSERT INTO test VALUES (1)"))
		assert.True(t, isSupportedStatement("UPDATE test SET value = 1"))
		assert.True(t, isSupportedStatement("DELETE FROM test"))
		assert.True(t, isSupportedStatement("WITH cte AS (SELECT * FROM test) SELECT * FROM cte"))
	})

	t.Run("Unsupported Statement", func(t *testing.T) {
		assert.False(t, isSupportedStatement("DROP TABLE test"))
		assert.False(t, isSupportedStatement("ALTER TABLE test ADD COLUMN value INT"))
	})
}
