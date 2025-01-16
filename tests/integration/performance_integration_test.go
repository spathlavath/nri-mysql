//go:build integration_performance_metrics

package integration

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/newrelic/nri-mysql/tests/integration/helpers"
	"github.com/newrelic/nri-mysql/tests/integration/jsonschema"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	iName = "mysql"

	defaultPerfContainer = "integration_nri-mysql_perf_1"
	// mysql config
	defaultBinPath                = "/nri-mysql"
	defaultMysqlUser              = "root"
	defaultMysqlPass              = ""
	defaultMysqlPort              = 3306
	defaultEnableQueryPerformance = false
	defaultSlowQueryFetchInterval = 3000

	// cli flags
	perfContainer = flag.String("perfContainer", defaultPerfContainer, "container where the integration is installed and used for validating performance monitoring metrics")
	binPath                = flag.String("bin", defaultBinPath, "Integration binary path")
	user                   = flag.String("user", defaultMysqlUser, "Mysql user name")
	psw                    = flag.String("psw", defaultMysqlPass, "Mysql user password")
	port                   = flag.Int("port", defaultMysqlPort, "Mysql port")
	enableQueryPerformance = flag.Bool("enable_query_performance", defaultEnableQueryPerformance, "flag to enable and disable collecting performance metrics")
	slowQueryFetchInterval = flag.Int("slow_query_fetch_interval", defaultSlowQueryFetchInterval, "retrives slow queries that ran in last n seconds")
)

type MysqlPerformanceConfig struct {
	Version  string // Mysql server version
	Hostname string // Hostname for the Mysql service. (Will be the master mysql service inside the docker-compose file).
}

var (
	// Config's for version 8.0.40, 8.4.0 are commented because in github action's when we are trying to run mulitple mysql servers its failing
	
	MysqlPerfConfigs = []MysqlPerformanceConfig{
		{
			Version:  "8.0.40",
			Hostname: "mysql_perf_8-0-40",
		},
		{
			Version:  "8.4.0",
			Hostname: "mysql_perf_8-4-0",
		},
		{
			Version:  "9.1.0",
			Hostname: "mysql_perf_latest-supported",
		},
	}
)

func runIntegrationAndGetStdoutWithError(t *testing.T, targetContainer string, envVars ...string) (string, string, error) {
	t.Helper()

	command := make([]string, 0)
	command = append(command, *binPath)
	if user != nil {
		command = append(command, "-username="+*user)
	}
	if psw != nil {
		command = append(command, "-password="+*psw)
	}
	if targetContainer != "" {
		command = append(command, "-hostname="+targetContainer)
	}
	if port != nil {
		command = append(command, "-port="+strconv.Itoa(*port))
	}
	if slowQueryFetchInterval != nil {
		command = append(command, "-slow_query_fetch_interval="+strconv.Itoa(*slowQueryFetchInterval))
	}
	stdout, stderr, err := helpers.ExecInContainer(*perfContainer, command, envVars...)

	return stdout, stderr, err
}

func executeBlockingSessionQuery(mysqlPerfConfig MysqlPerformanceConfig) error {
	flag.Parse()

	if testing.Verbose() {
		log.SetLevel(log.DebugLevel)
	}

	masterErr := helpers.WaitForPort(*perfContainer, mysqlPerfConfig.Hostname, *port, 60*time.Second)
	if masterErr != nil {
		return masterErr
	}

	blockingSessionQuery := "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; USE employees; START TRANSACTION; UPDATE employees SET last_name = 'Blocking' WHERE emp_no = 10001;"
	blockingSessionCmd := []string{`mysql`, `-u`, `root`, `-e`, blockingSessionQuery}
	// uncomment line 110-113 and comment line 114 to see if the below mysql query is blocked and doesn't execute.
	// _, blockingSessionErr, err := helpers.ExecInContainer(mysqlPerfConfig.Hostname, blockingSessionCmd, fmt.Sprintf("MYSQL_PWD=%s", *psw))
	// if blockingSessionErr != "" {
	// 	log.Debug("Error exec blocking session queries: ", blockingSessionErr, err)
	// }
	go helpers.ExecInContainer(mysqlPerfConfig.Hostname, blockingSessionCmd, fmt.Sprintf("MYSQL_PWD=%s", *psw))
	log.Info("wait for the blocking session query to get executed for host :"+ mysqlPerfConfig.Hostname)
	time.Sleep(10 * time.Second)
	log.Info("wait complete")
	log.Info("Executing blocking sessions complete!")

	return nil
}

func teardown() error {
	return nil
}

func TestMain(m *testing.M) {
	log.Info("wait for mysql servers with performance schema/extensions enabled to come up and running")
	time.Sleep(80 * time.Second)
	log.Info("wait complete")
	for _, mysqlPerfConfig := range MysqlPerfConfigs {
		err := executeBlockingSessionQuery(mysqlPerfConfig)
		if err != nil {
			fmt.Println(err)
			tErr := teardown()
			if tErr != nil {
				fmt.Printf("Error during the teardown of the tests: %s\n", tErr)
			}
			os.Exit(1)
		}
	}

	result := m.Run()

	err := teardown()
	if err != nil {
		fmt.Printf("Error during the teardown of the tests: %s\n", err)
	}

	os.Exit(result)
}

func testPerfOutputIsValidJSON(t *testing.T, mysqlPerfConfig MysqlPerformanceConfig) {
	t.Run(mysqlPerfConfig.Version, func(t *testing.T) {
		stdout, stderr, err := runIntegrationAndGetStdoutWithError(t, mysqlPerfConfig.Hostname)
		if stderr != "" {
			log.Debug("Integration command Standard Error: ", stderr)
		}
		require.NoError(t, err)
		outputMetricsList := strings.Split(stdout, "\n")
		for _, outputMetrics := range outputMetricsList {
			outputMetrics = strings.TrimSpace(outputMetrics)
			if outputMetrics == "" {
				continue
			}
			var j map[string]interface{}
			err := json.Unmarshal([]byte(outputMetrics), &j)
			assert.NoError(t, err, "Integration output should be a JSON dict")
		}
	})
}

func TestPerfOutputIsValidJSON(t *testing.T) {
	for _, mysqlConfig := range MysqlPerfConfigs {
		testPerfOutputIsValidJSON(t, mysqlConfig)
	}
}

func runValidMysqlPerfConfigTest(t *testing.T, args []string, outputMetricsFile string, testName string) {
	for _, mysqlPerfConfig := range MysqlPerfConfigs {
		t.Run(testName+mysqlPerfConfig.Version, func(t *testing.T) {
			args = append(args, fmt.Sprintf("NRIA_CACHE_PATH=/tmp/%v.json", testName))
			stdout, stderr, err := runIntegrationAndGetStdoutWithError(t, mysqlPerfConfig.Hostname, args...)
			if stderr != "" {
				log.Debug("Integration command Standard Error: ", stderr)
			}
			require.NoError(t, err)
			outputMetricsList := strings.Split(stdout, "\n")
			outputMetricsConfigs := []struct {
				name           string
				stdout         string
				schemaFileName string
			}{
				{
					"DeafutlMetrics",
					outputMetricsList[0],
					outputMetricsFile,
				},
				{
					"SlowQueryMetrics",
					outputMetricsList[1],
					"mysql-schema-slow-queries.json",
				},
				{
					"IndividualQueryMetrics",
					outputMetricsList[2],
					"mysql-schema-individual-queries.json",
				},
				{
					"QueryExecutionMetrics",
					outputMetricsList[3],
					"mysql-schema-query-execution.json",
				},
				{
					"WaitEventsMetrics",
					outputMetricsList[4],
					"mysql-schema-wait-events.json",
				},
				{
					"BlockingSessionMetrics",
					outputMetricsList[5],
					"mysql-schema-blocking-sessions.json",
				},
			}
			for _, outputConfig := range outputMetricsConfigs {
				schemaPath := filepath.Join("json-schema-performance-files", outputConfig.schemaFileName)
				err := jsonschema.Validate(schemaPath, outputConfig.stdout)
				require.NoError(t, err, "The output of MySQL integration doesn't have expected format")
			}
		})
	}
}

func TestPerfMySQLIntegrationValidArguments(t *testing.T) {
	testCases := []struct {
		name              string
		args              []string
		outputMetricsFile string
	}{
		{
			name: "RemoteEntity_EnableQueryPerformance",
			args: []string{
				"REMOTE_MONITORING=true",
				"ENABLE_QUERY_PERFORMANCE=true",
			},
			outputMetricsFile: "mysql-schema-master.json",
		},
		{
			name: "LocalEntity_EnableQueryPerformance",
			args: []string{
				"ENABLE_QUERY_PERFORMANCE=true",
			},
			outputMetricsFile: "mysql-schema-master-localentity.json",
		},
		{
			name: "OnlyMetrics_EnableQueryPerformance",
			args: []string{
				"METRICS=true",
				"ENABLE_QUERY_PERFORMANCE=true",
			},
			outputMetricsFile: "mysql-schema-metrics-master.json",
		},
		{
			name: "OnlyInventory_EnableQueryPerformance",
			args: []string{
				"INVENTORY=true",
				"ENABLE_QUERY_PERFORMANCE=true",
			},
			outputMetricsFile: "mysql-schema-inventory-master.json",
		},
	}

	for _, testCase := range testCases {
		runValidMysqlPerfConfigTest(t, testCase.args, testCase.outputMetricsFile, testCase.name)
	}
}
