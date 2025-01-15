//go:build integration

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

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/newrelic/nri-mysql/tests/integration/helpers"
	"github.com/newrelic/nri-mysql/tests/integration/jsonschema"
)

var (
	iName = "mysql"

	defaultContainer = "integration_nri-mysql_1"
	// mysql config
	defaultBinPath   = "/nri-mysql"
	defaultMysqlUser = "root"
	defaultMysqlPass = "DBpwd1234"
	defaultMysqlPort = 3306
	defaultMysqlDB   = "database"
	defaultEnableQueryPerformance = false
	defaultSlowQueryFetchInterval = 3000

	// cli flags
	container = flag.String("container", defaultContainer, "container where the integration is installed")
	binPath   = flag.String("bin", defaultBinPath, "Integration binary path")
	user      = flag.String("user", defaultMysqlUser, "Mysql user name")
	psw       = flag.String("psw", defaultMysqlPass, "Mysql user password")
	port      = flag.Int("port", defaultMysqlPort, "Mysql port")
	database  = flag.String("database", defaultMysqlDB, "Mysql database")
	enableQueryPerformance = flag.Bool("enable_query_performance", defaultEnableQueryPerformance, "flag to enable and disable collecting performance metrics")
	slowQueryFetchInterval = flag.Int("slow_query_fetch_interval", defaultSlowQueryFetchInterval, "retrives slow queries that ran in last n seconds")
)

type MysqlConfig struct {
	Version        string // Mysql server version
	MasterHostname string // MasterHostname for the Mysql service. (Will be the master mysql service inside the docker-compose file).
	SlaveHostname  string // SlaveHostname for the Mysql service. (Will be the slave mysql service inside the docker-compose file).
}

var (
	MysqlConfigs = []MysqlConfig{
		{
			Version:        "5.7.35",
			MasterHostname: "mysql_master-5-7-35",
			SlaveHostname:  "mysql_slave-5-7-35",
		},
		{
			/*
				The query cache variables are removed from MySQL 8.0 - https://dev.mysql.com/doc/refman/5.7/en/query-cache-status-and-maintenance.html
				Due to which the all qcache metrics are not supported from this version

				From MySQL 8.0.23 the statement CHANGE MASTER TO is deprecated. The alias CHANGE REPLICATION SOURCE TO should be used instead.
				The parameters for the statement also have aliases that replace the term MASTER with the term SOURCE.
				For example, MASTER_HOST and MASTER_PORT can now be entered as SOURCE_HOST and SOURCE_PORT.
				More Info - https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-23.html
			*/
			Version:        "8.0.40",
			MasterHostname: "mysql_master-8-0-40",
			SlaveHostname:  "mysql_slave-8-0-40",
		},
		{
			Version:        "9.1.0",
			MasterHostname: "mysql_master-latest-supported",
			SlaveHostname:  "mysql_slave-latest-supported",
		},
	}
)

type MysqlPerformanceConfig struct {
	Version  string // Mysql server version
	Hostname string // Hostname for the Mysql service. (Will be the master mysql service inside the docker-compose file).
}

var (
	// TODO uncomment the version 8.4.0, 9.1.0 after the epic branch is updated with main repo
	// TODO as check if the existing integration tests are passing
	// TODO add new test to check if we are getting expected error while running unconfigured mysql server with enable_query_performance flag as true
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

// Returns the standard output, or fails testing if the command returned an error
func runIntegration(t *testing.T, targetContainer string, envVars ...string) string {
	t.Helper()

	command := make([]string, 0)
	command = append(command, *binPath)
	if user != nil {
		command = append(command, "--username", *user)
	}
	if psw != nil {
		command = append(command, "--password", *psw)
	}
	if targetContainer != "" {
		command = append(command, "--hostname", targetContainer)
	}
	if port != nil {
		command = append(command, "--port", strconv.Itoa(*port))
	}
	if database != nil {
		command = append(command, "--database", *database)
	}
	stdout, stderr, err := helpers.ExecInContainer(*container, command, envVars...)
	if stderr != "" {
		log.Debug("Integration command Standard Error: ", stderr)
	}
	require.NoError(t, err)

	return stdout
}

func runIntegrationAndGetStdoutWithError(t *testing.T, targetContainer string, envVars ...string) (string, string, error) {
	t.Helper()

	command := make([]string, 0)
	command = append(command, *binPath)
	if user != nil {
		command = append(command, "--username", *user)
	}
	if psw != nil {
		command = append(command, "--password", *psw)
	}
	if targetContainer != "" {
		command = append(command, "--hostname", targetContainer)
	}
	if port != nil {
		command = append(command, "--port", strconv.Itoa(*port))
	}
	if slowQueryFetchInterval != nil {
		command = append(command, "-slow_query_fetch_interval="+strconv.Itoa(*slowQueryFetchInterval))
	}
	stdout, stderr, err := helpers.ExecInContainer(*container, command, envVars...)

	return stdout, stderr, err
}

func checkVersion(dbVersion string) bool {
	parts := strings.Split(dbVersion, ".")

	majorVersion, err1 := strconv.Atoi(parts[0])
	minorVersion, err2 := strconv.Atoi(parts[1])

	if err1 != nil || err2 != nil {
		return false
	}

	if majorVersion == 8 {
		if minorVersion >= 4 {
			return true
		} else {
			return false
		}
	} else if majorVersion > 8 {
		return true
	}
	return false
}

func isDBVersionLessThan8(dbVersion string) bool {
	parts := strings.Split(dbVersion, ".")

	majorVersion, err := strconv.Atoi(parts[0])
	if err != nil {
		return true
	}

	return majorVersion < 8
}

func setup(mysqlConfig MysqlConfig) error {
	flag.Parse()

	if testing.Verbose() {
		log.SetLevel(log.DebugLevel)
	}

	masterErr := helpers.WaitForPort(*container, mysqlConfig.MasterHostname, *port, 60*time.Second)
	if masterErr != nil {
		return masterErr
	}

	slaveErr := helpers.WaitForPort(*container, mysqlConfig.SlaveHostname, *port, 30*time.Second)
	if slaveErr != nil {
		return slaveErr
	}

	// Retrieve log filename and position from master
	var masterStatusQuery = ""
	if checkVersion(mysqlConfig.Version) {
		masterStatusQuery = `SHOW BINARY LOG STATUS;`
	} else {
		masterStatusQuery = `SHOW MASTER STATUS;`
	}
	masterStatusCmd := []string{`mysql`, `-u`, `root`, `-e`, masterStatusQuery}
	masterStatusOut, masterStatusErr, err := helpers.ExecInContainer(mysqlConfig.MasterHostname, masterStatusCmd, fmt.Sprintf("MYSQL_PWD=%s", *psw))
	if masterStatusErr != "" {
		log.Debug("Error fetching Master Log filename and Position: ", masterStatusErr)
		return err
	}

	masterStatus := strings.Fields(masterStatusOut)
	masterLogFile := masterStatus[5]
	masterLogPos := masterStatus[6]

	// Activate MASTER/SLAVE replication
	var replication_stmt = ""
	if isDBVersionLessThan8(mysqlConfig.Version) {
		replication_stmt = fmt.Sprintf(`CHANGE MASTER TO MASTER_HOST='%s', MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_LOG_FILE='%s', MASTER_LOG_POS=%v; START SLAVE;`, mysqlConfig.MasterHostname, *user, *psw, masterLogFile, masterLogPos)
	} else {
		replication_stmt = fmt.Sprintf(`CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_USER='%s', SOURCE_PASSWORD='%s', SOURCE_LOG_FILE='%s', SOURCE_LOG_POS=%v; START REPLICA; GRANT ALL ON *.* TO %s;`, mysqlConfig.MasterHostname, *user, *psw, masterLogFile, masterLogPos, *user)
	}
	replicationCmd := []string{`mysql`, `-u`, `root`, `-e`, replication_stmt}
	_, replicationStatusErr, err := helpers.ExecInContainer(mysqlConfig.SlaveHostname, replicationCmd, fmt.Sprintf("MYSQL_PWD=%s", *psw))
	if replicationStatusErr != "" {
		log.Debug("Error creating Master/Slave replication: ", replicationStatusErr)
		return err
	}
	log.Info("Setup Complete!")

	return nil
}

func executeBlockingSessionQuery(mysqlPerfConfig MysqlPerformanceConfig) error {
	flag.Parse()

	if testing.Verbose() {
		log.SetLevel(log.DebugLevel)
	}

	masterErr := helpers.WaitForPort(*container, mysqlPerfConfig.Hostname, *port, 60*time.Second)
	if masterErr != nil {
		return masterErr
	}

	// wait for the performance docker compose to run the same query we are using below to lock particular row.
	time.Sleep(10 * time.Second)

	blockingSessionQuery := "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; USE employees; START TRANSACTION; UPDATE employees SET last_name = 'Blocking' WHERE emp_no = 10001;"
	blockingSessionCmd := []string{`mysql`, `-u`, `root`, `-e`, blockingSessionQuery}
	// uncomment line 110-113 and comment line 114 to see if the below mysql query is blocked and doesn't execute.
	// _, blockingSessionErr, err := helpers.ExecInContainer(mysqlPerfConfig.Hostname, blockingSessionCmd, fmt.Sprintf("MYSQL_PWD=%s", *psw))
	// if blockingSessionErr != "" {
	// 	log.Debug("Error exec blocking session queries: ", blockingSessionErr, err)
	// }
	go helpers.ExecInContainer(mysqlPerfConfig.Hostname, blockingSessionCmd, fmt.Sprintf("MYSQL_PWD=%s", *psw))
	log.Info("Executing blocking sessions complete!")

	return nil
}

func teardown() error {
	return nil
}

func TestMain(m *testing.M) {
	for _, mysqlConfig := range MysqlConfigs {
		err := setup(mysqlConfig)
		if err != nil {
			fmt.Println(err)
			tErr := teardown()
			if tErr != nil {
				fmt.Printf("Error during the teardown of the tests: %s\n", tErr)
			}
			os.Exit(1)
		}
	}

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

func testOutputIsValidJSON(t *testing.T, mysqlConfig MysqlConfig) {
	stdout := runIntegration(t, mysqlConfig.MasterHostname)
	var j map[string]interface{}
	err := json.Unmarshal([]byte(stdout), &j)
	assert.NoError(t, err, "Integration output should be a JSON dict")
}

func TestOutputIsValidJSON(t *testing.T) {
	for _, mysqlConfig := range MysqlConfigs {
		testOutputIsValidJSON(t, mysqlConfig)
	}
}

func testMySQLIntegrationValidArguments_RemoteEntity(t *testing.T, mysqlConfig MysqlConfig) {
	testName := helpers.GetTestName(t)
	stdout := runIntegration(t, mysqlConfig.MasterHostname, fmt.Sprintf("NRIA_CACHE_PATH=/tmp/%v.json", testName), "REMOTE_MONITORING=true")
	schemaDir := fmt.Sprintf("json-schema-files-%s", mysqlConfig.Version)
	schemaPath := filepath.Join(schemaDir, "mysql-schema-master.json")
	err := jsonschema.Validate(schemaPath, stdout)
	require.NoError(t, err, "The output of MySQL integration doesn't have expected format")
}

func TestMySQLIntegrationValidArguments_RemoteEntity(t *testing.T) {
	for _, mysqlConfig := range MysqlConfigs {
		testMySQLIntegrationValidArguments_RemoteEntity(t, mysqlConfig)
	}
}

func testMySQLIntegrationValidArguments_LocalEntity(t *testing.T, mysqlConfig MysqlConfig) {
	testName := helpers.GetTestName(t)
	stdout := runIntegration(t, mysqlConfig.MasterHostname, fmt.Sprintf("NRIA_CACHE_PATH=/tmp/%v.json", testName))
	schemaDir := fmt.Sprintf("json-schema-files-%s", mysqlConfig.Version)
	schemaPath := filepath.Join(schemaDir, "mysql-schema-master-localentity.json")
	err := jsonschema.Validate(schemaPath, stdout)
	require.NoError(t, err, "The output of MySQL integration doesn't have expected format")
}

func TestMySQLIntegrationValidArguments_LocalEntity(t *testing.T) {
	for _, mysqlConfig := range MysqlConfigs {
		testMySQLIntegrationValidArguments_LocalEntity(t, mysqlConfig)
	}
}

func testMySQLIntegrationOnlyMetrics(t *testing.T, mysqlConfig MysqlConfig) {
	testName := helpers.GetTestName(t)
	stdout := runIntegration(t, mysqlConfig.MasterHostname, "METRICS=true", fmt.Sprintf("NRIA_CACHE_PATH=/tmp/%v.json", testName))
	schemaDir := fmt.Sprintf("json-schema-files-%s", mysqlConfig.Version)
	schemaPath := filepath.Join(schemaDir, "mysql-schema-metrics-master.json")
	err := jsonschema.Validate(schemaPath, stdout)
	require.NoError(t, err, "The output of MySQL integration doesn't have expected format.")
}

func TestMySQLIntegrationOnlyMetrics(t *testing.T) {
	for _, mysqlConfig := range MysqlConfigs {
		testMySQLIntegrationOnlyMetrics(t, mysqlConfig)
	}
}

func testMySQLIntegrationOnlyInventory(t *testing.T, mysqlConfig MysqlConfig) {
	testName := helpers.GetTestName(t)
	stdout := runIntegration(t, mysqlConfig.MasterHostname, "INTEGRATION=true", fmt.Sprintf("NRIA_CACHE_PATH=/tmp/%v.json", testName))
	schemaDir := fmt.Sprintf("json-schema-files-%s", mysqlConfig.Version)
	schemaPath := filepath.Join(schemaDir, "mysql-schema-inventory-master.json")
	err := jsonschema.Validate(schemaPath, stdout)
	require.NoError(t, err, "The output of MySQL integration doesn't have expected format.")
}

func TestMySQLIntegrationOnlyInventory(t *testing.T) {
	for _, mysqlConfig := range MysqlConfigs {
		testMySQLIntegrationOnlyInventory(t, mysqlConfig)
	}
}

func testMySQLIntegrationOnlySlaveMetrics(t *testing.T, mysqlConfig MysqlConfig) {
	testName := helpers.GetTestName(t)
	stdout := runIntegration(t, mysqlConfig.SlaveHostname, "METRICS=true", "EXTENDED_METRICS=true", fmt.Sprintf("NRIA_CACHE_PATH=/tmp/%v.json", testName))
	schemaDir := fmt.Sprintf("json-schema-files-%s", mysqlConfig.Version)
	schemaPath := filepath.Join(schemaDir, "mysql-schema-metrics-slave.json")
	err := jsonschema.Validate(schemaPath, stdout)
	require.NoError(t, err, "The output of MySQL integration doesn't have expected format.")
}

func TestMySQLIntegrationOnlySlaveMetrics(t *testing.T) {
	for _, mysqlConfig := range MysqlConfigs {
		testMySQLIntegrationOnlySlaveMetrics(t, mysqlConfig)
	}
}

func testPerfOutputIsValidJSON(t *testing.T, mysqlPerfConfig MysqlPerformanceConfig) {
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

func runUnconfiguredMysqlPerfConfigTest(t *testing.T, args []string, outputMetricsFile string, expectedError string, testName string) {
	for _, mysqlUnconfiguredPerfConfig := range MysqlConfigs {
		if isDBVersionLessThan8(mysqlUnconfiguredPerfConfig.Version) {
			continue
		}
		t.Run(testName+mysqlUnconfiguredPerfConfig.Version, func(t *testing.T) {
			args = append(args, fmt.Sprintf("NRIA_CACHE_PATH=/tmp/%v.json", testName))
			stdout, stderr, err := runIntegrationAndGetStdoutWithError(t, mysqlUnconfiguredPerfConfig.MasterHostname, args...)
			outputMetricsList := strings.Split(stdout, "\n")
			assert.Empty(t, outputMetricsList[1], "Unexpected stdout content")
			helpers.AssertReceivedErrors(t, expectedError, strings.Split(stderr, "\n")...)
			schemaPath := filepath.Join("json-schema-performance-files", outputMetricsFile)
			err = jsonschema.Validate(schemaPath, outputMetricsList[0])
			require.NoError(t, err, "The output of MySQL integration doesn't have expected format")
		})
	}
}

func TestUnconfiguredPerfMySQLIntegration(t *testing.T) {
	testCases := []struct {
		name              string
		args              []string
		outputMetricsFile string
		expectedError     string
	}{
		{
			name: "RemoteEntity_EnableQueryPerformance",
			args: []string{
				"REMOTE_MONITORING=true",
				"ENABLE_QUERY_PERFORMANCE=true",
			},
			outputMetricsFile: "mysql-schema-master.json",
			expectedError:     "essential consumer is not enabled: events_stages_current",
		},
		{
			name: "LocalEntity_EnableQueryPerformance",
			args: []string{
				"ENABLE_QUERY_PERFORMANCE=true",
			},
			outputMetricsFile: "mysql-schema-master-localentity.json",
			expectedError:     "essential consumer is not enabled: events_stages_current",
		},
		{
			name: "OnlyMetrics_EnableQueryPerformance",
			args: []string{
				"METRICS=true",
				"ENABLE_QUERY_PERFORMANCE=true",
			},
			outputMetricsFile: "mysql-schema-metrics-master.json",
			expectedError:     "essential consumer is not enabled: events_stages_current",
		},
		{
			name: "OnlyInventory_EnableQueryPerformance",
			args: []string{
				"INVENTORY=true",
				"ENABLE_QUERY_PERFORMANCE=true",
			},
			outputMetricsFile: "mysql-schema-inventory-master.json",
			expectedError:     "essential consumer is not enabled: events_stages_current",
		},
	}
	for _, testCase := range testCases {
		runUnconfiguredMysqlPerfConfigTest(t, testCase.args, testCase.outputMetricsFile, testCase.expectedError, testCase.name)
	}
}
