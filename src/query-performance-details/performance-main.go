package query_performance_details

import (
	"fmt"
	"net"
	"net/url"
	"strconv"

	"github.com/newrelic/infra-integrations-sdk/v3/log"
	arguments "github.com/newrelic/nri-mysql/src/args"
)

func generateDSN(args arguments.ArgumentList) string {
	query := url.Values{}
	if args.OldPasswords {
		query.Add("allowOldPasswords", "true")
	}
	if args.EnableTLS {
		query.Add("tls", "true")
	}
	if args.InsecureSkipVerify {
		query.Add("tls", "skip-verify")
	}
	extraArgsMap, err := url.ParseQuery(args.ExtraConnectionURLArgs)
	if err == nil {
		for k, v := range extraArgsMap {
			query.Add(k, v[0])
		}
	} else {
		log.Warn("Could not successfully parse ExtraConnectionURLArgs.", err.Error())
	}
	if args.Socket != "" {
		log.Info("Socket parameter is defined, ignoring host and port parameters")
		return fmt.Sprintf("%s:%s@unix(%s)/%s?%s", args.Username, args.Password, args.Socket, args.Database, query.Encode())
	}

	// Convert hostname and port to DSN address format
	mysqlURL := net.JoinHostPort(args.Hostname, strconv.Itoa(args.Port))

	return fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", args.Username, args.Password, mysqlURL, args.Database, query.Encode())
}

func PopulateQueryPerformanceMetrics(args arguments.ArgumentList) {
	dsn := generateDSN(args)
	db, err := openDB(dsn)
	fatalIfErr(err)
	defer db.close()

	inventory, errorPerf := db.queryX("select * from employees")
	fatalIfErr(errorPerf)
	fmt.Printf("Populaing query %v\n", inventory)

	performanceSchemaEnabled, err := isPerformanceSchemaEnabled(db)
	fmt.Printf("Performance Schema enabled: %v\n", performanceSchemaEnabled)
	if !performanceSchemaEnabled {
		fmt.Println("Performance Schema is not enabled. Skipping validation.")
		return
	}

	fmt.Printf("Performance Schema is enabled\n")

}

func isPerformanceSchemaEnabled(db dataSource) (bool, error) {
	var variableName, performanceSchemaEnabled string
	rows, err := db.queryX("SHOW GLOBAL VARIABLES LIKE 'performance_schema';")

	if !rows.Next() {
		fmt.Println("No rows found")
		return false, nil
	}

	if errScanning := rows.Scan(&variableName, &performanceSchemaEnabled); err != nil {
		fatalIfErr(errScanning)
	}

	if err != nil {
		return false, fmt.Errorf("failed to check Performance Schema status: %w", err)
	}
	return performanceSchemaEnabled == "ON", nil
}

func fatalIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
