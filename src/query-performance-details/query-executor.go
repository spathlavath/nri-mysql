package query_performance_details

import (
	"context"
	"time"

	"github.com/newrelic/infra-integrations-sdk/v3/log"
)

// executeQuery executes a given query and scans the results into the provided destination slice.
func executeQuery[T any](db dataSource, query string, dest *[]T) ([]T, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryxContext(ctx, query)
	if err != nil {
		log.Error("Failed to execute query: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var item T
		if err := rows.StructScan(&item); err != nil {
			log.Error("Failed to scan row: %v", err)
			return nil, err
		}
		*dest = append(*dest, item)
	}

	if err := rows.Err(); err != nil {
		log.Error("Error iterating over rows: %v", err)
		return nil, err
	}

	return *dest, nil
}
