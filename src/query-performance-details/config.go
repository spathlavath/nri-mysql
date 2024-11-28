package query_performance_details

import (
	"io"
	"os"

	performancedatamodel "github.com/newrelic/nri-mysql/src/query-performance-details/performance-data-models"
	"gopkg.in/yaml.v2"
)

func LoadConfig(configFile string) (*performancedatamodel.PerformanceMonitoringConfig, error) {
	file, err := os.Open(configFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var config performancedatamodel.PerformanceMonitoringConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
