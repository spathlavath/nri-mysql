package mysqlapm

import (
	"os"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
)

var ArgsAppName = ""
var ArgsKey = ""
var NewrelicApp = newrelic.Application{}
var Txn *newrelic.Transaction = nil

func InitNewRelicApp() {
	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName(ArgsAppName),
		newrelic.ConfigLicense(ArgsKey),
		newrelic.ConfigDebugLogger(os.Stdout),
		newrelic.ConfigDatastoreRawQuery(true),
	)
	if err != nil {
		log.Error("Error creating new relic application: %s", err.Error())
	}

	// Ensure the application is connected
	if err := app.WaitForConnection(10 * time.Second); err != nil {
		log.Debug("New Relic Application did not connect:", err)
		return
	}

	// Log application connection status
	if app != nil {
		log.Debug("New Relic application initialized successfully")
		NewrelicApp = *app
	} else {
		log.Error("New Relic application initialization failed")
	}
}
