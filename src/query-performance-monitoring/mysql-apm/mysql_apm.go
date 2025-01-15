package mysqlapm

import "github.com/newrelic/go-agent/v3/newrelic"

var ArgsAppName = ""
var ArgsKey = ""
var NewrelicApp = newrelic.Application{}
var Txn *newrelic.Transaction = nil
