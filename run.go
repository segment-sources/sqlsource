package sqlsource

import (
	"io"
	"strconv"

	"github.com/Lilibuth12/sqlsource/client"
	"github.com/Lilibuth12/sqlsource/domain"
	"github.com/Lilibuth12/sqlsource/driver"
	"github.com/Sirupsen/logrus"
	"github.com/asaskevich/govalidator"
	"github.com/segmentio/objects-go"
	"github.com/tj/docopt"
)

const (
	Version = "0.0.1-beta"
)

var usage = `
Usage:
  dbsource
    [--debug]
    [--init]
    [--concurrency=<c>]
    [--schema=<schema-path>]
    --write-key=<segment-write-key>
    --hostname=<hostname>
    --port=<port>
    --username=<username>
    --password=<password>
    --database=<database>
    [-- <extra-driver-options>...]
  dbsource -h | --help
  dbsource --version
$P
Options:
    "github.com/segmentio/source-db-lib/internal/domain"
  -h --help                   Show this screen
  --version                   Show version
  --write-key=<key>           Segment source write key
  --concurrency=<c>           Number of concurrent table scans [default: 1]
  --hostname=<hostname>       Database instance hostname
  --port=<port>               Database instance port number
  --username=<username>       Database instance username
  --password=<password>       Database instance password
  --database=<database>       Database instance name
  --schema=<schema-path>	  The path to the schema json file [default: schema.json]

`

func Run(d driver.Driver) {
	app := &driver.Base{d}

	m, err := docopt.Parse(usage, nil, true, Version, false)
	if err != nil {
		logrus.Error(err)
		return
	}

	config := &domain.Config{
		Init:         m["--init"].(bool),
		Hostname:     m["--hostname"].(string),
		Port:         m["--port"].(string),
		Username:     m["--username"].(string),
		Password:     m["--password"].(string),
		Database:     m["--database"].(string),
		ExtraOptions: m["<extra-driver-options>"].([]string),
	}

	if m["--debug"].(bool) {
		logrus.SetLevel(logrus.DebugLevel)
	}

	concurrency, err := strconv.Atoi(m["--concurrency"].(string))
	if err != nil {
		logrus.Error(err)
		return
	}

	// Validate the configuration
	if _, err := govalidator.ValidateStruct(config); err != nil {
		logrus.Error(err)
		return
	}

	// Initialize the source
	filename := m["--schema"].(string)
	if config.Init {
		client.InitSchema(app, config, filename)
		return
	}

	description, err := client.ParseSchema(filename)
	if err == io.EOF {
		logrus.Error("Empty schema, did you run `--init`?")
		return
	} else if err != nil {
		logrus.Error(err)
		return
	}

	// Build Segment client and define publish function for when we scan over the collections.
	writeKey := m["--write-key"].(string)
	if writeKey == "" {
		logrus.Fatal("Write key is required when not in init mode.")
	}

	segmentClient := objects.New(writeKey)
	defer segmentClient.Close()
	setWrapper := func(o *objects.Object) {
		if err := segmentClient.Set(o); err != nil {
			logrus.WithFields(logrus.Fields{"id": o.ID, "collection": o.Collection, "properties": o.Properties}).Warn(err)
		}
	}

	if err := client.Sync(app, config, description, concurrency, setWrapper); err != nil {
		logrus.Error("sql source failed to complete", err)
		return
	}
}
