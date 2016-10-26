package client

import (
	"os"

	"github.com/Lilibuth12/sqlsource/domain"
	"github.com/Lilibuth12/sqlsource/driver"
	"github.com/Sirupsen/logrus"
	"github.com/tj/go-sync/semaphore"
)

// InitSchema ...
func InitSchema(app *driver.Base, config *domain.Config, fileName string) {
	logrus.Info("will output schema to ", fileName)
	schemaFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logrus.Error(err)
		return
	}
	defer schemaFile.Close()

	// Initialize DB connection.
	if err := app.Driver.Init(config); err != nil {
		logrus.Error(err)
		return
	}

	description, err := app.Driver.Describe()
	if err != nil {
		logrus.Error(err)
		return
	}
	if err := description.Save(schemaFile); err != nil {
		logrus.Error(err)
		return
	}

	schemaFile.Sync()
	logrus.Infof("Saved to `%s`", schemaFile.Name())
}

// ParseSchema ...
func ParseSchema(fileName string) (*domain.Description, error) {
	// We must not be in init mode at this point, begin uploading source data.
	schemaFile, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer schemaFile.Close()

	return domain.NewDescriptionFromReader(schemaFile)
}

// Sync ...
func Sync(app *driver.Base, config *domain.Config, description *domain.Description, concurrency int, setWrapper domain.ObjectPublisher) error {
	// Initialize DB connection.
	if err := app.Driver.Init(config); err != nil {
		logrus.Error(err)
		return err
	}

	// Launch goroutines to scan the documents in each collection.
	sem := make(semaphore.Semaphore, concurrency)

	for table := range description.Iter() {
		sem.Acquire()
		go func(table *domain.Table) {
			defer sem.Release()
			logrus.WithFields(logrus.Fields{"table": table.TableName, "schema": table.SchemaName}).Info("Scan started")
			if err := app.ScanTable(table, setWrapper); err != nil {
				logrus.Error(err)
			}
			logrus.WithFields(logrus.Fields{"table": table.TableName, "schema": table.SchemaName}).Info("Scan finished")
		}(table)
	}

	sem.Wait()

	// Log status
	for table := range description.Iter() {
		logrus.WithFields(logrus.Fields{"schema": table.SchemaName, "table": table.TableName, "count": table.State.ScannedRows}).Info("Sync Finished")
	}

	return nil
}
