package driver

import (
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/segment-sources/sqlsource/domain"
	"github.com/segmentio/go-snakecase"
	"github.com/segmentio/objects-go"
)

type Driver interface {
	Init(*domain.Config) error
	Describe() (*domain.Description, error)
	Scan(t *domain.Table, afterPKValues []interface{}) (SqlRows, error)
	Transform(row map[string]interface{}) map[string]interface{}
}

type SqlRows interface {
	Next() bool
	MapScan(map[string]interface{}) error
	Err() error
	Close() error
}

type Base struct {
	Driver Driver
}

func (b *Base) ScanTable(t *domain.Table, publisher domain.ObjectPublisher) error {
	var lastPkValues []interface{}

	for {
		var err error
		lastPkValues, err = b.scanTableChunk(t, lastPkValues, publisher)

		if err != nil {
			return err
		}

		if lastPkValues == nil {
			return nil
		}
	}
}

// scanTableChunk performs Scan operation on the driver and returns values of primary keys from the last row or an empty
// array if no rows were returned from the driver
func (b *Base) scanTableChunk(t *domain.Table, afterPKValues []interface{}, publisher domain.ObjectPublisher) ([]interface{}, error) {
	rows, err := b.Driver.Scan(t, afterPKValues)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	lastPkValues := make([]interface{}, len(t.PrimaryKeys))
	rowsFound := false
	for rows.Next() {
		rowsFound = true
		row := map[string]interface{}{}
		if err := rows.MapScan(row); err != nil {
			return nil, err
		}
		log.WithFields(log.Fields{"row": row, "table": t.TableName, "schema": t.SchemaName}).Debugf("Received Row")
		t.IncrScanned()

		for i, p := range t.PrimaryKeys {
			lastPkValues[i] = row[p]
		}

		row = b.Driver.Transform(row)
		pks := []string{}
		for _, p := range t.PrimaryKeys {
			pks = append(pks, fmt.Sprintf("%v", row[p]))
		}

		publisher(&objects.Object{
			ID:         strings.Join(pks, "_"),
			Collection: snakecase.Snakecase(fmt.Sprintf("%s_%s", t.SchemaName, t.TableName)),
			Properties: row,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if !rowsFound {
		return nil, nil
	}

	return lastPkValues, nil
}
