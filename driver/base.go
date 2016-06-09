package driver

import (
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jmoiron/sqlx"
	"github.com/segment-sources/sqlsource/domain"
	"github.com/segmentio/go-snakecase"
	"github.com/segmentio/objects-go"
)

type Driver interface {
	Init(*domain.Config) error
	Describe() (*domain.Description, error)
	Scan(t *domain.Table, publisher domain.ObjectPublisher) error
}

type Base struct {
	Connection *sqlx.DB
}

func (b *Base) Scan(t *domain.Table, publisher domain.ObjectPublisher) error {
	query := fmt.Sprintf("SELECT %s FROM %q.%q", t.ColumnToSQL(), t.SchemaName, t.TableName)
	log.Debugf("Executing query: %v", query)

	rows, err := b.Connection.Queryx(query)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		row := map[string]interface{}{}
		if err := rows.MapScan(row); err != nil {
			return err
		}
		log.WithFields(log.Fields{"row": row, "table": t.TableName, "schema": t.SchemaName}).Debugf("Received Row")
		t.IncrScanned()

		pks := []string{}
		for _, p := range t.PrimaryKeys {
			pks = append(pks, fmt.Sprintf("%v", row[p]))
			delete(row, p)
		}

		publisher(&objects.Object{
			ID:         snakecase.Snakecase(strings.Join(pks, "_")),
			Collection: snakecase.Snakecase(fmt.Sprintf("%s_%s", t.SchemaName, t.TableName)),
			Properties: row,
		})
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}
