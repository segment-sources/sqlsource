package driver

import (
	"fmt"
	"strings"

	"github.com/Lilibuth12/sqlsource/domain"
	"github.com/Sirupsen/logrus"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type MySQL struct {
	Connection *sqlx.DB
}

func (m *MySQL) Init(c *domain.Config) error {
	config := mysql.Config{
		User:   c.Username,
		Passwd: c.Password,
		DBName: c.Database,
		Net:    "tcp",
		Addr:   c.Hostname + ":" + c.Port,
		Params: map[string]string{},
	}

	for _, option := range c.ExtraOptions {
		splitEq := strings.Split(option, "=")
		if len(splitEq) != 2 {
			continue
		}
		config.Params[splitEq[0]] = splitEq[1]
	}

	db, err := sqlx.Connect("mysql", config.FormatDSN())
	if err != nil {
		return err
	}

	m.Connection = db

	return nil
}

func (m *MySQL) Scan(t *domain.Table) (*sqlx.Rows, error) {
	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s`", mysqlColumnsToSQL(t), t.SchemaName, t.TableName)
	logrus.Debugf("Executing query: %v", query)

	// Note: We have to get a Statement so that the MySQL driver
	// will use its binary protocol during the scan, and do proper
	// type conversion of incoming results.
	stmt, err := m.Connection.Preparex(query)
	if err != nil {
		return nil, err
	}

	return stmt.Queryx()
}

func (m *MySQL) Transform(row map[string]interface{}) map[string]interface{} {
	// The MySQL driver returns text and date columns as []byte instead of string.
	for k, v := range row {
		switch val := v.(type) {
		case []byte:
			row[k] = string(val)
		}
	}

	return row
}

func mysqlColumnsToSQL(t *domain.Table) string {
	var c []string
	for _, column := range t.Columns {
		c = append(c, fmt.Sprintf("`%s`", column))
	}

	return strings.Join(c, ", ")
}

func (m *MySQL) Describe() (*domain.Description, error) {
	describeQuery := `
        SELECT table_schema, table_name, column_name, CASE column_key WHEN 'PRI' THEN true ELSE false END as is_primary_key
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
    `

	res := domain.NewDescription()

	rows, err := m.Connection.Queryx(describeQuery)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		row := &tableDescriptionRow{}
		if err := rows.StructScan(row); err != nil {
			return nil, err
		}
		res.AddColumn(&domain.Column{Name: row.ColumnName, Schema: row.SchemaName, Table: row.TableName, IsPrimaryKey: row.IsPrimary})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}
