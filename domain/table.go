package domain

import (
	"fmt"
	"strings"
	"sync/atomic"
)

type Table struct {
	SchemaName  string   `json:"-"`
	TableName   string   `json:"-"`
	PrimaryKeys []string `json:"primary_keys"`
	Columns     []string `json:"columns"`
	ScannedRows uint64

	// For future features
	MarkerColumn string      `json:"marker_column,omitempty"`
	LastMarker   interface{} `json:"last_marker,omitempty"`
}

func (t *Table) IncrScanned() {
	atomic.AddUint64(&t.ScannedRows, 1)
}

func (t *Table) ColumnToSQL() string {
	c := []string{}
	for _, column := range t.Columns {
		c = append(c, fmt.Sprintf("%q", column))
	}

	return strings.Join(c, ", ")
}
