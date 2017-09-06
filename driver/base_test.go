package driver

import (
	"errors"
	"github.com/segment-sources/sqlsource/domain"
	"github.com/segmentio/objects-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

type Result struct {
	Rows    []map[string]interface{}
	current map[string]interface{}
}

func (r *Result) Next() bool {
	if len(r.Rows) > 0 {
		r.current = r.Rows[0]
		r.Rows = r.Rows[1:]
		return true
	}

	return false
}

func (r *Result) MapScan(dest map[string]interface{}) error {
	for k, v := range r.current {
		dest[k] = v
	}
	return nil
}

func (r *Result) Err() error {
	return nil
}

func (r *Result) Close() error {
	return nil
}

type DriverMock struct {
	ScanCallback func(t *domain.Table, afterPKValues []interface{}) (SqlRows, error)
}

func (m *DriverMock) Init(*domain.Config) error {
	panic("not implemented")
}

func (m *DriverMock) Describe() (*domain.Description, error) {
	panic("not implemented")
}

func (m *DriverMock) Scan(t *domain.Table, afterPKValues []interface{}) (SqlRows, error) {
	return m.ScanCallback(t, afterPKValues)
}

func (m *DriverMock) Transform(row map[string]interface{}) map[string]interface{} {
	return row
}

func TestBase_ScanTable(t *testing.T) {
	a := assert.New(t)

	published := []*objects.Object{}
	publisher := func(o *objects.Object) {
		published = append(published, o)
	}

	results := []*Result{
		{
			Rows: []map[string]interface{}{
				{
					"user_id":   3,
					"group_id":  8,
					"joined_at": 1504648056,
				},
				{
					"user_id":   5,
					"group_id":  4,
					"joined_at": 1504648110,
				},
			},
		},
		{
			Rows: []map[string]interface{}{
				{
					"user_id":   5,
					"group_id":  8,
					"joined_at": 1504648167,
				},
				{
					"user_id":   7,
					"group_id":  3,
					"joined_at": 1504648170,
				},
			},
		},
		{
			Rows: []map[string]interface{}{},
		},
	}
	scanCount := 0
	scanCallback := func(t *domain.Table, afterPKValues []interface{}) (SqlRows, error) {
		switch scanCount {
		case 0:
			if !a.Nil(afterPKValues) {
				return nil, errors.New("afterPKValues expected to be nil")
			}
		case 1:
			if !a.Equal([]interface{}{5, 4}, afterPKValues) {
				return nil, errors.New("afterPKValues expected to be []interface{}{5, 4}")
			}
		case 2:
			if !a.Equal([]interface{}{7, 3}, afterPKValues) {
				return nil, errors.New("afterPKValues expected to be []interface{}{7, 3}")
			}
		}
		scanCount++

		result := results[0]
		results = results[1:]
		return result, nil
	}

	var testTable = &domain.Table{
		TableName: "memberships",
		PrimaryKeys: []string{
			"user_id",
			"group_id",
		},
	}
	base := Base{
		Driver: &DriverMock{ScanCallback: scanCallback},
	}
	err := base.ScanTable(testTable, publisher)
	if !a.NoError(err) {
		return
	}

	expectedPublished := []*objects.Object{
		{
			ID:         "3_8",
			Collection: "memberships",
			Properties: map[string]interface{}{
				"user_id":   3,
				"group_id":  8,
				"joined_at": 1504648056,
			},
		},
		{
			ID:         "5_4",
			Collection: "memberships",
			Properties: map[string]interface{}{
				"user_id":   5,
				"group_id":  4,
				"joined_at": 1504648110,
			},
		},
		{
			ID:         "5_8",
			Collection: "memberships",
			Properties: map[string]interface{}{
				"user_id":   5,
				"group_id":  8,
				"joined_at": 1504648167,
			},
		},
		{
			ID:         "7_3",
			Collection: "memberships",
			Properties: map[string]interface{}{
				"user_id":   7,
				"group_id":  3,
				"joined_at": 1504648170,
			},
		},
	}
	a.Equal(expectedPublished, published)
}
