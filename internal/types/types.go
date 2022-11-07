package types

import (
	"fmt"
	"time"

	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/go-zetasqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type (
	GetQueryResultsResponse struct {
		JobReference *bigqueryv2.JobReference `json:"jobReference"`
		Schema       *bigqueryv2.TableSchema  `json:"schema"`
		Rows         []*TableRow              `json:"rows"`
		TotalRows    uint64                   `json:"totalRows,string"`
		JobComplete  bool                     `json:"jobComplete"`
		TotalBytes   uint64                   `json:"-"`
	}

	QueryResponse struct {
		JobReference *bigqueryv2.JobReference `json:"jobReference"`
		Schema       *bigqueryv2.TableSchema  `json:"schema"`
		Rows         []*TableRow              `json:"rows"`
		TotalRows    uint64                   `json:"totalRows,string"`
		JobComplete  bool                     `json:"jobComplete"`
		TotalBytes   int64                    `json:"-"`
	}

	TableDataList struct {
		Rows      []*TableRow `json:"rows"`
		TotalRows uint64      `json:"totalRows,string"`
	}

	TableRow struct {
		F []*TableCell `json:"f,omitempty"`
	}

	// Redefines the TableCell type to return null explicitly
	// because TableCell for bigqueryv2 is omitted if V is nil,
	TableCell struct {
		V     interface{} `json:"v"`
		Bytes int64       `json:"-"`
		Name  string      `json:"-"`
	}
)

func (r *TableRow) AVROValue(fields []*types.AVROFieldSchema) (map[string]interface{}, error) {
	rowMap := map[string]interface{}{}
	for idx, cell := range r.F {
		v, err := cell.AVROValue(fields[idx])
		if err != nil {
			return nil, err
		}
		rowMap[cell.Name] = v
	}
	return rowMap, nil
}

func (c *TableCell) AVROValue(schema *types.AVROFieldSchema) (interface{}, error) {
	switch v := c.V.(type) {
	case TableRow:
		fields := types.TableFieldSchemasToAVRO(schema.Type.TypeSchema.Fields)
		return v.AVROValue(fields)
	case []*TableCell:
		ret := make([]interface{}, 0, len(v))
		for _, vv := range v {
			avrov, err := vv.AVROValue(schema)
			if err != nil {
				return nil, err
			}
			ret = append(ret, avrov)
		}
		return ret, nil
	default:
		text, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("failed to cast to string from %s", v)
		}
		value, err := schema.Type.CastValue(text)
		if err != nil {
			return nil, err
		}
		if types.Mode(schema.Type.TypeSchema.Mode) == types.RequiredMode {
			return value, nil
		}
		return map[string]interface{}{schema.Type.Key(): value}, nil
	}
}

func Format(schema *bigqueryv2.TableSchema, rows []*TableRow, useInt64Timestamp bool) []*TableRow {
	if !useInt64Timestamp {
		return rows
	}
	formattedRows := make([]*TableRow, 0, len(rows))
	for _, row := range rows {
		cells := make([]*TableCell, 0, len(row.F))
		for colIdx, cell := range row.F {
			if schema.Fields[colIdx].Type == "TIMESTAMP" {
				t, _ := zetasqlite.TimeFromTimestampValue(cell.V.(string))
				microsec := t.UnixNano() / int64(time.Microsecond)
				cells = append(cells, &TableCell{
					V: fmt.Sprint(microsec),
				})
			} else {
				cells = append(cells, cell)
			}
		}
		formattedRows = append(formattedRows, &TableRow{
			F: cells,
		})
	}
	return formattedRows
}
