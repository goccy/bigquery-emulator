package types

import (
	"fmt"
	"time"

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
	}
)

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
