package types

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/googlesqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type (
	GetQueryResultsResponse struct {
		JobReference *bigqueryv2.JobReference `json:"jobReference"`
		Schema       *bigqueryv2.TableSchema  `json:"schema"`
		Rows         []*TableRow              `json:"rows"`
		TotalRows    uint64                   `json:"totalRows,string"`
		JobComplete  bool                     `json:"jobComplete"`
		PageToken    string                   `json:"pageToken,omitempty"`
		TotalBytes   uint64                   `json:"-"`
	}

	QueryResponse struct {
		JobReference   *bigqueryv2.JobReference   `json:"jobReference"`
		Schema         *bigqueryv2.TableSchema    `json:"schema"`
		Rows           []*TableRow                `json:"rows"`
		TotalRows      uint64                     `json:"totalRows,string"`
		JobComplete    bool                       `json:"jobComplete"`
		TotalBytes     int64                      `json:"-"`
		ChangedCatalog *googlesqlite.ChangedCatalog `json:"-"`
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

func (r *TableRow) Data() (map[string]interface{}, error) {
	rowMap := map[string]interface{}{}
	for _, cell := range r.F {
		v, err := cell.Data()
		if err != nil {
			return nil, err
		}
		rowMap[cell.Name] = v
	}
	return rowMap, nil
}

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

func (c *TableCell) Data() (interface{}, error) {
	switch v := c.V.(type) {
	case TableRow:
		return v.Data()
	case []*TableCell:
		ret := make([]interface{}, 0, len(v))
		for _, vv := range v {
			data, err := vv.Data()
			if err != nil {
				return nil, err
			}
			ret = append(ret, data)
		}
		return ret, nil
	default:
		if v == nil {
			return nil, nil
		}
		text, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("failed to cast to string from %s", v)
		}
		return text, nil
	}
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
		if v == nil {
			return map[string]interface{}{schema.Type.Key(): nil}, nil
		}
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

func (r *TableRow) AppendValueToARROWBuilder(builder *array.RecordBuilder) error {
	for idx, cell := range r.F {
		if err := cell.AppendValueToARROWBuilder(builder.Field(idx)); err != nil {
			return err
		}
	}
	return nil
}

func (r *TableRow) appendValueToARROWBuilder(builder *array.StructBuilder) error {
	for idx, cell := range r.F {
		if err := cell.AppendValueToARROWBuilder(builder.FieldBuilder(idx)); err != nil {
			return err
		}
	}
	return nil
}

func (c *TableCell) AppendValueToARROWBuilder(builder array.Builder) error {
	switch v := c.V.(type) {
	case TableRow:
		b, ok := builder.(*array.StructBuilder)
		if !ok {
			return fmt.Errorf("failed to convert to struct builder from %T", builder)
		}
		b.Append(true)
		return v.appendValueToARROWBuilder(b)
	case []*TableCell:
		listBuilder, ok := builder.(*array.ListBuilder)
		if !ok {
			return fmt.Errorf("failed to convert to list builder from %T", builder)
		}
		b := listBuilder.ValueBuilder()
		for _, vv := range v {
			listBuilder.Append(true)
			if err := vv.AppendValueToARROWBuilder(b); err != nil {
				return err
			}
		}
		return nil
	default:
		if v == nil {
			return types.AppendValueToARROWBuilder(nil, builder)
		}
		text, ok := v.(string)
		if !ok {
			return fmt.Errorf("failed to cast to string from %s", v)
		}
		return types.AppendValueToARROWBuilder(&text, builder)
	}
}

// Format converts TIMESTAMP result cells from the raw canonical timestamp
// produced by the SQL backend into the representation the BigQuery REST API
// returns: int64 microseconds-since-epoch when useInt64Timestamp is set, and
// otherwise a float seconds-since-epoch string. Every official client decodes
// a TIMESTAMP value as one of those two numeric forms, never as a formatted
// datetime string.
func Format(schema *bigqueryv2.TableSchema, rows []*TableRow, useInt64Timestamp bool) []*TableRow {
	formattedRows := make([]*TableRow, 0, len(rows))
	for _, row := range rows {
		cells := make([]*TableCell, 0, len(row.F))
		for colIdx, cell := range row.F {
			if schema.Fields[colIdx].Type == "TIMESTAMP" && cell.V != nil {
				cells = append(cells, &TableCell{
					V: formatTimestampCell(cell.V, useInt64Timestamp),
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

// formatTimestampCell renders one TIMESTAMP cell value. A non-string value or
// an unparseable timestamp is passed through unchanged.
func formatTimestampCell(v interface{}, useInt64Timestamp bool) interface{} {
	raw, ok := v.(string)
	if !ok {
		return v
	}
	t, err := googlesqlite.TimeFromTimestampValue(raw)
	if err != nil {
		return v
	}
	micros := t.UnixMicro()
	if useInt64Timestamp {
		return fmt.Sprint(micros)
	}
	sec := micros / int64(time.Second/time.Microsecond)
	frac := micros % int64(time.Second/time.Microsecond)
	if frac < 0 {
		frac += int64(time.Second / time.Microsecond)
		sec--
	}
	return fmt.Sprintf("%d.%06d", sec, frac)
}
