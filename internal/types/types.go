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

func (r *TableRow) AVROValue(namespace string, fields []*types.AVROFieldSchema) (map[string]interface{}, error) {
	rowMap := map[string]interface{}{}
	for idx, cell := range r.F {
		v, err := cell.AVROValue(namespace, fields[idx])
		if err != nil {
			return nil, err
		}
		rowMap[cell.Name] = v
	}
	return rowMap, nil
}

// avroRecordUnionKey returns the Avro union branch name for a nullable record
// field. goavro identifies a record branch within a union by the record's
// full name, which is the enclosing namespace joined with the record name.
func avroRecordUnionKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "." + name
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

func (c *TableCell) AVROValue(namespace string, schema *types.AVROFieldSchema) (interface{}, error) {
	switch v := c.V.(type) {
	case TableRow:
		fields := types.TableFieldSchemasToAVRO(schema.Type.TypeSchema.Fields)
		recordValue, err := v.AVROValue(namespace, fields)
		if err != nil {
			return nil, err
		}
		// The schema for a record field mirrors AVROType.MarshalJSON:
		// REQUIRED/REPEATED records are encoded bare (REPEATED records are
		// the bare element type of an array), while a nullable record is an
		// Avro union and goavro requires its value to be wrapped in a
		// single-key map keyed by the record's full name.
		switch types.Mode(schema.Type.TypeSchema.Mode) {
		case types.RequiredMode, types.RepeatedMode:
			return recordValue, nil
		default:
			return map[string]interface{}{
				avroRecordUnionKey(namespace, schema.Type.TypeSchema.Name): recordValue,
			}, nil
		}
	case []*TableCell:
		ret := make([]interface{}, 0, len(v))
		for _, vv := range v {
			avrov, err := vv.AVROValue(namespace, schema)
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
		// A bare value for REQUIRED fields and for the element type of
		// REPEATED arrays; a union-wrapped value for nullable fields.
		switch types.Mode(schema.Type.TypeSchema.Mode) {
		case types.RequiredMode, types.RepeatedMode:
			return value, nil
		default:
			return map[string]interface{}{schema.Type.Key(): value}, nil
		}
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
		// BigQuery REPEATED fields are always non-null: a REPEATED column is
		// either empty or populated, never null. Append(true) marks this list
		// slot as valid (non-null) and opens it for elements. The slot is closed
		// implicitly when Append is next called (for the following row). A nil
		// []*TableCell is therefore treated identically to an empty slice.
		// The original bug (issue #399) placed this call inside the element
		// loop, opening N slots instead of 1 and causing a NewRecord panic.
		listBuilder.Append(true)
		b := listBuilder.ValueBuilder()
		for _, vv := range v {
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
