package types

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/go-zetasqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type (
	GetQueryResultsResponse struct {
		JobReference *bigqueryv2.JobReference `json:"jobReference"`
		Schema       *bigqueryv2.TableSchema  `json:"schema"`
		Rows         []*TableRow              `json:"rows,omitempty"`
		TotalRows    uint64                   `json:"totalRows,string"`
		JobComplete  bool                     `json:"jobComplete"`
		TotalBytes   uint64                   `json:"-"`
	}

	QueryResponse struct {
		JobReference   *bigqueryv2.JobReference   `json:"jobReference"`
		Schema         *bigqueryv2.TableSchema    `json:"schema"`
		Rows           []*TableRow                `json:"rows"`
		TotalRows      uint64                     `json:"totalRows,string"`
		JobComplete    bool                       `json:"jobComplete"`
		TotalBytes     int64                      `json:"-"`
		ChangedCatalog *zetasqlite.ChangedCatalog `json:"-"`
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

func Format(schema *bigqueryv2.TableSchema, rows []*TableRow, useInt64Timestamp bool) []*TableRow {
	if !useInt64Timestamp {
		return rows
	}
	formattedRows := make([]*TableRow, 0, len(rows))
	for _, row := range rows {
		cells := make([]*TableCell, 0, len(row.F))
		for colIdx, cell := range row.F {
			if schema.Fields[colIdx].Type == "TIMESTAMP" && cell.V != nil {
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
