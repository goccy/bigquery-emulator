package types

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-json"
	"github.com/glassmonkey/zetasql-wasm/types"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type Project struct {
	ID       string     `yaml:"id" validate:"required"`
	Datasets []*Dataset `yaml:"datasets" validate:"required"`
	Jobs     []*Job     `yaml:"jobs"`
}

type Dataset struct {
	ID       string     `yaml:"id" validate:"required"`
	Tables   []*Table   `yaml:"tables"`
	Models   []*Model   `yaml:"models"`
	Routines []*Routine `yaml:"routines"`
}

type Table struct {
	ID       string                 `yaml:"id" validate:"required"`
	Columns  []*Column              `yaml:"columns" validate:"required"`
	Data     Data                   `yaml:"data"`
	Metadata map[string]interface{} `yaml:"metadata"`
}

func (t *Table) ToBigqueryV2(projectID, datasetID string) *bigqueryv2.Table {
	fields := make([]*bigqueryv2.TableFieldSchema, len(t.Columns))
	for i, col := range t.Columns {
		fields[i] = col.TableFieldSchema()
	}
	now := time.Now().Unix()
	return &bigqueryv2.Table{
		Type: "TABLE",
		Kind: "bigquery#table",
		Id:   fmt.Sprintf("%s:%s.%s", projectID, datasetID, t.ID),
		TableReference: &bigqueryv2.TableReference{
			ProjectId: projectID,
			DatasetId: datasetID,
			TableId:   t.ID,
		},
		Schema: &bigqueryv2.TableSchema{
			Fields: fields,
		},
		NumRows:          uint64(len(t.Data)),
		CreationTime:     now,
		LastModifiedTime: uint64(now),
	}
}

func (t *Table) SetupMetadata(projectID, datasetID string) {
	encodedTableData, _ := json.Marshal(t.ToBigqueryV2(projectID, datasetID))
	var tableMetadata map[string]interface{}
	_ = json.Unmarshal(encodedTableData, &tableMetadata)
	t.Metadata = tableMetadata
}

type Data []map[string]interface{}

type Mode string

func (m *Mode) UnmarshalYAML(b []byte) error {
	switch strings.ToLower(string(b)) {
	case strings.ToLower(string(NullableMode)):
		*m = NullableMode
	case strings.ToLower(string(RequiredMode)):
		*m = RequiredMode
	case strings.ToLower(string(RepeatedMode)):
		*m = RepeatedMode
	}
	return nil
}

const (
	NullableMode Mode = "NULLABLE"
	RequiredMode Mode = "REQUIRED"
	RepeatedMode Mode = "REPEATED"
)

type Column struct {
	Name   string    `yaml:"name" validate:"required"`
	Type   Type      `yaml:"type" validate:"type"`
	Mode   Mode      `yaml:"mode" validate:"mode"`
	Fields []*Column `yaml:"fields"`
}

func (c *Column) FormatType() string {
	var typ string
	if c.Type.ZetaSQLTypeKind() == types.Struct {
		formatTypes := make([]string, 0, len(c.Fields))
		for _, field := range c.Fields {
			formatTypes = append(formatTypes, fmt.Sprintf("`%s` %s", field.Name, field.FormatType()))
		}
		typ = fmt.Sprintf("STRUCT<%s>", strings.Join(formatTypes, ","))
	} else {
		// Kind.String() returns the proto enum name ("TYPE_INT64",
		// "TYPE_STRING", ...); ZetaSQL DDL expects the SQL name
		// ("INT64", "STRING", ...). Strip the proto namespace.
		typ = strings.TrimPrefix(c.Type.ZetaSQLTypeKind().String(), "TYPE_")
	}
	if c.Mode == RepeatedMode {
		return fmt.Sprintf("ARRAY<%s>", typ)
	} else if c.Mode == RequiredMode {
		return fmt.Sprintf("%s NOT NULL", typ)
	}
	return typ
}

func (c *Column) TableFieldSchema() *bigqueryv2.TableFieldSchema {
	return tableFieldSchemaFromColumn(c)
}

func tableFieldSchemaFromColumn(c *Column) *bigqueryv2.TableFieldSchema {
	if len(c.Fields) == 0 {
		return &bigqueryv2.TableFieldSchema{
			Name: c.Name,
			Type: string(c.Type.FieldType()),
			Mode: string(c.Mode),
		}
	}
	fields := make([]*bigqueryv2.TableFieldSchema, 0, len(c.Fields))
	for _, field := range c.Fields {
		fields = append(fields, tableFieldSchemaFromColumn(field))
	}
	return &bigqueryv2.TableFieldSchema{
		Name:   c.Name,
		Type:   string(c.Type.FieldType()),
		Fields: fields,
		Mode:   string(c.Mode),
	}
}

type Job struct {
	ID       string                 `yaml:"id" validate:"required"`
	Metadata map[string]interface{} `yaml:"metadata"`
}

type Model struct {
	ID       string                 `yaml:"id" validate:"required"`
	Metadata map[string]interface{} `yaml:"metadata"`
}

type Routine struct {
	ID       string                 `yaml:"id" validate:"required"`
	Metadata map[string]interface{} `yaml:"metadata"`
}

type Type string

func TypeFromKind(kind int) Type {
	switch types.TypeKind(kind) {
	case types.Int32:
		return INT64
	case types.Int64:
		return INT64
	case types.Uint32:
		return INT64
	case types.Uint64:
		return INT64
	case types.Bool:
		return BOOL
	case types.Float:
		return FLOAT
	case types.Double:
		return FLOAT64
	case types.String:
		return STRING
	case types.Bytes:
		return BYTES
	case types.Date:
		return DATE
	case types.Timestamp:
		return TIMESTAMP
	case types.Enum:
		return INT64
	case types.Array:
		return ARRAY
	case types.Struct:
		return STRUCT
	case types.Time:
		return TIME
	case types.Datetime:
		return DATETIME
	case types.Geography:
		return GEOGRAPHY
	case types.Numeric:
		return NUMERIC
	case types.BigNumeric:
		return BIGNUMERIC
	case types.Json:
		return JSON
	case types.Interval:
		return INTERVAL
	}
	return ""
}

func (t Type) ZetaSQLTypeKind() types.TypeKind {
	switch t {
	case INT64:
		return types.Int64
	case INT:
		return types.Int64
	case SMALLINT:
		return types.Int64
	case INTEGER:
		return types.Int64
	case BIGINT:
		return types.Int64
	case TINYINT:
		return types.Int64
	case BYTEINT:
		return types.Int64
	case NUMERIC:
		return types.Numeric
	case BIGNUMERIC:
		return types.BigNumeric
	case DECIMAL:
		return types.Numeric
	case BIGDECIMAL:
		return types.BigNumeric
	case FLOAT:
		return types.Float
	case FLOAT64:
		return types.Double
	case DOUBLE:
		return types.Double
	case BOOLEAN:
		return types.Bool
	case BOOL:
		return types.Bool
	case STRING:
		return types.String
	case BYTES:
		return types.Bytes
	case DATE:
		return types.Date
	case DATETIME:
		return types.Datetime
	case TIME:
		return types.Time
	case TIMESTAMP:
		return types.Timestamp
	case INTERVAL:
		return types.Interval
	case ARRAY:
		return types.Array
	case STRUCT:
		return types.Struct
	case GEOGRAPHY:
		return types.Geography
	case JSON:
		return types.Json
	case RECORD:
		return types.Struct
	}
	return types.TypeKind(0)
}

func (t Type) FieldType() FieldType {
	switch t {
	case INT64:
		return FieldInteger
	case INT:
		return FieldInteger
	case SMALLINT:
		return FieldInteger
	case INTEGER:
		return FieldInteger
	case BIGINT:
		return FieldInteger
	case TINYINT:
		return FieldInteger
	case BYTEINT:
		return FieldInteger
	case NUMERIC:
		return FieldNumeric
	case BIGNUMERIC:
		return FieldBignumeric
	case DECIMAL:
		return FieldNumeric
	case BIGDECIMAL:
		return FieldBignumeric
	case FLOAT:
		return FieldFloat
	case FLOAT64:
		return FieldFloat
	case DOUBLE:
		return FieldFloat
	case BOOLEAN:
		return FieldBoolean
	case BOOL:
		return FieldBoolean
	case STRING:
		return FieldString
	case BYTES:
		return FieldBytes
	case DATE:
		return FieldDate
	case DATETIME:
		return FieldDatetime
	case TIME:
		return FieldTime
	case TIMESTAMP:
		return FieldTimestamp
	case INTERVAL:
		return FieldInterval
	case ARRAY:
		return FieldRecord
	case STRUCT:
		return FieldRecord
	case GEOGRAPHY:
		return FieldRecord
	case JSON:
		return FieldJSON
	case RECORD:
		return FieldRecord
	}
	return ""
}

func TableFieldSchemaFromZetaSQLType(name string, t types.Type) *bigqueryv2.TableFieldSchema {
	kind := t.Kind()
	typ := string(TypeFromKind(int(kind)).FieldType())
	switch kind {
	case types.Array:
		at := t.AsArray()
		elem := TableFieldSchemaFromZetaSQLType("", at.ElementType)
		return &bigqueryv2.TableFieldSchema{
			Name:   name,
			Type:   elem.Type,
			Fields: elem.Fields,
			Mode:   "REPEATED",
		}
	case types.Struct:
		st := t.AsStruct()
		fieldNum := len(st.Fields)
		fields := make([]*bigqueryv2.TableFieldSchema, 0, fieldNum)
		for i := 0; i < len(st.Fields); i++ {
			field := st.Fields[i]
			fields = append(fields, TableFieldSchemaFromZetaSQLType(field.Name, field.Type))
		}
		return &bigqueryv2.TableFieldSchema{
			Name:   name,
			Type:   typ,
			Fields: fields,
		}
	}
	return &bigqueryv2.TableFieldSchema{
		Name: name,
		Type: typ,
	}
}

const (
	INT64      Type = "INT64"
	INT        Type = "INT"
	SMALLINT   Type = "SMALLINT"
	INTEGER    Type = "INTEGER"
	BIGINT     Type = "BIGINT"
	TINYINT    Type = "TINYINT"
	BYTEINT    Type = "BYTEINT"
	NUMERIC    Type = "NUMERIC"
	BIGNUMERIC Type = "BIGNUMERIC"
	DECIMAL    Type = "DECIMAL"
	BIGDECIMAL Type = "BIGDECIMAL"
	BOOLEAN    Type = "BOOLEAN"
	BOOL       Type = "BOOL"
	FLOAT      Type = "FLOAT"
	FLOAT64    Type = "FLOAT64"
	DOUBLE     Type = "DOUBLE"
	STRING     Type = "STRING"
	BYTES      Type = "BYTES"
	DATE       Type = "DATE"
	DATETIME   Type = "DATETIME"
	TIME       Type = "TIME"
	TIMESTAMP  Type = "TIMESTAMP"
	INTERVAL   Type = "INTERVAL"
	ARRAY      Type = "ARRAY"
	STRUCT     Type = "STRUCT"
	GEOGRAPHY  Type = "GEOGRAPHY"
	JSON       Type = "JSON"
	RECORD     Type = "RECORD"
)

type FieldType string

const (
	FieldInteger    FieldType = "INTEGER"
	FieldBoolean    FieldType = "BOOLEAN"
	FieldFloat      FieldType = "FLOAT"
	FieldString     FieldType = "STRING"
	FieldBytes      FieldType = "BYTES"
	FieldDate       FieldType = "DATE"
	FieldTimestamp  FieldType = "TIMESTAMP"
	FieldRecord     FieldType = "RECORD"
	FieldTime       FieldType = "TIME"
	FieldDatetime   FieldType = "DATETIME"
	FieldGeography  FieldType = "GEOGRAPHY"
	FieldNumeric    FieldType = "NUMERIC"
	FieldBignumeric FieldType = "BIGNUMERIC"
	FieldInterval   FieldType = "INTERVAL"
	FieldJSON       FieldType = "JSON"
)

func init() {
	for _, v := range []struct {
		fieldType   FieldType
		bqFieldType bigquery.FieldType
	}{
		{FieldInteger, bigquery.IntegerFieldType},
		{FieldBoolean, bigquery.BooleanFieldType},
		{FieldFloat, bigquery.FloatFieldType},
		{FieldString, bigquery.StringFieldType},
		{FieldBytes, bigquery.BytesFieldType},
		{FieldDate, bigquery.DateFieldType},
		{FieldTimestamp, bigquery.TimestampFieldType},
		{FieldRecord, bigquery.RecordFieldType},
		{FieldTime, bigquery.TimeFieldType},
		{FieldDatetime, bigquery.DateTimeFieldType},
		{FieldGeography, bigquery.GeographyFieldType},
		{FieldNumeric, bigquery.NumericFieldType},
		{FieldBignumeric, bigquery.BigNumericFieldType},
		{FieldInterval, bigquery.IntervalFieldType},
		{FieldJSON, bigquery.JSONFieldType},
	} {
		validateFieldType(v.fieldType, v.bqFieldType)
	}
}

func validateFieldType(typ FieldType, fieldType bigquery.FieldType) {
	if string(typ) != string(fieldType) {
		panic(fmt.Sprintf("FieldType is %s but bigquery.FieldType is %s", typ, fieldType))
	}
}

func NewProject(id string, datasets ...*Dataset) *Project {
	return &Project{
		ID:       id,
		Datasets: datasets,
	}
}

func NewDataset(id string, tables ...*Table) *Dataset {
	return &Dataset{
		ID:     id,
		Tables: tables,
	}
}

func NewTable(id string, columns []*Column, data Data) *Table {
	return &Table{
		ID:      id,
		Columns: columns,
		Data:    data,
	}
}

func NewTableWithSchema(t *bigqueryv2.Table, data Data) (*Table, error) {
	columns := make([]*Column, 0, len(t.Schema.Fields))
	nameToFieldMap := map[string]*bigqueryv2.TableFieldSchema{}
	for _, field := range t.Schema.Fields {
		nameToFieldMap[field.Name] = field
		columns = append(columns, NewColumnWithSchema(field))
	}
	newData := Data{}
	for _, row := range data {
		rowData := map[string]interface{}{}
		for k, v := range row {
			field, exists := nameToFieldMap[k]
			if !exists {
				continue
			}
			v, err := normalizeData(v, field)
			if err != nil {
				return nil, err
			}
			rowData[k] = v
		}
		newData = append(newData, rowData)
	}
	return &Table{ID: t.TableReference.TableId, Columns: columns, Data: newData}, nil
}

type ColumnOption func(c *Column)

func ColumnMode(mode Mode) ColumnOption {
	return func(c *Column) {
		c.Mode = mode
	}
}

func ColumnFields(fields ...*Column) ColumnOption {
	return func(c *Column) {
		c.Fields = fields
	}
}

func NewColumn(name string, typ Type, opts ...ColumnOption) *Column {
	c := &Column{
		Name: name,
		Type: typ,
		Mode: NullableMode,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func NewColumnWithSchema(s *bigqueryv2.TableFieldSchema) *Column {
	fields := make([]*Column, 0, len(s.Fields))
	for _, field := range s.Fields {
		fields = append(fields, NewColumnWithSchema(field))
	}
	return &Column{
		Name:   s.Name,
		Type:   Type(s.Type),
		Mode:   Mode(s.Mode),
		Fields: fields,
	}
}

func parseDate(v string) (time.Time, error) {
	return time.Parse("2006-01-02", v)
}

func parseTime(v string) (time.Time, error) {
	return time.Parse("15:04:05.999999", v)
}

func parseDatetime(v string) (time.Time, error) {
	if t, err := time.Parse("2006-01-02T15:04:05.999999", v); err == nil {
		return t, nil
	}
	return time.Parse("2006-01-02 15:04:05.999999", v)
}

func normalizeData(v interface{}, field *bigqueryv2.TableFieldSchema) (interface{}, error) {
	rv := reflect.ValueOf(v)
	kind := rv.Kind()
	if Mode(field.Mode) == RepeatedMode {
		if kind != reflect.Slice && kind != reflect.Array {
			return nil, fmt.Errorf("invalid value type %T for ARRAY column", v)
		}
		values := make([]interface{}, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			value, err := normalizeData(rv.Index(i).Interface(), &bigqueryv2.TableFieldSchema{
				Fields: field.Fields,
			})
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}
	if kind == reflect.Map {
		fieldMap := map[string]*bigqueryv2.TableFieldSchema{}
		columnNameToValueMap := map[string]interface{}{}
		for _, f := range field.Fields {
			fieldMap[f.Name] = f
			columnNameToValueMap[f.Name] = nil
		}
		for _, key := range rv.MapKeys() {
			if key.Kind() != reflect.String {
				return nil, fmt.Errorf("invalid value type %s for STRUCT column", key.Kind())
			}
			columnName := key.Interface().(string)
			value, err := normalizeData(rv.MapIndex(key).Interface(), fieldMap[columnName])
			if err != nil {
				return nil, err
			}
			columnNameToValueMap[columnName] = value
		}
		fields := make([]map[string]interface{}, 0, len(fieldMap))
		for _, f := range field.Fields {
			value, exists := columnNameToValueMap[f.Name]
			if !exists {
				return nil, fmt.Errorf("failed to find value from %v by %s", columnNameToValueMap, f.Name)
			}
			fields = append(fields, map[string]interface{}{f.Name: value})
		}
		return fields, nil
	}
	return v, nil
}
