package types

import (
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-json"
	"github.com/goccy/go-zetasql/types"
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

func (t *Table) SetupMetadata(projectID, datasetID string) {
	fields := make([]*bigqueryv2.TableFieldSchema, len(t.Columns))
	for i, col := range t.Columns {
		fields[i] = col.TableFieldSchema()
	}
	now := time.Now().Unix()
	encodedTableData, _ := json.Marshal(&bigqueryv2.Table{
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
	})
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
	if c.Type.ZetaSQLTypeKind() == types.STRUCT {
		formatTypes := make([]string, 0, len(c.Fields))
		for _, field := range c.Fields {
			formatTypes = append(formatTypes, fmt.Sprintf("`%s` %s", field.Name, field.FormatType()))
		}
		typ = fmt.Sprintf("STRUCT<%s>", strings.Join(formatTypes, ","))
	} else {
		typ = c.Type.ZetaSQLTypeKind().String()
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
	case types.INT32:
		return INT64
	case types.INT64:
		return INT64
	case types.UINT32:
		return INT64
	case types.UINT64:
		return INT64
	case types.BOOL:
		return BOOL
	case types.FLOAT:
		return FLOAT
	case types.DOUBLE:
		return FLOAT64
	case types.STRING:
		return STRING
	case types.BYTES:
		return BYTES
	case types.DATE:
		return DATE
	case types.TIMESTAMP:
		return TIMESTAMP
	case types.ENUM:
		return INT64
	case types.ARRAY:
		return ARRAY
	case types.STRUCT:
		return STRUCT
	case types.TIME:
		return TIME
	case types.DATETIME:
		return DATETIME
	case types.GEOGRAPHY:
		return GEOGRAPHY
	case types.NUMERIC:
		return NUMERIC
	case types.BIG_NUMERIC:
		return BIGNUMERIC
	case types.JSON:
		return JSON
	case types.INTERVAL:
		return INTERVAL
	}
	return ""
}

func (t Type) ZetaSQLTypeKind() types.TypeKind {
	switch t {
	case INT64:
		return types.INT64
	case INT:
		return types.INT64
	case SMALLINT:
		return types.INT64
	case INTEGER:
		return types.INT64
	case BIGINT:
		return types.INT64
	case TINYINT:
		return types.INT64
	case BYTEINT:
		return types.INT64
	case NUMERIC:
		return types.NUMERIC
	case BIGNUMERIC:
		return types.BIG_NUMERIC
	case DECIMAL:
		return types.NUMERIC
	case BIGDECIMAL:
		return types.BIG_NUMERIC
	case FLOAT:
		return types.FLOAT
	case FLOAT64:
		return types.DOUBLE
	case DOUBLE:
		return types.DOUBLE
	case BOOLEAN:
		return types.BOOL
	case BOOL:
		return types.BOOL
	case STRING:
		return types.STRING
	case BYTES:
		return types.BYTES
	case DATE:
		return types.DATE
	case DATETIME:
		return types.DATETIME
	case TIME:
		return types.TIME
	case TIMESTAMP:
		return types.TIMESTAMP
	case INTERVAL:
		return types.INTERVAL
	case ARRAY:
		return types.ARRAY
	case STRUCT:
		return types.STRUCT
	case GEOGRAPHY:
		return types.GEOGRAPHY
	case JSON:
		return types.JSON
	case RECORD:
		return types.STRUCT
	}
	return types.UNKNOWN
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
	case types.ARRAY:
		at := t.AsArray()
		elem := TableFieldSchemaFromZetaSQLType("", at.ElementType())
		return &bigqueryv2.TableFieldSchema{
			Name:   name,
			Type:   elem.Type,
			Fields: elem.Fields,
			Mode:   "REPEATED",
		}
	case types.STRUCT:
		st := t.AsStruct()
		fieldNum := st.NumFields()
		fields := make([]*bigqueryv2.TableFieldSchema, 0, fieldNum)
		for i := 0; i < st.NumFields(); i++ {
			field := st.Field(i)
			fields = append(fields, TableFieldSchemaFromZetaSQLType(field.Name(), field.Type()))
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

type ColumnOption func(c *Column)

func ColumnMode(mode Mode) ColumnOption {
	return func(c *Column) {
		c.Mode = mode
	}
}

func ColumnFields(fields []*Column) ColumnOption {
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

func parseDate(v string) (time.Time, error) {
	return time.Parse("2006-01-02", v)
}

func parseTime(v string) (time.Time, error) {
	return time.Parse("15:04:05", v)
}
