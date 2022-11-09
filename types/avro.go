package types

import (
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	"github.com/goccy/go-zetasqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type AVROSchema struct {
	Namespace string             `json:"namespace"`
	Name      string             `json:"name"`
	Type      string             `json:"type"`
	Fields    []*AVROFieldSchema `json:"fields"`
}

type AVROFieldSchema struct {
	Type *AVROType `json:"type"`
	Name string    `json:"name"`
}

type AVROType struct {
	TypeSchema *bigqueryv2.TableFieldSchema
}

func (t *AVROType) Key() string {
	switch FieldType(t.TypeSchema.Type) {
	case FieldInteger:
		return "long"
	case FieldBoolean:
		return "boolean"
	case FieldFloat:
		return "double"
	case FieldString:
		return "string"
	case FieldBytes:
		return "bytes"
	case FieldDate:
		return "int.date"
	case FieldDatetime:
		return "string.datetime"
	case FieldTime:
		return "long.time-micros"
	case FieldTimestamp:
		return "long.timestamp-micros"
	case FieldJSON:
		return "string"
	case FieldRecord:
		return t.TypeSchema.Name
	case FieldNumeric:
		return "bytes.decimal"
	case FieldBignumeric:
		return "bytes.decimal"
	}
	return ""
}

func (t *AVROType) CastValue(v string) (interface{}, error) {
	switch FieldType(t.TypeSchema.Type) {
	case FieldInteger:
		return strconv.ParseInt(v, 10, 64)
	case FieldBoolean:
		return strconv.ParseBool(v)
	case FieldFloat:
		return strconv.ParseFloat(v, 64)
	case FieldString:
		return v, nil
	case FieldJSON:
		return v, nil
	case FieldBytes:
		return []byte(v), nil
	case FieldNumeric, FieldBignumeric:
		r := new(big.Rat)
		r.SetString(v)
		return r, nil
	case FieldDate:
		return parseDate(v)
	case FieldDatetime:
		if t, err := time.Parse("2006-01-02T15:04:05.999999", v); err == nil {
			return t, nil
		}
		return time.Parse("2006-01-02 15:04:05.999999", v)
	case FieldTime:
		return parseTime(v)
	case FieldTimestamp:
		return zetasqlite.TimeFromTimestampValue(v)
	}
	return v, nil
}

func (t *AVROType) MarshalJSON() ([]byte, error) {
	b, err := marshalAVROType(t.TypeSchema)
	if err != nil {
		return nil, err
	}
	typ := json.RawMessage(b)
	switch Mode(t.TypeSchema.Mode) {
	case RepeatedMode:
		return json.Marshal(map[string]interface{}{
			"type":  "array",
			"items": typ,
		})
	case RequiredMode:
		return json.Marshal(typ)
	default:
		return json.Marshal([]interface{}{
			"null",
			typ,
		})
	}
}

func marshalAVROType(t *bigqueryv2.TableFieldSchema) ([]byte, error) {
	switch FieldType(t.Type) {
	case FieldInteger:
		return []byte(`"long"`), nil
	case FieldBoolean:
		return []byte(`"boolean"`), nil
	case FieldFloat:
		return []byte(`"double"`), nil
	case FieldString:
		return []byte(`"string"`), nil
	case FieldBytes:
		return []byte(`"bytes"`), nil
	case FieldDate:
		return json.Marshal(map[string]string{
			"type":        "int",
			"logicalType": "date",
		})
	case FieldDatetime:
		return json.Marshal(map[string]string{
			"type":        "string",
			"logicalType": "datetime",
		})
	case FieldTime:
		return json.Marshal(map[string]string{
			"type":        "long",
			"logicalType": "time-micros",
		})
	case FieldTimestamp:
		return json.Marshal(map[string]string{
			"type":        "long",
			"logicalType": "timestamp-micros",
		})
	case FieldJSON:
		return json.Marshal(map[string]string{
			"type":    "string",
			"sqlType": "JSON",
		})
	case FieldRecord:
		var fields []interface{}
		for _, field := range TableFieldSchemasToAVRO(t.Fields) {
			b, err := json.Marshal(field.Type)
			if err != nil {
				return nil, err
			}
			fields = append(fields, map[string]interface{}{
				"name": field.Name,
				"type": json.RawMessage(b),
			})
		}
		return json.Marshal(map[string]interface{}{
			"type":   "record",
			"name":   t.Name,
			"fields": fields,
		})
	case FieldNumeric:
		return json.Marshal(map[string]interface{}{
			"type":        "bytes",
			"logicalType": "decimal",
			"precision":   38,
			"scale":       9,
		})
	case FieldBignumeric:
		return json.Marshal(map[string]interface{}{
			"type":        "bytes",
			"logicalType": "decimal",
			"precision":   77,
			"scale":       38,
		})
	case FieldGeography:
		return json.Marshal(map[string]string{
			"type":    "string",
			"sqlType": "GEOGRAPHY",
		})
	case FieldInterval:
		return json.Marshal(map[string]string{
			"type":    "string",
			"sqlType": "INTERVAL",
		})
	}
	return nil, fmt.Errorf("unsupported avro type %s", t.Type)
}

func TableToAVRO(t *bigqueryv2.Table) *AVROSchema {
	return &AVROSchema{
		Namespace: fmt.Sprintf("%s.%s", t.TableReference.ProjectId, t.TableReference.DatasetId),
		Name:      t.TableReference.TableId,
		Type:      "record",
		Fields:    TableFieldSchemasToAVRO(t.Schema.Fields),
	}
}

func TableFieldSchemasToAVRO(fields []*bigqueryv2.TableFieldSchema) []*AVROFieldSchema {
	ret := make([]*AVROFieldSchema, 0, len(fields))
	for _, field := range fields {
		ret = append(ret, TableFieldSchemaToAVRO(field))
	}
	return ret
}

func TableFieldSchemaToAVRO(s *bigqueryv2.TableFieldSchema) *AVROFieldSchema {
	return &AVROFieldSchema{
		Type: &AVROType{TypeSchema: s},
		Name: s.Name,
	}
}
