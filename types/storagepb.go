package types

import (
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TableToProto(t *bigqueryv2.Table) *storagepb.TableSchema {
	return &storagepb.TableSchema{
		Fields: TableFieldSchemasToProto(t.Schema.Fields),
	}
}

func TableFieldSchemasToProto(fields []*bigqueryv2.TableFieldSchema) []*storagepb.TableFieldSchema {
	ret := make([]*storagepb.TableFieldSchema, 0, len(fields))
	for _, field := range fields {
		ret = append(ret, TableFieldSchemaToProto(field))
	}
	return ret
}

func TableFieldSchemaToProto(s *bigqueryv2.TableFieldSchema) *storagepb.TableFieldSchema {
	return &storagepb.TableFieldSchema{
		Name:   s.Name,
		Type:   FieldType(s.Type).Proto(),
		Mode:   Mode(s.Mode).Proto(),
		Fields: TableFieldSchemasToProto(s.Fields),
	}
}

func (t FieldType) Proto() storagepb.TableFieldSchema_Type {
	switch t {
	case FieldInteger:
		return storagepb.TableFieldSchema_INT64
	case FieldBoolean:
		return storagepb.TableFieldSchema_BOOL
	case FieldFloat:
		return storagepb.TableFieldSchema_DOUBLE
	case FieldString:
		return storagepb.TableFieldSchema_STRING
	case FieldBytes:
		return storagepb.TableFieldSchema_BYTES
	case FieldDate:
		return storagepb.TableFieldSchema_DATE
	case FieldTimestamp:
		return storagepb.TableFieldSchema_TIMESTAMP
	case FieldRecord:
		return storagepb.TableFieldSchema_STRUCT
	case FieldTime:
		return storagepb.TableFieldSchema_TIME
	case FieldDatetime:
		return storagepb.TableFieldSchema_DATETIME
	case FieldGeography:
		return storagepb.TableFieldSchema_GEOGRAPHY
	case FieldNumeric:
		return storagepb.TableFieldSchema_NUMERIC
	case FieldBignumeric:
		return storagepb.TableFieldSchema_BIGNUMERIC
	case FieldInterval:
		return storagepb.TableFieldSchema_INTERVAL
	case FieldJSON:
		return storagepb.TableFieldSchema_JSON
	}
	return storagepb.TableFieldSchema_TYPE_UNSPECIFIED
}

func (m Mode) Proto() storagepb.TableFieldSchema_Mode {
	switch m {
	case RequiredMode:
		return storagepb.TableFieldSchema_REQUIRED
	case RepeatedMode:
		return storagepb.TableFieldSchema_REPEATED
	}
	return storagepb.TableFieldSchema_NULLABLE
}
