package types

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/goccy/go-zetasqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TableToARROW(t *bigqueryv2.Table) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(t.Schema.Fields))
	for _, field := range t.Schema.Fields {
		f, err := TableFieldToARROW(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, *f)
	}
	return arrow.NewSchema(fields, nil), nil
}

func TableFieldToARROW(f *bigqueryv2.TableFieldSchema) (*arrow.Field, error) {
	field, err := tableFieldToARROW(f)
	if err != nil {
		return nil, err
	}
	switch Mode(f.Mode) {
	case RepeatedMode:
		return &arrow.Field{
			Name: f.Name,
			Type: arrow.ListOfField(*field),
		}, nil
	case RequiredMode:
		return field, nil
	}
	field.Nullable = true
	return field, nil
}

func tableFieldToARROW(f *bigqueryv2.TableFieldSchema) (*arrow.Field, error) {
	switch FieldType(f.Type) {
	case FieldInteger:
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Int64}, nil
	case FieldBoolean:
		return &arrow.Field{Name: f.Name, Type: arrow.FixedWidthTypes.Boolean}, nil
	case FieldFloat:
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Float64}, nil
	case FieldString:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String}, nil
	case FieldBytes:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.Binary}, nil
	case FieldDate:
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Date32}, nil
	case FieldDatetime:
		return &arrow.Field{
			Name: f.Name,
			Type: arrow.FixedWidthTypes.Timestamp_us,
			Metadata: arrow.MetadataFrom(
				map[string]string{
					"ARROW:extension:name": "google:sqlType:datetime",
				},
			),
		}, nil
	case FieldTime:
		return &arrow.Field{Name: f.Name, Type: arrow.FixedWidthTypes.Time64us}, nil
	case FieldTimestamp:
		return &arrow.Field{Name: f.Name, Type: arrow.FixedWidthTypes.Timestamp_us}, nil
	case FieldJSON:
		return &arrow.Field{
			Name: f.Name,
			Type: arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(
				map[string]string{
					"ARROW:extension:name": "google:sqlType:json",
				},
			),
		}, nil
	case FieldRecord:
		fields := make([]arrow.Field, 0, len(f.Fields))
		for _, field := range f.Fields {
			fieldV, err := TableFieldToARROW(field)
			if err != nil {
				return nil, err
			}
			fields = append(fields, *fieldV)
		}
		return &arrow.Field{Name: f.Name, Type: arrow.StructOf(fields...)}, nil
	case FieldNumeric:
		// TODO: current arrow library doesn't support decimal type.
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Float64}, nil
	case FieldBignumeric:
		// TODO: current arrow library doesn't support decimal type.
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Float64}, nil
	case FieldGeography:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String}, nil
	case FieldInterval:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String}, nil
	}
	return nil, fmt.Errorf("unsupported arrow type %s", f.Type)
}

func AppendValueToARROWBuilder(ptrv *string, builder array.Builder) error {
	if ptrv == nil {
		builder.AppendNull()
		return nil
	}
	v := *ptrv
	switch b := builder.(type) {
	case *array.Int64Builder:
		i64, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}
		b.Append(i64)
		return nil
	case *array.Float64Builder:
		f64, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return err
		}
		b.Append(f64)
		return nil
	case *array.BooleanBuilder:
		cond, err := strconv.ParseBool(v)
		if err != nil {
			return err
		}
		b.Append(cond)
		return nil
	case *array.StringBuilder:
		b.Append(v)
		return nil
	case *array.BinaryBuilder:
		b.Append([]byte(v))
		return nil
	case *array.Date32Builder:
		t, err := parseDate(v)
		if err != nil {
			return err
		}
		b.Append(arrow.Date32(int32(t.Sub(time.Unix(0, 0)) / (24 * time.Hour))))
		return nil
	case *array.Time64Builder:
		t, err := parseTime(v)
		if err != nil {
			return err
		}
		b.Append(arrow.Time64(t.UnixMicro()))
	case *array.TimestampBuilder:
		t, err := zetasqlite.TimeFromTimestampValue(v)
		if err != nil {
			return err
		}
		b.Append(arrow.Timestamp(t.UnixMicro()))
		return nil
	}
	return fmt.Errorf("unexpected builder type %T", builder)
}
