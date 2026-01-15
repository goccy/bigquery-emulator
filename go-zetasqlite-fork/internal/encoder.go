package internal

import (
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

func EncodeNamedValues(v []driver.NamedValue, params []*ast.ParameterNode) ([]sql.NamedArg, error) {
	if len(v) != len(params) {
		return nil, fmt.Errorf(
			"failed to match named values num (%d) and params num (%d)",
			len(v), len(params),
		)
	}
	ret := make([]sql.NamedArg, 0, len(v))
	for idx, vv := range v {
		converted, err := encodeNamedValue(vv, params[idx])
		if err != nil {
			return nil, fmt.Errorf("failed to convert value from %+v: %w", vv, err)
		}
		ret = append(ret, converted)
	}
	return ret, nil
}

func EncodeGoValues(v []interface{}, params []*ast.ParameterNode) ([]interface{}, error) {
	if len(v) != len(params) {
		return nil, fmt.Errorf(
			"failed to match args values num (%d) and params num (%d)",
			len(v), len(params),
		)
	}
	ret := make([]interface{}, 0, len(v))
	for idx, vv := range v {
		value, err := EncodeGoValue(params[idx].Type(), vv)
		if err != nil {
			return nil, err
		}
		ret = append(ret, value)
	}
	return ret, nil
}

func EncodeGoValue(t types.Type, v interface{}) (interface{}, error) {
	value, err := ValueFromGoValue(v)
	if err != nil {
		return nil, err
	}
	casted, err := CastValue(t, value)
	if err != nil {
		return nil, err
	}
	return EncodeValue(casted)
}

func EncodeValue(v Value) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	switch vv := v.(type) {
	case IntValue:
		return v.ToInt64()
	case FloatValue:
		return v.ToFloat64()
	case BoolValue:
		return v.ToBool()
	case *SafeValue:
		return EncodeValue(vv.value)
	}
	layout, err := valueLayoutFromValue(v)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(layout)
	if err != nil {
		return nil, fmt.Errorf("failed to encode value: %w", err)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func LiteralFromValue(v Value) (string, error) {
	if v == nil {
		return "null", nil
	}
	switch vv := v.(type) {
	case IntValue:
		i64, err := v.ToInt64()
		if err != nil {
			return "", err
		}
		return fmt.Sprint(i64), nil
	case FloatValue:
		f64, err := v.ToFloat64()
		if err != nil {
			return "", err
		}
		value := strconv.FormatFloat(f64, 'g', -1, 64)
		if !strings.Contains(value, ".") && !strings.Contains(value, "e") {
			// append x.0 suffix to keep float value context
			value = fmt.Sprintf("%s.0", value)
		}
		return value, nil
	case BoolValue:
		b, err := v.ToBool()
		if err != nil {
			return "", err
		}
		return fmt.Sprint(b), nil
	case *SafeValue:
		return LiteralFromValue(vv.value)
	}
	layout, err := valueLayoutFromValue(v)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(layout)
	if err != nil {
		return "", fmt.Errorf("failed to encode value: %w", err)
	}
	return fmt.Sprintf("%q", base64.StdEncoding.EncodeToString(b)), nil
}

func LiteralFromZetaSQLValue(v types.Value) (string, error) {
	value, err := ValueFromZetaSQLValue(v)
	if err != nil {
		return "", err
	}
	return LiteralFromValue(value)
}

func ValueFromZetaSQLValue(v types.Value) (Value, error) {
	if v.IsNull() {
		return nil, nil
	}
	switch v.Type().Kind() {
	case types.INT32, types.INT64, types.UINT32, types.UINT64:
		return intValueFromLiteral(v.SQLLiteral(0))
	case types.BOOL:
		return boolValueFromLiteral(v.SQLLiteral(0))
	case types.FLOAT, types.DOUBLE:
		return floatValueFromLiteral(v.SQLLiteral(0))
	case types.STRING:
		return StringValue(v.StringValue()), nil
	case types.ENUM:
		return stringValueFromLiteral(v.SQLLiteral(0))
	case types.BYTES:
		return bytesValueFromLiteral(v.SQLLiteral(0)), nil
	case types.DATE:
		return dateValueFromLiteral(v.ToInt64()), nil
	case types.DATETIME:
		return datetimeValueFromLiteral(v.ToPacked64DatetimeMicros()), nil
	case types.TIME:
		return timeValueFromLiteral(v.ToPacked64TimeMicros()), nil
	case types.TIMESTAMP:
		microsec := v.ToUnixMicros()
		microSecondsInSecond := int64(time.Second) / int64(time.Microsecond)
		sec := microsec / microSecondsInSecond
		remainder := microsec - (sec * microSecondsInSecond)
		return timestampValueFromLiteral(time.Unix(sec, remainder*int64(time.Microsecond)))
	case types.NUMERIC, types.BIG_NUMERIC:
		return numericValueFromLiteral(v.SQLLiteral(0))
	case types.INTERVAL:
		return intervalValueFromLiteral(v.SQLLiteral(0))
	case types.JSON:
		return jsonValueFromLiteral(v.JSONString())
	case types.ARRAY:
		return arrayValueFromLiteral(v)
	case types.STRUCT:
		return structValueFromLiteral(v)
	}
	return nil, fmt.Errorf("unsupported literal type: %s", v.Type().Kind())
}

func intValueFromLiteral(lit string) (IntValue, error) {
	v, err := strconv.ParseInt(lit, 10, 64)
	if err != nil {
		return 0, err
	}
	return IntValue(v), nil
}

func boolValueFromLiteral(lit string) (BoolValue, error) {
	v, err := strconv.ParseBool(lit)
	if err != nil {
		return false, err
	}
	return BoolValue(v), nil
}

func floatValueFromLiteral(lit string) (FloatValue, error) {
	v, err := strconv.ParseFloat(lit, 64)
	if err != nil {
		return 0, err
	}
	return FloatValue(v), nil
}

func stringValueFromLiteral(lit string) (StringValue, error) {
	v, err := strconv.Unquote(lit)
	if err != nil {
		return "", fmt.Errorf("failed to unquote from string literal: %w", err)
	}
	return StringValue(v), nil
}

func bytesValueFromLiteral(lit string) BytesValue {
	// use a workaround because ToBytes doesn't work with certain values.
	unquoted, err := strconv.Unquote(lit[1:])
	if err != nil {
		return BytesValue(lit)
	}
	return BytesValue(unquoted)
}

func dateValueFromLiteral(days int64) DateValue {
	t := time.Unix(int64(time.Duration(days)*24*(time.Hour/time.Second)), 0)
	return DateValue(t)
}

const (
	secShift     = 0
	minShift     = 6
	hourShift    = 12
	dayShift     = 17
	monthShift   = 22
	yearShift    = 26
	microSecMask = 0xFFFFF
	secMask      = 0b111111
	minMask      = 0b111111 << minShift
	hourMask     = 0b11111 << hourShift
	dayMask      = 0b11111 << dayShift
	monthMask    = 0b1111 << monthShift
	yearMask     = 0x3FFF << yearShift
)

func datetimeValueFromLiteral(bit int64) DatetimeValue {
	b := bit >> 20
	year := (b & yearMask) >> yearShift
	month := (b & monthMask) >> monthShift
	day := (b & dayMask) >> dayShift
	hour := (b & hourMask) >> hourShift
	min := (b & minMask) >> minShift
	sec := (b & secMask) >> secShift
	microSec := (bit & microSecMask) >> 0
	t := time.Date(
		int(year),
		time.Month(month),
		int(day),
		int(hour),
		int(min),
		int(sec),
		int(microSec)*1000, time.UTC,
	)
	return DatetimeValue(t)
}

func timeValueFromLiteral(bit int64) TimeValue {
	b := bit >> 20
	hour := (b & hourMask) >> hourShift
	min := (b & minMask) >> minShift
	sec := (b & secMask) >> secShift
	microSec := (bit & microSecMask) >> 0
	t := time.Date(0, 0, 0, int(hour), int(min), int(sec), int(microSec)*1000, time.UTC)
	return TimeValue(t)
}

func timestampValueFromLiteral(t time.Time) (TimestampValue, error) {
	return TimestampValue(t), nil
}

var (
	numericLiteralPattern = regexp.MustCompile(`NUMERIC "(.+)"`)
)

func numericValueFromLiteral(lit string) (*NumericValue, error) {
	matches := numericLiteralPattern.FindAllStringSubmatch(lit, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("unexpected numeric literal: %s", lit)
	}
	if len(matches[0]) != 2 {
		return nil, fmt.Errorf("unexpected numeric literal: %s", lit)
	}
	numericLit := matches[0][1]
	r := new(big.Rat)
	r.SetString(numericLit)
	if strings.Contains(lit, "BIGNUMERIC") {
		return &NumericValue{Rat: r, isBigNumeric: true}, nil
	}
	return &NumericValue{Rat: r}, nil
}

func jsonValueFromLiteral(lit string) (JsonValue, error) {
	return JsonValue(lit), nil
}

var (
	intervalLiteralPattern = regexp.MustCompile(`INTERVAL "(.+)"`)
)

func intervalValueFromLiteral(lit string) (*IntervalValue, error) {
	matches := intervalLiteralPattern.FindAllStringSubmatch(lit, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("unexpected interval literal: %s", lit)
	}
	if len(matches[0]) != 2 {
		return nil, fmt.Errorf("unexpected interval literal: %s", lit)
	}
	intervalLit := matches[0][1]
	return parseInterval(intervalLit)
}

func arrayValueFromLiteral(v types.Value) (*ArrayValue, error) {
	ret := &ArrayValue{}
	for i := 0; i < v.NumElements(); i++ {
		elem := v.Element(i)
		value, err := ValueFromZetaSQLValue(elem)
		if err != nil {
			return nil, fmt.Errorf("failed to convert from zetasql value: %w", err)
		}
		ret.values = append(ret.values, value)
	}
	return ret, nil
}

func structValueFromLiteral(v types.Value) (*StructValue, error) {
	ret := &StructValue{
		m: map[string]Value{},
	}
	structType := v.Type().AsStruct()
	for i := 0; i < v.NumFields(); i++ {
		field := v.Field(i)
		name := structType.Field(i).Name()
		value, err := ValueFromZetaSQLValue(field)
		if err != nil {
			return nil, err
		}
		ret.keys = append(ret.keys, name)
		ret.values = append(ret.values, value)
		ret.m[name] = value
	}
	return ret, nil
}

func CastValue(t types.Type, v Value) (Value, error) {
	if v == nil {
		return nil, nil
	}
	switch t.Kind() {
	case types.INT32, types.INT64, types.UINT32, types.UINT64:
		i64, err := v.ToInt64()
		if err != nil {
			return nil, err
		}
		return IntValue(i64), nil
	case types.BOOL:
		b, err := v.ToBool()
		if err != nil {
			return nil, err
		}
		return BoolValue(b), nil
	case types.FLOAT, types.DOUBLE:
		f64, err := v.ToFloat64()
		if err != nil {
			return nil, err
		}
		return FloatValue(f64), nil
	case types.STRING, types.ENUM:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		return StringValue(s), nil
	case types.BYTES:
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		return BytesValue(b), nil
	case types.DATE:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	case types.DATETIME:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		return DatetimeValue(t), nil
	case types.TIME:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		return TimeValue(t), nil
	case types.TIMESTAMP:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		return TimestampValue(t), nil
	case types.INTERVAL:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		return parseInterval(s)
	case types.ARRAY:
		array, err := v.ToArray()
		if err != nil {
			return nil, err
		}
		elemType := t.AsArray().ElementType()
		ret := &ArrayValue{}
		for _, value := range array.values {
			casted, err := CastValue(elemType, value)
			if err != nil {
				return nil, err
			}
			ret.values = append(ret.values, casted)
		}
		return ret, nil
	case types.STRUCT:
		if array, ok := v.(*ArrayValue); ok {
			ret := &StructValue{m: map[string]Value{}}
			for _, value := range array.values {
				st, err := value.ToStruct()
				if err != nil {
					return nil, err
				}
				ret.keys = append(ret.keys, st.keys...)
				ret.values = append(ret.values, st.values...)
				for i, k := range st.keys {
					ret.m[k] = st.values[i]
				}
			}
			return ret, nil
		}
		s, err := v.ToStruct()
		if err != nil {
			return nil, err
		}
		typ := t.AsStruct()
		anonymousStruct := true
		for _, key := range s.keys {
			if key != "" {
				anonymousStruct = false
			}
		}
		if anonymousStruct {
			return s, nil
		}
		ret := &StructValue{m: s.m}
		for i := 0; i < typ.NumFields(); i++ {
			key := typ.Field(i).Name()
			value, exists := s.m[key]
			if !exists {
				ret.keys = append(ret.keys, key)
				ret.values = append(ret.values, nil)
				continue
			}
			casted, err := CastValue(typ.Field(i).Type(), value)
			if err != nil {
				return nil, err
			}
			ret.keys = append(ret.keys, key)
			ret.values = append(ret.values, casted)
			ret.m[key] = casted
		}
		return ret, nil
	case types.NUMERIC:
		r, err := v.ToRat()
		if err != nil {
			return nil, err
		}
		return &NumericValue{Rat: r}, nil
	case types.BIG_NUMERIC:
		r, err := v.ToRat()
		if err != nil {
			return nil, err
		}
		return &NumericValue{Rat: r, isBigNumeric: true}, nil
	case types.JSON:
		j, err := v.ToJSON()
		if err != nil {
			return nil, err
		}
		return JsonValue(j), nil
	case types.GEOGRAPHY:
		return v, nil
	}
	return nil, fmt.Errorf("unsupported cast %s value", t.Kind())
}

func ValueFromGoValue(v interface{}) (Value, error) {
	if isNullValue(v) {
		return nil, nil
	}
	return valueFromGoReflectValue(reflect.ValueOf(v))
}

func valueFromGoReflectValue(v reflect.Value) (Value, error) {
	kind := v.Type().Kind()
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return IntValue(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return IntValue(int64(v.Uint())), nil
	case reflect.Float32, reflect.Float64:
		return FloatValue(v.Float()), nil
	case reflect.Bool:
		return BoolValue(v.Bool()), nil
	case reflect.String:
		return StringValue(v.String()), nil
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return BytesValue(v.Bytes()), nil
		}
		ret := &ArrayValue{}
		for i := 0; i < v.Len(); i++ {
			elem, err := valueFromGoReflectValue(v.Index(i))
			if err != nil {
				return nil, err
			}
			ret.values = append(ret.values, elem)
		}
		return ret, nil
	case reflect.Map:
		ret := &StructValue{m: map[string]Value{}}
		iter := v.MapRange()
		for iter.Next() {
			key, err := valueFromGoReflectValue(iter.Key())
			if err != nil {
				return nil, err
			}
			k, err := key.ToString()
			if err != nil {
				return nil, err
			}
			value, err := valueFromGoReflectValue(iter.Value())
			if err != nil {
				return nil, err
			}
			ret.keys = append(ret.keys, k)
			ret.values = append(ret.values, value)
			ret.m[k] = value
		}
		return ret, nil
	case reflect.Struct:
		t, ok := v.Interface().(time.Time)
		if ok {
			return TimestampValue(t), nil
		}
		ret := &StructValue{m: map[string]Value{}}
		typ := v.Type()
		for i := 0; i < v.NumField(); i++ {
			key := typ.Field(i).Name
			value, err := valueFromGoReflectValue(v.Field(i))
			if err != nil {
				return nil, err
			}
			ret.keys = append(ret.keys, key)
			ret.values = append(ret.values, value)
			ret.m[key] = value
		}
		return ret, nil
	case reflect.Ptr:
		return valueFromGoReflectValue(v.Elem())
	case reflect.Interface:
		vv := v.Interface()
		if isNullValue(vv) {
			return nil, nil
		}
		return valueFromGoReflectValue(reflect.ValueOf(vv))
	}
	return nil, fmt.Errorf("cannot convert %s type to zetasqlite value type", kind)
}

func encodeNamedValue(v driver.NamedValue, param *ast.ParameterNode) (sql.NamedArg, error) {
	value, err := EncodeGoValue(param.Type(), v.Value)
	if err != nil {
		return sql.NamedArg{}, err
	}
	return sql.NamedArg{
		Name:  strings.ToLower(v.Name),
		Value: value,
	}, nil
}

func valueLayoutFromValue(v Value) (*ValueLayout, error) {
	switch vv := v.(type) {
	case StringValue:
		return &ValueLayout{
			Header: StringValueType,
			Body:   string(vv),
		}, nil
	case BytesValue:
		return &ValueLayout{
			Header: BytesValueType,
			Body:   base64.StdEncoding.EncodeToString([]byte(vv)),
		}, nil
	case *NumericValue:
		b, err := vv.Rat.MarshalText()
		if err != nil {
			return nil, err
		}
		if vv.isBigNumeric {
			return &ValueLayout{
				Header: BigNumericValueType,
				Body:   string(b),
			}, nil
		}
		return &ValueLayout{
			Header: NumericValueType,
			Body:   string(b),
		}, nil
	case DateValue:
		body, err := vv.ToString()
		if err != nil {
			return nil, err
		}
		return &ValueLayout{
			Header: DateValueType,
			Body:   body,
		}, nil
	case DatetimeValue:
		body, err := vv.ToString()
		if err != nil {
			return nil, err
		}
		return &ValueLayout{
			Header: DatetimeValueType,
			Body:   body,
		}, nil
	case TimeValue:
		body, err := vv.ToString()
		if err != nil {
			return nil, err
		}
		return &ValueLayout{
			Header: TimeValueType,
			Body:   body,
		}, nil
	case TimestampValue:
		return &ValueLayout{
			Header: TimestampValueType,
			Body:   fmt.Sprint(time.Time(vv).UnixMicro()),
		}, nil
	case *IntervalValue:
		s, err := vv.ToString()
		if err != nil {
			return nil, err
		}
		return &ValueLayout{
			Header: IntervalValueType,
			Body:   s,
		}, nil
	case JsonValue:
		return &ValueLayout{
			Header: JsonValueType,
			Body:   string(vv),
		}, nil
	case *ArrayValue:
		values := make([]interface{}, 0, len(vv.values))
		for _, v := range vv.values {
			if v == nil {
				values = append(values, nil)
				continue
			}
			value, err := EncodeValue(v)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		body, err := json.Marshal(values)
		if err != nil {
			return nil, err
		}
		return &ValueLayout{
			Header: ArrayValueType,
			Body:   string(body),
		}, nil
	case *StructValue:
		values := make([]interface{}, 0, len(vv.values))
		for _, v := range vv.values {
			value, err := EncodeValue(v)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		body, err := json.Marshal(&StructValueLayout{
			Keys:   vv.keys,
			Values: values,
		})
		if err != nil {
			return nil, err
		}
		return &ValueLayout{
			Header: StructValueType,
			Body:   string(body),
		}, nil
	case *GeographyValue:
		s, err := vv.ToWKT()
		if err != nil {
			return nil, err
		}
		return &ValueLayout{
			Header: GeographyValueType,
			Body:   s,
		}, nil
	}
	return nil, fmt.Errorf("unexpected value type to get value layout: %T", v)
}
