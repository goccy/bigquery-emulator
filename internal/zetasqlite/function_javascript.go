package zetasqlite

import (
	"fmt"
	"math/big"
	"time"

	"github.com/dop251/goja"
	"github.com/glassmonkey/zetasql-wasm/types"
)

func EVAL_JAVASCRIPT(code string, retType *Type, argNames []string, args []Value) (Value, error) {
	vm := goja.New()
	for i := 0; i < len(args); i++ {
		var v interface{}
		if args[i] != nil {
			structV, ok := args[i].(*StructValue)
			if ok {
				v = structV.m
			} else {
				v = args[i].Interface()
			}
		}
		if err := vm.Set(argNames[i], v); err != nil {
			return nil, fmt.Errorf(
				"failed to set argument variable for %s as %v",
				argNames[i],
				args[i],
			)
		}
	}
	evalCode := fmt.Sprintf(`
function zetasqlite_javascript_func() { %s }
zetasqlite_javascript_func();
`, code)
	ret, err := vm.RunString(evalCode)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate javascript code %s: %w", code, err)
	}
	typ, err := retType.ToZetaSQLType()
	if err != nil {
		return nil, fmt.Errorf("failed to get return type: %w", err)
	}
	value, err := castJavaScriptValue(typ, ret)
	if err != nil {
		return nil, fmt.Errorf("failed to convert zetasqlite value from %v: %w", ret, err)
	}
	return value, nil
}

func castJavaScriptValue(t types.Type, v goja.Value) (Value, error) {
	if v == nil {
		return nil, nil
	}
	switch t.Kind() {
	case types.Int32, types.Int64, types.Uint32, types.Uint64:
		return IntValue(v.ToInteger()), nil
	case types.Bool:
		return BoolValue(v.ToBoolean()), nil
	case types.Float, types.Double:
		return FloatValue(v.ToFloat()), nil
	case types.String, types.Enum:
		return StringValue(v.ToString().String()), nil
	case types.Bytes:
		return BytesValue(v.ToString().String()), nil
	case types.Date:
		t, err := parseDate(v.ToString().String())
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	case types.Datetime:
		t, err := parseDatetime(v.ToString().String())
		if err != nil {
			return nil, err
		}
		return DatetimeValue(t), nil
	case types.Time:
		t, err := parseTime(v.ToString().String())
		if err != nil {
			return nil, err
		}
		return TimeValue(t), nil
	case types.Timestamp:
		t, err := parseTimestamp(v.ToString().String(), time.UTC)
		if err != nil {
			return nil, err
		}
		return TimestampValue(t), nil
	case types.Interval:
		return parseInterval(v.ToString().String())
	case types.Numeric:
		r := new(big.Rat)
		r.SetString(v.ToNumber().String())
		return &NumericValue{Rat: r}, nil
	case types.BigNumeric:
		r := new(big.Rat)
		r.SetString(v.ToNumber().String())
		return &NumericValue{Rat: r}, nil
	case types.Json:
		return JsonValue(v.ToString().String()), nil
	case types.Array:
		elemType := t.AsArray().ElementType
		var ret ArrayValue
		for _, vv := range v.Export().([]interface{}) {
			base, err := ValueFromGoValue(vv)
			if err != nil {
				return nil, err
			}
			elem, err := CastValue(elemType, base)
			if err != nil {
				return nil, err
			}
			ret.values = append(ret.values, elem)
		}
		return &ret, nil
	case types.Struct:
		base, err := ValueFromGoValue(v.Export())
		if err != nil {
			return nil, err
		}
		return CastValue(t, base)
	case types.Geography:
		base, err := ValueFromGoValue(v.Export())
		if err != nil {
			return nil, err
		}
		return CastValue(t, base)
	}
	return nil, fmt.Errorf("unsupported cast %s from JavaScript value", t.Kind())
}
