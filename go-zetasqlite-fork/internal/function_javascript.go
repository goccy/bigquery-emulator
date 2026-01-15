package internal

import (
	"fmt"
	"math/big"
	"time"

	"github.com/dop251/goja"
	"github.com/goccy/go-zetasql/types"
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
	case types.INT32, types.INT64, types.UINT32, types.UINT64:
		return IntValue(v.ToInteger()), nil
	case types.BOOL:
		return BoolValue(v.ToBoolean()), nil
	case types.FLOAT, types.DOUBLE:
		return FloatValue(v.ToFloat()), nil
	case types.STRING, types.ENUM:
		return StringValue(v.ToString().String()), nil
	case types.BYTES:
		return BytesValue(v.ToString().String()), nil
	case types.DATE:
		t, err := parseDate(v.ToString().String())
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	case types.DATETIME:
		t, err := parseDatetime(v.ToString().String())
		if err != nil {
			return nil, err
		}
		return DatetimeValue(t), nil
	case types.TIME:
		t, err := parseTime(v.ToString().String())
		if err != nil {
			return nil, err
		}
		return TimeValue(t), nil
	case types.TIMESTAMP:
		t, err := parseTimestamp(v.ToString().String(), time.UTC)
		if err != nil {
			return nil, err
		}
		return TimestampValue(t), nil
	case types.INTERVAL:
		return parseInterval(v.ToString().String())
	case types.NUMERIC:
		r := new(big.Rat)
		r.SetString(v.ToNumber().String())
		return &NumericValue{Rat: r}, nil
	case types.BIG_NUMERIC:
		r := new(big.Rat)
		r.SetString(v.ToNumber().String())
		return &NumericValue{Rat: r}, nil
	case types.JSON:
		return JsonValue(v.ToString().String()), nil
	case types.ARRAY:
		elemType := t.AsArray().ElementType()
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
	case types.STRUCT:
		base, err := ValueFromGoValue(v.Export())
		if err != nil {
			return nil, err
		}
		return CastValue(t, base)
	case types.GEOGRAPHY:
		base, err := ValueFromGoValue(v.Export())
		if err != nil {
			return nil, err
		}
		return CastValue(t, base)
	}
	return nil, fmt.Errorf("unsupported cast %s from JavaScript value", t.Kind())
}
