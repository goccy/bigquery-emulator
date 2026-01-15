package internal

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

func ADD(a, b Value) (Value, error) {
	return a.Add(b)
}

func SUB(a, b Value) (Value, error) {
	return a.Sub(b)
}

func MUL(a, b Value) (Value, error) {
	return a.Mul(b)
}

func OP_DIV(a, b Value) (Value, error) {
	return a.Div(b)
}

func EQ(a, b Value) (Value, error) {
	cond, err := a.EQ(b)
	if err != nil {
		return nil, err
	}
	return BoolValue(cond), nil
}

func NOT_EQ(a, b Value) (Value, error) {
	cond, err := a.EQ(b)
	if err != nil {
		return nil, err
	}
	return BoolValue(!cond), nil
}

func GT(a, b Value) (Value, error) {
	cond, err := a.GT(b)
	if err != nil {
		return nil, err
	}
	return BoolValue(cond), nil
}

func GTE(a, b Value) (Value, error) {
	cond, err := a.GTE(b)
	if err != nil {
		return nil, err
	}
	return BoolValue(cond), nil
}

func LT(a, b Value) (Value, error) {
	cond, err := a.LT(b)
	if err != nil {
		return nil, err
	}
	return BoolValue(cond), nil
}

func LTE(a, b Value) (Value, error) {
	cond, err := a.LTE(b)
	if err != nil {
		return nil, err
	}
	return BoolValue(cond), nil
}

func BIT_NOT(a Value) (Value, error) {
	v, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	return IntValue(^v), nil
}

func BIT_LEFT_SHIFT(a, b Value) (Value, error) {
	va, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToInt64()
	if err != nil {
		return nil, err
	}
	return IntValue(va << vb), nil
}

func BIT_RIGHT_SHIFT(a, b Value) (Value, error) {
	va, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToInt64()
	if err != nil {
		return nil, err
	}
	return IntValue(va >> vb), nil
}

func BIT_AND(a, b Value) (Value, error) {
	va, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToInt64()
	if err != nil {
		return nil, err
	}
	return IntValue(va & vb), nil
}

func BIT_OR(a, b Value) (Value, error) {
	va, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToInt64()
	if err != nil {
		return nil, err
	}
	return IntValue(va | vb), nil
}

func BIT_XOR(a, b Value) (Value, error) {
	va, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToInt64()
	if err != nil {
		return nil, err
	}
	return IntValue(va ^ vb), nil
}

func ARRAY_IN(a, b Value) (Value, error) {
	array, err := b.ToArray()
	if err != nil {
		return nil, err
	}
	cond, err := array.Has(a)
	if err != nil {
		return nil, err
	}
	return BoolValue(cond), nil
}

func STRUCT_FIELD(v Value, idx int) (Value, error) {
	sv, err := v.ToStruct()
	if err != nil {
		return nil, err
	}
	return sv.values[idx], nil
}

func ARRAY_OFFSET(v Value, idx int) (Value, error) {
	array, err := v.ToArray()
	if err != nil {
		return nil, err
	}
	if idx < 0 || len(array.values) <= idx {
		return nil, fmt.Errorf("OFFSET(%d) is out of range", idx)
	}
	return array.values[idx], nil
}

func ARRAY_SAFE_OFFSET(v Value, idx int) (Value, error) {
	array, err := v.ToArray()
	if err != nil {
		return nil, err
	}
	if idx < 0 || len(array.values) <= idx {
		return nil, nil
	}
	return array.values[idx], nil
}

func ARRAY_ORDINAL(v Value, idx int) (Value, error) {
	array, err := v.ToArray()
	if err != nil {
		return nil, err
	}
	if idx < 1 || len(array.values) < idx {
		return nil, fmt.Errorf("ORDINAL(%d) is out of range", idx)
	}
	return array.values[idx-1], nil
}

func ARRAY_SAFE_ORDINAL(v Value, idx int) (Value, error) {
	array, err := v.ToArray()
	if err != nil {
		return nil, err
	}
	if idx < 1 || len(array.values) < idx {
		return nil, nil
	}
	return array.values[idx-1], nil
}

func LIKE(a, b Value) (Value, error) {
	va, err := a.ToString()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToString()
	if err != nil {
		return nil, err
	}
	wildcard := strings.Replace(regexp.QuoteMeta(vb), "%", ".*", -1)
	matchLimits := fmt.Sprintf("^%s$", wildcard)
	re, err := regexp.Compile(matchLimits)
	if err != nil {
		return nil, err
	}
	return BoolValue(re.MatchString(va)), nil
}

func BETWEEN(target, start, end Value) (Value, error) {
	greaterThanStart, err := target.GTE(start)
	if err != nil {
		return nil, err
	}
	lessThanEnd, err := target.LTE(end)
	if err != nil {
		return nil, err
	}

	return BoolValue(greaterThanStart && lessThanEnd), nil
}

func IN(a Value, values ...Value) (Value, error) {
	if a == nil {
		return nil, nil
	}
	for _, v := range values {
		if v == nil {
			continue
		}
		cond, err := a.EQ(v)
		if err != nil {
			return nil, err
		}
		if cond {
			return BoolValue(true), nil
		}
	}
	return BoolValue(false), nil
}

func IS_NULL(a Value) (Value, error) {
	return BoolValue(a == nil), nil
}

func IS_TRUE(a Value) (Value, error) {
	b, err := a.ToBool()
	if err != nil {
		return nil, err
	}
	return BoolValue(b), nil
}

func IS_FALSE(a Value) (Value, error) {
	b, err := a.ToBool()
	if err != nil {
		return nil, err
	}
	return BoolValue(!b), nil
}

func NOT(a Value) (Value, error) {
	v, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	return BoolValue(v == 0), nil
}

func AND(args ...Value) (Value, error) {
	for _, v := range args {
		if v == nil {
			continue
		}
		cond, err := v.ToBool()
		if err != nil {
			return nil, err
		}
		if !cond {
			return BoolValue(false), nil
		}
	}
	// if exists null value and not exists false value, returns null.
	if existsNull(args) {
		return nil, nil
	}
	return BoolValue(true), nil
}

func OR(args ...Value) (Value, error) {
	for _, v := range args {
		if v == nil {
			continue
		}
		cond, err := v.ToBool()
		if err != nil {
			return nil, err
		}
		if cond {
			return BoolValue(true), nil
		}
	}
	// if exists null value and not exists true value, returns null.
	if existsNull(args) {
		return nil, nil
	}
	return BoolValue(false), nil
}

func IS_DISTINCT_FROM(a, b Value) (Value, error) {
	if a == nil || b == nil {
		eq := a == nil && b == nil
		return BoolValue(!eq), nil
	}
	cond, err := a.EQ(b)
	if err != nil {
		return nil, err
	}
	return BoolValue(!cond), nil
}

func IS_NOT_DISTINCT_FROM(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return BoolValue(a == nil && b == nil), nil
	}
	cond, err := a.EQ(b)
	if err != nil {
		return nil, err
	}
	return BoolValue(cond), nil
}

func COALESCE(args ...Value) (Value, error) {
	for _, arg := range args {
		if arg == nil {
			continue
		}
		return arg, nil
	}
	return nil, nil
}

func IF(cond, trueV, falseV Value) (Value, error) {
	if cond == nil {
		return falseV, nil
	}
	b, err := cond.ToBool()
	if err != nil {
		return nil, err
	}
	if b {
		return trueV, nil
	}
	return falseV, nil
}

func IFNULL(expr, nullResult Value) (Value, error) {
	if expr == nil {
		return nullResult, nil
	}
	return expr, nil
}

func NULLIF(expr, exprToMatch Value) (Value, error) {
	if expr == nil {
		return nil, nil
	}
	cond, err := expr.EQ(exprToMatch)
	if err != nil {
		return nil, err
	}
	if cond {
		return nil, nil
	}
	return expr, nil
}

func MAKE_ARRAY(args ...Value) (Value, error) {
	return &ArrayValue{values: args}, nil
}

func MAKE_STRUCT(args ...Value) (Value, error) {
	keys := make([]string, len(args)/2)
	values := make([]Value, len(args)/2)
	fieldMap := map[string]Value{}
	for i := 0; i < len(args)/2; i++ {
		key := args[i*2]
		value := args[i*2+1]
		k, err := key.ToString()
		if err != nil {
			return nil, err
		}
		keys[i] = k
		values[i] = value
		fieldMap[k] = value
	}
	return &StructValue{
		keys:   keys,
		values: values,
		m:      fieldMap,
	}, nil
}

func EXTRACT(v Value, part, zone string) (Value, error) {
	switch vv := v.(type) {
	case *IntervalValue:
		switch part {
		case "YEAR":
			return IntValue(vv.Years), nil
		case "MONTH":
			return IntValue(vv.Months), nil
		case "DAY":
			return IntValue(vv.Days), nil
		case "HOUR":
			return IntValue(vv.Hours), nil
		case "MINUTE":
			return IntValue(vv.Minutes), nil
		case "SECOND":
			return IntValue(vv.Seconds), nil
		case "MILLISECOND":
			return IntValue(vv.SubSecondNanos / int32(time.Millisecond)), nil
		case "MICROSECOND":
			return IntValue(vv.SubSecondNanos / int32(time.Microsecond)), nil
		}
		return nil, fmt.Errorf("EXTRACT: unexpected part %s for interval", part)
	case DateValue, DatetimeValue, TimeValue, TimestampValue:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		if _, ok := v.(TimestampValue); ok {
			loc, err := toLocation(zone)
			if err != nil {
				return nil, err
			}
			t = t.In(loc)
		}
		switch part {
		case "ISOYEAR":
			year, _ := t.ISOWeek()
			return IntValue(year), nil
		case "YEAR":
			return IntValue(t.Year()), nil
		case "MONTH":
			return IntValue(t.Month()), nil
		case "ISOWEEK":
			_, week := t.ISOWeek()
			return IntValue(week), nil
		case "WEEK":
			_, week := t.AddDate(0, 0, -int(t.Weekday())).ISOWeek()
			return IntValue(week), nil
		case "DAY":
			return IntValue(t.Day()), nil
		case "DAYOFYEAR":
			return IntValue(t.YearDay()), nil
		case "DAYOFWEEK":
			return IntValue(int(t.Weekday()) + 1), nil
		case "QUARTER":
			day := t.YearDay()
			const quarterDays = 91
			switch {
			case day <= quarterDays:
				return IntValue(1), nil
			case day <= quarterDays*2:
				return IntValue(2), nil
			case day <= quarterDays*3:
				return IntValue(3), nil
			}
			return IntValue(4), nil
		case "HOUR":
			return IntValue(t.Hour()), nil
		case "MINUTE":
			return IntValue(t.Minute()), nil
		case "SECOND":
			return IntValue(t.Second()), nil
		case "MILLISECOND":
			return IntValue(t.Nanosecond() / int(time.Millisecond)), nil
		case "MICROSECOND":
			return IntValue(t.Nanosecond() / int(time.Microsecond)), nil
		case "DATE":
			return DateValue(t), nil
		case "DATETIME":
			return DatetimeValue(t), nil
		case "TIME":
			return TimeValue(t), nil
		}
		return nil, fmt.Errorf("EXTRACT: unexpected part %s for data/datetime/time/timestamp", part)
	}
	return nil, fmt.Errorf("EXTRACT: value type must be INTERVAL or DATE or DATETIME or TIME or TIMESTAMP")
}

func GENERATE_UUID() (Value, error) {
	id := uuid.NewString()
	return StringValue(id), nil
}

func CAST(expr Value, fromType, toType *Type, isSafeCast bool) (Value, error) {
	from, err := fromType.ToZetaSQLType()
	if err != nil {
		return nil, fmt.Errorf("failed to get zetasql type from cast base type: %w", err)
	}
	to, err := toType.ToZetaSQLType()
	if err != nil {
		return nil, fmt.Errorf("failed to get zetasql type from cast target type: %w", err)
	}
	fromValue, err := CastValue(from, expr)
	if err != nil {
		if isSafeCast {
			return nil, nil
		}
		return nil, err
	}
	casted, err := CastValue(to, fromValue)
	if err != nil {
		if isSafeCast {
			return nil, nil
		}
		return nil, err
	}
	return casted, nil
}
