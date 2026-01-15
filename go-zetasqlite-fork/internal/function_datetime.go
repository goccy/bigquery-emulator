package internal

import (
	"fmt"
	"time"
)

func CURRENT_DATETIME(zone string) (Value, error) {
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	return CURRENT_DATETIME_WITH_TIME(time.Now().In(loc))
}

func CURRENT_DATETIME_WITH_TIME(v time.Time) (Value, error) {
	return DatetimeValue(v), nil
}

func DATETIME(args ...Value) (Value, error) {
	if len(args) == 6 {
		year, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		month, err := args[1].ToInt64()
		if err != nil {
			return nil, err
		}
		day, err := args[2].ToInt64()
		if err != nil {
			return nil, err
		}
		hour, err := args[3].ToInt64()
		if err != nil {
			return nil, err
		}
		minute, err := args[4].ToInt64()
		if err != nil {
			return nil, err
		}
		second, err := args[5].ToInt64()
		if err != nil {
			return nil, err
		}
		location, err := toLocation("")
		if err != nil {
			return nil, err
		}
		return DatetimeValue(time.Date(
			int(year),
			time.Month(month),
			int(day),
			int(hour),
			int(minute),
			int(second),
			0,
			location,
		)), nil
	}
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("DATETIME: invalid argument num %d", len(args))
	}
	switch v := args[0].(type) {
	case DateValue:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		if len(args) == 2 {
			t2, err := args[1].ToTime()
			if err != nil {
				return nil, fmt.Errorf("DATETIME: second argument must be time type: %w", err)
			}
			return DatetimeValue(time.Date(
				t.Year(),
				t.Month(),
				t.Day(),
				t2.Hour(),
				t2.Minute(),
				t2.Second(),
				t2.Nanosecond(),
				t2.Location(),
			)), nil
		}
		return DatetimeValue(t), nil
	case TimestampValue:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		if len(args) == 2 {
			zone, err := args[1].ToString()
			if err != nil {
				return nil, fmt.Errorf("DATETIME: second argument must be string type: %w", err)
			}
			loc, err := toLocation(zone)
			if err != nil {
				return nil, err
			}
			return DatetimeValue(t.In(loc)), nil
		}
		return DatetimeValue(t), nil
	}
	return nil, fmt.Errorf("DATETIME: first argument must be DATE or TIMESTAMP type")
}

func DATETIME_ADD(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return DatetimeValue(t.Add(time.Duration(v) * time.Microsecond)), nil
	case "MILLISECOND":
		return DatetimeValue(t.Add(time.Duration(v) * time.Millisecond)), nil
	case "SECOND":
		return DatetimeValue(t.Add(time.Duration(v) * time.Second)), nil
	case "MINUTE":
		return DatetimeValue(t.Add(time.Duration(v) * time.Minute)), nil
	case "HOUR":
		return DatetimeValue(t.Add(time.Duration(v) * time.Hour)), nil
	default:
		date, err := DATE_ADD(t, v, part)
		if err != nil {
			return nil, fmt.Errorf("DATETIME_ADD: %w", err)
		}
		datetime, err := date.ToTime()
		if err != nil {
			return nil, fmt.Errorf("DATETIME_ADD: %w", err)
		}
		return DatetimeValue(
			time.Date(
				datetime.Year(),
				datetime.Month(),
				datetime.Day(),
				t.Hour(),
				t.Minute(),
				t.Second(),
				t.Nanosecond(),
				t.Location(),
			),
		), nil
	}
}

func DATETIME_SUB(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return DatetimeValue(t.Add(-time.Duration(v) * time.Microsecond)), nil
	case "MILLISECOND":
		return DatetimeValue(t.Add(-time.Duration(v) * time.Millisecond)), nil
	case "SECOND":
		return DatetimeValue(t.Add(-time.Duration(v) * time.Second)), nil
	case "MINUTE":
		return DatetimeValue(t.Add(-time.Duration(v) * time.Minute)), nil
	case "HOUR":
		return DatetimeValue(t.Add(-time.Duration(v) * time.Hour)), nil
	default:
		date, err := DATE_SUB(t, v, part)
		if err != nil {
			return nil, fmt.Errorf("DATETIME_SUB: %w", err)
		}
		datetime, err := date.ToTime()
		if err != nil {
			return nil, fmt.Errorf("DATETIME_SUB: %w", err)
		}
		return DatetimeValue(
			time.Date(
				datetime.Year(),
				datetime.Month(),
				datetime.Day(),
				t.Hour(),
				t.Minute(),
				t.Second(),
				t.Nanosecond(),
				t.Location(),
			),
		), nil
	}
}

func DATETIME_DIFF(a, b time.Time, part string) (Value, error) {
	diff := a.Sub(b)
	switch part {
	case "MICROSECOND":
		return IntValue(diff / time.Microsecond), nil
	case "MILLISECOND":
		return IntValue(diff / time.Millisecond), nil
	case "SECOND":
		return IntValue(diff / time.Second), nil
	case "MINUTE":
		return IntValue(diff / time.Minute), nil
	case "HOUR":
		return IntValue(diff / time.Hour), nil
	}

	value, err := DATE_DIFF(a, b, part)
	if err != nil {
		return nil, fmt.Errorf("DATETIME_DIFF: %w", err)
	}
	return value, nil
}

func DATETIME_TRUNC(t time.Time, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return DatetimeValue(t), nil
	case "MILLISECOND":
		sec := time.Duration(t.Second()) - time.Duration(t.Second())/time.Microsecond
		return DatetimeValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			int(sec),
			0,
			t.Location(),
		)), nil
	case "SECOND":
		sec := time.Duration(t.Second()) / time.Second
		return DatetimeValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			int(sec),
			0,
			t.Location(),
		)), nil
	case "MINUTE":
		return DatetimeValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			0,
			0,
			t.Location(),
		)), nil
	case "HOUR":
		return DatetimeValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			0,
			0,
			0,
			t.Location(),
		)), nil
	default:
		date, err := DATE_TRUNC(t, part)
		if err != nil {
			return nil, fmt.Errorf("DATETIME_TRUNC: %w", err)
		}
		datetime, err := date.ToTime()
		if err != nil {
			return nil, fmt.Errorf("DATETIME_TRUNC: %w", err)
		}
		return DatetimeValue(
			time.Date(
				datetime.Year(),
				datetime.Month(),
				datetime.Day(),
				datetime.Hour(),
				datetime.Minute(),
				datetime.Second(),
				datetime.Nanosecond(),
				datetime.Location(),
			),
		), nil
	}
}

func FORMAT_DATETIME(format string, t time.Time) (Value, error) {
	s, err := formatTime(format, &t, FormatTypeDatetime)
	if err != nil {
		return nil, err
	}
	return StringValue(s), nil
}

func PARSE_DATETIME(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeDatetime)
	if err != nil {
		return nil, err
	}
	return DatetimeValue(*t), nil
}
