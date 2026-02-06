package internal

import (
	"fmt"
	"time"
)

func CURRENT_TIME(zone string) (Value, error) {
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	return CURRENT_TIME_WITH_TIME(time.Now().In(loc))
}

func CURRENT_TIME_WITH_TIME(v time.Time) (Value, error) {
	return TimeValue(v), nil
}

func TIME(args ...Value) (Value, error) {
	if len(args) == 3 {
		hour, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		min, err := args[1].ToInt64()
		if err != nil {
			return nil, err
		}
		sec, err := args[2].ToInt64()
		if err != nil {
			return nil, err
		}
		loc, err := toLocation("")
		if err != nil {
			return nil, err
		}
		return TimeValue(time.Date(0, 0, 0, int(hour), int(min), int(sec), 0, loc)), nil
	}
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("TIME: invalid argument num %d", len(args))
	}
	switch args[0].(type) {
	case TimestampValue:
		t, err := args[0].ToTime()
		if err != nil {
			return nil, err
		}
		if len(args) == 2 {
			zone, err := args[1].ToString()
			if err != nil {
				return nil, err
			}
			loc, err := toLocation(zone)
			if err != nil {
				return nil, err
			}
			return TimeValue(t.In(loc)), nil
		}
		return TimeValue(t), nil
	case DatetimeValue:
		t, err := args[0].ToTime()
		if err != nil {
			return nil, err
		}
		return TimeValue(t), nil
	}
	return nil, fmt.Errorf("TIME: invalid first argument type %T", args[0])
}

func TIME_ADD(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return TimeValue(t.Add(time.Duration(v) * time.Microsecond)), nil
	case "MILLISECOND":
		return TimeValue(t.Add(time.Duration(v) * time.Millisecond)), nil
	case "SECOND":
		return TimeValue(t.Add(time.Duration(v) * time.Second)), nil
	case "MINUTE":
		return TimeValue(t.Add(time.Duration(v) * time.Minute)), nil
	case "HOUR":
		return TimeValue(t.Add(time.Duration(v) * time.Hour)), nil
	}
	return nil, fmt.Errorf("TIME_ADD: unexpected part value %s", part)
}

func TIME_SUB(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return TimeValue(t.Add(-time.Duration(v) * time.Microsecond)), nil
	case "MILLISECOND":
		return TimeValue(t.Add(-time.Duration(v) * time.Millisecond)), nil
	case "SECOND":
		return TimeValue(t.Add(-time.Duration(v) * time.Second)), nil
	case "MINUTE":
		return TimeValue(t.Add(-time.Duration(v) * time.Minute)), nil
	case "HOUR":
		return TimeValue(t.Add(-time.Duration(v) * time.Hour)), nil
	}
	return nil, fmt.Errorf("TIME_SUB: unexpected part value %s", part)
}

func TIME_DIFF(a, b time.Time, part string) (Value, error) {
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
	return nil, fmt.Errorf("TIME_DIFF: unexpected part value %s", part)
}

func TIME_TRUNC(t time.Time, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return TimeValue(t), nil
	case "MILLISECOND":
		sec := time.Duration(t.Second()) - time.Duration(t.Second())/time.Microsecond
		return TimeValue(time.Date(
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
		return TimeValue(time.Date(
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
		return TimeValue(time.Date(
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
		return TimeValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			0,
			0,
			0,
			t.Location(),
		)), nil
	}
	return nil, fmt.Errorf("TIME_TRUNC: unexpected part value %s", part)
}

func FORMAT_TIME(format string, t time.Time) (Value, error) {
	s, err := formatTime(format, &t, FormatTypeTime)
	if err != nil {
		return nil, err
	}
	return StringValue(s), nil
}

func PARSE_TIME(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeTime)
	if err != nil {
		return nil, err
	}
	return TimeValue(*t), nil
}
