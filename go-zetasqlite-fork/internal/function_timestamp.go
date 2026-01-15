package internal

import (
	"fmt"
	"time"
)

func CURRENT_TIMESTAMP(zone string) (Value, error) {
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	return CURRENT_TIMESTAMP_WITH_TIME(time.Now().In(loc))
}

func CURRENT_TIMESTAMP_WITH_TIME(v time.Time) (Value, error) {
	return TimestampValue(v), nil
}

func STRING(t time.Time, zone string) (Value, error) {
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	return StringValue(t.In(loc).Format("2006-01-02 15:04:05.999999999+00")), nil
}

func TIMESTAMP(v Value, zone string) (Value, error) {
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	switch v.(type) {
	case StringValue:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		t, err := parseTimestamp(s, loc)
		if err != nil {
			return nil, err
		}
		return TimestampValue(t), nil
	case DateValue, DatetimeValue:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		modified, err := modifyTimeZone(t, loc)
		if err != nil {
			return nil, err
		}
		return TimestampValue(modified), nil
	}
	return nil, fmt.Errorf("TIMESTAMP: invalid first argument type %T", v)
}

func TIMESTAMP_ADD(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return TimestampValue(t.Add(time.Duration(v) * time.Microsecond)), nil
	case "MILLISECOND":
		return TimestampValue(t.Add(time.Duration(v) * time.Millisecond)), nil
	case "SECOND":
		return TimestampValue(t.Add(time.Duration(v) * time.Second)), nil
	case "MINUTE":
		return TimestampValue(t.Add(time.Duration(v) * time.Minute)), nil
	case "HOUR":
		return TimestampValue(t.Add(time.Duration(v) * time.Hour)), nil
	case "DAY":
		return TimestampValue(t.AddDate(0, 0, int(v))), nil
	}
	return nil, fmt.Errorf("TIMESTAMP_ADD: unexpected part value %s", part)
}

func TIMESTAMP_SUB(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return TimestampValue(t.Add(-time.Duration(v) * time.Microsecond)), nil
	case "MILLISECOND":
		return TimestampValue(t.Add(-time.Duration(v) * time.Millisecond)), nil
	case "SECOND":
		return TimestampValue(t.Add(-time.Duration(v) * time.Second)), nil
	case "MINUTE":
		return TimestampValue(t.Add(-time.Duration(v) * time.Minute)), nil
	case "HOUR":
		return TimestampValue(t.Add(-time.Duration(v) * time.Hour)), nil
	case "DAY":
		return TimestampValue(t.AddDate(0, 0, -int(v))), nil
	}
	return nil, fmt.Errorf("TIMESTAMP_SUB: unexpected part value %s", part)
}

func TIMESTAMP_DIFF(a, b time.Time, part string) (Value, error) {
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
	default:
		dateDiff, err := DATE_DIFF(a, b, part)
		if err != nil {
			return nil, fmt.Errorf("TIMESTAMP_DIFF: %w", err)
		}

		return dateDiff, nil
	}
}

func TIMESTAMP_TRUNC(t time.Time, part, zone string) (Value, error) {
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	t = t.In(loc)

	switch part {
	case "MICROSECOND":
		return TimestampValue(t), nil
	case "MILLISECOND":
		nsec := (t.Nanosecond() / int(time.Millisecond)) * int(time.Millisecond)
		return TimestampValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
			nsec,
			loc,
		)), nil
	case "SECOND":
		return TimestampValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
			0,
			loc,
		)), nil
	case "MINUTE":
		return TimestampValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			0,
			0,
			loc,
		)), nil
	case "HOUR":
		return TimestampValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			0,
			0,
			0,
			loc,
		)), nil
	default:
		date, err := DATE_TRUNC(t, part)
		if err != nil {
			return nil, fmt.Errorf("TIMESTAMP_TRUNC: %w", err)
		}
		dateTime, err := date.ToTime()
		if err != nil {
			return nil, fmt.Errorf("TIMESTAMP_TRUNC: %w", err)
		}
		return TimestampValue(time.Date(
			dateTime.Year(),
			dateTime.Month(),
			dateTime.Day(),
			0,
			0,
			0,
			0,
			loc,
		)), nil
	}
}

func FORMAT_TIMESTAMP(format string, t time.Time, zone string) (Value, error) {
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	t = t.In(loc)
	s, err := formatTime(format, &t, FormatTypeTimestamp)
	if err != nil {
		return nil, err
	}
	return StringValue(s), nil
}

func PARSE_TIMESTAMP(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeTimestamp)
	if err != nil {
		return nil, err
	}
	return TimestampValue(*t), nil
}

func PARSE_TIMESTAMP_WITH_TIMEZONE(format, date, zone string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeTimestamp)
	if err != nil {
		return nil, err
	}
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	modified, err := modifyTimeZone(*t, loc)
	if err != nil {
		return nil, err
	}
	return TimestampValue(modified), nil
}

func TIMESTAMP_SECONDS(sec int64) (Value, error) {
	return TimestampValue(time.Unix(sec, 0)), nil
}

func TIMESTAMP_MILLIS(sec int64) (Value, error) {
	return TimestampValue(time.UnixMicro(sec * 1000)), nil
}

func TIMESTAMP_MICROS(sec int64) (Value, error) {
	return TimestampValue(time.UnixMicro(sec)), nil
}

func UNIX_SECONDS(t time.Time) (Value, error) {
	return IntValue(t.Unix()), nil
}

func UNIX_MILLIS(t time.Time) (Value, error) {
	return IntValue(t.UnixMilli()), nil
}

func UNIX_MICROS(t time.Time) (Value, error) {
	return IntValue(t.UnixMicro()), nil
}
