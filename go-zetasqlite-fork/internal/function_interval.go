package internal

import (
	"fmt"

	"cloud.google.com/go/bigquery"
)

func INTERVAL(value int64, part string) (Value, error) {
	switch part {
	case "YEAR":
		return &IntervalValue{IntervalValue: &bigquery.IntervalValue{Years: int32(value)}}, nil
	case "MONTH":
		return &IntervalValue{IntervalValue: &bigquery.IntervalValue{Months: int32(value)}}, nil
	case "DAY":
		return &IntervalValue{IntervalValue: &bigquery.IntervalValue{Days: int32(value)}}, nil
	case "HOUR":
		return &IntervalValue{IntervalValue: &bigquery.IntervalValue{Hours: int32(value)}}, nil
	case "MINUTE":
		return &IntervalValue{IntervalValue: &bigquery.IntervalValue{Minutes: int32(value)}}, nil
	case "SECOND":
		return &IntervalValue{IntervalValue: &bigquery.IntervalValue{Seconds: int32(value)}}, nil
	case "NANOSECOND":
		return &IntervalValue{IntervalValue: &bigquery.IntervalValue{SubSecondNanos: int32(value)}}, nil
	}
	return nil, fmt.Errorf("unexpected interval part: %s", part)
}

func MAKE_INTERVAL(year, month, day, hour, minute, second int64) (Value, error) {
	return &IntervalValue{
		IntervalValue: &bigquery.IntervalValue{
			Years:   int32(year),
			Months:  int32(month),
			Days:    int32(day),
			Hours:   int32(hour),
			Minutes: int32(minute),
			Seconds: int32(second),
		},
	}, nil
}

func JUSTIFY_DAYS(v *IntervalValue) (Value, error) {
	if v.Days > 29 {
		v.Months += v.Days / 30
		v.Days %= 30
	} else if v.Days < -29 {
		v.Months += v.Days / 30
		v.Days %= 30
	}
	if v.Months > 11 {
		v.Months -= 12
		v.Years++
	} else if v.Months < -11 {
		v.Months += 12
		v.Years--
	}
	return v, nil
}

func JUSTIFY_HOURS(v *IntervalValue) (Value, error) {
	if v.Seconds > 59 {
		v.Minutes += v.Seconds / 60
		v.Seconds %= 60
	} else if v.Seconds < -59 {
		v.Minutes += v.Seconds / 60
		v.Seconds %= 60
	}
	if v.Minutes > 59 {
		v.Hours += v.Minutes / 60
		v.Minutes %= 60
	} else if v.Minutes < -59 {
		v.Hours += v.Hours / 60
		v.Minutes %= 60
	}
	if v.Hours > 23 {
		v.Days += v.Hours / 24
		v.Hours %= 24
	} else if v.Hours < -23 {
		v.Days += v.Hours / 24
		v.Hours %= 24
	}
	return v, nil
}

func JUSTIFY_INTERVAL(v *IntervalValue) (Value, error) {
	if _, err := JUSTIFY_HOURS(v); err != nil {
		return nil, err
	}
	return JUSTIFY_DAYS(v)
}
