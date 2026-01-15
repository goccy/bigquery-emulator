package internal

import (
	"fmt"
	"strings"
	"time"
)

func CURRENT_DATE(zone string) (Value, error) {
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	return CURRENT_DATE_WITH_TIME(time.Now().In(loc))
}

func CURRENT_DATE_WITH_TIME(v time.Time) (Value, error) {
	return DateValue(v), nil
}

func DATE(args ...Value) (Value, error) {
	if len(args) == 3 {
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
		return DateValue(time.Time{}.AddDate(int(year)-1, int(month)-1, int(day)-1)), nil
	} else if len(args) == 2 {
		t, err := args[0].ToTime()
		if err != nil {
			return nil, err
		}
		zone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		loc, err := toLocation(zone)
		if err != nil {
			return nil, err
		}
		return DateValue(t.In(loc)), nil
	} else {
		t, err := args[0].ToTime()
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	}
}

func DATE_ADD(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "DAY":
		return DateValue(t.AddDate(0, 0, int(v))), nil
	case "WEEK":
		return DateValue(t.AddDate(0, 0, int(v*7))), nil
	case "MONTH":
		return DateValue(addMonth(t, int(v))), nil
	case "YEAR":
		return DateValue(addYear(t, int(v))), nil
	case "QUARTER":
		return DateValue(addMonth(t, 3)), nil
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

func DATE_SUB(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "DAY":
		return DateValue(t.AddDate(0, 0, int(-v))), nil
	case "WEEK":
		return DateValue(t.AddDate(0, 0, int(-v*7))), nil
	case "MONTH":
		return DateValue(addMonth(t, int(-v))), nil
	case "YEAR":
		return DateValue(addYear(t, int(-v))), nil
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

var WeekPartToOffset = map[string]int{
	"WEEK":           0,
	"WEEK_MONDAY":    1,
	"WEEK_TUESDAY":   2,
	"WEEK_WEDNESDAY": 3,
	"WEEK_THURSDAY":  4,
	"WEEK_FRIDAY":    5,
	"WEEK_SATURDAY":  6,
}

func DATE_DIFF(a, b time.Time, part string) (Value, error) {
	yearISOA, weekA := a.ISOWeek()
	yearISOB, weekB := b.ISOWeek()

	if strings.HasPrefix(part, "WEEK") {
		boundary, ok := WeekPartToOffset[part]

		if !ok {
			return nil, fmt.Errorf("unsupported week date part: %s", part)
		}

		isNegative := false
		start, end := b, a
		if b.Unix() > a.Unix() {
			start, end = a, b
			isNegative = true
		}

		// Manually calculate the number of days based off Unix seconds
		// time.Time.Sub returns "Infinite" max duration for the case of 9999-12-31.Sub(0001-01-01)
		// The maximum time.Duration is ~290 years due to being represented in int64 nanosecond resolution
		days := (end.Unix() - start.Unix()) / 24 / 60 / 60
		// Calculate number of complete weeks between start and end
		fullWeeks := days / 7
		remainder := days % 7

		counts := [7]int64{}

		for _, day := range WeekPartToOffset {
			counts[day] = fullWeeks
		}

		startingDay := int64(start.Weekday())

		for remainder > 0 {
			counts[(startingDay+remainder)%7]++
			remainder--
		}

		result := counts[boundary]

		if isNegative {
			result = -result
		}

		return IntValue(result), nil
	}

	diff := a.Sub(b)

	switch part {
	case "DAY":
		diffDay := diff / (24 * time.Hour)
		mod := diff % (24 * time.Hour)
		if mod > 0 {
			diffDay++
		} else if mod < 0 {
			diffDay--
		}
		return IntValue(diffDay), nil
	case "ISOWEEK":
		return IntValue((a.Year()-b.Year())*48 + weekA - weekB), nil
	case "MONTH":
		return IntValue((a.Year()*12 + int(a.Month())) - (b.Year()*12 + int(b.Month()))), nil
	case "YEAR":
		return IntValue(a.Year() - b.Year()), nil
	case "ISOYEAR":
		return IntValue(yearISOA - yearISOB), nil
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

var quarterStartMonths = []time.Month{time.January, time.April, time.July, time.October}

func DATE_TRUNC(t time.Time, part string) (Value, error) {
	yearISO, weekISO := t.ISOWeek()

	if strings.HasPrefix(part, "WEEK") {
		startOfWeek, ok := WeekPartToOffset[part]
		if !ok {
			return nil, fmt.Errorf("unknown week part: %s", part)
		}

		for int(t.Weekday()) != startOfWeek {
			t = t.AddDate(0, 0, -1)
		}

		return DateValue(t), nil
	}

	switch part {
	case "DAY":
		return DateValue(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())), nil
	case "ISOWEEK":
		return DateValue(time.Date(
			yearISO,
			0,
			7*weekISO,
			0,
			0,
			0,
			0,
			t.Location(),
		)), nil
	case "MONTH":
		return DateValue(time.Time{}.AddDate(t.Year()-1, int(t.Month())-1, 0)), nil
	case "QUARTER":
		return DateValue( // 1, 4, 7, 10
			time.Date(
				t.Year(),
				quarterStartMonths[int64((t.Month()-1)/3)],
				1,
				0,
				0,
				0,
				0,
				t.Location(),
			),
		), nil
	case "YEAR":
		return DateValue(time.Time{}.AddDate(t.Year()-1, 0, 0)), nil
	case "ISOYEAR":
		firstDay := time.Date(
			yearISO,
			1,
			1,
			0,
			0,
			0,
			0,
			t.Location(),
		)
		return DateValue(firstDay.AddDate(0, 0, 1-int(firstDay.Weekday()))), nil
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

func DATE_FROM_UNIX_DATE(unixdate int64) (Value, error) {
	t := time.Unix(int64(time.Duration(unixdate)*24*time.Hour/time.Second), 0)
	return DateValue(t), nil
}

func FORMAT_DATE(format string, t time.Time) (Value, error) {
	s, err := formatTime(format, &t, FormatTypeDate)
	if err != nil {
		return nil, err
	}
	return StringValue(s), nil
}

func LAST_DAY(t time.Time, part string) (Value, error) {
	switch part {
	case "YEAR":
		return DateValue(time.Date(t.Year()+1, time.Month(1), 0, 0, 0, 0, 0, t.Location())), nil
	case "QUARTER":
		return nil, fmt.Errorf("LAST_DAY: unimplemented QUARTER part")
	case "MONTH":
		return DateValue(t.AddDate(0, 1, -t.Day())), nil
	case "WEEK":
		return DateValue(t.AddDate(0, 0, 6-int(t.Weekday()))), nil
	case "WEEK_MONDAY":
		return DateValue(t.AddDate(0, 0, 7-int(t.Weekday()))), nil
	case "WEEK_TUESDAY":
		return DateValue(t.AddDate(0, 0, 8-int(t.Weekday()))), nil
	case "WEEK_WEDNESDAY":
		return DateValue(t.AddDate(0, 0, 9-int(t.Weekday()))), nil
	case "WEEK_THURSDAY":
		return DateValue(t.AddDate(0, 0, 10-int(t.Weekday()))), nil
	case "WEEK_FRIDAY":
		return DateValue(t.AddDate(0, 0, 11-int(t.Weekday()))), nil
	case "WEEK_SATURDAY":
		return DateValue(t.AddDate(0, 0, 12-int(t.Weekday()))), nil
	case "ISOWEEK":
		return DateValue(t.AddDate(0, 0, 6-int(t.Weekday()))), nil
	case "ISOYEAR":
		return DateValue(time.Date(t.Year()+1, time.Month(1), 0, 0, 0, 0, 0, t.Location())), nil
	}
	return nil, fmt.Errorf("LAST_DAY: unexpected part %s", part)
}

func PARSE_DATE(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeDate)
	if err != nil {
		return nil, err
	}
	return DateValue(*t), nil
}

func UNIX_DATE(t time.Time) (Value, error) {
	return IntValue(t.Unix() / int64(24*time.Hour/time.Second)), nil
}

func addMonth(t time.Time, m int) time.Time {
	curYear, curMonth, curDay := t.Date()

	first := time.Date(curYear, curMonth, 1, 0, 0, 0, 0, t.Location())
	year, month, _ := first.AddDate(0, m, 0).Date()
	after := time.Date(year, month, curDay, 0, 0, 0, 0, time.UTC)
	if month != after.Month() {
		return first.AddDate(0, m+1, -1)
	}
	return t.AddDate(0, m, 0)
}

func addYear(t time.Time, y int) time.Time {
	curYear, curMonth, curDay := t.Date()

	first := time.Date(curYear, curMonth, 1, 0, 0, 0, 0, t.Location())
	year, month, _ := first.AddDate(y, 0, 0).Date()
	after := time.Date(year, month, curDay, 0, 0, 0, 0, t.Location())
	if month != after.Month() {
		return first.AddDate(y, 1, -1)
	}
	return t.AddDate(y, 0, 0)
}
