package zetasqlite

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TimeFromTimestampValue zetasqlite returns string values ​​by default for timestamp values.
// This function is a helper function to convert that value to time.Time type.
func TimeFromTimestampValue(v string) (time.Time, error) {
	// ParseFloat is too imprecise to use, instead split into seconds and fractional seconds
	parts := strings.Split(v, ".")
	if len(parts) > 2 {
		return time.Time{}, fmt.Errorf("invalid timestamp string (multiple delimiters) %s", v)
	}
	seconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	micros := int64(0)
	if len(parts) == 2 {
		// Pad fractional places to microseconds i.e. (.1 to 100000 micros)
		microsString := parts[1]
		for len(microsString) < 6 {
			microsString += "0"
		}
		m, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		micros = m
	}
	nanos := micros * int64(time.Microsecond)
	return time.Unix(seconds, nanos), err
}
