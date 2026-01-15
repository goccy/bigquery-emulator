package internal

import (
	"testing"
	"time"
)

func formatTimestamp(s string) (string, error) {
	loc, err := time.LoadLocation("")
	if err != nil {
		return "", err
	}
	t, err := parseTimestamp(s, loc)
	if err != nil {
		return "", err
	}
	return t.Format(time.RFC3339Nano), nil
}

func TestTimestampValue(t *testing.T) {
	if !datetimeRe.MatchString("2022-01-01 00:00:00") {
		t.Fatalf("mismatch timestamp value")
	}
	formatted, err := formatTimestamp("2022-01-01 00:00:00")
	if err != nil {
		t.Fatal(err)
	}
	if formatted != "2022-01-01T00:00:00Z" {
		t.Fatalf("failed to format timestamp")
	}
}
