package zetasqlite_test

import (
	"os"
	"testing"
	"time"

	"github.com/goccy/go-zetasqlite"
)

func TestTimestamp(t *testing.T) {
	for _, test := range []struct {
		name      string
		timestamp string
		expected  string
	}{
		{name: "min does not round", timestamp: "-62135596800.0", expected: "0001-01-01T00:00:00.0Z"},
		{name: "max does not round", timestamp: "2534023007999.999999", expected: "9999-12-31T23:59:59.999999999Z"},
		{name: "microsecond places are handled", timestamp: "0.1", expected: "1970-01-01T00:00:00.100000000Z"},
	} {
		os.Setenv("TZ", "UTC")
		t.Run(test.name, func(t *testing.T) {
			ti, err := zetasqlite.TimeFromTimestampValue(test.timestamp)
			if err != nil {
				t.Fatalf("%s", err)
			}
			expected, err := time.Parse(time.RFC3339Nano, test.expected)
			if err != nil {
				t.Fatalf("%s", err)
			}
			if (ti.IsZero()) && (expected.IsZero()) {
				return
			}

			if ti.Equal(expected) {
				t.Fatalf("expected %s got %s", expected, ti)
			}
		})
	}
}
