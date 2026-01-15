package internal_test

import (
	"testing"
	"time"

	"github.com/goccy/go-zetasqlite/internal"
)

func Test_TIMESTAMP_TRUNC(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		input    time.Time
		part     string
		expected time.Time
		isErr    bool
	}{
		{
			input:    mustParseTime(t, "2024-06-28T08:24:31.123456Z"),
			part:     "MICROSECOND",
			expected: mustParseTime(t, "2024-06-28T08:24:31.123456Z"),
			isErr:    false,
		},
		{
			input:    mustParseTime(t, "2024-06-28T08:24:31.123456Z"),
			part:     "MILLISECOND",
			expected: mustParseTime(t, "2024-06-28T08:24:31.123Z"),
			isErr:    false,
		},
		{
			input:    mustParseTime(t, "2024-06-28T08:24:31.123456Z"),
			part:     "SECOND",
			expected: mustParseTime(t, "2024-06-28T08:24:31.0Z"),
			isErr:    false,
		},
		{
			input:    mustParseTime(t, "2024-06-28T08:24:31.123456Z"),
			part:     "MINUTE",
			expected: mustParseTime(t, "2024-06-28T08:24:00.0Z"),
			isErr:    false,
		},
		{
			input:    mustParseTime(t, "2024-06-28T08:24:31.123456Z"),
			part:     "HOUR",
			expected: mustParseTime(t, "2024-06-28T08:00:00.0Z"),
			isErr:    false,
		},
		{
			input:    mustParseTime(t, "2024-06-28T08:24:31.123456Z"),
			part:     "INVALID_PART",
			expected: mustParseTime(t, "2024-06-28T00:00:00.0Z"),
			isErr:    true,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.part, func(t *testing.T) {
			t.Parallel()

			res, err := internal.TIMESTAMP_TRUNC(tc.input, tc.part, "")

			if err != nil {
				if !tc.isErr {
					t.Fatal("unexpected err", err)
				}

				return
			}
			if tc.isErr {
				t.Fatal("expected err, got nil")
			}

			resTime, err := res.ToTime()
			if err != nil {
				t.Fatal(err)
			}
			if resTime != tc.expected {
				t.Fatalf("result %s not equal %v", resTime, tc.expected)
			}
		})
	}
}

func mustParseTime(t *testing.T, str string) time.Time {
	res, err := time.Parse(time.RFC3339Nano, str)
	if err != nil {
		t.Fatal(err)
	}

	return res
}
