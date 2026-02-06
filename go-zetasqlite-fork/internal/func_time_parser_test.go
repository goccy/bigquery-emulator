package internal

import "testing"

func TestTimeParser(t *testing.T) {
	for _, test := range []struct {
		name             string
		text             []rune
		minValue         int64
		maxValue         int64
		expectedProgress int
		expectedResult   int
		expectedErr      string
	}{
		{
			name:             "single digit but multiple allowed; non-digit character terminates",
			text:             []rune{'2', '/'},
			minValue:         1,
			maxValue:         12,
			expectedResult:   2,
			expectedProgress: 1,
		},
		{
			name:             "multiple digits; non-digit character terminates",
			text:             []rune{'1', '2', '/'},
			minValue:         1,
			maxValue:         12,
			expectedResult:   12,
			expectedProgress: 2,
		},
		{
			name:             "leading zero digit but multiple allowed; non-digit character terminates",
			text:             []rune{'0', '2', '/'},
			minValue:         1,
			maxValue:         12,
			expectedResult:   2,
			expectedProgress: 2,
		},

		{
			name:             "leading zero digit but multiple allowed; non-digit character terminates",
			text:             []rune{'0', '0', '2', '/'},
			minValue:         1,
			maxValue:         9999,
			expectedResult:   2,
			expectedProgress: 3,
		},
		{
			name:        "multiple digits but exceeds limit; non-digit character terminates",
			text:        []rune{'2', '2', '/'},
			minValue:    1,
			maxValue:    12,
			expectedErr: "part [22] is greater than maximum value [12]",
		},
		{
			name:        "multiple digits but lower than start bound; non-digit character terminates",
			text:        []rune{'0', '0', '/'},
			minValue:    1,
			maxValue:    12,
			expectedErr: "part [0] is less than minimum value [1]",
		},
		{
			name:             "multiple digits but lower than start bound; non-digit character terminates",
			text:             []rune{'4', '-'},
			minValue:         1,
			maxValue:         12,
			expectedResult:   4,
			expectedProgress: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			progress, result, err := parseDigitRespectingOptionalPlaces(test.text, test.minValue, test.maxValue)
			if err != nil {
				if test.expectedErr != err.Error() {
					t.Fatalf("unexpected error message: expected [%s] but got [%s]", test.expectedErr, err.Error())
				} else {
					// expected error occurred, consider test successful
					return
				}
			}

			if progress != test.expectedProgress {
				t.Fatalf("unexpected progress: expected [%d] but got [%d]", test.expectedProgress, progress)
			}

			if result != int64(test.expectedResult) {
				t.Fatalf("unexpected result: expected [%d] but got [%d]", test.expectedResult, result)
			}
		})
	}
}
