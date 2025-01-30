package contentdata

import "testing"

func TestBigNumericFromBytes(t *testing.T) {
	tests := []struct {
		input    []uint8
		expected string
	}{
		{[]uint8{0, 0, 0, 0, 0, 54, 106, 188, 74, 131, 144, 97, 94, 142, 92, 32, 218, 254}, "-1000"},
		{[]uint8{0, 0, 0, 0, 0, 202, 149, 67, 181, 124, 111, 158, 161, 113, 163, 223, 37, 1}, "1000"},
		{[]uint8{0, 0, 0, 192, 5, 115, 196, 212, 216, 175, 10, 207, 166, 15, 226, 107, 175, 83, 170, 215, 0}, "12312314324.23423423"},
		{[]uint8{0, 0, 0, 0, 124, 113, 12, 44, 225, 164, 189, 0, 214, 22, 87, 236, 0}, "3.1415"},
		{[]uint8{0, 0, 0, 0, 224, 88, 126, 10, 83, 11, 97, 48, 185, 58, 193, 82}, "1.1"},
		{[]uint8{0, 0, 0, 0, 64, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75}, "1"},
		{[]uint8{0x00}, "0"},
	}

	for _, test := range tests {
		result, err := BigNumericFromBytes(test.input)
		if err != nil {
			t.Errorf("BigNumericFromBytes(%v) returned error: %v", test.input, err)
		}
		if result != test.expected {
			t.Errorf("BigNumericFromBytes(%v) = %v; want %v", test.input, result, test.expected)
		}
	}
}
