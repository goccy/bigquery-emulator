package internal

import (
	"math/bits"
)

func BIT_COUNT(v Value) (Value, error) {
	switch v.(type) {
	case BytesValue:
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		var sum int64
		for _, vv := range b {
			sum += int64(bits.OnesCount8(vv))
		}
		return IntValue(sum), nil
	default:
		vv, err := v.ToInt64()
		if err != nil {
			return nil, err
		}
		return IntValue(bits.OnesCount64(uint64(vv))), nil
	}
}
