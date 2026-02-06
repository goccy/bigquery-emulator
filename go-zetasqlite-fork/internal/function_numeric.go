package internal

import (
	"fmt"
	"math/big"
)

func PARSE_NUMERIC(numeric string) (Value, error) {
	r := new(big.Rat)
	if _, ok := r.SetString(numeric); !ok {
		return nil, fmt.Errorf("unexpected numeric literal: %s", numeric)
	}
	return &NumericValue{Rat: r}, nil
}

func PARSE_BIGNUMERIC(numeric string) (Value, error) {
	r := new(big.Rat)
	if _, ok := r.SetString(numeric); !ok {
		return nil, fmt.Errorf("unexpected numeric literal: %s", numeric)
	}
	return &NumericValue{Rat: r, isBigNumeric: true}, nil
}
