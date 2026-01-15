package internal

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"gonum.org/v1/gonum/floats/scalar"
)

func ABS(a Value) (Value, error) {
	f64, err := a.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Abs(f64)), nil
}

func SIGN(a Value) (Value, error) {
	f64, err := a.ToFloat64()
	if err != nil {
		return nil, err
	}
	if math.Signbit(f64) {
		return IntValue(-1), nil
	} else if f64 == 0 {
		return IntValue(0), nil
	}
	return IntValue(1), nil
}

func IS_INF(a Value) (Value, error) {
	f64, err := a.ToFloat64()
	if err != nil {
		return nil, err
	}
	return BoolValue(math.IsInf(f64, 0) || math.IsInf(f64, -1)), nil
}

func IS_NAN(a Value) (Value, error) {
	f64, err := a.ToFloat64()
	if err != nil {
		return nil, err
	}
	return BoolValue(math.IsNaN(f64)), nil
}

func IEEE_DIVIDE(x, y Value) (Value, error) {
	x64, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	y64, err := y.ToFloat64()
	if err != nil {
		return nil, err
	}
	if x64 == 0 {
		if y64 == 0 {
			return FloatValue(math.NaN()), nil
		}
		if math.IsNaN(y64) {
			return FloatValue(math.NaN()), nil
		}
		return FloatValue(0), nil
	}
	if math.IsNaN(x64) {
		if y64 == 0 {
			return FloatValue(math.NaN()), nil
		}
	} else if math.IsInf(x64, 0) || math.IsInf(x64, -1) {
		if math.IsInf(y64, 0) || math.IsInf(y64, -1) {
			return FloatValue(math.NaN()), nil
		}
	} else if y64 == 0 {
		if x64 > 0 {
			return FloatValue(math.Inf(1)), nil
		} else if x64 < 0 {
			return FloatValue(math.Inf(-1)), nil
		}
	}
	return FloatValue(x64 / y64), nil
}

//nolint:gosec
func RAND() (Value, error) {
	rand.Seed(time.Now().UnixNano())
	return FloatValue(rand.Float64()), nil
}

func SQRT(x Value) (Value, error) {
	f, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Sqrt(f)), nil
}

func POW(x, y Value) (Value, error) {
	xf, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	yf, err := y.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Pow(xf, yf)), nil
}

func EXP(x Value) (Value, error) {
	f, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Exp(f)), nil
}

func LN(x Value) (Value, error) {
	f, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Log(f)), nil
}

func LOG(x, y Value) (Value, error) {
	xf, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	yi, err := x.ToInt64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Ldexp(xf, int(yi))), nil
}

func LOG10(x Value) (Value, error) {
	f, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Log10(f)), nil
}

func GREATEST(args ...Value) (Value, error) {
	var max Value
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
		if max == nil {
			max = arg
			continue
		}
		gt, err := arg.GT(max)
		if err != nil {
			return nil, err
		}
		if gt {
			max = arg
		}
	}
	return max, nil
}

func LEAST(args ...Value) (Value, error) {
	var min Value
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
		if min == nil {
			min = arg
			continue
		}
		less, err := arg.LT(min)
		if err != nil {
			return nil, err
		}
		if less {
			min = arg
		}
	}
	return min, nil
}

func DIV(x, y Value) (Value, error) {
	xv, err := x.ToInt64()
	if err != nil {
		return nil, err
	}
	yv, err := y.ToInt64()
	if err != nil {
		return nil, err
	}
	if yv == 0 {
		return nil, fmt.Errorf("DIV: zero divided")
	}
	return IntValue(xv / yv), nil
}

func SAFE_DIVIDE(x, y Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	yv, err := y.ToFloat64()
	if err != nil {
		return nil, err
	}
	if yv == 0 {
		return nil, nil
	}
	return FloatValue(xv / yv), nil
}

func SAFE_MULTIPLY(x, y Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	yv, err := y.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(xv * yv), nil
}

func SAFE_NEGATE(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(-xv), nil
}

func SAFE_ADD(x, y Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	yv, err := y.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(xv + yv), nil
}

func SAFE_SUBTRACT(x, y Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	yv, err := y.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(xv - yv), nil
}

func MOD(x, y Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	yv, err := y.ToFloat64()
	if err != nil {
		return nil, err
	}
	if yv == 0 {
		return nil, fmt.Errorf("MOD: zero divided")
	}
	return FloatValue(math.Mod(xv, yv)), nil
}

func ROUND(x Value, precision int) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(scalar.Round(xv, precision)), nil
}

func TRUNC(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Trunc(xv)), nil
}
func CEIL(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Ceil(xv)), nil
}

func FLOOR(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Floor(xv)), nil
}

func COS(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Cos(xv)), nil
}

func COSH(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Cosh(xv)), nil
}

func ACOS(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Acos(xv)), nil
}

func ACOSH(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Acosh(xv)), nil
}

func SIN(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Sin(xv)), nil
}

func SINH(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Sinh(xv)), nil
}

func ASIN(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Asin(xv)), nil
}

func ASINH(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Asinh(xv)), nil
}

func TAN(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Tan(xv)), nil
}

func TANH(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Tanh(xv)), nil
}

func ATAN(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Atan(xv)), nil
}

func ATANH(x Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Atanh(xv)), nil
}

func ATAN2(x, y Value) (Value, error) {
	xv, err := x.ToFloat64()
	if err != nil {
		return nil, err
	}
	yv, err := y.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Atan2(xv, yv)), nil
}

func RANGE_BUCKET(point Value, array *ArrayValue) (Value, error) {
	if point == nil {
		return nil, nil
	}
	var idx int
	for _, v := range array.values {
		if v == nil {
			return nil, fmt.Errorf("RANGE_BUCKET: NULL value found in array")
		}
		cond, err := point.GTE(v)
		if err != nil {
			return nil, err
		}
		if !cond {
			break
		}
		idx++
	}
	return IntValue(idx), nil
}
