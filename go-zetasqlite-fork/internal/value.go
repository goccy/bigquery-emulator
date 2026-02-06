package internal

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-json"
)

type Value interface {
	Add(Value) (Value, error)
	Sub(Value) (Value, error)
	Mul(Value) (Value, error)
	Div(Value) (Value, error)
	EQ(Value) (bool, error)
	GT(Value) (bool, error)
	GTE(Value) (bool, error)
	LT(Value) (bool, error)
	LTE(Value) (bool, error)
	ToInt64() (int64, error)
	ToString() (string, error)
	ToBytes() ([]byte, error)
	ToFloat64() (float64, error)
	ToBool() (bool, error)
	ToArray() (*ArrayValue, error)
	ToStruct() (*StructValue, error)
	ToJSON() (string, error)
	ToTime() (time.Time, error)
	ToRat() (*big.Rat, error)
	Format(verb rune) string
	Interface() interface{}
}

type IntValue int64

func (iv IntValue) Add(v Value) (Value, error) {
	v2, err := v.ToInt64()
	if err != nil {
		return nil, err
	}
	return IntValue(int64(iv) + v2), nil
}

func (iv IntValue) Sub(v Value) (Value, error) {
	v2, err := v.ToInt64()
	if err != nil {
		return nil, err
	}
	return IntValue(int64(iv) - v2), nil
}

func (iv IntValue) Mul(v Value) (Value, error) {
	v2, err := v.ToInt64()
	if err != nil {
		return nil, err
	}
	return IntValue(int64(iv) * v2), nil
}

func (iv IntValue) Div(v Value) (Value, error) {
	v2, err := v.ToInt64()
	if err != nil {
		return nil, err
	}
	if v2 == 0 {
		return nil, fmt.Errorf("zero divided error ( %d / 0 )", iv)
	}
	return IntValue(int64(iv) / v2), nil
}

func (iv IntValue) EQ(v Value) (bool, error) {
	v2, err := v.ToInt64()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to int64", v)
	}
	return int64(iv) == v2, nil
}

func (iv IntValue) GT(v Value) (bool, error) {
	v2, err := v.ToInt64()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to int64", v)
	}
	return int64(iv) > v2, nil
}

func (iv IntValue) GTE(v Value) (bool, error) {
	v2, err := v.ToInt64()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to int64", v)
	}
	return int64(iv) >= v2, nil
}

func (iv IntValue) LT(v Value) (bool, error) {
	v2, err := v.ToInt64()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to int64", v)
	}
	return int64(iv) < v2, nil
}

func (iv IntValue) LTE(v Value) (bool, error) {
	v2, err := v.ToInt64()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to int64", v)
	}
	return int64(iv) <= v2, nil
}

func (iv IntValue) ToInt64() (int64, error) {
	return int64(iv), nil
}

func (iv IntValue) ToString() (string, error) {
	return fmt.Sprint(iv), nil
}

func (iv IntValue) ToBytes() ([]byte, error) {
	return []byte(fmt.Sprint(iv)), nil
}

func (iv IntValue) ToFloat64() (float64, error) {
	return float64(iv), nil
}

func (iv IntValue) ToBool() (bool, error) {
	switch iv {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("failed to convert %d to bool type", iv)
	}
}

func (iv IntValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert %d to array type", iv)
}

func (iv IntValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert %d to struct type", iv)
}

func (iv IntValue) ToJSON() (string, error) {
	return fmt.Sprint(iv), nil
}

func (iv IntValue) ToTime() (time.Time, error) {
	v := int64(iv)
	if v > time.Unix(0, 0).Unix()*int64(time.Millisecond) {
		return TimestampFromInt64Value(v)
	}
	return DateFromInt64Value(v)
}

func (iv IntValue) ToRat() (*big.Rat, error) {
	r := new(big.Rat)
	r.SetInt64(int64(iv))
	return r, nil
}

func (iv IntValue) Format(verb rune) string {
	return fmt.Sprint(iv)
}

func (iv IntValue) Interface() interface{} {
	return int64(iv)
}

type StringValue string

func (sv StringValue) Add(v Value) (Value, error) {
	v2, err := v.ToString()
	if err != nil {
		return nil, err
	}
	return StringValue(string(sv) + v2), nil
}

func (sv StringValue) Sub(v Value) (Value, error) {
	return nil, fmt.Errorf("sub operation is unsupported for string %v", sv)
}

func (sv StringValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("mul operation is unsupported for string %v", sv)
}

func (sv StringValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("div operation is unsupported for string %v", sv)
}

func (sv StringValue) EQ(v Value) (bool, error) {
	v2, err := v.ToString()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to string", v)
	}
	return string(sv) == v2, nil
}

func (sv StringValue) GT(v Value) (bool, error) {
	v2, err := v.ToString()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to string", v)
	}
	return string(sv) > v2, nil
}

func (sv StringValue) GTE(v Value) (bool, error) {
	v2, err := v.ToString()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to string", v)
	}
	return string(sv) >= v2, nil
}

func (sv StringValue) LT(v Value) (bool, error) {
	v2, err := v.ToString()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to string", v)
	}
	return string(sv) < v2, nil
}

func (sv StringValue) LTE(v Value) (bool, error) {
	v2, err := v.ToString()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to string", v)
	}
	return string(sv) <= v2, nil
}

func (sv StringValue) ToInt64() (int64, error) {
	if sv == "" {
		return 0, nil
	}
	toParse := string(sv)
	base := 10
	if strings.Contains(strings.ToLower(toParse), "0x") {
		base = 0
	}
	return strconv.ParseInt(toParse, base, 64)
}

func (sv StringValue) ToString() (string, error) {
	return string(sv), nil
}

func (sv StringValue) ToBytes() ([]byte, error) {
	return []byte(string(sv)), nil
}

func (sv StringValue) ToFloat64() (float64, error) {
	if sv == "" {
		return 0, nil
	}
	return strconv.ParseFloat(string(sv), 64)
}

func (sv StringValue) ToBool() (bool, error) {
	if sv == "" {
		return false, nil
	}
	return strconv.ParseBool(string(sv))
}

func (sv StringValue) ToArray() (*ArrayValue, error) {
	if sv == "" {
		return nil, nil
	}
	return nil, fmt.Errorf("failed to convert array from string: %v", sv)
}

func (sv StringValue) ToStruct() (*StructValue, error) {
	if sv == "" {
		return nil, nil
	}
	return nil, fmt.Errorf("failed to convert struct from string: %v", sv)
}

func (sv StringValue) ToJSON() (string, error) {
	return strconv.Quote(string(sv)), nil
}

func (sv StringValue) ToTime() (time.Time, error) {
	raw := string(sv)
	switch {
	case isDate(raw):
		return parseDate(raw)
	case isDatetime(raw):
		return parseDatetime(raw)
	case isTime(raw):
		return parseTime(raw)
	case isTimestamp(raw):
		return parseTimestamp(raw, time.UTC)
	}
	return time.Time{}, fmt.Errorf("failed to convert %s to time.Time type", sv)
}

func (sv StringValue) ToRat() (*big.Rat, error) {
	r := new(big.Rat)
	r.SetString(string(sv))
	return r, nil
}

func (sv StringValue) Format(verb rune) string {
	switch verb {
	case 't':
		return string(sv)
	case 'T':
		return strconv.Quote(string(sv))
	}
	return string(sv)
}

func (sv StringValue) Interface() interface{} {
	return string(sv)
}

type BytesValue []byte

func (bv BytesValue) Add(v Value) (Value, error) {
	v2, err := v.ToBytes()
	if err != nil {
		return nil, err
	}
	return BytesValue(append([]byte(bv), v2...)), nil
}

func (bv BytesValue) Sub(v Value) (Value, error) {
	return nil, fmt.Errorf("sub operation is unsupported for bytes %v", bv)
}

func (bv BytesValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("mul operation is unsupported for bytes %v", bv)
}

func (bv BytesValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("div operation is unsupported for bytes %v", bv)
}

func (bv BytesValue) EQ(v Value) (bool, error) {
	v2, err := v.ToBytes()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to bytes", v)
	}
	return bytes.Equal([]byte(bv), v2), nil
}

func (bv BytesValue) GT(v Value) (bool, error) {
	v2, err := v.ToBytes()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to bytes", v)
	}
	return string(bv) > string(v2), nil
}

func (bv BytesValue) GTE(v Value) (bool, error) {
	v2, err := v.ToBytes()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to bytes", v)
	}
	return string(bv) >= string(v2), nil
}

func (bv BytesValue) LT(v Value) (bool, error) {
	v2, err := v.ToBytes()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to bytes", v)
	}
	return string(bv) < string(v2), nil
}

func (bv BytesValue) LTE(v Value) (bool, error) {
	v2, err := v.ToBytes()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to bytes", v)
	}
	return string(bv) <= string(v2), nil
}

func (bv BytesValue) ToInt64() (int64, error) {
	if len(bv) == 0 {
		return 0, nil
	}
	return strconv.ParseInt(string(bv), 10, 64)
}

func (bv BytesValue) ToString() (string, error) {
	return base64.StdEncoding.EncodeToString([]byte(bv)), nil
}

func (bv BytesValue) ToBytes() ([]byte, error) {
	return []byte(bv), nil
}

func (bv BytesValue) ToFloat64() (float64, error) {
	if len(bv) == 0 {
		return 0, nil
	}
	return strconv.ParseFloat(string(bv), 64)
}

func (bv BytesValue) ToBool() (bool, error) {
	if len(bv) == 0 {
		return false, nil
	}
	return strconv.ParseBool(string(bv))
}

func (bv BytesValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert array from bytes: %v", bv)
}

func (bv BytesValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert struct from bytes: %v", bv)
}

func (bv BytesValue) ToJSON() (string, error) {
	v, err := bv.ToString()
	if err != nil {
		return "", err
	}
	return strconv.Quote(v), nil
}

func (bv BytesValue) ToTime() (time.Time, error) {
	raw := string(bv)
	switch {
	case isDate(raw):
		return parseDate(raw)
	case isDatetime(raw):
		return parseDatetime(raw)
	case isTime(raw):
		return parseTime(raw)
	case isTimestamp(raw):
		return parseTimestamp(raw, time.UTC)
	}
	return time.Time{}, fmt.Errorf("failed to convert time.Time from bytes: %s", bv)
}

func (bv BytesValue) ToRat() (*big.Rat, error) {
	r := new(big.Rat)
	r.SetString(string(bv))
	return r, nil
}

func printableChar(v byte) bool {
	if 0x20 <= v && v <= 0x7e {
		return true
	}
	return false
}

func (bv BytesValue) Format(verb rune) string {
	switch verb {
	case 't':
		var ret string
		for _, b := range bv {
			if printableChar(b) {
				ret += fmt.Sprintf("%c", b)
			} else {
				ret += fmt.Sprintf("\\x%02x", b)
			}
		}
		return ret
	case 'T':
		ret := `b"`
		for _, b := range bv {
			if printableChar(b) {
				ret += fmt.Sprintf("%c", b)
			} else {
				ret += fmt.Sprintf("\\x%02x", b)
			}
		}
		ret += `"`
		return ret
	}
	v, _ := bv.ToString()
	return v
}

func (bv BytesValue) Interface() interface{} {
	return []byte(bv)
}

type FloatValue float64

func (fv FloatValue) Add(v Value) (Value, error) {
	v2, err := v.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(float64(fv) + v2), nil
}

func (fv FloatValue) Sub(v Value) (Value, error) {
	v2, err := v.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(float64(fv) - v2), nil
}

func (fv FloatValue) Mul(v Value) (Value, error) {
	v2, err := v.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(float64(fv) * v2), nil
}

func (fv FloatValue) Div(v Value) (Value, error) {
	v2, err := v.ToFloat64()
	if err != nil {
		return nil, err
	}
	if v2 == 0 {
		return nil, fmt.Errorf("zero divided error ( %f / 0 )", fv)
	}
	return FloatValue(float64(fv) / v2), nil
}

func (fv FloatValue) EQ(v Value) (bool, error) {
	v2, err := v.ToFloat64()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to float64", v)
	}
	return float64(fv) == v2, nil
}

func (fv FloatValue) GT(v Value) (bool, error) {
	v2, err := v.ToFloat64()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to float64", v)
	}
	return float64(fv) > v2, nil
}

func (fv FloatValue) GTE(v Value) (bool, error) {
	v2, err := v.ToFloat64()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to float64", v)
	}
	return float64(fv) >= v2, nil
}

func (fv FloatValue) LT(v Value) (bool, error) {
	v2, err := v.ToFloat64()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to float64", v)
	}
	return float64(fv) < v2, nil
}

func (fv FloatValue) LTE(v Value) (bool, error) {
	v2, err := v.ToFloat64()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to float64", v)
	}
	return float64(fv) <= v2, nil
}

func (fv FloatValue) ToInt64() (int64, error) {
	return int64(fv), nil
}

func (fv FloatValue) ToString() (string, error) {
	return fmt.Sprint(fv), nil
}

func (fv FloatValue) ToBytes() ([]byte, error) {
	return []byte(fmt.Sprint(fv)), nil
}

func (fv FloatValue) ToFloat64() (float64, error) {
	return float64(fv), nil
}

func (fv FloatValue) ToBool() (bool, error) {
	switch fmt.Sprint(fv) {
	case "1":
		return true, nil
	case "0":
		return false, nil
	}
	return false, fmt.Errorf("failed to convert %f to bool type", fv)
}

func (fv FloatValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert array from float64: %v", fv)
}

func (fv FloatValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert struct from float64: %v", fv)
}

func (fv FloatValue) ToJSON() (string, error) {
	return fmt.Sprint(fv), nil
}

func (fv FloatValue) ToTime() (time.Time, error) {
	return TimestampFromFloatValue(float64(fv))
}

func (fv FloatValue) ToRat() (*big.Rat, error) {
	r := new(big.Rat)
	r.SetFloat64(float64(fv))
	return r, nil
}

func (fv FloatValue) Format(verb rune) string {
	return fmt.Sprint(fv)
}

func (fv FloatValue) Interface() interface{} {
	return float64(fv)
}

type NumericValue struct {
	*big.Rat
	isBigNumeric bool
}

func (nv *NumericValue) Add(v Value) (Value, error) {
	z := new(big.Rat)
	x := nv.Rat
	y, err := v.ToRat()
	if err != nil {
		return nil, err
	}
	nv.Rat = z.Add(x, y)
	return nv, nil
}

func (nv *NumericValue) Sub(v Value) (Value, error) {
	z := new(big.Rat)
	x := nv.Rat
	y, err := v.ToRat()
	if err != nil {
		return nil, err
	}
	zy := new(big.Rat)
	nv.Rat = z.Add(x, zy.Neg(y))
	return nv, nil
}

func (nv *NumericValue) Mul(v Value) (Value, error) {
	z := new(big.Rat)
	x := nv.Rat
	y, err := v.ToRat()
	if err != nil {
		return nil, err
	}
	nv.Rat = z.Mul(x, y)
	return nv, nil
}

func (nv *NumericValue) Div(v Value) (ret Value, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = err.(error)
		}
	}()
	z := new(big.Rat)
	x := nv.Rat
	y, err := v.ToRat()
	if err != nil {
		return nil, err
	}
	zy := new(big.Rat)
	nv.Rat = z.Mul(x, zy.Inv(y))
	return nv, nil
}

func (nv *NumericValue) EQ(v Value) (bool, error) {
	x := nv.Rat
	y, err := v.ToRat()
	if err != nil {
		return false, err
	}
	return x.Cmp(y) == 0, nil
}

func (nv *NumericValue) GT(v Value) (bool, error) {
	x := nv.Rat
	y, err := v.ToRat()
	if err != nil {
		return false, err
	}
	return x.Cmp(y) > 0, nil
}

func (nv *NumericValue) GTE(v Value) (bool, error) {
	x := nv.Rat
	y, err := v.ToRat()
	if err != nil {
		return false, err
	}
	return x.Cmp(y) >= 0, nil
}

func (nv *NumericValue) LT(v Value) (bool, error) {
	x := nv.Rat
	y, err := v.ToRat()
	if err != nil {
		return false, err
	}
	return x.Cmp(y) < 0, nil
}

func (nv *NumericValue) LTE(v Value) (bool, error) {
	x := nv.Rat
	y, err := v.ToRat()
	if err != nil {
		return false, err
	}
	return x.Cmp(y) <= 0, nil
}

func (nv *NumericValue) ToInt64() (int64, error) {
	return nv.Rat.Num().Int64(), nil
}

func (nv *NumericValue) toString() string {
	var v string
	if nv.isBigNumeric {
		v = nv.Rat.FloatString(38)
	} else {
		v = nv.Rat.FloatString(9)
	}
	v = strings.TrimRight(v, "0")
	v = strings.TrimRight(v, ".")
	return v
}

func (nv *NumericValue) ToString() (string, error) {
	return nv.toString(), nil
}

func (nv *NumericValue) ToBytes() ([]byte, error) {
	return []byte(nv.toString()), nil
}

func (nv *NumericValue) ToFloat64() (float64, error) {
	f, _ := nv.Rat.Float64()
	return f, nil
}

func (nv *NumericValue) ToBool() (bool, error) {
	v := nv.Rat.Num().Int64()
	if v == 1 {
		return true, nil
	} else if v == 0 {
		return false, nil
	}
	return false, fmt.Errorf("failed to convert numeric value to bool type")
}

func (nv *NumericValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert array from numeric value")
}

func (nv *NumericValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert struct from numeric value")
}

func (nv *NumericValue) ToJSON() (string, error) {
	return nv.toString(), nil
}

func (nv *NumericValue) ToTime() (time.Time, error) {
	return time.Time{}, fmt.Errorf("failed to convert time.Time from numeric value")
}

func (nv *NumericValue) ToRat() (*big.Rat, error) {
	return nv.Rat, nil
}

func (nv *NumericValue) Format(verb rune) string {
	return nv.toString()
}

func (nv *NumericValue) Interface() interface{} {
	return nv.Rat.String()
}

type BoolValue bool

func (bv BoolValue) Add(v Value) (Value, error) {
	return nil, fmt.Errorf("add operation is unsupported for bool %v", bv)
}

func (bv BoolValue) Sub(v Value) (Value, error) {
	return nil, fmt.Errorf("sub operation is unsupported for bool %v", bv)
}

func (bv BoolValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("mul operation is unsupported for bool %v", bv)
}

func (bv BoolValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("div operation is unsupported for bool %v", bv)
}

func (bv BoolValue) EQ(v Value) (bool, error) {
	v2, err := v.ToBool()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to bool", v)
	}
	return bool(bv) == v2, nil
}

func (bv BoolValue) GT(v Value) (bool, error) {
	return false, fmt.Errorf("gt operation is unsupported for bool %v", bv)
}

func (bv BoolValue) GTE(v Value) (bool, error) {
	return false, fmt.Errorf("gte operation is unsupported for bool %v", bv)
}

func (bv BoolValue) LT(v Value) (bool, error) {
	return false, fmt.Errorf("lt operation is unsupported for bool %v", bv)
}

func (bv BoolValue) LTE(v Value) (bool, error) {
	return false, fmt.Errorf("lte operation is unsupported for bool %v", bv)
}

func (bv BoolValue) ToInt64() (int64, error) {
	if bv {
		return 1, nil
	}
	return 0, nil
}

func (bv BoolValue) ToString() (string, error) {
	return fmt.Sprint(bv), nil
}

func (bv BoolValue) ToBytes() ([]byte, error) {
	return []byte(fmt.Sprint(bv)), nil
}

func (bv BoolValue) ToFloat64() (float64, error) {
	if bv {
		return 1, nil
	}
	return 0, nil
}

func (bv BoolValue) ToBool() (bool, error) {
	return bool(bv), nil
}

func (bv BoolValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert bool from array: %v", bv)
}

func (bv BoolValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert bool from struct: %v", bv)
}

func (bv BoolValue) ToJSON() (string, error) {
	return fmt.Sprint(bv), nil
}

func (bv BoolValue) ToTime() (time.Time, error) {
	return time.Time{}, fmt.Errorf("failed to convert bool from time.Time: %v", bv)
}

func (bv BoolValue) ToRat() (*big.Rat, error) {
	r := new(big.Rat)
	if bv {
		r.SetInt64(1)
		return r, nil
	}
	r.SetInt64(0)
	return r, nil
}

func (bv BoolValue) Format(verb rune) string {
	return fmt.Sprint(bv)
}

func (bv BoolValue) Interface() interface{} {
	return bool(bv)
}

type JsonValue string

func (jv JsonValue) Add(v Value) (Value, error) {
	return nil, fmt.Errorf("add operation is unsupported for json %v", jv)
}

func (jv JsonValue) Sub(v Value) (Value, error) {
	return nil, fmt.Errorf("sub operation is unsupported for json %v", jv)
}

func (jv JsonValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("mul operation is unsupported for json %v", jv)
}

func (jv JsonValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("div operation is unsupported for json %v", jv)
}

func (jv JsonValue) EQ(v Value) (bool, error) {
	return false, fmt.Errorf("eq operation is unsupported for json %v", jv)
}

func (jv JsonValue) GT(v Value) (bool, error) {
	return false, fmt.Errorf("gt operation is unsupported for json %v", jv)
}

func (jv JsonValue) GTE(v Value) (bool, error) {
	return false, fmt.Errorf("gte operation is unsupported for json %v", jv)
}

func (jv JsonValue) LT(v Value) (bool, error) {
	return false, fmt.Errorf("lt operation is unsupported for json %v", jv)
}

func (jv JsonValue) LTE(v Value) (bool, error) {
	return false, fmt.Errorf("lte operation is unsupported for json %v", jv)
}

func (jv JsonValue) ToInt64() (int64, error) {
	return strconv.ParseInt(string(jv), 0, 64)
}

func (jv JsonValue) ToString() (string, error) {
	return string(jv), nil
}

func (jv JsonValue) ToBytes() ([]byte, error) {
	return []byte(string(jv)), nil
}

func (jv JsonValue) ToFloat64() (float64, error) {
	return strconv.ParseFloat(string(jv), 64)
}

func (jv JsonValue) ToBool() (bool, error) {
	return strconv.ParseBool(string(jv))
}

func (jv JsonValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert json from array: %v", jv)
}

func (jv JsonValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert json from struct: %v", jv)
}

func (jv JsonValue) ToJSON() (string, error) {
	return string(jv), nil
}

func (jv JsonValue) ToTime() (time.Time, error) {
	return time.Time{}, fmt.Errorf("failed to convert json from time.Time: %v", jv)
}

func (jv JsonValue) ToRat() (*big.Rat, error) {
	i64, err := strconv.ParseInt(string(jv), 0, 64)
	if err != nil {
		return nil, err
	}
	r := new(big.Rat)
	r.SetInt64(i64)
	return r, nil
}

func (jv JsonValue) Format(verb rune) string {
	return string(jv)
}

func (jv JsonValue) Interface() interface{} {
	var v interface{}
	if err := json.Unmarshal([]byte(jv), &v); err != nil {
		return nil
	}
	return v
}

func (jv JsonValue) reflectTypeToJsonType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return "number"
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Struct, reflect.Map:
		return "object"
	case reflect.Ptr:
		return jv.reflectTypeToJsonType(t.Elem())
	}
	return "unknown"
}

func (jv JsonValue) Type() string {
	if string(jv) == "null" {
		return "null"
	}
	rv := reflect.ValueOf(jv.Interface())
	return jv.reflectTypeToJsonType(rv.Type())
}

type ArrayValue struct {
	values []Value
}

func (av *ArrayValue) Has(v Value) (bool, error) {
	for _, val := range av.values {
		cond, err := val.EQ(v)
		if err != nil {
			return false, err
		}
		if cond {
			return true, nil
		}
	}
	return false, nil
}

func (av *ArrayValue) Add(v Value) (Value, error) {
	return nil, fmt.Errorf("add operation is unsupported for array %v", av)
}

func (av *ArrayValue) Sub(v Value) (Value, error) {
	return nil, fmt.Errorf("sub operation is unsupported for array %v", av)
}

func (av *ArrayValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("mul operation is unsupported for array %v", av)
}

func (av *ArrayValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("div operation is unsupported for array %v", av)
}

func (av *ArrayValue) EQ(v Value) (bool, error) {
	arr, err := v.ToArray()
	if err != nil {
		return false, err
	}
	if len(arr.values) != len(av.values) {
		return false, nil
	}
	for idx, value := range av.values {
		cond, err := arr.values[idx].EQ(value)
		if err != nil {
			return false, err
		}
		if !cond {
			return false, nil
		}
	}
	return true, nil
}

func (av *ArrayValue) GT(v Value) (bool, error) {
	arr, err := v.ToArray()
	if err != nil {
		return false, err
	}
	if len(arr.values) != len(av.values) {
		return false, nil
	}
	for idx, value := range av.values {
		cond, err := arr.values[idx].GT(value)
		if err != nil {
			return false, err
		}
		if !cond {
			return false, nil
		}
	}
	return true, nil
}

func (av *ArrayValue) GTE(v Value) (bool, error) {
	arr, err := v.ToArray()
	if err != nil {
		return false, err
	}
	if len(arr.values) != len(av.values) {
		return false, nil
	}
	for idx, value := range av.values {
		cond, err := arr.values[idx].GTE(value)
		if err != nil {
			return false, err
		}
		if !cond {
			return false, nil
		}
	}
	return true, nil
}

func (av *ArrayValue) LT(v Value) (bool, error) {
	arr, err := v.ToArray()
	if err != nil {
		return false, err
	}
	if len(arr.values) != len(av.values) {
		return false, nil
	}
	for idx, value := range av.values {
		cond, err := arr.values[idx].LT(value)
		if err != nil {
			return false, err
		}
		if !cond {
			return false, nil
		}
	}
	return true, nil
}

func (av *ArrayValue) LTE(v Value) (bool, error) {
	arr, err := v.ToArray()
	if err != nil {
		return false, err
	}
	if len(arr.values) != len(av.values) {
		return false, nil
	}
	for idx, value := range av.values {
		cond, err := arr.values[idx].LTE(value)
		if err != nil {
			return false, err
		}
		if !cond {
			return false, nil
		}
	}
	return true, nil
}

func (av *ArrayValue) ToInt64() (int64, error) {
	return 0, fmt.Errorf("failed to convert int64 from array %v", av)
}

func (av *ArrayValue) ToString() (string, error) {
	elems := []string{}
	for _, v := range av.values {
		if v == nil {
			elems = append(elems, "null")
			continue
		}
		elem, err := v.ToJSON()
		if err != nil {
			return "", err
		}
		elems = append(elems, elem)
	}
	return fmt.Sprintf("[%s]", strings.Join(elems, ",")), nil
}

func (av *ArrayValue) ToBytes() ([]byte, error) {
	v, err := av.ToString()
	if err != nil {
		return nil, err
	}
	return []byte(v), nil
}

func (av *ArrayValue) ToFloat64() (float64, error) {
	return 0, fmt.Errorf("failed to convert float64 from array %v", av)
}

func (av *ArrayValue) ToBool() (bool, error) {
	return false, fmt.Errorf("failed to convert bool from array %v", av)
}

func (av *ArrayValue) ToArray() (*ArrayValue, error) {
	return av, nil
}

func (av *ArrayValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert struct from array %v", av)
}

func (av *ArrayValue) ToJSON() (string, error) {
	return av.ToString()
}

func (av *ArrayValue) ToTime() (time.Time, error) {
	return time.Time{}, fmt.Errorf("failed to convert time.Time from array %v", av)
}

func (av *ArrayValue) ToRat() (*big.Rat, error) {
	return nil, fmt.Errorf("failed to convert *big.Rat from array %v", av)
}

func (av *ArrayValue) Format(verb rune) string {
	elems := []string{}
	for _, v := range av.values {
		if v == nil {
			elems = append(elems, "NULL")
			continue
		}
		elems = append(elems, v.Format(verb))
	}
	return fmt.Sprintf("[%s]", strings.Join(elems, ", "))
}

func (av *ArrayValue) Interface() interface{} {
	var arr []interface{}
	for _, v := range av.values {
		if v == nil {
			arr = append(arr, nil)
		} else {
			arr = append(arr, v.Interface())
		}
	}
	return arr
}

type StructValue struct {
	keys   []string
	values []Value
	m      map[string]Value
}

func (sv *StructValue) Add(v Value) (Value, error) {
	return nil, fmt.Errorf("add operation is unsupported for struct %v", sv)
}

func (sv *StructValue) Sub(v Value) (Value, error) {
	return nil, fmt.Errorf("sub operation is unsupported for struct %v", sv)
}

func (sv *StructValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("mul operation is unsupported for struct %v", sv)
}

func (sv *StructValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("div operation is unsupported for struct %v", sv)
}

func (sv *StructValue) EQ(v Value) (bool, error) {
	st, err := v.ToStruct()
	if err != nil {
		return false, err
	}
	if len(st.m) != len(sv.m) {
		return false, nil
	}
	for key := range sv.m {
		cond, err := st.m[key].EQ(sv.m[key])
		if err != nil {
			return false, err
		}
		if !cond {
			return false, nil
		}
	}
	return true, nil
}

func (sv *StructValue) GT(v Value) (bool, error) {
	st, err := v.ToStruct()
	if err != nil {
		return false, err
	}
	if len(st.m) != len(sv.m) {
		return false, nil
	}
	for key := range sv.m {
		cond, err := st.m[key].GT(sv.m[key])
		if err != nil {
			return false, err
		}
		if !cond {
			return false, nil
		}
	}
	return true, nil
}

func (sv *StructValue) GTE(v Value) (bool, error) {
	st, err := v.ToStruct()
	if err != nil {
		return false, err
	}
	if len(st.m) != len(sv.m) {
		return false, nil
	}
	for key := range sv.m {
		cond, err := st.m[key].GTE(sv.m[key])
		if err != nil {
			return false, err
		}
		if !cond {
			return false, nil
		}
	}
	return true, nil
}

func (sv *StructValue) LT(v Value) (bool, error) {
	st, err := v.ToStruct()
	if err != nil {
		return false, err
	}
	if len(st.m) != len(sv.m) {
		return false, nil
	}
	for key := range sv.m {
		cond, err := st.m[key].LT(sv.m[key])
		if err != nil {
			return false, err
		}
		if !cond {
			return false, nil
		}
	}
	return true, nil
}

func (sv *StructValue) LTE(v Value) (bool, error) {
	st, err := v.ToStruct()
	if err != nil {
		return false, err
	}
	if len(st.m) != len(sv.m) {
		return false, nil
	}
	for key := range sv.m {
		cond, err := st.m[key].LTE(sv.m[key])
		if err != nil {
			return false, err
		}
		if !cond {
			return false, nil
		}
	}
	return true, nil
}

func (sv *StructValue) ToInt64() (int64, error) {
	return 0, fmt.Errorf("failed to convert int64 from struct %v", sv)
}

func (sv *StructValue) ToString() (string, error) {
	fields := []string{}
	for i := 0; i < len(sv.keys); i++ {
		key := sv.keys[i]
		value := sv.values[i]
		if value == nil {
			fields = append(
				fields,
				fmt.Sprintf("%s:null", strconv.Quote(key)),
			)
			continue
		}
		v, err := value.ToJSON()
		if err != nil {
			return "", err
		}
		fields = append(
			fields,
			fmt.Sprintf("%s:%s", strconv.Quote(key), v),
		)
	}
	return fmt.Sprintf("{%s}", strings.Join(fields, ",")), nil
}

func (sv *StructValue) ToBytes() ([]byte, error) {
	v, err := sv.ToString()
	if err != nil {
		return nil, err
	}
	return []byte(v), nil
}

func (sv *StructValue) ToFloat64() (float64, error) {
	return 0, fmt.Errorf("failed to convert float64 from struct %v", sv)
}

func (sv *StructValue) ToBool() (bool, error) {
	return false, fmt.Errorf("failed to convert bool from struct %v", sv)
}

func (sv *StructValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert array from struct %v", sv)
}

func (sv *StructValue) ToStruct() (*StructValue, error) {
	return sv, nil
}

func (sv *StructValue) ToJSON() (string, error) {
	return sv.ToString()
}

func (sv *StructValue) ToTime() (time.Time, error) {
	return time.Time{}, fmt.Errorf("failed to convert time.Time from struct %v", sv)
}

func (sv *StructValue) ToRat() (*big.Rat, error) {
	return nil, fmt.Errorf("failed to convert *big.Rat from struct %v", sv)
}

func (sv *StructValue) Format(verb rune) string {
	elems := []string{}
	for _, v := range sv.values {
		if v == nil {
			elems = append(elems, "NULL")
			continue
		}
		elems = append(elems, v.Format(verb))
	}
	return fmt.Sprintf("(%s)", strings.Join(elems, ", "))
}

func (sv *StructValue) Interface() interface{} {
	fields := []map[string]interface{}{}
	for i := 0; i < len(sv.keys); i++ {
		key := sv.keys[i]
		value := sv.values[i]
		if value == nil {
			fields = append(fields, map[string]interface{}{
				key: nil,
			})
		} else {
			fields = append(fields, map[string]interface{}{
				key: value.Interface(),
			})
		}
	}
	return fields
}

type DateValue time.Time

func (d DateValue) AddDateWithInterval(v int, interval string) (Value, error) {
	switch interval {
	case "WEEK":
		return DateValue(time.Time(d).AddDate(0, 0, v*7)), nil
	case "MONTH":
		return DateValue(time.Time(d).AddDate(0, v, 0)), nil
	case "YEAR":
		return DateValue(time.Time(d).AddDate(v, 0, 0)), nil
	default:
		return DateValue(time.Time(d).AddDate(0, 0, v)), nil
	}
}

func (d DateValue) Add(v Value) (Value, error) {
	src := time.Time(d)
	switch vv := v.(type) {
	case *IntervalValue:
		return DatetimeValue(time.Date(
			src.Year()+int(vv.Years),
			time.Month(int(src.Month())+int(vv.Months)),
			src.Day()+int(vv.Days),
			src.Hour()+int(vv.Hours),
			src.Minute()+int(vv.Minutes),
			src.Second()+int(vv.Seconds),
			src.Nanosecond()+int(vv.SubSecondNanos),
			src.Location(),
		)), nil
	case IntValue:
		return DateValue(time.Time(d).AddDate(0, 0, int(vv))), nil
	}
	return nil, fmt.Errorf("failed to use add operator for date and %T type", v)
}

func (d DateValue) Sub(v Value) (Value, error) {
	src := time.Time(d)
	switch vv := v.(type) {
	case *IntervalValue:
		return DatetimeValue(time.Date(
			src.Year()-int(vv.Years),
			time.Month(int(src.Month())-int(vv.Months)),
			src.Day()-int(vv.Days),
			src.Hour()-int(vv.Hours),
			src.Minute()-int(vv.Minutes),
			src.Second()-int(vv.Seconds),
			src.Nanosecond()-int(vv.SubSecondNanos),
			src.Location(),
		)), nil
	case IntValue:
		return DateValue(time.Time(d).AddDate(0, 0, -int(vv))), nil
	}
	dst, err := v.ToTime()
	if err != nil {
		return nil, err
	}
	duration := time.Time(d).Sub(dst)
	days := duration / (24 * time.Hour)
	return &IntervalValue{
		IntervalValue: &bigquery.IntervalValue{
			Days: int32(days),
		},
	}, nil
}

func (d DateValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("mul operation is unsupported for date %v", d)
}

func (d DateValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("div operation is unsupported for date %v", d)
}

func (d DateValue) EQ(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(d).Equal(v2), nil
}

func (d DateValue) GT(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(d).After(v2), nil
}

func (d DateValue) GTE(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(d).Equal(v2) || time.Time(d).After(v2), nil
}

func (d DateValue) LT(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(d).Before(v2), nil
}

func (d DateValue) LTE(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(d).Equal(v2) || time.Time(d).Before(v2), nil
}

func (d DateValue) ToInt64() (int64, error) {
	return time.Time(d).Unix(), nil
}

func (d DateValue) ToString() (string, error) {
	return time.Time(d).Format("2006-01-02"), nil
}

func (d DateValue) ToBytes() ([]byte, error) {
	v, err := d.ToString()
	if err != nil {
		return nil, err
	}
	return []byte(v), nil
}

func (d DateValue) ToFloat64() (float64, error) {
	return float64(time.Time(d).Unix()), nil
}

func (d DateValue) ToBool() (bool, error) {
	return false, fmt.Errorf("failed to convert %v to bool type", d)
}

func (d DateValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert %v to array type", d)
}

func (d DateValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert %v to struct type", d)
}

func (d DateValue) ToJSON() (string, error) {
	return d.ToString()
}

func (d DateValue) ToTime() (time.Time, error) {
	return time.Time(d), nil
}

func (d DateValue) ToRat() (*big.Rat, error) {
	return nil, fmt.Errorf("failed to convert *big.Rat from date %v", d)
}

func (d DateValue) Format(verb rune) string {
	formatted := time.Time(d).Format("2006-01-02")
	switch verb {
	case 't':
		return formatted
	case 'T':
		return fmt.Sprintf(`DATE %q`, formatted)
	}
	return formatted
}

func (d DateValue) Interface() interface{} {
	return time.Time(d).Format("2006-01-02")
}

const (
	datetimeFormat = "2006-01-02T15:04:05.999999"
)

type DatetimeValue time.Time

func (d DatetimeValue) Add(v Value) (Value, error) {
	src := time.Time(d)
	if vv, ok := v.(*IntervalValue); ok {
		return DatetimeValue(time.Date(
			src.Year()+int(vv.Years),
			time.Month(int(src.Month())+int(vv.Months)),
			src.Day()+int(vv.Days),
			src.Hour()+int(vv.Hours),
			src.Minute()+int(vv.Minutes),
			src.Second()+int(vv.Seconds),
			src.Nanosecond()+int(vv.SubSecondNanos),
			src.Location(),
		)), nil
	}
	return nil, fmt.Errorf("failed to use add operator for datetime and %T type", v)
}

func (d DatetimeValue) Sub(v Value) (Value, error) {
	src := time.Time(d)
	if vv, ok := v.(*IntervalValue); ok {
		return DatetimeValue(time.Date(
			src.Year()-int(vv.Years),
			time.Month(int(src.Month())-int(vv.Months)),
			src.Day()-int(vv.Days),
			src.Hour()-int(vv.Hours),
			src.Minute()-int(vv.Minutes),
			src.Second()-int(vv.Seconds),
			src.Nanosecond()-int(vv.SubSecondNanos),
			src.Location(),
		)), nil
	}
	dst, err := v.ToTime()
	if err != nil {
		return nil, err
	}
	duration := src.Sub(dst)
	return &IntervalValue{IntervalValue: bigquery.IntervalValueFromDuration(duration)}, nil
}

func (d DatetimeValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("mul operation is unsupported for datetime %v", d)
}

func (d DatetimeValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("div operation is unsupported for datetime %v", d)
}

func (d DatetimeValue) EQ(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(d).Equal(v2), nil
}

func (d DatetimeValue) GT(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(d).After(v2), nil
}

func (d DatetimeValue) GTE(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(d).Equal(v2) || time.Time(d).After(v2), nil
}

func (d DatetimeValue) LT(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(d).Before(v2), nil
}

func (d DatetimeValue) LTE(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(d).Equal(v2) || time.Time(d).Before(v2), nil
}

func (d DatetimeValue) ToInt64() (int64, error) {
	return time.Time(d).Unix(), nil
}

func (d DatetimeValue) ToString() (string, error) {
	return time.Time(d).Format(datetimeFormat), nil
}

func (d DatetimeValue) ToBytes() ([]byte, error) {
	v, err := d.ToString()
	if err != nil {
		return nil, err
	}
	return []byte(v), nil
}

func (d DatetimeValue) ToFloat64() (float64, error) {
	return float64(time.Time(d).Unix()), nil
}

func (d DatetimeValue) ToBool() (bool, error) {
	return false, fmt.Errorf("failed to convert %v to bool type", d)
}

func (d DatetimeValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert %v to array type", d)
}

func (d DatetimeValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert %v to struct type", d)
}

func (d DatetimeValue) ToJSON() (string, error) {
	return d.ToString()
}

func (d DatetimeValue) ToTime() (time.Time, error) {
	return time.Time(d), nil
}

func (d DatetimeValue) ToRat() (*big.Rat, error) {
	return nil, fmt.Errorf("failed to convert *big.Rat from datetime %v", d)
}

func (d DatetimeValue) Format(verb rune) string {
	formatted := time.Time(d).Format(datetimeFormat)
	switch verb {
	case 't':
		return formatted
	case 'T':
		return fmt.Sprintf(`DATETIME %q`, formatted)
	}
	return formatted
}

func (d DatetimeValue) Interface() interface{} {
	return time.Time(d).Format(datetimeFormat)
}

type TimeValue time.Time

func (t TimeValue) Add(v Value) (Value, error) {
	return nil, fmt.Errorf("add operation is unsupported for time %v", t)
}

func (t TimeValue) Sub(v Value) (Value, error) {
	return nil, fmt.Errorf("sub operation is unsupported for time %v", t)
}

func (t TimeValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("mul operation is unsupported for time %v", t)
}

func (t TimeValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("div operation is unsupported for time %v", t)
}

func (t TimeValue) EQ(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(t).Equal(v2), nil
}

func (t TimeValue) GT(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(t).After(v2), nil
}

func (t TimeValue) GTE(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(t).Equal(v2) || time.Time(t).After(v2), nil
}

func (t TimeValue) LT(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(t).Before(v2), nil
}

func (t TimeValue) LTE(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(t).Equal(v2) || time.Time(t).Before(v2), nil
}

func (t TimeValue) ToInt64() (int64, error) {
	return time.Time(t).Unix(), nil
}

func (t TimeValue) ToString() (string, error) {
	return time.Time(t).Format("15:04:05.999999"), nil
}

func (t TimeValue) ToBytes() ([]byte, error) {
	v, err := t.ToString()
	if err != nil {
		return nil, err
	}
	return []byte(v), nil
}

func (t TimeValue) ToFloat64() (float64, error) {
	return float64(time.Time(t).Unix()), nil
}

func (t TimeValue) ToBool() (bool, error) {
	return false, fmt.Errorf("failed to convert %v to bool type", t)
}

func (t TimeValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert %v to array type", t)
}

func (t TimeValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert %v to struct type", t)
}

func (t TimeValue) ToJSON() (string, error) {
	return t.ToString()
}

func (t TimeValue) ToTime() (time.Time, error) {
	return time.Time(t), nil
}

func (t TimeValue) ToRat() (*big.Rat, error) {
	return nil, fmt.Errorf("failed to convert *big.Rat from time %v", t)
}

func (t TimeValue) Format(verb rune) string {
	formatted := time.Time(t).Format("15:04:05.999999")
	switch verb {
	case 't':
		return formatted
	case 'T':
		return fmt.Sprintf(`TIME %q`, formatted)
	}
	return formatted
}

func (t TimeValue) Interface() interface{} {
	return time.Time(t).Format("15:04:05.999999")
}

type TimestampValue time.Time

func (t TimestampValue) AddValueWithPart(v int64, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return TimestampValue(time.Time(t).Add(time.Duration(v) * time.Microsecond)), nil
	case "MILLISECOND":
		return TimestampValue(time.Time(t).Add(time.Duration(v) * time.Millisecond)), nil
	case "SECOND":
		return TimestampValue(time.Time(t).Add(time.Duration(v) * time.Second)), nil
	case "MINUTE":
		return TimestampValue(time.Time(t).Add(time.Duration(v) * time.Minute)), nil
	case "HOUR":
		return TimestampValue(time.Time(t).Add(time.Duration(v) * time.Hour)), nil
	case "DAY":
		return TimestampValue(time.Time(t).Add(time.Duration(v) * time.Hour * 24)), nil
	default:
		return nil, fmt.Errorf("unknown part value for timestamp: %s", part)
	}
}

func (t TimestampValue) Add(v Value) (Value, error) {
	src := time.Time(t)
	if vv, ok := v.(*IntervalValue); ok {
		return TimestampValue(time.Date(
			src.Year()+int(vv.Years),
			time.Month(int(src.Month())+int(vv.Months)),
			src.Day()+int(vv.Days),
			src.Hour()+int(vv.Hours),
			src.Minute()+int(vv.Minutes),
			src.Second()+int(vv.Seconds),
			src.Nanosecond()+int(vv.SubSecondNanos),
			src.Location(),
		)), nil
	}
	return nil, fmt.Errorf("failed to use add operator for timestamp and %T type", v)
}

func (t TimestampValue) Sub(v Value) (Value, error) {
	src := time.Time(t)
	if vv, ok := v.(*IntervalValue); ok {
		return TimestampValue(time.Date(
			src.Year()-int(vv.Years),
			time.Month(int(src.Month())-int(vv.Months)),
			src.Day()-int(vv.Days),
			src.Hour()-int(vv.Hours),
			src.Minute()-int(vv.Minutes),
			src.Second()-int(vv.Seconds),
			src.Nanosecond()-int(vv.SubSecondNanos),
			src.Location(),
		)), nil
	}
	dst, err := v.ToTime()
	if err != nil {
		return nil, err
	}
	duration := src.Sub(dst)
	return &IntervalValue{IntervalValue: bigquery.IntervalValueFromDuration(duration)}, nil
}

func (t TimestampValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("mul operation is unsupported for timestamp %v", t)
}

func (t TimestampValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("div operation is unsupported for timestamp %v", t)
}

func (t TimestampValue) EQ(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(t).Equal(v2), nil
}

func (t TimestampValue) GT(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(t).After(v2), nil
}

func (t TimestampValue) GTE(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(t).Equal(v2) || time.Time(t).After(v2), nil
}

func (t TimestampValue) LT(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(t).Before(v2), nil
}

func (t TimestampValue) LTE(v Value) (bool, error) {
	v2, err := v.ToTime()
	if err != nil {
		return false, fmt.Errorf("failed to convert %v to time.Time", v)
	}
	return time.Time(t).Equal(v2) || time.Time(t).Before(v2), nil
}

func (t TimestampValue) ToInt64() (int64, error) {
	return time.Time(t).Unix(), nil
}

func (t TimestampValue) ToString() (string, error) {
	return time.Time(t).Format(time.RFC3339Nano), nil
}

func (t TimestampValue) ToBytes() ([]byte, error) {
	v, err := t.ToString()
	if err != nil {
		return nil, err
	}
	return []byte(v), nil
}

func (t TimestampValue) ToFloat64() (float64, error) {
	return float64(time.Time(t).Unix()), nil
}

func (t TimestampValue) ToBool() (bool, error) {
	return false, fmt.Errorf("failed to convert %v to bool type", t)
}

func (t TimestampValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("failed to convert %v to array type", t)
}

func (t TimestampValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("failed to convert %v to struct type", t)
}

func (t TimestampValue) ToJSON() (string, error) {
	return t.ToString()
}

func (t TimestampValue) ToTime() (time.Time, error) {
	return time.Time(t), nil
}

func (t TimestampValue) ToRat() (*big.Rat, error) {
	return nil, fmt.Errorf("failed to convert *big.Rat from timestamp %v", t)
}

func (t TimestampValue) Format(verb rune) string {
	const timestampPrintableFormat = "2006-01-02 15:04:05"
	formatted := time.Time(t).UTC().Format(timestampPrintableFormat) + "+00"
	switch verb {
	case 't':
		return formatted
	case 'T':
		return fmt.Sprintf(`TIMESTAMP %q`, formatted)
	}
	return formatted
}

func (t TimestampValue) Interface() interface{} {
	return time.Time(t).Format(time.RFC3339)
}

type IntervalValue struct {
	*bigquery.IntervalValue
}

func (iv *IntervalValue) Add(v Value) (Value, error) {
	return nil, fmt.Errorf("unsupported add operator for interval value")
}

func (iv *IntervalValue) Sub(v Value) (Value, error) {
	return nil, fmt.Errorf("unsupported sub operator for interval value")
}

func (iv *IntervalValue) Mul(v Value) (Value, error) {
	return nil, fmt.Errorf("unsupported mul operator for interval value")
}

func (iv *IntervalValue) Div(v Value) (Value, error) {
	return nil, fmt.Errorf("unsupported div operator for interval value")
}

func (iv *IntervalValue) EQ(v Value) (bool, error) {
	return false, fmt.Errorf("unsupported eq operator for interval value")
}

func (iv *IntervalValue) GT(v Value) (bool, error) {
	return false, fmt.Errorf("unsupported gt operator for interval value")
}

func (iv *IntervalValue) GTE(v Value) (bool, error) {
	return false, fmt.Errorf("unsupporte gte operator for interval value")
}

func (iv *IntervalValue) LT(v Value) (bool, error) {
	return false, fmt.Errorf("unsupported lt operator for interval value")
}

func (iv *IntervalValue) LTE(v Value) (bool, error) {
	return false, fmt.Errorf("unsupported lte operator for interval value")
}

func (iv *IntervalValue) ToInt64() (int64, error) {
	return 0, fmt.Errorf("unsupported int64 cast for interval value")
}

func (iv *IntervalValue) ToString() (string, error) {
	if iv.Years == 0 && iv.Months < 0 {
		return "-" + iv.String(), nil
	}
	return iv.String(), nil
}

func (iv *IntervalValue) ToBytes() ([]byte, error) {
	s, err := iv.ToString()
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}

func (iv *IntervalValue) ToFloat64() (float64, error) {
	return 0, fmt.Errorf("unsupported float64 cast for interval value")
}

func (iv *IntervalValue) ToBool() (bool, error) {
	return false, fmt.Errorf("unsupported bool cast for interval value")
}

func (iv *IntervalValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("unsupported array cast for interval value")
}

func (iv *IntervalValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("unsupported struct cast for interval value")
}

func (iv *IntervalValue) ToJSON() (string, error) {
	s, err := iv.ToString()
	if err != nil {
		return "", err
	}
	return strconv.Quote(s), nil
}

func (iv *IntervalValue) ToTime() (time.Time, error) {
	return time.Time{}, fmt.Errorf("unsupported time cast for interval value")
}

func (iv *IntervalValue) ToRat() (*big.Rat, error) {
	return nil, fmt.Errorf("unsupported numeric cast for interval value")
}

func (iv *IntervalValue) Format(verb rune) string {
	s, err := iv.ToString()
	if err != nil {
		return ""
	}
	return s
}

func (iv *IntervalValue) Interface() interface{} {
	s, err := iv.ToString()
	if err != nil {
		return nil
	}
	return s
}

type SafeValue struct {
	value Value
}

func (v *SafeValue) Add(arg Value) (Value, error) {
	ret, err := v.value.Add(arg)
	if err != nil {
		return nil, nil
	}
	return ret, nil
}

func (v *SafeValue) Sub(arg Value) (Value, error) {
	ret, err := v.value.Sub(arg)
	if err != nil {
		return nil, nil
	}
	return ret, nil
}

func (v *SafeValue) Mul(arg Value) (Value, error) {
	ret, err := v.value.Mul(arg)
	if err != nil {
		return nil, nil
	}
	return ret, nil
}

func (v *SafeValue) Div(arg Value) (Value, error) {
	ret, err := v.value.Div(arg)
	if err != nil {
		return nil, nil
	}
	return ret, nil
}

func (v *SafeValue) EQ(arg Value) (bool, error) {
	ret, err := v.value.EQ(arg)
	if err != nil {
		return false, nil
	}
	return ret, nil
}

func (v *SafeValue) GT(arg Value) (bool, error) {
	ret, err := v.value.GT(arg)
	if err != nil {
		return false, nil
	}
	return ret, nil
}

func (v *SafeValue) GTE(arg Value) (bool, error) {
	ret, err := v.value.GTE(arg)
	if err != nil {
		return false, nil
	}
	return ret, nil
}

func (v *SafeValue) LT(arg Value) (bool, error) {
	ret, err := v.value.LT(arg)
	if err != nil {
		return false, nil
	}
	return ret, nil
}

func (v *SafeValue) LTE(arg Value) (bool, error) {
	ret, err := v.value.LTE(arg)
	if err != nil {
		return false, nil
	}
	return ret, nil
}

func (v *SafeValue) ToInt64() (int64, error) {
	ret, err := v.value.ToInt64()
	if err != nil {
		return 0, nil
	}
	return ret, nil
}

func (v *SafeValue) ToString() (string, error) {
	ret, err := v.value.ToString()
	if err != nil {
		return "", nil
	}
	return ret, nil
}

func (v *SafeValue) ToBytes() ([]byte, error) {
	ret, err := v.value.ToBytes()
	if err != nil {
		return nil, nil
	}
	return ret, nil
}

func (v *SafeValue) ToFloat64() (float64, error) {
	ret, err := v.value.ToFloat64()
	if err != nil {
		return 0, nil
	}
	return ret, nil
}

func (v *SafeValue) ToBool() (bool, error) {
	ret, err := v.value.ToBool()
	if err != nil {
		return false, nil
	}
	return ret, nil
}

func (v *SafeValue) ToArray() (*ArrayValue, error) {
	ret, err := v.value.ToArray()
	if err != nil {
		return &ArrayValue{}, nil
	}
	return ret, nil
}

func (v *SafeValue) ToStruct() (*StructValue, error) {
	ret, err := v.value.ToStruct()
	if err != nil {
		return &StructValue{}, nil
	}
	return ret, nil
}

func (v *SafeValue) ToJSON() (string, error) {
	ret, err := v.value.ToJSON()
	if err != nil {
		return "", nil
	}
	return ret, nil
}

func (v *SafeValue) ToTime() (time.Time, error) {
	ret, err := v.value.ToTime()
	if err != nil {
		return time.Time{}, nil
	}
	return ret, nil
}

func (v *SafeValue) ToRat() (*big.Rat, error) {
	ret, err := v.value.ToRat()
	if err != nil {
		return nil, nil
	}
	return ret, nil
}

func (v *SafeValue) Format(verb rune) string {
	return v.value.Format(verb)
}

func (v *SafeValue) Interface() interface{} {
	return v.value.Interface()
}

var (
	dateRe     = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	datetimeRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}$`)
	timeRe     = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}`)
)

func isDate(date string) bool {
	return dateRe.MatchString(date)
}

func isDatetime(datetime string) bool {
	return datetimeRe.MatchString(datetime)
}

func isTime(v string) bool {
	return timeRe.MatchString(v)
}

func isTimestamp(timestamp string) bool {
	loc, err := toLocation("")
	if err != nil {
		return false
	}
	if _, err := parseTimestamp(timestamp, loc); err != nil {
		return false
	}
	return true
}

func parseDate(date string) (time.Time, error) {
	return time.Parse("2006-01-02", date)
}

func parseDatetime(datetime string) (time.Time, error) {
	if t, err := time.Parse(datetimeFormat, datetime); err == nil {
		return t, nil
	}
	return time.Parse("2006-01-02 15:04:05.999999", datetime)
}

func parseTime(t string) (time.Time, error) {
	return time.Parse("15:04:05.999999", t)
}

func parseTimestamp(timestamp string, loc *time.Location) (time.Time, error) {
	if t, err := time.ParseInLocation("2006-01-02T15:04:05.999999999Z07:00", timestamp, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02T15:04:05.999999999-07:00", timestamp, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02T15:04:05.999999999-07", timestamp, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02T15:04:05.999999999 MST", timestamp, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02T15:04:05", timestamp, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05.999999999Z07:00", timestamp, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05.999999999-07:00", timestamp, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05.999999999-07", timestamp, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05.999999999 MST", timestamp, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", timestamp, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02", timestamp, loc); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("failed to parse timestamp. unexpected format %s", timestamp)
}

func DateFromInt64Value(v int64) (time.Time, error) {
	return time.Unix(0, 0).Add(time.Duration(v) * 24 * time.Hour), nil
}

func TimestampFromFloatValue(f float64) (time.Time, error) {
	secs := math.Trunc(f)
	micros := math.Trunc((f-secs)*1e6 + 0.5)
	return time.Unix(int64(secs), int64(micros)*1000).UTC(), nil
}

func TimestampFromInt64Value(v int64) (time.Time, error) {
	sec := v / int64(time.Millisecond)
	msec := v - sec*int64(time.Millisecond)
	return time.Unix(sec, msec*int64(time.Millisecond)).UTC(), nil
}

func parseInterval(v string) (*IntervalValue, error) {
	if v == "" {
		return nil, fmt.Errorf("interval value is empty")
	}
	isNegative := v[0] == '-'
	interval, err := bigquery.ParseInterval(v)
	if err != nil {
		return nil, err
	}
	if isNegative && interval.Months > 0 {
		interval.Months *= -1
	}
	return &IntervalValue{IntervalValue: interval}, nil
}

func isNullValue(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	if _, ok := v.([]byte); ok {
		if rv.IsNil() {
			return true
		}
	}
	return false
}
