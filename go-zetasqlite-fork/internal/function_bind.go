package internal

import (
	"errors"
	"fmt"
	"sync"

	"github.com/goccy/go-json"
)

type SQLiteFunction func(...interface{}) (interface{}, error)
type BindFunction func(...Value) (Value, error)
type AggregateBindFunction func() func() *Aggregator
type WindowBindFunction func() func() *WindowAggregator

type FuncInfo struct {
	Name     string
	BindFunc BindFunction
}

type AggregateFuncInfo struct {
	Name     string
	BindFunc AggregateBindFunction
}

type WindowFuncInfo struct {
	Name     string
	BindFunc WindowBindFunction
}

func existsNull(args []Value) bool {
	for _, v := range args {
		if v == nil {
			return true
		}
	}
	return false
}

func convertArgs(args ...interface{}) ([]Value, error) {
	values := make([]Value, 0, len(args))
	for _, arg := range args {
		value, err := DecodeValue(arg)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

type Aggregator struct {
	distinctMap map[string]struct{}
	distinctNil bool
	step        func([]Value, *AggregatorOption) error
	done        func() (Value, error)
}

func (a *Aggregator) Step(stepArgs ...interface{}) error {
	values, err := convertArgs(stepArgs...)
	if err != nil {
		return err
	}
	values, opt := parseAggregateOptions(values...)
	if opt.IgnoreNulls {
		filtered := []Value{}
		for _, v := range values {
			if v == nil {
				continue
			}
			filtered = append(filtered, v)
		}
		values = filtered
		if len(values) == 0 {
			return nil
		}
	}
	if opt.Distinct {
		if len(values) < 1 {
			return fmt.Errorf("DISTINCT option required at least one argument")
		}
		if values[0] == nil {
			if a.distinctNil {
				return nil
			}
			a.distinctNil = true
		} else {
			key, err := values[0].ToString()
			if err != nil {
				return err
			}
			if _, exists := a.distinctMap[key]; exists {
				return nil
			}
			a.distinctMap[key] = struct{}{}
		}
	}
	return a.step(values, opt)
}

func (a *Aggregator) Done() (interface{}, error) {
	ret, err := a.done()
	if err != nil {
		return nil, err
	}
	return EncodeValue(ret)
}

func newAggregator(
	step func([]Value, *AggregatorOption) error,
	done func() (Value, error)) *Aggregator {
	return &Aggregator{
		distinctMap: map[string]struct{}{},
		step:        step,
		done:        done,
	}
}

type WindowAggregator struct {
	distinctMap map[string]struct{}
	agg         *WindowFuncAggregatedStatus
	step        func([]Value, *WindowFuncStatus, *WindowFuncAggregatedStatus) error
	done        func(*WindowFuncAggregatedStatus) (Value, error)
	once        sync.Once
}

func (a *WindowAggregator) Step(stepArgs ...interface{}) error {
	values, err := convertArgs(stepArgs...)
	if err != nil {
		return err
	}
	values, opt := parseAggregateOptions(values...)
	values, windowOpt := parseWindowOptions(values...)
	a.once.Do(func() {
		a.agg.opt = opt
	})
	return a.step(values, windowOpt, a.agg)
}

func (a *WindowAggregator) Done() (interface{}, error) {
	ret, err := a.done(a.agg)
	if err != nil {
		return nil, err
	}
	return EncodeValue(ret)
}

func newWindowAggregator(
	step func([]Value, *WindowFuncStatus, *WindowFuncAggregatedStatus) error,
	done func(*WindowFuncAggregatedStatus) (Value, error)) *WindowAggregator {
	return &WindowAggregator{
		distinctMap: map[string]struct{}{},
		agg:         newWindowFuncAggregatedStatus(),
		step:        step,
		done:        done,
	}
}

func bindAdd(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return ADD(args[0], args[1])
}

func bindSub(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return SUB(args[0], args[1])
}

func bindMul(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return MUL(args[0], args[1])
}

func bindOpDiv(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return OP_DIV(args[0], args[1])
}

func bindEqual(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return EQ(args[0], args[1])
}

func bindNotEqual(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return NOT_EQ(args[0], args[1])
}

func bindGreater(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return GT(args[0], args[1])
}

func bindGreaterOrEqual(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return GTE(args[0], args[1])
}

func bindLess(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return LT(args[0], args[1])
}

func bindLessOrEqual(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return LTE(args[0], args[1])
}

func bindBitNot(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return BIT_NOT(args[0])
}

func bindBitLeftShift(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return BIT_LEFT_SHIFT(args[0], args[1])
}

func bindBitRightShift(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return BIT_RIGHT_SHIFT(args[0], args[1])
}

func bindBitAnd(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return BIT_AND(args[0], args[1])
}

func bindBitOr(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return BIT_OR(args[0], args[1])
}

func bindBitXor(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return BIT_XOR(args[0], args[1])
}

func bindInArray(args ...Value) (Value, error) {
	if existsNull(args) {
		return BoolValue(false), nil
	}
	return ARRAY_IN(args[0], args[1])
}

func bindStructField(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return STRUCT_FIELD(args[0], int(i64))
}

func bindJsonField(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	jsonValue, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	fieldName, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return JSON_FIELD(jsonValue, fieldName)
}

func bindSubscript(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	jsonValue, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return JSON_SUBSCRIPT(jsonValue, args[1])
}

func bindArrayAtOffset(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_OFFSET(args[0], int(i64))
}

func bindSafeArrayAtOffset(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_SAFE_OFFSET(args[0], int(i64))
}

func bindArrayAtOrdinal(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_ORDINAL(args[0], int(i64))
}

func bindSafeArrayAtOrdinal(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_SAFE_ORDINAL(args[0], int(i64))
}

func bindIsDistinctFrom(args ...Value) (Value, error) {
	return IS_DISTINCT_FROM(args[0], args[1])
}

func bindIsNotDistinctFrom(args ...Value) (Value, error) {
	return IS_NOT_DISTINCT_FROM(args[0], args[1])
}

func bindExtract(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	zone := "UTC"
	if len(args) == 3 {
		timeZone, err := args[2].ToString()
		if err != nil {
			return nil, err
		}
		zone = timeZone
	}
	return EXTRACT(args[0], part, zone)
}

func bindExtractDate(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	zone := "UTC"
	if len(args) == 2 {
		timeZone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		zone = timeZone
	}
	return EXTRACT(args[0], "DATE", zone)
}

func bindSessionUser(_ ...Value) (Value, error) {
	return SESSION_USER()
}

func bindGenerateUUID(_ ...Value) (Value, error) {
	return GENERATE_UUID()
}

func bindError(args ...Value) (Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return nil, errors.New(v)
}

func bindBitCount(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return BIT_COUNT(args[0])
}

func bindLike(args ...Value) (Value, error) {
	if existsNull(args) {
		return BoolValue(false), nil
	}
	return LIKE(args[0], args[1])
}

func bindBetween(args ...Value) (Value, error) {
	if existsNull(args) {
		return BoolValue(false), nil
	}
	return BETWEEN(args[0], args[1], args[2])
}

func bindIn(args ...Value) (Value, error) {
	return IN(args[0], args[1:]...)
}

func bindIsNull(args ...Value) (Value, error) {
	return IS_NULL(args[0])
}

func bindIsTrue(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return IS_TRUE(args[0])
}

func bindIsFalse(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return IS_FALSE(args[0])
}

func bindNot(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return NOT(args[0])
}

func bindAnd(args ...Value) (Value, error) {
	return AND(args...)
}

func bindOr(args ...Value) (Value, error) {
	return OR(args...)
}

func bindCoalesce(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("COALESCE: invalid argument num %d", len(args))
	}
	return COALESCE(args...)
}

func bindIf(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("IF: invalid argument num %d", len(args))
	}
	return IF(args[0], args[1], args[2])
}

func bindIfNull(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("IFNULL: invalid argument num %d", len(args))
	}
	return IFNULL(args[0], args[1])
}

func bindNullIf(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NULLIF: invalid argument num %d", len(args))
	}
	return NULLIF(args[0], args[1])
}

func bindCast(args ...Value) (Value, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("CAST: invalid argument num %d", len(args))
	}
	jsonEncodedFromType, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	jsonEncodedToType, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	var fromType Type
	if err := json.Unmarshal([]byte(jsonEncodedFromType), &fromType); err != nil {
		return nil, err
	}
	var toType Type
	if err := json.Unmarshal([]byte(jsonEncodedToType), &toType); err != nil {
		return nil, err
	}
	isSafeCast, err := args[3].ToBool()
	if err != nil {
		return nil, err
	}
	return CAST(args[0], &fromType, &toType, isSafeCast)
}

func bindInterval(args ...Value) (Value, error) {
	value, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return INTERVAL(value, part)
}

func bindMakeInterval(args ...Value) (Value, error) {
	year, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	month, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	day, err := args[2].ToInt64()
	if err != nil {
		return nil, err
	}
	hour, err := args[3].ToInt64()
	if err != nil {
		return nil, err
	}
	minute, err := args[4].ToInt64()
	if err != nil {
		return nil, err
	}
	second, err := args[5].ToInt64()
	if err != nil {
		return nil, err
	}
	return MAKE_INTERVAL(year, month, day, hour, minute, second)
}

func bindJustifyDays(args ...Value) (Value, error) {
	interval, ok := args[0].(*IntervalValue)
	if !ok {
		return nil, fmt.Errorf("JUSTIFY_DAYS: unexpected argument type %T", args[0])
	}
	return JUSTIFY_DAYS(interval)
}

func bindJustifyHours(args ...Value) (Value, error) {
	interval, ok := args[0].(*IntervalValue)
	if !ok {
		return nil, fmt.Errorf("JUSTIFY_HOURS: unexpected argument type %T", args[0])
	}
	return JUSTIFY_HOURS(interval)
}

func bindJustifyInterval(args ...Value) (Value, error) {
	interval, ok := args[0].(*IntervalValue)
	if !ok {
		return nil, fmt.Errorf("JUSTIFY_INTERVAL: unexpected argument type %T", args[0])
	}
	return JUSTIFY_INTERVAL(interval)
}

func bindStGeogPoint(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_GEOGPOINT: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}

	longitude, err := args[0].ToFloat64()
	if err != nil {
		return nil, fmt.Errorf("ST_GEOGPOINT: invalid longitude argument: %w", err)
	}
	latitude, err := args[1].ToFloat64()
	if err != nil {
		return nil, fmt.Errorf("ST_GEOGPOINT: invalid latitude argument: %w", err)
	}

	return ST_GEOGPOINT(longitude, latitude)
}

func bindStGeogFromText(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_GEOGFROMTEXT: invalid argument num %d", len(args))
	}

	wkt, err := args[0].ToString()
	if err != nil {
		return nil, fmt.Errorf("ST_GEOGFROMTEXT: invalid argument: %w", err)
	}

	return ST_GEOGFROMTEXT(wkt)
}

func bindStDistance(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_DISTANCE: invalid argument num %d", len(args))
	}

	geo1, ok := args[0].(*GeographyValue)
	if !ok {
		return nil, fmt.Errorf("ST_DISTANCE: unexpected argument type %T", args[0])
	}
	geo2, ok := args[1].(*GeographyValue)
	if !ok {
		return nil, fmt.Errorf("ST_DISTANCE: unexpected argument type %T", args[0])
	}

	return ST_DISTANCE(geo1, geo2)
}

func bindParseNumeric(args ...Value) (Value, error) {
	numeric, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_NUMERIC(numeric)
}

func bindParseBigNumeric(args ...Value) (Value, error) {
	numeric, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_BIGNUMERIC(numeric)
}

func bindFarmFingerprint(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FARM_FINGERPRINT: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return FARM_FINGERPRINT(v)
}

func bindMD5(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MD5: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return MD5(v)
}

func bindSha1(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA1: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return SHA1(v)
}

func bindSha256(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA256: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return SHA256(v)
}

func bindSha512(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA512: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return SHA512(v)
}

func bindAscii(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ASCII: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	ascii, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	if ascii == "" {
		return IntValue(0), nil
	}
	return ASCII(ascii)
}

func bindByteLength(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BYTE_LENGTH: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return BYTE_LENGTH(v)
}

func bindCharLength(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CHAR_LENGTH: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return CHAR_LENGTH(v)
}

func bindChr(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CHR: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return CHR(v)
}

func bindCodePointsToBytes(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CODE_POINTS_TO_BYTES: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToArray()
	if err != nil {
		return nil, err
	}
	return CODE_POINTS_TO_BYTES(v)
}

func bindCodePointsToString(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CODE_POINTS_TO_STRING: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToArray()
	if err != nil {
		return nil, err
	}
	return CODE_POINTS_TO_STRING(v)
}

func bindCollate(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("COLLATE: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	if args[1] == nil {
		return nil, fmt.Errorf("COLLATE: collation_specification must be string literal")
	}
	value, err := args[0].ToString()
	if err != nil {
		return nil, fmt.Errorf("COLLATE: value must be string: %w", err)
	}
	spec, err := args[1].ToString()
	if err != nil {
		return nil, fmt.Errorf("COLLATE: collation_specification must be string literal: %w", err)
	}
	return COLLATE(value, spec)
}

func bindConcat(args ...Value) (Value, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("CONCAT: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	return CONCAT(args...)
}

func bindContainsSubstr(args ...Value) (Value, error) {
	if args[1] == nil {
		return nil, fmt.Errorf("CONTAINS_SUBSTR: search literal must be not null")
	}
	if existsNull(args) {
		return nil, nil
	}
	search, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return CONTAINS_SUBSTR(args[0], search)
}

func bindEndsWith(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ENDS_WITH: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	return ENDS_WITH(args[0], args[1])
}

func bindFormat(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("FORMAT: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	if len(args) > 1 {
		return FORMAT(format, args[1:]...)
	}
	return FORMAT(format)
}

func bindFromBase32(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FROM_BASE32: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return FROM_BASE32(v)
}

func bindFromBase64(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FROM_BASE64: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return FROM_BASE64(v)
}

func bindFromHex(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FROM_HEX: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return FROM_HEX(v)
}

func bindInitcap(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("INITCAP: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	var delimiters []rune
	if len(args) == 2 {
		v, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		delimiters = []rune{}
		for _, vv := range v {
			delimiters = append(delimiters, vv)
		}
	}
	value, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return INITCAP(value, delimiters)
}

func bindInstr(args ...Value) (Value, error) {
	if len(args) != 2 && len(args) != 3 && len(args) != 4 {
		return nil, fmt.Errorf("INSTR: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	var (
		position   int64 = 0
		occurrence int64 = 1
	)
	if len(args) >= 3 {
		pos, err := args[2].ToInt64()
		if err != nil {
			return nil, err
		}
		position = pos
	}
	if len(args) == 4 {
		occur, err := args[3].ToInt64()
		if err != nil {
			return nil, err
		}
		occurrence = occur
	}
	return INSTR(args[0], args[1], position, occurrence)
}

func bindLeft(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LEFT: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	length, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return LEFT(args[0], length)
}

func bindLength(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LENGTH: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	return LENGTH(args[0])
}

func bindLpad(args ...Value) (Value, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, fmt.Errorf("LPAD: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	var pattern Value
	if len(args) == 3 {
		pattern = args[2]
	}
	length, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return LPAD(args[0], length, pattern)
}

func bindLower(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOWER: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	return LOWER(args[0])
}

func bindLtrim(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("LTRIM: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	cutset := " "
	if len(args) == 2 {
		v, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		cutset = v
	}
	return LTRIM(args[0], cutset)
}

func bindNormalize(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("NORMALIZE: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	mode := "NFC"
	if len(args) == 2 {
		v, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		mode = v
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return NORMALIZE(v, mode)
}

func bindNormalizeAndCasefold(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("NORMALIZE_AND_CASEFOLD: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	mode := "NFC"
	if len(args) == 2 {
		v, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		mode = v
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return NORMALIZE_AND_CASEFOLD(v, mode)
}

func bindRegexpContains(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	regexp, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return REGEXP_CONTAINS(v, regexp)
}

func bindRegexpExtract(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	regexp, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	var pos int64 = 1
	if len(args) > 2 {
		p, err := args[2].ToInt64()
		if err != nil {
			return nil, err
		}
		pos = p
	}
	var occurrence int64 = 1
	if len(args) > 3 {
		o, err := args[3].ToInt64()
		if err != nil {
			return nil, err
		}
		occurrence = o
	}
	return REGEXP_EXTRACT(args[0], regexp, pos, occurrence)
}

func bindRegexpExtractAll(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	regexp, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return REGEXP_EXTRACT_ALL(args[0], regexp)
}

func bindRegexpInstr(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	var (
		pos           int64 = 1
		occurrence    int64 = 1
		occurrencePos int64 = 0
	)
	if len(args) > 2 {
		p, err := args[2].ToInt64()
		if err != nil {
			return nil, err
		}
		pos = p
	}
	if len(args) > 3 {
		o, err := args[3].ToInt64()
		if err != nil {
			return nil, err
		}
		occurrence = o
	}
	if len(args) > 4 {
		p, err := args[4].ToInt64()
		if err != nil {
			return nil, err
		}
		occurrencePos = p
	}
	return REGEXP_INSTR(args[0], args[1], pos, occurrence, occurrencePos)
}

func bindRegexpReplace(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return REGEXP_REPLACE(args[0], args[1], args[2])
}

func bindReplace(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return REPLACE(args[0], args[1], args[2])
}

func bindRepeat(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	repetitions, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return REPEAT(args[0], repetitions)
}

func bindReverse(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return REVERSE(args[0])
}

func bindRight(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	length, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return RIGHT(args[0], length)
}

func bindRpad(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	length, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	var pat Value
	if len(args) > 2 {
		pat = args[2]
	}
	return RPAD(args[0], length, pat)
}

func bindRtrim(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	var cutset string = " "
	if len(args) > 1 {
		v, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		cutset = v
	}
	return RTRIM(args[0], cutset)
}

func bindSafeConvertBytesToString(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return SAFE_CONVERT_BYTES_TO_STRING(v)
}

func bindSoundex(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return SOUNDEX(v)
}

func bindSplit(args ...Value) (Value, error) {
	if existsNull(args) {
		return &ArrayValue{}, nil
	}
	var delim Value
	if len(args) > 1 {
		delim = args[1]
	}
	return SPLIT(args[0], delim)
}

func bindStartsWith(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("STARTS_WITH: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	return STARTS_WITH(args[0], args[1])
}

func bindStrpos(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("STRPOS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	return STRPOS(args[0], args[1])
}

func bindSubstr(args ...Value) (Value, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, fmt.Errorf("SUBSTR: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	pos, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	var length *int64
	if len(args) == 3 {
		v, err := args[2].ToInt64()
		if err != nil {
			return nil, err
		}
		length = &v
	}
	return SUBSTR(args[0], pos, length)
}

func bindToBase32(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TO_BASE32: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	b, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return TO_BASE32(b)
}

func bindToBase64(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TO_BASE64: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	b, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return TO_BASE64(b)
}

func bindToCodePoints(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TO_CODE_POINTS: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return &ArrayValue{}, nil
	}
	return TO_CODE_POINTS(args[0])
}

func bindToHex(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TO_HEX: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	b, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return TO_HEX(b)
}

func bindTranslate(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TRANSLATE: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	return TRANSLATE(args[0], args[1], args[2])
}

func bindTrim(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("TRIM: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	if len(args) == 2 {
		return TRIM(args[0], args[1])
	}
	return TRIM(args[0], nil)
}

func bindUnicode(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNICODE: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return UNICODE(v)
}

func bindUpper(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UPPER: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	return UPPER(args[0])
}

func bindJsonExtract(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	path, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return JSON_EXTRACT(v, path)
}

func bindJsonExtractScalar(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	path, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return JSON_EXTRACT_SCALAR(v, path)
}

func bindJsonExtractArray(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	path, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return JSON_EXTRACT_ARRAY(v, path)
}

func bindJsonExtractStringArray(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	path, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return JSON_EXTRACT_STRING_ARRAY(v, path)
}

func bindJsonQuery(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	path, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return JSON_QUERY(v, path)
}

func bindJsonValue(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	path, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return JSON_VALUE(v, path)
}

func bindJsonQueryArray(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	path, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return JSON_QUERY_ARRAY(v, path)
}

func bindJsonValueArray(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	path, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return JSON_VALUE_ARRAY(v, path)
}

func bindParseJson(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	mode, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_JSON(v, mode)
}

func bindToJson(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("TO_JSON: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	var stringifyWideNumbers bool
	if len(args) == 2 {
		b, err := args[1].ToBool()
		if err != nil {
			return nil, err
		}
		stringifyWideNumbers = b
	}
	return TO_JSON(args[0], stringifyWideNumbers)
}

func bindToJsonString(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("TO_JSON_STRING: invalid argument num %d", len(args))
	}
	var prettyPrint bool
	if len(args) == 2 {
		b, err := args[1].ToBool()
		if err != nil {
			return nil, err
		}
		prettyPrint = b
	}
	return TO_JSON_STRING(args[0], prettyPrint)
}

func bindBool(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BOOL: invalid argument num %d", len(args))
	}
	return args[0], nil
}

func bindInt64(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("INT64: invalid argument num %d", len(args))
	}
	return args[0], nil
}

func bindDouble(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("FLOAT64: invalid argument num %d", len(args))
	}
	mode, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	switch mode {
	case "exact":
		return args[0], nil
	case "round":
		return args[0], nil
	}
	return nil, fmt.Errorf("unexpected wide_number_mode: %s", mode)
}

func bindJsonType(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("JSON_TYPE: invalid argument num %d", len(args))
	}
	value, ok := args[0].(JsonValue)
	if !ok {
		return nil, fmt.Errorf("JSON_TYPE: failed to convert %T to JSON value", args[0])
	}
	return JSON_TYPE(value)
}

func bindAbs(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return ABS(args[0])
}

func bindSign(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return SIGN(args[0])
}

func bindIsInf(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return IS_INF(args[0])
}

func bindIsNaN(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return IS_NAN(args[0])
}

func bindIEEEDivide(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return IEEE_DIVIDE(args[0], args[1])
}

func bindRand(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return RAND()
}

func bindSqrt(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return SQRT(args[0])
}

func bindPow(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return POW(args[0], args[1])
}

func bindExp(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return EXP(args[0])
}

func bindLn(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return LN(args[0])
}

func bindLog(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	if len(args) == 1 {
		return LN(args[0])
	}
	return LOG(args[0], args[1])
}

func bindLog10(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return LOG10(args[0])
}

func bindGreatest(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return GREATEST(args...)
}

func bindLeast(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return LEAST(args...)
}

func bindDiv(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return DIV(args[0], args[1])
}

func bindSafeDivide(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return SAFE_DIVIDE(args[0], args[1])
}

func bindSafeMultiply(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return SAFE_MULTIPLY(args[0], args[1])
}

func bindSafeNegate(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return SAFE_NEGATE(args[0])
}

func bindSafeAdd(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return SAFE_ADD(args[0], args[1])
}

func bindSafeSubtract(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return SAFE_SUBTRACT(args[0], args[1])
}

func bindMod(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return MOD(args[0], args[1])
}

func bindRound(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("ROUND: invalid argument num %d", len(args))
	}
	var precision int = 0
	if len(args) == 2 {
		i64, err := args[1].ToInt64()
		if err != nil {
			return nil, err
		}
		precision = int(i64)
	}

	if existsNull(args) {
		return nil, nil
	}

	return ROUND(args[0], precision)
}

func bindTrunc(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return TRUNC(args[0])
}

func bindCeil(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return CEIL(args[0])
}

func bindFloor(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return FLOOR(args[0])
}

func bindCos(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return COS(args[0])
}

func bindCosh(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return COSH(args[0])
}

func bindAcos(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return ACOS(args[0])
}

func bindAcosh(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return ACOSH(args[0])
}

func bindSin(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return SIN(args[0])
}

func bindSinh(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return SINH(args[0])
}

func bindAsin(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return ASIN(args[0])
}

func bindAsinh(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return ASINH(args[0])
}

func bindTan(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return TAN(args[0])
}

func bindTanh(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return TANH(args[0])
}

func bindAtan(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return ATAN(args[0])
}

func bindAtanh(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return ATANH(args[0])
}

func bindAtan2(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return ATAN2(args[0], args[1])
}

func bindRangeBucket(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	array, err := args[1].ToArray()
	if err != nil {
		return nil, err
	}
	return RANGE_BUCKET(args[0], array)
}

func bindCurrentDate(args ...Value) (Value, error) {
	if len(args) == 0 {
		return CURRENT_DATE("")
	}
	if len(args) == 2 {
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		zone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		loc, err := toLocation(zone)
		if err != nil {
			return nil, err
		}
		return CURRENT_DATE_WITH_TIME(timeFromUnixNano(unixNano).In(loc))
	}
	switch args[0].(type) {
	case IntValue:
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		return CURRENT_DATE_WITH_TIME(timeFromUnixNano(unixNano))
	case StringValue:
		zone, err := args[0].ToString()
		if err != nil {
			return nil, err
		}
		return CURRENT_DATE(zone)
	}
	return nil, fmt.Errorf("CURRENT_DATE: unexpected argument type %T", args[0])
}

func bindDate(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return DATE(args...)
}

func bindDateAdd(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_ADD: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATE_ADD(t, num, part)
}

func bindDateSub(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_SUB: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATE_SUB(t, num, part)
}

func bindDateDiff(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_DIFF: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	t2, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATE_DIFF(t, t2, part)
}

func bindDateTrunc(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("DATE_TRUNC: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return DATE_TRUNC(t, part)
}

func bindDateFromUnixDate(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DATE_FROM_UNIX_DATE: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	unixdate, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return DATE_FROM_UNIX_DATE(unixdate)
}

func bindFormatDate(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("FORMAT_DATE: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	t, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	return FORMAT_DATE(format, t)
}

func bindLastDay(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("LAST_DAY: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	var part = "MONTH"
	if len(args) == 2 {
		p, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		part = p
	}
	return LAST_DAY(t, part)
}

func bindParseDate(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("PARSE_DATE: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	target, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_DATE(format, target)
}

func bindUnixDate(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNIX_DATE: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	return UNIX_DATE(t)
}

func bindCurrentDatetime(args ...Value) (Value, error) {
	if len(args) == 0 {
		return CURRENT_DATETIME("")
	}
	if len(args) == 2 {
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		zone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		loc, err := toLocation(zone)
		if err != nil {
			return nil, err
		}
		return CURRENT_DATETIME_WITH_TIME(timeFromUnixNano(unixNano).In(loc))
	}
	switch args[0].(type) {
	case IntValue:
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		return CURRENT_DATETIME_WITH_TIME(timeFromUnixNano(unixNano))
	case StringValue:
		zone, err := args[0].ToString()
		if err != nil {
			return nil, err
		}
		return CURRENT_DATETIME(zone)
	}
	return nil, fmt.Errorf("CURRENT_DATETIME: unexpected argument type %T", args[0])
}

func bindDatetime(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return DATETIME(args...)
}

func bindDatetimeAdd(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATETIME_ADD: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATETIME_ADD(t, num, part)
}

func bindDatetimeSub(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATETIME_SUB: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATETIME_SUB(t, num, part)
}

func bindDatetimeDiff(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATETIME_DIFF: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	t2, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATETIME_DIFF(t, t2, part)
}

func bindDatetimeTrunc(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("DATETIME_TRUNC: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return DATETIME_TRUNC(t, part)
}

func bindFormatDatetime(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("FORMAT_DATETIME: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	t, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	return FORMAT_DATETIME(format, t)
}

func bindParseDatetime(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("PARSE_DATETIME: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	target, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_DATETIME(format, target)
}

func bindCurrentTime(args ...Value) (Value, error) {
	if len(args) == 0 {
		return CURRENT_TIME("")
	}
	if len(args) == 2 {
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		zone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		loc, err := toLocation(zone)
		if err != nil {
			return nil, err
		}
		return CURRENT_TIME_WITH_TIME(timeFromUnixNano(unixNano).In(loc))
	}
	switch args[0].(type) {
	case IntValue:
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		return CURRENT_TIME_WITH_TIME(timeFromUnixNano(unixNano))
	case StringValue:
		zone, err := args[0].ToString()
		if err != nil {
			return nil, err
		}
		return CURRENT_TIME(zone)
	}
	return nil, fmt.Errorf("CURRENT_TIME: unexpected argument type %T", args[0])
}

func bindTime(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return TIME(args...)
}

func bindTimeAdd(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIME_ADD: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIME_ADD(t, num, part)
}

func bindTimeSub(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIME_SUB: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIME_SUB(t, num, part)
}

func bindTimeDiff(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIME_DIFF: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	t2, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIME_DIFF(t, t2, part)
}

func bindTimeTrunc(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("TIME_TRUNC: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return TIME_TRUNC(t, part)
}

func bindFormatTime(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("FORMAT_TIME: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	t, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	return FORMAT_TIME(format, t)
}

func bindParseTime(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("PARSE_TIME: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	target, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_TIME(format, target)
}

func bindCurrentTimestamp(args ...Value) (Value, error) {
	if len(args) == 0 {
		return CURRENT_TIMESTAMP("")
	}
	if len(args) == 2 {
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		zone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		loc, err := toLocation(zone)
		if err != nil {
			return nil, err
		}
		return CURRENT_TIMESTAMP_WITH_TIME(timeFromUnixNano(unixNano).In(loc))
	}
	switch args[0].(type) {
	case IntValue:
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		return CURRENT_TIMESTAMP_WITH_TIME(timeFromUnixNano(unixNano))
	case StringValue:
		zone, err := args[0].ToString()
		if err != nil {
			return nil, err
		}
		return CURRENT_TIMESTAMP(zone)
	}
	return nil, fmt.Errorf("CURRENT_TIMESTAMP: unexpected argument type %T", args[0])
}

func bindString(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("STRING: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	jsonValue, ok := args[0].(JsonValue)
	if ok {
		return StringValue(fmt.Sprint(jsonValue.Interface())), nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	var zone string
	if len(args) == 2 {
		z, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		zone = z
	}
	return STRING(t, zone)
}

func bindTimestamp(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("TIMESTAMP: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	var zone string
	if len(args) == 2 {
		z, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		zone = z
	}
	return TIMESTAMP(args[0], zone)
}

func bindTimestampAdd(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIMESTAMP_ADD: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_ADD(t, num, part)
}

func bindTimestampSub(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIMESTAMP_SUB: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_SUB(t, num, part)
}

func bindTimestampDiff(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIMESTAMP_DIFF: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	t2, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_DIFF(t, t2, part)
}

func bindTimestampTrunc(args ...Value) (Value, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, fmt.Errorf("TIMESTAMP_TRUNC: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	var zone string
	if len(args) == 3 {
		z, err := args[2].ToString()
		if err != nil {
			return nil, err
		}
		zone = z
	}
	return TIMESTAMP_TRUNC(t, part, zone)
}

func bindFormatTimestamp(args ...Value) (Value, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, fmt.Errorf("FORMAT_TIMESTAMP: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	t, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	var zone string
	if len(args) == 3 {
		z, err := args[2].ToString()
		if err != nil {
			return nil, err
		}
		zone = z
	}
	return FORMAT_TIMESTAMP(format, t, zone)
}

func bindParseTimestamp(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	target, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	if len(args) == 2 {
		return PARSE_TIMESTAMP(format, target)
	}
	timeZone, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_TIMESTAMP_WITH_TIMEZONE(format, target, timeZone)
}

func bindTimestampSeconds(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TIMESTAMP_SECONDS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	sec, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_SECONDS(sec)
}

func bindTimestampMillis(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TIMESTAMP_MILLIS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	millisec, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_MILLIS(millisec)
}

func bindTimestampMicros(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TIMESTAMP_MICROS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	microsec, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_MICROS(microsec)
}

func bindUnixSeconds(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNIX_SECONDS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	return UNIX_SECONDS(t)
}

func bindUnixMillis(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNIX_MILLIS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	return UNIX_MILLIS(t)
}

func bindUnixMicros(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNIX_MICROS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	return UNIX_MICROS(t)
}

func bindArrayConcat(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("ARRAY_CONCAT: required arguments")
	}
	return ARRAY_CONCAT(args...)
}

func bindArrayLength(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ARRAY_LENGTH: invalid argument num %d", len(args))
	}
	arr, err := args[0].ToArray()
	if err != nil {
		return nil, err
	}
	return ARRAY_LENGTH(arr)
}

func bindArrayToString(args ...Value) (Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("ARRAY_TO_STRING: invalid argument num %d", len(args))
	}
	arr, err := args[0].ToArray()
	if err != nil {
		return nil, err
	}
	delim, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	if len(args) == 3 {
		nullText, err := args[2].ToString()
		if err != nil {
			return nil, err
		}
		return ARRAY_TO_STRING(arr, delim, nullText)
	}
	return ARRAY_TO_STRING(arr, delim)
}

func bindGenerateArray(args ...Value) (Value, error) {
	if len(args) != 3 && len(args) != 2 {
		return nil, fmt.Errorf("GENERATE_ARRAY: invalid argument num %d", len(args))
	}
	if len(args) == 3 {
		return GENERATE_ARRAY(args[0], args[1], args[2])
	}
	return GENERATE_ARRAY(args[0], args[1])
}

func bindGenerateDateArray(args ...Value) (Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("GENERATE_DATE_ARRAY: invalid argument num %d", len(args))
	}
	if len(args) == 2 {
		return GENERATE_DATE_ARRAY(args[0], args[1])
	}
	return GENERATE_DATE_ARRAY(args[0], args[1], args[2:]...)
}

func bindGenerateTimestampArray(args ...Value) (Value, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("GENERATE_TIMESTAMP_ARRAY: invalid argument num %d", len(args))
	}
	step, err := args[2].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[3].ToString()
	if err != nil {
		return nil, err
	}
	return GENERATE_TIMESTAMP_ARRAY(args[0], args[1], step, part)
}

func bindArrayReverse(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ARRAY_REVERSE: invalid argument num %d", len(args))
	}
	arr, err := args[0].ToArray()
	if err != nil {
		return nil, err
	}
	return ARRAY_REVERSE(arr)
}

func bindMakeArray(args ...Value) (Value, error) {
	return MAKE_ARRAY(args...)
}

func bindMakeStruct(args ...Value) (Value, error) {
	if len(args)%2 != 0 {
		return nil, fmt.Errorf("MAKE_STRUCT: unexpected argument num %d", len(args))
	}
	return MAKE_STRUCT(args...)
}

func bindDistinct(args ...Value) (Value, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("DISTINCT: invalid argument num %d", len(args))
	}
	return DISTINCT()
}

func bindLimit(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LIMIT: invalid argument num %d", len(args))
	}
	i64, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return LIMIT(i64)
}

func bindIgnoreNulls(args ...Value) (Value, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("IGNORE_NULLS: invalid argument num %d", len(args))
	}
	return IGNORE_NULLS()
}

func bindOrderBy(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ORDER_BY: invalid argument num %d", len(args))
	}
	b, err := args[1].ToBool()
	if err != nil {
		return nil, err
	}
	return ORDER_BY(args[0], b)
}

func bindWindowFrameUnit(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WINDOW_FRAME_UNIT: invalid argument num %d", len(args))
	}
	i64, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_FRAME_UNIT(i64)
}

func bindWindowPartition(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WINDOW_PARTITION: invalid argument num %d", len(args))
	}
	return WINDOW_PARTITION(args[0])
}

func bindWindowBoundaryStart(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("WINDOW_BOUNDARY_START: invalid argument num %d", len(args))
	}
	a0, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	a1, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_BOUNDARY_START(a0, a1)
}

func bindWindowBoundaryEnd(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("WINDOW_BOUNDARY_END: invalid argument num %d", len(args))
	}
	a0, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	a1, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_BOUNDARY_END(a0, a1)
}

func bindWindowRowID(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WINDOW_ROWID: invalid argument num %d", len(args))
	}
	a0, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_ROWID(a0)
}

func bindWindowOrderBy(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("WINDOW_ORDER_BY: invalid argument num %d", len(args))
	}
	isAsc, err := args[1].ToBool()
	if err != nil {
		return nil, err
	}
	return WINDOW_ORDER_BY(args[0], isAsc)
}

func bindEvalJavaScript(args ...Value) (Value, error) {
	code, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	encodedType, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	var typ Type
	if err := json.Unmarshal([]byte(encodedType), &typ); err != nil {
		return nil, fmt.Errorf("failed to decode type information from %s: %w", encodedType, err)
	}
	if len(args) == 2 {
		return EVAL_JAVASCRIPT(code, &typ, nil, nil)
	}
	argNames, err := args[2].ToArray()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(argNames.values))
	for _, val := range argNames.values {
		name, err := val.ToString()
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return EVAL_JAVASCRIPT(code, &typ, names, args[3:])
}

func bindNetHost(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NET.HOST: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return NET_HOST(v)
}

func bindNetIpFromString(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NET.IP_FROM_STRING: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return NET_IP_FROM_STRING(v)
}

func bindNetIpNetMask(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NET.IP_NET_MASK: invalid argument num %d", len(args))
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	output, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	prefix, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return NET_IP_NET_MASK(output, prefix)
}

func bindNetIpToString(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NET.IP_TO_STRING: invalid argument num %d", len(args))
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return NET_IP_TO_STRING(v)
}

func bindNetIpTrunc(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NET.IP_TRUNC: invalid argument num %d", len(args))
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	length, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return NET_IP_TRUNC(v, length)
}

func bindNetIpv4FromInt64(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NET.IPV4_FROM_INT64: invalid argument num %d", len(args))
	}
	v, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return NET_IPV4_FROM_INT64(v)
}

func bindNetIpv4ToInt64(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NET.IPV4_TO_INT64: invalid argument num %d", len(args))
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return NET_IPV4_TO_INT64(v)
}

func bindNetPublicSuffix(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NET.PUBLIC_SUFFIX: invalid argument num %d", len(args))
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return NET_PUBLIC_SUFFIX(v)
}

func bindNetRegDomain(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NET.REG_DOMAIN: invalid argument num %d", len(args))
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return NET_REG_DOMAIN(v)
}

func bindNetSafeIpFromString(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NET.IP_SAFE_FROM_STRING: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return NET_SAFE_IP_FROM_STRING(v)
}

func bindArray() func() *Aggregator {
	return func() *Aggregator {
		fn := &ARRAY{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindAnyValue() func() *Aggregator {
	return func() *Aggregator {
		fn := &ANY_VALUE{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindArrayAgg() func() *Aggregator {
	return func() *Aggregator {
		fn := &ARRAY_AGG{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindArrayConcatAgg() func() *Aggregator {
	return func() *Aggregator {
		fn := &ARRAY_CONCAT_AGG{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if args[0] == nil {
					return nil
				}
				array, err := args[0].ToArray()
				if err != nil {
					return err
				}
				return fn.Step(array, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindAvg() func() *Aggregator {
	return func() *Aggregator {
		fn := &AVG{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindCount() func() *Aggregator {
	return func() *Aggregator {
		fn := &COUNT{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindCountStar() func() *Aggregator {
	return func() *Aggregator {
		fn := &COUNT_STAR{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindBitAndAgg() func() *Aggregator {
	return func() *Aggregator {
		fn := &BIT_AND_AGG{IntValue(-1)}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindBitOrAgg() func() *Aggregator {
	return func() *Aggregator {
		fn := &BIT_OR_AGG{-1}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindBitXorAgg() func() *Aggregator {
	return func() *Aggregator {
		fn := &BIT_XOR_AGG{1}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindCountIf() func() *Aggregator {
	return func() *Aggregator {
		fn := &COUNTIF{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindLogicalAnd() func() *Aggregator {
	return func() *Aggregator {
		fn := &LOGICAL_AND{true}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindLogicalOr() func() *Aggregator {
	return func() *Aggregator {
		fn := &LOGICAL_OR{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindMax() func() *Aggregator {
	return func() *Aggregator {
		fn := &MAX{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindMin() func() *Aggregator {
	return func() *Aggregator {
		fn := &MIN{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindStringAgg() func() *Aggregator {
	return func() *Aggregator {
		fn := &STRING_AGG{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) == 1 {
					return fn.Step(args[0], "", opt)
				}
				delim, err := args[1].ToString()
				if err != nil {
					return err
				}
				return fn.Step(args[0], delim, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindSum() func() *Aggregator {
	return func() *Aggregator {
		fn := &SUM{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindCorr() func() *Aggregator {
	return func() *Aggregator {
		fn := &CORR{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], args[1], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindCovarPop() func() *Aggregator {
	return func() *Aggregator {
		fn := &COVAR_POP{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], args[1], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindCovarSamp() func() *Aggregator {
	return func() *Aggregator {
		fn := &COVAR_SAMP{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], args[1], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindStddevPop() func() *Aggregator {
	return func() *Aggregator {
		fn := &STDDEV_POP{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindStddevSamp() func() *Aggregator {
	return func() *Aggregator {
		fn := &STDDEV_SAMP{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindStddev() func() *Aggregator {
	return func() *Aggregator {
		fn := &STDDEV{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindVarPop() func() *Aggregator {
	return func() *Aggregator {
		fn := &VAR_POP{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindVarSamp() func() *Aggregator {
	return func() *Aggregator {
		fn := &VAR_SAMP{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindVariance() func() *Aggregator {
	return func() *Aggregator {
		fn := &VARIANCE{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindApproxCountDistinct() func() *Aggregator {
	return func() *Aggregator {
		fn := &APPROX_COUNT_DISTINCT{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindApproxQuantiles() func() *Aggregator {
	return func() *Aggregator {
		fn := &APPROX_QUANTILES{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if args[1] == nil {
					return fmt.Errorf("APPROX_QUANTILES: number must be not null")
				}
				num, err := args[1].ToInt64()
				if err != nil {
					return err
				}
				return fn.Step(args[0], num, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindApproxTopCount() func() *Aggregator {
	return func() *Aggregator {
		fn := &APPROX_TOP_COUNT{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if args[1] == nil {
					return fmt.Errorf("APPROX_TOP_COUNT: number must be not null")
				}
				num, err := args[1].ToInt64()
				if err != nil {
					return err
				}
				return fn.Step(args[0], num, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindApproxTopSum() func() *Aggregator {
	return func() *Aggregator {
		fn := &APPROX_TOP_SUM{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if args[2] == nil {
					return fmt.Errorf("APPROX_TOP_SUM: number must be not null")
				}
				num, err := args[2].ToInt64()
				if err != nil {
					return err
				}
				return fn.Step(args[0], args[1], num, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindHllCountInit() func() *Aggregator {
	return func() *Aggregator {
		fn := &HLL_COUNT_INIT{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				precision := int64(15)
				if len(args) == 2 {
					if args[1] == nil {
						return nil
					}
					v, err := args[1].ToInt64()
					if err != nil {
						return err
					}
					precision = v
				}
				return fn.Step(args[0], precision, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindHllCountMerge() func() *Aggregator {
	return func() *Aggregator {
		fn := &HLL_COUNT_MERGE{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if args[0] == nil {
					return nil
				}
				b, err := args[0].ToBytes()
				if err != nil {
					return err
				}
				return fn.Step(b, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindHllCountMergePartial() func() *Aggregator {
	return func() *Aggregator {
		fn := &HLL_COUNT_MERGE_PARTIAL{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if args[0] == nil {
					return nil
				}
				b, err := args[0].ToBytes()
				if err != nil {
					return err
				}
				return fn.Step(b, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
		)
	}
}

func bindHllCountExtract(args ...Value) (Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	b, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return HLL_COUNT_EXTRACT(b)
}

func bindWindowAnyValue() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_ANY_VALUE{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowArrayAgg() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_ARRAY_AGG{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowAvg() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_AVG{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowCount() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_COUNT{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowCountStar() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_COUNT_STAR{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowCountIf() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_COUNTIF{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowMax() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_MAX{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowMin() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_MIN{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowStringAgg() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_STRING_AGG{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				var delim string
				if len(args) > 1 {
					d, err := args[1].ToString()
					if err != nil {
						return err
					}
					delim = d
				}
				return fn.Step(args[0], delim, windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowSum() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_SUM{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowCorr() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_CORR{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], args[1], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowCovarPop() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_COVAR_POP{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], args[1], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowCovarSamp() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_COVAR_SAMP{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], args[1], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowStddevPop() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_STDDEV_POP{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowStddevSamp() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_STDDEV_SAMP{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowStddev() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_STDDEV{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowVarPop() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_VAR_POP{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowVarSamp() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_VAR_SAMP{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowVariance() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_VARIANCE{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowFirstValue() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_FIRST_VALUE{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowLastValue() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_LAST_VALUE{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowNthValue() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_NTH_VALUE{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if args[1] == nil {
					return fmt.Errorf("NTH_VALUE: constant integer expression must be not null value")
				}
				num, err := args[1].ToInt64()
				if err != nil {
					return err
				}
				return fn.Step(args[0], num, windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowLead() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_LEAD{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				var offset int64 = 1
				if len(args) >= 2 {
					if args[1] == nil {
						return fmt.Errorf("LEAD: offset is must be not null value")
					}
					v, err := args[1].ToInt64()
					if err != nil {
						return err
					}
					offset = v
				}
				if offset < 0 {
					return fmt.Errorf("LEAD: offset is must be positive value %d", offset)
				}
				var defaultValue Value
				if len(args) == 3 {
					defaultValue = args[2]
				}
				return fn.Step(args[0], offset, defaultValue, windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowLag() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_LAG{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				var offset int64 = 1
				if len(args) >= 2 {
					if args[1] == nil {
						return fmt.Errorf("LAG: offset is must be not null value")
					}
					v, err := args[1].ToInt64()
					if err != nil {
						return err
					}
					offset = v
				}
				if offset < 0 {
					return fmt.Errorf("LAG: offset is must be positive value %d", offset)
				}
				var defaultValue Value
				if len(args) == 3 {
					defaultValue = args[2]
				}
				return fn.Step(args[0], offset, defaultValue, windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowPercentileCont() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_PERCENTILE_CONT{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], args[1], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowPercentileDisc() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_PERCENTILE_DISC{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(args[0], args[1], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowRank() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_RANK{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowDenseRank() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_DENSE_RANK{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowPercentRank() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_PERCENT_RANK{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowCumeDist() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_CUME_DIST{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowNtile() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_NTILE{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if args[0] == nil {
					return fmt.Errorf("NTILE: constant integer expression must be not null value")
				}
				num, err := args[0].ToInt64()
				if err != nil {
					return err
				}
				if num <= 0 {
					return fmt.Errorf("NTILE: constant integer expression must be positive value")
				}
				return fn.Step(num, windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}

func bindWindowRowNumber() func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_ROW_NUMBER{}
		return newWindowAggregator(
			func(args []Value, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
		)
	}
}
