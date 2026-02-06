package internal

import (
	"fmt"
	"sync"

	"github.com/goccy/go-json"
	"github.com/mattn/go-sqlite3"
)

var normalFuncs = []*FuncInfo{
	{Name: "add", BindFunc: bindAdd},
	{Name: "subtract", BindFunc: bindSub},
	{Name: "multiply", BindFunc: bindMul},
	{Name: "divide", BindFunc: bindOpDiv},
	{Name: "equal", BindFunc: bindEqual},
	{Name: "not_equal", BindFunc: bindNotEqual},
	{Name: "greater", BindFunc: bindGreater},
	{Name: "greater_or_equal", BindFunc: bindGreaterOrEqual},
	{Name: "less", BindFunc: bindLess},
	{Name: "less_or_equal", BindFunc: bindLessOrEqual},
	{Name: "bitwise_not", BindFunc: bindBitNot},
	{Name: "bitwise_left_shift", BindFunc: bindBitLeftShift},
	{Name: "bitwise_right_shift", BindFunc: bindBitRightShift},
	{Name: "bitwise_and", BindFunc: bindBitAnd},
	{Name: "bitwise_or", BindFunc: bindBitOr},
	{Name: "bitwise_xor", BindFunc: bindBitXor},
	{Name: "in_array", BindFunc: bindInArray},
	{Name: "get_struct_field", BindFunc: bindStructField},
	{Name: "get_json_field", BindFunc: bindJsonField},
	{Name: "subscript", BindFunc: bindSubscript},
	{Name: "array_at_offset", BindFunc: bindArrayAtOffset},
	{Name: "array_at_ordinal", BindFunc: bindArrayAtOrdinal},
	{Name: "safe_array_at_offset", BindFunc: bindSafeArrayAtOffset},
	{Name: "safe_array_at_ordinal", BindFunc: bindSafeArrayAtOrdinal},
	{Name: "is_distinct_from", BindFunc: bindIsDistinctFrom},
	{Name: "is_not_distinct_from", BindFunc: bindIsNotDistinctFrom},

	// security functions
	{Name: "session_user", BindFunc: bindSessionUser},

	// uuid functions
	{Name: "generate_uuid", BindFunc: bindGenerateUUID},

	// debugging functions
	{Name: "error", BindFunc: bindError},

	// date functions
	{Name: "current_date", BindFunc: bindCurrentDate},
	{Name: "extract", BindFunc: bindExtract},
	{Name: "extract_date", BindFunc: bindExtractDate},
	{Name: "date", BindFunc: bindDate},
	{Name: "date_add", BindFunc: bindDateAdd},
	{Name: "date_sub", BindFunc: bindDateSub},
	{Name: "date_diff", BindFunc: bindDateDiff},
	{Name: "date_trunc", BindFunc: bindDateTrunc},
	{Name: "date_from_unix_date", BindFunc: bindDateFromUnixDate},
	{Name: "format_date", BindFunc: bindFormatDate},
	{Name: "last_day", BindFunc: bindLastDay},
	{Name: "parse_date", BindFunc: bindParseDate},
	{Name: "unix_date", BindFunc: bindUnixDate},

	// datetime functions
	{Name: "current_datetime", BindFunc: bindCurrentDatetime},
	{Name: "datetime", BindFunc: bindDatetime},
	{Name: "datetime_add", BindFunc: bindDatetimeAdd},
	{Name: "datetime_sub", BindFunc: bindDatetimeSub},
	{Name: "datetime_diff", BindFunc: bindDatetimeDiff},
	{Name: "datetime_trunc", BindFunc: bindDatetimeTrunc},
	{Name: "format_datetime", BindFunc: bindFormatDatetime},
	{Name: "parse_datetime", BindFunc: bindParseDatetime},

	// time functions
	{Name: "current_time", BindFunc: bindCurrentTime},
	{Name: "time", BindFunc: bindTime},
	{Name: "time_add", BindFunc: bindTimeAdd},
	{Name: "time_sub", BindFunc: bindTimeSub},
	{Name: "time_diff", BindFunc: bindTimeDiff},
	{Name: "time_trunc", BindFunc: bindTimeTrunc},
	{Name: "format_time", BindFunc: bindFormatTime},
	{Name: "parse_time", BindFunc: bindParseTime},

	// timestamp functions
	{Name: "current_timestamp", BindFunc: bindCurrentTimestamp},
	{Name: "string", BindFunc: bindString},
	{Name: "timestamp", BindFunc: bindTimestamp},
	{Name: "timestamp_add", BindFunc: bindTimestampAdd},
	{Name: "timestamp_sub", BindFunc: bindTimestampSub},
	{Name: "timestamp_diff", BindFunc: bindTimestampDiff},
	{Name: "timestamp_trunc", BindFunc: bindTimestampTrunc},
	{Name: "format_timestamp", BindFunc: bindFormatTimestamp},
	{Name: "parse_timestamp", BindFunc: bindParseTimestamp},
	{Name: "timestamp_seconds", BindFunc: bindTimestampSeconds},
	{Name: "timestamp_millis", BindFunc: bindTimestampMillis},
	{Name: "timestamp_micros", BindFunc: bindTimestampMicros},
	{Name: "unix_seconds", BindFunc: bindUnixSeconds},
	{Name: "unix_millis", BindFunc: bindUnixMillis},
	{Name: "unix_micros", BindFunc: bindUnixMicros},
	{Name: "like", BindFunc: bindLike},
	{Name: "between", BindFunc: bindBetween},
	{Name: "in", BindFunc: bindIn},
	{Name: "is_null", BindFunc: bindIsNull},
	{Name: "is_true", BindFunc: bindIsTrue},
	{Name: "is_false", BindFunc: bindIsFalse},
	{Name: "not", BindFunc: bindNot},
	{Name: "and", BindFunc: bindAnd},
	{Name: "or", BindFunc: bindOr},
	{Name: "coalesce", BindFunc: bindCoalesce},
	{Name: "if", BindFunc: bindIf},
	{Name: "ifnull", BindFunc: bindIfNull},
	{Name: "nullif", BindFunc: bindNullIf},
	{Name: "length", BindFunc: bindLength},
	{Name: "cast", BindFunc: bindCast},

	// interval functions
	{Name: "interval", BindFunc: bindInterval},
	{Name: "make_interval", BindFunc: bindMakeInterval},
	{Name: "justify_days", BindFunc: bindJustifyDays},
	{Name: "justify_hours", BindFunc: bindJustifyHours},
	{Name: "justify_interval", BindFunc: bindJustifyInterval},

	// geography functions
	{Name: "st_geogpoint", BindFunc: bindStGeogPoint},
	{Name: "st_geogfromtext", BindFunc: bindStGeogFromText},
	{Name: "st_distance", BindFunc: bindStDistance},

	// numeric/bignumeric functions
	{Name: "parse_numeric", BindFunc: bindParseNumeric},
	{Name: "parse_bignumeric", BindFunc: bindParseBigNumeric},

	// hash functions
	{Name: "farm_fingerprint", BindFunc: bindFarmFingerprint},
	{Name: "md5", BindFunc: bindMD5},
	{Name: "sha1", BindFunc: bindSha1},
	{Name: "sha256", BindFunc: bindSha256},
	{Name: "sha512", BindFunc: bindSha512},

	// string functions
	{Name: "ascii", BindFunc: bindAscii},
	{Name: "byte_length", BindFunc: bindByteLength},
	{Name: "char_length", BindFunc: bindCharLength},
	{Name: "chr", BindFunc: bindChr},
	{Name: "code_points_to_bytes", BindFunc: bindCodePointsToBytes},
	{Name: "code_points_to_string", BindFunc: bindCodePointsToString},
	{Name: "collate", BindFunc: bindCollate},
	{Name: "concat", BindFunc: bindConcat},
	{Name: "contains_substr", BindFunc: bindContainsSubstr},
	{Name: "ends_with", BindFunc: bindEndsWith},
	{Name: "format", BindFunc: bindFormat},
	{Name: "from_base32", BindFunc: bindFromBase32},
	{Name: "from_base64", BindFunc: bindFromBase64},
	{Name: "from_hex", BindFunc: bindFromHex},
	{Name: "initcap", BindFunc: bindInitcap},
	{Name: "instr", BindFunc: bindInstr},
	{Name: "left", BindFunc: bindLeft},
	{Name: "length", BindFunc: bindLength},
	{Name: "lpad", BindFunc: bindLpad},
	{Name: "lower", BindFunc: bindLower},
	{Name: "ltrim", BindFunc: bindLtrim},
	{Name: "normalize", BindFunc: bindNormalize},
	{Name: "normalize_and_casefold", BindFunc: bindNormalizeAndCasefold},
	{Name: "regexp_contains", BindFunc: bindRegexpContains},
	{Name: "regexp_extract", BindFunc: bindRegexpExtract},
	{Name: "regexp_extract_all", BindFunc: bindRegexpExtractAll},
	{Name: "regexp_instr", BindFunc: bindRegexpInstr},
	{Name: "regexp_replace", BindFunc: bindRegexpReplace},
	{Name: "replace", BindFunc: bindReplace},
	{Name: "repeat", BindFunc: bindRepeat},
	{Name: "reverse", BindFunc: bindReverse},
	{Name: "right", BindFunc: bindRight},
	{Name: "rpad", BindFunc: bindRpad},
	{Name: "rtrim", BindFunc: bindRtrim},
	{Name: "safe_convert_bytes_to_string", BindFunc: bindSafeConvertBytesToString},
	{Name: "soundex", BindFunc: bindSoundex},
	{Name: "split", BindFunc: bindSplit},
	{Name: "starts_with", BindFunc: bindStartsWith},
	{Name: "strpos", BindFunc: bindStrpos},
	{Name: "substr", BindFunc: bindSubstr},
	{Name: "to_base32", BindFunc: bindToBase32},
	{Name: "to_base64", BindFunc: bindToBase64},
	{Name: "to_code_points", BindFunc: bindToCodePoints},
	{Name: "to_hex", BindFunc: bindToHex},
	{Name: "translate", BindFunc: bindTranslate},
	{Name: "trim", BindFunc: bindTrim},
	{Name: "unicode", BindFunc: bindUnicode},
	{Name: "upper", BindFunc: bindUpper},

	// json functions
	{Name: "json_extract", BindFunc: bindJsonExtract},
	{Name: "json_extract_scalar", BindFunc: bindJsonExtractScalar},
	{Name: "json_extract_array", BindFunc: bindJsonExtractArray},
	{Name: "json_extract_string_array", BindFunc: bindJsonExtractStringArray},
	{Name: "json_query", BindFunc: bindJsonQuery},
	{Name: "json_value", BindFunc: bindJsonValue},
	{Name: "json_query_array", BindFunc: bindJsonQueryArray},
	{Name: "json_value_array", BindFunc: bindJsonValueArray},
	{Name: "parse_json", BindFunc: bindParseJson},
	{Name: "to_json", BindFunc: bindToJson},
	{Name: "to_json_string", BindFunc: bindToJsonString},
	{Name: "bool", BindFunc: bindBool},
	{Name: "int64", BindFunc: bindInt64},
	{Name: "double", BindFunc: bindDouble},
	{Name: "json_type", BindFunc: bindJsonType},

	// math functions

	{Name: "abs", BindFunc: bindAbs},
	{Name: "sign", BindFunc: bindSign},
	{Name: "is_inf", BindFunc: bindIsInf},
	{Name: "is_nan", BindFunc: bindIsNaN},
	{Name: "ieee_divide", BindFunc: bindIEEEDivide},
	{Name: "rand", BindFunc: bindRand},
	{Name: "sqrt", BindFunc: bindSqrt},
	{Name: "pow", BindFunc: bindPow},
	{Name: "power", BindFunc: bindPow},
	{Name: "exp", BindFunc: bindExp},
	{Name: "ln", BindFunc: bindLn},
	{Name: "log", BindFunc: bindLog},
	{Name: "log10", BindFunc: bindLog10},
	{Name: "greatest", BindFunc: bindGreatest},
	{Name: "least", BindFunc: bindLeast},
	{Name: "div", BindFunc: bindDiv},
	{Name: "safe_divide", BindFunc: bindSafeDivide},
	{Name: "safe_multiply", BindFunc: bindSafeMultiply},
	{Name: "safe_negate", BindFunc: bindSafeNegate},
	{Name: "safe_add", BindFunc: bindSafeAdd},
	{Name: "safe_subtract", BindFunc: bindSafeSubtract},
	{Name: "mod", BindFunc: bindMod},
	{Name: "round", BindFunc: bindRound},
	{Name: "trunc", BindFunc: bindTrunc},
	{Name: "ceil", BindFunc: bindCeil},
	{Name: "ceiling", BindFunc: bindCeil},
	{Name: "floor", BindFunc: bindFloor},
	{Name: "cos", BindFunc: bindCos},
	{Name: "cosh", BindFunc: bindCosh},
	{Name: "acos", BindFunc: bindAcos},
	{Name: "acosh", BindFunc: bindAcosh},
	{Name: "sin", BindFunc: bindSin},
	{Name: "sinh", BindFunc: bindSinh},
	{Name: "asin", BindFunc: bindAsin},
	{Name: "asinh", BindFunc: bindAsinh},
	{Name: "tan", BindFunc: bindTan},
	{Name: "tanh", BindFunc: bindTanh},
	{Name: "atan", BindFunc: bindAtan},
	{Name: "atanh", BindFunc: bindAtanh},
	{Name: "atan2", BindFunc: bindAtan2},
	{Name: "range_bucket", BindFunc: bindRangeBucket},

	// array functions
	{Name: "array_concat", BindFunc: bindArrayConcat},
	{Name: "array_length", BindFunc: bindArrayLength},
	{Name: "array_to_string", BindFunc: bindArrayToString},
	{Name: "generate_array", BindFunc: bindGenerateArray},
	{Name: "generate_date_array", BindFunc: bindGenerateDateArray},
	{Name: "generate_timestamp_array", BindFunc: bindGenerateTimestampArray},
	{Name: "array_reverse", BindFunc: bindArrayReverse},
	{Name: "make_array", BindFunc: bindMakeArray},
	{Name: "make_struct", BindFunc: bindMakeStruct},

	// hyperloglog++ functions
	{Name: "hll_count_extract", BindFunc: bindHllCountExtract},

	// bit functions
	{Name: "bit_count", BindFunc: bindBitCount},

	// aggregate option funcs
	{Name: "distinct", BindFunc: bindDistinct},
	{Name: "limit", BindFunc: bindLimit},
	{Name: "order_by", BindFunc: bindOrderBy},
	{Name: "ignore_nulls", BindFunc: bindIgnoreNulls},

	// window option funcs
	{Name: "window_frame_unit", BindFunc: bindWindowFrameUnit},
	{Name: "window_partition", BindFunc: bindWindowPartition},
	{Name: "window_boundary_start", BindFunc: bindWindowBoundaryStart},
	{Name: "window_boundary_end", BindFunc: bindWindowBoundaryEnd},
	{Name: "window_rowid", BindFunc: bindWindowRowID},
	{Name: "window_order_by", BindFunc: bindWindowOrderBy},

	// javascript funcs
	{Name: "eval_javascript", BindFunc: bindEvalJavaScript},

	// net funcs
	{Name: "net_host", BindFunc: bindNetHost},
	{Name: "net_ip_from_string", BindFunc: bindNetIpFromString},
	{Name: "net_ip_net_mask", BindFunc: bindNetIpNetMask},
	{Name: "net_ip_to_string", BindFunc: bindNetIpToString},
	{Name: "net_ip_trunc", BindFunc: bindNetIpTrunc},
	{Name: "net_ipv4_from_int64", BindFunc: bindNetIpv4FromInt64},
	{Name: "net_ipv4_to_int64", BindFunc: bindNetIpv4ToInt64},
	{Name: "net_public_suffix", BindFunc: bindNetPublicSuffix},
	{Name: "net_reg_domain", BindFunc: bindNetRegDomain},
	{Name: "net_safe_ip_from_string", BindFunc: bindNetSafeIpFromString},
}

var aggregateFuncs = []*AggregateFuncInfo{
	{Name: "array", BindFunc: bindArray},

	// aggregate functions
	{Name: "any_value", BindFunc: bindAnyValue},
	{Name: "array_agg", BindFunc: bindArrayAgg},
	{Name: "array_concat_agg", BindFunc: bindArrayConcatAgg},
	{Name: "avg", BindFunc: bindAvg},
	{Name: "count", BindFunc: bindCount},
	{Name: "count_star", BindFunc: bindCountStar},
	{Name: "bit_and", BindFunc: bindBitAndAgg},
	{Name: "bit_or", BindFunc: bindBitOrAgg},
	{Name: "bit_xor", BindFunc: bindBitXorAgg},
	{Name: "countif", BindFunc: bindCountIf},
	{Name: "logical_and", BindFunc: bindLogicalAnd},
	{Name: "logical_or", BindFunc: bindLogicalOr},
	{Name: "max", BindFunc: bindMax},
	{Name: "min", BindFunc: bindMin},
	{Name: "string_agg", BindFunc: bindStringAgg},
	{Name: "sum", BindFunc: bindSum},

	// statistical aggregate functions
	{Name: "corr", BindFunc: bindCorr},
	{Name: "covar_pop", BindFunc: bindCovarPop},
	{Name: "covar_samp", BindFunc: bindCovarSamp},
	{Name: "stddev_pop", BindFunc: bindStddevPop},
	{Name: "stddev_samp", BindFunc: bindStddevSamp},
	{Name: "stddev", BindFunc: bindStddev},
	{Name: "var_pop", BindFunc: bindVarPop},
	{Name: "var_samp", BindFunc: bindVarSamp},
	{Name: "variance", BindFunc: bindVariance},

	// approximate aggregate functions
	{Name: "approx_count_distinct", BindFunc: bindApproxCountDistinct},
	{Name: "approx_quantiles", BindFunc: bindApproxQuantiles},
	{Name: "approx_top_count", BindFunc: bindApproxTopCount},
	{Name: "approx_top_sum", BindFunc: bindApproxTopSum},

	// hyperloglog++ functions
	{Name: "hll_count_init", BindFunc: bindHllCountInit},
	{Name: "hll_count_merge", BindFunc: bindHllCountMerge},
	{Name: "hll_count_merge_partial", BindFunc: bindHllCountMergePartial},
}

var windowFuncs = []*WindowFuncInfo{
	// aggregate functions
	{Name: "any_value", BindFunc: bindWindowAnyValue},
	{Name: "array_agg", BindFunc: bindWindowArrayAgg},
	{Name: "avg", BindFunc: bindWindowAvg},
	{Name: "count", BindFunc: bindWindowCount},
	{Name: "count_star", BindFunc: bindWindowCountStar},
	{Name: "countif", BindFunc: bindWindowCountIf},
	{Name: "max", BindFunc: bindWindowMax},
	{Name: "min", BindFunc: bindWindowMin},
	{Name: "string_agg", BindFunc: bindWindowStringAgg},
	{Name: "sum", BindFunc: bindWindowSum},

	// statistical aggregate functions
	{Name: "corr", BindFunc: bindWindowCorr},
	{Name: "covar_pop", BindFunc: bindWindowCovarPop},
	{Name: "covar_samp", BindFunc: bindWindowCovarSamp},
	{Name: "stddev_pop", BindFunc: bindWindowStddevPop},
	{Name: "stddev_samp", BindFunc: bindWindowStddevSamp},
	{Name: "stddev", BindFunc: bindWindowStddev},
	{Name: "var_pop", BindFunc: bindWindowVarPop},
	{Name: "var_samp", BindFunc: bindWindowVarSamp},
	{Name: "variance", BindFunc: bindWindowVariance},

	// navigation functions
	{Name: "first_value", BindFunc: bindWindowFirstValue},
	{Name: "last_value", BindFunc: bindWindowLastValue},
	{Name: "nth_value", BindFunc: bindWindowNthValue},
	{Name: "lead", BindFunc: bindWindowLead},
	{Name: "lag", BindFunc: bindWindowLag},
	{Name: "percentile_cont", BindFunc: bindWindowPercentileCont},
	{Name: "percentile_disc", BindFunc: bindWindowPercentileDisc},

	// numbering functions
	{Name: "rank", BindFunc: bindWindowRank},
	{Name: "dense_rank", BindFunc: bindWindowDenseRank},
	{Name: "percent_rank", BindFunc: bindWindowPercentRank},
	{Name: "cume_dist", BindFunc: bindWindowCumeDist},
	{Name: "ntile", BindFunc: bindWindowNtile},
	{Name: "row_number", BindFunc: bindWindowRowNumber},
}

type NameAndFunc struct {
	Name string
	Func interface{}
}

var (
	funcMapMu          sync.RWMutex
	registerFuncOnce   sync.Once
	normalFuncMap      = map[string][]*NameAndFunc{}
	aggregateFuncMap   = map[string][]*NameAndFunc{}
	windowFuncMap      = map[string][]*NameAndFunc{}
	currentTimeFuncMap = map[string]struct{}{
		"current_date":      {},
		"current_datetime":  {},
		"current_time":      {},
		"current_timestamp": {},
	}
)

func RegisterFunctions(conn *sqlite3.SQLiteConn) error {
	funcMapMu.RLock()
	defer funcMapMu.RUnlock()

	var onceErr error
	registerFuncOnce.Do(func() {
		for _, info := range normalFuncs {
			setupNormalFuncMap(info)
		}
		for _, info := range aggregateFuncs {
			setupAggregateFuncMap(info)
		}
		for _, info := range windowFuncs {
			setupWindowFuncMap(info)
		}
	})
	if onceErr != nil {
		return onceErr
	}

	if err := conn.RegisterFunc("zetasqlite_decode_array", func(v interface{}) (string, error) {
		decoded, err := DecodeValue(v)
		if err != nil {
			return "", err
		}
		if decoded == nil {
			return "[]", nil
		}
		array, err := decoded.ToArray()
		if err != nil {
			return "", err
		}
		encodedValues := make([]interface{}, 0, len(array.values))
		for _, value := range array.values {
			v, err := EncodeValue(value)
			if err != nil {
				return "", err
			}
			encodedValues = append(encodedValues, v)
		}
		b, err := json.Marshal(encodedValues)
		if err != nil {
			return "", err
		}
		return string(b), err
	}, true); err != nil {
		return fmt.Errorf("failed to register decode_array function: %w", err)
	}

	if err := conn.RegisterFunc("zetasqlite_group_by", func(v interface{}) (interface{}, error) {
		decoded, err := DecodeValue(v)
		if err != nil {
			return "", err
		}
		if decoded == nil {
			return nil, nil
		}
		return decoded.Interface(), nil
	}, true); err != nil {
		return fmt.Errorf("failed to register group_by function: %w", err)
	}

	if err := conn.RegisterCollation("zetasqlite_collate", func(a, b string) int {
		va, _ := DecodeValue(a)
		vb, _ := DecodeValue(b)
		eq, _ := va.EQ(vb)
		if eq {
			return 0
		}
		cond, _ := va.GT(vb)
		if cond {
			return 1
		}
		return -1
	}); err != nil {
		return fmt.Errorf("failed to register collate function: %w", err)
	}

	for _, values := range normalFuncMap {
		for _, v := range values {
			if err := conn.RegisterFunc(v.Name, v.Func, true); err != nil {
				return fmt.Errorf("failed to register function %s: %w", v.Name, err)
			}
		}
	}
	for _, values := range aggregateFuncMap {
		for _, v := range values {
			if err := conn.RegisterAggregator(v.Name, v.Func, true); err != nil {
				return fmt.Errorf("failed to register aggregate function %s: %w", v.Name, err)
			}
		}
	}
	for _, values := range windowFuncMap {
		for _, v := range values {
			if err := conn.RegisterAggregator(v.Name, v.Func, true); err != nil {
				return fmt.Errorf("failed to register window function %s: %w", v.Name, err)
			}
		}
	}
	return nil
}

func setupNormalFuncMap(info *FuncInfo) {
	normalFuncMap[info.Name] = append(normalFuncMap[info.Name], &NameAndFunc{
		Name: fmt.Sprintf("zetasqlite_%s", info.Name),
		Func: func(args ...interface{}) (interface{}, error) {
			values, err := convertArgs(args...)
			if err != nil {
				return nil, err
			}
			ret, err := info.BindFunc(values...)
			if err != nil {
				return nil, err
			}
			return EncodeValue(ret)
		},
	}, &NameAndFunc{
		Name: fmt.Sprintf("zetasqlite_safe_%s", info.Name),
		Func: func(args ...interface{}) (interface{}, error) {
			values, err := convertArgs(args...)
			if err != nil {
				return nil, err
			}
			ret, err := info.BindFunc(values...)
			if err != nil {
				// Note, this should only suppress semantic errors based on the
				// input data. See
				// https://github.com/google/zetasql/blob/master/docs/resolved_ast.md#resolvedfunctioncallbase
				return nil, nil
			}
			return EncodeValue(ret)
		},
	})
}

func setupAggregateFuncMap(info *AggregateFuncInfo) {
	aggregateFuncMap[info.Name] = append(aggregateFuncMap[info.Name], &NameAndFunc{
		Name: fmt.Sprintf("zetasqlite_%s", info.Name),
		Func: info.BindFunc(),
	})
}

func setupWindowFuncMap(info *WindowFuncInfo) {
	windowFuncMap[info.Name] = append(windowFuncMap[info.Name], &NameAndFunc{
		Name: fmt.Sprintf("zetasqlite_window_%s", info.Name),
		Func: info.BindFunc(),
	})
}
