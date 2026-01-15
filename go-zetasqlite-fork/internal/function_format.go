package internal

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/goccy/go-json"
)

type FormatInfo struct {
	validate func(Value) error
	parse    func(*FormatParam, []Value) ([]rune, error)
}

var formatSpecifierTable = map[rune]*FormatInfo{
	'd': {
		validate: validateDecimalInteger,
		parse:    parseInteger,
	},
	'i': {
		validate: validateDecimalInteger,
		parse:    parseInteger,
	},
	'o': {
		validate: validateOctal,
		parse:    parseInteger,
	},
	'x': {
		validate: validateHexInteger,
		parse:    parseInteger,
	},
	'X': {
		validate: validateHexInteger,
		parse:    parseInteger,
	},
	'f': {
		validate: validateDecimalNotation,
		parse:    parseFloat,
	},
	'F': {
		validate: validateDecimalNotation,
		parse:    parseFloat,
	},
	'e': {
		validate: validateScientificNotation,
		parse:    parseFloat,
	},
	'E': {
		validate: validateScientificNotation,
		parse:    parseFloat,
	},
	'g': {
		validate: validateDecimalOrScientificNotation,
		parse:    parseFloat,
	},
	'G': {
		validate: validateDecimalOrScientificNotation,
		parse:    parseFloat,
	},
	'p': {
		validate: validateOneLineJSON,
		parse:    parseOneLineJSON,
	},
	'P': {
		validate: validateMultiLineJSON,
		parse:    parseMultiLineJSON,
	},
	's': {
		validate: validateString,
		parse:    parseString,
	},
	't': {
		validate: validatePrintableString,
		parse:    parsePrintableString,
	},
	'T': {
		validate: validatePrintableString,
		parse:    parsePrintableString,
	},
	'%': {
		validate: validatePercent,
		parse:    parsePercent,
	},
}

func validateDecimalInteger(arg Value) error {
	if _, ok := arg.(IntValue); !ok {
		return fmt.Errorf("decimal integer format (%%d or %%i) required int64 type")
	}
	return nil
}

func validateOctal(arg Value) error {
	if _, ok := arg.(IntValue); !ok {
		return fmt.Errorf("octal format (%%o) required int64 type")
	}
	i64, _ := arg.ToInt64()
	if i64 < 0 {
		return fmt.Errorf("octal format (%%o) required positive value")
	}
	return nil
}

func validateHexInteger(arg Value) error {
	if _, ok := arg.(IntValue); !ok {
		return fmt.Errorf("hexadecimal integer format (%%x or %%X) required int64 type")
	}
	i64, _ := arg.ToInt64()
	if i64 < 0 {
		return fmt.Errorf("hexadecimal integer format (%%x or %%X) required positive value")
	}
	return nil
}

func validateDecimalNotation(arg Value) error {
	switch arg.(type) {
	case *NumericValue, FloatValue:
		return nil
	}
	return fmt.Errorf("decimal notation format (%%f or %%F) required float64 or numeric or bignumeric type")
}

func validateScientificNotation(arg Value) error {
	switch arg.(type) {
	case *NumericValue, FloatValue:
		return nil
	}
	return fmt.Errorf("scientific notation format (%%e or %%E) required float64 or numeric or bignumeric type")
}

func validateDecimalOrScientificNotation(arg Value) error {
	switch arg.(type) {
	case *NumericValue, FloatValue:
		return nil
	}
	return fmt.Errorf("decimal or scientific notation format (%%g or %%G) required float64 or numeric or bignumeric type")
}

func validateOneLineJSON(arg Value) error {
	if _, ok := arg.(JsonValue); !ok {
		return fmt.Errorf("one-line printable string format (%%p) required json type")
	}
	return nil
}

func validateMultiLineJSON(arg Value) error {
	if _, ok := arg.(JsonValue); !ok {
		return fmt.Errorf("multi-line printable string format (%%P) required json type")
	}
	return nil
}

func validateString(arg Value) error {
	if _, ok := arg.(StringValue); !ok {
		return fmt.Errorf("string of characters format (%%s) required string type")
	}
	return nil
}

func validatePrintableString(arg Value) error {
	if _, err := arg.ToString(); err != nil {
		return fmt.Errorf("printable string format (%%t): %w", err)
	}
	return nil
}

func validatePercent(arg Value) error {
	return nil
}

func parseInteger(param *FormatParam, args []Value) ([]rune, error) {
	format := "%"
	if param.flag == FormatFlagZero {
		format += "0"
	}
	var width int
	width, args = param.width.format(args)
	if width > 0 {
		format += fmt.Sprint(width)
	}
	v, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	switch param.flag {
	case FormatFlagPlus:
		if v > 0 {
			format = "+" + format
		}
	case FormatFlagSpace:
		format = " " + format
	case FormatFlagQuote:
		if param.specifier != 'd' && param.specifier != 'i' {
			return nil, fmt.Errorf("currently doesn't support ' flag for %c specifier", param.specifier)
		}
		text := []rune(fmt.Sprint(v))
		commaCount := len(text) / 3
		if len(text)%3 == 0 {
			commaCount -= 1
		}
		numWithComma := make([]rune, len(text)+commaCount)
		targetIdx := len(text) + commaCount - 1
		charCount := 0
		for i := len(text) - 1; i >= 0; i-- {
			numWithComma[targetIdx] = text[i]
			charCount++
			if charCount%3 == 0 {
				targetIdx--
				if targetIdx >= 0 {
					numWithComma[targetIdx] = ','
				}
			}
			targetIdx--
		}
		return numWithComma, nil
	case FormatFlagMinus:
		return nil, fmt.Errorf("currently doesn't support - flag for integer value")
	case FormatFlagSharp:
		return nil, fmt.Errorf("currently doesn't support # flag for integer value")
	}
	integerFmt := param.specifier
	if integerFmt == 'i' {
		integerFmt = 'd'
	}
	format += string(integerFmt)
	ret := fmt.Sprintf(format, v)
	return []rune(ret), nil
}

func parseFloat(param *FormatParam, args []Value) ([]rune, error) {
	var (
		width, precision int
	)
	width, args = param.width.format(args)
	precision, args = param.precision.format(args)
	v, err := args[0].ToFloat64()
	if err != nil {
		return nil, err
	}
	floatFmt := param.specifier
	if floatFmt == 'F' {
		floatFmt = 'f'
	}
	format := strconv.FormatFloat(v, byte(floatFmt), precision, 64)
	remain := width - len(format)
	if remain > 0 {
		if param.flag == FormatFlagZero {
			format = strings.Repeat("0", remain) + format
		} else {
			format = strings.Repeat(" ", remain) + format
		}
	}
	switch param.flag {
	case FormatFlagPlus:
		if v > 0 {
			format = "+" + format
		}
	case FormatFlagSpace:
		format = " " + format
	case FormatFlagMinus:
		return nil, fmt.Errorf("currently doesn't support - flag for float value")
	case FormatFlagSharp:
		return nil, fmt.Errorf("currently doesn't support # flag for float value")
	case FormatFlagQuote:
		return nil, fmt.Errorf("currently doesn't support ' flag for float value")
	}
	return []rune(format), nil
}

func parseOneLineJSON(param *FormatParam, args []Value) ([]rune, error) {
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(v)); err != nil {
		return nil, err
	}
	return []rune(buf.String()), nil
}

func parseMultiLineJSON(param *FormatParam, args []Value) ([]rune, error) {
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, []byte(v), "", "  "); err != nil {
		return nil, err
	}
	return []rune(buf.String()), nil
}

func parseString(param *FormatParam, args []Value) ([]rune, error) {
	s, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return []rune(s), nil
}

func parsePrintableString(param *FormatParam, args []Value) ([]rune, error) {
	return []rune(args[0].Format(param.specifier)), nil
}

func parsePercent(param *FormatParam, args []Value) ([]rune, error) {
	return []rune{'%'}, nil
}

type FormatContext struct {
	src []rune
	idx int
}

func (c *FormatContext) current() rune {
	if len(c.src) > c.idx {
		return c.src[c.idx]
	}
	return rune(0)
}

//nolint:unparam
func (c *FormatContext) progress(num int) {
	c.idx += num
}

type FormatParam struct {
	flag      FormatFlag
	width     *FormatWidth
	precision *FormatPrecision
	specifier rune
}

func (p *FormatParam) requiredArgNum() int {
	if p.specifier == '%' {
		return 0
	}
	num := 1
	if p.width != nil && p.width.fromArg {
		num++
	}
	if p.precision != nil && p.precision.fromArg {
		num++
	}
	return num
}

func (p *FormatParam) validateArgs(info *FormatInfo, args []Value) error {
	if p.specifier == '%' {
		return nil
	}
	if p.width != nil && p.width.fromArg {
		if _, ok := args[0].(IntValue); !ok {
			return fmt.Errorf("width type required int64 value. but specified %T", args[0])
		}
		args = args[1:]
	}
	if p.precision != nil && p.precision.fromArg {
		if _, ok := args[0].(IntValue); !ok {
			return fmt.Errorf("precision type required int64 value. but specified %T", args[0])
		}
		args = args[1:]
	}
	return info.validate(args[0])
}

type FormatFlag int

const (
	FormatFlagNone  FormatFlag = 0
	FormatFlagMinus FormatFlag = 1
	FormatFlagPlus  FormatFlag = 2
	FormatFlagSpace FormatFlag = 3
	FormatFlagSharp FormatFlag = 4
	FormatFlagZero  FormatFlag = 5
	FormatFlagQuote FormatFlag = 6
)

type FormatWidth struct {
	num     int
	fromArg bool
}

func (w *FormatWidth) format(args []Value) (int, []Value) {
	if w == nil {
		return 0, args
	}
	if w.fromArg {
		width, _ := args[0].ToInt64()
		if width <= 0 {
			return 0, args[1:]
		}
		return int(width), args[1:]
	}
	if w.num <= 0 {
		return 0, args
	}
	return w.num, args
}

type FormatPrecision struct {
	num     int
	fromArg bool
}

func (p *FormatPrecision) format(args []Value) (int, []Value) {
	if p == nil {
		return 6, args
	}
	if p.fromArg {
		precision, _ := args[0].ToInt64()
		if precision <= 0 {
			return -1, args[1:]
		}
		return int(precision), args[1:]
	}
	if p.num <= 0 {
		return -1, args
	}
	return p.num, args
}

func parseFormat(format string, args ...Value) (string, error) {
	ctx := &FormatContext{src: []rune(format)}
	formatArgs := args
	result := []rune{}
	for ctx.idx < len(ctx.src) {
		c := ctx.current()
		if c != '%' {
			result = append(result, c)
			ctx.progress(1)
			continue
		}
		ctx.progress(1)
		if len(ctx.src) <= ctx.idx {
			return "", fmt.Errorf("invalid format")
		}
		flag := parseFormatFlag(ctx)
		width, err := parseFormatWidth(ctx)
		if err != nil {
			return "", err
		}
		precision, err := parseFormatPrecision(ctx)
		if err != nil {
			return "", err
		}
		specifier := ctx.current()
		param := &FormatParam{
			flag:      flag,
			width:     width,
			precision: precision,
			specifier: specifier,
		}
		info, exists := formatSpecifierTable[param.specifier]
		if !exists {
			return "", fmt.Errorf("unexpected format type %%%c", specifier)
		}
		num := param.requiredArgNum()
		if len(formatArgs) < num {
			return "", fmt.Errorf("not enough arguments for format")
		}
		args := formatArgs[:num]
		if err := param.validateArgs(info, args); err != nil {
			return "", fmt.Errorf("invalid argument type: %w", err)
		}
		text, err := info.parse(param, args)
		if err != nil {
			return "", err
		}
		if len(formatArgs) > num {
			formatArgs = formatArgs[num:]
		} else if num != 0 {
			formatArgs = nil
		}
		result = append(result, text...)
		ctx.progress(1)
	}
	return string(result), nil
}

func parseFormatFlag(ctx *FormatContext) FormatFlag {
	switch ctx.current() {
	case '-':
		ctx.progress(1)
		return FormatFlagMinus
	case '+':
		ctx.progress(1)
		return FormatFlagPlus
	case ' ':
		ctx.progress(1)
		return FormatFlagSpace
	case '#':
		ctx.progress(1)
		return FormatFlagSharp
	case '0':
		ctx.progress(1)
		return FormatFlagZero
	case '\'':
		ctx.progress(1)
		return FormatFlagQuote
	}
	return FormatFlagNone
}

func parseFormatWidth(ctx *FormatContext) (*FormatWidth, error) {
	start := ctx.idx
	end := start
	for {
		switch ctx.current() {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			ctx.progress(1)
			continue
		case '*':
			ctx.progress(1)
			return &FormatWidth{fromArg: true}, nil
		}
		end = ctx.idx
		break
	}
	if start != end {
		width := ctx.src[start:end]
		i64, err := strconv.ParseInt(string(width), 10, 64)
		if err != nil {
			return nil, err
		}
		return &FormatWidth{num: int(i64)}, nil
	}
	return nil, nil
}

func parseFormatPrecision(ctx *FormatContext) (*FormatPrecision, error) {
	if ctx.current() != '.' {
		return nil, nil
	}
	ctx.progress(1)
	start := ctx.idx
	end := start
	for {
		switch ctx.current() {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			ctx.progress(1)
			continue
		case '*':
			return &FormatPrecision{fromArg: true}, nil
		}
		end = ctx.idx
		break
	}
	if start != end {
		width := ctx.src[start:end]
		i64, err := strconv.ParseInt(string(width), 10, 64)
		if err != nil {
			return nil, err
		}
		return &FormatPrecision{num: int(i64)}, nil
	}
	return nil, nil
}
