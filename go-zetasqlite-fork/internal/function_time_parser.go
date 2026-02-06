package internal

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type DayOfWeek string

const (
	Sunday    DayOfWeek = "Sunday"
	Monday    DayOfWeek = "Monday"
	Tuesday   DayOfWeek = "Tuesday"
	Wednesday DayOfWeek = "Wednesday"
	Thursday  DayOfWeek = "Thursday"
	Friday    DayOfWeek = "Friday"
	Saturday  DayOfWeek = "Saturday"
)

type Month string

const (
	January   Month = "January"
	February  Month = "February"
	March     Month = "March"
	April     Month = "April"
	May       Month = "May"
	June      Month = "June"
	July      Month = "July"
	August    Month = "August"
	September Month = "September"
	October   Month = "October"
	November  Month = "November"
	December  Month = "December"
)

var (
	dayOfWeeks = []DayOfWeek{
		Sunday,
		Monday,
		Tuesday,
		Wednesday,
		Thursday,
		Friday,
		Saturday,
	}
	months = []Month{
		January,
		February,
		March,
		April,
		May,
		June,
		July,
		August,
		September,
		October,
		November,
		December,
	}
)

type TimeFormatType int

func (t TimeFormatType) String() string {
	switch t {
	case FormatTypeDate:
		return "date"
	case FormatTypeDatetime:
		return "datetime"
	case FormatTypeTime:
		return "time"
	case FormatTypeTimestamp:
		return "timestamp"
	}
	return "unknown"
}

const (
	FormatTypeDate      TimeFormatType = 0
	FormatTypeDatetime  TimeFormatType = 1
	FormatTypeTime      TimeFormatType = 2
	FormatTypeTimestamp TimeFormatType = 3
)

type ParseFunction func(text []rune, t *time.Time) (int, error)

type FormatTimeInfo struct {
	AvailableTypes []TimeFormatType
	Parse          ParseFunction
	Format         func(*time.Time) ([]rune, error)
}

type TimeParserPostProcessor struct {
	ShouldPostProcessResult func(map[rune][2]int) bool
	PostProcessResult       func([]rune, *time.Time)
}

func (i *FormatTimeInfo) Available(typ TimeFormatType) bool {
	for _, t := range i.AvailableTypes {
		if t == typ {
			return true
		}
	}
	return false
}

var formatPatternMap = map[rune]*FormatTimeInfo{
	'A': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekOfDayParser,
		Format: weekOfDayFormatter,
	},
	'a': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  shortWeekOfDayParser,
		Format: shortWeekOfDayFormatter,
	},
	'B': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  monthParser,
		Format: monthFormatter,
	},
	'b': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  shortMonthParser,
		Format: shortMonthFormatter,
	},
	'C': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  centuryParser,
		Format: centuryFormatter,
	},
	'c': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  ansicParser,
		Format: ansicFormatter,
	},
	'D': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  monthDayYearParser,
		Format: monthDayYearFormatter,
	},
	'd': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  dayParser,
		Format: dayFormatter,
	},
	'e': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  composeParseFunctions("day of month format", []ParseFunction{leadingSpaceAllowedParser, dayParser}),
		Format: dayFormatter,
	},
	'F': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse: composeParseFunctions("year-month-day format", []ParseFunction{
			yearParser,
			hyphenParser,
			monthNumberParser,
			hyphenParser,
			dayParser,
		}),
		Format: yearMonthDayFormatter,
	},
	'G': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  yearISOParser,
		Format: yearISOFormatter,
	},
	'g': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  centuryISOParser,
		Format: centuryISOFormatter,
	},
	'H': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hourParser,
		Format: hourFormatter,
	},
	'h': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  shortMonthParser,
		Format: shortMonthFormatter,
	},
	'I': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hour12Parser,
		Format: hour12Formatter,
	},
	'J': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  yearISOParser,
		Format: yearISOFormatter,
	},
	'j': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  dayOfYearParser,
		Format: dayOfYearFormatter,
	},
	'k': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  composeParseFunctions("24-hour clock hour", []ParseFunction{leadingSpaceAllowedParser, hourParser}),
		Format: hour24SpacePrecedingSingleDigitFormatter,
	},
	'l': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  composeParseFunctions("12-hour clock hour", []ParseFunction{leadingSpaceAllowedParser, hour12Parser}),
		Format: hour12SpacePrecedingSingleDigitFormatter,
	},
	'M': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  minuteParser,
		Format: minuteFormatter,
	},
	'm': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  monthNumberParser,
		Format: monthNumberFormatter,
	},
	'n': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTime, FormatTypeTimestamp,
		},
		Parse:  newLineParser,
		Format: newLineFormatter,
	},
	'P': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  smallAMPMParser,
		Format: smallAMPMFormatter,
	},
	'p': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  largeAMPMParser,
		Format: largeAMPMFormatter,
	},
	'Q': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  quarterParser,
		Format: quarterFormatter,
	},
	'R': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse: composeParseFunctions("hour:minute format", []ParseFunction{
			hourParser,
			colonParser,
			minuteParser,
		}),
		Format: hourMinuteFormatter,
	},
	'S': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  secondParser,
		Format: secondFormatter,
	},
	's': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  unixtimeSecondsParser,
		Format: unixtimeSecondsFormatter,
	},
	'T': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hourMinuteSecondParser,
		Format: hourMinuteSecondFormatter,
	},
	't': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  tabParser,
		Format: tabFormatter,
	},
	'U': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekOfYearParser,
		Format: weekOfYearFormatter,
	},
	'u': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekNumberParser,
		Format: weekNumberFormatter,
	},
	'V': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekOfYearISOParser,
		Format: weekOfYearISOFormatter,
	},
	'W': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekOfYearParser,
		Format: weekOfYearFormatter,
	},
	'w': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekNumberZeroBaseParser,
		Format: weekNumberZeroBaseFormatter,
	},
	'X': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hourMinuteSecondParser,
		Format: hourMinuteSecondFormatter,
	},
	'x': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  monthDayYearParser,
		Format: monthDayYearFormatter,
	},
	'Y': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  yearParser,
		Format: yearFormatter,
	},
	'y': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  yearWithoutCenturyParser,
		Format: yearWithoutCenturyFormatter,
	},
	'Z': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTimestamp,
		},
		Parse:  timeZoneParser,
		Format: timeZoneFormatter,
	},
	'z': {
		AvailableTypes: []TimeFormatType{
			FormatTypeTimestamp,
		},
		Parse:  timeZoneOffsetParser,
		Format: timeZoneOffsetFormatter,
	},
	'%': {
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTime, FormatTypeTimestamp,
		},
		Parse:  escapeParser,
		Format: escapeFormatter,
	},
}

var postProcessorPatternMap = map[rune]*TimeParserPostProcessor{
	'p': {
		ShouldPostProcessResult: ampmShouldPostProcessResult,
		PostProcessResult:       ampmPostProcessor,
	},
}

func createStaticTextParser(static string) ParseFunction {
	length := len(static)
	return func(text []rune, t *time.Time) (int, error) {
		if len(text) < length || string(text[:length]) != static {
			return 0, fmt.Errorf("[%s] not found", static)
		}
		return length, nil
	}
}

var hyphenParser = createStaticTextParser("-")
var colonParser = createStaticTextParser(":")
var slashParser = createStaticTextParser("/")

func composeParseFunctions(name string, parsers []ParseFunction) ParseFunction {
	return func(text []rune, t *time.Time) (int, error) {
		progress := 0
		for _, parser := range parsers {
			step, err := parser(text[progress:], t)
			if err != nil {
				return 0, fmt.Errorf("could not parse %s: %s after [%s]", name, err, string(text[:progress]))
			}
			progress += step
		}
		return progress, nil
	}
}
func weekOfDayParser(text []rune, t *time.Time) (int, error) {
	for _, dayOfWeek := range dayOfWeeks {
		if len(text) < len(dayOfWeek) {
			continue
		}
		src := strings.ToLower(string(dayOfWeek))
		dst := strings.ToLower(string(text[:len(dayOfWeek)]))
		if src == dst {
			return len(dayOfWeek), nil
		}
	}
	return 0, fmt.Errorf("unexpected day of week")
}

func weekOfDayFormatter(t *time.Time) ([]rune, error) {
	return []rune(dayOfWeeks[int(t.Weekday())]), nil
}

func shortWeekOfDayParser(text []rune, t *time.Time) (int, error) {
	const shortLen = 3
	if len(text) < shortLen {
		return 0, fmt.Errorf("unexpected short day of week")
	}

	for _, dayOfWeek := range dayOfWeeks {
		src := strings.ToLower(string(dayOfWeek))[:shortLen]
		dst := strings.ToLower(string(text[:shortLen]))
		if src == dst {
			return shortLen, nil
		}
	}
	return 0, fmt.Errorf("unexpected short day of week")
}

func shortWeekOfDayFormatter(t *time.Time) ([]rune, error) {
	const shortLen = 3
	return []rune(string(dayOfWeeks[int(t.Weekday())])[:shortLen]), nil
}

func monthParser(text []rune, t *time.Time) (int, error) {
	for monthIdx, month := range months {
		if len(text) < len(month) {
			continue
		}
		src := strings.ToLower(string(month))
		dst := strings.ToLower(string(text[:len(month)]))
		if src == dst {
			*t = time.Date(
				t.Year(),
				time.Month(monthIdx+1),
				t.Day(),
				t.Hour(),
				t.Minute(),
				t.Second(),
				t.Nanosecond(),
				t.Location(),
			)
			return len(month), nil
		}
	}
	return 0, fmt.Errorf("unexpected month")
}

func monthFormatter(t *time.Time) ([]rune, error) {
	monthIdx := int(t.Month())
	return []rune(months[monthIdx-1]), nil
}

func shortMonthParser(text []rune, t *time.Time) (int, error) {
	const shortLen = 3

	if len(text) < shortLen {
		return 0, fmt.Errorf("unexpected short month")
	}
	for monthIdx, month := range months {
		src := strings.ToLower(string(month))[:shortLen]
		dst := strings.ToLower(string(text[:shortLen]))
		if src == dst {
			*t = t.AddDate(0, monthIdx+1-int(t.Month()), 0)
			return shortLen, nil
		}
	}
	return 0, fmt.Errorf("unexpected short month")
}

func shortMonthFormatter(t *time.Time) ([]rune, error) {
	const shortLen = 3
	monthIdx := int(t.Month())
	return []rune(string(months[monthIdx-1])[:shortLen]), nil
}

func centuryParser(text []rune, t *time.Time) (int, error) {
	const centuryLen = 2
	if len(text) < centuryLen {
		return 0, fmt.Errorf("unexpected century number")
	}
	c, err := strconv.ParseInt(string(text[:centuryLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected century number")
	}
	if c < 0 {
		return 0, fmt.Errorf("invalid century number %d", c)
	}
	*t = time.Date(
		int(c*100-99),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
	return centuryLen, nil
}

func centuryFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprint(t.Year())[:2]), nil
}

func yearWithoutCenturyParser(text []rune, t *time.Time) (int, error) {
	progress, year, err := parseDigitRespectingOptionalPlaces(text, 0, 99)
	if err != nil {
		return 0, fmt.Errorf("could not parse year without century: %s", err)
	}
	if year >= 69 {
		year += 1900
	} else {
		year += 2000
	}
	*t = time.Date(
		int(year),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
	return progress, nil
}

func yearWithoutCenturyFormatter(t *time.Time) ([]rune, error) {
	year := t.Format("2006")
	return []rune(year[len(year)-2:]), nil
}

func ansicParser(text []rune, t *time.Time) (int, error) {
	v, err := time.Parse("Mon Jan 02 15:04:05 2006", string(text))
	if err != nil {
		return 0, err
	}
	*t = v
	return len(text), nil
}

func ansicFormatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("Mon Jan 02 15:04:05 2006")), nil
}

var monthDayYearParser = composeParseFunctions("month/day/year format", []ParseFunction{
	monthNumberParser,
	slashParser,
	dayParser,
	slashParser,
	yearWithoutCenturyParser,
})

func monthDayYearFormatter(t *time.Time) ([]rune, error) {
	year := fmt.Sprint(t.Year())
	return []rune(
		fmt.Sprintf(
			"%s/%s/%s",
			fmt.Sprintf("%02d", t.Month()),
			fmt.Sprintf("%02d", t.Day()),
			year[2:],
		),
	), nil
}

func dayParser(text []rune, t *time.Time) (int, error) {
	progress, days, err := parseDigitRespectingOptionalPlaces(text, 1, 31)
	if err != nil {
		return 0, fmt.Errorf("could not parse day number: %s", err)
	}
	*t = time.Date(
		t.Year(),
		t.Month(),
		int(days),
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
	return progress, nil
}

func dayFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Day())), nil
}

func yearMonthDayFormatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("2006-01-02")), nil
}

func yearISOParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented year ISO matcher")
}

func yearISOFormatter(t *time.Time) ([]rune, error) {
	year, _ := t.ISOWeek()
	return []rune(fmt.Sprint(year)), nil
}

func centuryISOParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented century ISO matcher")
}

func centuryISOFormatter(t *time.Time) ([]rune, error) {
	year, _ := t.ISOWeek()
	return []rune(fmt.Sprint(year)), nil
}

func hourParser(text []rune, t *time.Time) (int, error) {
	progress, h, err := parseDigitRespectingOptionalPlaces(text, 0, 23)
	if err != nil {
		return 0, fmt.Errorf("could not parse hour number: %s", err)
	}
	*t = time.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		int(h),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
	return progress, nil
}

func leadingSpaceAllowedParser(text []rune, t *time.Time) (int, error) {
	if len(text) < 2 {
		return 0, fmt.Errorf("text must be at least 2 characters long")
	}
	progress := 0
	if text[0] == ' ' {
		progress += 1
	}
	return progress, nil
}

func hour24SpacePrecedingSingleDigitFormatter(t *time.Time) ([]rune, error) {
	hour := []rune(fmt.Sprintf("%d", t.Hour()))
	if len(hour) == 1 {
		return []rune(fmt.Sprintf(" %s", string(hour))), nil
	}
	return hour, nil
}

func hour12SpacePrecedingSingleDigitFormatter(t *time.Time) ([]rune, error) {
	hour := []rune(fmt.Sprintf("%d", t.Hour()%12))
	if len(hour) == 1 {
		return []rune(fmt.Sprintf(" %s", string(hour))), nil
	}
	return hour, nil
}

func hourFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Hour())), nil
}

func hour12Parser(text []rune, t *time.Time) (int, error) {
	progress, h, err := parseDigitRespectingOptionalPlaces(text, 1, 12)
	if err != nil {
		return 0, fmt.Errorf("could not parse hour number: %s", err)
	}
	*t = time.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		int(h),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
	return progress, nil
}

func hour12Formatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Hour())), nil
}

func dayOfYearParser(text []rune, t *time.Time) (int, error) {
	progress, d, err := parseDigitRespectingOptionalPlaces(text, 1, 366)
	if err != nil {
		return 0, fmt.Errorf("could not parse day of year number: %s", err)
	}
	dayOfYear := int(d) - 1
	year := t.Year()
	stubDate := time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, dayOfYear)
	*t = time.Date(
		year,
		stubDate.Month(),
		stubDate.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
	return progress, nil
}

func dayOfYearFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprint(t.YearDay())), nil
}

func minuteParser(text []rune, t *time.Time) (int, error) {
	progress, m, err := parseDigitRespectingOptionalPlaces(text, 0, 59)
	if err != nil {
		return 0, fmt.Errorf("unexpected minute number: %s", err)
	}
	*t = time.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		int(m),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
	return progress, nil
}

func minuteFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Minute())), nil
}

func parseDigitRespectingOptionalPlaces(text []rune, minNumber int64, maxNumber int64) (int, int64, error) {
	// Given a target value of `minNumber` and `maxNumber`, parse the given text up to `maxNumber`'s places
	// If a non-digit character is encountered, consider the digit parsed and move on
	// e.g. ('3', 0, 99) == 3  ('03', 0, 99) == 3 ('04/', 0, 999) == 4

	textLen := len(text)
	places := len(fmt.Sprint(maxNumber))
	var parts []string
	if textLen == 0 {
		return 0, 0, fmt.Errorf("empty text")
	}

	// Format tokens require at least 1 character most `places` characters
	steps := places
	if textLen < places {
		steps = textLen
	}

	for i := 0; i < steps; i++ {
		char := string(text[i])
		_, err := strconv.ParseInt(char, 10, 64)

		// If we have encountered an error, we have encountered a non-digit
		if err != nil {
			// If we have not parsed any digits yet, the input text cannot be parsed
			if len(parts) == 0 {
				return 0, 0, fmt.Errorf("leading character is not a digit")
			}
			// If we already have parsed some digits, we assume the character was part of the format string (eg - or /)
			break
		}
		parts = append(parts, char)
	}

	result, err := strconv.ParseInt(strings.Join(parts, ""), 10, 64)

	// These parts have already been parsed/formatted once, we don't expect this error to occur, but must handle anyway
	if err != nil {
		return 0, 0, err
	}

	if result > maxNumber {
		return 0, 0, fmt.Errorf("part [%d] is greater than maximum value [%d]", result, maxNumber)
	}

	if result < minNumber {
		return 0, 0, fmt.Errorf("part [%d] is less than minimum value [%d]", result, minNumber)
	}

	return len(parts), result, nil
}

func monthNumberParser(text []rune, t *time.Time) (int, error) {
	progress, months, err := parseDigitRespectingOptionalPlaces(text, 1, 12)
	if err != nil {
		return 0, fmt.Errorf("could not parse month: %s", err)
	}
	*t = time.Date(
		t.Year(),
		time.Month(months),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
	return progress, nil
}

func monthNumberFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Month())), nil
}

func newLineParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented new line matcher")
}

func newLineFormatter(t *time.Time) ([]rune, error) {
	return []rune("\n"), nil
}

func smallAMPMParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("this cannot be used with parsing, instead, use %%p")
}

func smallAMPMFormatter(t *time.Time) ([]rune, error) {
	if t.Hour() < 12 {
		return []rune("am"), nil
	}
	return []rune("pm"), nil
}

func largeAMPMParser(text []rune, t *time.Time) (int, error) {
	progress := 0
	if len(text) < 2 {
		return progress, fmt.Errorf("cannot parse am/pm format: remaining text [%s]", string(text))
	}
	toParse := string(text[:2])
	timeOfDay := strings.ToLower(toParse)
	if timeOfDay != "am" && timeOfDay != "pm" {
		return 0, fmt.Errorf("cannot parse am/pm format: [%s] is not am/pm", string(text))
	}
	progress += 2
	return progress, nil
}

func ampmPostProcessor(text []rune, t *time.Time) {
	morning := strings.EqualFold(string(text), "am")
	hour := t.Hour()
	if morning {
		hour %= 12
	}
	if !morning && hour < 12 {
		hour += 12
	}
	*t = time.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		hour,
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
}

func ampmShouldPostProcessResult(tokens map[rune][2]int) bool {
	// Any 24-hour format tokens override am/pm parsing
	overrideTokens := []rune{'X', 'T', 'R', 'k', 'H'}
	for _, token := range overrideTokens {
		if _, ok := tokens[token]; ok {
			return false
		}
	}

	// Process deferred parse if 12-hour format tokens were used, otherwise we can no-op
	deferredTokens := []rune{'l', 'I'}
	for _, token := range deferredTokens {
		if _, ok := tokens[token]; ok {
			return true
		}
	}

	return false
}

func largeAMPMFormatter(t *time.Time) ([]rune, error) {
	if t.Hour() < 12 {
		return []rune("AM"), nil
	}
	return []rune("PM"), nil
}

func quarterParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented quater matcher")
}

func quarterFormatter(t *time.Time) ([]rune, error) {
	day := t.YearDay()
	switch {
	case day < 90:
		return []rune("1"), nil
	case day < 180:
		return []rune("2"), nil
	case day < 270:
		return []rune("3"), nil
	}
	return []rune("4"), nil
}

func hourMinuteFormatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("15:04")), nil
}

func secondParser(text []rune, t *time.Time) (int, error) {
	progress, s, err := parseDigitRespectingOptionalPlaces(text, 0, 59)
	if err != nil {
		return 0, fmt.Errorf("unexpected second number: %s", err)
	}
	*t = time.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		int(s),
		t.Nanosecond(),
		t.Location(),
	)
	return progress, nil
}

func secondFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Second())), nil
}

func unixtimeSecondsParser(text []rune, t *time.Time) (int, error) {
	const unixtimeLen = 10
	if len(text) < unixtimeLen {
		return 0, fmt.Errorf("unexpected unixtime length")
	}
	u, err := strconv.ParseInt(string(text[:unixtimeLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected unixtime number")
	}
	if u < 0 {
		return 0, fmt.Errorf("invalid unixtime number %d", u)
	}
	*t = time.Unix(u, 0)
	return unixtimeLen, nil
}

func unixtimeSecondsFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprint(t.Unix())), nil
}

var hourMinuteSecondParser = composeParseFunctions("hour:minute:second format", []ParseFunction{
	hourParser,
	colonParser,
	minuteParser,
	colonParser,
	secondParser,
})

func hourMinuteSecondFormatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("15:04:05")), nil
}

func tabParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented tab matcher")
}

func tabFormatter(t *time.Time) ([]rune, error) {
	return []rune("\t"), nil
}

func weekOfYearParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week of year matcher")
}

func weekOfYearFormatter(t *time.Time) ([]rune, error) {
	_, week := t.ISOWeek()
	return []rune(fmt.Sprint(week)), nil
}

func weekNumberParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week number matcher")
}

func weekNumberFormatter(t *time.Time) ([]rune, error) {
	_, week := t.ISOWeek()
	return []rune(fmt.Sprint(week)), nil
}

func weekOfYearISOParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week of year ISO matcher")
}

func weekOfYearISOFormatter(t *time.Time) ([]rune, error) {
	_, week := t.ISOWeek()
	return []rune(fmt.Sprint(week)), nil
}

func weekNumberZeroBaseParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week number zero base matcher")
}

func weekNumberZeroBaseFormatter(t *time.Time) ([]rune, error) {
	_, week := t.ISOWeek()
	return []rune(fmt.Sprint(week - 1)), nil
}

func yearParser(text []rune, t *time.Time) (int, error) {
	progress, y, err := parseDigitRespectingOptionalPlaces(text, 1, 9999)
	if err != nil {
		return 0, fmt.Errorf("could not parse year: %s", err)
	}
	*t = time.Date(
		int(y),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
	return progress, nil
}

func yearFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprint(t.Year())), nil
}

func timeZoneParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented time zone matcher")
}

func timeZoneFormatter(t *time.Time) ([]rune, error) {
	name, _ := t.Zone()
	return []rune(name), nil
}

func timeZoneOffsetParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented time zone offset matcher")
}

func timeZoneOffsetFormatter(t *time.Time) ([]rune, error) {
	_, offset := t.Zone()
	return []rune(fmt.Sprint(offset)), nil
}

func escapeParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented escape matcher")
}

func escapeFormatter(t *time.Time) ([]rune, error) {
	return []rune("%"), nil
}

func parseTimeFormat(formatStr, targetStr string, typ TimeFormatType) (*time.Time, error) {
	format := []rune(formatStr)
	target := []rune(targetStr)
	var (
		targetIdx int
		formatIdx int
	)
	epoch := time.Unix(0, 0).UTC()
	var ret = &epoch

	var tokenToParseIndices = map[rune][2]int{}
	for formatIdx < len(format) {
		c := format[formatIdx]
		if c == '%' {
			formatIdx++
			if formatIdx >= len(format) {
				return nil, fmt.Errorf("invalid time format")
			}
			c = format[formatIdx]
			if c == 'E' {
				formatIdx++
				if formatIdx >= len(format) {
					return nil, fmt.Errorf("invalid time format")
				}
				info, formatProgress, err := combinationPatternInfo(format[formatIdx:])

				if err != nil {
					return nil, err
				}
				if !info.Available(typ) {
					return nil, fmt.Errorf("unavailable format by %s type", typ)
				}
				if targetIdx >= len(target) {
					return nil, fmt.Errorf("invalid target text")
				}
				if formatIdx >= len(format) {
					return nil, fmt.Errorf("invalid format text")
				}
				progress, err := info.Parse(target[targetIdx:], ret)
				if err != nil {
					return nil, err
				}
				tokenToParseIndices[c] = [2]int{targetIdx, targetIdx + progress}
				targetIdx += progress
				formatIdx += formatProgress
				continue
			}
			info := formatPatternMap[c]
			if info == nil {
				return nil, fmt.Errorf("unexpected format type %%%c", c)
			}
			if !info.Available(typ) {
				return nil, fmt.Errorf("unavailable format by %s type", typ)
			}
			if targetIdx >= len(target) {
				return nil, fmt.Errorf("invalid target text")
			}
			progress, err := info.Parse(target[targetIdx:], ret)
			tokenToParseIndices[c] = [2]int{targetIdx, targetIdx + progress}
			if err != nil {
				return nil, fmt.Errorf("error parsing [%s] with format [%s]: %s", string(target), formatStr, err)
			}
			targetIdx += progress
			formatIdx++
		} else if c == ' ' {
			formatIdx++
			// Slurp whitespaces when parsing a whitespace token
			for targetIdx < len(target) && target[targetIdx] == ' ' {
				targetIdx++
			}
		} else {
			formatIdx++
			targetIdx++
		}
	}
	if targetIdx != len(target) {
		return nil, fmt.Errorf("error parsing [%s] with format [%s]: found unparsed text [%s]", string(target), formatStr, string(target[targetIdx:]))
	}

	// Post-process any deferred parsers
	for token, indices := range tokenToParseIndices {
		info, ok := postProcessorPatternMap[token]
		if ok {
			start := indices[0]
			end := indices[1]

			if !info.ShouldPostProcessResult(tokenToParseIndices) {
				continue
			}

			info.PostProcessResult(target[start:end], ret)
		}
	}

	return ret, nil
}

func formatTime(formatStr string, t *time.Time, typ TimeFormatType) (string, error) {
	format := []rune(formatStr)
	var ret []rune
	for i := 0; i < len(format); i++ {
		c := format[i]
		if c == '%' {
			i++
			if i >= len(format) {
				return "", fmt.Errorf("invalid time format")
			}
			c = format[i]
			if c == 'E' {
				i++
				if i >= len(format) {
					return "", fmt.Errorf("invalid time format")
				}
				info, formatProgress, err := combinationPatternInfo(format[i:])
				if err != nil {
					return "", err
				}
				if !info.Available(typ) {
					return "", fmt.Errorf("unavailable format by %s type", typ)
				}
				formatted, err := info.Format(t)
				if err != nil {
					return "", err
				}
				ret = append(ret, formatted...)
				i += formatProgress
				continue
			}
			info := formatPatternMap[c]
			if info == nil {
				return "", fmt.Errorf("unexpected format type %%%c", c)
			}
			if !info.Available(typ) {
				return "", fmt.Errorf("unavailable format by %s type", typ)
			}
			formatted, err := info.Format(t)
			if err != nil {
				return "", err
			}
			ret = append(ret, formatted...)
		} else {
			ret = append(ret, c)
		}
	}
	return string(ret), nil
}

type CombinationFormatTimeInfo struct {
	AvailableTypes []TimeFormatType
	Parse          func([]rune, *time.Time) (int, error)
	Format         func(*time.Time) ([]rune, error)
}

func (i *CombinationFormatTimeInfo) Available(typ TimeFormatType) bool {
	for _, t := range i.AvailableTypes {
		if t == typ {
			return true
		}
	}
	return false
}

func combinationPatternInfo(format []rune) (*CombinationFormatTimeInfo, int, error) {
	switch format[0] {
	case 'z':
		return &CombinationFormatTimeInfo{
			AvailableTypes: []TimeFormatType{
				FormatTypeTimestamp,
			},
			Parse:  timeZoneRFC3339Parser,
			Format: timeZoneRFC3339Formatter,
		}, 1, nil
	case '1', '2', '3', '5', '6':
		if len(format) > 1 && format[1] == 'S' {
			precision, _ := strconv.Atoi(string(format[0]))
			return &CombinationFormatTimeInfo{
				AvailableTypes: []TimeFormatType{
					FormatTypeTime,
					FormatTypeDatetime,
					FormatTypeTimestamp,
				},
				Parse: func(target []rune, ret *time.Time) (int, error) {
					return timePrecisionParser(precision, target, ret)
				},
				Format: func(t *time.Time) ([]rune, error) {
					return timePrecisionFormatter(precision, t)
				},
			}, 2, nil
		}
	case '4':
		if len(format) > 1 {
			switch format[1] {
			case 'S':
				return &CombinationFormatTimeInfo{
					AvailableTypes: []TimeFormatType{
						FormatTypeTime,
						FormatTypeDatetime,
						FormatTypeTimestamp,
					},
					Parse: func(target []rune, ret *time.Time) (int, error) {
						return timePrecisionParser(4, target, ret)
					},
					Format: func(t *time.Time) ([]rune, error) {
						return timePrecisionFormatter(4, t)
					},
				}, 2, nil
			case 'Y':
				return &CombinationFormatTimeInfo{
					AvailableTypes: []TimeFormatType{
						FormatTypeDate,
						FormatTypeDatetime,
						FormatTypeTimestamp,
					},
					Parse:  yearParser,
					Format: timeYear4Formatter,
				}, 2, nil
			}
		}
	case '*':
		if len(format) > 1 && format[1] == 'S' {
			return &CombinationFormatTimeInfo{
				AvailableTypes: []TimeFormatType{
					FormatTypeTime,
					FormatTypeDatetime,
					FormatTypeTimestamp,
				},
				Parse: func(target []rune, ret *time.Time) (int, error) {
					return timePrecisionParser(6, target, ret)
				},
				Format: func(t *time.Time) ([]rune, error) {
					return timePrecisionFormatter(6, t)
				},
			}, 2, nil
		}
	}
	return nil, 0, fmt.Errorf("unexpected format type %%%c", format[0])
}

func timeZoneRFC3339Parser(target []rune, t *time.Time) (int, error) {
	targetIdx := 0
	if target[targetIdx] == 'Z' {
		targetIdx++
		*t = time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
			t.Nanosecond(),
			time.UTC,
		)
		return targetIdx, nil
	}
	if target[targetIdx] == '+' || target[targetIdx] == '-' {
		s := target[targetIdx]
		targetIdx++
		fmtLen := len("00:00")
		if len(target[targetIdx:]) != fmtLen {
			return 0, fmt.Errorf("unexpected offset format")
		}
		splitted := strings.Split(string(target[targetIdx:]), ":")
		if len(splitted) != 2 {
			return 0, fmt.Errorf("unexpected offset format")
		}
		hour := splitted[0]
		minute := splitted[1]
		if len(hour) != 2 || len(minute) != 2 {
			return 0, fmt.Errorf("unexpected hour:minute format")
		}
		h, err := strconv.ParseInt(hour, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("unexpected hour:minute format: %w", err)
		}
		m, err := strconv.ParseInt(minute, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("unexpected hour:minute format: %w", err)
		}
		hs := int(h*60*60 + m*60)
		if s == '-' {
			hs *= -1
		}
		*t = time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
			t.Nanosecond(),
			time.FixedZone("", hs),
		)
		targetIdx += fmtLen
		return targetIdx, nil
	}
	return 0, fmt.Errorf("unexpected offset format: %%%c", target[targetIdx])
}

func timeZoneRFC3339Formatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("-07:00")), nil
}

var timePrecisionMatcher = regexp.MustCompile(`\d{2}\.?\d*`)

func timePrecisionParser(precision int, text []rune, t *time.Time) (int, error) {
	const maxNanosecondsLength = 9
	extracted := timePrecisionMatcher.FindString(string(text))
	if extracted == "" {
		return 0, fmt.Errorf("failed to parse seconds.nanoseconds for %s", string(text))
	}
	fmtLen := len(extracted)
	splitted := strings.Split(extracted, ".")
	seconds := splitted[0]
	nanoseconds := strconv.Itoa(t.Nanosecond())
	if len(splitted) == 2 {
		nanoseconds = splitted[1]
		if len(nanoseconds) > precision {
			nanoseconds = nanoseconds[:precision]
		}
		nanoseconds += strings.Repeat("0", maxNanosecondsLength-len(nanoseconds))
	}
	s, err := strconv.ParseInt(seconds, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse seconds parameter for %s: %w", string(text), err)
	}
	n, err := strconv.ParseInt(nanoseconds, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse nanoseconds parameter for %s: %w", string(text), err)
	}
	*t = time.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		int(s),
		int(n),
		t.Location(),
	)
	return fmtLen, nil
}

func timePrecisionFormatter(precision int, t *time.Time) ([]rune, error) {
	return []rune(t.Format(fmt.Sprintf("05.%s", strings.Repeat("0", precision)))), nil
}

func timeYear4Formatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("2006")), nil
}
