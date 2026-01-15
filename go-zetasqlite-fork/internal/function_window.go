package internal

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"gonum.org/v1/gonum/stat"
)

type WINDOW_ANY_VALUE struct {
}

func (f *WINDOW_ANY_VALUE) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_ANY_VALUE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var value Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		value = values[start]
		return nil
	}); err != nil {
		return nil, err
	}
	return value, nil
}

type WINDOW_ARRAY_AGG struct {
}

func (f *WINDOW_ARRAY_AGG) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if v == nil {
		return fmt.Errorf("ARRAY_AGG: input value must be not null")
	}
	return agg.Step(v, opt)
}

func (f *WINDOW_ARRAY_AGG) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	ret := &ArrayValue{}
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		var (
			filteredValues []Value
			valueMap       = map[string]struct{}{}
		)
		for _, v := range values[start : end+1] {
			if agg.IgnoreNulls() {
				if v == nil {
					continue
				}
			}
			if agg.Distinct() {
				key, err := v.ToString()
				if err != nil {
					return err
				}
				if _, exists := valueMap[key]; exists {
					continue
				}
				valueMap[key] = struct{}{}
			}
			filteredValues = append(filteredValues, v)
		}
		ret.values = filteredValues
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

type WINDOW_AVG struct {
}

func (f *WINDOW_AVG) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_AVG) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var avg Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		var (
			sum      Value
			valueMap = map[string]struct{}{}
		)
		for _, value := range values[start : end+1] {
			if value == nil {
				continue
			}
			if agg.Distinct() {
				key, err := value.ToString()
				if err != nil {
					return err
				}
				if _, exists := valueMap[key]; exists {
					continue
				}
				valueMap[key] = struct{}{}
			}
			if sum == nil {
				f64, err := value.ToFloat64()
				if err != nil {
					return err
				}
				sum = FloatValue(f64)
			} else {
				added, err := sum.Add(value)
				if err != nil {
					return err
				}
				sum = added
			}
		}
		if sum == nil {
			return nil
		}
		ret, err := sum.Div(FloatValue(float64(len(values[start : end+1]))))
		if err != nil {
			return err
		}
		avg = ret
		return nil
	}); err != nil {
		return nil, err
	}
	return avg, nil
}

type WINDOW_COUNT struct {
}

func (f *WINDOW_COUNT) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_COUNT) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var count int64
	if err := agg.Done(func(values []Value, start, end int) error {
		valueMap := map[string]struct{}{}
		for _, v := range values[start : end+1] {
			if v == nil {
				continue
			}
			if agg.Distinct() {
				key, err := v.ToString()
				if err != nil {
					return err
				}
				if _, exists := valueMap[key]; exists {
					continue
				}
				valueMap[key] = struct{}{}
			}
			count++
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return IntValue(count), nil
}

type WINDOW_COUNT_STAR struct {
}

func (f *WINDOW_COUNT_STAR) Step(opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_COUNT_STAR) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var count int64
	if err := agg.Done(func(values []Value, start, end int) error {
		count = int64(len(values[start : end+1]))
		return nil
	}); err != nil {
		return nil, err
	}
	return IntValue(count), nil
}

type WINDOW_COUNTIF struct {
}

func (f *WINDOW_COUNTIF) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_COUNTIF) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var count int64
	if err := agg.Done(func(values []Value, start, end int) error {
		for _, value := range values[start : end+1] {
			if value == nil {
				continue
			}
			cond, err := value.ToBool()
			if err != nil {
				return err
			}
			if cond {
				count++
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return IntValue(count), nil
}

type WINDOW_MAX struct {
}

func (f *WINDOW_MAX) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_MAX) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		max Value
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		for _, value := range values[start : end+1] {
			if value == nil {
				continue
			}
			if max == nil {
				max = value
			} else {
				cond, err := value.GT(max)
				if err != nil {
					return err
				}
				if cond {
					max = value
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return max, nil
}

type WINDOW_MIN struct {
}

func (f *WINDOW_MIN) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_MIN) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		min Value
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		for _, value := range values[start : end+1] {
			if value == nil {
				continue
			}
			if min == nil {
				min = value
			} else {
				cond, err := value.LT(min)
				if err != nil {
					return err
				}
				if cond {
					min = value
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return min, nil
}

type WINDOW_STRING_AGG struct {
	delim string
	once  sync.Once
}

func (f *WINDOW_STRING_AGG) Step(v Value, delim string, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	f.once.Do(func() {
		if delim == "" {
			delim = ","
		}
		f.delim = delim
	})
	return agg.Step(v, opt)
}

func (f *WINDOW_STRING_AGG) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var strValues []string
	if err := agg.Done(func(values []Value, start, end int) error {
		valueMap := map[string]struct{}{}
		for _, value := range values[start : end+1] {
			if value == nil {
				continue
			}
			if agg.Distinct() {
				key, err := value.ToString()
				if err != nil {
					return err
				}
				if _, exists := valueMap[key]; exists {
					continue
				}
				valueMap[key] = struct{}{}
			}
			text, err := value.ToString()
			if err != nil {
				return err
			}
			strValues = append(strValues, text)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(strValues) == 0 {
		return nil, nil
	}
	return StringValue(strings.Join(strValues, f.delim)), nil
}

type WINDOW_SUM struct {
}

func (f *WINDOW_SUM) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_SUM) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var sum Value
	if err := agg.Done(func(values []Value, start, end int) error {
		valueMap := map[string]struct{}{}
		for _, value := range values[start : end+1] {
			if value == nil {
				continue
			}
			if agg.Distinct() {
				key, err := value.ToString()
				if err != nil {
					return err
				}
				if _, exists := valueMap[key]; exists {
					continue
				}
				valueMap[key] = struct{}{}
			}
			if sum == nil {
				sum = value
			} else {
				added, err := sum.Add(value)
				if err != nil {
					return err
				}
				sum = added
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return sum, nil
}

type WINDOW_FIRST_VALUE struct {
}

func (f *WINDOW_FIRST_VALUE) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_FIRST_VALUE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var firstValue Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		filteredValues := []Value{}
		for _, value := range values[start : end+1] {
			if agg.IgnoreNulls() {
				if value == nil {
					continue
				}
			}
			filteredValues = append(filteredValues, value)
		}
		if len(filteredValues) == 0 {
			return nil
		}
		firstValue = filteredValues[0]
		return nil
	}); err != nil {
		return nil, err
	}
	return firstValue, nil
}

type WINDOW_LAST_VALUE struct {
}

func (f *WINDOW_LAST_VALUE) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_LAST_VALUE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var lastValue Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		filteredValues := []Value{}
		for _, value := range values[start : end+1] {
			if agg.IgnoreNulls() {
				if value == nil {
					continue
				}
			}
			filteredValues = append(filteredValues, value)
		}
		if len(filteredValues) == 0 {
			return nil
		}
		lastValue = filteredValues[len(filteredValues)-1]
		return nil
	}); err != nil {
		return nil, err
	}
	return lastValue, nil
}

type WINDOW_NTH_VALUE struct {
	once sync.Once
	num  int64
}

func (f *WINDOW_NTH_VALUE) Step(v Value, num int64, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	f.once.Do(func() {
		f.num = num
	})
	return agg.Step(v, opt)
}

func (f *WINDOW_NTH_VALUE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var nthValue Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		filteredValues := []Value{}
		for _, value := range values[start : end+1] {
			if agg.IgnoreNulls() {
				if value == nil {
					continue
				}
			}
			filteredValues = append(filteredValues, value)
		}
		if len(filteredValues) == 0 {
			return nil
		}
		num := f.num - 1
		if 0 <= f.num && f.num < int64(len(filteredValues)) {
			nthValue = filteredValues[num]
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return nthValue, nil
}

type WINDOW_LEAD struct {
	once         sync.Once
	offset       int64
	defaultValue Value
}

func (f *WINDOW_LEAD) Step(v Value, offset int64, defaultValue Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	f.once.Do(func() {
		f.offset = offset
		f.defaultValue = defaultValue
	})
	return agg.Step(v, opt)
}

func (f *WINDOW_LEAD) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var leadValue Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		if start+int(f.offset) >= len(values) {
			return nil
		}
		leadValue = values[start+int(f.offset)]
		return nil
	}); err != nil {
		return nil, err
	}
	if leadValue == nil {
		return f.defaultValue, nil
	}
	return leadValue, nil
}

type WINDOW_LAG struct {
	lagOnce      sync.Once
	offset       int64
	defaultValue Value
}

func (f *WINDOW_LAG) Step(v Value, offset int64, defaultValue Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	f.lagOnce.Do(func() {
		f.offset = offset
		f.defaultValue = defaultValue
	})
	return agg.Step(v, opt)
}

func (f *WINDOW_LAG) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var lagValue Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		if start-int(f.offset) < 0 {
			return nil
		}
		lagValue = values[start-int(f.offset)]
		return nil
	}); err != nil {
		return nil, err
	}
	if lagValue == nil {
		return f.defaultValue, nil
	}
	return lagValue, nil
}

type WINDOW_PERCENTILE_CONT struct {
	once       sync.Once
	percentile Value
}

func (f *WINDOW_PERCENTILE_CONT) Step(v, percentile Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	f.once.Do(func() {
		f.percentile = percentile
	})
	return agg.Step(v, opt)
}

func (f *WINDOW_PERCENTILE_CONT) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	if cond, _ := f.percentile.LT(IntValue(0)); cond {
		return nil, fmt.Errorf("PERCENTILE_CONT: percentile value must be greater than zero")
	}
	if cond, _ := f.percentile.GT(IntValue(1)); cond {
		return nil, fmt.Errorf("PERCENTILE_CONT: percentile value must be less than one")
	}
	var (
		maxValue         Value
		minValue         Value
		floorValue       Value
		ceilingValue     Value
		rowNumber        float64
		floorRowNumber   float64
		ceilingRowNumber float64
		nonNullValues    []int
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		var filteredValues []Value
		for _, value := range values {
			if agg.IgnoreNulls() {
				if value == nil {
					continue
				}
			}
			int64Val, err := value.ToInt64()
			if err != nil {
				return err
			}
			nonNullValues = append(nonNullValues, int(int64Val))
			filteredValues = append(filteredValues, value)
		}
		if len(filteredValues) == 0 {
			return nil
		}

		// Calculate row number at percentile
		percentile, err := f.percentile.ToFloat64()
		if err != nil {
			return err
		}
		sort.Ints(nonNullValues)

		// rowNumber = (1 + (percentile * (length of array - 1)
		rowNumber = 1 + percentile*float64(len(nonNullValues)-1)
		floorRowNumber = math.Floor(rowNumber)
		floorValue = FloatValue(nonNullValues[int(floorRowNumber-1)])
		ceilingRowNumber = math.Ceil(rowNumber)
		ceilingValue = FloatValue(nonNullValues[int(ceilingRowNumber-1)])

		maxValue = filteredValues[0]
		minValue = filteredValues[0]
		for _, value := range filteredValues {
			if value == nil {
				// TODO: support RESPECT NULLS
				continue
			}
			if maxValue == nil {
				maxValue = value
			}
			if minValue == nil {
				minValue = value
			}
			if cond, _ := value.GT(maxValue); cond {
				maxValue = value
			}
			if cond, _ := value.LT(minValue); cond {
				minValue = value
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if maxValue == nil || minValue == nil {
		return nil, nil
	}
	if cond, _ := maxValue.EQ(IntValue(0)); cond {
		return FloatValue(0), nil
	}

	//nolint:gocritic
	// if ceilingRowNumber = floorRowNumber = rowNumber, return value at rownNumber which is equivalent of floorValue
	if ceilingRowNumber == floorRowNumber && ceilingRowNumber == rowNumber {
		return floorValue, nil
	}

	// (value of row at ceilingRowNumber) * (rowNumber – floorRowNumber) +
	// (value of row at floorRowNumber) * (ceilingRowNumber – rowNumber)
	leftSide, err := ceilingValue.Mul(FloatValue(rowNumber - floorRowNumber))
	if err != nil {
		return nil, err
	}
	rightSide, err := floorValue.Mul(FloatValue(ceilingRowNumber - rowNumber))
	if err != nil {
		return nil, err
	}

	ret, err := leftSide.Add(rightSide)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

type WINDOW_PERCENTILE_DISC struct {
	once       sync.Once
	percentile Value
}

func (f *WINDOW_PERCENTILE_DISC) Step(v, percentile Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	f.once.Do(func() {
		f.percentile = percentile
	})
	return agg.Step(v, opt)
}

func (f *WINDOW_PERCENTILE_DISC) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	if cond, _ := f.percentile.LT(IntValue(0)); cond {
		return nil, fmt.Errorf("PERCENTILE_DISC: percentile value must be greater than zero")
	}
	if cond, _ := f.percentile.GT(IntValue(1)); cond {
		return nil, fmt.Errorf("PERCENTILE_DISC: percentile value must be less than one")
	}
	var sortedValues []Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		var filteredValues []Value
		for _, value := range values {
			if agg.IgnoreNulls() {
				if value == nil {
					continue
				}
			}
			filteredValues = append(filteredValues, value)
		}
		if len(filteredValues) == 0 {
			return nil
		}
		sort.Slice(filteredValues, func(i, j int) bool {
			if filteredValues[i] == nil {
				return true
			}
			if filteredValues[j] == nil {
				return false
			}
			cond, _ := filteredValues[i].LT(filteredValues[j])
			return cond
		})
		sortedValues = filteredValues
		return nil
	}); err != nil {
		return nil, err
	}
	pickPoint, err := f.percentile.Mul(IntValue(len(sortedValues)))
	if err != nil {
		return nil, err
	}
	if cond, _ := pickPoint.EQ(IntValue(0)); cond {
		return sortedValues[0], nil
	}
	fIdx, err := pickPoint.ToFloat64()
	if err != nil {
		return nil, err
	}
	idx := int64(fIdx)
	if float64(idx) < fIdx {
		idx += 1
	}
	idx -= 1
	if idx > 0 {
		return sortedValues[idx], nil
	}
	return nil, nil
}

type WINDOW_RANK struct {
}

func (f *WINDOW_RANK) Step(opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_RANK) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var rankValue Value
	if err := agg.Done(func(_ []Value, start, end int) error {
		var (
			orderByValues []Value
			isAsc         bool = true
			isAscOnce     sync.Once
		)
		for _, value := range agg.SortedValues {
			orderByValues = append(orderByValues, value.OrderBy[len(value.OrderBy)-1].Value)
			isAscOnce.Do(func() {
				isAsc = value.OrderBy[len(value.OrderBy)-1].IsAsc
			})
		}
		if start >= len(orderByValues) || end < 0 {
			return nil
		}
		if len(orderByValues) == 0 {
			return nil
		}
		if start != end {
			return fmt.Errorf("Rank must be same value of start and end")
		}
		lastIdx := start
		var (
			rank        = 0
			sameRankNum = 1
			maxValue    int64
		)
		if isAsc {
			for idx := 0; idx <= lastIdx; idx++ {
				curValue, err := orderByValues[idx].ToInt64()
				if err != nil {
					return err
				}
				if maxValue < curValue {
					maxValue = curValue
					rank += sameRankNum
					sameRankNum = 1
				} else {
					sameRankNum++
				}
			}
		} else {
			maxValue = math.MaxInt64
			for idx := 0; idx <= lastIdx; idx++ {
				curValue, err := orderByValues[idx].ToInt64()
				if err != nil {
					return err
				}
				if maxValue > curValue {
					maxValue = curValue
					rank += sameRankNum
					sameRankNum = 1
				} else {
					sameRankNum++
				}
			}
		}
		rankValue = IntValue(rank)
		return nil
	}); err != nil {
		return nil, err
	}
	return rankValue, nil
}

type WINDOW_DENSE_RANK struct {
}

func (f *WINDOW_DENSE_RANK) Step(opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_DENSE_RANK) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var rankValue Value
	if err := agg.Done(func(_ []Value, start, end int) error {
		var (
			orderByValues []Value
			isAscOnce     sync.Once
			isAsc         bool = true
		)
		for _, value := range agg.SortedValues {
			orderByValues = append(orderByValues, value.OrderBy[len(value.OrderBy)-1].Value)
			isAscOnce.Do(func() {
				isAsc = value.OrderBy[len(value.OrderBy)-1].IsAsc
			})
		}
		if start >= len(orderByValues) || end < 0 {
			return nil
		}
		if len(orderByValues) == 0 {
			return nil
		}
		if start != end {
			return fmt.Errorf("rank must be same value of start and end")
		}
		lastIdx := start
		var (
			rank     = 0
			maxValue int64
		)
		if isAsc {
			for idx := 0; idx <= lastIdx; idx++ {
				curValue, err := orderByValues[idx].ToInt64()
				if err != nil {
					return err
				}
				if maxValue < curValue {
					maxValue = curValue
					rank++
				}
			}
		} else {
			maxValue = math.MaxInt64
			for idx := 0; idx <= lastIdx; idx++ {
				curValue, err := orderByValues[idx].ToInt64()
				if err != nil {
					return err
				}
				if maxValue > curValue {
					maxValue = curValue
					rank++
				}
			}
		}
		rankValue = IntValue(rank)
		return nil
	}); err != nil {
		return nil, err
	}
	return rankValue, nil
}

type WINDOW_PERCENT_RANK struct {
}

func (f *WINDOW_PERCENT_RANK) Step(opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_PERCENT_RANK) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		rankValue int
		lineNum   int
	)
	if err := agg.Done(func(_ []Value, start, end int) error {
		var (
			orderByValues []Value
			isAsc         bool = true
			isAscOnce     sync.Once
		)
		for _, value := range agg.SortedValues {
			orderByValues = append(orderByValues, value.OrderBy[len(value.OrderBy)-1].Value)
			isAscOnce.Do(func() {
				isAsc = value.OrderBy[len(value.OrderBy)-1].IsAsc
			})
		}
		if start >= len(orderByValues) || end < 0 {
			return nil
		}
		if len(orderByValues) == 0 {
			return nil
		}
		if start != end {
			return fmt.Errorf("PERCENT_RANK: must be same value of start and end")
		}
		lineNum = len(orderByValues)
		lastIdx := start
		var (
			rank        = 0
			sameRankNum = 1
			maxValue    int64
		)
		if isAsc {
			for idx := 0; idx <= lastIdx; idx++ {
				curValue, err := orderByValues[idx].ToInt64()
				if err != nil {
					return err
				}
				if maxValue < curValue {
					maxValue = curValue
					rank += sameRankNum
					sameRankNum = 1
				} else {
					sameRankNum++
				}
			}
		} else {
			maxValue = math.MaxInt64
			for idx := 0; idx <= lastIdx; idx++ {
				curValue, err := orderByValues[idx].ToInt64()
				if err != nil {
					return err
				}
				if maxValue > curValue {
					maxValue = curValue
					rank += sameRankNum
					sameRankNum = 1
				} else {
					sameRankNum++
				}
			}
		}
		rankValue = rank
		return nil
	}); err != nil {
		return nil, err
	}
	if lineNum == 1 {
		return FloatValue(0), nil
	}
	return FloatValue(float64(rankValue-1) / float64(lineNum-1)), nil
}

type WINDOW_CUME_DIST struct {
}

func (f *WINDOW_CUME_DIST) Step(opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_CUME_DIST) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var cumeDistValue float64
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		cumeDistValue = float64(start+1) / float64(len(values))
		return nil
	}); err != nil {
		return nil, err
	}
	return FloatValue(cumeDistValue), nil
}

type WINDOW_NTILE struct {
	once sync.Once
	num  int64
}

func (f *WINDOW_NTILE) Step(num int64, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	f.once.Do(func() {
		f.num = num
	})
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_NTILE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var ntileValue int64
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		length := int64(len(values))
		dupCount := length/f.num - 1
		if length%f.num > 0 {
			dupCount++
		}
		normalizeValues := []int64{}
		for i := 0; i < len(values); i++ {
			normalizeValues = append(normalizeValues, int64(i+1))
			if dupCount > 0 {
				normalizeValues = append(normalizeValues, int64(i+1))
				dupCount--
			}
		}
		ntileValue = normalizeValues[start]
		return nil
	}); err != nil {
		return nil, err
	}
	return IntValue(ntileValue), nil
}

type WINDOW_ROW_NUMBER struct {
}

func (f *WINDOW_ROW_NUMBER) Step(opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_ROW_NUMBER) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var rowNum Value
	if err := agg.Done(func(_ []Value, start, end int) error {
		rowNum = IntValue(start + 1)
		return nil
	}); err != nil {
		return nil, err
	}
	return rowNum, nil
}

type WINDOW_CORR struct {
}

func (f *WINDOW_CORR) Step(x, y Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if x == nil || y == nil {
		return nil
	}
	return agg.Step(&ArrayValue{values: []Value{x, y}}, opt)
}

func (f *WINDOW_CORR) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		x []float64
		y []float64
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			arr, err := value.ToArray()
			if err != nil {
				return err
			}
			if len(arr.values) != 2 {
				return fmt.Errorf("invalid corr arguments")
			}
			x1, err := arr.values[0].ToFloat64()
			if err != nil {
				return err
			}
			x2, err := arr.values[1].ToFloat64()
			if err != nil {
				return err
			}
			x = append(x, x1)
			y = append(y, x2)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(x) == 0 || len(y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Correlation(x, y, nil)), nil
}

type WINDOW_COVAR_POP struct {
}

func (f *WINDOW_COVAR_POP) Step(x, y Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if x == nil || y == nil {
		return nil
	}
	return agg.Step(&ArrayValue{values: []Value{x, y}}, opt)
}

func (f *WINDOW_COVAR_POP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		x []float64
		y []float64
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			arr, err := value.ToArray()
			if err != nil {
				return err
			}
			if len(arr.values) != 2 {
				return fmt.Errorf("invalid corr arguments")
			}
			x1, err := arr.values[0].ToFloat64()
			if err != nil {
				return err
			}
			x2, err := arr.values[1].ToFloat64()
			if err != nil {
				return err
			}
			x = append(x, x1)
			y = append(y, x2)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(x) == 0 || len(y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Covariance(x, y, nil)), nil
}

type WINDOW_COVAR_SAMP struct {
}

func (f *WINDOW_COVAR_SAMP) Step(x, y Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if x == nil || y == nil {
		return nil
	}
	return agg.Step(&ArrayValue{values: []Value{x, y}}, opt)
}

func (f *WINDOW_COVAR_SAMP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		x []float64
		y []float64
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			arr, err := value.ToArray()
			if err != nil {
				return err
			}
			if len(arr.values) != 2 {
				return fmt.Errorf("invalid corr arguments")
			}
			x1, err := arr.values[0].ToFloat64()
			if err != nil {
				return err
			}
			x2, err := arr.values[1].ToFloat64()
			if err != nil {
				return err
			}
			x = append(x, x1)
			y = append(y, x2)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(x) == 0 || len(y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Covariance(x, y, nil)), nil
}

type WINDOW_STDDEV_POP struct {
}

func (f *WINDOW_STDDEV_POP) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_STDDEV_POP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var stddevpop []float64
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			f64, err := value.ToFloat64()
			if err != nil {
				return err
			}
			stddevpop = append(stddevpop, f64)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(stddevpop) == 0 {
		return nil, nil
	}
	_, std := stat.PopMeanStdDev(stddevpop, nil)
	return FloatValue(std), nil
}

type WINDOW_STDDEV_SAMP struct {
}

func (f *WINDOW_STDDEV_SAMP) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_STDDEV_SAMP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var stddevsamp []float64
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			f64, err := value.ToFloat64()
			if err != nil {
				return err
			}
			stddevsamp = append(stddevsamp, f64)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(stddevsamp) == 0 {
		return nil, nil
	}
	return FloatValue(stat.StdDev(stddevsamp, nil)), nil
}

type WINDOW_STDDEV = WINDOW_STDDEV_SAMP

type WINDOW_VAR_POP struct {
}

func (f *WINDOW_VAR_POP) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_VAR_POP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var varpop []float64
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			f64, err := value.ToFloat64()
			if err != nil {
				return err
			}
			varpop = append(varpop, f64)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(varpop) == 0 {
		return nil, nil
	}
	_, variance := stat.PopMeanVariance(varpop, nil)
	return FloatValue(variance), nil
}

type WINDOW_VAR_SAMP struct {
}

func (f *WINDOW_VAR_SAMP) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(v, opt)
}

func (f *WINDOW_VAR_SAMP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var varsamp []float64
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			f64, err := value.ToFloat64()
			if err != nil {
				return err
			}
			varsamp = append(varsamp, f64)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(varsamp) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Variance(varsamp, nil)), nil
}

type WINDOW_VARIANCE = WINDOW_VAR_SAMP
