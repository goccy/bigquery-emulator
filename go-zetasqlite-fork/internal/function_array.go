package internal

import (
	"fmt"
	"strings"
)

func ARRAY_CONCAT(args ...Value) (Value, error) {
	arr := &ArrayValue{}
	for _, arg := range args {
		subarr, err := arg.ToArray()
		if err != nil {
			return nil, err
		}
		arr.values = append(arr.values, subarr.values...)
	}
	return arr, nil
}

func ARRAY_LENGTH(v *ArrayValue) (Value, error) {
	return IntValue(len(v.values)), nil
}

func ARRAY_TO_STRING(arr *ArrayValue, delim string, nullText ...string) (Value, error) {
	var elems []string
	for _, v := range arr.values {
		if v == nil {
			if len(nullText) == 0 {
				continue
			} else {
				elems = append(elems, nullText[0])
			}
		} else {
			elems = append(elems, v.Format('t'))
		}
	}
	return StringValue(strings.Join(elems, delim)), nil
}

func GENERATE_ARRAY(start, end Value, step ...Value) (Value, error) {
	var stepValue Value
	if len(step) > 0 {
		stepValue = step[0]
	} else {
		stepValue = IntValue(1)
	}
	return generateArray(start, end, stepValue)
}

func GENERATE_DATE_ARRAY(start, end Value, step ...Value) (Value, error) {
	if len(step) > 2 {
		return nil, fmt.Errorf("invalid step value %v", step)
	}
	var (
		stepValue int64 = 1
		interval        = "DAY"
	)
	if len(step) == 2 {
		stepV, err := step[0].ToInt64()
		if err != nil {
			return nil, err
		}
		intervalV, err := step[1].ToString()
		if err != nil {
			return nil, err
		}
		stepValue = stepV
		interval = intervalV
	} else if len(step) == 1 {
		stepV, err := step[0].ToInt64()
		if err != nil {
			return nil, err
		}
		stepValue = stepV
	}
	return generateDateArray(start, end, int(stepValue), interval)
}

func GENERATE_TIMESTAMP_ARRAY(start, end Value, step int64, part string) (Value, error) {
	if start == nil || end == nil || step == 0 {
		return nil, nil
	}
	isLT, err := start.LTE(end)
	if err != nil {
		return nil, err
	}
	arr := &ArrayValue{}
	isPositiveStepValue := step > 0
	if isLT && !isPositiveStepValue {
		// start less than end and step is negative value
		return arr, nil
	} else if !isLT && isPositiveStepValue {
		// start greater than end and step is positive value
		return arr, nil
	}
	cur := start
	for {
		arr.values = append(arr.values, cur)
		after, err := cur.(TimestampValue).AddValueWithPart(step, part)
		if err != nil {
			return nil, err
		}
		if isLT {
			cond, err := after.LTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		} else {
			cond, err := after.GTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		}
		cur = after
	}
	return arr, nil
}

func generateArray(start, end, step Value) (Value, error) {
	if start == nil || end == nil || step == nil {
		return nil, nil
	}
	isLT, err := start.LTE(end)
	if err != nil {
		return nil, err
	}
	arr := &ArrayValue{}
	isPositiveStepValue, err := step.GT(IntValue(0))
	if err != nil {
		return nil, err
	}
	if isLT && !isPositiveStepValue {
		// start less than end and step is negative value
		return arr, nil
	} else if !isLT && isPositiveStepValue {
		// start greater than end and step is positive value
		return arr, nil
	}
	cur := start
	for {
		arr.values = append(arr.values, cur)
		after, err := cur.Add(step)
		if err != nil {
			return nil, err
		}
		if isLT {
			cond, err := after.LTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		} else {
			cond, err := after.GTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		}
		cur = after
	}
	return arr, nil
}

func generateDateArray(start, end Value, step int, interval string) (Value, error) {
	if start == nil || end == nil || step == 0 {
		return nil, nil
	}
	isLT, err := start.LTE(end)
	if err != nil {
		return nil, err
	}
	arr := &ArrayValue{}
	isPositiveStepValue := step > 0
	if isLT && !isPositiveStepValue {
		// start less than end and step is negative value
		return arr, nil
	} else if !isLT && isPositiveStepValue {
		// start greater than end and step is positive value
		return arr, nil
	}
	cur := start
	for {
		arr.values = append(arr.values, cur)
		after, err := cur.(DateValue).AddDateWithInterval(step, interval)
		if err != nil {
			return nil, err
		}
		if isLT {
			cond, err := after.LTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		} else {
			cond, err := after.GTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		}
		cur = after
	}
	return arr, nil
}

func ARRAY_REVERSE(v *ArrayValue) (Value, error) {
	ret := &ArrayValue{}
	for i := len(v.values) - 1; i >= 0; i-- {
		ret.values = append(ret.values, v.values[i])
	}
	return ret, nil
}
