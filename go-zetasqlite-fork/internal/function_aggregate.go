package internal

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/DataDog/go-hll"
	"github.com/spaolacci/murmur3"
	"gonum.org/v1/gonum/stat"
)

type OrderedValue struct {
	OrderBy []*AggregateOrderBy
	Value   Value
}

type ARRAY struct {
	once   sync.Once
	opt    *AggregatorOption
	values []*OrderedValue
}

func (f *ARRAY) Step(v Value, opt *AggregatorOption) error {
	f.once.Do(func() { f.opt = opt })
	f.values = append(f.values, &OrderedValue{
		Value: v,
	})
	return nil
}

func (f *ARRAY) Done() (Value, error) {
	values := make([]Value, 0, len(f.values))
	for _, v := range f.values {
		values = append(values, v.Value)
	}
	return &ArrayValue{
		values: values,
	}, nil
}

type ANY_VALUE struct {
	once  sync.Once
	opt   *AggregatorOption
	value Value
}

func (f *ANY_VALUE) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	f.once.Do(func() {
		f.opt = opt
		f.value = v
	})
	return nil
}

func (f *ANY_VALUE) Done() (Value, error) {
	return f.value, nil
}

type ARRAY_AGG struct {
	once   sync.Once
	opt    *AggregatorOption
	values []*OrderedValue
}

func (f *ARRAY_AGG) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return fmt.Errorf("ARRAY_AGG: input value must be not null")
	}
	f.once.Do(func() { f.opt = opt })
	f.values = append(f.values, &OrderedValue{
		OrderBy: opt.OrderBy,
		Value:   v,
	})
	return nil
}

func (f *ARRAY_AGG) Done() (Value, error) {
	f.values = sortAggregatedValues(f.values, f.opt)
	if f.opt != nil && f.opt.Limit != nil {
		minLen := int64(len(f.values))
		if *f.opt.Limit < minLen {
			minLen = *f.opt.Limit
		}
		f.values = f.values[:minLen]
	}
	values := make([]Value, 0, len(f.values))
	for _, v := range f.values {
		values = append(values, v.Value)
	}
	return &ArrayValue{
		values: values,
	}, nil
}

type ARRAY_CONCAT_AGG struct {
	once   sync.Once
	opt    *AggregatorOption
	values []*OrderedValue
}

func (f *ARRAY_CONCAT_AGG) Step(v *ArrayValue, opt *AggregatorOption) error {
	if v == nil {
		return fmt.Errorf("ARRAY_CONCAT_AGG: NULL value unsupported")
	}
	f.once.Do(func() { f.opt = opt })
	f.values = append(f.values, &OrderedValue{
		OrderBy: opt.OrderBy,
		Value:   v,
	})
	return nil
}

func (f *ARRAY_CONCAT_AGG) Done() (Value, error) {
	f.values = sortAggregatedValues(f.values, f.opt)

	if f.opt != nil && f.opt.Limit != nil {
		minLen := int64(len(f.values))
		if *f.opt.Limit < minLen {
			minLen = *f.opt.Limit
		}
		f.values = f.values[:minLen]
	}

	var values []Value
	for _, v := range f.values {
		a, err := v.Value.ToArray()
		if err != nil {
			return nil, err
		}
		values = append(values, a.values...)
	}

	return &ArrayValue{
		values: values,
	}, nil
}

type AVG struct {
	sum Value
	num int64
}

func (f *AVG) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	if f.sum == nil {
		f.sum = v
	} else {
		added, err := f.sum.Add(v)
		if err != nil {
			return err
		}
		f.sum = added
	}
	f.num++
	return nil
}

func (f *AVG) Done() (Value, error) {
	if f.sum == nil {
		return nil, nil
	}
	base, err := f.sum.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(base / float64(f.num)), nil
}

type BIT_AND_AGG struct {
	value Value
}

func (f *BIT_AND_AGG) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	i64, err := v.ToInt64()
	if err != nil {
		return err
	}
	if f.value == nil {
		f.value = IntValue(i64)
	} else {
		curI64, err := f.value.ToInt64()
		if err != nil {
			return err
		}
		f.value = IntValue(curI64 & i64)
	}
	return nil
}

func (f *BIT_AND_AGG) Done() (Value, error) {
	return f.value, nil
}

type BIT_OR_AGG struct {
	value int64
}

func (f *BIT_OR_AGG) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	i64, err := v.ToInt64()
	if err != nil {
		return err
	}
	if f.value == -1 {
		f.value = i64
	} else {
		f.value |= i64
	}
	return nil
}

func (f *BIT_OR_AGG) Done() (Value, error) {
	return IntValue(f.value), nil
}

type BIT_XOR_AGG struct {
	value int64
}

func (f *BIT_XOR_AGG) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	i64, err := v.ToInt64()
	if err != nil {
		return err
	}
	if f.value == 1 {
		f.value = i64
	} else {
		f.value ^= i64
	}
	return nil
}

func (f *BIT_XOR_AGG) Done() (Value, error) {
	return IntValue(f.value), nil
}

type COUNT struct {
	count Value
}

func (f *COUNT) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	if f.count == nil {
		f.count = IntValue(1)
	} else {
		added, err := f.count.Add(IntValue(1))
		if err != nil {
			return err
		}
		f.count = added
	}
	return nil
}

func (f *COUNT) Done() (Value, error) {
	if f.count == nil {
		return IntValue(0), nil
	}
	return f.count, nil
}

type COUNT_STAR struct {
	count int64
}

func (f *COUNT_STAR) Step(opt *AggregatorOption) error {
	f.count++
	return nil
}

func (f *COUNT_STAR) Done() (Value, error) {
	return IntValue(f.count), nil
}

type COUNTIF struct {
	count Value
}

func (f *COUNTIF) Step(cond Value, opt *AggregatorOption) error {
	if cond == nil {
		return nil
	}
	b, err := cond.ToBool()
	if err != nil {
		return err
	}
	if b {
		if f.count == nil {
			f.count = IntValue(1)
		} else {
			added, err := f.count.Add(IntValue(1))
			if err != nil {
				return err
			}
			f.count = added
		}
	}
	return nil
}

func (f *COUNTIF) Done() (Value, error) {
	if f.count == nil {
		return IntValue(0), nil
	}
	return f.count, nil
}

type LOGICAL_AND struct {
	v bool
}

func (f *LOGICAL_AND) Step(cond Value, opt *AggregatorOption) error {
	if cond == nil {
		return nil
	}
	b, err := cond.ToBool()
	if err != nil {
		return err
	}
	if !b {
		f.v = false
	}
	return nil
}

func (f *LOGICAL_AND) Done() (Value, error) {
	return BoolValue(f.v), nil
}

type LOGICAL_OR struct {
	v bool
}

func (f *LOGICAL_OR) Step(cond Value, opt *AggregatorOption) error {
	if cond == nil {
		return nil
	}
	b, err := cond.ToBool()
	if err != nil {
		return err
	}
	if b {
		f.v = true
	}
	return nil
}

func (f *LOGICAL_OR) Done() (Value, error) {
	return BoolValue(f.v), nil
}

type MAX struct {
	initialized bool
	max         Value
}

func (f *MAX) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	if f.initialized {
		cond, err := v.GT(f.max)
		if err != nil {
			return err
		}
		if cond {
			f.max = v
		}
	} else {
		f.max = v
		f.initialized = true
	}
	return nil
}

func (f *MAX) Done() (Value, error) {
	return f.max, nil
}

type MIN struct {
	initialized bool
	min         Value
}

func (f *MIN) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	if f.initialized {
		cond, err := v.LT(f.min)
		if err != nil {
			return err
		}
		if cond {
			f.min = v
		}
	} else {
		f.min = v
		f.initialized = true
	}
	return nil
}

func (f *MIN) Done() (Value, error) {
	return f.min, nil
}

type STRING_AGG struct {
	values []*OrderedValue
	delim  string
	opt    *AggregatorOption
	once   sync.Once
}

func (f *STRING_AGG) Step(v Value, delim string, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	f.once.Do(func() {
		if delim == "" {
			delim = ","
		}
		f.delim = delim
		f.opt = opt
	})
	f.values = append(f.values, &OrderedValue{
		OrderBy: opt.OrderBy,
		Value:   v,
	})
	return nil
}

func sortAggregatedValues(values []*OrderedValue, opt *AggregatorOption) []*OrderedValue {
	if opt != nil && len(opt.OrderBy) == 0 {
		return values
	}

	sort.Slice(values, func(i, j int) bool {
		for orderBy := 0; orderBy < len(values[0].OrderBy); orderBy++ {
			iV := values[i].OrderBy[orderBy].Value
			jV := values[j].OrderBy[orderBy].Value
			isAsc := values[0].OrderBy[orderBy].IsAsc
			if iV == nil {
				return isAsc
			}
			if jV == nil {
				return !isAsc
			}
			isEqual, _ := iV.EQ(jV)
			if isEqual {
				// break tie with subsequent fields
				continue
			}
			if isAsc {
				cond, _ := iV.LT(jV)
				return cond
			} else {
				cond, _ := iV.GT(jV)
				return cond
			}
		}
		return false
	})
	return values
}

func (f *STRING_AGG) Done() (Value, error) {
	f.values = sortAggregatedValues(f.values, f.opt)

	if f.opt != nil && f.opt.Limit != nil {
		minLen := int64(len(f.values))
		if *f.opt.Limit < minLen {
			minLen = *f.opt.Limit
		}
		f.values = f.values[:minLen]
	}
	values := make([]string, 0, len(f.values))

	foundNotNilValue := false
	for _, v := range f.values {
		text, err := v.Value.ToString()
		if err != nil {
			return nil, err
		}
		foundNotNilValue = true
		values = append(values, text)
	}
	if !foundNotNilValue {
		return nil, nil
	}
	return StringValue(strings.Join(values, f.delim)), nil
}

type SUM struct {
	sum Value
}

func (f *SUM) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	if f.sum == nil {
		f.sum = v
	} else {
		added, err := f.sum.Add(v)
		if err != nil {
			return err
		}
		f.sum = added
	}
	return nil
}

func (f *SUM) Done() (Value, error) {
	return f.sum, nil
}

type CORR struct {
	x []float64
	y []float64
}

func (f *CORR) Step(x, y Value, opt *AggregatorOption) error {
	if x == nil || y == nil {
		return nil
	}
	vx, err := x.ToFloat64()
	if err != nil {
		return err
	}
	vy, err := y.ToFloat64()
	if err != nil {
		return err
	}
	f.x = append(f.x, vx)
	f.y = append(f.y, vy)
	return nil
}

func (f *CORR) Done() (Value, error) {
	if len(f.x) == 0 || len(f.y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Correlation(f.x, f.y, nil)), nil
}

type COVAR_POP struct {
	x []float64
	y []float64
}

func (f *COVAR_POP) Step(x, y Value, opt *AggregatorOption) error {
	if x == nil || y == nil {
		return nil
	}
	vx, err := x.ToFloat64()
	if err != nil {
		return err
	}
	vy, err := y.ToFloat64()
	if err != nil {
		return err
	}
	f.x = append(f.x, vx)
	f.y = append(f.y, vy)
	return nil
}

func (f *COVAR_POP) Done() (Value, error) {
	if len(f.x) == 0 || len(f.y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Covariance(f.x, f.y, nil)), nil
}

type COVAR_SAMP struct {
	x []float64
	y []float64
}

func (f *COVAR_SAMP) Step(x, y Value, opt *AggregatorOption) error {
	if x == nil || y == nil {
		return nil
	}
	vx, err := x.ToFloat64()
	if err != nil {
		return err
	}
	vy, err := y.ToFloat64()
	if err != nil {
		return err
	}
	f.x = append(f.x, vx)
	f.y = append(f.y, vy)
	return nil
}

func (f *COVAR_SAMP) Done() (Value, error) {
	if len(f.x) == 0 || len(f.y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Covariance(f.x, f.y, nil)), nil
}

type STDDEV_POP struct {
	v []float64
}

func (f *STDDEV_POP) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	f64, err := v.ToFloat64()
	if err != nil {
		return err
	}
	f.v = append(f.v, f64)
	return nil
}

func (f *STDDEV_POP) Done() (Value, error) {
	if len(f.v) == 0 {
		return nil, nil
	}
	_, std := stat.PopMeanStdDev(f.v, nil)
	return FloatValue(std), nil
}

type STDDEV_SAMP struct {
	v []float64
}

func (f *STDDEV_SAMP) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	f64, err := v.ToFloat64()
	if err != nil {
		return err
	}
	f.v = append(f.v, f64)
	return nil
}

func (f *STDDEV_SAMP) Done() (Value, error) {
	if len(f.v) == 0 {
		return nil, nil
	}
	return FloatValue(stat.StdDev(f.v, nil)), nil
}

type STDDEV = STDDEV_SAMP

type VAR_POP struct {
	v []float64
}

func (f *VAR_POP) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	f64, err := v.ToFloat64()
	if err != nil {
		return err
	}
	f.v = append(f.v, f64)
	return nil
}

func (f *VAR_POP) Done() (Value, error) {
	if len(f.v) == 0 {
		return nil, nil
	}
	_, variance := stat.PopMeanVariance(f.v, nil)
	return FloatValue(variance), nil
}

type VAR_SAMP struct {
	v []float64
}

func (f *VAR_SAMP) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	f64, err := v.ToFloat64()
	if err != nil {
		return err
	}
	f.v = append(f.v, f64)
	return nil
}

func (f *VAR_SAMP) Done() (Value, error) {
	if len(f.v) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Variance(f.v, nil)), nil
}

type VARIANCE = VAR_SAMP

type APPROX_COUNT_DISTINCT struct {
	once     sync.Once
	valueMap map[string]struct{}
}

func (f *APPROX_COUNT_DISTINCT) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	f.once.Do(func() { f.valueMap = map[string]struct{}{} })
	value, err := v.ToString()
	if err != nil {
		return err
	}
	f.valueMap[value] = struct{}{}
	return nil
}

func (f *APPROX_COUNT_DISTINCT) Done() (Value, error) {
	return IntValue(len(f.valueMap)), nil
}

type APPROX_QUANTILES struct {
	once   sync.Once
	values []Value
	num    int64
}

func (f *APPROX_QUANTILES) Step(v Value, num int64, opt *AggregatorOption) error {
	f.once.Do(func() {
		f.num = num
	})
	f.values = append(f.values, v)
	return nil
}

func (f *APPROX_QUANTILES) Done() (Value, error) {
	if len(f.values) == 0 {
		return nil, nil
	}
	if f.num == 0 {
		return &ArrayValue{values: []Value{f.values[0]}}, nil
	}
	if f.num == 1 {
		return &ArrayValue{values: []Value{f.values[0], f.values[len(f.values)-1]}}, nil
	}
	ratio := float64(100) / float64(f.num)
	length := float64(len(f.values))
	quantiles := []Value{}
	for i := float64(0); i < 100; i += ratio {
		fIdx := length * (i / 100)
		idx := int64(fIdx)
		if float64(idx) < fIdx {
			idx += 1
		}
		if idx > 0 {
			quantiles = append(quantiles, f.values[idx-1])
		} else {
			quantiles = append(quantiles, f.values[idx])
		}
	}
	quantiles = append(quantiles, f.values[len(f.values)-1])
	return &ArrayValue{values: quantiles}, nil
}

type APPROX_TOP_COUNT struct {
	once     sync.Once
	valueMap map[Value]*StructValue
	num      int64
}

func (f *APPROX_TOP_COUNT) Step(v Value, num int64, opt *AggregatorOption) error {
	f.once.Do(func() {
		f.valueMap = map[Value]*StructValue{}
		f.num = num
	})
	value, exists := f.valueMap[v]
	if exists {
		cur, _ := value.values[1].ToInt64()
		value.values[1] = IntValue(cur + 1)
		value.m["count"] = IntValue(cur + 1)
	} else {
		f.valueMap[v] = &StructValue{
			keys:   []string{"value", "count"},
			values: []Value{v, IntValue(1)},
			m: map[string]Value{
				"value": v,
				"count": IntValue(1),
			},
		}
	}
	return nil
}

func (f *APPROX_TOP_COUNT) Done() (Value, error) {
	if len(f.valueMap) == 0 {
		return nil, nil
	}
	if int64(len(f.valueMap)) < f.num {
		return nil, fmt.Errorf("APPROX_TOP_COUNT: required number is larger than number of input values")
	}
	values := make([]*StructValue, 0, len(f.valueMap))
	for _, v := range f.valueMap {
		values = append(values, v)
	}
	sort.Slice(values, func(i, j int) bool {
		cond, _ := values[i].values[1].GT(values[j].values[1])
		return cond
	})
	ret := &ArrayValue{}
	for _, v := range values[:f.num] {
		ret.values = append(ret.values, v)
	}
	return ret, nil
}

type APPROX_TOP_SUM struct {
	once     sync.Once
	valueMap map[Value]*StructValue
	num      int64
}

func (f *APPROX_TOP_SUM) Step(v, weight Value, num int64, opt *AggregatorOption) error {
	f.once.Do(func() {
		f.valueMap = map[Value]*StructValue{}
		f.num = num
	})
	value, exists := f.valueMap[v]
	if exists {
		if weight != nil {
			var sum Value
			if value.values[1] == nil {
				sum = weight
			} else {
				added, err := value.values[1].Add(weight)
				if err != nil {
					return err
				}
				sum = added
			}
			value.values[1] = sum
			value.m["sum"] = sum
		}
	} else {
		f.valueMap[v] = &StructValue{
			keys:   []string{"value", "sum"},
			values: []Value{v, weight},
			m: map[string]Value{
				"value": v,
				"sum":   weight,
			},
		}
	}
	return nil
}

func (f *APPROX_TOP_SUM) Done() (Value, error) {
	if len(f.valueMap) == 0 {
		return nil, nil
	}
	if int64(len(f.valueMap)) < f.num {
		return nil, fmt.Errorf("APPROX_TOP_SUM: required number is larger than number of input values")
	}
	values := make([]*StructValue, 0, len(f.valueMap))
	for _, v := range f.valueMap {
		values = append(values, v)
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].values[1] == nil {
			return false
		}
		if values[j].values[1] == nil {
			return true
		}
		cond, _ := values[i].values[1].GT(values[j].values[1])
		return cond
	})
	ret := &ArrayValue{}
	for _, v := range values[:f.num] {
		ret.values = append(ret.values, v)
	}
	return ret, nil
}

func init() {
	_ = hll.Defaults(hll.Settings{
		Log2m:             15,
		Regwidth:          8,
		ExplicitThreshold: hll.AutoExplicitThreshold,
		SparseEnabled:     true,
	})
}

type HLL_COUNT_INIT struct {
	once sync.Once
	hll  *hll.Hll
}

func (f *HLL_COUNT_INIT) Step(input Value, precision int64, opt *AggregatorOption) (e error) {
	f.once.Do(func() {
		h, err := hll.NewHll(hll.Settings{Log2m: int(precision)})
		if err != nil {
			e = err
		}
		f.hll = &h
	})
	var v uint64
	switch input.(type) {
	case IntValue:
		s, err := input.ToString()
		if err != nil {
			return err
		}
		v = murmur3.Sum64([]byte(s))
	case *NumericValue:
		b, err := input.ToBytes()
		if err != nil {
			return err
		}
		v = murmur3.Sum64(b)
	case StringValue:
		s, err := input.ToString()
		if err != nil {
			return err
		}
		v = murmur3.Sum64([]byte(s))
	case BytesValue:
		b, err := input.ToBytes()
		if err != nil {
			return err
		}
		v = murmur3.Sum64(b)
	}
	f.hll.AddRaw(v)
	return nil
}

func (f *HLL_COUNT_INIT) Done() (Value, error) {
	if f.hll == nil {
		return nil, nil
	}
	return BytesValue(f.hll.ToBytes()), nil
}

type HLL_COUNT_MERGE struct {
	hll *hll.Hll
}

func (f *HLL_COUNT_MERGE) Step(sketch []byte, opt *AggregatorOption) error {
	h, err := hll.FromBytes(sketch)
	if err != nil {
		return err
	}
	if f.hll == nil {
		f.hll = &h
	} else {
		f.hll.Union(h)
	}
	return nil
}

func (f *HLL_COUNT_MERGE) Done() (Value, error) {
	if f.hll == nil {
		return IntValue(0), nil
	}
	return IntValue(f.hll.Cardinality()), nil
}

type HLL_COUNT_MERGE_PARTIAL struct {
	hll *hll.Hll
}

func (f *HLL_COUNT_MERGE_PARTIAL) Step(sketch []byte, opt *AggregatorOption) error {
	h, err := hll.FromBytes(sketch)
	if err != nil {
		return err
	}
	if f.hll == nil {
		f.hll = &h
	} else {
		f.hll.Union(h)
	}
	return nil
}

func (f *HLL_COUNT_MERGE_PARTIAL) Done() (Value, error) {
	if f.hll == nil {
		return nil, nil
	}
	return BytesValue(f.hll.ToBytes()), nil
}

func HLL_COUNT_EXTRACT(sketch []byte) (Value, error) {
	h, err := hll.FromBytes(sketch)
	if err != nil {
		return nil, err
	}
	return IntValue(h.Cardinality()), nil
}
