package internal

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/goccy/go-json"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

type WindowFuncOptionType string

const (
	WindowFuncOptionUnknown   WindowFuncOptionType = "window_unknown"
	WindowFuncOptionFrameUnit WindowFuncOptionType = "window_frame_unit"
	WindowFuncOptionStart     WindowFuncOptionType = "window_boundary_start"
	WindowFuncOptionEnd       WindowFuncOptionType = "window_boundary_end"
	WindowFuncOptionPartition WindowFuncOptionType = "window_partition"
	WindowFuncOptionRowID     WindowFuncOptionType = "window_rowid"
	WindowFuncOptionOrderBy   WindowFuncOptionType = "window_order_by"
)

type WindowFuncOption struct {
	Type  WindowFuncOptionType `json:"type"`
	Value interface{}          `json:"value"`
}

func (o *WindowFuncOption) UnmarshalJSON(b []byte) error {
	type windowFuncOption WindowFuncOption

	var v windowFuncOption
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	o.Type = v.Type
	switch v.Type {
	case WindowFuncOptionFrameUnit:
		var value struct {
			Value WindowFrameUnitType `json:"value"`
		}
		if err := json.Unmarshal(b, &value); err != nil {
			return err
		}
		o.Value = value.Value
	case WindowFuncOptionStart, WindowFuncOptionEnd:
		var value struct {
			Value *WindowBoundary `json:"value"`
		}
		if err := json.Unmarshal(b, &value); err != nil {
			return err
		}
		o.Value = value.Value
	case WindowFuncOptionRowID:
		var value struct {
			Value int64 `json:"value"`
		}
		if err := json.Unmarshal(b, &value); err != nil {
			return err
		}
		o.Value = value.Value
	case WindowFuncOptionPartition:
		value, err := DecodeValue(v.Value)
		if err != nil {
			return fmt.Errorf("failed to convert %v to Value: %w", v.Value, err)
		}
		o.Value = value
	case WindowFuncOptionOrderBy:
		var value struct {
			Value *WindowOrderBy `json:"value"`
		}
		if err := json.Unmarshal(b, &value); err != nil {
			return err
		}
		o.Value = value.Value
	}
	return nil
}

type WindowFrameUnitType int

const (
	WindowFrameUnitUnknown WindowFrameUnitType = 0
	WindowFrameUnitRows    WindowFrameUnitType = 1
	WindowFrameUnitRange   WindowFrameUnitType = 2
)

type WindowBoundaryType int

const (
	WindowBoundaryTypeUnknown    WindowBoundaryType = 0
	WindowUnboundedPrecedingType WindowBoundaryType = 1
	WindowOffsetPrecedingType    WindowBoundaryType = 2
	WindowCurrentRowType         WindowBoundaryType = 3
	WindowOffsetFollowingType    WindowBoundaryType = 4
	WindowUnboundedFollowingType WindowBoundaryType = 5
)

type WindowBoundary struct {
	Type   WindowBoundaryType `json:"type"`
	Offset int64              `json:"offset"`
}

func getWindowFrameUnitOptionFuncSQL(frameUnit ast.FrameUnit) string {
	var typ WindowFrameUnitType
	switch frameUnit {
	case ast.FrameUnitRows:
		typ = WindowFrameUnitRows
	case ast.FrameUnitRange:
		typ = WindowFrameUnitRange
	}
	return fmt.Sprintf("zetasqlite_window_frame_unit(%d)", typ)
}

func toWindowBoundaryType(boundaryType ast.BoundaryType) WindowBoundaryType {
	switch boundaryType {
	case ast.UnboundedPrecedingType:
		return WindowUnboundedPrecedingType
	case ast.OffsetPrecedingType:
		return WindowOffsetPrecedingType
	case ast.CurrentRowType:
		return WindowCurrentRowType
	case ast.OffsetFollowingType:
		return WindowOffsetFollowingType
	case ast.UnboundedFollowingType:
		return WindowUnboundedFollowingType
	}
	return WindowBoundaryTypeUnknown
}

func getWindowBoundaryStartOptionFuncSQL(boundaryType ast.BoundaryType, offset string) string {
	typ := toWindowBoundaryType(boundaryType)
	if offset == "" {
		offset = "0"
	}
	return fmt.Sprintf("zetasqlite_window_boundary_start(%d, %s)", typ, offset)
}

func getWindowBoundaryEndOptionFuncSQL(boundaryType ast.BoundaryType, offset string) string {
	typ := toWindowBoundaryType(boundaryType)
	if offset == "" {
		offset = "0"
	}
	return fmt.Sprintf("zetasqlite_window_boundary_end(%d, %s)", typ, offset)
}

func getWindowPartitionOptionFuncSQL(column string) string {
	return fmt.Sprintf("zetasqlite_window_partition(%s)", column)
}

func getWindowRowIDOptionFuncSQL() string {
	return "zetasqlite_window_rowid(`row_id`)"
}

func getWindowOrderByOptionFuncSQL(column string, isAsc bool) string {
	return fmt.Sprintf("zetasqlite_window_order_by(%s, %t)", column, isAsc)
}

func WINDOW_FRAME_UNIT(frameUnit int64) (Value, error) {
	b, err := json.Marshal(&WindowFuncOption{
		Type:  WindowFuncOptionFrameUnit,
		Value: frameUnit,
	})
	if err != nil {
		return nil, err
	}
	return StringValue(string(b)), nil
}

func WINDOW_BOUNDARY_START(boundaryType, offset int64) (Value, error) {
	b, err := json.Marshal(&WindowFuncOption{
		Type: WindowFuncOptionStart,
		Value: &WindowBoundary{
			Type:   WindowBoundaryType(boundaryType),
			Offset: offset,
		},
	})
	if err != nil {
		return nil, err
	}
	return StringValue(string(b)), nil
}

func WINDOW_BOUNDARY_END(boundaryType, offset int64) (Value, error) {
	b, err := json.Marshal(&WindowFuncOption{
		Type: WindowFuncOptionEnd,
		Value: &WindowBoundary{
			Type:   WindowBoundaryType(boundaryType),
			Offset: offset,
		},
	})
	if err != nil {
		return nil, err
	}
	return StringValue(string(b)), nil
}

func WINDOW_PARTITION(partition Value) (Value, error) {
	v, err := EncodeValue(partition)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(&WindowFuncOption{
		Type:  WindowFuncOptionPartition,
		Value: v,
	})
	if err != nil {
		return nil, err
	}
	return StringValue(string(b)), nil
}

func WINDOW_ROWID(id int64) (Value, error) {
	b, err := json.Marshal(&WindowFuncOption{
		Type:  WindowFuncOptionRowID,
		Value: id,
	})
	if err != nil {
		return nil, err
	}
	return StringValue(string(b)), nil
}

type WindowOrderBy struct {
	Value Value `json:"value"`
	IsAsc bool  `json:"isAsc"`
}

func (w *WindowOrderBy) UnmarshalJSON(b []byte) error {
	var v struct {
		Value interface{} `json:"value"`
		IsAsc bool        `json:"isAsc"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	value, err := DecodeValue(v.Value)
	if err != nil {
		return err
	}
	w.Value = value
	w.IsAsc = v.IsAsc
	return nil
}

func WINDOW_ORDER_BY(value Value, isAsc bool) (Value, error) {
	v, err := EncodeValue(value)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(&WindowFuncOption{
		Type: WindowFuncOptionOrderBy,
		Value: struct {
			Value interface{} `json:"value"`
			IsAsc bool        `json:"isAsc"`
		}{
			Value: v,
			IsAsc: isAsc,
		},
	})
	if err != nil {
		return nil, err
	}
	return StringValue(string(b)), nil
}

type WindowFuncStatus struct {
	FrameUnit  WindowFrameUnitType
	Start      *WindowBoundary
	End        *WindowBoundary
	Partitions []Value
	RowID      int64
	OrderBy    []*WindowOrderBy
}

func (s *WindowFuncStatus) Partition() (string, error) {
	partitions := make([]string, 0, len(s.Partitions))
	for _, p := range s.Partitions {
		text, err := p.ToString()
		if err != nil {
			return "", err
		}
		partitions = append(partitions, text)
	}
	return strings.Join(partitions, "_"), nil
}

func parseWindowOptions(args ...Value) ([]Value, *WindowFuncStatus) {
	var (
		filteredArgs []Value
		opt          = &WindowFuncStatus{}
	)
	for _, arg := range args {
		if arg == nil {
			filteredArgs = append(filteredArgs, nil)
			continue
		}
		text, err := arg.ToString()
		if err != nil {
			filteredArgs = append(filteredArgs, arg)
			continue
		}
		var v WindowFuncOption
		if err := json.Unmarshal([]byte(text), &v); err != nil {
			filteredArgs = append(filteredArgs, arg)
			continue
		}
		switch v.Type {
		case WindowFuncOptionFrameUnit:
			opt.FrameUnit = v.Value.(WindowFrameUnitType)
		case WindowFuncOptionStart:
			opt.Start = v.Value.(*WindowBoundary)
		case WindowFuncOptionEnd:
			opt.End = v.Value.(*WindowBoundary)
		case WindowFuncOptionPartition:
			opt.Partitions = append(opt.Partitions, v.Value.(Value))
		case WindowFuncOptionRowID:
			opt.RowID = v.Value.(int64)
		case WindowFuncOptionOrderBy:
			opt.OrderBy = append(opt.OrderBy, v.Value.(*WindowOrderBy))
		default:
			filteredArgs = append(filteredArgs, arg)
			continue
		}
	}
	return filteredArgs, opt
}

type WindowOrderedValue struct {
	OrderBy []*WindowOrderBy
	Value   Value
}

type PartitionedValue struct {
	Partition string
	Value     *WindowOrderedValue
}

type WindowFuncAggregatedStatus struct {
	FrameUnit            WindowFrameUnitType
	Start                *WindowBoundary
	End                  *WindowBoundary
	RowID                int64
	once                 sync.Once
	PartitionToValuesMap map[string][]*WindowOrderedValue
	PartitionedValues    []*PartitionedValue
	Values               []*WindowOrderedValue
	SortedValues         []*WindowOrderedValue
	opt                  *AggregatorOption
}

func newWindowFuncAggregatedStatus() *WindowFuncAggregatedStatus {
	return &WindowFuncAggregatedStatus{
		PartitionToValuesMap: map[string][]*WindowOrderedValue{},
	}
}

func (s *WindowFuncAggregatedStatus) Step(value Value, status *WindowFuncStatus) error {
	s.once.Do(func() {
		s.FrameUnit = status.FrameUnit
		s.Start = status.Start
		s.End = status.End
		s.RowID = status.RowID
	})
	if s.FrameUnit != status.FrameUnit {
		return fmt.Errorf("mismatch frame unit type %d != %d", s.FrameUnit, status.FrameUnit)
	}
	if s.Start != nil {
		if s.Start.Type != status.Start.Type {
			return fmt.Errorf("mismatch boundary type %d != %d", s.Start.Type, status.Start.Type)
		}
	}
	if s.End != nil {
		if s.End.Type != status.End.Type {
			return fmt.Errorf("mismatch boundary type %d != %d", s.End.Type, status.End.Type)
		}
	}
	if s.RowID != status.RowID {
		return fmt.Errorf("mismatch rowid %d != %d", s.RowID, status.RowID)
	}
	v := &WindowOrderedValue{
		OrderBy: status.OrderBy,
		Value:   value,
	}
	if len(status.Partitions) != 0 {
		partition, err := status.Partition()
		if err != nil {
			return fmt.Errorf("failed to get partition: %w", err)
		}
		s.PartitionToValuesMap[partition] = append(s.PartitionToValuesMap[partition], v)
		s.PartitionedValues = append(s.PartitionedValues, &PartitionedValue{
			Partition: partition,
			Value:     v,
		})
	}
	s.Values = append(s.Values, v)
	return nil
}

func (s *WindowFuncAggregatedStatus) Done(cb func([]Value, int, int) error) error {
	if s.RowID <= 0 {
		return fmt.Errorf("invalid rowid. rowid must be greater than zero")
	}
	values := s.FilteredValues()
	sortedValues := make([]*WindowOrderedValue, len(values))
	copy(sortedValues, values)
	if len(sortedValues) != 0 {
		sort.Slice(sortedValues, func(i, j int) bool {
			for orderBy := 0; orderBy < len(sortedValues[0].OrderBy); orderBy++ {
				iV := sortedValues[i].OrderBy[orderBy].Value
				jV := sortedValues[j].OrderBy[orderBy].Value
				isAsc := sortedValues[0].OrderBy[orderBy].IsAsc
				if iV == nil {
					return true
				}
				if jV == nil {
					return false
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
	}
	s.SortedValues = sortedValues
	start, err := s.getIndexFromBoundary(s.Start)
	if err != nil {
		return fmt.Errorf("failed to get start index: %w", err)
	}
	end, err := s.getIndexFromBoundary(s.End)
	if err != nil {
		return fmt.Errorf("failed to get end index: %w", err)
	}
	resultValues := make([]Value, 0, len(sortedValues))
	for _, value := range sortedValues {
		resultValues = append(resultValues, value.Value)
	}
	if start >= len(resultValues) || end < 0 {
		return nil
	}
	if start < 0 {
		start = 0
	}
	if end >= len(resultValues) {
		end = len(resultValues) - 1
	}
	return cb(resultValues, start, end)
}

func (s *WindowFuncAggregatedStatus) IgnoreNulls() bool {
	return s.opt.IgnoreNulls
}

func (s *WindowFuncAggregatedStatus) Distinct() bool {
	return s.opt.Distinct
}

func (s *WindowFuncAggregatedStatus) FilteredValues() []*WindowOrderedValue {
	if len(s.PartitionedValues) != 0 {
		return s.PartitionToValuesMap[s.Partition()]
	}
	return s.Values
}

func (s *WindowFuncAggregatedStatus) Partition() string {
	return s.PartitionedValues[s.RowID-1].Partition
}

func (s *WindowFuncAggregatedStatus) getIndexFromBoundary(boundary *WindowBoundary) (int, error) {
	switch s.FrameUnit {
	case WindowFrameUnitRows:
		return s.getIndexFromBoundaryByRows(boundary)
	case WindowFrameUnitRange:
		return s.getIndexFromBoundaryByRange(boundary)
	default:
		return s.currentIndexByRows()
	}
}

func (s *WindowFuncAggregatedStatus) getIndexFromBoundaryByRows(boundary *WindowBoundary) (int, error) {
	switch boundary.Type {
	case WindowUnboundedPrecedingType:
		return 0, nil
	case WindowCurrentRowType:
		return s.currentIndexByRows()
	case WindowUnboundedFollowingType:
		return len(s.FilteredValues()) - 1, nil
	case WindowOffsetPrecedingType:
		cur, err := s.currentIndexByRows()
		if err != nil {
			return 0, err
		}
		return cur - int(boundary.Offset), nil
	case WindowOffsetFollowingType:
		cur, err := s.currentIndexByRows()
		if err != nil {
			return 0, err
		}
		return cur + int(boundary.Offset), nil
	}
	return 0, fmt.Errorf("unsupported boundary type %d", boundary.Type)
}

func (s *WindowFuncAggregatedStatus) currentIndexByRows() (int, error) {
	if len(s.PartitionedValues) != 0 {
		return s.partitionedCurrentIndexByRows()
	}
	curRowID := int(s.RowID - 1)
	curValue := s.Values[curRowID]
	for idx, value := range s.SortedValues {
		if value == curValue {
			return idx, nil
		}
	}
	return 0, fmt.Errorf("failed to find current index")
}

func (s *WindowFuncAggregatedStatus) partitionedCurrentIndexByRows() (int, error) {
	curRowID := int(s.RowID - 1)
	curValue := s.PartitionedValues[curRowID]
	for idx, value := range s.SortedValues {
		if value == curValue.Value {
			return idx, nil
		}
	}
	return 0, fmt.Errorf("failed to find current index")
}

func (s *WindowFuncAggregatedStatus) getIndexFromBoundaryByRange(boundary *WindowBoundary) (int, error) {
	switch boundary.Type {
	case WindowUnboundedPrecedingType:
		return 0, nil
	case WindowUnboundedFollowingType:
		return len(s.FilteredValues()) - 1, nil
	case WindowCurrentRowType:
		value, err := s.currentRangeValue()
		if err != nil {
			return 0, err
		}
		return s.lookupMaxIndexFromRangeValue(value)
	case WindowOffsetPrecedingType:
		value, err := s.currentRangeValue()
		if err != nil {
			return 0, err
		}
		sub, err := value.Sub(IntValue(boundary.Offset))
		if err != nil {
			return 0, err
		}
		return s.lookupMinIndexFromRangeValue(sub)
	case WindowOffsetFollowingType:
		value, err := s.currentRangeValue()
		if err != nil {
			return 0, err
		}
		add, err := value.Add(IntValue(boundary.Offset))
		if err != nil {
			return 0, err
		}
		return s.lookupMaxIndexFromRangeValue(add)
	}
	return 0, fmt.Errorf("unsupported boundary type %d", boundary.Type)
}

func (s *WindowFuncAggregatedStatus) currentRangeValue() (Value, error) {
	if len(s.PartitionedValues) != 0 {
		return s.partitionedCurrentRangeValue()
	}
	curRowID := int(s.RowID - 1)
	curValue := s.Values[curRowID]
	if len(curValue.OrderBy) == 0 {
		return nil, fmt.Errorf("required order by column for analytic range scanning")
	}
	return curValue.OrderBy[len(curValue.OrderBy)-1].Value, nil
}

func (s *WindowFuncAggregatedStatus) partitionedCurrentRangeValue() (Value, error) {
	curRowID := int(s.RowID - 1)
	curValue := s.PartitionedValues[curRowID]
	if len(curValue.Value.OrderBy) == 0 {
		return nil, fmt.Errorf("required order by column for analytic range scanning")
	}
	return curValue.Value.OrderBy[len(curValue.Value.OrderBy)-1].Value, nil
}

func (s *WindowFuncAggregatedStatus) lookupMinIndexFromRangeValue(rangeValue Value) (int, error) {
	minIndex := -1
	for idx := len(s.SortedValues) - 1; idx >= 0; idx-- {
		value := s.SortedValues[idx]
		if len(value.OrderBy) == 0 {
			continue
		}
		target := value.OrderBy[len(value.OrderBy)-1].Value
		cond, err := rangeValue.LTE(target)
		if err != nil {
			return 0, err
		}
		if cond {
			minIndex = idx
		}
	}
	return minIndex, nil
}

func (s *WindowFuncAggregatedStatus) lookupMaxIndexFromRangeValue(rangeValue Value) (int, error) {
	maxIndex := -1
	for idx := 0; idx < len(s.SortedValues); idx++ {
		value := s.SortedValues[idx]
		if len(value.OrderBy) == 0 {
			continue
		}
		target := value.OrderBy[len(value.OrderBy)-1].Value
		cond, err := rangeValue.GTE(target)
		if err != nil {
			return 0, err
		}
		if cond {
			maxIndex = idx
		}
	}
	return maxIndex, nil
}
