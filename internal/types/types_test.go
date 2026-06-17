package types

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestAppendValueToARROWBuilder_List is a regression test for issue #399.
// AppendValueToARROWBuilder was calling listBuilder.Append(true) inside the
// per-element loop instead of once per row, producing N list slots instead of
// 1 and causing a row-count mismatch panic in array.RecordBuilder.NewRecord.
func TestAppendValueToARROWBuilder_List(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
	}, nil)
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()

	listBldr := rb.Field(0).(*array.ListBuilder)

	rows := []struct {
		cells   []*TableCell
		wantLen int
	}{
		{
			cells:   []*TableCell{{V: "1"}, {V: "2"}, {V: "3"}},
			wantLen: 3,
		},
		{
			cells:   []*TableCell{},
			wantLen: 0,
		},
		{
			cells:   []*TableCell{{V: "4"}},
			wantLen: 1,
		},
		{
			// A nil []*TableCell is a typed nil stored in the interface V field.
			// BigQuery has no null-array concept for REPEATED columns, so nil
			// must behave identically to an empty slice: a valid, non-null,
			// zero-length list slot.
			cells:   nil,
			wantLen: 0,
		},
	}

	for _, row := range rows {
		cell := &TableCell{V: row.cells}
		if err := cell.AppendValueToARROWBuilder(listBldr); err != nil {
			t.Fatalf("AppendValueToARROWBuilder: %v", err)
		}
	}

	// NewRecord panics (row-count mismatch) when the bug is present.
	rec := rb.NewRecord()
	defer rec.Release()

	if got := rec.NumRows(); got != int64(len(rows)) {
		t.Fatalf("NumRows = %d, want %d", got, len(rows))
	}

	col := rec.Column(0).(*array.List)
	for i, row := range rows {
		start, end := col.ValueOffsets(i)
		if got := int(end - start); got != row.wantLen {
			t.Errorf("row %d: list length = %d, want %d", i, got, row.wantLen)
		}
	}

	// Verify actual element values in the first row (cells "1","2","3" → 1,2,3).
	valCol := col.ListValues().(*array.Int64)
	start, end := col.ValueOffsets(0)
	wantVals := []int64{1, 2, 3}
	if got := int(end - start); got != len(wantVals) {
		t.Fatalf("row 0 element count = %d, want %d", got, len(wantVals))
	}
	for i, wv := range wantVals {
		if got := valCol.Value(int(start) + i); got != wv {
			t.Errorf("row 0 element %d = %d, want %d", i, got, wv)
		}
	}
}
