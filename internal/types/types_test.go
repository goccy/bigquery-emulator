package types

import (
	"fmt"
	"testing"
	"time"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TestFormatCellHandlesNilField(t *testing.T) {
	cell := &TableCell{V: "value"}
	got := formatCell(nil, cell, true)
	if got != cell {
		t.Fatalf("formatCell(nil, cell) = %#v, want same cell pointer", got)
	}
}

func TestFormatCellRecordPointerFormatsNestedTimestamp(t *testing.T) {
	field := &bigqueryv2.TableFieldSchema{
		Type: "RECORD",
		Fields: []*bigqueryv2.TableFieldSchema{
			{Type: "TIMESTAMP"},
		},
	}
	row := &TableRow{
		F: []*TableCell{{V: "2026-06-15T00:00:00Z"}},
	}
	cell := &TableCell{V: row}

	got := formatCell(field, cell, true)
	gotRow, ok := got.V.(*TableRow)
	if !ok {
		t.Fatalf("got.V type = %T, want *TableRow", got.V)
	}
	if len(gotRow.F) != 1 {
		t.Fatalf("len(gotRow.F) = %d, want 1", len(gotRow.F))
	}
	tm, err := time.Parse(time.RFC3339, "2026-06-15T00:00:00Z")
	if err != nil {
		t.Fatalf("time.Parse failed: %v", err)
	}
	want := fmt.Sprint(tm.UnixMicro())
	if gotRow.F[0].V != want {
		t.Fatalf("nested timestamp = %v, want %v", gotRow.F[0].V, want)
	}
}
