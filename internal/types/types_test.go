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

func TestFormatCellRepeatedRecordUnquotesNestedDate(t *testing.T) {
	field := &bigqueryv2.TableFieldSchema{
		Type: "RECORD",
		Mode: "REPEATED",
		Fields: []*bigqueryv2.TableFieldSchema{
			{Name: "name", Type: "STRING"},
			{Name: "nested_date", Type: "DATE"},
		},
	}
	elemRow := &TableRow{
		F: []*TableCell{
			{V: "b"},
			{V: `"2025-03-29"`},
		},
	}
	cell := &TableCell{V: []*TableCell{{V: elemRow}}}

	got := formatCell(field, cell, true)
	cells, ok := got.V.([]*TableCell)
	if !ok || len(cells) != 1 {
		t.Fatalf("got.V = %#v, want a single-element []*TableCell", got.V)
	}
	gotRow, ok := cells[0].V.(*TableRow)
	if !ok {
		t.Fatalf("cells[0].V type = %T, want *TableRow", cells[0].V)
	}
	if want := "2025-03-29"; gotRow.F[1].V != want {
		t.Fatalf("nested date = %#v, want %q", gotRow.F[1].V, want)
	}
}

func TestFormatDateCellPassesThroughBareDate(t *testing.T) {
	if got := formatDateCell("2025-03-29"); got != "2025-03-29" {
		t.Fatalf("formatDateCell(bare date) = %#v, want unchanged", got)
	}
}

func TestFormatDateCellPassesThroughUnparseableValue(t *testing.T) {
	if got := formatDateCell(`"not-a-date"`); got != `"not-a-date"` {
		t.Fatalf("formatDateCell(unparseable) = %#v, want unchanged", got)
	}
}
