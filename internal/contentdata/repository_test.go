package contentdata

import (
	"strings"
	"testing"

	"github.com/goccy/googlesqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TestValidateIdent(t *testing.T) {
	valid := []string{
		"my_table",
		"Dataset123",
		"project-id",
		"name with spaces",
		"日本語テーブル",
		"",
	}
	for _, id := range valid {
		if err := validateIdent("table", id); err != nil {
			t.Errorf("validateIdent(%q) returned error, want nil: %v", id, err)
		}
	}

	invalid := []string{
		"tbl`",
		"`; DROP TABLE x; --",
		"tbl\\",
		"a\\`; DROP TABLE victim; --",
	}
	for _, id := range invalid {
		if err := validateIdent("table", id); err == nil {
			t.Errorf("validateIdent(%q) returned nil, want error", id)
		}
	}
}

func TestTablePathRejectsInjection(t *testing.T) {
	r := NewRepository()

	got, err := r.tablePath("proj", "ds", "tbl")
	if err != nil {
		t.Fatalf("tablePath returned unexpected error: %v", err)
	}
	if want := "proj.ds.tbl"; got != want {
		t.Errorf("tablePath = %q, want %q", got, want)
	}

	// The payload below escaped the backtick-quoted identifier and dropped a
	// table when escaping alone was relied upon; it must now be rejected
	// before any SQL is built.
	cases := []struct{ project, dataset, table string }{
		{"a`; DROP TABLE victim; --", "ds", "tbl"},
		{"proj", "d`s", "tbl"},
		{"proj", "ds", "a\\`; DROP TABLE victim; --"},
	}
	for _, c := range cases {
		if _, err := r.tablePath(c.project, c.dataset, c.table); err == nil {
			t.Errorf("tablePath(%q, %q, %q) returned nil, want error", c.project, c.dataset, c.table)
		}
	}
}

func TestRoutinePathRejectsInjection(t *testing.T) {
	r := NewRepository()

	got, err := r.routinePath("proj", "ds", "fn")
	if err != nil {
		t.Fatalf("routinePath returned unexpected error: %v", err)
	}
	if want := "proj.ds.fn"; got != want {
		t.Errorf("routinePath = %q, want %q", got, want)
	}

	if _, err := r.routinePath("proj", "ds", "fn`; DROP TABLE victim; --"); err == nil {
		t.Error("routinePath with a backtick in the routine ID returned nil, want error")
	}
}

func TestEscapeIdent(t *testing.T) {
	if got, want := escapeIdent("plain"), "plain"; got != want {
		t.Errorf("escapeIdent(plain) = %q, want %q", got, want)
	}
	if got := escapeIdent("a`b"); !strings.Contains(got, "``") {
		t.Errorf("escapeIdent(a`b) = %q, want a doubled backtick", got)
	}
}

// TestCoerceScalarParameterValue covers coerceScalarParameterValue's per-type
// branches, including the DATE / NUMERIC / BIGNUMERIC cases this fix adds.
func TestCoerceScalarParameterValue(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		got, err := coerceScalarParameterValue("42", "INT64")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != int64(42) {
			t.Errorf("got %v (%T), want int64(42)", got, got)
		}
	})

	t.Run("date", func(t *testing.T) {
		got, err := coerceScalarParameterValue("2026-09-15", "DATE")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(googlesqlite.Date); !ok {
			t.Fatalf("got %v (%T), want googlesqlite.Date", got, got)
		}
	})

	t.Run("date empty string is NULL", func(t *testing.T) {
		got, err := coerceScalarParameterValue("", "DATE")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("got %v, want nil (NULL)", got)
		}
	})

	t.Run("date malformed value is an error, not a silent STRING passthrough", func(t *testing.T) {
		if _, err := coerceScalarParameterValue("not-a-date", "DATE"); err == nil {
			t.Error("expected an error for a malformed DATE value")
		}
	})

	t.Run("numeric", func(t *testing.T) {
		got, err := coerceScalarParameterValue("123.456", "NUMERIC")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		n, ok := got.(*googlesqlite.Numeric)
		if !ok {
			t.Fatalf("got %v (%T), want *googlesqlite.Numeric", got, got)
		}
		if n.IsBigNumeric {
			t.Error("NUMERIC parameter must not set IsBigNumeric")
		}
	})

	t.Run("bignumeric", func(t *testing.T) {
		got, err := coerceScalarParameterValue("123456789012345678901234567890.123456789", "BIGNUMERIC")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		n, ok := got.(*googlesqlite.Numeric)
		if !ok {
			t.Fatalf("got %v (%T), want *googlesqlite.Numeric", got, got)
		}
		if !n.IsBigNumeric {
			t.Error("BIGNUMERIC parameter must set IsBigNumeric")
		}
	})

	t.Run("numeric malformed value is an error", func(t *testing.T) {
		if _, err := coerceScalarParameterValue("not-a-number", "NUMERIC"); err == nil {
			t.Error("expected an error for a malformed NUMERIC value")
		}
	})

	t.Run("unknown type passes the string through unchanged", func(t *testing.T) {
		got, err := coerceScalarParameterValue("abc", "SOME_FUTURE_TYPE")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "abc" {
			t.Errorf("got %v, want the original string unchanged", got)
		}
	})
}

// TestQueryParameterValueToGoValue_ArrayOfDates is the regression for the
// bug this fix addresses: an ARRAY<DATE> parameter's elements must each be
// coerced to googlesqlite.Date, not left as the raw JSON strings the
// BigQuery REST API sends. Before this fix, coerceScalarParameterValue ran
// once on the top-level ARRAY value (a no-op — arrays aren't strings), so
// every element reached the query as an unconverted string and googlesqlite
// inferred ARRAY<STRING> instead of the declared ARRAY<DATE> — the root
// cause of `date_col IN UNNEST(@dates)` failing to analyze.
func TestQueryParameterValueToGoValue_ArrayOfDates(t *testing.T) {
	r := NewRepository()
	ptype := &bigqueryv2.QueryParameterType{
		Type:      "ARRAY",
		ArrayType: &bigqueryv2.QueryParameterType{Type: "DATE"},
	}
	pvalue := &bigqueryv2.QueryParameterValue{
		ArrayValues: []*bigqueryv2.QueryParameterValue{
			{Value: "2026-09-14"},
			{Value: "2026-09-15"},
		},
	}

	got, err := r.queryParameterValueToGoValue(ptype, pvalue)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := got.([]interface{})
	if !ok {
		t.Fatalf("got %v (%T), want []interface{}", got, got)
	}
	if len(arr) != 2 {
		t.Fatalf("got %d elements, want 2", len(arr))
	}
	for i, elem := range arr {
		if _, ok := elem.(googlesqlite.Date); !ok {
			t.Errorf("element %d: got %v (%T), want googlesqlite.Date — "+
				"elements are still reaching the query as raw strings", i, elem, elem)
		}
	}
}
