package contentdata

import (
	"strings"
	"testing"
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
