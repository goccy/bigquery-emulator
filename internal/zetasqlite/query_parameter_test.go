package zetasqlite

import (
	"database/sql"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestNamedQueryParameter locks in the fix for the boot regression reported
// after the zetasql-wasm v0.5.0 migration: named query parameters were not
// registered with the analyzer, so any query referencing @name failed with
// "Query parameter 'name' not found" before the metadata layer could
// complete its first SELECT and the server never reached Listen.
func TestNamedQueryParameter(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		args  []any
		want  []string
	}{
		{
			name:  "from unnest string array",
			query: "SELECT x FROM UNNEST(@ids) AS x",
			args:  []any{sql.Named("ids", []string{"a", "b", "c"})},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "in unnest mirrors metadata layer",
			query: `SELECT id FROM (SELECT "a" AS id UNION ALL SELECT "b" UNION ALL SELECT "c") WHERE id IN UNNEST(@ids)`,
			args:  []any{sql.Named("ids", []string{"a", "c"})},
			want:  []string{"a", "c"},
		},
		{
			name:  "edge: empty array yields no rows",
			query: "SELECT x FROM UNNEST(@ids) AS x",
			args:  []any{sql.Named("ids", []string{})},
			want:  nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			db := openTestDB(t)

			// Act
			rows, err := db.QueryContext(t.Context(), tc.query, tc.args...)
			if err != nil {
				t.Fatalf("QueryContext: %v", err)
			}
			defer rows.Close()

			// Assert
			got := scanStringRows(t, rows)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("(-want +got):\n%s", diff)
			}
		})
	}
}

// TestNamedQueryParameter_InUnnestPredicate is the production-shape regression
// test for the boot blocker: it replays internal/metadata/repository.go's
// findProjects query against a small table seeded with INSERT VALUES, which
// is the exact path that fails on a fresh server.
func TestNamedQueryParameter_InUnnestPredicate(t *testing.T) {
	for _, tc := range []struct {
		name string
		ids  []string
		want []string
	}{
		{
			name: "subset matches",
			ids:  []string{"a", "c"},
			want: []string{"a", "c"},
		},
		{
			name: "all match",
			ids:  []string{"a", "b", "c"},
			want: []string{"a", "b", "c"},
		},
		{
			name: "no match yields no rows",
			ids:  []string{"x", "y"},
			want: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			db := openTestDBWithABCTable(t)

			// Act
			rows, err := db.QueryContext(t.Context(),
				"SELECT id FROM t WHERE id IN UNNEST(@ids)",
				sql.Named("ids", tc.ids))
			if err != nil {
				t.Fatalf("QueryContext: %v", err)
			}
			defer rows.Close()

			// Assert
			got := scanStringRows(t, rows)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("(-want +got):\n%s", diff)
			}
		})
	}
}

// TestPositionalQueryParameter mirrors TestNamedQueryParameter for the "?"
// flavour, which additionally requires the analyzer to disable
// AllowUndeclaredParameters mode (ZetaSQL forbids combining undeclared
// parameters with positional binding).
func TestPositionalQueryParameter(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		args  []any
		want  []string
	}{
		{
			name:  "from unnest string array",
			query: "SELECT x FROM UNNEST(?) AS x",
			args:  []any{[]string{"alpha", "beta"}},
			want:  []string{"alpha", "beta"},
		},
		{
			name:  "edge: empty array yields no rows",
			query: "SELECT x FROM UNNEST(?) AS x",
			args:  []any{[]string{}},
			want:  nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			db := openTestDB(t)

			// Act
			rows, err := db.QueryContext(t.Context(), tc.query, tc.args...)
			if err != nil {
				t.Fatalf("QueryContext: %v", err)
			}
			defer rows.Close()

			// Assert
			got := scanStringRows(t, rows)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("(-want +got):\n%s", diff)
			}
		})
	}
}

// openTestDB returns a per-test in-memory zetasqlite DB, registered for
// cleanup. Trivial: no branches besides the open guard.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// openTestDBWithABCTable returns a per-test in-memory zetasqlite DB seeded
// with `t (id STRING)` containing 'a','b','c'. Mirrors the minimal shape
// of the metadata layer's `projects` table that triggers the boot path.
// Trivial: linear setup, no branches besides the open / exec guards.
func openTestDBWithABCTable(t *testing.T) *sql.DB {
	t.Helper()
	db := openTestDB(t)
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE t (id STRING)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), "INSERT INTO t (id) VALUES ('a'), ('b'), ('c')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	return db
}

// scanStringRows drains rows as a single STRING column. Trivial per R9: no
// branches, no fallback return, no formatting transform.
func scanStringRows(t *testing.T, rows *sql.Rows) []string {
	t.Helper()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return out
}
