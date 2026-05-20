package types

import (
	"testing"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

// TestNewTableWithSchemaCaseInsensitiveColumns is a regression test for
// https://github.com/goccy/bigquery-emulator/issues/380: a row keyed with a
// different letter case than the table schema (e.g. a Storage Write API proto
// field `createdat` against a `createdAt` column) had its value silently
// dropped. BigQuery column names are case-insensitive.
func TestNewTableWithSchemaCaseInsensitiveColumns(t *testing.T) {
	table := &bigqueryv2.Table{
		TableReference: &bigqueryv2.TableReference{TableId: "t"},
		Schema: &bigqueryv2.TableSchema{
			Fields: []*bigqueryv2.TableFieldSchema{
				{Name: "createdAt", Type: "STRING"},
				{Name: "Info", Type: "RECORD", Fields: []*bigqueryv2.TableFieldSchema{
					{Name: "userName", Type: "STRING"},
				}},
			},
		},
	}
	// Every key differs in case from its schema column.
	data := Data{
		{
			"createdat": "2024-01-01",
			"info":      map[string]interface{}{"username": "alice"},
		},
	}
	got, err := NewTableWithSchema(table, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got.Data))
	}
	row := got.Data[0]

	// The scalar value must survive under the schema's canonical name.
	if row["createdAt"] != "2024-01-01" {
		t.Errorf("createdAt = %v; want \"2024-01-01\"", row["createdAt"])
	}

	// The nested STRUCT field must likewise resolve case-insensitively.
	// normalizeData renders a STRUCT as a slice of single-key maps.
	info, ok := row["Info"].([]map[string]interface{})
	if !ok || len(info) != 1 {
		t.Fatalf("Info = %#v; want one STRUCT field", row["Info"])
	}
	if info[0]["userName"] != "alice" {
		t.Errorf("Info.userName = %v; want \"alice\"", info[0]["userName"])
	}
}
