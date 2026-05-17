package types

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func arrowSchemaFor(t *testing.T, fields ...*bigqueryv2.TableFieldSchema) *arrow.Schema {
	t.Helper()
	schema, err := TableToARROW(&bigqueryv2.Table{
		Schema: &bigqueryv2.TableSchema{Fields: fields},
	})
	if err != nil {
		t.Fatalf("TableToARROW: %v", err)
	}
	return schema
}

// TestTableToARROWDecimalTypes checks that NUMERIC and BIGNUMERIC map to Arrow
// decimal types (not float64, which cannot hold their full precision).
func TestTableToARROWDecimalTypes(t *testing.T) {
	schema := arrowSchemaFor(t,
		&bigqueryv2.TableFieldSchema{Name: "num", Type: "NUMERIC", Mode: "NULLABLE"},
		&bigqueryv2.TableFieldSchema{Name: "bignum", Type: "BIGNUMERIC", Mode: "NULLABLE"},
	)

	d128, ok := schema.Field(0).Type.(*arrow.Decimal128Type)
	if !ok {
		t.Fatalf("NUMERIC mapped to %T, want *arrow.Decimal128Type", schema.Field(0).Type)
	}
	if d128.Precision != 38 || d128.Scale != 9 {
		t.Errorf("NUMERIC precision/scale = %d/%d, want 38/9", d128.Precision, d128.Scale)
	}

	d256, ok := schema.Field(1).Type.(*arrow.Decimal256Type)
	if !ok {
		t.Fatalf("BIGNUMERIC mapped to %T, want *arrow.Decimal256Type", schema.Field(1).Type)
	}
	if d256.Precision != 76 || d256.Scale != 38 {
		t.Errorf("BIGNUMERIC precision/scale = %d/%d, want 76/38", d256.Precision, d256.Scale)
	}
}

// TestAppendDecimalBoundaryValues verifies that NUMERIC/BIGNUMERIC values with
// far more significant digits than a float64 (~17) can hold survive the round
// trip through the Arrow builders exactly.
func TestAppendDecimalBoundaryValues(t *testing.T) {
	const (
		numValue    = "12345678901234567890.123456789"                                              // 29 significant digits
		bigNumValue = "1234567890123456789012345678901234567.89012345678901234567890123456789012345" // 75 significant digits
	)
	schema := arrowSchemaFor(t,
		&bigqueryv2.TableFieldSchema{Name: "num", Type: "NUMERIC", Mode: "NULLABLE"},
		&bigqueryv2.TableFieldSchema{Name: "bignum", Type: "BIGNUMERIC", Mode: "NULLABLE"},
	)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()

	num, bignum := numValue, bigNumValue
	if err := AppendValueToARROWBuilder(&num, builder.Field(0)); err != nil {
		t.Fatalf("append NUMERIC: %v", err)
	}
	if err := AppendValueToARROWBuilder(&bignum, builder.Field(1)); err != nil {
		t.Fatalf("append BIGNUMERIC: %v", err)
	}
	rec := builder.NewRecord()
	defer rec.Release()

	want128, err := decimal128.FromString(numValue, 38, 9)
	if err != nil {
		t.Fatalf("decimal128.FromString: %v", err)
	}
	if got := rec.Column(0).(*array.Decimal128).Value(0); got != want128 {
		t.Errorf("NUMERIC round-trip = %v, want %v", got, want128)
	}

	want256, err := decimal256.FromString(bigNumValue, 76, 38)
	if err != nil {
		t.Fatalf("decimal256.FromString: %v", err)
	}
	if got := rec.Column(1).(*array.Decimal256).Value(0); got != want256 {
		t.Errorf("BIGNUMERIC round-trip = %v, want %v", got, want256)
	}
}

// TestAppendDateBoundaryValues verifies the DATE encoding round-trips the
// extreme dates BigQuery allows (0001-01-01 .. 9999-12-31).
func TestAppendDateBoundaryValues(t *testing.T) {
	schema := arrowSchemaFor(t,
		&bigqueryv2.TableFieldSchema{Name: "d", Type: "DATE", Mode: "NULLABLE"},
	)
	for _, want := range []string{"0001-01-01", "1970-01-01", "9999-12-31"} {
		builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
		value := want
		if err := AppendValueToARROWBuilder(&value, builder.Field(0)); err != nil {
			builder.Release()
			t.Fatalf("append DATE %s: %v", want, err)
		}
		rec := builder.NewRecord()
		got := rec.Column(0).(*array.Date32).Value(0).ToTime().UTC().Format("2006-01-02")
		if got != want {
			t.Errorf("DATE round-trip = %s, want %s", got, want)
		}
		rec.Release()
		builder.Release()
	}
}
