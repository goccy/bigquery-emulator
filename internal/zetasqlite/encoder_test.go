package zetasqlite

import (
	"reflect"
	"testing"
	"time"

	"github.com/glassmonkey/zetasql-wasm/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestValueFromZetaSQLValue locks in the lift for kinds whose Go
// representation in *types.LiteralValue does not pass cleanly through
// reflect-based generic conversion: DATE (int32 days since epoch) and
// TIMESTAMP (*timestamppb.Timestamp). The lift goes through
// zetasql-wasm v0.8.0 typed accessors (AsDateDays / AsTimestamp), so
// the proto representation never leaks into this package.
func TestValueFromZetaSQLValue(t *testing.T) {
	t.Setenv("TZ", "UTC")

	tsTime := time.Date(2020, 9, 22, 12, 30, 0, 0, time.UTC)
	tsProto := timestamppb.New(tsTime)

	for _, tc := range []struct {
		name string
		sut  *types.LiteralValue
		want Value
	}{
		{
			name: "nil literal",
			sut:  nil,
			want: nil,
		},
		{
			name: "nil inner value (SQL NULL)",
			sut:  &types.LiteralValue{Type: types.DateType(), Value: nil},
			want: nil,
		},
		{
			name: "DATE int32 days lifted to DateValue",
			sut:  &types.LiteralValue{Type: types.DateType(), Value: int32(18527)},
			want: dateValueFromLiteral(18527),
		},
		{
			name: "TIMESTAMP proto lifted to TimestampValue",
			sut:  &types.LiteralValue{Type: types.TimestampType(), Value: tsProto},
			want: TimestampValue(tsTime),
		},
		{
			name: "INT64 falls through to IntValue",
			sut:  &types.LiteralValue{Type: types.Int64Type(), Value: int64(42)},
			want: IntValue(42),
		},
		{
			name: "STRING falls through to StringValue",
			sut:  &types.LiteralValue{Type: types.StringType(), Value: "hello"},
			want: StringValue("hello"),
		},
		{
			name: "BOOL falls through to BoolValue",
			sut:  &types.LiteralValue{Type: types.BoolType(), Value: true},
			want: BoolValue(true),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			got, err := ValueFromZetaSQLValue(tc.sut)

			// Assert
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}
