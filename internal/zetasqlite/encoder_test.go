package zetasqlite

import (
	"reflect"
	"testing"
	"time"

	"github.com/glassmonkey/zetasql-wasm/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestValueFromZetaSQLValue locks in the type-driven dispatch for kinds
// whose Go representation in *types.LiteralValue does not lift cleanly
// through reflect-based generic conversion: DATE comes back as int32
// (days since epoch), TIMESTAMP as *timestamppb.Timestamp. Without this
// dispatch DATE literals collapse to IntValue and TIMESTAMP literals
// panic in reflect when the proto's unexported fields are read.
func TestValueFromZetaSQLValue(t *testing.T) {
	t.Setenv("TZ", "UTC")

	tsTime := time.Date(2020, 9, 22, 12, 30, 0, 0, time.UTC)
	tsProto := timestamppb.New(tsTime)

	for _, tc := range []struct {
		name    string
		sut     *types.LiteralValue
		want    Value
		wantErr bool
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
			name: "DATE wrong-type value yields error",
			sut:  &types.LiteralValue{Type: types.DateType(), Value: int64(18527)},
			want: nil,
			wantErr: true,
		},
		{
			name: "TIMESTAMP proto lifted to TimestampValue",
			sut:  &types.LiteralValue{Type: types.TimestampType(), Value: tsProto},
			want: TimestampValue(tsTime),
		},
		{
			name: "TIMESTAMP wrong-type value yields error",
			sut:  &types.LiteralValue{Type: types.TimestampType(), Value: tsTime},
			want: nil,
			wantErr: true,
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
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v, wantErr %v", err, tc.wantErr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}
