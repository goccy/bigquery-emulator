package server

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestDecodeProtoWrapperValues is a regression test for
// https://github.com/goccy/bigquery-emulator/issues/364: a Storage Write API
// proto field of a google.protobuf wrapper type (wrappers.proto) was decoded
// as a one-field STRUCT (`{"value": x}`) instead of the bare scalar `x`. The
// schema-driven normalizer cannot reconcile a STRUCT with the scalar column a
// wrapper type maps to, so the write failed.
func TestDecodeProtoWrapperValues(t *testing.T) {
	s := &storageWriteServer{}
	cases := []struct {
		name string
		msg  protoreflect.ProtoMessage
		want interface{}
	}{
		{"DoubleValue", wrapperspb.Double(1.5), 1.5},
		{"FloatValue", wrapperspb.Float(2.5), 2.5},
		{"Int64Value", wrapperspb.Int64(7), int64(7)},
		{"Int32Value", wrapperspb.Int32(8), int64(8)},
		{"UInt64Value", wrapperspb.UInt64(9), uint64(9)},
		{"UInt32Value", wrapperspb.UInt32(10), uint64(10)},
		{"BoolValue", wrapperspb.Bool(true), true},
		{"StringValue", wrapperspb.String("hi"), "hi"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := protoreflect.ValueOfMessage(tc.msg.ProtoReflect())
			got, err := s.decodeProtoReflectValueFromKind(protoreflect.MessageKind, v)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != tc.want {
				t.Fatalf("decoded %T(%v); want %T(%v) — wrapper was not unwrapped",
					got, got, tc.want, tc.want)
			}
		})
	}

	// BytesValue unwraps to a []byte, compared separately.
	t.Run("BytesValue", func(t *testing.T) {
		v := protoreflect.ValueOfMessage(wrapperspb.Bytes([]byte("ab")).ProtoReflect())
		got, err := s.decodeProtoReflectValueFromKind(protoreflect.MessageKind, v)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		b, ok := got.([]byte)
		if !ok || string(b) != "ab" {
			t.Fatalf("decoded %T(%v); want []byte(\"ab\")", got, got)
		}
	})
}
