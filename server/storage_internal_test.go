package server

import (
	"context"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/goccy/bigquery-emulator/types"
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

func TestDefaultWriteStreamNameForms(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "dataset"
		tableID   = "sample"
	)
	ctx := context.Background()
	bqServer, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(StructSource(types.NewProject(
		projectID,
		types.NewDataset(datasetID,
			types.NewTable(tableID,
				[]*types.Column{types.NewColumn("name", types.STRING)},
				nil,
			),
		),
	))); err != nil {
		t.Fatal(err)
	}
	defer bqServer.Close()

	writeServer := &storageWriteServer{
		server:    bqServer,
		streamMap: map[string]*writeStreamStatus{},
	}
	tablePath := "projects/test/datasets/dataset/tables/sample"
	canonicalName := tablePath + "/_default"
	var firstStatus *writeStreamStatus
	for _, streamName := range []string{
		tablePath + "/_default",
		tablePath + "/streams/_default",
	} {
		t.Run(streamName, func(t *testing.T) {
			stream, err := writeServer.createDefaultStream(ctx, &storagepb.GetWriteStreamRequest{Name: streamName})
			if err != nil {
				t.Fatalf("createDefaultStream: %v", err)
			}
			if stream.GetName() != canonicalName {
				t.Fatalf("stream name = %q; want %q", stream.GetName(), canonicalName)
			}
			status, gotName, err := writeServer.getOrCreateWriteStreamStatus(ctx, streamName)
			if err != nil {
				t.Fatalf("getOrCreateWriteStreamStatus: %v", err)
			}
			if gotName != canonicalName {
				t.Fatalf("resolved stream name = %q; want %q", gotName, canonicalName)
			}
			if firstStatus == nil {
				firstStatus = status
			} else if status != firstStatus {
				t.Fatal("default stream name forms resolved to different statuses")
			}
			if status.streamType != storagepb.WriteStream_COMMITTED {
				t.Fatalf("stream type = %v; want COMMITTED", status.streamType)
			}
			if status.projectID != projectID || status.datasetID != datasetID || status.tableID != tableID {
				t.Fatalf("status table = %s.%s.%s; want %s.%s.%s",
					status.projectID, status.datasetID, status.tableID,
					projectID, datasetID, tableID)
			}
		})
	}
	if got := len(writeServer.streamMap); got != 1 {
		t.Fatalf("default stream map entries = %d; want 1", got)
	}
}

func TestDefaultWriteStreamLazyResolverConsumers(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "dataset"
		tableID   = "sample"
	)
	ctx := context.Background()
	bqServer, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(StructSource(types.NewProject(
		projectID,
		types.NewDataset(datasetID,
			types.NewTable(tableID,
				[]*types.Column{types.NewColumn("name", types.STRING)},
				nil,
			),
		),
	))); err != nil {
		t.Fatal(err)
	}
	defer bqServer.Close()

	writeServer := &storageWriteServer{
		server:    bqServer,
		streamMap: map[string]*writeStreamStatus{},
	}
	streamName := "projects/test/datasets/dataset/tables/sample/streams/_default"
	if _, err := writeServer.FinalizeWriteStream(ctx, &storagepb.FinalizeWriteStreamRequest{Name: streamName}); err == nil {
		t.Fatal("FinalizeWriteStream should not create a missing default stream")
	}
	if got := len(writeServer.streamMap); got != 0 {
		t.Fatalf("default stream map entries after finalize = %d; want 0", got)
	}

	stream, err := writeServer.GetWriteStream(ctx, &storagepb.GetWriteStreamRequest{Name: streamName})
	if err != nil {
		t.Fatalf("GetWriteStream: %v", err)
	}
	if stream.GetName() != "projects/test/datasets/dataset/tables/sample/_default" {
		t.Fatalf("stream name = %q; want canonical default stream", stream.GetName())
	}
	if got := len(writeServer.streamMap); got != 1 {
		t.Fatalf("default stream map entries = %d; want 1", got)
	}
}
