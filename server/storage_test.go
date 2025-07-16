// FYI: https://cloud.google.com/bigquery/docs/reference/storage/libraries?hl=ja#client-libraries-usage-go
package server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/GoogleCloudPlatform/golang-samples/bigquery/snippets/managedwriter/exampleproto"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/go-json"
	gax "github.com/googleapis/gax-go/v2"
	goavro "github.com/linkedin/goavro/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/goccy/bigquery-emulator/types"
)

var (
	rpcOpts = gax.WithGRPCOptions(
		grpc.MaxCallRecvMsgSize(1024 * 1024 * 129),
	)
	outputColumns = []string{"id", "name", "structarr", "birthday", "skillNum", "created_at"}
)

func TestStorageReadAVRO(t *testing.T) {
	const (
		project = "test"
		dataset = "dataset1"
		table   = "table_a"
	)
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.YAMLSource(filepath.Join("testdata", "data.yaml"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Close()
	}()
	opts, err := testServer.GRPCClientOptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bqReadClient, err := bqStorage.NewBigQueryReadClient(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer bqReadClient.Close()

	readTable := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, dataset, table)

	tableReadOptions := &storagepb.ReadSession_TableReadOptions{
		SelectedFields: outputColumns,
		RowRestriction: `id = 1`,
	}

	createReadSessionRequest := &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", project),
		ReadSession: &storagepb.ReadSession{
			Table:       readTable,
			DataFormat:  storagepb.DataFormat_AVRO,
			ReadOptions: tableReadOptions,
		},
		MaxStreamCount: 1,
	}

	// Create the session from the request.
	session, err := bqReadClient.CreateReadSession(ctx, createReadSessionRequest, rpcOpts)
	if err != nil {
		t.Fatalf("CreateReadSession: %v", err)
	}
	if len(session.GetStreams()) == 0 {
		t.Fatalf("no streams in session.  if this was a small query result, consider writing to output to a named table.")
	}

	// We'll use only a single stream for reading data from the table.  Because
	// of dynamic sharding, this will yield all the rows in the table. However,
	// if you wanted to fan out multiple readers you could do so by having a
	// increasing the MaxStreamCount.
	readStream := session.GetStreams()[0].Name

	ch := make(chan *storagepb.ReadRowsResponse)

	// Use a waitgroup to coordinate the reading and decoding goroutines.
	var wg sync.WaitGroup

	// Start the reading in one goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := processStream(t, ctx, bqReadClient, readStream, ch); err != nil {
			t.Fatalf("processStream failure: %v", err)
		}
		close(ch)
	}()

	// Start Avro processing and decoding in another goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := processAvro(t, ctx, session.GetAvroSchema().GetSchema(), ch); err != nil {
			t.Fatalf("error processing %s: %v", storagepb.DataFormat_AVRO, err)
		}
	}()

	// Wait until both the reading and decoding goroutines complete.
	wg.Wait()
}

func TestStorageReadARROW(t *testing.T) {
	const (
		project = "test"
		dataset = "dataset1"
		table   = "table_a"
	)
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.YAMLSource(filepath.Join("testdata", "data.yaml"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Close()
	}()
	opts, err := testServer.GRPCClientOptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bqReadClient, err := bqStorage.NewBigQueryReadClient(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer bqReadClient.Close()

	readTable := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, dataset, table)

	tableReadOptions := &storagepb.ReadSession_TableReadOptions{
		SelectedFields: outputColumns,
		RowRestriction: `id = 1`,
	}

	createReadSessionRequest := &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", project),
		ReadSession: &storagepb.ReadSession{
			Table:       readTable,
			DataFormat:  storagepb.DataFormat_ARROW,
			ReadOptions: tableReadOptions,
		},
		MaxStreamCount: 1,
	}

	// Create the session from the request.
	session, err := bqReadClient.CreateReadSession(ctx, createReadSessionRequest, rpcOpts)
	if err != nil {
		t.Fatalf("CreateReadSession: %v", err)
	}
	if len(session.GetStreams()) == 0 {
		t.Fatalf("no streams in session.  if this was a small query result, consider writing to output to a named table.")
	}

	// We'll use only a single stream for reading data from the table.  Because
	// of dynamic sharding, this will yield all the rows in the table. However,
	// if you wanted to fan out multiple readers you could do so by having a
	// increasing the MaxStreamCount.
	readStream := session.GetStreams()[0].Name

	ch := make(chan *storagepb.ReadRowsResponse)

	// Use a waitgroup to coordinate the reading and decoding goroutines.
	var wg sync.WaitGroup

	// Start the reading in one goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := processStream(t, ctx, bqReadClient, readStream, ch); err != nil {
			t.Fatalf("processStream failure: %v", err)
		}
		close(ch)
	}()

	// Start Avro processing and decoding in another goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := processArrow(t, ctx, session.GetArrowSchema().GetSerializedSchema(), ch); err != nil {
			t.Fatalf("error processing %s: %v", storagepb.DataFormat_ARROW, err)
		}
	}()

	// Wait until both the reading and decoding goroutines complete.
	wg.Wait()
}

func processStream(t *testing.T, ctx context.Context, client *bqStorage.BigQueryReadClient, st string, ch chan<- *storagepb.ReadRowsResponse) error {
	var offset int64

	// Streams may be long-running.  Rather than using a global retry for the
	// stream, implement a retry that resets once progress is made.
	retryLimit := 3
	retries := 0
	for {
		// Send the initiating request to start streaming row blocks.
		rowStream, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{
			ReadStream: st,
			Offset:     offset,
		}, rpcOpts)
		if err != nil {
			return fmt.Errorf("couldn't invoke ReadRows: %v", err)
		}

		// Process the streamed responses.
		for {
			r, err := rowStream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				// If there is an error, check whether it is a retryable
				// error with a retry delay and sleep instead of increasing
				// retries count.
				var retryDelayDuration time.Duration
				if errorStatus, ok := status.FromError(err); ok && errorStatus.Code() == codes.ResourceExhausted {
					for _, detail := range errorStatus.Details() {
						retryInfo, ok := detail.(*errdetails.RetryInfo)
						if !ok {
							continue
						}
						retryDelay := retryInfo.GetRetryDelay()
						retryDelayDuration = time.Duration(retryDelay.Seconds)*time.Second + time.Duration(retryDelay.Nanos)*time.Nanosecond
						break
					}
				}
				if retryDelayDuration != 0 {
					t.Fatalf("processStream failed with a retryable error, retrying in %v", retryDelayDuration)
					time.Sleep(retryDelayDuration)
				} else {
					retries++
					if retries >= retryLimit {
						return fmt.Errorf("processStream retries exhausted: %v", err)
					}
				}
				// break the inner loop, and try to recover by starting a new streaming
				// ReadRows call at the last known good offset.
				break
			} else {
				// Reset retries after a successful response.
				retries = 0
			}

			rc := r.GetRowCount()
			if rc > 0 {
				// Bookmark our progress in case of retries and send the rowblock on the channel.
				offset = offset + rc
				// We're making progress, reset retries.
				retries = 0
				ch <- r
			}
		}
	}
}

// processAvro receives row blocks from a channel, and uses the provided Avro
// schema to decode the blocks into individual row messages for printing.  Will
// continue to run until the channel is closed or the provided context is
// cancelled.
func processAvro(t *testing.T, ctx context.Context, schema string, ch <-chan *storagepb.ReadRowsResponse) error {
	// Establish a decoder that can process blocks of messages using the
	// reference schema. All blocks share the same schema, so the decoder
	// can be long-lived.
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return fmt.Errorf("couldn't create codec: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			// Context was cancelled.  Stop.
			return ctx.Err()
		case rows, ok := <-ch:
			if !ok {
				// Channel closed, no further avro messages.  Stop.
				return nil
			}
			undecoded := rows.GetAvroRows().GetSerializedBinaryRows()
			for len(undecoded) > 0 {
				datum, remainingBytes, err := codec.NativeFromBinary(undecoded)
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("decoding error with %d bytes remaining: %v", len(undecoded), err)
				}
				validateDatum(t, datum)
				undecoded = remainingBytes
			}
		}
	}
}

func processArrow(t *testing.T, ctx context.Context, schema []byte, ch <-chan *storagepb.ReadRowsResponse) error {
	mem := memory.NewGoAllocator()
	buf := bytes.NewBuffer(schema)
	r, err := ipc.NewReader(buf, ipc.WithAllocator(mem))
	if err != nil {
		return err
	}
	aschema := r.Schema()
	for {
		select {
		case <-ctx.Done():
			// Context was cancelled.  Stop.
			return ctx.Err()
		case rows, ok := <-ch:
			if !ok {
				// Channel closed, no further arrow messages.  Stop.
				return nil
			}
			undecoded := rows.GetArrowRecordBatch().GetSerializedRecordBatch()
			if len(undecoded) > 0 {
				buf = bytes.NewBuffer(undecoded)
				r, err = ipc.NewReader(buf, ipc.WithAllocator(mem), ipc.WithSchema(aschema))
				if err != nil {
					return err
				}
				for r.Next() {
					rec := r.Record()
					validateArrowRecord(t, rec)
				}
			}
		}
	}
}

func validateArrowRecord(t *testing.T, record arrow.Record) {
	out, err := record.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	list := []map[string]interface{}{}
	if err := json.Unmarshal(out, &list); err != nil {
		t.Fatal(err)
	}
	if len(list) == 0 {
		t.Fatal("failed to get arrow record")
	}
	first := list[0]
	if len(first) != len(outputColumns) {
		t.Fatalf("failed to get arrow record %+v", first)
	}
}

func validateDatum(t *testing.T, d interface{}) {
	m, ok := d.(map[string]interface{})
	if !ok {
		t.Logf("failed type assertion: %v", d)
	}
	if len(m) != len(outputColumns) {
		t.Fatalf("failed to receive table data. expected columns %v but got %v", outputColumns, m)
	}
}

func TestStorageWrite(t *testing.T) {
	for _, test := range []struct {
		name                            string
		streamType                      storagepb.WriteStream_Type
		isDefaultStream                 bool
		expectedRowsAfterFirstWrite     int
		expectedRowsAfterSecondWrite    int
		expectedRowsAfterThirdWrite     int
		expectedRowsAfterExplicitCommit int
	}{
		{
			name:                            "pending",
			streamType:                      storagepb.WriteStream_PENDING,
			expectedRowsAfterFirstWrite:     0,
			expectedRowsAfterSecondWrite:    0,
			expectedRowsAfterThirdWrite:     0,
			expectedRowsAfterExplicitCommit: 6,
		},
		{
			name:                            "committed",
			streamType:                      storagepb.WriteStream_COMMITTED,
			expectedRowsAfterFirstWrite:     1,
			expectedRowsAfterSecondWrite:    4,
			expectedRowsAfterThirdWrite:     6,
			expectedRowsAfterExplicitCommit: 6,
		},
		{
			name:                            "default",
			streamType:                      storagepb.WriteStream_COMMITTED,
			isDefaultStream:                 true,
			expectedRowsAfterFirstWrite:     1,
			expectedRowsAfterSecondWrite:    4,
			expectedRowsAfterThirdWrite:     6,
			expectedRowsAfterExplicitCommit: 6,
		},
	} {
		const (
			projectID = "test"
			datasetID = "test"
			tableID   = "sample"
		)

		ctx := context.Background()
		bqServer, err := server.New(server.TempStorage)
		if err != nil {
			t.Fatal(err)
		}
		if err := bqServer.Load(
			server.StructSource(
				types.NewProject(
					projectID,
					types.NewDataset(
						datasetID,
						types.NewTable(
							tableID,
							[]*types.Column{
								types.NewColumn("bool_col", types.BOOL),
								types.NewColumn("bytes_col", types.BYTES),
								types.NewColumn("float64_col", types.FLOAT64),
								types.NewColumn("int64_col", types.INT64),
								types.NewColumn("string_col", types.STRING),
								types.NewColumn("date_col", types.DATE),
								types.NewColumn("datetime_col", types.DATETIME),
								types.NewColumn("geography_col", types.GEOGRAPHY),
								types.NewColumn("numeric_col", types.NUMERIC),
								types.NewColumn("bignumeric_col", types.BIGNUMERIC),
								types.NewColumn("time_col", types.TIME),
								types.NewColumn("timestamp_col", types.TIMESTAMP),
								types.NewColumn("int64_list", types.INT64, types.ColumnMode(types.RepeatedMode)),
								types.NewColumn(
									"struct_col",
									types.STRUCT,
									types.ColumnFields(
										types.NewColumn("sub_int_col", types.INT64),
									),
								),
								types.NewColumn(
									"struct_list",
									types.STRUCT,
									types.ColumnFields(
										types.NewColumn("sub_int_col", types.INT64),
									),
									types.ColumnMode(types.RepeatedMode),
								),
							},
							nil,
						),
					),
				),
			),
		); err != nil {
			t.Fatal(err)
		}
		testServer := bqServer.TestServer()
		defer func() {
			testServer.Close()
			bqServer.Close()
		}()
		opts, err := testServer.GRPCClientOptions(ctx)
		if err != nil {
			t.Fatal(err)
		}

		client, err := managedwriter.NewClient(ctx, projectID, opts...)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		t.Run(test.name, func(t *testing.T) {
			var writeStreamName string
			fullTableName := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)
			if !test.isDefaultStream {
				writeStream, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
					Parent: fullTableName,
					WriteStream: &storagepb.WriteStream{
						Type: test.streamType,
					},
				})
				if err != nil {
					t.Fatalf("CreateWriteStream: %v", err)
				}
				writeStreamName = writeStream.GetName()
			}
			m := &exampleproto.SampleData{}
			descriptorProto, err := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
			if err != nil {
				t.Fatalf("NormalizeDescriptor: %v", err)
			}
			var writerOptions []managedwriter.WriterOption
			if test.isDefaultStream {
				writerOptions = append(writerOptions, managedwriter.WithType(managedwriter.DefaultStream))
				writerOptions = append(writerOptions, managedwriter.WithDestinationTable(fullTableName))
			} else {
				writerOptions = append(writerOptions, managedwriter.WithStreamName(writeStreamName))
			}
			writerOptions = append(writerOptions, managedwriter.WithSchemaDescriptor(descriptorProto))
			managedStream, err := client.NewManagedStream(
				ctx,
				writerOptions...,
			)
			if err != nil {
				t.Fatalf("NewManagedStream: %v", err)
			}

			bqClient, err := bigquery.NewClient(
				ctx,
				projectID,
				option.WithEndpoint(testServer.URL),
				option.WithoutAuthentication(),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer bqClient.Close()

			rows, err := generateExampleMessages(1)
			if err != nil {
				t.Fatalf("generateExampleMessages: %v", err)
			}

			var (
				curOffset int64
				results   []*managedwriter.AppendResult
			)
			result, err := managedStream.AppendRows(ctx, rows, managedwriter.WithOffset(0))
			if err != nil {
				t.Fatalf("AppendRows first call error: %v", err)
			}

			iter := bqClient.Dataset(datasetID).Table(tableID).Read(ctx)
			resultRowCount := countRows(t, iter)
			if resultRowCount != test.expectedRowsAfterFirstWrite {
				t.Fatalf("expected the number of rows after first AppendRows %d but got %d", test.expectedRowsAfterFirstWrite, resultRowCount)
			}

			results = append(results, result)
			curOffset = curOffset + 1
			rows, err = generateExampleMessages(3)
			if err != nil {
				t.Fatalf("generateExampleMessages: %v", err)
			}
			result, err = managedStream.AppendRows(ctx, rows, managedwriter.WithOffset(curOffset))
			if err != nil {
				t.Fatalf("AppendRows second call error: %v", err)
			}

			iter = bqClient.Dataset(datasetID).Table(tableID).Read(ctx)
			resultRowCount = countRows(t, iter)
			if resultRowCount != test.expectedRowsAfterSecondWrite {
				t.Fatalf("expected the number of rows after second AppendRows %d but got %d", test.expectedRowsAfterSecondWrite, resultRowCount)
			}

			results = append(results, result)
			curOffset = curOffset + 3
			rows, err = generateExampleMessages(2)
			if err != nil {
				t.Fatalf("generateExampleMessages: %v", err)
			}
			result, err = managedStream.AppendRows(ctx, rows, managedwriter.WithOffset(curOffset))
			if err != nil {
				t.Fatalf("AppendRows third call error: %v", err)
			}
			results = append(results, result)

			for k, v := range results {
				recvOffset, err := v.GetResult(ctx)
				if err != nil {
					t.Fatalf("append %d returned error: %v", k, err)
				}
				t.Logf("Successfully appended data at offset %d", recvOffset)
			}

			iter = bqClient.Dataset(datasetID).Table(tableID).Read(ctx)
			resultRowCount = countRows(t, iter)
			if resultRowCount != test.expectedRowsAfterThirdWrite {
				t.Fatalf("expected the number of rows after third AppendRows %d but got %d", test.expectedRowsAfterThirdWrite, resultRowCount)
			}

			rowCount, err := managedStream.Finalize(ctx)
			if err != nil {
				t.Fatalf("error during Finalize: %v", err)
			}

			t.Logf("Stream %s finalized with %d rows", managedStream.StreamName(), rowCount)

			req := &storagepb.BatchCommitWriteStreamsRequest{
				Parent:       managedwriter.TableParentFromStreamName(managedStream.StreamName()),
				WriteStreams: []string{managedStream.StreamName()},
			}

			resp, err := client.BatchCommitWriteStreams(ctx, req)
			if err != nil {
				t.Fatalf("client.BatchCommit: %v", err)
			}
			if len(resp.GetStreamErrors()) > 0 {
				t.Fatalf("stream errors present: %v", resp.GetStreamErrors())
			}

			iter = bqClient.Dataset(datasetID).Table(tableID).Read(ctx)
			resultRowCount = countRows(t, iter)
			if resultRowCount != test.expectedRowsAfterExplicitCommit {
				t.Fatalf("expected the number of rows after Finalize %d but got %d", test.expectedRowsAfterExplicitCommit, resultRowCount)
			}

			t.Logf("Table data committed at %s", resp.GetCommitTime().AsTime().Format(time.RFC3339Nano))
		})
	}
}

func countRows(t *testing.T, iter *bigquery.RowIterator) int {
	var resultRowCount int
	for {
		v := map[string]bigquery.Value{}
		if err := iter.Next(&v); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		resultRowCount++
	}
	return resultRowCount
}

func generateExampleMessages(numMessages int) ([][]byte, error) {
	msgs := make([][]byte, numMessages)
	for i := 0; i < numMessages; i++ {

		random := rand.New(rand.NewSource(time.Now().UnixNano()))

		// Our example data embeds an array of structs, so we'll construct that first.
		sList := make([]*exampleproto.SampleStruct, 5)
		for i := 0; i < int(random.Int63n(5)+1); i++ {
			sList[i] = &exampleproto.SampleStruct{
				SubIntCol: proto.Int64(random.Int63()),
			}
		}

		m := &exampleproto.SampleData{
			BoolCol:    proto.Bool(true),
			BytesCol:   []byte("some bytes"),
			Float64Col: proto.Float64(3.14),
			Int64Col:   proto.Int64(123),
			StringCol:  proto.String("example string value"),

			// These types require special encoding/formatting to transmit.

			// DATE values are number of days since the Unix epoch.

			DateCol: proto.Int32(int32(time.Now().UnixNano() / 86400000000000)),

			// DATETIME uses the literal format.
			DatetimeCol: proto.String("2022-01-01 12:13:14.000000"),

			// GEOGRAPHY uses Well-Known-Text (WKT) format.
			GeographyCol: proto.String("POINT(-122.350220 47.649154)"),

			// NUMERIC and BIGNUMERIC can be passed as string, or more efficiently
			// using a packed byte representation.
			NumericCol:    proto.String("99999999999999999999999999999.999999999"),
			BignumericCol: proto.String("578960446186580977117854925043439539266.34992332820282019728792003956564819967"),

			// TIME also uses literal format.
			TimeCol: proto.String("12:13:14.000000"),

			// TIMESTAMP uses microseconds since Unix epoch.
			TimestampCol: proto.Int64(time.Now().UnixNano() / 1000),

			// Int64List is an array of INT64 types.
			Int64List: []int64{2, 4, 6, 8},

			// This is a required field, and thus must be present.
			RowNum: proto.Int64(23),

			// StructCol is a single nested message.
			StructCol: &exampleproto.SampleStruct{
				SubIntCol: proto.Int64(random.Int63()),
			},

			// StructList is a repeated array of a nested message.
			StructList: sList,
		}
		b, err := proto.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("error generating message %d: %w", i, err)
		}
		msgs[i] = b
	}
	return msgs, nil
}

func TestStorageWriteDynamicDescriptor(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "test"
		tableID   = "sample"
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
					types.NewTable(
						tableID,
						[]*types.Column{
							types.NewColumn("timestamp", types.TIMESTAMP),
							types.NewColumn("msg", types.STRING),
						},
						nil,
					),
				),
			),
		),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Close()
	}()
	opts, err := testServer.GRPCClientOptions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	client, err := managedwriter.NewClient(ctx, projectID, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	fullTableName := managedwriter.TableParentFromParts(
		projectID,
		datasetID,
		tableID,
	)

	msgDesc, protoDesc := dynamicProtoDescriptors(t)
	stream, err := client.NewManagedStream(
		ctx,
		managedwriter.WithDestinationTable(fullTableName),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(protoDesc),
	)
	if err != nil {
		t.Fatal(err)
	}

	// [timestamppb.Timestamp] supports decoding from RFC3339 timestamps
	msg := dynamicpb.NewMessage(msgDesc)
	if err := protojson.Unmarshal(
		[]byte(`{"timestamp": "2025-07-16T18:18:07-04:00", "msg": "hello"}`),
		msg,
	); err != nil {
		t.Fatal(err)
	}

	row, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	res, err := stream.AppendRows(ctx, [][]byte{row})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := res.GetResult(ctx); err != nil {
		t.Fatal(err)
	}
}

// dynamicProtoDescriptors creates a protobuf at runtime, returning the message and
// type descriptors.
//
// This is specifically needed to verify sending [timestamppb.Timestamp] values to the
// storage write API for feature parity with BigQuery.
func dynamicProtoDescriptors(t *testing.T) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto) {
	t.Helper()

	scope := "test"
	dp := &descriptorpb.DescriptorProto{
		Name: proto.String(scope),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:     proto.String("timestamp"),
				Number:   proto.Int32(1),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".google.protobuf.Timestamp"),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_REQUIRED.Enum(),
			},
			{
				Name:   proto.String("msg"),
				Number: proto.Int32(2),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_REQUIRED.Enum(),
			},
		},
	}
	fdp := &descriptorpb.FileDescriptorProto{
		MessageType: []*descriptorpb.DescriptorProto{dp},
		Name:        proto.String(scope + ".proto"),
		Syntax:      proto.String("proto2"),
		Dependency: []string{
			"google/protobuf/wrappers.proto",
			"google/protobuf/timestamp.proto",
		},
	}

	fdpList := []*descriptorpb.FileDescriptorProto{
		fdp,
		protodesc.ToFileDescriptorProto(wrapperspb.File_google_protobuf_wrappers_proto),
		protodesc.ToFileDescriptorProto(timestamppb.File_google_protobuf_timestamp_proto),
	}
	fds := &descriptorpb.FileDescriptorSet{File: fdpList}

	files, err := protodesc.NewFiles(fds)
	if err != nil {
		t.Fatal(err)
	}

	found, err := files.FindDescriptorByName(protoreflect.FullName(scope))
	if err != nil {
		t.Fatal(err)
	}

	messageDescriptor := found.(protoreflect.MessageDescriptor)
	protoDescriptor, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	return messageDescriptor, protoDescriptor
}
