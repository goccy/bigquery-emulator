// FYI: https://cloud.google.com/bigquery/docs/reference/storage/libraries?hl=ja#client-libraries-usage-go
package server_test

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	"github.com/goccy/bigquery-emulator/server"
	gax "github.com/googleapis/gax-go/v2"
	goavro "github.com/linkedin/goavro/v2"
	bqStoragepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	rpcOpts = gax.WithGRPCOptions(
		grpc.MaxCallRecvMsgSize(1024 * 1024 * 129),
	)
	avroOutputColumns = []string{"id", "name", "structarr", "birthday", "skillNum", "created_at"}
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

	tableReadOptions := &bqStoragepb.ReadSession_TableReadOptions{
		SelectedFields: avroOutputColumns,
		RowRestriction: `id = 1`,
	}

	dataFormat := bqStoragepb.DataFormat_AVRO
	createReadSessionRequest := &bqStoragepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", project),
		ReadSession: &bqStoragepb.ReadSession{
			Table:       readTable,
			DataFormat:  dataFormat,
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

	ch := make(chan *bqStoragepb.ReadRowsResponse)

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
			t.Fatalf("error processing %s: %v", dataFormat, err)
		}
	}()

	// Wait until both the reading and decoding goroutines complete.
	wg.Wait()

}

func processStream(t *testing.T, ctx context.Context, client *bqStorage.BigQueryReadClient, st string, ch chan<- *bqStoragepb.ReadRowsResponse) error {
	var offset int64

	// Streams may be long-running.  Rather than using a global retry for the
	// stream, implement a retry that resets once progress is made.
	retryLimit := 3
	retries := 0
	for {
		// Send the initiating request to start streaming row blocks.
		rowStream, err := client.ReadRows(ctx, &bqStoragepb.ReadRowsRequest{
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
func processAvro(t *testing.T, ctx context.Context, schema string, ch <-chan *bqStoragepb.ReadRowsResponse) error {
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

func validateDatum(t *testing.T, d interface{}) {
	m, ok := d.(map[string]interface{})
	if !ok {
		t.Logf("failed type assertion: %v", d)
	}
	if len(m) != len(avroOutputColumns) {
		t.Fatalf("failed to receive table data. expected columns %v but got %v", avroOutputColumns, m)
	}
}
