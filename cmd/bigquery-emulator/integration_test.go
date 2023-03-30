package main

import (
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"context"
	"flag"
	"fmt"
	"github.com/goccy/bigquery-emulator/cmd/bigquery-emulator/testdata"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/rs/xid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/googleapis/gax-go/v2/apierror"
	"go.opencensus.io/stats/view"
	googleoption "google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	defaultTestTimeout = 45 * time.Second

	httpPort = uint16(9053)
	grpcPort = uint16(9063)
	httpURL  = fmt.Sprintf("http://localhost:%d", httpPort)
	grpcURL  = fmt.Sprintf("localhost:%d", grpcPort)
)

// our test data has cardinality 5 for names, 3 for values
var (
	testSimpleData = []*testdata.SimpleMessageProto2{
		{Name: proto.String("one"), Value: proto.Int64(1)},
		{Name: proto.String("two"), Value: proto.Int64(2)},
		{Name: proto.String("three"), Value: proto.Int64(3)},
		{Name: proto.String("four"), Value: proto.Int64(1)},
		{Name: proto.String("five"), Value: proto.Int64(2)},
	}

	testSimpleDataProto3 = []*testdata.SimpleMessageProto3{
		{Name: "zero", Value: &wrapperspb.Int64Value{Value: 0}},
		{Name: "one", Value: &wrapperspb.Int64Value{Value: 1}},
		{Name: "two", Value: &wrapperspb.Int64Value{Value: 2}},
		{Name: "three", Value: &wrapperspb.Int64Value{Value: 3}},
		{Name: "four", Value: &wrapperspb.Int64Value{Value: 1}},
		{Name: "five", Value: &wrapperspb.Int64Value{Value: 2}},
	}
)

var (
	disableEmulator = flag.Bool("disable-emulator", false, "disable the bigquery emulator and run tests against the real BigQuery GCP service")
	projectID       = flag.String("project-id", "emulator-test", "GCP project ID")
)

func TestMain(m *testing.M) {
	flag.Parse()

	if !*disableEmulator {
		go func() {
			opt := option{
				Project:   *projectID,
				HTTPPort:  httpPort,
				GRPCPort:  grpcPort,
				LogLevel:  server.LogLevelDebug,
				LogFormat: server.LogFormatConsole,
			}
			if err := runServer([]string{}, opt); err != nil {
				panic(err)
			}
		}()
		// TODO(dm): check if the server is ready instead of just sleeping
		time.Sleep(5 * time.Second)
	}

	exitCode := m.Run()

	os.Exit(exitCode)
}
func getTestClients(ctx context.Context, t *testing.T) (*managedwriter.Client, *bigquery.Client) {
	if testing.Short() {
		t.Skip("Integration tests skipped in short mode")
	}

	ts := TokenSource(ctx, "https://www.googleapis.com/auth/bigquery")
	if ts == nil {
		t.Skip("Integration tests skipped. See CONTRIBUTING.md for details")
	}

	storageClientOptions := []googleoption.ClientOption{}
	if !*disableEmulator {
		storageClientOptions = append(storageClientOptions, googleoption.WithEndpoint(grpcURL))
		storageClientOptions = append(storageClientOptions, googleoption.WithGRPCDialOption(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		))
		storageClientOptions = append(storageClientOptions, googleoption.WithoutAuthentication())
	} else {
		storageClientOptions = append(storageClientOptions, googleoption.WithTokenSource(ts))
	}

	client, err := managedwriter.NewClient(ctx, *projectID, storageClientOptions...)
	if err != nil {
		t.Fatalf("couldn't create managedwriter client: %v", err)
	}

	bqClientOptions := []googleoption.ClientOption{}
	if !*disableEmulator {
		bqClientOptions = append(bqClientOptions, googleoption.WithEndpoint(httpURL))
		bqClientOptions = append(bqClientOptions, googleoption.WithoutAuthentication())
	} else {
		storageClientOptions = append(storageClientOptions, googleoption.WithTokenSource(ts))
	}
	bqClient, err := bigquery.NewClient(ctx, *projectID, bqClientOptions...)
	if err != nil {
		t.Fatalf("couldn't create bigquery client: %v", err)
	}
	return client, bqClient
}

// setupTestDataset generates a unique dataset for testing, and a cleanup that can be deferred.
func setupTestDataset(ctx context.Context, t *testing.T, bqc *bigquery.Client, location string) (ds *bigquery.Dataset, cleanup func(), err error) {
	dataset := bqc.Dataset(xid.New().String())
	if err := dataset.Create(ctx, &bigquery.DatasetMetadata{Location: location}); err != nil {
		return nil, nil, err
	}
	return dataset, func() {
		if err := dataset.DeleteWithContents(ctx); err != nil {
			t.Logf("could not cleanup dataset %q: %v", dataset.DatasetID, err)
		}
	}, nil
}

// setupDynamicDescriptors aids testing when not using a supplied proto
func setupDynamicDescriptors(t *testing.T, schema bigquery.Schema) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto) {
	convertedSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		t.Fatalf("adapt.BQSchemaToStorageTableSchema: %v", err)
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(convertedSchema, "root")
	if err != nil {
		t.Fatalf("adapt.StorageSchemaToDescriptor: %v", err)
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		t.Fatalf("adapted descriptor is not a message descriptor")
	}
	return messageDescriptor, protodesc.ToDescriptorProto(messageDescriptor)
}

func TestIntegration_ManagedWriter(t *testing.T) {
	mwClient, bqClient := getTestClients(context.Background(), t)
	defer mwClient.Close()
	defer bqClient.Close()

	dataset, cleanup, err := setupTestDataset(context.Background(), t, bqClient, "us-east1")
	if err != nil {
		t.Fatalf("failed to init test dataset: %v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	t.Run("group", func(t *testing.T) {
		t.Run("testDefaultStream", func(t *testing.T) {
			t.Parallel()
			testDefaultStream(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("DefaultStreamDynamicJSON", func(t *testing.T) {
			t.Parallel()
			testDefaultStreamDynamicJSON(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("CommittedStream", func(t *testing.T) {
			t.Parallel()
			testCommittedStream(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("ErrorBehaviors", func(t *testing.T) {
			t.Parallel()
			testErrorBehaviors(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("BufferedStream", func(t *testing.T) {
			t.Parallel()
			testBufferedStream(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("managedwriter.PendingStream", func(t *testing.T) {
			t.Parallel()
			testPendingStream(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("SchemaEvolution", func(t *testing.T) {
			t.Parallel()
			testSchemaEvolution(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("SimpleCDC", func(t *testing.T) {
			t.Parallel()
			testSimpleCDC(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("Instrumentation", func(t *testing.T) {
			// Don't run this in parallel, we only want to collect stats from this subtest.
			testInstrumentation(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("TestLargeInsertNoRetry", func(t *testing.T) {
			testLargeInsertNoRetry(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("TestLargeInsertWithRetry", func(t *testing.T) {
			testLargeInsertWithRetry(ctx, t, mwClient, bqClient, dataset)
		})
		t.Run("KnownWrapperTypes", func(t *testing.T) {
			t.Parallel()
			testKnownWrapperTypes(ctx, t, mwClient, bqClient, dataset)
			testKnownWrapperTypesRecords(ctx, t, mwClient, bqClient, dataset)
		})

	})
}

func testKnownWrapperTypesRecords(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageWithWrapperSchema}); err != nil {
		t.Fatalf("failed to create test table %q: %v", testTable.FullyQualifiedName(), err)
	}
	// We'll use a precompiled test proto, but we need it's corresponding descriptorproto representation
	// to send as the stream's schema.
	m := &testdata.SimpleMessageProto3{}
	descriptorProto, err := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("failed to normalize descriptor: %s", err)
	}

	// setup a new stream.
	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %s", err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "before send", withExactRowCount(0))

	// Now, send the test rows grouped into in a single append.
	var dataBy [][]byte
	for k, data := range testSimpleDataProto3 {
		protoBy, err := proto.Marshal(data)
		if err != nil {
			t.Errorf("failed to marshal message %d: %v", k, err)
		}
		dataBy = append(dataBy, protoBy)
	}
	result, err := ms.AppendRows(ctx, dataBy)
	if err != nil {
		t.Errorf("grouped-row append failed: %v", err)
	}
	// Wait for the result to indicate ready, then validate again.  Our total rows have increased, but
	// cardinality should not.
	o, err := result.GetResult(ctx)
	if err != nil {
		t.Errorf("result error for last send: %v", err)
	}
	if o != managedwriter.NoStreamOffset {
		t.Errorf("offset mismatch, got %d want %d", o, managedwriter.NoStreamOffset)
	}

	validateTableConstraints(ctx, t, bqClient, testTable, "after second send round",
		withExactRowCount(int64(len(testSimpleDataProto3))),
		withDistinctValues("name", int64(len(testSimpleDataProto3))),
		withDistinctValues("value.value", int64(3)),
	)
}

func testKnownWrapperTypes(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %q: %v", testTable.FullyQualifiedName(), err)
	}
	// We'll use a precompiled test proto, but we need it's corresponding descriptorproto representation
	// to send as the stream's schema.
	m := &testdata.SimpleMessageProto3{}
	descriptorProto, err := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("failed to normalize descriptor: %s", err)
	}

	// setup a new stream.
	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %s", err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "before send", withExactRowCount(0))

	// Now, send the test rows grouped into in a single append.
	var dataBy [][]byte
	for k, data := range testSimpleDataProto3 {
		protoBy, err := proto.Marshal(data)
		if err != nil {
			t.Errorf("failed to marshal message %d: %v", k, err)
		}
		dataBy = append(dataBy, protoBy)
	}
	result, err := ms.AppendRows(ctx, dataBy)
	if err != nil {
		t.Errorf("grouped-row append failed: %v", err)
	}
	// Wait for the result to indicate ready, then validate again.  Our total rows have increased, but
	// cardinality should not.
	o, err := result.GetResult(ctx)
	if err != nil {
		t.Errorf("result error for last send: %v", err)
	}
	if o != managedwriter.NoStreamOffset {
		t.Errorf("offset mismatch, got %d want %d", o, managedwriter.NoStreamOffset)
	}

	validateTableConstraints(ctx, t, bqClient, testTable, "after second send round",
		withExactRowCount(int64(len(testSimpleDataProto3))),
		withDistinctValues("name", int64(len(testSimpleDataProto3))),
		withDistinctValues("value", int64(4)),
	)
}

func testDefaultStream(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %q: %v", testTable.FullyQualifiedName(), err)
	}
	// We'll use a precompiled test proto, but we need it's corresponding descriptorproto representation
	// to send as the stream's schema.
	m := &testdata.SimpleMessageProto2{}
	descriptorProto := protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())

	// setup a new stream.
	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %s", err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "before send", withExactRowCount(0))

	// First, send the test rows individually.
	var result *managedwriter.AppendResult
	for k, data := range testSimpleData {
		protoBy, err := proto.Marshal(data)
		if err != nil {
			t.Errorf("failed to marshal message %d: %v", k, err)
		}
		dataBy := [][]byte{protoBy}
		result, err = ms.AppendRows(ctx, dataBy)
		if err != nil {
			t.Errorf("single-row append %d failed: %v", k, err)
		}
	}
	// Wait for the result to indicate ready, then validate.
	o, err := result.GetResult(ctx)
	if err != nil {
		t.Fatalf("result error for last send: %v", err)
	}
	if o != managedwriter.NoStreamOffset {
		t.Errorf("offset mismatch, got %d want %d", o, managedwriter.NoStreamOffset)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "after first send round",
		withExactRowCount(int64(len(testSimpleData))),
		withDistinctValues("name", int64(len(testSimpleData))))

	// Now, send the test rows grouped into in a single append.
	var dataBy [][]byte
	for k, data := range testSimpleData {
		protoBy, err := proto.Marshal(data)
		if err != nil {
			t.Errorf("failed to marshal message %d: %v", k, err)
		}
		dataBy = append(dataBy, protoBy)
	}
	result, err = ms.AppendRows(ctx, dataBy)
	if err != nil {
		t.Errorf("grouped-row append failed: %v", err)
	}
	// Wait for the result to indicate ready, then validate again.  Our total rows have increased, but
	// cardinality should not.
	o, err = result.GetResult(ctx)
	if err != nil {
		t.Errorf("result error for last send: %v", err)
	}
	if o != managedwriter.NoStreamOffset {
		t.Errorf("offset mismatch, got %d want %d", o, managedwriter.NoStreamOffset)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "after second send round",
		withExactRowCount(int64(2*len(testSimpleData))),
		withDistinctValues("name", int64(len(testSimpleData))),
		withDistinctValues("value", int64(4)),
	)
}

func testDefaultStreamDynamicJSON(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %s: %v", testTable.FullyQualifiedName(), err)
	}

	md, descriptorProto := setupDynamicDescriptors(t, testdata.SimpleMessageSchema)

	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "before send",
		withExactRowCount(0))

	sampleJSONData := [][]byte{
		[]byte(`{"name": "one", "value": 1}`),
		[]byte(`{"name": "two", "value": 2}`),
		[]byte(`{"name": "three", "value": 3}`),
		[]byte(`{"name": "four", "value": 4}`),
		[]byte(`{"name": "five", "value": 5}`),
	}

	var result *managedwriter.AppendResult
	for k, v := range sampleJSONData {
		message := dynamicpb.NewMessage(md)

		// First, json->proto message
		err = protojson.Unmarshal(v, message)
		if err != nil {
			t.Fatalf("failed to Unmarshal json message for row %d: %v", k, err)
		}
		// Then, proto message -> bytes.
		b, err := proto.Marshal(message)
		if err != nil {
			t.Fatalf("failed to marshal proto bytes for row %d: %v", k, err)
		}
		result, err = ms.AppendRows(ctx, [][]byte{b})
		if err != nil {
			t.Errorf("single-row append %d failed: %v", k, err)
		}
	}

	// Wait for the result to indicate ready, then validate.
	o, err := result.GetResult(ctx)
	if err != nil {
		t.Errorf("result error for last send: %v", err)
	}
	if o != managedwriter.NoStreamOffset {
		t.Errorf("offset mismatch, got %d want %d", o, managedwriter.NoStreamOffset)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "after send",
		withExactRowCount(int64(len(sampleJSONData))),
		withDistinctValues("name", int64(len(sampleJSONData))),
		withDistinctValues("value", int64(len(sampleJSONData))))
}

func testBufferedStream(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %s: %v", testTable.FullyQualifiedName(), err)
	}

	m := &testdata.SimpleMessageProto2{}
	descriptorProto := protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())

	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.BufferedStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}

	validateTableConstraints(ctx, t, bqClient, testTable, "before send",
		withExactRowCount(0))

	var expectedRows int64
	for k, mesg := range testSimpleData {
		b, err := proto.Marshal(mesg)
		if err != nil {
			t.Errorf("failed to marshal message %d: %v", k, err)
		}
		data := [][]byte{b}
		results, err := ms.AppendRows(ctx, data)
		if err != nil {
			t.Errorf("single-row append %d failed: %v", k, err)
		}
		// Wait for acknowledgement.
		offset, err := results.GetResult(ctx)
		if err != nil {
			t.Errorf("got error from pending result %d: %v", k, err)
		}
		validateTableConstraints(ctx, t, bqClient, testTable, fmt.Sprintf("before flush %d", k),
			withExactRowCount(expectedRows),
			withDistinctValues("name", expectedRows))

		// move offset and re-validate.
		flushOffset, err := ms.FlushRows(ctx, offset)
		if err != nil {
			t.Errorf("failed to flush offset to %d: %v", offset, err)
		}
		expectedRows = flushOffset + 1
		validateTableConstraints(ctx, t, bqClient, testTable, fmt.Sprintf("after flush %d", k),
			withExactRowCount(expectedRows),
			withDistinctValues("name", expectedRows))
	}
}

func testCommittedStream(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %s: %v", testTable.FullyQualifiedName(), err)
	}

	m := &testdata.SimpleMessageProto2{}
	descriptorProto := protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())

	// setup a new stream.
	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "before send",
		withExactRowCount(0))

	var result *managedwriter.AppendResult
	for k, mesg := range testSimpleData {
		b, err := proto.Marshal(mesg)
		if err != nil {
			t.Errorf("failed to marshal message %d: %v", k, err)
		}
		data := [][]byte{b}
		result, err = ms.AppendRows(ctx, data, managedwriter.WithOffset(int64(k)))
		if err != nil {
			t.Errorf("single-row append %d failed: %v", k, err)
		}
	}
	// Wait for the result to indicate ready, then validate.
	o, err := result.GetResult(ctx)
	if err != nil {
		t.Errorf("result error for last send: %v", err)
	}
	wantOffset := int64(len(testSimpleData) - 1)
	if o != wantOffset {
		t.Errorf("offset mismatch, got %d want %d", o, wantOffset)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "after send",
		withExactRowCount(int64(len(testSimpleData))))
}

// testSimpleCDC demonstrates basic Change Data Capture (CDC) functionality.   We add an initial set of
// rows to a table, then use CDC to apply updates.
func testSimpleCDC(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())

	if err := testTable.Create(ctx, &bigquery.TableMetadata{
		Schema: testdata.ExampleEmployeeSchema,
		Clustering: &bigquery.Clustering{
			Fields: []string{"id"},
		},
	}); err != nil {
		t.Fatalf("failed to create test table %s: %v", testTable.FullyQualifiedName(), err)
	}

	// Mark the primary key using an ALTER TABLE DDL.
	tableIdentifier, _ := testTable.Identifier(bigquery.StandardSQLID)
	sql := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY(id) NOT ENFORCED;", tableIdentifier)
	if _, err := bqClient.Query(sql).Read(ctx); err != nil {
		t.Fatalf("failed ALTER TABLE: %v", err)
	}

	m := &testdata.ExampleEmployeeCDC{}
	descriptorProto, err := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("NormalizeDescriptor: %v", err)
	}

	// Setup an initial writer for sending initial inserts.
	writer, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	defer writer.Close()
	validateTableConstraints(ctx, t, bqClient, testTable, "before send",
		withExactRowCount(0))

	initialEmployees := []*testdata.ExampleEmployeeCDC{
		{
			Id:           proto.Int64(1),
			Username:     proto.String("alice"),
			GivenName:    proto.String("Alice CEO"),
			Departments:  []string{"product", "support", "internal"},
			Salary:       proto.Int64(1),
			XCHANGE_TYPE: proto.String("INSERT"),
		},
		{
			Id:           proto.Int64(2),
			Username:     proto.String("bob"),
			GivenName:    proto.String("Bob Bobberson"),
			Departments:  []string{"research"},
			Salary:       proto.Int64(100000),
			XCHANGE_TYPE: proto.String("INSERT"),
		},
		{
			Id:           proto.Int64(3),
			Username:     proto.String("clarice"),
			GivenName:    proto.String("Clarice Clearwater"),
			Departments:  []string{"product"},
			Salary:       proto.Int64(100001),
			XCHANGE_TYPE: proto.String("INSERT"),
		},
	}

	// First append inserts all the initial employees.
	data := make([][]byte, len(initialEmployees))
	for k, mesg := range initialEmployees {
		b, err := proto.Marshal(mesg)
		if err != nil {
			t.Fatalf("failed to marshal record %d: %v", k, err)
		}
		data[k] = b
	}
	result, err := writer.AppendRows(ctx, data)
	if err != nil {
		t.Errorf("initial insert failed (%s): %v", writer.StreamName(), err)
	}
	if _, err := result.GetResult(ctx); err != nil {
		t.Errorf("result error for initial insert (%s): %v", writer.StreamName(), err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "initial inserts",
		withExactRowCount(int64(len(initialEmployees))))

	// Create a second writer for applying modifications.
	updateWriter, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	defer updateWriter.Close()

	// Change bob via an UPSERT CDC
	newBob := proto.Clone(initialEmployees[1]).(*testdata.ExampleEmployeeCDC)
	newBob.Salary = proto.Int64(105000)
	newBob.Departments = []string{"research", "product"}
	newBob.XCHANGE_TYPE = proto.String("UPSERT")
	b, err := proto.Marshal(newBob)
	if err != nil {
		t.Fatalf("failed to marshal new bob: %v", err)
	}
	result, err = updateWriter.AppendRows(ctx, [][]byte{b})
	if err != nil {
		t.Fatalf("bob modification failed (%s): %v", updateWriter.StreamName(), err)
	}
	if _, err := result.GetResult(ctx); err != nil {
		t.Fatalf("result error for bob modification (%s): %v", updateWriter.StreamName(), err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "after bob modification",
		withExactRowCount(int64(len(initialEmployees))),
		withDistinctValues("id", int64(len(initialEmployees))))

	// remote clarice via DELETE CDC
	removeClarice := &testdata.ExampleEmployeeCDC{
		Id:           proto.Int64(3),
		XCHANGE_TYPE: proto.String("DELETE"),
	}
	b, err = proto.Marshal(removeClarice)
	if err != nil {
		t.Fatalf("failed to marshal clarice removal: %v", err)
	}
	result, err = updateWriter.AppendRows(ctx, [][]byte{b})
	if err != nil {
		t.Fatalf("clarice removal failed (%s): %v", updateWriter.StreamName(), err)
	}
	if _, err := result.GetResult(ctx); err != nil {
		t.Fatalf("result error for clarice removal (%s): %v", updateWriter.StreamName(), err)
	}

	validateTableConstraints(ctx, t, bqClient, testTable, "after clarice removal",
		withExactRowCount(int64(len(initialEmployees))-1))
}

// testErrorBehaviors intentionally issues problematic requests to verify error behaviors.
func testErrorBehaviors(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %s: %v", testTable.FullyQualifiedName(), err)
	}

	m := &testdata.SimpleMessageProto2{}
	descriptorProto := protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())

	// setup a new stream.
	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "before send",
		withExactRowCount(0))

	data := make([][]byte, len(testSimpleData))
	for k, mesg := range testSimpleData {
		b, err := proto.Marshal(mesg)
		if err != nil {
			t.Errorf("failed to marshal message %d: %v", k, err)
		}
		data[k] = b
	}

	// Send an append at an invalid offset.
	result, err := ms.AppendRows(ctx, data, managedwriter.WithOffset(99))
	if err != nil {
		t.Errorf("failed to send append: %v", err)
	}
	off, err := result.GetResult(ctx)
	if err == nil {
		t.Fatalf("expected error, got offset %d", off)
	}

	apiErr, ok := apierror.FromError(err)
	if !ok {
		t.Fatalf("expected apierror, got %[1]T: %[1]v", err)
	}
	se := &storagepb.StorageError{}
	e := apiErr.Details().ExtractProtoMessage(se)
	if e != nil {
		t.Errorf("expected storage error, but extraction failed: %v", e)
	}
	wantCode := storagepb.StorageError_OFFSET_OUT_OF_RANGE
	if se.GetCode() != wantCode {
		t.Errorf("wanted %s, got %s", wantCode.String(), se.GetCode().String())
	}
	// Send "real" append to advance the offset.
	result, err = ms.AppendRows(ctx, data, managedwriter.WithOffset(0))
	if err != nil {
		t.Errorf("failed to send append: %v", err)
	}
	off, err = result.GetResult(ctx)
	if err != nil {
		t.Fatalf("expected offset, got error %v", err)
	}
	wantOffset := int64(0)
	if off != wantOffset {
		t.Errorf("offset mismatch, got %d want %d", off, wantOffset)
	}
	// Now, send at the start offset again.
	result, err = ms.AppendRows(ctx, data, managedwriter.WithOffset(0))
	if err != nil {
		t.Fatalf("failed to send append: %v", err)
	}
	off, err = result.GetResult(ctx)
	if err == nil {
		t.Errorf("expected error, got offset %d", off)
	}
	apiErr, ok = apierror.FromError(err)
	if !ok {
		t.Fatalf("expected apierror, got %T: %v", err, err)
	}
	se = &storagepb.StorageError{}
	e = apiErr.Details().ExtractProtoMessage(se)
	if e != nil {
		t.Errorf("expected storage error, but extraction failed: %v", e)
	}
	wantCode = storagepb.StorageError_OFFSET_ALREADY_EXISTS
	if se.GetCode() != wantCode {
		t.Errorf("wanted %s, got %s", wantCode.String(), se.GetCode().String())
	}
	// Finalize the stream.
	if _, err := ms.Finalize(ctx); err != nil {
		t.Errorf("Finalize had error: %v", err)
	}
	// Send another append, which is disallowed for finalized streams.
	result, err = ms.AppendRows(ctx, data)
	if err != nil {
		t.Errorf("failed to send append: %v", err)
	}
	off, err = result.GetResult(ctx)
	if err == nil {
		t.Fatalf("expected error, got offset %d", off)
	}
	apiErr, ok = apierror.FromError(err)
	if !ok {
		t.Errorf("expected apierror, got %T: %v", err, err)
	}
	se = &storagepb.StorageError{}
	e = apiErr.Details().ExtractProtoMessage(se)
	if e != nil {
		t.Errorf("expected storage error, but extraction failed: %v", e)
	}
	wantCode = storagepb.StorageError_STREAM_FINALIZED
	if se.GetCode() != wantCode {
		t.Errorf("wanted %s, got %s", wantCode.String(), se.GetCode().String())
	}
}

func testPendingStream(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %s: %v", testTable.FullyQualifiedName(), err)
	}

	m := &testdata.SimpleMessageProto2{}
	descriptorProto := protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())

	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.PendingStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "before send",
		withExactRowCount(0))

	// Send data.
	var result *managedwriter.AppendResult
	for k, mesg := range testSimpleData {
		b, err := proto.Marshal(mesg)
		if err != nil {
			t.Errorf("failed to marshal message %d: %v", k, err)
		}
		data := [][]byte{b}
		result, err = ms.AppendRows(ctx, data, managedwriter.WithOffset(int64(k)))
		if err != nil {
			t.Errorf("single-row append %d failed: %v", k, err)
		}
		// Be explicit about waiting/checking each response.
		off, err := result.GetResult(ctx)
		if err != nil {
			t.Errorf("response %d error: %v", k, err)
		}
		if off != int64(k) {
			t.Errorf("offset mismatch, got %d want %d", off, k)
		}
	}
	wantRows := int64(len(testSimpleData))

	// Mark stream complete.
	trackedOffset, err := ms.Finalize(ctx)
	if err != nil {
		t.Errorf("Finalize: %v", err)
	}

	if trackedOffset != wantRows {
		t.Errorf("Finalize mismatched offset, got %d want %d", trackedOffset, wantRows)
	}

	// Commit stream and validate.
	req := &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       managedwriter.TableParentFromStreamName(ms.StreamName()),
		WriteStreams: []string{ms.StreamName()},
	}

	resp, err := mwClient.BatchCommitWriteStreams(ctx, req)
	if err != nil {
		t.Errorf("client.BatchCommit: %v", err)
	}
	if len(resp.StreamErrors) > 0 {
		t.Errorf("stream errors present: %v", resp.StreamErrors)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "after send",
		withExactRowCount(int64(len(testSimpleData))))
}

func testLargeInsertNoRetry(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %s: %v", testTable.FullyQualifiedName(), err)
	}

	m := &testdata.SimpleMessageProto2{}
	descriptorProto := protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())

	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "before send",
		withExactRowCount(0))

	// Construct a Very Large request.
	var data [][]byte
	targetSize := 11 * 1024 * 1024 // 11 MB
	b, err := proto.Marshal(testSimpleData[0])
	if err != nil {
		t.Errorf("failed to marshal message: %v", err)
	}

	numRows := targetSize / len(b)
	data = make([][]byte, numRows)

	for i := 0; i < numRows; i++ {
		data[i] = b
	}

	result, err := ms.AppendRows(ctx, data, managedwriter.WithOffset(0))
	if err != nil {
		t.Errorf("single append failed: %v", err)
	}
	_, err = result.GetResult(ctx)
	if err != nil {
		apiErr, ok := apierror.FromError(err)
		if !ok {
			t.Errorf("GetResult error was not an instance of ApiError")
		}
		status := apiErr.GRPCStatus()
		if status.Code() != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument status, got %v", status)
		}
	}
	// our next append should fail (we don't have retries enabled).
	if _, err = ms.AppendRows(ctx, [][]byte{b}); err == nil {
		t.Fatalf("expected second append to fail, got success: %v", err)
	}

	// The send failure triggers reconnect, so an additional append will succeed.
	result, err = ms.AppendRows(ctx, [][]byte{b})
	if err != nil {
		t.Fatalf("third append expected to succeed, got error: %v", err)
	}
	_, err = result.GetResult(ctx)
	if err != nil {
		t.Errorf("failure result from third append: %v", err)
	}
}

func testLargeInsertWithRetry(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %s: %v", testTable.FullyQualifiedName(), err)
	}

	m := &testdata.SimpleMessageProto2{}
	descriptorProto := protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())

	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "before send",
		withExactRowCount(0))

	// Construct a Very Large request.
	var data [][]byte
	targetSize := 11 * 1024 * 1024 // 11 MB
	b, err := proto.Marshal(testSimpleData[0])
	if err != nil {
		t.Errorf("failed to marshal message: %v", err)
	}

	numRows := targetSize / len(b)
	data = make([][]byte, numRows)

	for i := 0; i < numRows; i++ {
		data[i] = b
	}

	result, err := ms.AppendRows(ctx, data, managedwriter.WithOffset(0))
	if err != nil {
		t.Errorf("single append failed: %v", err)
	}
	_, err = result.GetResult(ctx)
	if err != nil {
		apiErr, ok := apierror.FromError(err)
		if !ok {
			t.Errorf("GetResult error was not an instance of ApiError")
		}
		status := apiErr.GRPCStatus()
		if status.Code() != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument status, got %v", status)
		}
	}

	// The second append will succeed, but internally will show a retry.
	result, err = ms.AppendRows(ctx, [][]byte{b})
	if err != nil {
		t.Fatalf("second append expected to succeed, got error: %v", err)
	}
	_, err = result.GetResult(ctx)
	if err != nil {
		t.Errorf("failure result from second append: %v", err)
	}
	if attempts, _ := result.TotalAttempts(ctx); attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func testInstrumentation(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testedViews := []*view.View{
		managedwriter.AppendRequestsView,
		managedwriter.AppendResponsesView,
		managedwriter.AppendClientOpenView,
	}

	if err := view.Register(testedViews...); err != nil {
		t.Fatalf("couldn't register views: %v", err)
	}

	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %q: %v", testTable.FullyQualifiedName(), err)
	}

	m := &testdata.SimpleMessageProto2{}
	descriptorProto := protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())

	// setup a new stream.
	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}

	var result *managedwriter.AppendResult
	for k, mesg := range testSimpleData {
		b, err := proto.Marshal(mesg)
		if err != nil {
			t.Errorf("failed to marshal message %d: %v", k, err)
		}
		data := [][]byte{b}
		result, err = ms.AppendRows(ctx, data)
		if err != nil {
			t.Errorf("single-row append %d failed: %v", k, err)
		}
	}
	// Wait for the result to indicate ready.
	result.Ready()
	// Ick.  Stats reporting can't force flushing, and there's a race here.  Sleep to give the recv goroutine a chance
	// to report.
	time.Sleep(time.Second)

	// metric to key tag names
	wantTags := map[string][]string{
		"cloud.google.com/go/bigquery/storage/managedwriter/stream_open_count":       nil,
		"cloud.google.com/go/bigquery/storage/managedwriter/stream_open_retry_count": nil,
		"cloud.google.com/go/bigquery/storage/managedwriter/append_requests":         {"streamID"},
		"cloud.google.com/go/bigquery/storage/managedwriter/append_request_bytes":    {"streamID"},
		"cloud.google.com/go/bigquery/storage/managedwriter/append_request_errors":   {"streamID"},
		"cloud.google.com/go/bigquery/storage/managedwriter/append_rows":             {"streamID"},
	}

	for _, tv := range testedViews {
		// Attempt to further improve race failures by retrying metrics retrieval.
		metricData, err := func() ([]*view.Row, error) {
			attempt := 0
			for {
				data, err := view.RetrieveData(tv.Name)
				attempt = attempt + 1
				if attempt > 5 {
					return data, err
				}
				if err == nil && len(data) == 1 {
					return data, err
				}
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
			}
		}()
		if err != nil {
			t.Errorf("view %q RetrieveData: %v", tv.Name, err)
		}
		if mlen := len(metricData); mlen != 1 {
			t.Errorf("%q: expected 1 row of metrics, got %d", tv.Name, mlen)
			continue
		}
		if wantKeys, ok := wantTags[tv.Name]; ok {
			if wantKeys == nil {
				if n := len(tv.TagKeys); n != 0 {
					t.Errorf("expected view %q to have no keys, but %d present", tv.Name, n)
				}
			} else {
				for _, wk := range wantKeys {
					var found bool
					for _, gk := range tv.TagKeys {
						if gk.Name() == wk {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("expected view %q to have key %q, but wasn't present", tv.Name, wk)
					}
				}
			}
		}
		entry := metricData[0].Data
		sum, ok := entry.(*view.SumData)
		if !ok {
			t.Errorf("unexpected metric type: %T", entry)
		}
		got := sum.Value
		var want int64
		switch tv {
		case managedwriter.AppendRequestsView:
			want = int64(len(testSimpleData))
		case managedwriter.AppendResponsesView:
			want = int64(len(testSimpleData))
		case managedwriter.AppendClientOpenView:
			want = 1
		}

		// float comparison; diff more than error bound is error
		if math.Abs(got-float64(want)) > 0.1 {
			t.Errorf("%q: metric mismatch, got %f want %d", tv.Name, got, want)
		}
	}
}

func testSchemaEvolution(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: testdata.SimpleMessageSchema}); err != nil {
		t.Fatalf("failed to create test table %s: %v", testTable.FullyQualifiedName(), err)
	}

	m := &testdata.SimpleMessageProto2{}
	descriptorProto := protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())

	// setup a new stream.
	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	validateTableConstraints(ctx, t, bqClient, testTable, "before send",
		withExactRowCount(0))

	var result *managedwriter.AppendResult
	var curOffset int64
	var latestRow []byte
	for k, mesg := range testSimpleData {
		b, err := proto.Marshal(mesg)
		if err != nil {
			t.Errorf("failed to marshal message %d: %v", k, err)
		}
		latestRow = b
		data := [][]byte{b}
		result, err = ms.AppendRows(ctx, data, managedwriter.WithOffset(curOffset))
		if err != nil {
			t.Errorf("single-row append %d failed: %v", k, err)
		}
		curOffset = curOffset + int64(len(data))
	}
	// Wait for the result to indicate ready, then validate.
	_, err = result.GetResult(ctx)
	if err != nil {
		t.Errorf("error on append: %v", err)
	}

	validateTableConstraints(ctx, t, bqClient, testTable, "after send",
		withExactRowCount(int64(len(testSimpleData))))

	// Now, evolve the underlying table schema.
	_, err = testTable.Update(ctx, bigquery.TableMetadataToUpdate{Schema: testdata.SimpleMessageEvolvedSchema}, "")
	if err != nil {
		t.Errorf("failed to evolve table schema: %v", err)
	}

	// Resend latest row until we get a new schema notification.
	// It _should_ be possible to send duplicates, but this currently will not propagate the schema error.
	// Internal issue: b/211899346
	for {
		resp, err := ms.AppendRows(ctx, [][]byte{latestRow}, managedwriter.WithOffset(curOffset))
		if err != nil {
			t.Errorf("got error on dupe append: %v", err)
			break
		}
		curOffset = curOffset + 1
		s, err := resp.UpdatedSchema(ctx)
		if err != nil {
			t.Errorf("getting schema error: %v", err)
			break
		}
		if s != nil {
			break
		}

	}

	// ready descriptor, send an additional append
	m2 := &testdata.SimpleMessageEvolvedProto2{
		Name:  proto.String("evolved"),
		Value: proto.Int64(180),
		Other: proto.String("hello evolution"),
	}
	descriptorProto = protodesc.ToDescriptorProto(m2.ProtoReflect().Descriptor())
	b, err := proto.Marshal(m2)
	if err != nil {
		t.Errorf("failed to marshal evolved message: %v", err)
	}
	// Try to force connection errors from concurrent appends.
	// We drop setting of offset to avoid commingling out-of-order append errors.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			res, err := ms.AppendRows(ctx, [][]byte{b}, managedwriter.UpdateSchemaDescriptor(descriptorProto))
			if err != nil {
				t.Errorf("failed evolved append: %v", err)
			}
			_, err = res.GetResult(ctx)
			if err != nil {
				t.Errorf("error on evolved append: %v", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	validateTableConstraints(ctx, t, bqClient, testTable, "after send",
		withExactRowCount(int64(curOffset+5)),
		withNullCount("name", 0),
		withNonNullCount("other", 5),
	)
}

func TestIntegration_ProtoNormalization(t *testing.T) {
	mwClient, bqClient := getTestClients(context.Background(), t)
	defer mwClient.Close()
	defer bqClient.Close()

	dataset, cleanup, err := setupTestDataset(context.Background(), t, bqClient, "us-east1")
	if err != nil {
		t.Fatalf("failed to init test dataset: %v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	t.Run("group", func(t *testing.T) {
		t.Run("ComplexType", func(t *testing.T) {
			t.Parallel()
			schema := testdata.ComplexTypeSchema
			mesg := &testdata.ComplexType{
				NestedRepeatedType: []*testdata.NestedType{
					{
						InnerType: []*testdata.InnerType{
							{Value: []string{"a", "b", "c"}},
							{Value: []string{"x", "y", "z"}},
						},
					},
				},
				InnerType: &testdata.InnerType{
					Value: []string{"top"},
				},
			}
			b, err := proto.Marshal(mesg)
			if err != nil {
				t.Fatalf("proto.Marshal: %v", err)
			}
			descriptor := (mesg).ProtoReflect().Descriptor()
			testProtoNormalization(ctx, t, mwClient, bqClient, dataset, schema, descriptor, b)
		})
		t.Run("WithWellKnownTypes", func(t *testing.T) {
			t.Parallel()
			schema := testdata.WithWellKnownTypesSchema
			mesg := &testdata.WithWellKnownTypes{
				Int64Value: proto.Int64(123),
				WrappedInt64: &wrapperspb.Int64Value{
					Value: 456,
				},
				StringValue: []string{"a", "b"},
				WrappedString: []*wrapperspb.StringValue{
					{Value: "foo"},
					{Value: "bar"},
				},
			}
			b, err := proto.Marshal(mesg)
			if err != nil {
				t.Fatalf("proto.Marshal: %v", err)
			}
			descriptor := (mesg).ProtoReflect().Descriptor()
			testProtoNormalization(ctx, t, mwClient, bqClient, dataset, schema, descriptor, b)
		})
		t.Run("WithExternalEnum", func(t *testing.T) {
			t.Parallel()
			schema := testdata.ExternalEnumMessageSchema
			mesg := &testdata.ExternalEnumMessage{
				MsgA: &testdata.EnumMsgA{
					Foo: proto.String("foo"),
					Bar: testdata.ExtEnum_THING.Enum(),
				},
				MsgB: &testdata.EnumMsgB{
					Baz: testdata.ExtEnum_OTHER_THING.Enum(),
				},
			}
			b, err := proto.Marshal(mesg)
			if err != nil {
				t.Fatalf("proto.Marshal: %v", err)
			}
			descriptor := (mesg).ProtoReflect().Descriptor()
			testProtoNormalization(ctx, t, mwClient, bqClient, dataset, schema, descriptor, b)
		})
	})
}

func testProtoNormalization(ctx context.Context, t *testing.T, mwClient *managedwriter.Client, bqClient *bigquery.Client, dataset *bigquery.Dataset, schema bigquery.Schema, descriptor protoreflect.MessageDescriptor, sampleRow []byte) {
	testTable := dataset.Table(xid.New().String())
	if err := testTable.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		t.Fatalf("failed to create test table %q: %v", testTable.FullyQualifiedName(), err)
	}

	dp, err := adapt.NormalizeDescriptor(descriptor)
	if err != nil {
		t.Fatalf("NormalizeDescriptor: %v", err)
	}

	// setup a new stream.
	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.ProjectID, testTable.DatasetID, testTable.TableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(dp),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	result, err := ms.AppendRows(ctx, [][]byte{sampleRow})
	if err != nil {
		t.Errorf("append failed: %v", err)
	}

	_, err = result.GetResult(ctx)
	if err != nil {
		t.Errorf("error in response: %v", err)
	}
}

func TestIntegration_MultiplexWrites(t *testing.T) {
	mwClient, bqClient := getTestClients(context.Background(), t)
	defer mwClient.Close()
	defer bqClient.Close()

	dataset, cleanup, err := setupTestDataset(context.Background(), t, bqClient, "us-east1")
	if err != nil {
		t.Fatalf("failed to init test dataset: %v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	wantWrites := 10

	testTables := []struct {
		tbl         *bigquery.Table
		schema      bigquery.Schema
		dp          *descriptorpb.DescriptorProto
		sampleRow   []byte
		constraints []constraintOption
	}{
		{
			tbl:    dataset.Table(xid.New().String()),
			schema: testdata.SimpleMessageSchema,
			dp: func() *descriptorpb.DescriptorProto {
				m := &testdata.SimpleMessageProto2{}
				dp, _ := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
				return dp
			}(),
			sampleRow: func() []byte {
				msg := &testdata.SimpleMessageProto2{
					Name:  proto.String("sample_name"),
					Value: proto.Int64(1001),
				}
				b, _ := proto.Marshal(msg)
				return b
			}(),
			constraints: []constraintOption{
				withExactRowCount(int64(wantWrites)),
				withStringValueCount("name", "sample_name", int64(wantWrites)),
			},
		},
		{
			tbl:    dataset.Table(xid.New().String()),
			schema: testdata.ValidationBaseSchema,
			dp: func() *descriptorpb.DescriptorProto {
				m := &testdata.ValidationP2Optional{}
				dp, _ := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
				return dp
			}(),
			sampleRow: func() []byte {
				msg := &testdata.ValidationP2Optional{
					Int64Field:  proto.Int64(69),
					StringField: proto.String("validation_string"),
				}
				b, _ := proto.Marshal(msg)
				return b
			}(),
			constraints: []constraintOption{
				withExactRowCount(int64(wantWrites)),
				withStringValueCount("string_field", "validation_string", int64(wantWrites)),
			},
		},
		{
			tbl:    dataset.Table(xid.New().String()),
			schema: testdata.GithubArchiveSchema,
			dp: func() *descriptorpb.DescriptorProto {
				m := &testdata.GithubArchiveMessageProto2{}
				dp, _ := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
				return dp
			}(),
			sampleRow: func() []byte {
				msg := &testdata.GithubArchiveMessageProto2{
					Payload: proto.String("payload_string"),
					Id:      proto.String("some_id"),
				}
				b, _ := proto.Marshal(msg)
				return b
			}(),
			constraints: []constraintOption{
				withExactRowCount(int64(wantWrites)),
				withStringValueCount("payload", "payload_string", int64(wantWrites)),
			},
		},
	}

	// setup tables
	for _, testTable := range testTables {
		if err := testTable.tbl.Create(ctx, &bigquery.TableMetadata{Schema: testTable.schema}); err != nil {
			t.Fatalf("failed to create test table %q: %v", testTable.tbl.FullyQualifiedName(), err)
		}
	}

	var results []*managedwriter.AppendResult
	for i := 0; i < wantWrites; i++ {
		for k, testTable := range testTables {
			// create a writer and send a single append
			ms, err := mwClient.NewManagedStream(ctx,
				managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(testTable.tbl.ProjectID, testTable.tbl.DatasetID, testTable.tbl.TableID)),
				managedwriter.WithType(managedwriter.DefaultStream),
				managedwriter.WithSchemaDescriptor(testTable.dp),
			)
			defer ms.Close() // we won't clean these up until the end of the test, rather than per use.
			if err != nil {
				t.Fatalf("failed to create ManagedStream for table %d on iteration %d: %v", k, i, err)
			}
			res, err := ms.AppendRows(ctx, [][]byte{testTable.sampleRow})
			if err != nil {
				t.Errorf("failed to append to table %d on iteration %d: %v", k, i, err)
			}
			results = append(results, res)
		}
	}

	// drain results
	for k, res := range results {
		if _, err := res.GetResult(ctx); err != nil {
			t.Errorf("result %d yielded error: %v", k, err)
		}
	}

	// validate the tables
	for _, testTable := range testTables {
		validateTableConstraints(ctx, t, bqClient, testTable.tbl, "", testTable.constraints...)
	}

}
