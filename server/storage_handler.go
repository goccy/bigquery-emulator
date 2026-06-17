package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/goccy/go-json"
	goavro "github.com/linkedin/goavro/v2"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/logger"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
)

var (
	_ storagepb.BigQueryReadServer  = &storageReadServer{}
	_ storagepb.BigQueryWriteServer = &storageWriteServer{}
)

type storageReadServer struct {
	server    *Server
	streamMap map[string]*readStreamStatus
	mu        sync.RWMutex
}

type readStreamStatus struct {
	projectID     string
	datasetID     string
	tableID       string
	outputColumns []string
	condition     string
	dataFormat    storagepb.DataFormat
	avroSchema    *types.AVROSchema
	arrowSchema   *arrow.Schema
	schemaText    string
}

type AVROSchema struct {
	ReadSessionSchema *storagepb.ReadSession_AvroSchema
	Schema            *types.AVROSchema
	Text              string
}

type ARROWSchema struct {
	ReadSessionSchema *storagepb.ReadSession_ArrowSchema
	Schema            *arrow.Schema
	Text              string
}

func (s *storageReadServer) CreateReadSession(ctx context.Context, req *storagepb.CreateReadSessionRequest) (*storagepb.ReadSession, error) {
	sessionID := randomID()
	sessionName := fmt.Sprintf("%s/locations/%s/sessions/%s", req.Parent, "location", sessionID)
	projectID, datasetID, tableID, err := getIDsFromPath(req.ReadSession.Table)
	if err != nil {
		return nil, err
	}
	tableMetadata, err := getTableMetadata(ctx, s.server, projectID, datasetID, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}
	// MaxStreamCount is documented as a request for an upper bound, with 0
	// asking the server to choose a sensible default. Real BigQuery always
	// returns at least one stream when data is available; the emulator only
	// supports a single stream, so treat any non-1 request (including the
	// unset 0) as 1 rather than producing a session with zero streams.
	if req.MaxStreamCount > 1 {
		return nil, fmt.Errorf("currently supported only one stream")
	}
	streamID := randomID()
	streamName := fmt.Sprintf("%s/streams/%s", sessionName, streamID)
	streams := []*storagepb.ReadStream{{Name: streamName}}
	readSession := &storagepb.ReadSession{
		Name:                       sessionName,
		ExpireTime:                 timestamppb.New(time.Now().Add(1 * time.Hour)),
		Streams:                    streams,
		EstimatedTotalBytesScanned: 0,
		DataFormat:                 req.ReadSession.DataFormat,
		Table:                      req.ReadSession.Table,
		ReadOptions:                req.ReadSession.ReadOptions,
		TableModifiers:             req.ReadSession.TableModifiers,
		TraceId:                    req.ReadSession.TraceId,
	}
	// ReadOptions is optional: a client may create a read session
	// without selected fields or a row restriction to read the whole
	// table. Guard the dereference so an absent ReadOptions reads
	// every column with no filter instead of panicking.
	var (
		outputColumns []string
		condition     string
	)
	if readOptions := req.ReadSession.ReadOptions; readOptions != nil {
		outputColumns = readOptions.SelectedFields
		condition = readOptions.RowRestriction
	}
	outputColumnMap := map[string]struct{}{}
	for _, outputColumn := range outputColumns {
		outputColumnMap[outputColumn] = struct{}{}
	}
	status := &readStreamStatus{
		projectID:     projectID,
		datasetID:     datasetID,
		tableID:       tableID,
		outputColumns: outputColumns,
		condition:     condition,
		dataFormat:    readSession.DataFormat,
	}
	switch readSession.DataFormat {
	case storagepb.DataFormat_AVRO:
		schema, err := s.getAVROSchema(tableMetadata, outputColumnMap)
		if err != nil {
			return nil, err
		}
		readSession.Schema = schema.ReadSessionSchema
		status.avroSchema = schema.Schema
		status.schemaText = schema.Text
	case storagepb.DataFormat_ARROW:
		schema, err := s.getARROWSchema(tableMetadata, outputColumnMap)
		if err != nil {
			return nil, err
		}
		readSession.Schema = schema.ReadSessionSchema
		status.arrowSchema = schema.Schema
		status.schemaText = schema.Text
	default:
		return nil, fmt.Errorf("unexpected data format %s", readSession.DataFormat)
	}
	s.mu.Lock()
	s.streamMap[streamName] = status
	s.mu.Unlock()
	return readSession, nil
}

func (s *storageReadServer) ReadRows(req *storagepb.ReadRowsRequest, stream storagepb.BigQueryRead_ReadRowsServer) error {
	s.mu.RLock()
	status := s.streamMap[req.ReadStream]
	s.mu.RUnlock()

	if status == nil {
		return fmt.Errorf("failed to find stream status from %s", req.ReadStream)
	}
	ctx := context.Background()
	ctx = logger.WithLogger(ctx, s.server.logger)

	response, err := s.query(ctx, status)
	if err != nil {
		return err
	}
	switch status.dataFormat {
	case storagepb.DataFormat_AVRO:
		if err := s.sendAVRORows(status, response, stream); err != nil {
			return err
		}
	case storagepb.DataFormat_ARROW:
		if err := s.sendARROWRows(status, response, stream); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageReadServer) SplitReadStream(ctx context.Context, req *storagepb.SplitReadStreamRequest) (*storagepb.SplitReadStreamResponse, error) {
	return nil, fmt.Errorf("unimplemented split read stream")
}

func (s *storageReadServer) buildQuery(status *readStreamStatus) string {
	var columns string
	if len(status.outputColumns) != 0 {
		outputColumns := make([]string, len(status.outputColumns))
		for idx, outputColumn := range status.outputColumns {
			outputColumns[idx] = fmt.Sprintf("`%s`", outputColumn)
		}
		columns = strings.Join(outputColumns, ",")
	} else {
		columns = "*"
	}
	var condition string
	if status.condition != "" {
		condition = fmt.Sprintf("WHERE %s", status.condition)
	}
	return fmt.Sprintf("SELECT %s FROM `%s` %s", columns, status.tableID, condition)
}

func (s *storageReadServer) query(ctx context.Context, status *readStreamStatus) (*internaltypes.QueryResponse, error) {
	conn, err := s.server.connMgr.Connection(ctx, status.projectID, status.datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()

	query := s.buildQuery(status)
	return s.server.contentRepo.Query(
		ctx,
		tx,
		status.projectID,
		status.datasetID,
		query,
		nil,
	)
}

func (s *storageReadServer) getAVROSchema(tableMetadata *bigqueryv2.Table, outputColumnMap map[string]struct{}) (*AVROSchema, error) {
	avroSchema := types.TableToAVRO(tableMetadata)
	if len(outputColumnMap) != 0 {
		filteredFields := make([]*types.AVROFieldSchema, 0, len(avroSchema.Fields))
		for _, field := range avroSchema.Fields {
			if _, exists := outputColumnMap[field.Name]; exists {
				filteredFields = append(filteredFields, field)
			}
		}
		avroSchema.Fields = filteredFields
	}
	schema, err := json.Marshal(avroSchema)
	if err != nil {
		return nil, err
	}
	schemaText := string(schema)
	return &AVROSchema{
		ReadSessionSchema: &storagepb.ReadSession_AvroSchema{
			AvroSchema: &storagepb.AvroSchema{Schema: schemaText},
		},
		Schema: avroSchema,
		Text:   schemaText,
	}, nil
}

func (s *storageReadServer) sendAVRORows(status *readStreamStatus, response *internaltypes.QueryResponse, stream storagepb.BigQueryRead_ReadRowsServer) error {
	codec, err := goavro.NewCodec(status.schemaText)
	if err != nil {
		return fmt.Errorf("failed to create avro codec from schema %s: %w", status.schemaText, err)
	}
	var buf []byte
	for _, row := range response.Rows {
		value, err := row.AVROValue(status.avroSchema.Namespace, status.avroSchema.Fields)
		if err != nil {
			return fmt.Errorf("failed to convert response fields to avro value: %w", err)
		}
		b, err := codec.BinaryFromNative(buf, value)
		if err != nil {
			return fmt.Errorf("failed to encode binary from go value: %w", err)
		}
		buf = b
	}
	rows := &storagepb.ReadRowsResponse_AvroRows{
		AvroRows: &storagepb.AvroRows{
			SerializedBinaryRows: buf,
			RowCount:             int64(response.TotalRows),
		},
	}
	if err := stream.Send(&storagepb.ReadRowsResponse{
		Rows:     rows,
		RowCount: int64(response.TotalRows),
		Schema: &storagepb.ReadRowsResponse_AvroSchema{
			AvroSchema: &storagepb.AvroSchema{
				Schema: status.schemaText,
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send read rows response for avro format: %w", err)
	}
	return nil
}

func (s *storageReadServer) getARROWSchema(tableMetadata *bigqueryv2.Table, outputColumnMap map[string]struct{}) (*ARROWSchema, error) {
	arrowSchema, err := types.TableToARROW(tableMetadata)
	if err != nil {
		return nil, err
	}
	if len(outputColumnMap) != 0 {
		filteredFields := make([]arrow.Field, 0, len(arrowSchema.Fields()))
		for _, field := range arrowSchema.Fields() {
			if _, exists := outputColumnMap[field.Name]; exists {
				filteredFields = append(filteredFields, field)
			}
		}
		arrowSchema = arrow.NewSchema(filteredFields, nil)
	}
	schemaText := arrowSchema.String()
	schema, err := s.getSerializedARROWSchema(arrowSchema)
	if err != nil {
		return nil, err
	}
	return &ARROWSchema{
		ReadSessionSchema: &storagepb.ReadSession_ArrowSchema{
			ArrowSchema: &storagepb.ArrowSchema{
				SerializedSchema: schema,
			},
		},
		Schema: arrowSchema,
		Text:   schemaText,
	}, nil
}

// splitIPCStream splits an Arrow IPC stream that contains exactly one schema
// message followed by one record-batch message (no EOS marker) into the two
// bare IPC-encapsulated messages required by the BigQuery Storage Read API.
//
// Each IPC message on the wire is framed as:
//
//	[continuation: 4 bytes][metaLen: 4 bytes LE][metadata: metaLen bytes][padding to 8-byte][body]
//
// The schema message body is always empty, so its total byte length is
// 4+4+metaLen rounded up to the next 8-byte boundary. Everything that follows
// is the record-batch message.
func splitIPCStream(data []byte) (schemaMsg, recordBatchMsg []byte, err error) {
	if len(data) < 8 {
		return nil, nil, fmt.Errorf("arrow IPC data too short (%d bytes)", len(data))
	}
	metaLen := int(binary.LittleEndian.Uint32(data[4:8]))
	schemaEnd := 8 + metaLen
	if pad := schemaEnd % 8; pad != 0 {
		schemaEnd += 8 - pad
	}
	if schemaEnd > len(data) {
		return nil, nil, fmt.Errorf("schema IPC message overruns buffer (need %d, have %d)", schemaEnd, len(data))
	}
	return data[:schemaEnd], data[schemaEnd:], nil
}

func (s *storageReadServer) getSerializedARROWSchema(schema *arrow.Schema) ([]byte, error) {
	mem := memory.NewGoAllocator()
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithAllocator(mem), ipc.WithSchema(schema))
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	record := builder.NewRecord()
	if err := writer.Write(record); err != nil {
		return nil, err
	}
	record.Release()
	// Do not call writer.Close(): we do not want the EOS marker.
	// buf now holds [schema_message][empty_record_batch_message].
	schemaBytes, _, err := splitIPCStream(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to extract schema IPC message: %w", err)
	}
	return schemaBytes, nil
}

func (s *storageReadServer) sendARROWRows(status *readStreamStatus, response *internaltypes.QueryResponse, stream storagepb.BigQueryRead_ReadRowsServer) error {
	schema, err := s.getSerializedARROWSchema(status.arrowSchema)
	if err != nil {
		return err
	}
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, status.arrowSchema)
	defer builder.Release()
	for _, row := range response.Rows {
		if err := row.AppendValueToARROWBuilder(builder); err != nil {
			return err
		}
	}
	record := builder.NewRecord()
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithAllocator(mem), ipc.WithSchema(status.arrowSchema))
	if err := writer.Write(record); err != nil {
		return err
	}
	record.Release()
	// Do not call writer.Close(): we do not want the EOS marker.
	// buf now holds [schema_message][record_batch_message]; extract only the record batch.
	_, recordBatchBytes, err := splitIPCStream(buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to extract record batch IPC message: %w", err)
	}
	rows := &storagepb.ReadRowsResponse_ArrowRecordBatch{
		ArrowRecordBatch: &storagepb.ArrowRecordBatch{
			SerializedRecordBatch: recordBatchBytes,
			RowCount:              int64(response.TotalRows),
		},
	}
	if err := stream.Send(&storagepb.ReadRowsResponse{
		Rows:     rows,
		RowCount: int64(response.TotalRows),
		Schema: &storagepb.ReadRowsResponse_ArrowSchema{
			ArrowSchema: &storagepb.ArrowSchema{
				SerializedSchema: schema,
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send read rows response for arrow format: %w", err)
	}
	return nil
}

type storageWriteServer struct {
	server    *Server
	streamMap map[string]*writeStreamStatus
	mu        sync.RWMutex
}

type writeStreamStatus struct {
	mu            sync.Mutex
	streamType    storagepb.WriteStream_Type
	stream        *storagepb.WriteStream
	projectID     string
	datasetID     string
	tableID       string
	tableMetadata *bigqueryv2.Table
	rows          types.Data
	finalized     bool
}

func newWriteStreamStatus(streamName string, streamType storagepb.WriteStream_Type, projectID, datasetID, tableID string, tableMetadata *bigqueryv2.Table) *writeStreamStatus {
	createTime := timestamppb.New(time.Now())
	var commitTime *timestamppb.Timestamp
	if streamType == storagepb.WriteStream_COMMITTED {
		commitTime = createTime
	}
	schema := types.TableToProto(tableMetadata)
	stream := &storagepb.WriteStream{
		Name:        streamName,
		Type:        streamType,
		CreateTime:  createTime,
		CommitTime:  commitTime,
		TableSchema: schema,
		WriteMode:   storagepb.WriteStream_INSERT,
	}
	return &writeStreamStatus{
		streamType:    streamType,
		stream:        stream,
		projectID:     projectID,
		datasetID:     datasetID,
		tableID:       tableID,
		tableMetadata: tableMetadata,
	}
}

func (s *writeStreamStatus) ensureAppendable() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalized {
		return fmt.Errorf("stream is already finalized")
	}
	return nil
}

func (s *writeStreamStatus) appendBufferedRows(data types.Data) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalized {
		return fmt.Errorf("stream is already finalized")
	}
	s.rows = append(s.rows, data...)
	return nil
}

func (s *storageWriteServer) CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest) (*storagepb.WriteStream, error) {
	projectID, datasetID, tableID, err := getIDsFromPath(req.Parent)
	if err != nil {
		return nil, err
	}
	tableMetadata, err := getTableMetadata(ctx, s.server, projectID, datasetID, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}
	streamID := randomID()
	streamName := fmt.Sprintf("%s/streams/%s", req.Parent, streamID)
	streamType := req.GetWriteStream().GetType()
	streamStatus := newWriteStreamStatus(streamName, streamType, projectID, datasetID, tableID, tableMetadata)
	s.mu.Lock()
	s.streamMap[streamName] = streamStatus
	s.mu.Unlock()
	return streamStatus.stream, nil
}

func (s *storageWriteServer) AppendRows(stream storagepb.BigQueryWrite_AppendRowsServer) error {
	req, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	msgDesc, err := s.getMessageDescriptor(req)
	if err != nil {
		return err
	}
	status, streamName, err := s.appendRows(req, msgDesc, stream, nil, "")
	if err != nil {
		return fmt.Errorf("failed to append rows: %w", err)
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		status, streamName, err = s.appendRows(req, msgDesc, stream, status, streamName)
		if err != nil {
			return fmt.Errorf("failed to append rows: %w", err)
		}
	}
	return nil
}

func (s *storageWriteServer) getMessageDescriptor(req *storagepb.AppendRowsRequest) (protoreflect.MessageDescriptor, error) {
	descProto := req.GetProtoRows().GetWriterSchema().GetProtoDescriptor()
	fdProto := &descriptorpb.FileDescriptorProto{
		Name: proto.String("proto"),
		MessageType: []*descriptorpb.DescriptorProto{
			descProto,
		},
	}
	fd, err := protodesc.NewFile(fdProto, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create file descriptor: %w", err)
	}
	return fd.Messages().ByName(protoreflect.Name(descProto.GetName())), nil
}

func (s *storageWriteServer) appendRows(req *storagepb.AppendRowsRequest, msgDesc protoreflect.MessageDescriptor, stream storagepb.BigQueryWrite_AppendRowsServer, fallbackStatus *writeStreamStatus, fallbackStreamName string) (*writeStreamStatus, string, error) {
	streamName := req.GetWriteStream()
	var status *writeStreamStatus
	if streamName == "" {
		status = fallbackStatus
		streamName = fallbackStreamName
	} else {
		var err error
		status, streamName, err = s.getOrCreateWriteStreamStatus(stream.Context(), streamName)
		if err != nil {
			return nil, "", err
		}
	}
	if status == nil {
		return nil, "", fmt.Errorf("write stream is not specified")
	}
	offset := int64(0)
	if req.GetOffset() != nil {
		offset = req.GetOffset().Value
	}
	rows := req.GetProtoRows().GetRows().GetSerializedRows()
	data, err := s.decodeData(msgDesc, rows)
	if err != nil {
		s.sendErrorMessage(stream, streamName, err)
		return nil, "", err
	}
	if status.streamType == storagepb.WriteStream_COMMITTED {
		if err := status.ensureAppendable(); err != nil {
			return nil, "", err
		}
		ctx := context.Background()
		ctx = logger.WithLogger(ctx, s.server.logger)

		conn, err := s.server.connMgr.Connection(ctx, status.projectID, status.datasetID)
		if err != nil {
			s.sendErrorMessage(stream, streamName, err)
			return nil, "", err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			s.sendErrorMessage(stream, streamName, err)
			return nil, "", err
		}
		defer tx.RollbackIfNotCommitted()
		if err := s.insertTableData(ctx, tx, status, data); err != nil {
			s.sendErrorMessage(stream, streamName, err)
			return nil, "", err
		}
		if err := tx.Commit(); err != nil {
			s.sendErrorMessage(stream, streamName, err)
			return nil, "", err
		}
	} else {
		if err := status.appendBufferedRows(data); err != nil {
			return nil, "", err
		}
	}
	if err := s.sendResult(stream, streamName, offset+int64(len(rows))); err != nil {
		return nil, "", err
	}
	return status, streamName, nil

}

func (s *storageWriteServer) lookupWriteStreamStatus(streamName string) (*writeStreamStatus, string, bool) {
	canonicalName := canonicalWriteStreamName(streamName)
	s.mu.RLock()
	status, exists := s.streamMap[canonicalName]
	s.mu.RUnlock()
	return status, canonicalName, exists
}

func (s *storageWriteServer) getOrCreateWriteStreamStatus(ctx context.Context, streamName string) (*writeStreamStatus, string, error) {
	status, canonicalName, exists := s.lookupWriteStreamStatus(streamName)
	if exists {
		return status, canonicalName, nil
	}
	if !isDefaultWriteStreamName(streamName) {
		return nil, "", fmt.Errorf("failed to get stream from %s", streamName)
	}
	status, canonicalName, err := s.createDefaultStreamStatus(ctx, streamName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get stream from %s", streamName)
	}
	return status, canonicalName, nil
}

func (s *storageWriteServer) sendResult(stream storagepb.BigQueryWrite_AppendRowsServer, streamName string, offset int64) error {
	return stream.Send(&storagepb.AppendRowsResponse{
		WriteStream: streamName,
		Response: &storagepb.AppendRowsResponse_AppendResult_{
			AppendResult: &storagepb.AppendRowsResponse_AppendResult{
				Offset: wrapperspb.Int64(offset),
			},
		},
	})
}

func (s *storageWriteServer) sendErrorMessage(stream storagepb.BigQueryWrite_AppendRowsServer, streamName string, err error) error {
	return stream.Send(&storagepb.AppendRowsResponse{
		WriteStream: streamName,
		Response: &storagepb.AppendRowsResponse_Error{
			Error: &status.Status{
				Code:    int32(codes.Internal),
				Message: err.Error(),
			},
		},
	})
}

func (s *storageWriteServer) decodeData(msgDesc protoreflect.MessageDescriptor, rows [][]byte) (types.Data, error) {
	data := types.Data{}
	for _, row := range rows {
		msg := dynamicpb.NewMessage(msgDesc)
		rowData, err := s.decodeRowData(row, msg)
		if err != nil {
			return nil, err
		}
		data = append(data, rowData)
	}
	return data, nil
}

func (s *storageWriteServer) decodeRowData(data []byte, msg *dynamicpb.Message) (map[string]interface{}, error) {
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("failed to decode message: %w", err)
	}
	ret := map[string]interface{}{}
	var decodeErr error
	msg.Range(func(f protoreflect.FieldDescriptor, val protoreflect.Value) bool {
		v, err := s.decodeProtoReflectValue(f, val)
		if err != nil {
			decodeErr = err
			return false
		}
		ret[f.TextName()] = v
		return true
	})
	return ret, decodeErr
}

func (s *storageWriteServer) decodeProtoReflectValue(f protoreflect.FieldDescriptor, v protoreflect.Value) (interface{}, error) {
	if f.IsList() {
		list := v.List()
		ret := make([]interface{}, 0, list.Len())
		if !list.IsValid() {
			return ret, nil
		}
		for i := 0; i < list.Len(); i++ {
			vv := list.Get(i)
			elem, err := s.decodeProtoReflectValueFromKind(f.Kind(), vv)
			if err != nil {
				return nil, err
			}
			ret = append(ret, elem)
		}
		return ret, nil
	}
	return s.decodeProtoReflectValueFromKind(f.Kind(), v)
}

func (s *storageWriteServer) decodeProtoReflectValueFromKind(kind protoreflect.Kind, v protoreflect.Value) (interface{}, error) {
	if !v.IsValid() {
		return nil, nil
	}
	switch kind {
	case protoreflect.BoolKind:
		return v.Bool(), nil
	case protoreflect.EnumKind:
		return v.Enum(), nil
	case protoreflect.Int32Kind:
		return v.Int(), nil
	case protoreflect.Sint32Kind:
		return v.Int(), nil
	case protoreflect.Uint32Kind:
		return v.Uint(), nil
	case protoreflect.Int64Kind:
		return v.Int(), nil
	case protoreflect.Sint64Kind:
		return v.Int(), nil
	case protoreflect.Uint64Kind:
		return v.Uint(), nil
	case protoreflect.Sfixed32Kind:
		return v.Int(), nil
	case protoreflect.Fixed32Kind:
		return v.Int(), nil
	case protoreflect.FloatKind:
		return v.Float(), nil
	case protoreflect.Sfixed64Kind:
		return v.Int(), nil
	case protoreflect.Fixed64Kind:
		return v.Float(), nil
	case protoreflect.DoubleKind:
		return v.Float(), nil
	case protoreflect.StringKind:
		return v.String(), nil
	case protoreflect.BytesKind:
		return v.Bytes(), nil
	case protoreflect.MessageKind:
		msg := v.Message()
		// A google.protobuf scalar wrapper (wrappers.proto) carries a
		// single `value` field; BigQuery maps a wrapper-typed proto
		// field to its underlying scalar column, so unwrap it rather
		// than rendering a one-field STRUCT.
		if scalar, ok, err := s.unwrapWrapperValue(msg); ok || err != nil {
			return scalar, err
		}
		structV := map[string]interface{}{}
		var decodeErr error
		msg.Range(func(f protoreflect.FieldDescriptor, val protoreflect.Value) bool {
			v, err := s.decodeProtoReflectValue(f, val)
			if err != nil {
				decodeErr = err
				return false
			}
			structV[f.TextName()] = v
			return true
		})
		return structV, decodeErr
	case protoreflect.GroupKind:
		return nil, fmt.Errorf("unsupported group kind for storage api")
	}
	return nil, fmt.Errorf("specified unknown kind")
}

// wrapperMessageNames is the set of google.protobuf scalar wrapper messages
// (wrappers.proto). Each carries a single scalar `value` field.
var wrapperMessageNames = map[protoreflect.FullName]struct{}{
	"google.protobuf.DoubleValue": {},
	"google.protobuf.FloatValue":  {},
	"google.protobuf.Int64Value":  {},
	"google.protobuf.UInt64Value": {},
	"google.protobuf.Int32Value":  {},
	"google.protobuf.UInt32Value": {},
	"google.protobuf.BoolValue":   {},
	"google.protobuf.StringValue": {},
	"google.protobuf.BytesValue":  {},
}

// unwrapWrapperValue reports whether msg is a google.protobuf scalar wrapper
// and, if so, returns its underlying `value`. A wrapper-typed proto field
// maps to its scalar BigQuery column, so the Storage Write API decoder must
// hand back the bare scalar instead of a single-field STRUCT, which the
// schema-driven normalizer cannot reconcile with a scalar column.
func (s *storageWriteServer) unwrapWrapperValue(msg protoreflect.Message) (interface{}, bool, error) {
	if _, ok := wrapperMessageNames[msg.Descriptor().FullName()]; !ok {
		return nil, false, nil
	}
	valueField := msg.Descriptor().Fields().ByName("value")
	if valueField == nil {
		return nil, false, nil
	}
	v, err := s.decodeProtoReflectValueFromKind(valueField.Kind(), msg.Get(valueField))
	return v, true, err
}

func (s *storageWriteServer) insertTableData(ctx context.Context, tx *connection.Tx, status *writeStreamStatus, data types.Data) error {
	tableDef, err := types.NewTableWithSchema(status.tableMetadata, data)
	if err != nil {
		return err
	}
	if err := s.server.contentRepo.AddTableData(
		ctx,
		tx,
		status.projectID,
		status.datasetID,
		tableDef,
	); err != nil {
		return fmt.Errorf("failed to add table data: %w", err)
	}
	return nil
}

func (s *storageWriteServer) GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest) (*storagepb.WriteStream, error) {
	status, _, err := s.getOrCreateWriteStreamStatus(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to find stream from %s", req.Name)
	}
	return status.stream, nil
}

func (s *storageWriteServer) FinalizeWriteStream(ctx context.Context, req *storagepb.FinalizeWriteStreamRequest) (*storagepb.FinalizeWriteStreamResponse, error) {
	status, _, exists := s.lookupWriteStreamStatus(req.GetName())
	if !exists {
		return nil, fmt.Errorf("failed to get stream from %s", req.GetName())
	}
	status.mu.Lock()
	status.finalized = true
	rowCount := int64(len(status.rows))
	status.mu.Unlock()
	return &storagepb.FinalizeWriteStreamResponse{
		RowCount: rowCount,
	}, nil
}

func (s *storageWriteServer) BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	var streamErrors []*storagepb.StorageError
	for _, streamName := range req.GetWriteStreams() {
		status, _, exists := s.lookupWriteStreamStatus(streamName)
		if !exists {
			streamErrors = append(streamErrors, &storagepb.StorageError{
				Code:         storagepb.StorageError_STREAM_NOT_FOUND,
				Entity:       streamName,
				ErrorMessage: fmt.Sprintf("failed to find stream from %s", streamName),
			})
			continue
		}
		status.mu.Lock()
		rows := append(types.Data(nil), status.rows...)
		status.mu.Unlock()
		conn, err := s.server.connMgr.Connection(ctx, status.projectID, status.datasetID)
		if err != nil {
			streamErrors = append(streamErrors, s.createUnspecifiedStorageError(streamName, err))
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			streamErrors = append(streamErrors, s.createUnspecifiedStorageError(streamName, err))
			continue
		}
		defer tx.RollbackIfNotCommitted()
		if err := s.insertTableData(ctx, tx, status, rows); err != nil {
			streamErrors = append(streamErrors, s.createUnspecifiedStorageError(streamName, err))
			continue
		}
		if err := tx.Commit(); err != nil {
			streamErrors = append(streamErrors, s.createUnspecifiedStorageError(streamName, err))
		}
	}
	return &storagepb.BatchCommitWriteStreamsResponse{
		CommitTime:   timestamppb.New(time.Now()),
		StreamErrors: streamErrors,
	}, nil
}

func (s *storageWriteServer) createUnspecifiedStorageError(streamName string, err error) *storagepb.StorageError {
	return &storagepb.StorageError{
		Code:         storagepb.StorageError_STORAGE_ERROR_CODE_UNSPECIFIED,
		Entity:       streamName,
		ErrorMessage: err.Error(),
	}
}

func (s *storageWriteServer) FlushRows(ctx context.Context, req *storagepb.FlushRowsRequest) (*storagepb.FlushRowsResponse, error) {
	streamName := req.GetWriteStream()
	status, _, exists := s.lookupWriteStreamStatus(streamName)
	if !exists {
		return nil, fmt.Errorf("failed to find stream from %s", streamName)
	}
	if req.GetOffset() == nil {
		return nil, fmt.Errorf("offset is required")
	}
	offset := req.GetOffset().Value
	status.mu.Lock()
	if offset < 0 || offset >= int64(len(status.rows)) {
		status.mu.Unlock()
		return nil, fmt.Errorf("offset %d is out of range", offset)
	}
	rows := append(types.Data(nil), status.rows[:offset+1]...)
	status.mu.Unlock()
	conn, err := s.server.connMgr.Connection(ctx, status.projectID, status.datasetID)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	if err := s.insertTableData(ctx, tx, status, rows); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &storagepb.FlushRowsResponse{
		Offset: offset,
	}, nil
}

// According to google documentation every table has a special stream named
// `_default` to which data can be written. This stream doesn't need to be
// created using CreateWriteStream. Client libraries use both
// projects/<projectId>/datasets/<datasetId>/tables/<tableId>/_default and
// projects/<projectId>/datasets/<datasetId>/tables/<tableId>/streams/_default,
// so accept both forms.
func (s *storageWriteServer) createDefaultStream(ctx context.Context, req *storagepb.GetWriteStreamRequest) (*storagepb.WriteStream, error) {
	status, _, err := s.createDefaultStreamStatus(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	return status.stream, nil
}

func (s *storageWriteServer) createDefaultStreamStatus(ctx context.Context, streamName string) (*writeStreamStatus, string, error) {
	tablePath, err := defaultWriteStreamTablePath(streamName)
	if err != nil {
		return nil, "", err
	}
	canonicalName := canonicalDefaultWriteStreamName(tablePath)
	projectID, datasetID, tableID, err := getIDsFromPath(tablePath)
	if err != nil {
		return nil, "", err
	}
	tableMetadata, err := getTableMetadata(ctx, s.server, projectID, datasetID, tableID)
	if err != nil {
		return nil, "", err
	}
	streamStatus := newWriteStreamStatus(canonicalName, storagepb.WriteStream_COMMITTED, projectID, datasetID, tableID, tableMetadata)
	s.mu.Lock()
	defer s.mu.Unlock()
	if status, exists := s.streamMap[canonicalName]; exists {
		return status, canonicalName, nil
	}
	s.streamMap[canonicalName] = streamStatus
	return streamStatus, canonicalName, nil
}

func isDefaultWriteStreamName(name string) bool {
	_, err := defaultWriteStreamTablePath(name)
	return err == nil
}

func canonicalWriteStreamName(name string) string {
	tablePath, err := defaultWriteStreamTablePath(name)
	if err != nil {
		return name
	}
	return canonicalDefaultWriteStreamName(tablePath)
}

func canonicalDefaultWriteStreamName(tablePath string) string {
	return tablePath + "/_default"
}

func defaultWriteStreamTablePath(name string) (string, error) {
	switch {
	case strings.HasSuffix(name, "/streams/_default"):
		return strings.TrimSuffix(name, "/streams/_default"), nil
	case strings.HasSuffix(name, "/_default"):
		return strings.TrimSuffix(name, "/_default"), nil
	default:
		return "", fmt.Errorf("unexpected default stream name: %s", name)
	}
}

func getIDsFromPath(path string) (string, string, string, error) {
	paths := strings.Split(path, "/")
	if len(paths)%2 != 0 {
		return "", "", "", fmt.Errorf("unexpected table path: %s", path)
	}
	var (
		projectID string
		datasetID string
		tableID   string
	)
	for i := 0; i < len(paths); i += 2 {
		switch paths[i] {
		case "projects":
			projectID = paths[i+1]
		case "datasets":
			datasetID = paths[i+1]
		case "tables":
			tableID = paths[i+1]
		}
	}
	if projectID == "" {
		return "", "", "", fmt.Errorf("unspecified project id")
	}
	if datasetID == "" {
		return "", "", "", fmt.Errorf("unspecified dataset id")
	}
	if tableID == "" {
		return "", "", "", fmt.Errorf("unspecified table id")
	}
	return projectID, datasetID, tableID, nil
}

func getTableMetadata(ctx context.Context, server *Server, projectID, datasetID, tableID string) (*bigqueryv2.Table, error) {
	project, err := server.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		return nil, err
	}
	if project == nil {
		return nil, fmt.Errorf("project %s is not found", projectID)
	}
	dataset := project.Dataset(datasetID)
	if dataset == nil {
		return nil, fmt.Errorf("dataset %s is not found in project %s", datasetID, projectID)
	}
	table := dataset.Table(tableID)
	if table == nil {
		return nil, fmt.Errorf("table %s is not found in dataset %s", tableID, datasetID)
	}
	return table.Content()
}

func registerStorageServer(grpcServer *grpc.Server, srv *Server) {
	storagepb.RegisterBigQueryReadServer(
		grpcServer,
		&storageReadServer{
			server:    srv,
			streamMap: map[string]*readStreamStatus{},
		},
	)
	storagepb.RegisterBigQueryWriteServer(
		grpcServer,
		&storageWriteServer{
			server:    srv,
			streamMap: map[string]*writeStreamStatus{},
		},
	)
}
