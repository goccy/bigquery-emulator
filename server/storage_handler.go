package server

import (
	"bytes"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/anypb"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/goccy/go-json"
	"github.com/linkedin/goavro/v2"
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
	streams := make([]*storagepb.ReadStream, 0, req.MaxStreamCount)
	streamID := randomID()
	streamName := fmt.Sprintf("%s/streams/%s", sessionName, streamID)
	if req.MaxStreamCount > 1 {
		return nil, fmt.Errorf("currently supported only one stream")
	}
	for i := int32(0); i < req.MaxStreamCount; i++ {
		streams = append(streams, &storagepb.ReadStream{
			Name: streamName,
		})
	}
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
	outputColumns := req.ReadSession.ReadOptions.SelectedFields
	condition := req.ReadSession.ReadOptions.RowRestriction
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
		value, err := row.AVROValue(status.avroSchema.Fields)
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

func (s *storageReadServer) getSerializedARROWSchema(schema *arrow.Schema) ([]byte, error) {
	mem := memory.NewGoAllocator()
	buf := new(bytes.Buffer)
	writer := ipc.NewWriter(buf, ipc.WithAllocator(mem), ipc.WithSchema(schema))
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	record := builder.NewRecord()
	if err := writer.Write(record); err != nil {
		return nil, err
	}
	record.Release()
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
	buf := new(bytes.Buffer)
	writer := ipc.NewWriter(buf, ipc.WithAllocator(mem), ipc.WithSchema(status.arrowSchema))
	if err := writer.Write(record); err != nil {
		return err
	}
	record.Release()
	if err := writer.Close(); err != nil {
		return err
	}
	rows := &storagepb.ReadRowsResponse_ArrowRecordBatch{
		ArrowRecordBatch: &storagepb.ArrowRecordBatch{
			SerializedRecordBatch: buf.Bytes(),
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
	streamType    storagepb.WriteStream_Type
	stream        *storagepb.WriteStream
	projectID     string
	datasetID     string
	tableID       string
	tableMetadata *bigqueryv2.Table
	rows          types.Data
	finalized     bool
	offset        int64
	defaultStream bool
	appendStream  storagepb.BigQueryWrite_AppendRowsServer
	messageDesc   protoreflect.MessageDescriptor
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

	writeStream := req.GetWriteStream()
	if writeStream == nil {
		return nil, fmt.Errorf("missing write stream")
	}
	streamName := writeStream.GetName()
	createTime := timestamppb.New(time.Now())
	streamType := writeStream.GetType()
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

	s.mu.Lock()
	s.streamMap[streamName] = &writeStreamStatus{
		streamType:    streamType,
		stream:        stream,
		projectID:     projectID,
		datasetID:     datasetID,
		tableID:       tableID,
		tableMetadata: tableMetadata,
		defaultStream: strings.HasSuffix(streamName, "_default"),
	}
	s.mu.Unlock()
	return stream, nil
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

	streamStatus, err := s.getWriteStream(req.GetWriteStream())
	if err != nil {
		return fmt.Errorf("failed to get write stream: %w", err)
	}

	streamStatus.appendStream = stream
	streamStatus.messageDesc = msgDesc

	// Validate the proto message descriptor against the stream table schema.
	nameToFieldMap := map[string]*bigqueryv2.TableFieldSchema{}
	for _, field := range streamStatus.tableMetadata.Schema.Fields {
		nameToFieldMap[field.Name] = field
	}
	// TODO(dm): check nested fields
	for i := 0; i < streamStatus.messageDesc.Fields().Len(); i++ {
		field := streamStatus.messageDesc.Fields().Get(i)
		schemaField, ok := nameToFieldMap[field.JSONName()]
		if !ok {
			err := fmt.Errorf("proto field %s not found in table %s schema", field.JSONName(), streamStatus.tableMetadata.Id)
			s.sendErrorMessage(streamStatus.appendStream, streamStatus.stream.GetName(), codes.InvalidArgument, err.Error())
			return err
		}
		kind := field.Kind()

		if kind.String() == "message" {
			messageName := string(field.Message().FullName())
			if strings.HasSuffix(messageName, "google_protobuf_Int64Value") {
				kind = protoreflect.Int64Kind
			} else if strings.HasSuffix(messageName, "google_protobuf_UInt64Value") {
				kind = protoreflect.Uint64Kind
			} else if strings.HasSuffix(messageName, "google_protobuf_Int42Value") {
				kind = protoreflect.Int32Kind
			} else if strings.HasSuffix(messageName, "google_protobuf_UInt32Value") {
				kind = protoreflect.Uint32Kind
			} else if strings.HasSuffix(messageName, "google_protobuf_StringValue") {
				kind = protoreflect.StringKind
			} else if strings.HasSuffix(messageName, "google_protobuf_BytesValue") {
				kind = protoreflect.BytesKind
			} else if strings.HasSuffix(messageName, "google_protobuf_DoubleValue") {
				kind = protoreflect.DoubleKind
			} else if strings.HasSuffix(messageName, "google_protobuf_FloatValue") {
				kind = protoreflect.FloatKind
			} else if strings.HasSuffix(messageName, "google_protobuf_BoolValue") {
				kind = protoreflect.BoolKind
			}
		}

		invalidSchema := false
		switch schemaField.Type {
		// TODO(dm): support other types, e.g. TIMESTAMP, DATE, TIME, DATETIME, GEOGRAPHY, JSON, RECORD and REPEATED
		case "INTEGER", "INT64":
			switch kind {
			case protoreflect.Int32Kind, protoreflect.Uint32Kind, protoreflect.Int64Kind, protoreflect.Uint64Kind, protoreflect.EnumKind:
			default:
				invalidSchema = true
			}
		case "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC":
			switch kind {
			case protoreflect.Int32Kind, protoreflect.Uint32Kind, protoreflect.Int64Kind, protoreflect.Uint64Kind, protoreflect.FloatKind, protoreflect.StringKind, protoreflect.BytesKind:
			default:
				invalidSchema = true
			}
		case "BOOLEAN", "BOOL":
			switch kind {
			case protoreflect.Int32Kind, protoreflect.Uint32Kind, protoreflect.Int64Kind, protoreflect.Uint64Kind, protoreflect.BoolKind:
			default:
				invalidSchema = true
			}
		case "BYTES":
			switch kind {
			case protoreflect.BytesKind, protoreflect.StringKind:
			default:
				invalidSchema = true
			}
		case "STRING":
			switch kind {
			case protoreflect.EnumKind, protoreflect.StringKind:
			default:
				invalidSchema = true
			}
		}
		if invalidSchema {
			schemaErr := fmt.Errorf("schema field mismatch for %s, bigquery expects %q and the proto message had a %q type", schemaField.Name, schemaField.Type, kind)
			s.sendErrorMessage(streamStatus.appendStream, streamStatus.stream.GetName(), codes.InvalidArgument, schemaErr.Error())
			return schemaErr
		}
	}

	for {
		if err := s.appendRows(req, streamStatus); err != nil {
			return fmt.Errorf("failed to append rows: %w", err)
		}

		req, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
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

func (s *storageWriteServer) appendRows(req *storagepb.AppendRowsRequest, streamStatus *writeStreamStatus) error {
	if streamStatus.finalized {
		return s.sendStorageErr(
			streamStatus.appendStream,
			streamStatus.stream.GetName(),
			codes.InvalidArgument,
			storagepb.StorageError_STREAM_FINALIZED,
			"stream has been finalized and cannot be appended")
	}

	var offset int64

	if !streamStatus.defaultStream {
		if req.GetOffset() != nil {
			offset = req.GetOffset().GetValue()
		}
		if offset > streamStatus.offset {
			return s.sendStorageErr(
				streamStatus.appendStream,
				streamStatus.stream.GetName(),
				codes.OutOfRange,
				storagepb.StorageError_OFFSET_OUT_OF_RANGE,
				fmt.Sprintf("the offset is out of range, expected %d but received %d", streamStatus.offset, offset))
		} else if offset < streamStatus.offset {
			return s.sendStorageErr(
				streamStatus.appendStream,
				streamStatus.stream.GetName(),
				codes.AlreadyExists,
				storagepb.StorageError_OFFSET_ALREADY_EXISTS,
				fmt.Sprintf("the offset is within the stream, expected %d but received %d", streamStatus.offset, offset))
		}
	}

	rows := req.GetProtoRows().GetRows().GetSerializedRows()
	data, err := s.decodeData(streamStatus.messageDesc, rows)
	if err != nil {
		s.sendErrorMessage(streamStatus.appendStream, streamStatus.stream.GetName(), codes.InvalidArgument, err.Error())
		return err
	}
	if streamStatus.streamType == storagepb.WriteStream_COMMITTED {
		ctx := context.Background()
		ctx = logger.WithLogger(ctx, s.server.logger)

		conn, err := s.server.connMgr.Connection(ctx, streamStatus.projectID, streamStatus.datasetID)
		if err != nil {
			s.sendErrorMessage(streamStatus.appendStream, streamStatus.stream.GetName(), codes.Internal, err.Error())
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			s.sendErrorMessage(streamStatus.appendStream, streamStatus.stream.GetName(), codes.Internal, err.Error())
			return err
		}
		defer tx.RollbackIfNotCommitted()
		if err := s.insertTableData(ctx, tx, streamStatus, data); err != nil {
			s.sendErrorMessage(streamStatus.appendStream, streamStatus.stream.GetName(), codes.Internal, err.Error())
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		if !streamStatus.defaultStream {
			streamStatus.offset += int64(len(rows))
		} else {
			offset = -1
		}
	} else {
		streamStatus.rows = append(streamStatus.rows, data...)
	}
	return s.sendResult(streamStatus.appendStream, streamStatus.stream.GetName(), offset)
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

func (s *storageWriteServer) sendErrorMessage(
	stream storagepb.BigQueryWrite_AppendRowsServer,
	streamName string,
	errCode codes.Code,
	errMessage string) error {
	return stream.Send(&storagepb.AppendRowsResponse{
		WriteStream: streamName,
		Response: &storagepb.AppendRowsResponse_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: errMessage,
			},
		},
	})
}

func (s *storageWriteServer) sendStorageErr(
	stream storagepb.BigQueryWrite_AppendRowsServer,
	streamName string,
	errCode codes.Code,
	storageErrorCode storagepb.StorageError_StorageErrorCode,
	errMessage string) error {
	storageErrBy, err := proto.Marshal(&storagepb.StorageError{
		Code:         storageErrorCode,
		Entity:       streamName,
		ErrorMessage: errMessage,
	})
	if err != nil {
		return err
	}
	return stream.Send(&storagepb.AppendRowsResponse{
		WriteStream: streamName,
		Response: &storagepb.AppendRowsResponse_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: errMessage,
				Details: []*anypb.Any{
					{
						TypeUrl: "type.googleapis.com/google.cloud.bigquery.storage.v1.StorageError",
						Value:   storageErrBy,
					},
				},
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
	streamStatus, err := s.getWriteStream(req.GetName())
	if err != nil {
		return nil, fmt.Errorf("failed to get write stream: %w", err)
	}
	return streamStatus.stream, nil
}

func (s *storageWriteServer) FinalizeWriteStream(ctx context.Context, req *storagepb.FinalizeWriteStreamRequest) (*storagepb.FinalizeWriteStreamResponse, error) {
	// TODO(dm): return an error when trying to finalize a default stream
	s.mu.RLock()
	defer s.mu.RUnlock()
	status, exists := s.streamMap[req.GetName()]
	if !exists {
		return nil, fmt.Errorf("failed to get stream from %s", req.GetName())
	}
	status.finalized = true
	return &storagepb.FinalizeWriteStreamResponse{
		RowCount: int64(len(status.rows)),
	}, nil
}

func (s *storageWriteServer) BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	var streamErrors []*storagepb.StorageError
	for _, streamName := range req.GetWriteStreams() {
		s.mu.RLock()
		status, exists := s.streamMap[streamName]
		s.mu.RUnlock()

		if !exists {
			streamErrors = append(streamErrors, &storagepb.StorageError{
				Code:         storagepb.StorageError_STREAM_NOT_FOUND,
				Entity:       streamName,
				ErrorMessage: fmt.Sprintf("failed to find stream from %s", streamName),
			})
			continue
		}
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
		if err := s.insertTableData(ctx, tx, status, status.rows); err != nil {
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
	s.mu.RLock()
	status, exists := s.streamMap[streamName]
	s.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("failed to find stream from %s", streamName)
	}
	offset := req.GetOffset().Value
	conn, err := s.server.connMgr.Connection(ctx, status.projectID, status.datasetID)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	if err := s.insertTableData(ctx, tx, status, status.rows[:offset+1]); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &storagepb.FlushRowsResponse{
		Offset: offset,
	}, nil
}

// getWriteStream accepts a fully qualified stream name and returns a writeStreamStatus reference
// the input stream name expected format is: projects/{{project}}/datasets/{{dataset}}/tables/{{table}}/streams/{{name}}
// if the stream can't be found and has the name of _default, it will be created on demand.
func (s *storageWriteServer) getWriteStream(streamName string) (*writeStreamStatus, error) {
	s.mu.RLock()
	streamStatus, _ := s.streamMap[streamName]
	s.mu.RUnlock()

	if streamStatus != nil {
		return streamStatus, nil
	}

	// create the default stream for the table
	if strings.HasSuffix(streamName, "_default") {
		nameParts := strings.Split(streamName, "/")
		if _, err := s.CreateWriteStream(context.Background(), &storagepb.CreateWriteStreamRequest{
			Parent: strings.Join(nameParts[:len(nameParts)-2], "/"),
			WriteStream: &storagepb.WriteStream{
				Name: streamName,
				Type: storagepb.WriteStream_COMMITTED,
			},
		}); err != nil {
			return nil, fmt.Errorf("failed to create default stream: %w", err)
		}
	}

	s.mu.RLock()
	streamStatus, _ = s.streamMap[streamName]
	if streamStatus == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("stream %s not found", streamName)
	}
	s.mu.RUnlock()
	return streamStatus, nil
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
	dataset := project.Dataset(datasetID)
	if dataset == nil {
		return nil, fmt.Errorf("dataset %s is not found in project %s", datasetID, projectID)
	}
	table := dataset.Table(tableID)
	if table == nil {
		return nil, fmt.Errorf("table %s is not found in dataset %s", tableID, datasetID)
	}
	return new(tablesGetHandler).Handle(ctx, &tablesGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
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
