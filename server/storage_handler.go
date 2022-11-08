package server

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/goccy/go-json"
	goavro "github.com/linkedin/goavro/v2"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goccy/bigquery-emulator/internal/logger"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
)

var _ storagepb.BigQueryReadServer = &storageReadServer{}

type storageReadServer struct {
	server    *Server
	streamMap map[string]*streamStatus
	mu        sync.RWMutex
}

type streamStatus struct {
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
	projectID, datasetID, tableID, err := s.getIDsFromPath(req.ReadSession.Table)
	if err != nil {
		return nil, err
	}
	tableMetadata, err := s.getTableMetadata(ctx, projectID, datasetID, tableID)
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
	status := &streamStatus{
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

func (s *storageReadServer) getIDsFromPath(path string) (string, string, string, error) {
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

func (s *storageReadServer) getTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigqueryv2.Table, error) {
	project, err := s.server.metaRepo.FindProject(ctx, projectID)
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
		server:  s.server,
		project: project,
		dataset: dataset,
		table:   table,
	})
}

func (s *storageReadServer) buildQuery(status *streamStatus) string {
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

func (s *storageReadServer) query(ctx context.Context, status *streamStatus) (*internaltypes.QueryResponse, error) {
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

func (s *storageReadServer) sendAVRORows(status *streamStatus, response *internaltypes.QueryResponse, stream storagepb.BigQueryRead_ReadRowsServer) error {
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

func (s *storageReadServer) sendARROWRows(status *streamStatus, response *internaltypes.QueryResponse, stream storagepb.BigQueryRead_ReadRowsServer) error {
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

func registerStorageServer(grpcServer *grpc.Server, srv *Server) {
	storagepb.RegisterBigQueryReadServer(
		grpcServer,
		&storageReadServer{
			server:    srv,
			streamMap: map[string]*streamStatus{},
		},
	)
}
