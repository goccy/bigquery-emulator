package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/types"
	goavro "github.com/linkedin/goavro/v2"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	schemaText    string
}

func (s *storageReadServer) CreateReadSession(ctx context.Context, req *storagepb.CreateReadSessionRequest) (*storagepb.ReadSession, error) {
	sessionName := fmt.Sprintf("%s/locations/%s/sessions/%s", req.Parent, "location", "session")
	paths := strings.Split(req.ReadSession.Table, "/")
	if len(paths)%2 != 0 {
		return nil, fmt.Errorf("unexpected table path: %s", req.ReadSession.Table)
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
		return nil, fmt.Errorf("unspecified project id")
	}
	if datasetID == "" {
		return nil, fmt.Errorf("unspecified dataset id")
	}
	if tableID == "" {
		return nil, fmt.Errorf("unspecified table id")
	}
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
	tableMetadata, err := new(tablesGetHandler).Handle(ctx, &tablesGetRequest{
		server:  s.server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}
	streams := make([]*storagepb.ReadStream, 0, req.MaxStreamCount)
	streamName := fmt.Sprintf("%s/streams/stream", sessionName)
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
	var (
		avroSchema *types.AVROSchema
		schemaText string
	)
	switch readSession.DataFormat {
	case storagepb.DataFormat_AVRO:
		avroSchema = types.TableToAVRO(tableMetadata)
		if len(outputColumns) != 0 {
			outputColumnMap := map[string]struct{}{}
			for _, outputColumn := range outputColumns {
				outputColumnMap[outputColumn] = struct{}{}
			}
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
		schemaText = string(schema)
		readSession.Schema = &storagepb.ReadSession_AvroSchema{
			AvroSchema: &storagepb.AvroSchema{Schema: schemaText},
		}
	case storagepb.DataFormat_ARROW:
		readSession.Schema = &storagepb.ReadSession_ArrowSchema{
			ArrowSchema: &storagepb.ArrowSchema{SerializedSchema: nil},
		}
	default:
		return nil, fmt.Errorf("unexpected data format %s", readSession.DataFormat)
	}
	s.mu.Lock()
	s.streamMap[streamName] = &streamStatus{
		projectID:     projectID,
		datasetID:     datasetID,
		tableID:       tableID,
		outputColumns: outputColumns,
		condition:     condition,
		dataFormat:    readSession.DataFormat,
		avroSchema:    avroSchema,
		schemaText:    schemaText,
	}
	s.mu.Unlock()
	return readSession, nil
}

func (s *storageReadServer) ReadRows(req *storagepb.ReadRowsRequest, stream storagepb.BigQueryRead_ReadRowsServer) error {
	s.mu.RLock()
	streamStatus := s.streamMap[req.ReadStream]
	s.mu.RUnlock()

	if streamStatus == nil {
		return fmt.Errorf("failed to find stream status from %s", req.ReadStream)
	}
	var columns string
	if len(streamStatus.outputColumns) != 0 {
		outputColumns := make([]string, len(streamStatus.outputColumns))
		for idx, outputColumn := range streamStatus.outputColumns {
			outputColumns[idx] = fmt.Sprintf("`%s`", outputColumn)
		}
		columns = strings.Join(outputColumns, ",")
	} else {
		columns = "*"
	}
	var condition string
	if streamStatus.condition != "" {
		condition = fmt.Sprintf("WHERE %s", streamStatus.condition)
	}
	query := fmt.Sprintf("SELECT %s FROM `%s` %s", columns, streamStatus.tableID, condition)

	ctx := context.Background()
	ctx = logger.WithLogger(ctx, s.server.logger)

	conn, err := s.server.connMgr.Connection(ctx, streamStatus.projectID, streamStatus.datasetID)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()

	response, err := s.server.contentRepo.Query(
		ctx,
		tx,
		streamStatus.projectID,
		streamStatus.datasetID,
		query,
		nil,
	)
	if err != nil {
		return err
	}
	switch streamStatus.dataFormat {
	case storagepb.DataFormat_AVRO:
		codec, err := goavro.NewCodec(streamStatus.schemaText)
		if err != nil {
			return err
		}
		var buf []byte
		for _, row := range response.Rows {
			value, err := row.AVROValue(streamStatus.avroSchema.Fields)
			if err != nil {
				return err
			}
			b, err := codec.BinaryFromNative(buf, value)
			if err != nil {
				return err
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
					Schema: streamStatus.schemaText,
				},
			},
		}); err != nil {
			return err
		}
	case storagepb.DataFormat_ARROW:

	}
	return nil
}

func (s *storageReadServer) SplitReadStream(ctx context.Context, req *storagepb.SplitReadStreamRequest) (*storagepb.SplitReadStreamResponse, error) {
	return nil, fmt.Errorf("failed to split read stream !!!!")
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
