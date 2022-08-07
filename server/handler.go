package server

import (
	"context"
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
)

func writeBadRequest(ctx context.Context, w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	logger.Logger(ctx).Error("bad request", zap.Error(err))
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintln(w, err.Error())
}

func writeInternalError(ctx context.Context, w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	logger.Logger(ctx).Error("internal error", zap.Error(err))
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintln(w, err.Error())
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, response interface{}) {
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Logger(ctx).Error("failed to encode json", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err.Error())
	}
}

const (
	discoveryAPIEndpoint = "/discovery/v1/apis/bigquery/v2/rest"
	uploadAPIEndpoint    = "/upload/bigquery/v2/projects/{projectId}/jobs"
)

//go:embed resources/discovery.json
var bigqueryAPIJSON []byte

var (
	discoveryAPIOnce     sync.Once
	discoveryAPIResponse map[string]interface{}
)

type discoveryHandler struct {
	server *Server
}

func newDiscoveryHandler(server *Server) *discoveryHandler {
	return &discoveryHandler{server: server}
}

func (h *discoveryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var decodeJSONErr error
	discoveryAPIOnce.Do(func() {
		if err := json.Unmarshal(bigqueryAPIJSON, &discoveryAPIResponse); err != nil {
			decodeJSONErr = err
			return
		}
		addr := h.server.httpServer.Addr
		if !strings.HasPrefix(addr, "http") {
			addr = "http://" + addr
		}
		discoveryAPIResponse["mtlsRootUrl"] = addr
		discoveryAPIResponse["rootUrl"] = addr
		discoveryAPIResponse["baseUrl"] = addr
	})
	if decodeJSONErr != nil {
		writeInternalError(ctx, w, decodeJSONErr)
		return
	}
	encodeResponse(ctx, w, discoveryAPIResponse)
}

type uploadHandler struct {
}

type UploadJobConfigurationLoad struct {
	AllowJaggedRows                    bool                                   `json:"allowJaggedRows,omitempty"`
	AllowQuotedNewlines                bool                                   `json:"allowQuotedNewlines,omitempty"`
	Autodetect                         bool                                   `json:"autodetect,omitempty"`
	Clustering                         *bigqueryv2.Clustering                 `json:"clustering,omitempty"`
	CreateDisposition                  string                                 `json:"createDisposition,omitempty"`
	DecimalTargetTypes                 []string                               `json:"decimalTargetTypes,omitempty"`
	DestinationEncryptionConfiguration *bigqueryv2.EncryptionConfiguration    `json:"destinationEncryptionConfiguration,omitempty"`
	DestinationTable                   *bigqueryv2.TableReference             `json:"destinationTable,omitempty"`
	DestinationTableProperties         *bigqueryv2.DestinationTableProperties `json:"destinationTableProperties,omitempty"`
	Encoding                           string                                 `json:"encoding,omitempty"`
	FieldDelimiter                     string                                 `json:"fieldDelimiter,omitempty"`
	HivePartitioningOptions            *bigqueryv2.HivePartitioningOptions    `json:"hivePartitioningOptions,omitempty"`
	IgnoreUnknownValues                bool                                   `json:"ignoreUnknownValues,omitempty"`
	JsonExtension                      string                                 `json:"jsonExtension,omitempty"`
	MaxBadRecords                      int64                                  `json:"maxBadRecords,omitempty"`
	NullMarker                         string                                 `json:"nullMarker,omitempty"`
	ParquetOptions                     *bigqueryv2.ParquetOptions             `json:"parquetOptions,omitempty"`
	PreserveAsciiControlCharacters     bool                                   `json:"preserveAsciiControlCharacters,omitempty"`
	ProjectionFields                   []string                               `json:"projectionFields,omitempty"`
	Quote                              *string                                `json:"quote,omitempty"`
	RangePartitioning                  *bigqueryv2.RangePartitioning          `json:"rangePartitioning,omitempty"`
	Schema                             *bigqueryv2.TableSchema                `json:"schema,omitempty"`
	SchemaInline                       string                                 `json:"schemaInline,omitempty"`
	SchemaInlineFormat                 string                                 `json:"schemaInlineFormat,omitempty"`
	SchemaUpdateOptions                []string                               `json:"schemaUpdateOptions,omitempty"`
	SkipLeadingRows                    json.Number                            `json:"skipLeadingRows,omitempty"`
	SourceFormat                       string                                 `json:"sourceFormat,omitempty"`
	SourceUris                         []string                               `json:"sourceUris,omitempty"`
	TimePartitioning                   *bigqueryv2.TimePartitioning           `json:"timePartitioning,omitempty"`
	UseAvroLogicalTypes                bool                                   `json:"useAvroLogicalTypes,omitempty"`
	WriteDisposition                   string                                 `json:"writeDisposition,omitempty"`
}

type UploadJobConfiguration struct {
	Load *UploadJobConfigurationLoad `json:"load"`
}

type UploadJob struct {
	JobReference  *bigqueryv2.JobReference `json:"jobReference"`
	Configuration *UploadJobConfiguration  `json:"configuration"`
}

func (j *UploadJob) ToJob() *bigqueryv2.Job {
	load := j.Configuration.Load
	skipLeadingRows, _ := load.SkipLeadingRows.Int64()
	return &bigqueryv2.Job{
		JobReference: j.JobReference,
		Configuration: &bigqueryv2.JobConfiguration{
			Load: &bigqueryv2.JobConfigurationLoad{
				AllowJaggedRows:                    load.AllowJaggedRows,
				AllowQuotedNewlines:                load.AllowQuotedNewlines,
				Autodetect:                         load.Autodetect,
				Clustering:                         load.Clustering,
				CreateDisposition:                  load.CreateDisposition,
				DecimalTargetTypes:                 load.DecimalTargetTypes,
				DestinationEncryptionConfiguration: load.DestinationEncryptionConfiguration,
				DestinationTable:                   load.DestinationTable,
				DestinationTableProperties:         load.DestinationTableProperties,
				Encoding:                           load.Encoding,
				FieldDelimiter:                     load.FieldDelimiter,
				HivePartitioningOptions:            load.HivePartitioningOptions,
				IgnoreUnknownValues:                load.IgnoreUnknownValues,
				JsonExtension:                      load.JsonExtension,
				MaxBadRecords:                      load.MaxBadRecords,
				NullMarker:                         load.NullMarker,
				ParquetOptions:                     load.ParquetOptions,
				PreserveAsciiControlCharacters:     load.PreserveAsciiControlCharacters,
				ProjectionFields:                   load.ProjectionFields,
				Quote:                              load.Quote,
				RangePartitioning:                  load.RangePartitioning,
				Schema:                             load.Schema,
				SchemaInline:                       load.SchemaInline,
				SchemaInlineFormat:                 load.SchemaInlineFormat,
				SchemaUpdateOptions:                load.SchemaUpdateOptions,
				SkipLeadingRows:                    skipLeadingRows,
				SourceFormat:                       load.SourceFormat,
				SourceUris:                         load.SourceUris,
				TimePartitioning:                   load.TimePartitioning,
				UseAvroLogicalTypes:                load.UseAvroLogicalTypes,
				WriteDisposition:                   load.WriteDisposition,
			},
		},
	}
}

func (h *uploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	var job UploadJob
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		writeBadRequest(ctx, w, err)
		return
	}
	res, err := h.Handle(ctx, &uploadRequest{
		server:  server,
		project: project,
		job:     &job,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	addr := server.httpServer.Addr
	if !strings.HasPrefix(addr, "http") {
		addr = "http://" + addr
	}
	addr = strings.TrimRight(addr, "/")
	w.Header().Add(
		"Location",
		fmt.Sprintf(
			"%s/upload/bigquery/v2/projects/%s/jobs?uploadType=resumable&upload_id=%s",
			addr,
			project.ID,
			job.JobReference.JobId,
		),
	)
	encodeResponse(ctx, w, res)
}

type uploadRequest struct {
	server  *Server
	project *metadata.Project
	job     *UploadJob
}

func (h *uploadHandler) Handle(ctx context.Context, r *uploadRequest) (*bigqueryv2.Job, error) {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	job := metadata.NewJob(r.server.metaRepo, r.job.JobReference.JobId, r.job.ToJob(), nil, nil)
	if err := r.project.AddJob(ctx, tx.Tx(), job); err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit job: %w", err)
	}
	return job.Content(), nil
}

type uploadContentHandler struct{}

func (h *uploadContentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	query := r.URL.Query()
	uploadType := query["uploadType"]
	if len(uploadType) == 0 {
		writeBadRequest(ctx, w, fmt.Errorf("uploadType parameter is not found"))
		return
	}
	if uploadType[0] != "resumable" {
		writeBadRequest(ctx, w, fmt.Errorf("uploadType parameter is not resumable %s", uploadType[0]))
		return
	}
	uploadID := query["upload_id"]
	if len(uploadID) == 0 {
		writeBadRequest(ctx, w, fmt.Errorf("upload_id parameter is not found"))
		return
	}
	jobID := uploadID[0]
	job := project.Job(jobID)
	if err := h.Handle(ctx, &uploadContentRequest{
		server:  server,
		project: project,
		job:     job,
		reader:  r.Body,
	}); err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	content := job.Content()
	content.Status = &bigqueryv2.JobStatus{State: "DONE"}
	encodeResponse(ctx, w, content)
}

type uploadContentRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
	reader  io.Reader
}

func (h *uploadContentHandler) getCandidateName(col string, columnNames []string) string {
	var (
		foundName  string
		foundCount int
	)
	for _, name := range columnNames {
		if strings.Contains(name, col) {
			foundName = name
			foundCount++
		}
	}
	if foundCount == 1 {
		return foundName
	}
	return ""
}

func (h *uploadContentHandler) existsColumnNameInCSVHeader(col string, header []string) bool {
	for _, h := range header {
		if col == h {
			return true
		}
	}
	return false
}

func (h *uploadContentHandler) Handle(ctx context.Context, r *uploadContentRequest) error {
	load := r.job.Content().Configuration.Load
	sourceFormat := load.SourceFormat
	switch sourceFormat {
	case "CSV":
		records, err := csv.NewReader(r.reader).ReadAll()
		if err != nil {
			return fmt.Errorf("failed to read csv: %w", err)
		}
		if len(records) == 0 {
			return fmt.Errorf("failed to find csv header")
		}
		if len(records) == 1 {
			return nil
		}
		tableRef := load.DestinationTable
		dataset := r.project.Dataset(tableRef.DatasetId)
		table := dataset.Table(tableRef.TableId)
		tableContent, err := table.Content()
		if err != nil {
			return err
		}
		columnToType := map[string]types.Type{}
		for _, field := range tableContent.Schema.Fields {
			columnToType[field.Name] = types.Type(field.Type)
		}
		header := records[0]
		columns := []*types.Column{}
		var ignoreHeader bool
		for _, col := range header {
			if _, exists := columnToType[col]; !exists {
				ignoreHeader = true
				break
			}
			columns = append(columns, &types.Column{
				Name: col,
				Type: columnToType[col],
			})
		}
		if ignoreHeader {
			columns = []*types.Column{}
			for _, field := range tableContent.Schema.Fields {
				columns = append(columns, &types.Column{
					Name: field.Name,
					Type: types.Type(field.Type),
				})
			}
		}
		data := types.Data{}
		for _, record := range records[1:] {
			rowData := map[string]interface{}{}
			if len(record) != len(columns) {
				return fmt.Errorf("invalid column number: found broken row data: %v", record)
			}
			for i := 0; i < len(record); i++ {
				colData := record[i]
				if colData == "" {
					rowData[columns[i].Name] = nil
				} else {
					rowData[columns[i].Name] = colData
				}
			}
			data = append(data, rowData)
		}
		tableDef := &types.Table{
			ID:      tableRef.TableId,
			Columns: columns,
			Data:    data,
		}
		conn, err := r.server.connMgr.Connection(ctx, tableRef.ProjectId, tableRef.DatasetId)
		if err != nil {
			return fmt.Errorf("failed to get connection: %w", err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.RollbackIfNotCommitted()
		if err := r.server.contentRepo.AddTableData(ctx, tx, tableRef.ProjectId, tableRef.DatasetId, tableDef); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (h *datasetsDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	params := mux.Vars(r)
	deleteContents, _ := strconv.ParseBool(params["deleteContents"])
	if err := h.Handle(ctx, &datasetsDeleteRequest{
		server:         server,
		project:        project,
		dataset:        dataset,
		deleteContents: deleteContents,
	}); err != nil {
		writeInternalError(ctx, w, err)
		return
	}
}

type datasetsDeleteRequest struct {
	server         *Server
	project        *metadata.Project
	dataset        *metadata.Dataset
	deleteContents bool
}

func (h *datasetsDeleteHandler) Handle(ctx context.Context, r *datasetsDeleteRequest) error {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.project.DeleteDataset(ctx, tx.Tx(), r.dataset.ID); err != nil {
		return fmt.Errorf("failed to delete dataset: %w", err)
	}
	if r.deleteContents {
		if err := r.server.contentRepo.DeleteTables(ctx, tx, r.project.ID, r.dataset.ID, r.dataset.TableIDs()); err != nil {
			return fmt.Errorf("failed to delete tables: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit delete dataset: %w", err)
	}
	return nil
}

func (h *datasetsGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &datasetsGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *datasetsGetHandler) Handle(ctx context.Context, r *datasetsGetRequest) (*bigqueryv2.Dataset, error) {
	newContent := *r.dataset.Content()
	newContent.DatasetReference = &bigqueryv2.DatasetReference{
		ProjectId: r.project.ID,
		DatasetId: r.dataset.ID,
	}
	return &newContent, nil
}

func (h *datasetsInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	var dataset bigqueryv2.Dataset
	if err := json.NewDecoder(r.Body).Decode(&dataset); err != nil {
		writeBadRequest(ctx, w, err)
		return
	}
	res, err := h.Handle(ctx, &datasetsInsertRequest{
		server:  server,
		project: project,
		dataset: &dataset,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsInsertRequest struct {
	server  *Server
	project *metadata.Project
	dataset *bigqueryv2.Dataset
}

func (h *datasetsInsertHandler) Handle(ctx context.Context, r *datasetsInsertRequest) (*bigqueryv2.DatasetListDatasets, error) {
	if r.dataset.DatasetReference == nil {
		return nil, fmt.Errorf("DatasetReference is nil")
	}
	datasetID := r.dataset.DatasetReference.DatasetId
	if datasetID == "" {
		return nil, fmt.Errorf("dataset id is empty")
	}
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()

	if err := r.project.AddDataset(
		ctx,
		tx.Tx(),
		metadata.NewDataset(
			r.server.metaRepo,
			datasetID,
			r.dataset,
			nil,
			nil,
			nil,
		),
	); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &bigqueryv2.DatasetListDatasets{
		DatasetReference: &bigqueryv2.DatasetReference{
			ProjectId: r.project.ID,
			DatasetId: datasetID,
		},
		Id:              datasetID,
		FriendlyName:    r.dataset.FriendlyName,
		Kind:            r.dataset.Kind,
		Labels:          r.dataset.Labels,
		Location:        r.dataset.Location,
		ForceSendFields: r.dataset.ForceSendFields,
		NullFields:      r.dataset.NullFields,
	}, nil
}

func (h *datasetsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	res, err := h.Handle(ctx, &datasetsListRequest{
		server:  server,
		project: project,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsListRequest struct {
	server  *Server
	project *metadata.Project
}

func (h *datasetsListHandler) Handle(ctx context.Context, r *datasetsListRequest) (*bigqueryv2.DatasetList, error) {
	datasetsRes := []*bigqueryv2.DatasetListDatasets{}
	for _, dataset := range r.project.Datasets() {
		content := dataset.Content()
		datasetsRes = append(datasetsRes, &bigqueryv2.DatasetListDatasets{
			DatasetReference: &bigqueryv2.DatasetReference{
				ProjectId: r.project.ID,
				DatasetId: dataset.ID,
			},
			FriendlyName:    content.FriendlyName,
			Id:              dataset.ID,
			Kind:            content.Kind,
			Labels:          content.Labels,
			Location:        content.Location,
			ForceSendFields: content.ForceSendFields,
			NullFields:      content.NullFields,
		})
	}
	return &bigqueryv2.DatasetList{
		Datasets: datasetsRes,
	}, nil
}

func (h *datasetsPatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var newDataset bigqueryv2.Dataset
	if err := json.NewDecoder(r.Body).Decode(&newDataset); err != nil {
		writeBadRequest(ctx, w, err)
		return
	}
	res, err := h.Handle(ctx, &datasetsPatchRequest{
		server:     server,
		project:    project,
		dataset:    dataset,
		newDataset: &newDataset,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsPatchRequest struct {
	server     *Server
	project    *metadata.Project
	dataset    *metadata.Dataset
	newDataset *bigqueryv2.Dataset
}

func (h *datasetsPatchHandler) Handle(ctx context.Context, r *datasetsPatchRequest) (*bigqueryv2.Dataset, error) {
	r.dataset.UpdateContentIfExists(r.newDataset)
	newContent := *r.dataset.Content()
	return &newContent, nil
}

func (h *datasetsUpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var newDataset bigqueryv2.Dataset
	if err := json.NewDecoder(r.Body).Decode(&newDataset); err != nil {
		writeBadRequest(ctx, w, err)
		return
	}
	res, err := h.Handle(ctx, &datasetsUpdateRequest{
		server:     server,
		project:    project,
		dataset:    dataset,
		newDataset: &newDataset,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsUpdateRequest struct {
	server     *Server
	project    *metadata.Project
	dataset    *metadata.Dataset
	newDataset *bigqueryv2.Dataset
}

func (h *datasetsUpdateHandler) Handle(ctx context.Context, r *datasetsUpdateRequest) (*bigqueryv2.Dataset, error) {
	r.dataset.UpdateContent(r.newDataset)
	newContent := *r.dataset.Content()
	return &newContent, nil
}

func (h *jobsCancelHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	res, err := h.Handle(ctx, &jobsCancelRequest{
		server:  server,
		project: project,
		job:     job,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsCancelRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
}

func (h *jobsCancelHandler) Handle(ctx context.Context, r *jobsCancelRequest) (*bigqueryv2.JobCancelResponse, error) {
	if err := r.job.Cancel(ctx); err != nil {
		return nil, err
	}
	return &bigqueryv2.JobCancelResponse{Job: r.job.Content()}, nil
}

func (h *jobsDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	if err := h.Handle(ctx, &jobsDeleteRequest{
		server:  server,
		project: project,
		job:     job,
	}); err != nil {
		writeInternalError(ctx, w, err)
		return
	}
}

type jobsDeleteRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
}

func (h *jobsDeleteHandler) Handle(ctx context.Context, r *jobsDeleteRequest) error {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, "")
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.project.DeleteJob(ctx, tx.Tx(), r.job.ID); err != nil {
		return fmt.Errorf("failed to delete job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (h *jobsGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	res, err := h.Handle(ctx, &jobsGetRequest{
		server:  server,
		project: project,
		job:     job,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsGetRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
}

func (h *jobsGetHandler) Handle(ctx context.Context, r *jobsGetRequest) (*bigqueryv2.Job, error) {
	content := *r.job.Content()
	content.Status = &bigqueryv2.JobStatus{State: "DONE"}
	return &content, nil
}

func (h *jobsGetQueryResultsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	res, err := h.Handle(ctx, &jobsGetQueryResultsRequest{
		server:  server,
		project: project,
		job:     job,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsGetQueryResultsRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
}

func (h *jobsGetQueryResultsHandler) Handle(ctx context.Context, r *jobsGetQueryResultsRequest) (*internaltypes.GetQueryResultsResponse, error) {
	response, err := r.job.Wait(ctx)
	if err != nil {
		return nil, err
	}
	return &internaltypes.GetQueryResultsResponse{
		JobReference: &bigqueryv2.JobReference{
			ProjectId: r.project.ID,
			JobId:     r.job.ID,
		},
		Schema:      response.Schema,
		TotalRows:   response.TotalRows,
		JobComplete: true,
		Rows:        response.Rows,
	}, nil
}

func (h *jobsInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	var job bigqueryv2.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		writeBadRequest(ctx, w, err)
		return
	}
	res, err := h.Handle(ctx, &jobsInsertRequest{
		server:  server,
		project: project,
		job:     &job,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsInsertRequest struct {
	server  *Server
	project *metadata.Project
	job     *bigqueryv2.Job
}

func (h *jobsInsertHandler) Handle(ctx context.Context, r *jobsInsertRequest) (*bigqueryv2.Job, error) {
	if r.job.Configuration == nil {
		return nil, fmt.Errorf("unspecified job configuration")
	}
	if r.job.Configuration.Query == nil {
		return nil, fmt.Errorf("unspecified job configuration query")
	}
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	job := metadata.NewJob(r.server.metaRepo, r.job.JobReference.JobId, r.job, nil, nil)
	if err := r.project.AddJob(ctx, tx.Tx(), job); err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}

	hasDestinationTable := r.job.Configuration.Query.DestinationTable != nil
	response, jobErr := r.server.contentRepo.Query(
		ctx,
		tx,
		r.project.ID,
		"",
		job.Query(),
		job.QueryParameters(),
	)
	if err := job.SetResult(ctx, tx.Tx(), response, jobErr); err != nil {
		return nil, fmt.Errorf("failed to set job result: %w", err)
	}

	if hasDestinationTable && jobErr == nil {
		// insert results to destination table
		tableRef := r.job.Configuration.Query.DestinationTable
		columns := []*types.Column{}
		for _, field := range response.Schema.Fields {
			columns = append(columns, &types.Column{
				Name: field.Name,
				Type: types.Type(field.Type),
			})
		}
		data := types.Data{}
		for _, row := range response.Rows {
			rowData := map[string]interface{}{}
			for idx, cell := range row.F {
				if cell.V == nil {
					rowData[columns[idx].Name] = nil
				} else {
					rowData[columns[idx].Name] = cell.V
				}
			}
			data = append(data, rowData)
		}
		tableDef := &types.Table{
			ID:      tableRef.TableId,
			Columns: columns,
			Data:    data,
		}
		if err := r.server.contentRepo.AddTableData(ctx, tx, tableRef.ProjectId, tableRef.DatasetId, tableDef); err != nil {
			return nil, fmt.Errorf("failed to add table data: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit job: %w", err)
	}
	content := *job.Content()
	if !hasDestinationTable {
		content.Configuration.Query.DestinationTable = &bigqueryv2.TableReference{
			ProjectId: r.project.ID,
			DatasetId: "anonymous",
			TableId:   "anonymous",
		}
	}
	content.Status = &bigqueryv2.JobStatus{
		State: "DONE",
	}
	content.Statistics = &bigqueryv2.JobStatistics{
		Query: &bigqueryv2.JobStatistics2{
			StatementType: "SELECT",
		},
	}
	return &content, nil
}

func (h *jobsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	res, err := h.Handle(ctx, &jobsListRequest{
		server:  server,
		project: project,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsListRequest struct {
	server  *Server
	project *metadata.Project
}

func (h *jobsListHandler) Handle(ctx context.Context, r *jobsListRequest) (*bigqueryv2.JobList, error) {
	jobs := []*bigqueryv2.JobListJobs{}
	for _, job := range r.project.Jobs() {
		content := job.Content()
		jobs = append(jobs, &bigqueryv2.JobListJobs{
			Id:           content.Id,
			JobReference: content.JobReference,
			Kind:         content.Kind,
			Statistics:   content.Statistics,
			Status:       content.Status,
			UserEmail:    content.UserEmail,
		})
	}
	return &bigqueryv2.JobList{Jobs: jobs}, nil
}

func (h *jobsQueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	var req bigqueryv2.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(ctx, w, err)
		return
	}
	res, err := h.Handle(ctx, &jobsQueryRequest{
		server:       server,
		project:      project,
		queryRequest: &req,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsQueryRequest struct {
	server       *Server
	project      *metadata.Project
	queryRequest *bigqueryv2.QueryRequest
}

func (h *jobsQueryHandler) Handle(ctx context.Context, r *jobsQueryRequest) (*internaltypes.QueryResponse, error) {
	var datasetID string
	if r.queryRequest.DefaultDataset != nil {
		datasetID = r.queryRequest.DefaultDataset.DatasetId
	}
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, datasetID)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	response, err := r.server.contentRepo.Query(
		ctx,
		tx,
		r.project.ID,
		datasetID,
		r.queryRequest.Query,
		r.queryRequest.QueryParameters,
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	response.JobReference = &bigqueryv2.JobReference{
		ProjectId: r.project.ID,
		JobId:     r.queryRequest.RequestId,
	}
	return response, nil
}

func (h *modelsDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	model := modelFromContext(ctx)
	if err := h.Handle(ctx, &modelsDeleteRequest{
		server:  server,
		project: project,
		dataset: dataset,
		model:   model,
	}); err != nil {
		writeInternalError(ctx, w, err)
	}
}

type modelsDeleteRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	model   *metadata.Model
}

func (h *modelsDeleteHandler) Handle(ctx context.Context, r *modelsDeleteRequest) error {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.dataset.DeleteModel(ctx, tx.Tx(), r.model.ID); err != nil {
		return fmt.Errorf("failed to delete model: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (h *modelsGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	model := modelFromContext(ctx)
	res, err := h.Handle(ctx, &modelsGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
		model:   model,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type modelsGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	model   *metadata.Model
}

func (h *modelsGetHandler) Handle(ctx context.Context, r *modelsGetRequest) (*bigqueryv2.Model, error) {
	return &bigqueryv2.Model{
		BestTrialId:             0,
		CreationTime:            0,
		DefaultTrialId:          0,
		Description:             "",
		EncryptionConfiguration: nil,
		Etag:                    "",
		ExpirationTime:          0,
		FeatureColumns:          nil,
		FriendlyName:            "",
		HparamSearchSpaces:      nil,
		HparamTrials:            nil,
		LabelColumns:            nil,
		Labels:                  nil,
		LastModifiedTime:        0,
		Location:                "",
		ModelReference:          nil,
		ModelType:               "",
		OptimalTrialIds:         nil,
		TrainingRuns:            nil,
	}, nil
}

func (h *modelsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &modelsListRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)

}

type modelsListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *modelsListHandler) Handle(ctx context.Context, r *modelsListRequest) (*bigqueryv2.ListModelsResponse, error) {
	models := []*bigqueryv2.Model{}
	for _, m := range r.dataset.Models() {
		_ = m
		models = append(models, &bigqueryv2.Model{})
	}
	return &bigqueryv2.ListModelsResponse{
		Models: models,
	}, nil
}

func (h *modelsPatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	model := modelFromContext(ctx)
	res, err := h.Handle(ctx, &modelsPatchRequest{
		server:  server,
		project: project,
		dataset: dataset,
		model:   model,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type modelsPatchRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	model   *metadata.Model
}

func (h *modelsPatchHandler) Handle(ctx context.Context, r *modelsPatchRequest) (*bigqueryv2.Model, error) {
	return &bigqueryv2.Model{}, nil
}

func (h *projectsGetServiceAccountHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	res, err := h.Handle(ctx, &projectsGetServiceAccountRequest{
		server:  server,
		project: project,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type projectsGetServiceAccountRequest struct {
	server  *Server
	project *metadata.Project
}

func (h *projectsGetServiceAccountHandler) Handle(ctx context.Context, r *projectsGetServiceAccountRequest) (*bigqueryv2.GetServiceAccountResponse, error) {
	return &bigqueryv2.GetServiceAccountResponse{}, nil
}

func (h *projectsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &projectsListRequest{
		server: server,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type projectsListRequest struct {
	server *Server
}

func (h *projectsListHandler) Handle(ctx context.Context, r *projectsListRequest) (*bigqueryv2.ProjectList, error) {
	projects, err := r.server.metaRepo.FindAllProjects(ctx)
	if err != nil {
		return nil, err
	}

	projectList := []*bigqueryv2.ProjectListProjects{}
	for _, p := range projects {
		projectList = append(projectList, &bigqueryv2.ProjectListProjects{
			Id: p.ID,
		})
	}
	return &bigqueryv2.ProjectList{
		Projects: projectList,
	}, nil
}

func (h *routinesDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	routine := routineFromContext(ctx)
	if err := h.Handle(ctx, &routinesDeleteRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: routine,
	}); err != nil {
		writeInternalError(ctx, w, err)
		return
	}
}

type routinesDeleteRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *metadata.Routine
}

func (h *routinesDeleteHandler) Handle(ctx context.Context, r *routinesDeleteRequest) error {
	return fmt.Errorf("unsupported bigquery.routines.delete")
}

func (h *routinesGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	routine := routineFromContext(ctx)
	res, err := h.Handle(ctx, &routinesGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: routine,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *metadata.Routine
}

func (h *routinesGetHandler) Handle(ctx context.Context, r *routinesGetRequest) (*bigqueryv2.Routine, error) {
	return nil, fmt.Errorf("unsupported bigquery.routines.get")
}

func (h *routinesInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var routine bigqueryv2.Routine
	if err := json.NewDecoder(r.Body).Decode(&routine); err != nil {
		writeBadRequest(ctx, w, err)
		return
	}
	res, err := h.Handle(ctx, &routinesInsertRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: &routine,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesInsertRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *bigqueryv2.Routine
}

func (h *routinesInsertHandler) Handle(ctx context.Context, r *routinesInsertRequest) (*bigqueryv2.Routine, error) {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.server.contentRepo.AddRoutineByMetaData(ctx, tx, r.routine); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return r.routine, nil
}

func (h *routinesListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &routinesListRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *routinesListHandler) Handle(ctx context.Context, r *routinesListRequest) (*bigqueryv2.ListRoutinesResponse, error) {
	var routineList []*bigqueryv2.Routine
	for _, routine := range r.dataset.Routines() {
		_ = routine
		routineList = append(routineList, &bigqueryv2.Routine{})
	}
	return &bigqueryv2.ListRoutinesResponse{
		Routines: routineList,
	}, fmt.Errorf("unsupported bigquery.routines.list")
}

func (h *routinesUpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	routine := routineFromContext(ctx)
	res, err := h.Handle(ctx, &routinesUpdateRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: routine,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesUpdateRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *metadata.Routine
}

func (h *routinesUpdateHandler) Handle(ctx context.Context, r *routinesUpdateRequest) (*bigqueryv2.Routine, error) {
	return nil, fmt.Errorf("unsupported bigquery.routines.update")
}

func (h *rowAccessPoliciesGetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesGetIamPolicyRequest{
		server: server,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesGetIamPolicyRequest struct {
	server *Server
}

func (h *rowAccessPoliciesGetIamPolicyHandler) Handle(ctx context.Context, r *rowAccessPoliciesGetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.getIamPolicy")
}

func (h *rowAccessPoliciesListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesListRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *rowAccessPoliciesListHandler) Handle(ctx context.Context, r *rowAccessPoliciesListRequest) (*bigqueryv2.ListRowAccessPoliciesResponse, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.list")
}

func (h *rowAccessPoliciesSetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesSetIamPolicyRequest{
		server: server,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesSetIamPolicyRequest struct {
	server *Server
}

func (h *rowAccessPoliciesSetIamPolicyHandler) Handle(ctx context.Context, r *rowAccessPoliciesSetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.setIamPolicy")
}

func (h *rowAccessPoliciesTestIamPermissionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesTestIamPermissionsRequest{
		server: server,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesTestIamPermissionsRequest struct {
	server *Server
}

func (h *rowAccessPoliciesTestIamPermissionsHandler) Handle(ctx context.Context, r *rowAccessPoliciesTestIamPermissionsRequest) (*bigqueryv2.TestIamPermissionsResponse, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.testIamPermissions")
}

func (h *tabledataInsertAllHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	var req bigqueryv2.TableDataInsertAllRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(ctx, w, err)
		return
	}
	res, err := h.Handle(ctx, &tabledataInsertAllRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
		req:     &req,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tabledataInsertAllRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
	req     *bigqueryv2.TableDataInsertAllRequest
}

func (h *tabledataInsertAllHandler) Handle(ctx context.Context, r *tabledataInsertAllRequest) (*bigqueryv2.TableDataInsertAllResponse, error) {
	return nil, fmt.Errorf("unsupported bigquery.tabledata.insertAll")
}

func (h *tabledataListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &tabledataListRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tabledataListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *tabledataListHandler) Handle(ctx context.Context, r *tabledataListRequest) (*bigqueryv2.TableDataList, error) {
	return nil, fmt.Errorf("unsupported bigquery.tabledata.list")
}

func (h *tablesDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	if err := h.Handle(ctx, &tablesDeleteRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	}); err != nil {
		writeInternalError(ctx, w, err)
	}
}

type tablesDeleteRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *tablesDeleteHandler) Handle(ctx context.Context, r *tablesDeleteRequest) error {
	return fmt.Errorf("unsupported bigquery.tables.delete")
}

func (h *tablesGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &tablesGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *tablesGetHandler) Handle(ctx context.Context, r *tablesGetRequest) (*bigqueryv2.Table, error) {
	table, err := r.table.Content()
	if err != nil {
		return nil, fmt.Errorf("failed to get table content: %w", err)
	}
	return table, nil
}

func (h *tablesGetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	var req bigqueryv2.GetIamPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(ctx, w, err)
		return
	}
	res, err := h.Handle(ctx, &tablesGetIamPolicyRequest{
		server: server,
		req:    &req,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesGetIamPolicyRequest struct {
	server *Server
	req    *bigqueryv2.GetIamPolicyRequest
}

func (h *tablesGetIamPolicyHandler) Handle(ctx context.Context, r *tablesGetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("bigquery.tables.getIamPolicy")
}

func (h *tablesInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var table bigqueryv2.Table
	if err := json.NewDecoder(r.Body).Decode(&table); err != nil {
		writeBadRequest(ctx, w, err)
		return
	}
	res, err := h.Handle(ctx, &tablesInsertRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   &table,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesInsertRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *bigqueryv2.Table
}

func (h *tablesInsertHandler) Handle(ctx context.Context, r *tablesInsertRequest) (*bigqueryv2.Table, error) {
	encodedTableData, err := json.Marshal(r.table)
	if err != nil {
		return nil, err
	}
	var tableMetadata map[string]interface{}
	if err := json.Unmarshal(encodedTableData, &tableMetadata); err != nil {
		return nil, err
	}

	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.server.contentRepo.CreateTable(ctx, tx, r.table); err != nil {
		return nil, err
	}
	if err := r.dataset.AddTable(
		ctx,
		tx.Tx(),
		metadata.NewTable(
			r.server.metaRepo,
			r.table.TableReference.TableId,
			tableMetadata,
		),
	); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit table: %w", err)
	}
	table := *r.table
	table.Id = r.table.TableReference.TableId
	table.CreationTime = time.Now().Unix()
	return &table, nil
}

func (h *tablesListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &tablesListRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *tablesListHandler) Handle(ctx context.Context, r *tablesListRequest) (*bigqueryv2.TableList, error) {
	return nil, fmt.Errorf("bigquery.tables.list")
}

func (h *tablesPatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &tablesPatchRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesPatchRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *tablesPatchHandler) Handle(ctx context.Context, r *tablesPatchRequest) (*bigqueryv2.Table, error) {
	return nil, fmt.Errorf("unsupported bigquery.tables.patch")
}

func (h *tablesSetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &tablesSetIamPolicyRequest{
		server: server,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesSetIamPolicyRequest struct {
	server *Server
}

func (h *tablesSetIamPolicyHandler) Handle(ctx context.Context, r *tablesSetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("unsupported bigquery.tables.setIamPolicy")
}

func (h *tablesTestIamPermissionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &tablesTestIamPermissionsRequest{
		server: server,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesTestIamPermissionsRequest struct {
	server *Server
}

func (h *tablesTestIamPermissionsHandler) Handle(ctx context.Context, r *tablesTestIamPermissionsRequest) (*bigqueryv2.TestIamPermissionsResponse, error) {
	return nil, fmt.Errorf("unsupported bigquery.tables.testIamPermissions")
}

func (h *tablesUpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &tablesUpdateRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesUpdateRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *tablesUpdateHandler) Handle(ctx context.Context, r *tablesUpdateRequest) (*bigqueryv2.Table, error) {
	return nil, fmt.Errorf("unsupported bigquery.tables.update")
}

type defaultHandler struct{}

func (h *defaultHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger.Logger(ctx).Error(
		"unexpected request path",
		zap.String("path", html.EscapeString(r.URL.Path)),
	)
	w.WriteHeader(http.StatusOK)
}
