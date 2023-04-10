package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/goccy/go-zetasqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
)

var schemata = []string{
	`
CREATE TABLE IF NOT EXISTS projects (
  id         STRING NOT NULL,
  datasetIDs ARRAY<STRING>,
  jobIDs     ARRAY<STRING>,
  PRIMARY KEY (id)
)`,
	`
CREATE TABLE IF NOT EXISTS jobs (
  id        STRING NOT NULL,
  projectID STRING NOT NULL,
  metadata  STRING,
  result    STRING,
  error     STRING,
  PRIMARY KEY (projectID, id)
)`,
	`
CREATE TABLE IF NOT EXISTS datasets (
  id         STRING NOT NULL,
  projectID  STRING NOT NULL,
  tableIDs   ARRAY<STRING>,
  modelIDs   ARRAY<STRING>,
  routineIDs ARRAY<STRING>,
  metadata   STRING,
  PRIMARY KEY (projectID, id)
)`,
	`
CREATE TABLE IF NOT EXISTS tables (
  id        STRING NOT NULL,
  projectID STRING NOT NULL,
  datasetID STRING NOT NULL,
  metadata  STRING,
  PRIMARY KEY (projectID, datasetID, id)
)`,
	`
CREATE TABLE IF NOT EXISTS models (
  id        STRING NOT NULL,
  projectID STRING NOT NULL,
  datasetID STRING NOT NULL,
  metadata  STRING,
  PRIMARY KEY (projectID, datasetID, id)
)`,
	`
CREATE TABLE IF NOT EXISTS routines (
  id        STRING NOT NULL,
  projectID STRING NOT NULL,
  datasetID STRING NOT NULL,
  metadata  STRING,
  PRIMARY KEY (projectID, datasetID, id)
)`,
}

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) (*Repository, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	for _, ddl := range schemata {
		if _, err := tx.ExecContext(context.Background(), ddl); err != nil {
			return nil, err
		}
	}
	return &Repository{
		db: db,
	}, nil
}

func (r *Repository) getConnection(ctx context.Context) (*sql.Conn, error) {
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	if err := conn.Raw(func(c interface{}) error {
		zetasqliteConn, ok := c.(*zetasqlite.ZetaSQLiteConn)
		if !ok {
			return fmt.Errorf("failed to get ZetaSQLiteConn from %T", c)
		}
		zetasqliteConn.SetNamePath([]string{})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to setup connection: %w", err)
	}
	return conn, nil
}

func (r *Repository) ProjectFromData(data *types.Project) *Project {
	datasets := make([]*Dataset, 0, len(data.Datasets))
	for _, ds := range data.Datasets {
		datasets = append(datasets, r.DatasetFromData(data.ID, ds))
	}
	jobs := make([]*Job, 0, len(data.Jobs))
	for _, j := range data.Jobs {
		jobs = append(jobs, r.JobFromData(data.ID, j))
	}
	return NewProject(r, data.ID, datasets, jobs)
}

func (r *Repository) DatasetFromData(projectID string, data *types.Dataset) *Dataset {
	tables := make([]*Table, 0, len(data.Tables))
	for _, table := range data.Tables {
		tables = append(tables, r.TableFromData(projectID, data.ID, table))
	}
	models := make([]*Model, 0, len(data.Models))
	for _, model := range data.Models {
		models = append(models, r.ModelFromData(projectID, data.ID, model))
	}
	routines := make([]*Routine, 0, len(data.Routines))
	for _, routine := range data.Routines {
		routines = append(routines, r.RoutineFromData(projectID, data.ID, routine))
	}
	return NewDataset(r, projectID, data.ID, nil, tables, models, routines)
}

func (r *Repository) JobFromData(projectID string, data *types.Job) *Job {
	return NewJob(r, projectID, data.ID, nil, nil, nil)
}

func (r *Repository) TableFromData(projectID, datasetID string, data *types.Table) *Table {
	return NewTable(r, projectID, datasetID, data.ID, data.Metadata)
}

func (r *Repository) ModelFromData(projectID, datasetID string, data *types.Model) *Model {
	return NewModel(r, projectID, datasetID, data.ID, data.Metadata)
}

func (r *Repository) RoutineFromData(projectID, datasetID string, data *types.Routine) *Routine {
	return NewRoutine(r, projectID, datasetID, data.ID, data.Metadata)
}

func (r *Repository) FindProjectWithConn(ctx context.Context, tx *sql.Tx, id string) (*Project, error) {
	projects, err := r.findProjects(ctx, tx, []string{id})
	if err != nil {
		return nil, err
	}
	if len(projects) != 1 {
		return nil, nil
	}
	if projects[0].ID != id {
		return nil, nil
	}
	return projects[0], nil
}

func (r *Repository) FindProject(ctx context.Context, id string) (*Project, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	projects, err := r.findProjects(ctx, tx, []string{id})
	if err != nil {
		return nil, err
	}
	if len(projects) != 1 {
		return nil, nil
	}
	if projects[0].ID != id {
		return nil, nil
	}
	return projects[0], nil
}

func (r *Repository) findProjects(ctx context.Context, tx *sql.Tx, ids []string) ([]*Project, error) {
	rows, err := tx.QueryContext(ctx, "SELECT id, datasetIDs, jobIDs FROM projects WHERE id IN UNNEST(@ids)", ids)
	if err != nil {
		return nil, fmt.Errorf("failed to get projects: %w", err)
	}
	defer rows.Close()
	projects := []*Project{}
	for rows.Next() {
		var (
			projectID  string
			datasetIDs []interface{}
			jobIDs     []interface{}
		)
		if err := rows.Scan(&projectID, &datasetIDs, &jobIDs); err != nil {
			return nil, err
		}
		datasets, err := r.findDatasets(ctx, tx, projectID, r.convertToStrings(datasetIDs))
		if err != nil {
			return nil, err
		}
		jobs, err := r.findJobs(ctx, tx, projectID, r.convertToStrings(jobIDs))
		if err != nil {
			return nil, err
		}
		projects = append(projects, NewProject(r, projectID, datasets, jobs))
	}
	return projects, nil
}

func (r *Repository) FindAllProjects(ctx context.Context) ([]*Project, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	rows, err := tx.QueryContext(ctx, "SELECT id, datasetIDs, jobIDs FROM projects")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	projects := []*Project{}
	for rows.Next() {
		var (
			projectID  string
			datasetIDs []interface{}
			jobIDs     []interface{}
		)
		if err := rows.Scan(&projectID, &datasetIDs, &jobIDs); err != nil {
			return nil, err
		}
		datasets, err := r.findDatasets(ctx, tx, projectID, r.convertToStrings(datasetIDs))
		if err != nil {
			return nil, err
		}
		jobs, err := r.findJobs(ctx, tx, projectID, r.convertToStrings(jobIDs))
		if err != nil {
			return nil, err
		}
		projects = append(projects, NewProject(r, projectID, datasets, jobs))
	}
	return projects, nil
}

func (r *Repository) AddProjectIfNotExists(ctx context.Context, tx *sql.Tx, project *Project) error {
	p, err := r.FindProjectWithConn(ctx, tx, project.ID)
	if err != nil {
		return err
	}
	if p == nil {
		return r.AddProject(ctx, tx, project)
	}
	return nil
}

func (r *Repository) AddProject(ctx context.Context, tx *sql.Tx, project *Project) error {
	if _, err := tx.Exec(
		"INSERT projects (id, datasetIDs, jobIDs) VALUES (@id, @datasetIDs, @jobIDs)",
		sql.Named("id", project.ID),
		sql.Named("datasetIDs", project.DatasetIDs()),
		sql.Named("jobIDs", project.JobIDs()),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateProject(ctx context.Context, tx *sql.Tx, project *Project) error {
	if _, err := tx.Exec(
		"UPDATE projects SET datasetIDs = @datasetIDs, jobIDs = @jobIDs WHERE id = @id",
		sql.Named("id", project.ID),
		sql.Named("datasetIDs", project.DatasetIDs()),
		sql.Named("jobIDs", project.JobIDs()),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteProject(ctx context.Context, tx *sql.Tx, project *Project) error {
	if _, err := tx.Exec("DELETE FROM projects WHERE id = @id", project.ID); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindJob(ctx context.Context, projectID, jobID string) (*Job, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	jobs, err := r.findJobs(ctx, tx, projectID, []string{jobID})
	if err != nil {
		return nil, err
	}
	if len(jobs) != 1 {
		return nil, nil
	}
	if jobs[0].ID != jobID {
		return nil, nil
	}
	return jobs[0], nil
}

func (r *Repository) findJobs(ctx context.Context, tx *sql.Tx, projectID string, jobIDs []string) ([]*Job, error) {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT id, projectID, metadata, result, error FROM jobs WHERE projectID = @projectID AND id IN UNNEST(@jobIDs)",
		sql.Named("projectID", projectID),
		sql.Named("jobIDs", jobIDs),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []*Job{}
	for rows.Next() {
		var (
			jobID     string
			projectID string
			metadata  string
			result    string
			jobErr    string
		)
		if err := rows.Scan(&jobID, &projectID, &metadata, &result, &jobErr); err != nil {
			return nil, err
		}
		var content bigqueryv2.Job
		if len(metadata) > 0 {
			if err := json.Unmarshal([]byte(metadata), &content); err != nil {
				return nil, fmt.Errorf("failed to decode metadata content %s: %w", metadata, err)
			}
		}
		var response internaltypes.QueryResponse
		if len(result) > 0 {
			if err := json.Unmarshal([]byte(result), &response); err != nil {
				return nil, fmt.Errorf("failed to decode job response %s: %w", result, err)
			}
		}
		var resErr error
		if jobErr != "" {
			resErr = errors.New(jobErr)
		}
		jobs = append(
			jobs,
			NewJob(r, projectID, jobID, &content, &response, resErr),
		)
	}
	return jobs, nil
}

func (r *Repository) AddJob(ctx context.Context, tx *sql.Tx, job *Job) error {
	metadata, err := json.Marshal(job.content)
	if err != nil {
		return err
	}
	result, err := json.Marshal(job.response)
	if err != nil {
		return err
	}
	var jobErr string
	if job.err != nil {
		jobErr = job.err.Error()
	}
	if _, err := tx.Exec(
		"INSERT jobs (id, projectID, metadata, result, error) VALUES (@id, @projectID, @metadata, @result, @error)",
		sql.Named("id", job.ID),
		sql.Named("projectID", job.ProjectID),
		sql.Named("metadata", string(metadata)),
		sql.Named("result", string(result)),
		sql.Named("error", jobErr),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateJob(ctx context.Context, tx *sql.Tx, job *Job) error {
	metadata, err := json.Marshal(job.content)
	if err != nil {
		return err
	}
	result, err := json.Marshal(job.response)
	if err != nil {
		return err
	}
	var jobErr string
	if job.err != nil {
		jobErr = job.err.Error()
	}
	if _, err := tx.Exec(
		"UPDATE jobs SET metadata = @metadata, result = @result, error = @error WHERE projectID = @projectID AND id = @id",
		sql.Named("id", job.ID),
		sql.Named("projectID", job.ProjectID),
		sql.Named("metadata", string(metadata)),
		sql.Named("result", string(result)),
		sql.Named("error", jobErr),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteJob(ctx context.Context, tx *sql.Tx, job *Job) error {
	if _, err := tx.Exec(
		"DELETE FROM jobs WHERE projectID = @projectID AND id = @id",
		job.ProjectID,
		job.ID,
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindDataset(ctx context.Context, projectID, datasetID string) (*Dataset, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	datasets, err := r.findDatasets(ctx, tx, projectID, []string{datasetID})
	if err != nil {
		return nil, err
	}
	if len(datasets) != 1 {
		return nil, nil
	}
	if datasets[0].ID != datasetID {
		return nil, nil
	}
	return datasets[0], nil
}

func (r *Repository) findDatasets(ctx context.Context, tx *sql.Tx, projectID string, datasetIDs []string) ([]*Dataset, error) {
	rows, err := tx.QueryContext(ctx,
		"SELECT id, projectID, tableIDs, modelIDs, routineIDs, metadata FROM datasets WHERE projectID = @projectID AND id IN UNNEST(@datasetIDs)",
		sql.Named("projectID", projectID),
		sql.Named("datasetIDs", datasetIDs),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	datasets := []*Dataset{}
	for rows.Next() {
		var (
			datasetID  string
			projectID  string
			tableIDs   []interface{}
			modelIDs   []interface{}
			routineIDs []interface{}
			metadata   string
		)
		if err := rows.Scan(&datasetID, &projectID, &tableIDs, &modelIDs, &routineIDs, &metadata); err != nil {
			return nil, err
		}
		tables, err := r.findTables(ctx, tx, projectID, datasetID, r.convertToStrings(tableIDs))
		if err != nil {
			return nil, err
		}
		models, err := r.findModels(ctx, tx, projectID, datasetID, r.convertToStrings(modelIDs))
		if err != nil {
			return nil, err
		}
		routines, err := r.findRoutines(ctx, tx, projectID, datasetID, r.convertToStrings(routineIDs))
		if err != nil {
			return nil, err
		}
		var content bigqueryv2.Dataset
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		datasets = append(
			datasets,
			NewDataset(r, projectID, datasetID, &content, tables, models, routines),
		)
	}
	return datasets, nil
}

func (r *Repository) AddDataset(ctx context.Context, tx *sql.Tx, dataset *Dataset) error {
	metadata, err := json.Marshal(dataset.content)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		"INSERT datasets (id, projectID, tableIDs, modelIDs, routineIDs, metadata) VALUES (@id, @projectID, @tableIDs, @modelIDs, @routineIDs, @metadata)",
		sql.Named("id", dataset.ID),
		sql.Named("projectID", dataset.ProjectID),
		sql.Named("tableIDs", dataset.TableIDs()),
		sql.Named("modelIDs", dataset.ModelIDs()),
		sql.Named("routineIDs", dataset.RoutineIDs()),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateDataset(ctx context.Context, tx *sql.Tx, dataset *Dataset) error {
	metadata, err := json.Marshal(dataset.content)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		"UPDATE datasets SET tableIDs = @tableIDs, modelIDs = @modelIDs, routineIDs = @routineIDs, metadata = @metadata WHERE projectID = @projectID AND id = @id",
		sql.Named("id", dataset.ID),
		sql.Named("projectID", dataset.ProjectID),
		sql.Named("tableIDs", dataset.TableIDs()),
		sql.Named("modelIDs", dataset.ModelIDs()),
		sql.Named("routineIDs", dataset.RoutineIDs()),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteDataset(ctx context.Context, tx *sql.Tx, dataset *Dataset) error {
	if _, err := tx.Exec(
		"DELETE FROM datasets WHERE projectID = @projectID AND id = @id",
		dataset.ProjectID,
		dataset.ID,
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindTable(ctx context.Context, projectID, datasetID, tableID string) (*Table, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	tables, err := r.findTables(ctx, tx, projectID, datasetID, []string{tableID})
	if err != nil {
		return nil, err
	}
	if len(tables) != 1 {
		return nil, nil
	}
	if tables[0].ID != tableID {
		return nil, nil
	}
	return tables[0], nil
}

func (r *Repository) findTables(ctx context.Context, tx *sql.Tx, projectID, datasetID string, tableIDs []string) ([]*Table, error) {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT id, metadata FROM tables WHERE projectID = @projectID AND datasetID = @datasetID AND id IN UNNEST(@tableIDs)",
		sql.Named("projectID", projectID),
		sql.Named("datasetID", datasetID),
		sql.Named("tableIDs", tableIDs),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := []*Table{}
	for rows.Next() {
		var (
			tableID  string
			metadata string
		)
		if err := rows.Scan(&tableID, &metadata); err != nil {
			return nil, err
		}
		var content map[string]interface{}
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		tables = append(
			tables,
			NewTable(r, projectID, datasetID, tableID, content),
		)
	}
	return tables, nil
}

func (r *Repository) AddTable(ctx context.Context, tx *sql.Tx, table *Table) error {
	metadata, err := json.Marshal(table.metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		"INSERT tables (id, projectID, datasetID, metadata) VALUES (@id, @projectID, @datasetID, @metadata)",
		sql.Named("id", table.ID),
		sql.Named("projectID", table.ProjectID),
		sql.Named("datasetID", table.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateTable(ctx context.Context, tx *sql.Tx, table *Table) error {
	metadata, err := json.Marshal(table.metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		"UPDATE tables SET metadata = @metadata WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id",
		sql.Named("id", table.ID),
		sql.Named("projectID", table.ProjectID),
		sql.Named("datasetID", table.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteTable(ctx context.Context, tx *sql.Tx, table *Table) error {
	if _, err := tx.Exec(
		"DELETE FROM tables WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id",
		table.ProjectID,
		table.DatasetID,
		table.ID,
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindModel(ctx context.Context, projectID, datasetID, modelID string) (*Model, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	models, err := r.findModels(ctx, tx, projectID, datasetID, []string{modelID})
	if err != nil {
		return nil, err
	}
	if len(models) != 1 {
		return nil, nil
	}
	if models[0].ID != modelID {
		return nil, nil
	}
	return models[0], nil
}

func (r *Repository) findModels(ctx context.Context, tx *sql.Tx, projectID, datasetID string, modelIDs []string) ([]*Model, error) {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT id, metadata FROM models WHERE projectID = @projectID AND datasetID = @datasetID AND id IN UNNEST(@modelIDs)",
		sql.Named("projectID", projectID),
		sql.Named("datasetID", datasetID),
		sql.Named("modelIDs", modelIDs),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	models := []*Model{}
	for rows.Next() {
		var (
			modelID  string
			metadata string
		)
		if err := rows.Scan(&modelID, &metadata); err != nil {
			return nil, err
		}
		var content map[string]interface{}
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		models = append(
			models,
			NewModel(r, projectID, datasetID, modelID, content),
		)
	}
	return models, nil
}

func (r *Repository) AddModel(ctx context.Context, tx *sql.Tx, model *Model) error {
	metadata, err := json.Marshal(model.metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		"INSERT models (id, projectID, datasetID, metadata) VALUES (@id, @projectID, @datasetID, @metadata)",
		sql.Named("id", model.ID),
		sql.Named("projectID", model.ProjectID),
		sql.Named("datasetID", model.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateModel(ctx context.Context, tx *sql.Tx, model *Model) error {
	metadata, err := json.Marshal(model.metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		"UPDATE models SET metadata = @metadata WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id",
		sql.Named("id", model.ID),
		sql.Named("projectID", model.ProjectID),
		sql.Named("datasetID", model.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteModel(ctx context.Context, tx *sql.Tx, model *Model) error {
	if _, err := tx.Exec(
		"DELETE FROM models WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id",
		model.ProjectID,
		model.DatasetID,
		model.ID,
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindRoutine(ctx context.Context, projectID, datasetID, routineID string) (*Routine, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	routines, err := r.findRoutines(ctx, tx, projectID, datasetID, []string{routineID})
	if err != nil {
		return nil, err
	}
	if len(routines) != 1 {
		return nil, nil
	}
	if routines[0].ID != routineID {
		return nil, nil
	}
	return routines[0], nil
}

func (r *Repository) findRoutines(ctx context.Context, tx *sql.Tx, projectID, datasetID string, routineIDs []string) ([]*Routine, error) {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT id, metadata FROM routines WHERE projectID = @projectID AND datasetID = @datasetID AND id IN UNNEST(@routineIDs)",
		sql.Named("projectID", projectID),
		sql.Named("datasetID", datasetID),
		sql.Named("routineIDs", routineIDs),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	routines := []*Routine{}
	for rows.Next() {
		var (
			routineID string
			metadata  string
		)
		if err := rows.Scan(&routineID, &metadata); err != nil {
			return nil, err
		}
		var content map[string]interface{}
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		routines = append(
			routines,
			NewRoutine(r, projectID, datasetID, routineID, content),
		)
	}
	return routines, nil
}

func (r *Repository) AddRoutine(ctx context.Context, tx *sql.Tx, routine *Routine) error {
	metadata, err := json.Marshal(routine.metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		"INSERT routines (id, projectID, datasetID, metadata) VALUES (@id, @projectID, @datasetID, @metadata)",
		sql.Named("id", routine.ID),
		sql.Named("projectID", routine.ProjectID),
		sql.Named("datasetID", routine.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateRoutine(ctx context.Context, tx *sql.Tx, routine *Routine) error {
	metadata, err := json.Marshal(routine.metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		"UPDATE routines SET metadata = @metadata WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id",
		sql.Named("id", routine.ID),
		sql.Named("projectID", routine.ProjectID),
		sql.Named("datasetID", routine.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteRoutine(ctx context.Context, tx *sql.Tx, routine *Routine) error {
	if _, err := tx.Exec(
		"DELETE FROM routines WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id",
		routine.ProjectID,
		routine.DatasetID,
		routine.ID,
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) convertToStrings(v []interface{}) []string {
	ret := make([]string, 0, len(v))
	for _, vv := range v {
		ret = append(ret, vv.(string))
	}
	return ret
}
