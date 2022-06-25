package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/go-zetasqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

var schemata = []string{
	`
CREATE TABLE IF NOT EXISTS projects (
  id         STRING NOT NULL,
  datasetIDs ARRAY<STRING>,
  jobIDs     ARRAY<STRING>
)`,
	`
CREATE TABLE IF NOT EXISTS jobs (
  id       STRING NOT NULL,
  metadata STRING,
  result   STRING,
  error    STRING
)`,
	`
CREATE TABLE IF NOT EXISTS datasets (
  id         STRING NOT NULL,
  tableIDs   ARRAY<STRING>,
  modelIDs   ARRAY<STRING>,
  routineIDs ARRAY<STRING>,
  metadata   STRING
)`,
	`
CREATE TABLE IF NOT EXISTS tables (
  id       STRING NOT NULL,
  metadata STRING
)`,
	`
CREATE TABLE IF NOT EXISTS models (
  id       STRING NOT NULL,
  metadata STRING
)`,
	`
CREATE TABLE IF NOT EXISTS routines (
  id       STRING NOT NULL,
  metadata STRING
)`,
}

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) (*Repository, error) {
	for _, ddl := range schemata {
		if _, err := db.Exec(ddl); err != nil {
			return nil, err
		}
	}
	return &Repository{
		db: db,
	}, nil
}

func (r *Repository) Begin(ctx context.Context) (*sql.Tx, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	return conn.BeginTx(ctx, nil)
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
		datasets = append(datasets, r.DatasetFromData(ds))
	}
	jobs := make([]*Job, 0, len(data.Jobs))
	for _, j := range data.Jobs {
		jobs = append(jobs, r.JobFromData(j))
	}
	return NewProject(r, data.ID, datasets, jobs)
}

func (r *Repository) DatasetFromData(data *types.Dataset) *Dataset {
	tables := make([]*Table, 0, len(data.Tables))
	for _, table := range data.Tables {
		tables = append(tables, r.TableFromData(table))
	}
	models := make([]*Model, 0, len(data.Models))
	for _, model := range data.Models {
		models = append(models, r.ModelFromData(model))
	}
	routines := make([]*Routine, 0, len(data.Routines))
	for _, routine := range data.Routines {
		routines = append(routines, r.RoutineFromData(routine))
	}
	return NewDataset(r, data.ID, nil, tables, models, routines)
}

func (r *Repository) JobFromData(data *types.Job) *Job {
	return NewJob(r, data.ID, nil, nil, nil)
}

func (r *Repository) TableFromData(data *types.Table) *Table {
	return NewTable(r, data.ID, data.Metadata)
}

func (r *Repository) ModelFromData(data *types.Model) *Model {
	return NewModel(r, data.ID, data.Metadata)
}

func (r *Repository) RoutineFromData(data *types.Routine) *Routine {
	return NewRoutine(r, data.ID, data.Metadata)
}

func (r *Repository) FindProject(ctx context.Context, id string) (*Project, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	projects, err := r.findProjects(ctx, conn, []string{id})
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

func (r *Repository) findProjects(ctx context.Context, conn *sql.Conn, ids []string) ([]*Project, error) {
	rows, err := conn.QueryContext(ctx, "SELECT id, datasetIDs, jobIDs FROM projects WHERE id IN UNNEST(@ids)", ids)
	if err != nil {
		return nil, fmt.Errorf("failed to get projects: %w", err)
	}
	defer rows.Close()
	projects := []*Project{}
	for rows.Next() {
		var (
			id         string
			datasetIDs []string
			jobIDs     []string
		)
		if err := rows.Scan(&id, &datasetIDs, &jobIDs); err != nil {
			return nil, err
		}
		datasets, err := r.findDatasets(ctx, conn, datasetIDs)
		if err != nil {
			return nil, err
		}
		jobs, err := r.findJobs(ctx, conn, jobIDs)
		if err != nil {
			return nil, err
		}
		projects = append(projects, NewProject(r, id, datasets, jobs))
	}
	return projects, nil
}

func (r *Repository) FindAllProjects(ctx context.Context) ([]*Project, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, "SELECT id, datasetIDs, jobIDs FROM projects")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	projects := []*Project{}
	for rows.Next() {
		var (
			id         string
			datasetIDs []string
			jobIDs     []string
		)
		if err := rows.Scan(&id, &datasetIDs, &jobIDs); err != nil {
			return nil, err
		}
		datasets, err := r.findDatasets(ctx, conn, datasetIDs)
		if err != nil {
			return nil, err
		}
		jobs, err := r.findJobs(ctx, conn, jobIDs)
		if err != nil {
			return nil, err
		}
		projects = append(projects, NewProject(r, id, datasets, jobs))
	}
	return projects, nil
}

func (r *Repository) AddProjectIfNotExists(ctx context.Context, tx *sql.Tx, project *Project) error {
	p, err := r.FindProject(ctx, project.ID)
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

func (r *Repository) FindJob(ctx context.Context, id string) (*Job, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	jobs, err := r.findJobs(ctx, conn, []string{id})
	if err != nil {
		return nil, err
	}
	if len(jobs) != 1 {
		return nil, nil
	}
	if jobs[0].ID != id {
		return nil, nil
	}
	return jobs[0], nil
}

func (r *Repository) findJobs(ctx context.Context, conn *sql.Conn, ids []string) ([]*Job, error) {
	rows, err := conn.QueryContext(ctx, "SELECT id, metadata, result, error FROM jobs WHERE id IN UNNEST(@ids)", ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []*Job{}
	for rows.Next() {
		var (
			id       string
			metadata string
			result   string
			jobErr   string
		)
		if err := rows.Scan(&id, &metadata, &result, &jobErr); err != nil {
			return nil, err
		}
		var content bigqueryv2.Job
		if len(metadata) > 0 {
			if err := json.Unmarshal([]byte(metadata), &content); err != nil {
				return nil, fmt.Errorf("failed to decode metadata content %s: %w", metadata, err)
			}
		}
		var response bigqueryv2.QueryResponse
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
			NewJob(r, id, &content, &response, resErr),
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
		"INSERT jobs (id, metadata, result, error) VALUES (@id, @metadata, @result, @error)",
		sql.Named("id", job.ID),
		sql.Named("metadata", metadata),
		sql.Named("result", result),
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
		"UPDATE jobs SET metadata = @metadata, result = @result, error = @error WHERE id = @id",
		sql.Named("id", job.ID),
		sql.Named("metadata", metadata),
		sql.Named("result", result),
		sql.Named("error", jobErr),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteJob(ctx context.Context, tx *sql.Tx, job *Job) error {
	if _, err := tx.Exec("DELETE FROM jobs WHERE id = @id", job.ID); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindDataset(ctx context.Context, id string) (*Dataset, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	datasets, err := r.findDatasets(ctx, conn, []string{id})
	if err != nil {
		return nil, err
	}
	if len(datasets) != 1 {
		return nil, nil
	}
	if datasets[0].ID != id {
		return nil, nil
	}
	return datasets[0], nil
}

func (r *Repository) findDatasets(ctx context.Context, conn *sql.Conn, ids []string) ([]*Dataset, error) {
	rows, err := conn.QueryContext(ctx,
		"SELECT id, tableIDs, modelIDs, routineIDs, metadata FROM datasets WHERE id IN UNNEST(@ids)",
		ids,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	datasets := []*Dataset{}
	for rows.Next() {
		var (
			id         string
			tableIDs   []string
			modelIDs   []string
			routineIDs []string
			metadata   string
		)
		if err := rows.Scan(&id, &tableIDs, &modelIDs, &routineIDs, &metadata); err != nil {
			return nil, err
		}
		tables, err := r.findTables(ctx, conn, tableIDs)
		if err != nil {
			return nil, err
		}
		models, err := r.findModels(ctx, conn, modelIDs)
		if err != nil {
			return nil, err
		}
		routines, err := r.findRoutines(ctx, conn, routineIDs)
		if err != nil {
			return nil, err
		}
		var content bigqueryv2.Dataset
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		datasets = append(
			datasets,
			NewDataset(r, id, &content, tables, models, routines),
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
		"INSERT datasets (id, tableIDs, modelIDs, routineIDs, metadata) VALUES (@id, @tableIDs, @modelIDs, @routineIDs, @metadata)",
		sql.Named("id", dataset.ID),
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
		"UPDATE datasets SET tableIDs = @tableIDs, modelIDs = @modelIDs, routineIDs = @routineIDs, metadata = @metadata WHERE id = @id",
		sql.Named("id", dataset.ID),
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
	if _, err := tx.Exec("DELETE FROM datasets WHERE id = @id", dataset.ID); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindTable(ctx context.Context, id string) (*Table, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tables, err := r.findTables(ctx, conn, []string{id})
	if err != nil {
		return nil, err
	}
	if len(tables) != 1 {
		return nil, nil
	}
	if tables[0].ID != id {
		return nil, nil
	}
	return tables[0], nil
}

func (r *Repository) findTables(ctx context.Context, conn *sql.Conn, ids []string) ([]*Table, error) {
	rows, err := conn.QueryContext(ctx, "SELECT id, metadata FROM tables WHERE id IN UNNEST(@ids)", ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := []*Table{}
	for rows.Next() {
		var (
			id       string
			metadata string
		)
		if err := rows.Scan(&id, &metadata); err != nil {
			return nil, err
		}
		var content map[string]interface{}
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		tables = append(
			tables,
			NewTable(r, id, content),
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
		"INSERT tables (id, metadata) VALUES (@id, @metadata)",
		sql.Named("id", table.ID),
		sql.Named("metadata", metadata),
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
		"UPDATE tables SET metadata = @metadata WHERE id = @id",
		sql.Named("id", table.ID),
		sql.Named("metadata", metadata),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteTable(ctx context.Context, tx *sql.Tx, table *Table) error {
	if _, err := tx.Exec("DELETE FROM tables WHERE id = @id", table.ID); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindModel(ctx context.Context, id string) (*Model, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	models, err := r.findModels(ctx, conn, []string{id})
	if err != nil {
		return nil, err
	}
	if len(models) != 1 {
		return nil, nil
	}
	if models[0].ID != id {
		return nil, nil
	}
	return models[0], nil
}

func (r *Repository) findModels(ctx context.Context, conn *sql.Conn, ids []string) ([]*Model, error) {
	rows, err := conn.QueryContext(ctx, "SELECT id, metadata FROM models WHERE id IN UNNEST(@ids)", ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	models := []*Model{}
	for rows.Next() {
		var (
			id       string
			metadata string
		)
		if err := rows.Scan(&id, &metadata); err != nil {
			return nil, err
		}
		var content map[string]interface{}
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		models = append(
			models,
			NewModel(r, id, content),
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
		"INSERT models (id, metadata) VALUES (@id, @metadata)",
		sql.Named("id", model.ID),
		sql.Named("metadata", metadata),
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
		"UPDATE models SET metadata = @metadata WHERE id = @id",
		sql.Named("id", model.ID),
		sql.Named("metadata", metadata),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteModel(ctx context.Context, tx *sql.Tx, model *Model) error {
	if _, err := tx.Exec("DELETE FROM models WHERE id = @id", model.ID); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindRoutine(ctx context.Context, id string) (*Routine, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	routines, err := r.findRoutines(ctx, conn, []string{id})
	if err != nil {
		return nil, err
	}
	if len(routines) != 1 {
		return nil, nil
	}
	if routines[0].ID != id {
		return nil, nil
	}
	return routines[0], nil
}

func (r *Repository) findRoutines(ctx context.Context, conn *sql.Conn, ids []string) ([]*Routine, error) {
	rows, err := conn.QueryContext(ctx, "SELECT id, metadata FROM routines WHERE id IN UNNEST(@ids)", ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	routines := []*Routine{}
	for rows.Next() {
		var (
			id       string
			metadata string
		)
		if err := rows.Scan(&id, &metadata); err != nil {
			return nil, err
		}
		var content map[string]interface{}
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		routines = append(
			routines,
			NewRoutine(r, id, content),
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
		"INSERT routines (id, metadata) VALUES (@id, @metadata)",
		sql.Named("id", routine.ID),
		sql.Named("metadata", metadata),
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
		"UPDATE routines SET metadata = @metadata WHERE id = @id",
		sql.Named("id", routine.ID),
		sql.Named("metadata", metadata),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteRoutine(ctx context.Context, tx *sql.Tx, routine *Routine) error {
	if _, err := tx.Exec("DELETE FROM routines WHERE id = @id", routine.ID); err != nil {
		return err
	}
	return nil
}
