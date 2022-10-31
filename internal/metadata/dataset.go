package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

var ErrDuplicatedTable = errors.New("table is already created")

type Dataset struct {
	ID         string
	ProjectID  string
	tables     []*Table
	tableMap   map[string]*Table
	models     []*Model
	modelMap   map[string]*Model
	routines   []*Routine
	routineMap map[string]*Routine
	mu         sync.RWMutex
	content    *bigqueryv2.Dataset
	repo       *Repository
}

func (d *Dataset) TableIDs() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	tableIDs := make([]string, 0, len(d.tables))
	for _, table := range d.tables {
		tableIDs = append(tableIDs, table.ID)
	}
	return tableIDs
}

func (d *Dataset) ModelIDs() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	modelIDs := make([]string, 0, len(d.models))
	for _, model := range d.models {
		modelIDs = append(modelIDs, model.ID)
	}
	return modelIDs
}

func (d *Dataset) RoutineIDs() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	routineIDs := make([]string, 0, len(d.routines))
	for _, routine := range d.routines {
		routineIDs = append(routineIDs, routine.ID)
	}
	return routineIDs
}

func (d *Dataset) Content() *bigqueryv2.Dataset {
	return d.content
}

func (d *Dataset) UpdateContentIfExists(newContent *bigqueryv2.Dataset) {
	if newContent.Description != "" {
		d.content.Description = newContent.Description
	}
	if newContent.Etag != "" {
		d.content.Etag = newContent.Etag
	}
	if newContent.FriendlyName != "" {
		d.content.FriendlyName = newContent.FriendlyName
	}
	if d.content.IsCaseInsensitive != newContent.IsCaseInsensitive {
		d.content.IsCaseInsensitive = newContent.IsCaseInsensitive
	}
	if newContent.Kind != "" {
		d.content.Kind = newContent.Kind
	}
	if len(newContent.Labels) != 0 {
		d.content.Labels = newContent.Labels
	}
	if newContent.LastModifiedTime != 0 {
		d.content.LastModifiedTime = newContent.LastModifiedTime
	}
	if newContent.Location != "" {
		d.content.Location = newContent.Location
	}
	if newContent.MaxTimeTravelHours != 0 {
		d.content.MaxTimeTravelHours = newContent.MaxTimeTravelHours
	}
	if newContent.SatisfiesPzs {
		d.content.SatisfiesPzs = newContent.SatisfiesPzs
	}
	if newContent.SelfLink != "" {
		d.content.SelfLink = newContent.SelfLink
	}
}

func (d *Dataset) UpdateContent(newContent *bigqueryv2.Dataset) {
	d.content = newContent
}

func (d *Dataset) Insert(ctx context.Context, tx *sql.Tx) error {
	return d.repo.AddDataset(ctx, tx, d)
}

func (d *Dataset) Delete(ctx context.Context, tx *sql.Tx) error {
	return d.repo.DeleteDataset(ctx, tx, d)
}

func (d *Dataset) DeleteModel(ctx context.Context, tx *sql.Tx, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	model, exists := d.modelMap[id]
	if !exists {
		return fmt.Errorf("model '%s' is not found in dataset '%s'", id, d.ID)
	}
	if err := model.Delete(ctx, tx); err != nil {
		return err
	}
	newModels := make([]*Model, 0, len(d.models))
	for _, model := range d.models {
		if model.ID == id {
			continue
		}
		newModels = append(newModels, model)
	}
	d.models = newModels
	delete(d.modelMap, id)
	if err := d.repo.UpdateDataset(ctx, tx, d); err != nil {
		return err
	}
	return nil
}

func (d *Dataset) AddTable(ctx context.Context, tx *sql.Tx, table *Table) error {
	d.mu.Lock()
	if _, exists := d.tableMap[table.ID]; exists {
		d.mu.Unlock()
		return fmt.Errorf("table %s: %w", table.ID, ErrDuplicatedTable)
	}
	if err := table.Insert(ctx, tx); err != nil {
		d.mu.Unlock()
		return err
	}
	d.tables = append(d.tables, table)
	d.tableMap[table.ID] = table
	d.mu.Unlock()

	if err := d.repo.UpdateDataset(ctx, tx, d); err != nil {
		return err
	}
	return nil
}

func (d *Dataset) Table(id string) *Table {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.tableMap[id]
}

func (d *Dataset) Model(id string) *Model {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.modelMap[id]
}

func (d *Dataset) Routine(id string) *Routine {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.routineMap[id]
}

func (d *Dataset) Tables() []*Table {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.tables
}

func (d *Dataset) Models() []*Model {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.models
}

func (d *Dataset) Routines() []*Routine {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.routines
}

func NewDataset(
	repo *Repository,
	projectID string,
	datasetID string,
	content *bigqueryv2.Dataset,
	tables []*Table,
	models []*Model,
	routines []*Routine) *Dataset {

	tableMap := map[string]*Table{}
	for _, table := range tables {
		tableMap[table.ID] = table
	}
	modelMap := map[string]*Model{}
	for _, model := range models {
		modelMap[model.ID] = model
	}
	routineMap := map[string]*Routine{}
	for _, routine := range routines {
		routineMap[routine.ID] = routine
	}

	return &Dataset{
		ID:         datasetID,
		ProjectID:  projectID,
		tables:     tables,
		tableMap:   tableMap,
		models:     models,
		modelMap:   modelMap,
		routines:   routines,
		routineMap: routineMap,
		content:    content,
		repo:       repo,
	}
}
