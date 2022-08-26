package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type Table struct {
	ID        string
	ProjectID string
	DatasetID string
	metadata  map[string]interface{}
	repo      *Repository
}

func (t *Table) Update(ctx context.Context, tx *sql.Tx, metadata map[string]interface{}) error {
	return t.repo.UpdateTable(ctx, tx, t)
}

func (t *Table) Insert(ctx context.Context, tx *sql.Tx) error {
	return t.repo.AddTable(ctx, tx, t)
}

func (t *Table) Delete(ctx context.Context, tx *sql.Tx) error {
	return t.repo.DeleteTable(ctx, tx, t)
}

func (t *Table) Content() (*bigqueryv2.Table, error) {
	encoded, err := json.Marshal(t.metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to encode metadata: %w", err)
	}
	var v bigqueryv2.Table
	if err := json.Unmarshal(encoded, &v); err != nil {
		return nil, fmt.Errorf("failed to decode metadata to table: %w", err)
	}
	return &v, nil
}

func NewTable(repo *Repository, projectID, datasetID, tableID string, metadata map[string]interface{}) *Table {
	return &Table{
		ID:        tableID,
		ProjectID: projectID,
		DatasetID: datasetID,
		metadata:  metadata,
		repo:      repo,
	}
}
