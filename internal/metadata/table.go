package metadata

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/goccy/go-json"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type Table struct {
	ID        string
	ProjectID string
	DatasetID string
	metadata  map[string]interface{}
	repo      *Repository
}

// Patch shallow-merges the provided top-level fields into the table metadata
// (the semantics of bigquery.tables.patch) and persists the result.
func (t *Table) Patch(ctx context.Context, tx *sql.Tx, patch map[string]interface{}) error {
	if t.metadata == nil {
		t.metadata = map[string]interface{}{}
	}
	for k, v := range patch {
		t.metadata[k] = v
	}
	return t.repo.UpdateTable(ctx, tx, t)
}

// Replace overwrites the table metadata with the provided resource (the
// semantics of bigquery.tables.update), preserving the immutable identity
// fields when the caller omits them.
func (t *Table) Replace(ctx context.Context, tx *sql.Tx, metadata map[string]interface{}) error {
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	for _, key := range []string{"id", "kind", "type", "tableReference", "creationTime", "selfLink"} {
		if _, ok := metadata[key]; ok {
			continue
		}
		if v, exists := t.metadata[key]; exists {
			metadata[key] = v
		}
	}
	t.metadata = metadata
	return t.repo.UpdateTable(ctx, tx, t)
}

func (t *Table) Insert(ctx context.Context, tx *sql.Tx) error {
	return t.repo.AddTable(ctx, tx, t)
}

func (t *Table) Delete(ctx context.Context, tx *sql.Tx) error {
	return t.repo.DeleteTable(ctx, tx, t)
}

// IsView reports whether the table metadata describes a (logical or
// materialized) view rather than an ordinary table.
func (t *Table) IsView() bool {
	typ, _ := t.metadata["type"].(string)
	return typ == "VIEW" || typ == "MATERIALIZED_VIEW"
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
