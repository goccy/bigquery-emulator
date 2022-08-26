package metadata

import (
	"context"
	"database/sql"
)

type Model struct {
	ID        string
	ProjectID string
	DatasetID string
	metadata  map[string]interface{}
	repo      *Repository
}

func (m *Model) Insert(ctx context.Context, tx *sql.Tx) error {
	return m.repo.AddModel(ctx, tx, m)
}

func (m *Model) Delete(ctx context.Context, tx *sql.Tx) error {
	return m.repo.DeleteModel(ctx, tx, m)
}

func NewModel(repo *Repository, projectID, datasetID, modelID string, metadata map[string]interface{}) *Model {
	return &Model{
		ID:        modelID,
		ProjectID: projectID,
		DatasetID: datasetID,
		metadata:  metadata,
		repo:      repo,
	}
}
