package metadata

import "context"

type Table struct {
	ID       string
	metadata map[string]interface{}
	repo     *Repository
}

func (t *Table) Insert(ctx context.Context) error {
	return nil
}

func (t *Table) Delete(ctx context.Context) error {
	return nil
}

func NewTable(repo *Repository, id string, metadata map[string]interface{}) *Table {
	return &Table{
		ID:       id,
		metadata: metadata,
		repo:     repo,
	}
}
