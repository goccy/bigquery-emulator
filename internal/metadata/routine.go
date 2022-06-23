package metadata

type Routine struct {
	ID       string
	metadata map[string]interface{}
	repo     *Repository
}

func NewRoutine(repo *Repository, id string, metadata map[string]interface{}) *Routine {
	return &Routine{
		ID:       id,
		metadata: metadata,
		repo:     repo,
	}
}
