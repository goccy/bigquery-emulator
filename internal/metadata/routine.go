package metadata

type Routine struct {
	ID        string
	ProjectID string
	DatasetID string
	metadata  map[string]interface{}
	repo      *Repository
}

func NewRoutine(repo *Repository, projectID, datasetID, routineID string, metadata map[string]interface{}) *Routine {
	return &Routine{
		ID:        routineID,
		ProjectID: projectID,
		DatasetID: datasetID,
		metadata:  metadata,
		repo:      repo,
	}
}
