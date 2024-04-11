package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type Job struct {
	ID        string
	ProjectID string
	content   *bigqueryv2.Job
	response  *internaltypes.QueryResponse
	err       error
	completed bool
	mu        sync.RWMutex
	repo      *Repository
}

func (j *Job) Query() string {
	return j.content.Configuration.Query.Query
}

func (j *Job) QueryParameters() []*bigqueryv2.QueryParameter {
	return j.content.Configuration.Query.QueryParameters
}

func (j *Job) SetResult(ctx context.Context, tx *sql.Tx, response *internaltypes.QueryResponse, err error) error {
	j.response = response
	j.err = err
	if err := j.repo.UpdateJob(ctx, tx, j); err != nil {
		return fmt.Errorf("failed to update job: %w", err)
	}
	return nil
}

func (j *Job) Content() *bigqueryv2.Job {
	return j.content
}

func (j *Job) Wait(ctx context.Context) (*internaltypes.QueryResponse, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.response != nil {
		return j.response, nil
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			foundJob, err := j.repo.FindJob(ctx, j.ProjectID, j.ID)
			if err != nil {
				return nil, err
			}
			if foundJob != nil {
				return foundJob.response, foundJob.err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (j *Job) Cancel(ctx context.Context) error {
	// TODO: job needs to be able to rollback
	return nil
}

func (j *Job) Insert(ctx context.Context, tx *sql.Tx) error {
	return j.repo.AddJob(ctx, tx, j)
}

func (j *Job) Delete(ctx context.Context, tx *sql.Tx) error {
	return j.repo.DeleteJob(ctx, tx, j)
}

func NewJob(repo *Repository, projectID, jobID string, content *bigqueryv2.Job, response *internaltypes.QueryResponse, err error) *Job {
	return &Job{
		ID:        jobID,
		ProjectID: projectID,
		content:   content,
		response:  response,
		err:       err,
		repo:      repo,
	}
}
