package metadata

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type Job struct {
	ID        string
	content   *bigqueryv2.Job
	response  *bigqueryv2.QueryResponse
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

func (j *Job) SetResult(ctx context.Context, tx *sql.Tx, response *bigqueryv2.QueryResponse, err error) {
	j.response = response
	j.err = err
	if err := j.repo.UpdateJob(ctx, tx, j); err != nil {
		log.Printf("failed to update job: %s", err.Error())
	}
}

func (j *Job) Content() *bigqueryv2.Job {
	return j.content
}

func (j *Job) Wait(ctx context.Context) (*bigqueryv2.QueryResponse, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			foundJob, err := j.repo.FindJob(ctx, nil, j.ID)
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

func NewJob(repo *Repository, id string, content *bigqueryv2.Job, response *bigqueryv2.QueryResponse, err error) *Job {
	return &Job{
		ID:       id,
		content:  content,
		response: response,
		err:      err,
		repo:     repo,
	}
}
