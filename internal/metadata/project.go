package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

var ErrDuplicatedDataset = errors.New("dataset is already created")
var ErrDatasetInUse = errors.New("dataset is in use, empty the dataset before deleting it")

type Project struct {
	ID         string
	datasets   []*Dataset
	datasetMap map[string]*Dataset
	jobs       []*Job
	jobMap     map[string]*Job
	mu         sync.RWMutex
	repo       *Repository
}

func (p *Project) DatasetIDs() []string {
	ids := make([]string, len(p.datasets))
	for i := 0; i < len(p.datasets); i++ {
		ids[i] = p.datasets[i].ID
	}
	return ids
}

func (p *Project) JobIDs() []string {
	ids := make([]string, len(p.jobs))
	for i := 0; i < len(p.jobs); i++ {
		ids[i] = p.jobs[i].ID
	}
	return ids
}

func (p *Project) Job(id string) *Job {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.jobMap[id]
}

func (p *Project) Jobs() []*Job {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.jobs
}

func (p *Project) Dataset(id string) *Dataset {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.datasetMap[id]
}

func (p *Project) Datasets() []*Dataset {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.datasets
}

func (p *Project) Insert(ctx context.Context, tx *sql.Tx) error {
	return p.repo.AddProject(ctx, tx, p)
}

func (p *Project) Delete(ctx context.Context, tx *sql.Tx) error {
	return p.repo.DeleteProject(ctx, tx, p)
}

func (p *Project) AddDataset(ctx context.Context, tx *sql.Tx, dataset *Dataset) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.datasetMap[dataset.ID]; exists {
		return fmt.Errorf("dataset %s: %w", dataset.ID, ErrDuplicatedDataset)
	}
	if err := dataset.Insert(ctx, tx); err != nil {
		return err
	}
	p.datasets = append(p.datasets, dataset)
	p.datasetMap[dataset.ID] = dataset
	if err := p.repo.UpdateProject(ctx, tx, p); err != nil {
		return err
	}
	return nil
}

func (p *Project) DeleteDataset(ctx context.Context, tx *sql.Tx, id string, inUseOk bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	dataset, exists := p.datasetMap[id]
	if !exists {
		return fmt.Errorf("dataset '%s' is not found in project '%s'", id, p.ID)
	}
	if !inUseOk && len(dataset.TableIDs()) > 0 {
		return fmt.Errorf("dataset %s: %w", id, ErrDatasetInUse)
	}
	if err := dataset.Delete(ctx, tx); err != nil {
		return err
	}
	newDatasets := make([]*Dataset, 0, len(p.datasets))
	for _, dataset := range p.datasets {
		if dataset.ID == id {
			continue
		}
		newDatasets = append(newDatasets, dataset)
	}
	p.datasets = newDatasets
	delete(p.datasetMap, id)
	if err := p.repo.UpdateProject(ctx, tx, p); err != nil {
		return err
	}
	return nil
}

func (p *Project) AddJob(ctx context.Context, tx *sql.Tx, job *Job) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.jobMap[job.ID]; exists {
		return fmt.Errorf("job %s is already created", job.ID)
	}
	if err := job.Insert(ctx, tx); err != nil {
		return err
	}
	p.jobs = append(p.jobs, job)
	p.jobMap[job.ID] = job
	if err := p.repo.UpdateProject(ctx, tx, p); err != nil {
		return err
	}
	return nil
}

func (p *Project) DeleteJob(ctx context.Context, tx *sql.Tx, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	job, exists := p.jobMap[id]
	if !exists {
		return fmt.Errorf("job '%s' is not found in project '%s'", id, p.ID)
	}
	if err := job.Delete(ctx, tx); err != nil {
		return err
	}
	newJobs := make([]*Job, 0, len(p.jobs))
	for _, job := range p.jobs {
		if job.ID == id {
			continue
		}
		newJobs = append(newJobs, job)
	}
	p.jobs = newJobs
	delete(p.jobMap, id)
	if err := p.repo.UpdateProject(ctx, tx, p); err != nil {
		return err
	}
	return nil
}

func NewProject(repo *Repository, id string, datasets []*Dataset, jobs []*Job) *Project {
	datasetMap := map[string]*Dataset{}
	for _, dataset := range datasets {
		datasetMap[dataset.ID] = dataset
	}
	jobMap := map[string]*Job{}
	for _, job := range jobs {
		jobMap[job.ID] = job
	}
	return &Project{
		ID:         id,
		datasets:   datasets,
		jobs:       jobs,
		datasetMap: datasetMap,
		jobMap:     jobMap,
		repo:       repo,
	}
}
