package server

import (
	"context"

	"github.com/goccy/bigquery-emulator/types"
)

func (s *Server) addProjects(ctx context.Context, projects []*types.Project) error {
	for _, project := range projects {
		if err := s.addProject(ctx, project); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) addProject(ctx context.Context, project *types.Project) error {
	tx, err := s.metaRepo.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Commit()
	for _, dataset := range project.Datasets {
		for _, table := range dataset.Tables {
			if err := s.addTableData(ctx, project, dataset, table); err != nil {
				return err
			}
		}
	}
	p := s.metaRepo.ProjectFromData(project)
	if found, _ := s.metaRepo.FindProject(ctx, p.ID); found != nil {
		if err := s.metaRepo.UpdateProject(ctx, tx, p); err != nil {
			return err
		}
	} else {
		if err := s.metaRepo.AddProject(ctx, tx, p); err != nil {
			return err
		}
	}
	for _, dataset := range p.Datasets() {
		if err := dataset.Insert(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) addTableData(ctx context.Context, project *types.Project, dataset *types.Dataset, table *types.Table) error {
	if err := s.contentRepo.CreateOrReplaceTable(ctx, project.ID, dataset.ID, table); err != nil {
		return err
	}
	if err := s.contentRepo.AddTableData(ctx, project.ID, dataset.ID, table); err != nil {
		return err
	}
	return nil
}
