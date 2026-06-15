package server

import (
	"context"

	"github.com/goccy/bigquery-emulator/internal/connection"
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
	conn, err := s.connMgr.Connection(ctx, project.ID, "")
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	for _, dataset := range project.Datasets {
		for _, table := range dataset.Tables {
			table.SetupMetadata(project.ID, dataset.ID)
			tableDef, err := types.NewTableWithSchema(table.ToBigqueryV2(project.ID, dataset.ID), table.Data)
			if err != nil {
				return err
			}
			if err := s.addTableData(ctx, tx, project, dataset, tableDef); err != nil {
				return err
			}
		}
	}
	p := s.metaRepo.ProjectFromData(project)
	found, err := s.metaRepo.FindProjectWithConn(ctx, tx.Tx(), p.ID)
	if err != nil {
		return err
	}
	if found == nil {
		// Brand-new project: insert the project and every source-described
		// dataset and table.
		if err := s.metaRepo.AddProjectIfNotExists(ctx, tx.Tx(), p); err != nil {
			return err
		}
		for _, dataset := range p.Datasets() {
			if err := dataset.Insert(ctx, tx.Tx()); err != nil {
				return err
			}
			for _, table := range dataset.Tables() {
				if err := table.Insert(ctx, tx.Tx()); err != nil {
					return err
				}
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}

	// The project already exists. This happens on every restart against a
	// persisted database: the project, its datasets and tables are already
	// stored. Reconcile the source-described objects with what is stored,
	// inserting only what is missing, so that re-running the binary does not
	// fail with a UNIQUE constraint violation and does not drop datasets or
	// tables created at runtime.
	for _, dataset := range p.Datasets() {
		foundDataset := found.Dataset(dataset.ID)
		if foundDataset == nil {
			// New dataset: insert it and link it into the existing project.
			if err := found.AddDataset(ctx, tx.Tx(), dataset); err != nil {
				return err
			}
			for _, table := range dataset.Tables() {
				if err := table.Insert(ctx, tx.Tx()); err != nil {
					return err
				}
			}
			continue
		}
		for _, table := range dataset.Tables() {
			if foundDataset.Table(table.ID) != nil {
				// The table already exists; refresh its metadata.
				if err := s.metaRepo.UpdateTable(ctx, tx.Tx(), table); err != nil {
					return err
				}
				continue
			}
			// New table in an existing dataset: AddTable inserts the table and
			// rewrites the dataset's table list as the union of the persisted
			// and newly added tables.
			if err := foundDataset.AddTable(ctx, tx.Tx(), table); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *Server) addTableData(ctx context.Context, tx *connection.Tx, project *types.Project, dataset *types.Dataset, table *types.Table) error {
	if err := s.contentRepo.CreateOrReplaceTable(ctx, tx, project.ID, dataset.ID, table); err != nil {
		return err
	}
	if err := s.contentRepo.AddTableData(ctx, tx, project.ID, dataset.ID, table); err != nil {
		return err
	}
	return nil
}
