package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/goccy/bigquery-emulator/types"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TestViewUpdate(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset"
		tableID   = "table"
		viewID    = "view"
	)

	s, err := New(TempStorage)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	s.httpServer = &http.Server{Addr: "localhost:9050"}

	if err := s.Load(
		StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
					types.NewTable(
						tableID,
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
						},
						nil,
					),
					&types.Table{
						ID: viewID,
						View: &types.View{
							Query: "SELECT * FROM `test.dataset.table`",
						},
					},
				),
			),
		),
	); err != nil {
		t.Fatalf("failed to load data: %v", err)
	}

	// 1. Get current view definition
	project, err := s.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		t.Fatalf("failed to find project: %v", err)
	}
	dataset := project.Dataset(datasetID)
	tableMeta := dataset.Table(viewID)

	getHandler := &tablesGetHandler{}
	table, err := getHandler.Handle(ctx, &tablesGetRequest{
		server:  s,
		project: project,
		dataset: dataset,
		table:   tableMeta,
	})
	if err != nil {
		t.Fatalf("failed to get view: %v", err)
	}

	if table.View == nil {
		t.Fatal("expected view definition, but got nil")
	}
	if table.View.Query != "SELECT * FROM `test.dataset.table`" {
		t.Errorf("unexpected view query: %s", table.View.Query)
	}

	// 2. Update view definition
	newQuery := "SELECT id FROM `test.dataset.table`"
	patchHandler := &tablesPatchHandler{}
	newTable := &bigqueryv2.Table{
		View: &bigqueryv2.ViewDefinition{
			Query: newQuery,
		},
	}
	updatedTable, err := patchHandler.Handle(ctx, &tablesPatchRequest{
		server:   s,
		project:  project,
		dataset:  dataset,
		table:    tableMeta,
		newTable: newTable,
	})
	if err != nil {
		t.Fatalf("failed to update view: %v", err)
	}

	if updatedTable.View.Query != newQuery {
		t.Errorf("unexpected updated view query: %s", updatedTable.View.Query)
	}

	// 3. Verify update
	table, err = getHandler.Handle(ctx, &tablesGetRequest{
		server:  s,
		project: project,
		dataset: dataset,
		table:   tableMeta,
	})
	if err != nil {
		t.Fatalf("failed to get view: %v", err)
	}

	if table.View.Query != newQuery {
		t.Errorf("unexpected view query after update: %s", table.View.Query)
	}
}

func TestViewUpdate_CreatedByAPI(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset"
		tableID   = "table"
		viewID    = "view_created_by_api"
	)

	s, err := New(TempStorage)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	s.httpServer = &http.Server{Addr: "localhost:9050"}

	// Load dependency table
	if err := s.Load(
		StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
					types.NewTable(
						tableID,
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
						},
						nil,
					),
				),
			),
		),
	); err != nil {
		t.Fatalf("failed to load data: %v", err)
	}

	project, err := s.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		t.Fatalf("failed to find project: %v", err)
	}
	dataset := project.Dataset(datasetID)

	// 1. Create view using tablesInsertHandler
	insertHandler := &tablesInsertHandler{}
	viewTable := &bigqueryv2.Table{
		TableReference: &bigqueryv2.TableReference{
			ProjectId: projectID,
			DatasetId: datasetID,
			TableId:   viewID,
		},
		View: &bigqueryv2.ViewDefinition{
			Query: "SELECT * FROM `test.dataset.table`",
		},
	}
	_, serverErr := insertHandler.Handle(ctx, &tablesInsertRequest{
		server:  s,
		project: project,
		dataset: dataset,
		table:   viewTable,
	})
	if serverErr != nil {
		t.Fatalf("failed to create view: %v", serverErr)
	}

	// 2. Verify creation
	tableMeta := dataset.Table(viewID)
	if tableMeta == nil {
		t.Fatal("view not found after creation")
	}
	getHandler := &tablesGetHandler{}
	table, err := getHandler.Handle(ctx, &tablesGetRequest{
		server:  s,
		project: project,
		dataset: dataset,
		table:   tableMeta,
	})
	if err != nil {
		t.Fatalf("failed to get view: %v", err)
	}
	if table.View == nil {
		t.Fatal("expected view definition, but got nil")
	}
	if table.View.Query != "SELECT * FROM `test.dataset.table`" {
		t.Errorf("unexpected view query: %s", table.View.Query)
	}

	// 3. Update view definition
	newQuery := "SELECT id FROM `test.dataset.table`"
	patchHandler := &tablesPatchHandler{}
	newTable := &bigqueryv2.Table{
		View: &bigqueryv2.ViewDefinition{
			Query: newQuery,
		},
	}
	updatedTable, err := patchHandler.Handle(ctx, &tablesPatchRequest{
		server:   s,
		project:  project,
		dataset:  dataset,
		table:    tableMeta,
		newTable: newTable,
	})
	if err != nil {
		t.Fatalf("failed to update view: %v", err)
	}

	if updatedTable.View.Query != newQuery {
		t.Errorf("unexpected updated view query: %s", updatedTable.View.Query)
	}
	if updatedTable.TableReference == nil {
		t.Error("expected table reference in updated table, but got nil")
	}

	// 4. Verify update
	table, err = getHandler.Handle(ctx, &tablesGetRequest{
		server:  s,
		project: project,
		dataset: dataset,
		table:   tableMeta,
	})
	if err != nil {
		t.Fatalf("failed to get view: %v", err)
	}

	if table.View.Query != newQuery {
		t.Errorf("unexpected view query after update: %s", table.View.Query)
	}
}
