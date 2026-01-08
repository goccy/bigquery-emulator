package server_test

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func TestViewLoadSupport(t *testing.T) {
	ctx := context.Background()
	const (
		projectName = "test-project"
		datasetName = "test_dataset"
		tableName   = "test_table"
		viewName    = "test_view"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}

	// Define project with a table and a view in metadata
	project := types.NewProject(projectName, types.NewDataset(datasetName,
		&types.Table{
			ID: tableName,
			Columns: []*types.Column{
				{Name: "id", Type: types.INT64},
				{Name: "name", Type: types.STRING},
			},
			Data: types.Data{
				{"id": 1, "name": "alice"},
				{"id": 2, "name": "bob"},
			},
		},
		&types.Table{
			ID: viewName,
			Metadata: map[string]interface{}{
				"tableReference": map[string]interface{}{
					"projectId": projectName,
					"datasetId": datasetName,
					"tableId":   viewName,
				},
				"view": map[string]interface{}{
					"query":        "SELECT * FROM `test-project.test_dataset.test_table`",
					"useLegacySql": false,
				},
				"type": "VIEW",
			},
		},
	))

	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Query data from the view
	query := client.Query("SELECT * FROM `test-project.test_dataset.test_view` ORDER BY id")
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type Row struct {
		ID   int    `bigquery:"id"`
		Name string `bigquery:"name"`
	}
	var gotRows []*Row
	for {
		var r Row
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		gotRows = append(gotRows, &r)
	}

	if len(gotRows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(gotRows))
	}
	if gotRows[0].Name != "alice" || gotRows[1].Name != "bob" {
		t.Fatalf("unexpected data: %+v, %+v", gotRows[0], gotRows[1])
	}
}
