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

func TestViewDataRepro(t *testing.T) {
	ctx := context.Background()
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table1"
		viewName    = "view1"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
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

	// 1. Create table
	table := client.Dataset(datasetName).Table(tableName)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "id", Type: bigquery.IntegerFieldType},
			{Name: "name", Type: bigquery.StringFieldType},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// 2. Add data to the table
	type Row struct {
		ID   int    `bigquery:"id"`
		Name string `bigquery:"name"`
	}
	rows := []*Row{
		{ID: 1, Name: "alice"},
		{ID: 2, Name: "bob"},
	}
	if err := table.Inserter().Put(ctx, rows); err != nil {
		t.Fatal(err)
	}

	// 3. Create view pointing on it
	view := client.Dataset(datasetName).Table(viewName)
	if err := view.Create(ctx, &bigquery.TableMetadata{
		ViewQuery: "SELECT id, name FROM dataset1.table1",
	}); err != nil {
		t.Fatal(err)
	}

	// 4. Query data from the view
	query := client.Query("SELECT * FROM dataset1.view1 ORDER BY id")
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
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
