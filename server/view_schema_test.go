package server_test

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

func TestViewSchema(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "test"
		tableID   = "table_for_view_schema"
		viewID    = "view_for_view_schema"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer bqServer.Stop(ctx)

	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
					types.NewTable(
						tableID,
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
							types.NewColumn("name", types.STRING),
						},
						types.Data{
							{"id": 1, "name": "foo"},
						},
					),
				),
			),
		),
	); err != nil {
		t.Fatalf("failed to load data: %v", err)
	}

	testServer := bqServer.TestServer()
	defer testServer.Close()

	client, err := bigquery.NewClient(
		ctx,
		projectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Create View
	view := client.Dataset(datasetID).Table(viewID)
	if err := view.Create(ctx, &bigquery.TableMetadata{
		ViewQuery: "SELECT * FROM `" + projectID + "." + datasetID + "." + tableID + "`",
	}); err != nil {
		t.Fatalf("failed to create view: %v", err)
	}

	// Get View
	meta, err := view.Metadata(ctx)
	if err != nil {
		t.Fatalf("failed to get view metadata: %v", err)
	}

	if meta.Schema == nil {
		t.Fatal("view schema is nil")
	}
	if len(meta.Schema) != 2 {
		t.Fatalf("unexpected schema length: %d", len(meta.Schema))
	}
	if meta.Schema[0].Name != "id" {
		t.Errorf("unexpected field name: %s", meta.Schema[0].Name)
	}
	if meta.Schema[1].Name != "name" {
		t.Errorf("unexpected field name: %s", meta.Schema[1].Name)
	}
}

func TestViewQuery(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "test"
		tableID   = "table_for_view_query"
		viewID    = "view_for_view_query"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer bqServer.Stop(ctx)

	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
					types.NewTable(
						tableID,
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
							types.NewColumn("name", types.STRING),
						},
						types.Data{
							{"id": 1, "name": "foo"},
							{"id": 2, "name": "bar"},
						},
					),
				),
			),
		),
	); err != nil {
		t.Fatalf("failed to load data: %v", err)
	}

	testServer := bqServer.TestServer()
	defer testServer.Close()

	client, err := bigquery.NewClient(
		ctx,
		projectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Create View
	view := client.Dataset(datasetID).Table(viewID)
	if err := view.Create(ctx, &bigquery.TableMetadata{
		ViewQuery: "SELECT * FROM `" + projectID + "." + datasetID + "." + tableID + "`",
	}); err != nil {
		t.Fatalf("failed to create view: %v", err)
	}

	// Query View
	q := client.Query("SELECT COUNT(*) FROM `" + projectID + "." + datasetID + "." + viewID + "`")
	it, err := q.Read(ctx)
	if err != nil {
		t.Fatalf("failed to query view: %v", err)
	}
	var values []bigquery.Value
	for {
		err := it.Next(&values)
		if err != nil {
			if err.Error() == "no more items in iterator" {
				break
			}
			t.Fatalf("failed to iterate: %v", err)
		}
		if len(values) != 1 {
			t.Fatalf("unexpected values length: %d", len(values))
		}
		if values[0].(int64) != 2 {
			t.Errorf("unexpected count: %d", values[0])
		}
	}
}

func TestViewQueryAPI(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "myproject"
		datasetID = "mydataset"
		tableID   = "table_for_view_query_api"
		viewID    = "view_for_view_query_api"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer bqServer.Stop(ctx)

	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
					types.NewTable(
						tableID,
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
							types.NewColumn("name", types.STRING),
						},
						types.Data{
							{"id": 1, "name": "foo"},
							{"id": 2, "name": "bar"},
						},
					),
				),
			),
		),
	); err != nil {
		t.Fatalf("failed to load data: %v", err)
	}

	testServer := bqServer.TestServer()
	defer testServer.Close()

	client, err := bigquery.NewClient(
		ctx,
		projectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Create View
	view := client.Dataset(datasetID).Table(viewID)
	if err := view.Create(ctx, &bigquery.TableMetadata{
		ViewQuery: "SELECT * FROM `" + projectID + "." + datasetID + "." + tableID + "`",
	}); err != nil {
		t.Fatalf("failed to create view: %v", err)
	}

	// Query View via API
	bqService, err := bigqueryv2.NewService(ctx, option.WithHTTPClient(testServer.Client()))
	if err != nil {
		t.Fatalf("failed to create bigquery service: %v", err)
	}

	useLegacySql := false
	queryRequest := &bigqueryv2.QueryRequest{
		Query:        "SELECT 1",
		UseLegacySql: &useLegacySql,
	}
	queryResponse, err := bqService.Jobs.Query(projectID, queryRequest).Context(ctx).Do()
	if err != nil {
		t.Fatalf("failed to query view via API: %v", err)
	}

	if len(queryResponse.Rows) != 1 {
		t.Fatalf("unexpected rows length: %d", len(queryResponse.Rows))
	}
}
