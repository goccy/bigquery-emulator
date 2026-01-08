package server

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-json"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

func TestViewQueryAPI_Isolated(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "view-query-project"
		datasetID = "view_query_dataset"
		tableID   = "source_table"
		viewID    = "test_view"
	)

	server, err := New(TempStorage)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer server.Close()

	if err := server.SetProject(projectID); err != nil {
		t.Fatalf("failed to set project: %v", err)
	}

	testServer := server.TestServer()
	defer testServer.Close()

	// Helper to create client
	newClient := func(t *testing.T) *bigquery.Client {
		t.Helper()
		client, err := bigquery.NewClient(
			ctx,
			projectID,
			option.WithEndpoint(testServer.URL),
			option.WithoutAuthentication(),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		return client
	}

	// 1. Create Dataset
	client := newClient(t)
	defer client.Close()
	if err := client.Dataset(datasetID).Create(ctx, nil); err != nil {
		t.Fatalf("failed to create dataset: %v", err)
	}

	// 2. Create Source Table
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "name", Type: bigquery.StringFieldType},
	}
	if err := client.Dataset(datasetID).Table(tableID).Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
	}); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// 3. Insert Data into Source Table
	inserter := client.Dataset(datasetID).Table(tableID).Inserter()
	items := []*struct {
		ID   int    `bigquery:"id"`
		Name string `bigquery:"name"`
	}{
		{ID: 1, Name: "alice"},
		{ID: 2, Name: "bob"},
	}
	if err := inserter.Put(ctx, items); err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// 4. Create View via API (using raw HTTP request to simulate jobs.insert or tables.insert)
	// But here we can use the client library which uses tables.insert
	viewQuery := "SELECT * FROM `" + projectID + "." + datasetID + "." + tableID + "`"
	if err := client.Dataset(datasetID).Table(viewID).Create(ctx, &bigquery.TableMetadata{
		ViewQuery: viewQuery,
	}); err != nil {
		t.Fatalf("failed to create view: %v", err)
	}

	// 5. Query View via jobs.query
	bqService, err := bigqueryv2.NewService(ctx, option.WithHTTPClient(testServer.Client()))
	if err != nil {
		t.Fatalf("failed to create bigquery service: %v", err)
	}

	useLegacySql := false
	queryRequest := &bigqueryv2.QueryRequest{
		Query:        "SELECT * FROM `" + projectID + "." + datasetID + "." + viewID + "`",
		UseLegacySql: &useLegacySql,
	}
	queryResponse, err := bqService.Jobs.Query(projectID, queryRequest).Context(ctx).Do()
	if err != nil {
		t.Fatalf("failed to query view via API: %v", err)
	}

	if len(queryResponse.Rows) != 2 {
		t.Fatalf("unexpected rows length: %d", len(queryResponse.Rows))
	}
}

func toBigQueryJsonValue(v interface{}) bigqueryv2.JsonValue {
	b, _ := json.Marshal(v)
	return bigqueryv2.JsonValue(b)
}
