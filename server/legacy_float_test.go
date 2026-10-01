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

func TestLegacyFloatColumnIsFloat64(t *testing.T) {
	const projectID = "test"
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject(projectID, types.NewDataset("dataset1")))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Close()
	}()
	client, err := bigquery.NewClient(ctx, projectID, option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "score", Type: bigquery.FloatFieldType},
		{Name: "rec", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{{Name: "score", Type: bigquery.FloatFieldType}}},
	}
	if err := client.Dataset("dataset1").Table("scores").Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		t.Fatal(err)
	}

	runLegacyFloatJob(ctx, t, client.Query("INSERT INTO dataset1.scores (id, score, rec) VALUES (1, 0.1, STRUCT(0.1))"))
	q := client.Query("UPDATE dataset1.scores SET score = @s, rec = STRUCT(@s) WHERE id = 1")
	q.Parameters = []bigquery.QueryParameter{{Name: "s", Value: 0.3}}
	runLegacyFloatJob(ctx, t, q)

	it, err := client.Query("SELECT score, rec.score, score = 0.3 FROM dataset1.scores").Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		t.Fatal(err)
	}
	if row[0] != 0.3 || row[1] != 0.3 || row[2] != true {
		t.Fatalf("got %v; want [0.3 0.3 true]", row)
	}
	if err := it.Next(&row); err != iterator.Done {
		t.Fatalf("want one row, got another: %v", err)
	}
}

func runLegacyFloatJob(ctx context.Context, t *testing.T, q *bigquery.Query) {
	t.Helper()
	job, err := q.Run(ctx)
	if err != nil {
		t.Fatalf("%s: %v", q.Q, err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("%s: %v", q.Q, err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("%s: %v", q.Q, err)
	}
}
