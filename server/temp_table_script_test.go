package server_test

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// TestScriptDropsItsTempTable covers a script that creates a temp table
// and drops it itself, the usual shape of a scripted test that builds
// fixtures per scenario. The dropped temp table was handed to the dataset
// metadata sync, which rejected its two-part name path with
// "unexpected table name path: [<project> <table>]" and failed the job
// after every statement had run. Temp tables belong to no dataset, so the
// sync must leave them alone. The script must also leave nothing behind
// in the dataset.
func TestScriptDropsItsTempTable(t *testing.T) {
	ctx := context.Background()
	const project = "test"

	bqServer, err := server.New(server.MemoryStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(project); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()
	client, err := bigquery.NewClient(ctx, project, option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Dataset("ds").Create(ctx, nil); err != nil {
		t.Fatal(err)
	}

	for _, script := range []string{
		"CREATE TEMP TABLE a AS SELECT 1 AS x; DROP TABLE a; SELECT 1 AS done",
		"CREATE TEMP TABLE b AS SELECT 1 AS x; SELECT COUNT(*) AS n FROM b",
		"BEGIN CREATE TEMP TABLE c AS SELECT 1 AS x; DROP TABLE c; END",
	} {
		q := client.Query(script)
		q.DefaultDatasetID = "ds"
		job, err := q.Run(ctx)
		if err != nil {
			t.Fatalf("%s: run: %v", script, err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatalf("%s: wait: %v", script, err)
		}
		if err := status.Err(); err != nil {
			t.Fatalf("%s: job failed: %v", script, err)
		}
	}

	it := client.Dataset("ds").Tables(ctx)
	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Errorf("temp table %s left behind as dataset metadata", table.TableID)
	}
}
