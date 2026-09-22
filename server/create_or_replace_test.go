package server_test

import (
	"context"
	"path/filepath"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// TestCreateOrReplaceExistingTable runs CREATE OR REPLACE TABLE against
// a table that already exists. It must replace the table, not fail with
// "409 ... table is already created".
func TestCreateOrReplaceExistingTable(t *testing.T) {
	ctx := context.Background()

	const projectName = "test"

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.YAMLSource(filepath.Join("testdata", "data.yaml"))); err != nil {
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

	exec := func(t *testing.T, sql string) error {
		t.Helper()
		job, err := client.Query(sql).Run(ctx)
		if err != nil {
			return err
		}
		status, err := job.Wait(ctx)
		if err != nil {
			return err
		}
		return status.Err()
	}
	count := func(t *testing.T, table string) int64 {
		t.Helper()
		it, err := client.Query("SELECT COUNT(*) FROM " + table).Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			t.Fatal(err)
		}
		if err := it.Next(&row); err != iterator.Done {
			t.Fatalf("expected one row, got %v", err)
		}
		return row[0].(int64)
	}

	t.Run("CTAS", func(t *testing.T) {
		if err := exec(t, "CREATE OR REPLACE TABLE dataset1.cor_ctas AS SELECT 1 AS id"); err != nil {
			t.Fatal(err)
		}
		if err := exec(t, "CREATE OR REPLACE TABLE dataset1.cor_ctas AS SELECT 1 AS id UNION ALL SELECT 2"); err != nil {
			t.Fatalf("replace: %v", err)
		}
		if got := count(t, "dataset1.cor_ctas"); got != 2 {
			t.Fatalf("rows after replace = %d, want 2", got)
		}
		md, err := client.Dataset("dataset1").Table("cor_ctas").Metadata(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if md.NumRows != 2 {
			t.Fatalf("metadata numRows after replace = %d, want 2", md.NumRows)
		}
	})

	t.Run("column list", func(t *testing.T) {
		if err := exec(t, "CREATE OR REPLACE TABLE dataset1.cor_cols (a INT64)"); err != nil {
			t.Fatal(err)
		}
		if err := exec(t, "CREATE OR REPLACE TABLE dataset1.cor_cols (a INT64, b STRING)"); err != nil {
			t.Fatalf("replace: %v", err)
		}
		md, err := client.Dataset("dataset1").Table("cor_cols").Metadata(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(md.Schema) != 2 || md.Schema[1].Name != "b" {
			t.Fatalf("schema after replace = %+v, want columns a, b", md.Schema)
		}
	})

	t.Run("CREATE TABLE without OR REPLACE still fails", func(t *testing.T) {
		if err := exec(t, "CREATE TABLE dataset1.cor_cols (a INT64)"); err == nil {
			t.Fatal("expected an error creating an existing table")
		}
	})

	t.Run("drop then create", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			if err := exec(t, "CREATE TABLE dataset1.cor_cycle AS SELECT 1 AS id"); err != nil {
				t.Fatalf("iteration %d create: %v", i, err)
			}
			if err := exec(t, "DROP TABLE dataset1.cor_cycle"); err != nil {
				t.Fatalf("iteration %d drop: %v", i, err)
			}
		}
	})
}
