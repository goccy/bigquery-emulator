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

func TestCreateOrReplaceView(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "test_dataset"
	)

	ctx := context.Background()

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

	// Create base table with 2 columns
	baseTableSQL := `CREATE TABLE test_dataset.base_table (id INT64, name STRING)`
	job, err := client.Query(baseTableSQL).Run(ctx)
	if err != nil {
		t.Fatalf("failed to create base table: %v", err)
	}
	if _, err := job.Wait(ctx); err != nil {
		t.Fatalf("failed to wait for create base table: %v", err)
	}

	// Insert some data
	insertSQL := `INSERT INTO test_dataset.base_table (id, name) VALUES (1, 'Alice'), (2, 'Bob')`
	job, err = client.Query(insertSQL).Run(ctx)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}
	if _, err := job.Wait(ctx); err != nil {
		t.Fatalf("failed to wait for insert: %v", err)
	}

	// Create initial view with 2 columns
	createViewSQL := `CREATE VIEW test_dataset.my_view AS SELECT id, name FROM test_dataset.base_table`
	job, err = client.Query(createViewSQL).Run(ctx)
	if err != nil {
		t.Fatalf("failed to create view: %v", err)
	}
	if _, err := job.Wait(ctx); err != nil {
		t.Fatalf("failed to wait for create view: %v", err)
	}

	// Verify initial view works
	query := client.Query("SELECT * FROM test_dataset.my_view")
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatalf("failed to query initial view: %v", err)
	}
	count := 0
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("failed to read row: %v", err)
		}
		count++
		if len(row) != 2 {
			t.Errorf("expected 2 columns, got %d", len(row))
		}
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}

	// Now replace the view with a different query (just id column)
	replaceViewSQL := `CREATE OR REPLACE VIEW test_dataset.my_view AS SELECT id FROM test_dataset.base_table`
	job, err = client.Query(replaceViewSQL).Run(ctx)
	if err != nil {
		t.Fatalf("failed to replace view: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("failed to wait for replace view: %v", err)
	}
	if status.Err() != nil {
		t.Fatalf("replace view job failed: %v", status.Err())
	}

	// Verify replaced view works and has only 1 column
	query2 := client.Query("SELECT * FROM test_dataset.my_view")
	it2, err := query2.Read(ctx)
	if err != nil {
		t.Fatalf("failed to query replaced view: %v", err)
	}
	count2 := 0
	for {
		var row []bigquery.Value
		err := it2.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("failed to read row from replaced view: %v", err)
		}
		count2++
		if len(row) != 1 {
			t.Errorf("expected 1 column after replace, got %d", len(row))
		}
	}
	if count2 != 2 {
		t.Errorf("expected 2 rows after replace, got %d", count2)
	}

	t.Log("CREATE OR REPLACE VIEW test passed successfully!")
}
