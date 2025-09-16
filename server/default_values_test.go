package server

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func TestDefaultValues(t *testing.T) {
	ctx := context.Background()

	const (
		projectID = "test"
		datasetID = "dataset1"
	)

	bqServer, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectID); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		StructSource(
			NewProject(
				projectID,
				NewDataset(datasetID),
			),
		),
	); err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	defer client.Close()

	// Test 1: Create table with DEFAULT values using SQL
	t.Run("CreateTableWithDefaults", func(t *testing.T) {
		query := `
		CREATE TABLE dataset1.test_defaults (
			id INT64 NOT NULL,
			name STRING DEFAULT 'Unknown',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
			status STRING DEFAULT 'active',
			score FLOAT64 DEFAULT 0.0
		)`

		job, err := client.Query(query).Run(ctx)
		if err != nil {
			t.Fatalf("Failed to create table with defaults: %v", err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatalf("Failed to wait for job: %v", err)
		}
		if err := status.Err(); err != nil {
			t.Fatalf("Job failed: %v", err)
		}
	})

	// Test 2: Insert data without specifying columns with defaults
	t.Run("InsertWithDefaults", func(t *testing.T) {
		query := `INSERT INTO dataset1.test_defaults (id) VALUES (1), (2), (3)`

		job, err := client.Query(query).Run(ctx)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatalf("Failed to wait for job: %v", err)
		}
		if err := status.Err(); err != nil {
			t.Fatalf("Insert job failed: %v", err)
		}
	})

	// Test 3: Verify DEFAULT values were applied
	t.Run("VerifyDefaults", func(t *testing.T) {
		query := `SELECT id, name, status, score FROM dataset1.test_defaults ORDER BY id`

		it, err := client.Query(query).Read(ctx)
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}

		expectedValues := []struct {
			id     int64
			name   string
			status string
			score  float64
		}{
			{1, "Unknown", "active", 0.0},
			{2, "Unknown", "active", 0.0},
			{3, "Unknown", "active", 0.0},
		}

		rowIndex := 0
		for {
			var row []bigquery.Value
			err := it.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("Failed to iterate: %v", err)
			}

			if rowIndex >= len(expectedValues) {
				t.Fatalf("Got more rows than expected")
			}

			expected := expectedValues[rowIndex]

			// Check id
			if id, ok := row[0].(int64); !ok || id != expected.id {
				t.Errorf("Row %d: expected id=%d, got %v", rowIndex, expected.id, row[0])
			}

			// Check name
			if name, ok := row[1].(string); !ok || name != expected.name {
				t.Errorf("Row %d: expected name=%s, got %v", rowIndex, expected.name, row[1])
			}

			// Check status
			if status, ok := row[2].(string); !ok || status != expected.status {
				t.Errorf("Row %d: expected status=%s, got %v", rowIndex, expected.status, row[2])
			}

			// Check score
			if score, ok := row[3].(float64); !ok || score != expected.score {
				t.Errorf("Row %d: expected score=%f, got %v", rowIndex, expected.score, row[3])
			}

			rowIndex++
		}

		if rowIndex != len(expectedValues) {
			t.Errorf("Expected %d rows, got %d", len(expectedValues), rowIndex)
		}
	})

	// Test 4: Insert with some explicit values
	t.Run("InsertMixedValues", func(t *testing.T) {
		query := `INSERT INTO dataset1.test_defaults (id, name, score) VALUES (4, 'Alice', 95.5)`

		job, err := client.Query(query).Run(ctx)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatalf("Failed to wait for job: %v", err)
		}
		if err := status.Err(); err != nil {
			t.Fatalf("Insert job failed: %v", err)
		}

		// Verify the mixed values
		verifyQuery := `SELECT id, name, status, score FROM dataset1.test_defaults WHERE id = 4`
		it, err := client.Query(verifyQuery).Read(ctx)
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}

		var row []bigquery.Value
		err = it.Next(&row)
		if err != nil {
			t.Fatalf("Failed to get row: %v", err)
		}

		// Check explicitly set values
		if id, ok := row[0].(int64); !ok || id != 4 {
			t.Errorf("Expected id=4, got %v", row[0])
		}
		if name, ok := row[1].(string); !ok || name != "Alice" {
			t.Errorf("Expected name=Alice, got %v", row[1])
		}
		// Check default value
		if status, ok := row[2].(string); !ok || status != "active" {
			t.Errorf("Expected status=active (default), got %v", row[2])
		}
		// Check explicitly set value
		if score, ok := row[3].(float64); !ok || score != 95.5 {
			t.Errorf("Expected score=95.5, got %v", row[3])
		}
	})
}

func TestDefaultValuesViaAPI(t *testing.T) {
	ctx := context.Background()

	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "test_api_defaults"
	)

	bqServer, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectID); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		StructSource(
			NewProject(
				projectID,
				NewDataset(datasetID),
			),
		),
	); err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	defer client.Close()

	// Create table via API with default values
	t.Run("CreateTableViaAPI", func(t *testing.T) {
		meta := &bigquery.TableMetadata{
			Schema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
				{Name: "email", Type: bigquery.StringFieldType, DefaultValueExpression: "'noreply@example.com'"},
				{Name: "age", Type: bigquery.IntegerFieldType, DefaultValueExpression: "18"},
				{Name: "active", Type: bigquery.BooleanFieldType, DefaultValueExpression: "true"},
			},
		}

		tableRef := client.Dataset(datasetID).Table(tableID)
		if err := tableRef.Create(ctx, meta); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert data to test defaults
		query := fmt.Sprintf(`INSERT INTO %s.%s (id) VALUES (100)`, datasetID, tableID)
		job, err := client.Query(query).Run(ctx)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatalf("Failed to wait for job: %v", err)
		}
		if err := status.Err(); err != nil {
			t.Fatalf("Insert job failed: %v", err)
		}

		// Verify defaults were applied
		verifyQuery := fmt.Sprintf(`SELECT id, email, age, active FROM %s.%s WHERE id = 100`, datasetID, tableID)
		it, err := client.Query(verifyQuery).Read(ctx)
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}

		var row []bigquery.Value
		err = it.Next(&row)
		if err != nil {
			t.Fatalf("Failed to get row: %v", err)
		}

		// Verify values
		if id, ok := row[0].(int64); !ok || id != 100 {
			t.Errorf("Expected id=100, got %v", row[0])
		}
		if email, ok := row[1].(string); !ok || email != "noreply@example.com" {
			t.Errorf("Expected email=noreply@example.com, got %v", row[1])
		}
		if age, ok := row[2].(int64); !ok || age != 18 {
			t.Errorf("Expected age=18, got %v", row[2])
		}
		if active, ok := row[3].(bool); !ok || !active {
			t.Errorf("Expected active=true, got %v", row[3])
		}
	})
}