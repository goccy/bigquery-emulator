package server_test

import (
	"context"
	"path/filepath"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"google.golang.org/api/option"
)

// TestJobStatementType checks statistics.query.statementType and, for a
// CREATE TABLE AS SELECT, the destination table and DDL target, which
// clients such as dbt-bigquery read after a job finishes.
func TestJobStatementType(t *testing.T) {
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

	run := func(t *testing.T, sql string) (*bigquery.Job, *bigquery.QueryStatistics) {
		t.Helper()
		job, err := client.Query(sql).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := status.Err(); err != nil {
			t.Fatal(err)
		}
		stats, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
		if !ok {
			t.Fatalf("unexpected statistics details %T", status.Statistics.Details)
		}
		return job, stats
	}

	// Run in order: later statements depend on objects created earlier.
	for _, tc := range []struct {
		sql  string
		want string
	}{
		{"SELECT 1 AS id", "SELECT"},
		{"WITH t AS (SELECT 1 AS id) SELECT id FROM t", "SELECT"},
		{"CREATE TABLE dataset1.st_plain (id INT64)", "CREATE_TABLE"},
		{"INSERT INTO dataset1.st_plain (id) VALUES (1), (2)", "INSERT"},
		{"UPDATE dataset1.st_plain SET id = 3 WHERE id = 2", "UPDATE"},
		{"DELETE FROM dataset1.st_plain WHERE id = 3", "DELETE"},
		{"CREATE OR REPLACE VIEW dataset1.st_view AS SELECT id FROM dataset1.st_plain", "CREATE_VIEW"},
		{"DROP VIEW dataset1.st_view", "DROP_VIEW"},
		{"CREATE OR REPLACE FUNCTION dataset1.st_fn(x INT64) AS (x * 2)", "CREATE_FUNCTION"},
		{"CREATE OR REPLACE TABLE FUNCTION dataset1.st_tvf(n INT64) AS (SELECT n AS v)", "CREATE_TABLE_FUNCTION"},
		{"DROP TABLE dataset1.st_plain", "DROP_TABLE"},
		{"SELECT 1; SELECT 2", "SCRIPT"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			_, stats := run(t, tc.sql)
			if stats.StatementType != tc.want {
				t.Errorf("statementType = %q, want %q", stats.StatementType, tc.want)
			}
		})
	}

	t.Run("CREATE OR REPLACE TABLE AS SELECT", func(t *testing.T) {
		job, stats := run(t, "CREATE OR REPLACE TABLE dataset1.st_ctas AS SELECT 1 AS id UNION ALL SELECT 2")
		if stats.StatementType != "CREATE_TABLE_AS_SELECT" {
			t.Fatalf("statementType = %q, want CREATE_TABLE_AS_SELECT", stats.StatementType)
		}
		if stats.DDLTargetTable == nil || stats.DDLTargetTable.TableID != "st_ctas" {
			t.Fatalf("ddlTargetTable = %+v, want dataset1.st_ctas", stats.DDLTargetTable)
		}
		cfg, err := job.Config()
		if err != nil {
			t.Fatal(err)
		}
		dst := cfg.(*bigquery.QueryConfig).Dst
		if dst == nil {
			t.Fatal("destination table is nil")
		}
		if dst.ProjectID != projectName || dst.DatasetID != "dataset1" || dst.TableID != "st_ctas" {
			t.Fatalf("destination = %s.%s.%s, want %s.dataset1.st_ctas", dst.ProjectID, dst.DatasetID, dst.TableID, projectName)
		}
		md, err := dst.Metadata(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if md.NumRows != 2 {
			t.Fatalf("destination numRows = %d, want 2", md.NumRows)
		}
	})
}
