package server_test

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"google.golang.org/api/option"
)

// TestAnonymousColumnNames checks the names BigQuery gives result columns
// that have no alias: f0_, f1_, ..., numbered over the anonymous columns
// only. The UDF documentation's AddFourAndDivide example selects
// `val, AddFourAndDivide(val, 2)` and returns columns `val` and `f0_`
// (https://cloud.google.com/bigquery/docs/user-defined-functions).
func TestAnonymousColumnNames(t *testing.T) {
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

	names := func(t *testing.T, sql string) []string {
		t.Helper()
		it, err := client.Query(sql).Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row []bigquery.Value
		_ = it.Next(&row)
		out := make([]string, 0, len(it.Schema))
		for _, f := range it.Schema {
			out = append(out, f.Name)
		}
		return out
	}

	if err := func() error {
		job, err := client.Query("CREATE FUNCTION dataset1.AddFourAndDivide(x INT64, y INT64) RETURNS FLOAT64 AS ((x + 4) / y)").Run(ctx)
		if err != nil {
			return err
		}
		status, err := job.Wait(ctx)
		if err != nil {
			return err
		}
		return status.Err()
	}(); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		sql  string
		want []string
	}{
		// The documented example: a named column, then an anonymous one.
		{"SELECT val, dataset1.AddFourAndDivide(val, 2) FROM UNNEST([2, 3, 5, 8]) AS val", []string{"val", "f0_"}},
		{"SELECT 1", []string{"f0_"}},
		{"SELECT 1, 2", []string{"f0_", "f1_"}},
		{"SELECT 1, 'a' AS x, 2", []string{"f0_", "x", "f1_"}},
		{"SELECT 1 EXCEPT DISTINCT SELECT 2", []string{"f0_"}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			if got := names(t, tc.sql); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("column names = %v, want %v", got, tc.want)
			}
		})
	}
}
