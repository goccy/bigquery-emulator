package server_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/go-json"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func TestSimpleQuery(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		"test",
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	t.Run("array", func(t *testing.T) {
		query := client.Query("SELECT [1, 2, 3] as a")
		it, err := query.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			t.Log("row = ", row)
		}
	})

	t.Run("empty array", func(t *testing.T) {
		query := client.Query("SELECT []")
		it, err := query.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row []bigquery.Value
		for {
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			t.Log("row = ", row)
		}
		if len(row) != 1 || row[0] == nil {
			t.Fatal("Failed to query empty ARRAY")
		}
	})

	t.Run("null array", func(t *testing.T) {
		query := client.Query("SELECT CAST(NULL AS ARRAY<STRING>)")
		it, err := query.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row []bigquery.Value
		for {
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			t.Log("row = ", row)
		}
		if len(row) != 1 || row[0] == nil {
			t.Fatal("Failed to query null ARRAY")
		}
	})
}

func TestDataset(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "project"
	)

	bqServer, err := server.New(server.MemoryStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
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

	fooDataset := client.Dataset("foo")
	if err := fooDataset.Create(ctx, &bigquery.DatasetMetadata{
		Name:        "foo",
		Description: "dataset for foo",
		Location:    "Tokyo",
		Labels:      map[string]string{"aaa": "bbb"},
	}); err != nil {
		t.Fatal(err)
	}

	barDataset := client.Dataset("bar")
	if err := barDataset.Create(ctx, &bigquery.DatasetMetadata{
		Name:        "bar",
		Description: "dataset for bar",
		Location:    "Tokyo",
		Labels:      map[string]string{"bbb": "ccc"},
	}); err != nil {
		t.Fatal(err)
	}

	if datasets := findDatasets(t, ctx, client); len(datasets) != 2 {
		t.Fatalf("failed to find datasets")
	}

	md, err := fooDataset.Update(ctx, bigquery.DatasetMetadataToUpdate{
		Name: "foo2",
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if md.Name != "foo2" {
		t.Fatalf("failed to update dataset metadata: md.Name = %s", md.Name)
	}

	if err := fooDataset.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := barDataset.DeleteWithContents(ctx); err != nil {
		t.Fatal(err)
	}

	if datasets := findDatasets(t, ctx, client); len(datasets) != 0 {
		t.Fatalf("failed to find datasets")
	}
}

func findDatasets(t *testing.T, ctx context.Context, client *bigquery.Client) []*bigquery.Dataset {
	t.Helper()
	var datasets []*bigquery.Dataset
	it := client.Datasets(ctx)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		datasets = append(datasets, ds)
	}
	return datasets
}

func TestJob(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

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

	query := client.Query("SELECT * FROM dataset1.table_a")
	job, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job.Config(); err != nil {
		t.Fatal(err)
	}
	if _, err := job.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	gotJob, err := client.JobFromID(ctx, job.ID())
	if err != nil {
		t.Fatal(err)
	}
	if gotJob.ID() != job.ID() {
		t.Fatalf("failed to get job expected ID %s. but got %s", job.ID(), gotJob.ID())
	}

	job2, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := job2.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := findJobs(t, ctx, client); len(jobs) != 2 {
		t.Fatalf("failed to find jobs. expected 2 jobs but found %d jobs", len(jobs))
	}
	if err := job2.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := findJobs(t, ctx, client); len(jobs) != 1 {
		t.Fatalf("failed to find jobs. expected 1 jobs but found %d jobs", len(jobs))
	}
}

func TestFetchData(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

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

	query := client.Query("SELECT * FROM dataset1.table_b")
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type TableB struct {
		Num    *big.Rat `bigquery:"num"`
		BigNum *big.Rat `bigquery:"bignum"`
		// INTERVAL type cannot assign to struct directly
		// Interval *bigquery.IntervalValue `bigquery:"interval"`
	}

	var row TableB
	_ = it.Next(&row)
	if row.Num.FloatString(4) != "1.2345" {
		t.Fatalf("failed to get NUMERIC value")
	}
	if row.BigNum.FloatString(12) != "1.234567891234" {
		t.Fatalf("failed to get BIGNUMERIC value")
	}
}

func findJobs(t *testing.T, ctx context.Context, client *bigquery.Client) []*bigquery.Job {
	t.Helper()
	var jobs []*bigquery.Job
	it := client.Jobs(ctx)
	for {
		job, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
	}
	return jobs
}

func TestQuery(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.YAMLSource(filepath.Join("testdata", "data.yaml")),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	const (
		projectName = "test"
	)

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

	query := client.Query("SELECT * FROM dataset1.table_a WHERE id = @id")
	query.QueryConfig.Parameters = []bigquery.QueryParameter{
		{Name: "id", Value: 1},
	}
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		t.Log("row = ", row)
	}
}

func TestQueryWithDestination(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.YAMLSource(filepath.Join("testdata", "data.yaml")),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	const (
		projectName = "test"
	)

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

	query := client.Query("SELECT id FROM dataset1.table_a")
	query.QueryConfig.Dst = &bigquery.Table{
		ProjectID: projectName,
		DatasetID: "dataset1",
		TableID:   "table_a_materialized",
	}
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		t.Log("row = ", row)
	}

	query = client.Query("SELECT id FROM dataset1.table_a_materialized")

	it, err = query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		t.Log("row = ", row)
	}
}

type TableSchema struct {
	Int      int
	Str      string
	Float    float64
	Struct   *StructType
	Array    []*StructType
	IntArray []int
	Time     time.Time
}

type StructType struct {
	A int
	B string
	C float64
}

func (s *TableSchema) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"Int":      s.Int,
		"Str":      s.Str,
		"Float":    s.Float,
		"Struct":   s.Struct,
		"Array":    s.Array,
		"IntArray": s.IntArray,
		"Time":     s.Time,
	}, "", nil
}

func TestTable(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		table1Name  = "table1"
		table2Name  = "table2"
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
	table1 := client.Dataset(datasetName).Table(table1Name)
	if err := table1.Create(ctx, nil); err != nil {
		t.Fatalf("%+v", err)
	}

	schema, err := bigquery.InferSchema(TableSchema{})
	if err != nil {
		t.Fatal(err)
	}
	table2 := client.Dataset(datasetName).Table(table2Name)
	if err := table2.Create(ctx, &bigquery.TableMetadata{
		Name:           "table2",
		Schema:         schema,
		ExpirationTime: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	tableIter := client.Dataset(datasetName).Tables(ctx)
	var tableCount int
	for {
		if _, err := tableIter.Next(); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		tableCount++
	}
	if tableCount != 2 {
		t.Fatalf("failed to get tables. expected 2 but got %d", tableCount)
	}
	insertRow := &TableSchema{
		Int:   1,
		Str:   "2",
		Float: 3,
		Struct: &StructType{
			A: 4,
			B: "5",
			C: 6,
		},
		Array: []*StructType{
			{
				A: 7,
				B: "8",
				C: 9,
			},
		},
		IntArray: []int{10},
		Time:     time.Now(),
	}
	if err := table2.Inserter().Put(ctx, []*TableSchema{insertRow}); err != nil {
		t.Fatal(err)
	}
	iter := table2.Read(ctx)
	var rows []*TableSchema
	for {
		var row TableSchema
		if err := iter.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &row)
	}
	if len(rows) != 1 {
		t.Fatalf("failed to get table data. got rows are %d", len(rows))
	}
	if diff := cmp.Diff(insertRow, rows[0], cmpopts.EquateApproxTime(1*time.Second)); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}

	if _, err := table2.Update(ctx, bigquery.TableMetadataToUpdate{
		Description: "updated table",
	}, ""); err != nil {
		t.Fatal(err)
	}
	if err := table2.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	// recreate table2
	if err := table2.Create(ctx, &bigquery.TableMetadata{
		Name:           "table2",
		Schema:         schema,
		ExpirationTime: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDirectDDL(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "foo"
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectID, types.NewDataset(datasetID))
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
		projectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	tableName := fmt.Sprintf("%s.%s.%s", projectID, datasetID, tableID)
	if _, err := client.Query(fmt.Sprintf("CREATE TABLE %s(name STRING)", tableName)).Run(ctx); err != nil {
		t.Fatal(err)
	}
	tableIter := client.Dataset(datasetID).Tables(ctx)
	table, err := tableIter.Next()
	if err != nil {
		if err != iterator.Done {
			t.Fatal(err)
		}
	}
	if table == nil {
		t.Fatal("failed to get created table")
	}
	if table.TableID != tableID {
		t.Fatalf("failed to get table. got table-id is %s", table.TableID)
	}
	if _, err := client.Query(`DROP TABLE test.dataset1.foo`).Run(ctx); err != nil {
		t.Fatal(err)
	}
	tableIter = client.Dataset(datasetID).Tables(ctx)
	var tableCount int
	for {
		if _, err := tableIter.Next(); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		tableCount++
	}
	if tableCount != 0 {
		t.Fatalf("failed to drop table. table count is %d", tableCount)
	}
}

func TestView(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table"
		viewName    = "view"
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
	schema, err := bigquery.InferSchema(TableSchema{})
	if err != nil {
		t.Fatal(err)
	}
	table := client.Dataset(datasetName).Table(tableName)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Name:           "table",
		Schema:         schema,
		ExpirationTime: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	insertRow := &TableSchema{
		Int:   -1,
		Str:   "2",
		Float: 3,
		Struct: &StructType{
			A: 4,
			B: "5",
			C: 6,
		},
		Array: []*StructType{
			{
				A: 7,
				B: "8",
				C: 9,
			},
		},
		IntArray: []int{10},
		Time:     time.Now(),
	}
	if err := table.Inserter().Put(ctx, []*TableSchema{insertRow}); err != nil {
		t.Fatal(err)
	}

	// view

	view := client.Dataset(datasetName).Table(viewName)

	if err := view.Create(ctx, &bigquery.TableMetadata{
		Name:      viewName,
		ViewQuery: "SELECT ABS(Int) AS Int FROM table",
	}); err != nil {
		t.Fatal(err)
	}

	query := client.Query("SELECT * FROM dataset1.view")

	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type ViewRow struct {
		Int int
	}
	var viewRows []*ViewRow
	for {
		var viewRow ViewRow
		if err := it.Next(&viewRow); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		viewRows = append(viewRows, &viewRow)
	}

	if len(viewRows) != 1 {
		t.Fatalf("failed to get view data. view rows length is %d", len(viewRows))
	}
	if viewRows[0].Int != 1 {
		t.Fatal("unexpected view row data")
	}
}

func TestViewLifecycle(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		viewName    = "v"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject(projectName, types.NewDataset(datasetName)))); err != nil {
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

	view := client.Dataset(datasetName).Table(viewName)
	if err := view.Create(ctx, &bigquery.TableMetadata{
		Name:      viewName,
		ViewQuery: "SELECT 1 AS n, 'x' AS s",
	}); err != nil {
		t.Fatal(err)
	}

	// The view must be marked as a view and carry the schema resolved from
	// its query (hydrated at creation, as real BigQuery does).
	md, err := view.Metadata(ctx)
	if err != nil {
		t.Fatalf("get view metadata: %v", err)
	}
	if md.Type != bigquery.ViewTable {
		t.Errorf("table type = %q, want %q", md.Type, bigquery.ViewTable)
	}
	if len(md.Schema) != 2 {
		t.Errorf("view schema has %d fields, want 2 (schema not hydrated)", len(md.Schema))
	}

	// DROP VIEW must succeed (DROP TABLE does not apply to a view).
	if err := view.Delete(ctx); err != nil {
		t.Fatalf("delete view: %v", err)
	}
	if _, err := view.Metadata(ctx); err == nil {
		t.Error("view metadata still resolves after delete")
	}
}

func TestDDLCreateView(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		viewName    = "ddlview"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject(projectName, types.NewDataset(datasetName)))); err != nil {
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

	// Create a view through a CREATE VIEW DDL statement.
	job, err := client.Query(fmt.Sprintf("CREATE VIEW %s.%s AS SELECT 1 AS n", datasetName, viewName)).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Err() != nil {
		t.Fatal(status.Err())
	}

	// The DDL-created view must be registered in the metadata as a view.
	md, err := client.Dataset(datasetName).Table(viewName).Metadata(ctx)
	if err != nil {
		t.Fatalf("DDL-created view was not registered: %v", err)
	}
	if md.Type != bigquery.ViewTable {
		t.Errorf("table type = %q, want %q", md.Type, bigquery.ViewTable)
	}
}

func TestDuplicateTable(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
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
	table := client.Dataset(datasetName).Table(tableName)
	if err := table.Create(ctx, nil); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := table.Create(ctx, nil); err != nil {
		ge := err.(*googleapi.Error)
		if ge.Code != 409 {
			t.Fatalf("%+v", ge)
		}
	} else {
		t.Fatalf(("Threre should be error, when table name duplicates."))
	}
}

func TestDuplicateTableWithSchema(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
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

	table := client.Dataset(datasetName).Table(tableName)

	schema := bigquery.Schema{
		{Name: "id", Required: true, Type: bigquery.StringFieldType},
		{Name: "data", Required: false, Type: bigquery.StringFieldType},
		{Name: "timestamp", Required: false, Type: bigquery.TimestampFieldType},
	}
	metaData := &bigquery.TableMetadata{Schema: schema}
	if err := table.Create(ctx, metaData); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := table.Create(ctx, metaData); err != nil {
		ge := err.(*googleapi.Error)
		if ge.Code != 409 {
			t.Fatalf("%+v", ge)
		}
	} else {
		t.Fatalf(("Threre should be error, when table name duplicates."))
	}
}

func TestDataFromStruct(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectName,
				types.NewDataset(
					"dataset1",
					types.NewTable(
						"table_a",
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
							types.NewColumn("name", types.STRING),
						},
						types.Data{
							{
								"id":   1,
								"name": "alice",
							},
							{
								"id":   2,
								"name": "bob",
							},
						},
					),
				),
			),
		),
	); err != nil {
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

	query := client.Query("SELECT * FROM dataset1.table_a WHERE id = @id")
	query.QueryConfig.Parameters = []bigquery.QueryParameter{
		{Name: "id", Value: 1},
	}
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		t.Log("row = ", row)
	}
	if err := client.Dataset("dataset1").DeleteWithContents(ctx); err != nil {
		t.Fatal(err)
	}
}

type dataset2Table struct {
	ID    int64
	Name2 string
}

func (t *dataset2Table) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"id":    t.ID,
		"name2": t.Name2,
	}, "", nil
}

func TestMultiDatasets(t *testing.T) {
	ctx := context.Background()

	const (
		projectName  = "test"
		datasetName1 = "dataset1"
		datasetName2 = "dataset2"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectName,
				types.NewDataset(
					"dataset1",
					types.NewTable(
						"a",
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
							types.NewColumn("name1", types.STRING),
						},
						types.Data{
							{
								"id":    1,
								"name1": "alice",
							},
						},
					),
				),
				types.NewDataset(
					"dataset2",
					types.NewTable(
						"a",
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
							types.NewColumn("name2", types.STRING),
						},
						types.Data{
							{
								"id":    1,
								"name2": "bob",
							},
						},
					),
				),
			),
		),
	); err != nil {
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

	{
		query := client.Query("SELECT * FROM `test.dataset1.a` WHERE id = @id")
		query.QueryConfig.Parameters = []bigquery.QueryParameter{
			{Name: "id", Value: 1},
		}
		it, err := query.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			t.Log("row = ", row)
		}
	}
	{
		query := client.Query("SELECT * FROM `test.dataset2.a` WHERE id = @id")
		query.QueryConfig.Parameters = []bigquery.QueryParameter{
			{Name: "id", Value: 1},
		}
		it, err := query.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			t.Log("row = ", row)
		}
	}
	{
		query := client.Query("SELECT name1 FROM `test.dataset2.a` WHERE id = @id")
		query.QueryConfig.Parameters = []bigquery.QueryParameter{
			{Name: "id", Value: 1},
		}
		if _, err := query.Read(ctx); err == nil {
			t.Fatal("expected error")
		}
	}
	if err := client.Dataset(datasetName2).Table("a").Inserter().Put(
		ctx,
		[]*dataset2Table{{ID: 3, Name2: "name3"}},
	); err != nil {
		t.Fatal(err)
	}
	{
		table := client.Dataset(datasetName1).Table("a")
		if table.DatasetID != datasetName1 {
			t.Fatalf("failed to get table")
		}
		if table.TableID != "a" {
			t.Fatalf("failed to get table")
		}
	}
}

func TestRoutine(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset1"
		routineID = "routine1"
	)
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
				),
			),
		),
	); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectID); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

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

	metaData := &bigquery.RoutineMetadata{
		Type:     "SCALAR_FUNCTION",
		Language: "SQL",
		Body:     "x * 3",
		Arguments: []*bigquery.RoutineArgument{
			{Name: "x", DataType: &bigquery.StandardSQLDataType{TypeKind: "INT64"}},
		},
	}

	routineRef := client.Dataset(datasetID).Routine(routineID)
	if err := routineRef.Create(ctx, metaData); err != nil {
		t.Fatalf("%+v", err)
	}

	query := client.Query("SELECT val, dataset1.routine1(val) FROM UNNEST([1, 2, 3, 4]) AS val")
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var (
		rowNum      int
		expectedSrc = []int64{1, 2, 3, 4}
		expectedDst = []int64{3, 6, 9, 12}
	)
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		if len(row) != 2 {
			t.Fatalf("failed to get row. got length %d", len(row))
		}
		src, ok := row[0].(int64)
		if !ok {
			t.Fatalf("failed to get row[0]. type is %T", row[0])
		}
		dst, ok := row[1].(int64)
		if !ok {
			t.Fatalf("failed to get row[1]. type is %T", row[1])
		}
		if expectedSrc[rowNum] != src {
			t.Fatalf("expected value is %d but got %d", expectedSrc[rowNum], src)
		}
		if expectedDst[rowNum] != dst {
			t.Fatalf("expected value is %d but got %d", expectedDst[rowNum], dst)
		}
		rowNum++
	}
}

func TestRoutineWithQuery(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset1"
		routineID = "routine1"
	)
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
				),
			),
		),
	); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectID); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

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
	routineName, err := client.Dataset(datasetID).Routine(routineID).Identifier(bigquery.StandardSQLID)
	if err != nil {
		t.Fatal(err)
	}
	sql := fmt.Sprintf(`
CREATE FUNCTION %s(
  arr ARRAY<STRUCT<name STRING, val INT64>>
) AS (
  (SELECT SUM(IF(elem.name = "foo",elem.val,null)) FROM UNNEST(arr) AS elem)
)`, routineName)
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

	queryText := fmt.Sprintf(`
SELECT %s([
  STRUCT<name STRING, val INT64>("foo", 10),
  STRUCT<name STRING, val INT64>("bar", 40),
  STRUCT<name STRING, val INT64>("foo", 20)
])`, routineName)
	query := client.Query(queryText)
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		if len(row) != 1 {
			t.Fatalf("failed to get row. got length %d", len(row))
		}
		src, ok := row[0].(int64)
		if !ok {
			t.Fatalf("failed to get row[0]. type is %T", row[0])
		}
		if src != 30 {
			t.Fatalf("expected 30 but got %d", src)
		}
	}
}

func TestContentEncoding(t *testing.T) {
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(context.Background())
	}()

	client := new(http.Client)
	b, err := json.Marshal(bigqueryv2.Job{
		Configuration: &bigqueryv2.JobConfiguration{
			Query: &bigqueryv2.JobConfigurationQuery{
				Query: "SELECT 1",
			},
		},
		JobReference: &bigqueryv2.JobReference{},
	})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	defer writer.Close()
	if _, err := writer.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/projects/test/jobs", testServer.URL), &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("Content-Encoding", "gzip")
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("failed to request with gzip: %s", string(body))
	}
}

func TestCreateTempTable(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				"test",
				types.NewDataset("dataset1"),
			),
		),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		"test",
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	{
		job, err := client.Query("CREATE TEMP TABLE dataset1.tmp ( id INT64 )").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := job.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	{
		job, err := client.Query("CREATE TEMP TABLE dataset1.tmp ( id INT64 )").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := job.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	{
		job, err := client.Query("CREATE TABLE dataset1.tmp ( id INT64 )").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := job.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	{
		job, err := client.Query("CREATE TABLE dataset1.tmp ( id INT64 )").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := job.Wait(ctx); err == nil {
			t.Fatal("expected error")
		}
	}
}

type TestTs struct {
	Name       string    `bigquery:"name"`
	ReportTime time.Time `bigquery:"report_time"`
}

func TestTabledataListInt64Timestamp(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
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

	client.Dataset(datasetName).Table(tableName).Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{
				Name: "name",
				Type: "STRING",
			},
			{
				Name: "report_time",
				Type: "TIMESTAMP",
			},
		},
	})
	// Insert data
	testData := []TestTs{
		{
			Name:       "test1",
			ReportTime: time.Now().UTC(),
		},
		{
			Name:       "test2",
			ReportTime: time.Now().UTC(),
		},
	}

	u := client.Dataset(datasetName).Table(tableName).Inserter()
	err = u.Put(ctx, testData)
	if err != nil {
		t.Fatalf("failed to insert rows: %s", err)
	}

	// Load the data
	it := client.Dataset(datasetName).Table(tableName).Read(ctx)
	var tData []TestTs
	for {
		var ts TestTs
		err := it.Next(&ts)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		tData = append(tData, ts)
	}
}

func TestQueryWithTimestampType(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
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

	table := client.Dataset(datasetName).Table(tableName)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: []*bigquery.FieldSchema{
			{Name: "ts", Type: bigquery.TimestampFieldType},
		},
	}); err != nil {
		t.Fatalf("%+v", err)
	}

	query := client.Query("SELECT CURRENT_TIMESTAMP() AS ts")
	query.QueryConfig.Dst = &bigquery.Table{
		ProjectID: projectName,
		DatasetID: datasetName,
		TableID:   table.TableID,
	}
	job, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("%+v", err)
	}
}

func TestLoadJSON(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
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

	table := client.Dataset(datasetName).Table(tableName)
	schema := bigquery.Schema{
		{Name: "ID", Type: bigquery.IntegerFieldType},
		{Name: "Name", Type: bigquery.StringFieldType},
	}

	{
		source := bigquery.NewReaderSource(bytes.NewBufferString(`
{"ID": 1, "Name": "John"}
`,
		))
		source.SourceFormat = bigquery.JSON
		source.Schema = schema

		job, err := table.LoaderFrom(source).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Err() != nil {
			t.Fatal(err)
		}
	}

	{
		source := bigquery.NewReaderSource(bytes.NewBufferString(`
{"ID": 2, "Name": "Joan"}
`,
		))
		source.SourceFormat = bigquery.JSON

		job, err := table.LoaderFrom(source).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Err() != nil {
			t.Fatal(err)
		}
	}

	query := client.Query(fmt.Sprintf("SELECT * FROM %s.%s ORDER BY ID", datasetName, tableName))
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type row struct {
		ID   int
		Name string
	}
	var rows []*row
	for {
		var r row
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &r)
	}
	if diff := cmp.Diff([]*row{
		{ID: 1, Name: "John"},
		{ID: 2, Name: "Joan"},
	}, rows); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

func TestLoadWriteTruncate(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject(projectName, types.NewDataset(datasetName)))); err != nil {
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

	table := client.Dataset(datasetName).Table(tableName)
	schema := bigquery.Schema{{Name: "ID", Type: bigquery.IntegerFieldType}}

	loadJSON := func(disposition bigquery.TableWriteDisposition, body string) {
		t.Helper()
		source := bigquery.NewReaderSource(bytes.NewBufferString(body))
		source.SourceFormat = bigquery.JSON
		source.Schema = schema
		loader := table.LoaderFrom(source)
		loader.WriteDisposition = disposition
		job, err := loader.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Err() != nil {
			t.Fatal(status.Err())
		}
	}

	// Initial load of two rows into the (new) table.
	loadJSON(bigquery.WriteAppend, "{\"ID\": 1}\n{\"ID\": 2}\n")
	// WRITE_TRUNCATE must replace the existing rows, not append to them.
	loadJSON(bigquery.WriteTruncate, "{\"ID\": 9}\n")

	it, err := client.Query(fmt.Sprintf("SELECT ID FROM %s.%s ORDER BY ID", datasetName, tableName)).Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int
	for {
		var r struct{ ID int }
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
	}
	if diff := cmp.Diff([]int{9}, ids); diff != "" {
		t.Errorf("rows after WRITE_TRUNCATE (-want +got):\n%s", diff)
	}
}

func TestLoadCSVAutodetect(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject(projectName, types.NewDataset(datasetName)))); err != nil {
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

	table := client.Dataset(datasetName).Table(tableName)
	source := bigquery.NewReaderSource(bytes.NewBufferString("id,name,score\n1,alice,9.5\n2,bob,8\n"))
	source.SourceFormat = bigquery.CSV
	source.AutoDetect = true
	source.SkipLeadingRows = 1

	job, err := table.LoaderFrom(source).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Err() != nil {
		t.Fatal(status.Err())
	}

	// The schema must be inferred: narrowest type per column.
	md, err := table.Metadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotTypes := map[string]bigquery.FieldType{}
	for _, f := range md.Schema {
		gotTypes[f.Name] = f.Type
	}
	wantTypes := map[string]bigquery.FieldType{
		"id":    bigquery.IntegerFieldType,
		"name":  bigquery.StringFieldType,
		"score": bigquery.FloatFieldType,
	}
	if diff := cmp.Diff(wantTypes, gotTypes); diff != "" {
		t.Errorf("autodetected column types (-want +got):\n%s", diff)
	}

	it, err := client.Query(fmt.Sprintf("SELECT id, name, score FROM %s.%s ORDER BY id", datasetName, tableName)).Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		ID    int
		Name  string
		Score float64
	}
	var rows []row
	for {
		var r row
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	if diff := cmp.Diff([]row{
		{ID: 1, Name: "alice", Score: 9.5},
		{ID: 2, Name: "bob", Score: 8},
	}, rows); diff != "" {
		t.Errorf("autodetected CSV rows (-want +got):\n%s", diff)
	}
}

func TestImportFromGCS(t *testing.T) {
	const (
		projectID  = "test"
		datasetID  = "dataset1"
		tableID    = "table_a"
		publicHost = "127.0.0.1"
		bucketName = "test-bucket"
		sourceName = "path/to/data.json"
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INT64),
					types.NewColumn("value", types.INT64),
				},
				nil,
			),
		),
	)
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := 0; i < 3; i++ {
		if err := enc.Encode(map[string]interface{}{
			"id":    i + 1,
			"value": i + 10,
		}); err != nil {
			t.Fatal(err)
		}
	}
	storageServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: []fakestorage.Object{
			{
				ObjectAttrs: fakestorage.ObjectAttrs{
					BucketName: bucketName,
					Name:       sourceName,
					Size:       int64(len(buf.Bytes())),
				},
				Content: buf.Bytes(),
			},
		},
		PublicHost: publicHost,
		Scheme:     "http",
	})
	if err != nil {
		t.Fatal(err)
	}

	storageServerURL := storageServer.URL()
	u, err := url.Parse(storageServerURL)
	if err != nil {
		t.Fatal(err)
	}
	storageEmulatorHost := fmt.Sprintf("http://%s:%s", publicHost, u.Port())
	t.Setenv("STORAGE_EMULATOR_HOST", storageEmulatorHost)

	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
		storageServer.Stop()
	}()

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

	gcsSourceURL := fmt.Sprintf("gs://%s/%s", bucketName, sourceName)
	gcsRef := bigquery.NewGCSReference(gcsSourceURL)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.AutoDetect = true
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Err() != nil {
		t.Fatal(status.Err())
	}

	query := client.Query(fmt.Sprintf("SELECT * FROM %s.%s", datasetID, tableID))
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type row struct {
		ID    int64
		Value int64
	}
	var rows []*row
	for {
		var r row
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &r)
	}
	if diff := cmp.Diff([]*row{
		{ID: 1, Value: 10},
		{ID: 2, Value: 11},
		{ID: 3, Value: 12},
	}, rows); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

func TestImportFromGCSEmulatorWithoutPublicHost(t *testing.T) {
	const (
		projectID  = "test"
		datasetID  = "dataset1"
		tableID    = "table_a"
		host       = "127.0.0.1"
		bucketName = "test-bucket"
		sourceName = "path/to/data.json"
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INT64),
					types.NewColumn("value", types.INT64),
				},
				nil,
			),
		),
	)
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := 0; i < 3; i++ {
		if err := enc.Encode(map[string]interface{}{
			"id":    i + 1,
			"value": i + 10,
		}); err != nil {
			t.Fatal(err)
		}
	}
	storageServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: []fakestorage.Object{
			{
				ObjectAttrs: fakestorage.ObjectAttrs{
					BucketName: bucketName,
					Name:       sourceName,
					Size:       int64(len(buf.Bytes())),
				},
				Content: buf.Bytes(),
			},
		},
		Host:   host,
		Scheme: "http",
	})
	if err != nil {
		t.Fatal(err)
	}

	storageServerURL := storageServer.URL()
	u, err := url.Parse(storageServerURL)
	if err != nil {
		t.Fatal(err)
	}
	storageEmulatorHost := fmt.Sprintf("http://%s:%s", host, u.Port())
	t.Setenv("STORAGE_EMULATOR_HOST", storageEmulatorHost)

	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
		storageServer.Stop()
	}()

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

	gcsSourceURL := fmt.Sprintf("gs://%s/%s", bucketName, sourceName)
	gcsRef := bigquery.NewGCSReference(gcsSourceURL)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.AutoDetect = true
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Err() != nil {
		t.Fatal(status.Err())
	}

	query := client.Query(fmt.Sprintf("SELECT * FROM %s.%s", datasetID, tableID))
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type row struct {
		ID    int64
		Value int64
	}
	var rows []*row
	for {
		var r row
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &r)
	}
	if diff := cmp.Diff([]*row{
		{ID: 1, Value: 10},
		{ID: 2, Value: 11},
		{ID: 3, Value: 12},
	}, rows); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

func TestImportWithWildcardFromGCS(t *testing.T) {
	const (
		projectID  = "test"
		datasetID  = "dataset1"
		tableID    = "table_a"
		publicHost = "127.0.0.1"
		bucketName = "test-bucket"
		sourceName = "path/to/*.json"
	)

	var (
		targetSourceFiles = []string{
			"path/to/data.json",
			"path/to/under/data.json",
		}
		nonTargetSourceFiles = []string{
			"path/not/data.json",
			"path/to/data.csv",
		}
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INT64),
					types.NewColumn("value", types.INT64),
				},
				nil,
			),
		),
	)
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	files := make([]string, len(targetSourceFiles)+len(nonTargetSourceFiles))
	copy(files, targetSourceFiles)
	copy(files[len(targetSourceFiles):], nonTargetSourceFiles)
	var initialObjects []fakestorage.Object
	for i, file := range files {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for j := 0; j < 3; j++ {
			if err := enc.Encode(map[string]interface{}{
				"id":    i*10 + j + 1,
				"value": (i+1)*10 + j + 1,
			}); err != nil {
				t.Fatal(err)
			}
		}
		initialObjects = append(initialObjects, fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: bucketName,
				Name:       file,
				Size:       int64(len(buf.Bytes())),
			},
			Content: buf.Bytes(),
		})
	}

	storageServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: initialObjects,
		PublicHost:     publicHost,
		Scheme:         "http",
	})
	if err != nil {
		t.Fatal(err)
	}

	storageServerURL := storageServer.URL()
	u, err := url.Parse(storageServerURL)
	if err != nil {
		t.Fatal(err)
	}
	storageEmulatorHost := fmt.Sprintf("http://%s:%s", publicHost, u.Port())
	t.Setenv("STORAGE_EMULATOR_HOST", storageEmulatorHost)

	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
		storageServer.Stop()
	}()

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

	gcsSourceURL := fmt.Sprintf("gs://%s/%s", bucketName, sourceName)
	gcsRef := bigquery.NewGCSReference(gcsSourceURL)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.AutoDetect = true
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Err() != nil {
		t.Fatal(status.Err())
	}

	query := client.Query(fmt.Sprintf("SELECT * FROM %s.%s", datasetID, tableID))
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type row struct {
		ID    int64
		Value int64
	}
	var rows []*row
	for {
		var r row
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &r)
	}
	if diff := cmp.Diff([]*row{
		{ID: 1, Value: 11},
		{ID: 2, Value: 12},
		{ID: 3, Value: 13},
		{ID: 11, Value: 21},
		{ID: 12, Value: 22},
		{ID: 13, Value: 23},
	}, rows); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

func TestExportToGCS(t *testing.T) {
	const (
		projectID  = "test"
		datasetID  = "dataset1"
		tableID    = "table_a"
		publicHost = "127.0.0.1"
		bucketName = "test-export-bucket"
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INT64),
					types.NewColumn("value", types.INT64),
				},
				types.Data{
					{"id": int64(1), "value": int64(21)},
					{"id": int64(2), "value": int64(22)},
					{"id": int64(3), "value": int64(23)},
				},
			),
		),
	)
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	storageServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		PublicHost: publicHost,
		Scheme:     "http",
	})
	if err != nil {
		t.Fatal(err)
	}

	storageServerURL := storageServer.URL()
	u, err := url.Parse(storageServerURL)
	if err != nil {
		t.Fatal(err)
	}
	storageEmulatorHost := fmt.Sprintf("http://%s:%s", publicHost, u.Port())
	t.Setenv("STORAGE_EMULATOR_HOST", storageEmulatorHost)

	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
		storageServer.Stop()
	}()

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

	type T struct {
		ID    int64
		Value int64
	}

	storageClient, err := storage.NewClient(
		ctx,
		option.WithEndpoint(fmt.Sprintf("%s/storage/v1/", storageEmulatorHost)),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("csv", func(t *testing.T) {
		sourceName := "path/to/data.csv"
		gcsSourceURL := fmt.Sprintf("gs://%s/%s", bucketName, sourceName)
		gcsRef := bigquery.NewGCSReference(gcsSourceURL)
		gcsRef.DestinationFormat = bigquery.CSV
		gcsRef.FieldDelimiter = ","
		extractor := client.DatasetInProject(projectID, datasetID).Table(tableID).ExtractorTo(gcsRef)
		extractor.DisableHeader = true
		job, err := extractor.Run(ctx)
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
		reader, err := storageClient.Bucket(bucketName).Object(sourceName).NewReader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		records, err := csv.NewReader(reader).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		var rows []*T
		for _, record := range records {
			id, err := strconv.ParseInt(record[0], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			value, err := strconv.ParseInt(record[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, &T{ID: id, Value: value})
		}
		if diff := cmp.Diff([]*T{
			{ID: 1, Value: 21},
			{ID: 2, Value: 22},
			{ID: 3, Value: 23},
		}, rows); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})

	t.Run("json", func(t *testing.T) {
		sourceName := "path/to/data.json"
		gcsSourceURL := fmt.Sprintf("gs://%s/%s", bucketName, sourceName)
		gcsRef := bigquery.NewGCSReference(gcsSourceURL)
		gcsRef.DestinationFormat = bigquery.JSON
		extractor := client.DatasetInProject(projectID, datasetID).Table(tableID).ExtractorTo(gcsRef)
		job, err := extractor.Run(ctx)
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
		reader, err := storageClient.Bucket(bucketName).Object(sourceName).NewReader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		dec := json.NewDecoder(reader)
		var rows []*T
		for dec.More() {
			var v struct {
				ID    json.Number
				Value json.Number
			}
			if err := dec.Decode(&v); err != nil {
				t.Fatal(err)
			}
			id, err := v.ID.Int64()
			if err != nil {
				t.Fatal(err)
			}
			value, err := v.Value.Int64()
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, &T{
				ID:    id,
				Value: value,
			})
		}
		if diff := cmp.Diff([]*T{
			{ID: 1, Value: 21},
			{ID: 2, Value: 22},
			{ID: 3, Value: 23},
		}, rows); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
}

func TestQueryWithNamedParams(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "test_dataset"
		tableID   = "test_table"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("created_at", types.TIMESTAMP),
					types.NewColumn("item", types.STRING),
					types.NewColumn("qty", types.NUMERIC),
				},
				types.Data{
					{
						"created_at": time.Now(),
						"item":       "something",
						"qty":        "123.45",
					},
				},
			),
		),
	)
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Close()
	}()

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
	query := client.Query(`
SELECT
 item, qty
FROM test_dataset.test_table
WHERE
 created_at > @someday AND
 qty >= @min_qty AND
 created_at > @someday - INTERVAL 1 DAY
ORDER BY qty DESC;`)
	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "min_qty",
			Value: 100,
		},
		{
			Name:  "someday",
			Value: time.Date(2022, 9, 9, 0, 0, 0, 0, time.UTC),
		},
	}

	job, err := query.Run(ctx)
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
	it, err := job.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var rowCount int
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		rowCount++
		t.Log(row)
	}
	if rowCount != 1 {
		t.Fatal("failed to get result")
	}
}

func TestQueryWithNullParams(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Close()
	}()

	client, err := bigquery.NewClient(
		ctx,
		"test",
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// A typed NULL query parameter must reach the query as a real NULL,
	// not as a coerced zero value or an empty string.
	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{"int64", bigquery.NullInt64{}},
		{"float64", bigquery.NullFloat64{}},
		{"bool", bigquery.NullBool{}},
		{"timestamp", bigquery.NullTimestamp{}},
		{"string", bigquery.NullString{}},
		{"date", bigquery.NullDate{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := client.Query("SELECT @p AS v")
			query.Parameters = []bigquery.QueryParameter{{Name: "p", Value: tc.value}}
			it, err := query.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				t.Fatal(err)
			}
			if len(row) != 1 {
				t.Fatalf("got %d columns, want 1", len(row))
			}
			if row[0] != nil {
				t.Errorf("NULL %s parameter: got %v, want nil", tc.name, row[0])
			}
		})
	}
}

func TestMultipleProject(t *testing.T) {
	const (
		mainProjectID = "main_project"
		mainDatasetID = "main_dataset"
		mainTableID   = "main_table"
		subProjectID  = "sub_project"
		subDatasetID  = "sub_dataset"
		subTableID    = "sub_table"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				mainProjectID,
				types.NewDataset(
					mainDatasetID,
					types.NewTable(
						mainTableID,
						[]*types.Column{
							types.NewColumn("name", types.STRING),
						},
						types.Data{
							{"name": "main-project-name-data"},
						},
					),
				),
			),
		),
		server.StructSource(
			types.NewProject(
				subProjectID,
				types.NewDataset(
					subDatasetID,
					types.NewTable(
						subTableID,
						[]*types.Column{
							types.NewColumn("name", types.STRING),
						},
						types.Data{
							{"name": "sub-project-name-data"},
						},
					),
				),
			),
		),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		mainProjectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	it, err := client.Query("SELECT * FROM sub_project.sub_dataset.sub_table").Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		if err != iterator.Done {
			t.Fatal(err)
		}
	}
	if len(row) != 1 {
		t.Fatalf("failed to get row. got length %d", len(row))
	}
	name, ok := row[0].(string)
	if !ok {
		t.Fatalf("failed to get row[0]. type is %T", row[0])
	}
	if name != "sub-project-name-data" {
		t.Fatalf("failed to get data from sub project: %s", name)
	}
}

func TestListProjects(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(types.NewProject("project1")),
		server.StructSource(types.NewProject("project2")),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	httpClient := &http.Client{}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/bigquery/v2/projects", testServer.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		t.Fatalf("expected status 200, got %d: %s", res.StatusCode, string(body))
	}

	var projectList bigqueryv2.ProjectList
	if err := json.NewDecoder(res.Body).Decode(&projectList); err != nil {
		t.Fatal(err)
	}

	if len(projectList.Projects) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(projectList.Projects))
	}

	for _, p := range projectList.Projects {
		if p.Id == "" {
			t.Error("Id should not be empty")
		}
		if p.NumericId == 0 {
			t.Error("NumericId should not be zero")
		}
		if p.FriendlyName == "" {
			t.Error("FriendlyName should not be empty")
		}
	}
}

// TestInformationSchema exercises the genuine INFORMATION_SCHEMA
// virtual catalog that googlesqlite ships (formerly we registered
// a manual fake `INFORMATION_SCHEMA.COLUMNS` table here as a
// workaround). The driver synthesises rows on demand from the live
// catalog, so registering a regular user table is enough — its
// columns appear automatically in INFORMATION_SCHEMA.COLUMNS.
func TestInformationSchema(t *testing.T) {
	const (
		projectID    = "test"
		datasetID    = "test_dataset"
		subProjectID = "sub"
		tableID      = "users"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
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
							{"id": 1, "name": "alice"},
						},
					),
				),
			),
			types.NewProject(subProjectID),
		),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	scanColumnNames := func(t *testing.T, q string, projectForClient string) []string {
		t.Helper()
		client, err := bigquery.NewClient(
			ctx,
			projectForClient,
			option.WithEndpoint(testServer.URL),
			option.WithoutAuthentication(),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		it, err := client.Query(q).Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			if len(row) == 0 {
				t.Fatalf("empty row from INFORMATION_SCHEMA.COLUMNS")
			}
			s, ok := row[0].(string)
			if !ok {
				t.Fatalf("expected first column STRING, got %T", row[0])
			}
			names = append(names, s)
		}
		return names
	}

	t.Run("query from same project", func(t *testing.T) {
		got := scanColumnNames(t,
			`SELECT column_name FROM test_dataset.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'users' ORDER BY ordinal_position`,
			projectID,
		)
		want := []string{"id", "name"}
		if len(got) != len(want) {
			t.Fatalf("got %d columns want %d: %v", len(got), len(want), got)
		}
		for i, w := range want {
			if got[i] != w {
				t.Errorf("column[%d]: got %q want %q", i, got[i], w)
			}
		}
	})

	t.Run("query from sub project", func(t *testing.T) {
		got := scanColumnNames(t,
			`SELECT column_name FROM test.test_dataset.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'users' ORDER BY ordinal_position`,
			subProjectID,
		)
		want := []string{"id", "name"}
		if len(got) != len(want) {
			t.Fatalf("got %d columns want %d: %v", len(got), len(want), got)
		}
		for i, w := range want {
			if got[i] != w {
				t.Errorf("column[%d]: got %q want %q", i, got[i], w)
			}
		}
	})
}

// rawRowSaver is a bigquery.ValueSaver that emits an arbitrary row map,
// including fields that may be absent from the destination table schema.
type rawRowSaver struct {
	row map[string]bigquery.Value
}

func (s rawRowSaver) Save() (map[string]bigquery.Value, string, error) {
	return s.row, "", nil
}

func TestInsertAllUnknownField(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "tbl"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject(projectName, types.NewDataset(datasetName)))); err != nil {
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

	table := client.Dataset(datasetName).Table(tableName)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Name: tableName,
		Schema: bigquery.Schema{
			{Name: "id", Type: bigquery.IntegerFieldType},
		},
	}); err != nil {
		t.Fatal(err)
	}
	inserter := table.Inserter()

	// A row whose fields are all declared inserts cleanly.
	if err := inserter.Put(ctx, rawRowSaver{row: map[string]bigquery.Value{"id": 1}}); err != nil {
		t.Fatalf("valid row was rejected: %v", err)
	}

	// A row carrying a field absent from the schema must be reported, not
	// silently accepted.
	if err := inserter.Put(ctx, rawRowSaver{row: map[string]bigquery.Value{"id": 2, "bogus": 3}}); err == nil {
		t.Fatal("insertAll accepted a row with an unknown field; want an error")
	}
}

func TestConcurrentQueries(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		"test",
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Many clients querying the emulator at once must not leak or wedge the
	// SQL connection pool (no "statement is closed", no hang).
	const (
		workers   = 16
		perWorker = 5
	)
	var wg sync.WaitGroup
	errCh := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				want := worker*1000 + i
				it, err := client.Query(fmt.Sprintf("SELECT %d AS n", want)).Read(ctx)
				if err != nil {
					errCh <- fmt.Errorf("worker %d query %d: %w", worker, i, err)
					continue
				}
				var row []bigquery.Value
				if err := it.Next(&row); err != nil {
					errCh <- fmt.Errorf("worker %d query %d read: %w", worker, i, err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// httpJSON issues a request with an optional body and decodes the JSON
// response. It is used to exercise REST-level behavior the typed client hides.
func httpJSON(t *testing.T, method, target, body string, headers map[string]string) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, target, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("%s %s: cannot decode response (status %d): %s", method, target, resp.StatusCode, data)
		}
	}
	return resp.StatusCode, out
}

// queryRows extracts the cell values of every row in a jobs.query response.
func queryRows(t *testing.T, body map[string]any) [][]string {
	t.Helper()
	rowsRaw, _ := body["rows"].([]any)
	rows := make([][]string, 0, len(rowsRaw))
	for _, r := range rowsRaw {
		fields, _ := r.(map[string]any)["f"].([]any)
		vals := make([]string, 0, len(fields))
		for _, f := range fields {
			v := f.(map[string]any)["v"]
			if v == nil {
				vals = append(vals, "<nil>")
				continue
			}
			vals = append(vals, fmt.Sprint(v))
		}
		rows = append(rows, vals)
	}
	return rows
}

type valueRow map[string]bigquery.Value

func (r valueRow) Save() (map[string]bigquery.Value, string, error) { return r, "", nil }

// TestPersistedDatabaseRestart covers #237/#397 (re-running the binary against
// a persisted database must not fail with a UNIQUE constraint violation) and
// #207 (datasets and their data must survive a restart).
func TestPersistedDatabaseRestart(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "t1"
	)
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "emulator.db")
	storage := server.Storage(fmt.Sprintf("file:%s?cache=shared", dbFile))

	// startRun mirrors what the CLI does on boot: open the database, register
	// the project, and load the --dataset-equivalent source.
	startRun := func() (*server.Server, *server.TestServer, *bigquery.Client) {
		t.Helper()
		s, err := server.New(storage)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.SetProject(projectID); err != nil {
			t.Fatal(err)
		}
		if err := s.Load(server.StructSource(types.NewProject(projectID, types.NewDataset(datasetID)))); err != nil {
			t.Fatalf("loading the persisted project must be idempotent: %+v", err)
		}
		ts := s.TestServer()
		c, err := bigquery.NewClient(ctx, projectID,
			option.WithEndpoint(ts.URL), option.WithoutAuthentication())
		if err != nil {
			t.Fatal(err)
		}
		return s, ts, c
	}

	// First run: create a table and stream three rows.
	s1, ts1, c1 := startRun()
	table := c1.Dataset(datasetID).Table(tableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{{Name: "x", Type: bigquery.IntegerFieldType}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := table.Inserter().Put(ctx, []valueRow{{"x": 1}, {"x": 2}, {"x": 3}}); err != nil {
		t.Fatal(err)
	}
	c1.Close()
	ts1.Close()
	if err := s1.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	// Second run against the same database file.
	s2, ts2, c2 := startRun()
	defer func() {
		c2.Close()
		ts2.Close()
		s2.Stop(ctx)
	}()

	// #207: the dataset is still present in the REST catalog.
	datasetIter := c2.Datasets(ctx)
	var datasetIDs []string
	for {
		ds, err := datasetIter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		datasetIDs = append(datasetIDs, ds.DatasetID)
	}
	if len(datasetIDs) != 1 || datasetIDs[0] != datasetID {
		t.Fatalf("dataset did not survive restart: got %v", datasetIDs)
	}

	// The table and its rows survive too.
	it, err := c2.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", datasetID, tableID)).Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(row[0]); got != "3" {
		t.Fatalf("expected 3 rows to survive restart, got %s", got)
	}
}

// TestTableNumRows covers #339/#249: tables.get must populate numRows.
func TestTableNumRows(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "t1"
	)
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(
		types.NewProject(projectID, types.NewDataset(datasetID)),
	)); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()
	client, err := bigquery.NewClient(ctx, projectID,
		option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	table := client.Dataset(datasetID).Table(tableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{{Name: "x", Type: bigquery.IntegerFieldType}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := table.Inserter().Put(ctx, []valueRow{{"x": 1}, {"x": 2}, {"x": 3}}); err != nil {
		t.Fatal(err)
	}
	md, err := table.Metadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if md.NumRows != 3 {
		t.Fatalf("expected numRows=3, got %d", md.NumRows)
	}
}

// TestTableUpdateHandlers covers #293/#363 (tables.patch must apply the body
// and return a full Table resource), #362 (the X-HTTP-Method-Override header
// must be honored) and #412 (tables.update must be implemented).
func TestTableUpdateHandlers(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "t1"
	)
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(
		types.NewProject(projectID, types.NewDataset(datasetID)),
	)); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()
	client, err := bigquery.NewClient(ctx, projectID,
		option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Dataset(datasetID).Table(tableID).Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{{Name: "x", Type: bigquery.IntegerFieldType}},
	}); err != nil {
		t.Fatal(err)
	}

	tableURL := fmt.Sprintf("%s/projects/%s/datasets/%s/tables/%s",
		testServer.URL, projectID, datasetID, tableID)

	// #293/#363: PATCH applies the body and returns a full Table resource.
	code, body := httpJSON(t, http.MethodPatch, tableURL, `{"description":"patched"}`, nil)
	if code != http.StatusOK {
		t.Fatalf("patch: expected 200, got %d (%v)", code, body)
	}
	for _, key := range []string{"kind", "type", "id", "creationTime"} {
		if body[key] == nil || body[key] == "" {
			t.Errorf("patch response is missing %q: %v", key, body)
		}
	}
	if body["description"] != "patched" {
		t.Errorf("patch did not apply description: %v", body["description"])
	}
	// The change is persisted, not merely echoed.
	if _, got := httpJSON(t, http.MethodGet, tableURL, "", nil); got["description"] != "patched" {
		t.Errorf("patched description was not persisted: %v", got["description"])
	}

	// #362: a POST tunneling PATCH via X-HTTP-Method-Override is routed to the
	// patch handler.
	code, body = httpJSON(t, http.MethodPost, tableURL, `{"friendlyName":"viaOverride"}`,
		map[string]string{"X-HTTP-Method-Override": "PATCH"})
	if code != http.StatusOK {
		t.Fatalf("method override: expected 200, got %d (%v)", code, body)
	}
	if body["friendlyName"] != "viaOverride" {
		t.Errorf("method override patch did not apply: %v", body["friendlyName"])
	}

	// #412: tables.update (PUT) replaces the resource instead of returning
	// "unsupported".
	put := `{"tableReference":{"projectId":"test","datasetId":"dataset1","tableId":"t1"},` +
		`"schema":{"fields":[{"name":"x","type":"INTEGER"}]},"friendlyName":"viaPut"}`
	code, body = httpJSON(t, http.MethodPut, tableURL, put, nil)
	if code != http.StatusOK {
		t.Fatalf("update: expected 200, got %d (%v)", code, body)
	}
	if body["friendlyName"] != "viaPut" {
		t.Errorf("update did not apply friendlyName: %v", body["friendlyName"])
	}
}

// TestJSONColumnInsertAll covers #144: a JSON column must accept both a
// json-encoded string and a native JSON object through tabledata.insertAll.
func TestJSONColumnInsertAll(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "jt"
	)
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(
		types.NewProject(projectID, types.NewDataset(datasetID)),
	)); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()
	base := fmt.Sprintf("%s/projects/%s/datasets/%s", testServer.URL, projectID, datasetID)

	if code, body := httpJSON(t, http.MethodPost, base+"/tables",
		`{"tableReference":{"projectId":"test","datasetId":"dataset1","tableId":"jt"},`+
			`"schema":{"fields":[{"name":"id","type":"INTEGER"},{"name":"data","type":"JSON"}]}}`,
		nil); code != http.StatusOK {
		t.Fatalf("create table: %d (%v)", code, body)
	}
	// A json-encoded string value.
	if code, body := httpJSON(t, http.MethodPost, base+"/tables/jt/insertAll",
		`{"rows":[{"json":{"id":"1","data":"{\"name\":\"Alice\"}"}}]}`, nil); code != http.StatusOK {
		t.Fatalf("insert json string: %d (%v)", code, body)
	}
	// A native JSON object value (previously panicked).
	if code, body := httpJSON(t, http.MethodPost, base+"/tables/jt/insertAll",
		`{"rows":[{"json":{"id":"2","data":{"name":"Bob"}}}]}`, nil); code != http.StatusOK {
		t.Fatalf("insert json object: %d (%v)", code, body)
	}

	code, body := httpJSON(t, http.MethodPost,
		fmt.Sprintf("%s/projects/%s/queries", testServer.URL, projectID),
		`{"query":"SELECT id, JSON_VALUE(data.name) FROM dataset1.jt ORDER BY id","useLegacySql":false}`,
		nil)
	if code != http.StatusOK {
		t.Fatalf("query: %d (%v)", code, body)
	}
	got := queryRows(t, body)
	want := [][]string{{"1", "Alice"}, {"2", "Bob"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("JSON column round-trip mismatch: got %v want %v", got, want)
	}
}

// multipartUpload builds a multipart/related body for the jobs upload endpoint.
func multipartUpload(jobJSON, data string) (contentType, bodyText string) {
	const boundary = "BQEMUTESTBOUNDARY"
	var b strings.Builder
	b.WriteString("--" + boundary + "\r\nContent-Type: application/json\r\n\r\n")
	b.WriteString(jobJSON + "\r\n")
	b.WriteString("--" + boundary + "\r\nContent-Type: application/octet-stream\r\n\r\n")
	b.WriteString(data + "\r\n")
	b.WriteString("--" + boundary + "--\r\n")
	return "multipart/related; boundary=" + boundary, b.String()
}

// TestUploadWithoutJobReference covers #136: a multipart upload whose metadata
// omits jobReference (as the Node.js client sends) must not panic.
func TestUploadWithoutJobReference(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "dataset1"
	)
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(
		types.NewProject(projectID, types.NewDataset(datasetID)),
	)); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	jobJSON := `{"configuration":{"load":{"sourceFormat":"CSV","autodetect":true,` +
		`"destinationTable":{"projectId":"test","datasetId":"dataset1","tableId":"uploaded"}}}}`
	contentType, body := multipartUpload(jobJSON, "x,y\r\n1,a\r\n2,b")
	code, resp := httpJSON(t, http.MethodPost,
		testServer.URL+"/upload/bigquery/v2/projects/test/jobs?uploadType=multipart",
		body, map[string]string{"Content-Type": contentType})
	if code != http.StatusOK {
		t.Fatalf("upload without jobReference: expected 200, got %d (%v)", code, resp)
	}

	code, resp = httpJSON(t, http.MethodPost,
		testServer.URL+"/projects/test/queries",
		`{"query":"SELECT COUNT(*) FROM dataset1.uploaded","useLegacySql":false}`, nil)
	if code != http.StatusOK {
		t.Fatalf("query uploaded table: %d (%v)", code, resp)
	}
	if rows := queryRows(t, resp); len(rows) != 1 || rows[0][0] != "2" {
		t.Fatalf("expected 2 uploaded rows, got %v", rows)
	}
}

// TestUploadToMissingDataset covers #396: uploading to a non-existent dataset
// must return a clean error instead of panicking the process.
func TestUploadToMissingDataset(t *testing.T) {
	const projectID = "test"
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(
		types.NewProject(projectID, types.NewDataset("dataset1")),
	)); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	jobJSON := `{"configuration":{"load":{"sourceFormat":"CSV","autodetect":true,` +
		`"destinationTable":{"projectId":"test","datasetId":"missing","tableId":"t"}}}}`
	contentType, body := multipartUpload(jobJSON, "x\r\n1")
	code, resp := httpJSON(t, http.MethodPost,
		testServer.URL+"/upload/bigquery/v2/projects/test/jobs?uploadType=multipart",
		body, map[string]string{"Content-Type": contentType})
	if code == http.StatusOK {
		t.Fatalf("upload to a missing dataset should fail, got 200")
	}
	if resp["error"] == nil {
		t.Fatalf("expected a structured error, got %v", resp)
	}

	// The server must still be responsive (it did not crash).
	if c, _ := httpJSON(t, http.MethodGet,
		testServer.URL+"/projects/test/datasets", "", nil); c != http.StatusOK {
		t.Fatalf("server is not responsive after a failed upload: %d", c)
	}
}

// TestExternalTable covers #392: an external table must be registered with the
// query engine so that it can be queried after creation.
func TestExternalTable(t *testing.T) {
	const (
		projectID  = "test"
		datasetID  = "dataset1"
		tableID    = "ext1"
		bucketName = "ext-bucket"
		objectName = "people.csv"
		publicHost = "127.0.0.1"
	)
	ctx := context.Background()

	storageServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: []fakestorage.Object{
			{
				ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucketName, Name: objectName},
				Content:     []byte("id,name\n1,Alice\n2,Bob\n3,Carol\n"),
			},
		},
		PublicHost: publicHost,
		Scheme:     "http",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer storageServer.Stop()
	u, err := url.Parse(storageServer.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("STORAGE_EMULATOR_HOST", fmt.Sprintf("http://%s:%s", publicHost, u.Port()))

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(
		types.NewProject(projectID, types.NewDataset(datasetID)),
	)); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()
	client, err := bigquery.NewClient(ctx, projectID,
		option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	table := client.Dataset(datasetID).Table(tableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		ExternalDataConfig: &bigquery.ExternalDataConfig{
			SourceFormat: bigquery.CSV,
			SourceURIs:   []string{fmt.Sprintf("gs://%s/%s", bucketName, objectName)},
			AutoDetect:   true,
		},
	}); err != nil {
		t.Fatalf("create external table: %+v", err)
	}

	it, err := client.Query(
		fmt.Sprintf("SELECT id, name FROM %s.%s ORDER BY id", datasetID, tableID),
	).Read(ctx)
	if err != nil {
		t.Fatalf("query external table: %+v", err)
	}
	var got [][]bigquery.Value
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 rows from the external table, got %d: %v", len(got), got)
	}

	md, err := table.Metadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if md.ExternalDataConfig == nil {
		t.Fatal("external data configuration did not round-trip on tables.get")
	}
}

// TestServeListenCallback covers #333: the bound listener addresses must be
// reported to the caller (so a requested port of 0 resolves to the real
// port). The library reports them through SetListenCallback instead of
// writing to stdout.
func TestServeListenCallback(t *testing.T) {
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject("test"); err != nil {
		t.Fatal(err)
	}

	addrCh := make(chan [2]string, 1)
	bqServer.SetListenCallback(func(httpAddr, grpcAddr string) {
		addrCh <- [2]string{httpAddr, grpcAddr}
	})
	go func() { _ = bqServer.Serve(ctx, "127.0.0.1:0", "127.0.0.1:0") }()
	defer bqServer.Stop(ctx)

	var addrs [2]string
	select {
	case addrs = <-addrCh:
	case <-time.After(10 * time.Second):
		t.Fatal("listen callback was not invoked")
	}
	for i, name := range []string{"http", "grpc"} {
		if addrs[i] == "" || strings.HasSuffix(addrs[i], ":0") {
			t.Errorf("%s callback reported an unbound address: %q", name, addrs[i])
			continue
		}
		conn, err := net.Dial("tcp", addrs[i])
		if err != nil {
			t.Errorf("%s address %q is not connectable: %v", name, addrs[i], err)
			continue
		}
		conn.Close()
	}
}

// TestDeleteDatasetCascadeRequiresDeleteContents is a regression test for
// https://github.com/goccy/bigquery-emulator/issues/341 and the related #161:
// deleting a non-empty dataset without `deleteContents=true` used to remove
// the dataset row while leaving its tables behind (orphaned, and unable to be
// recreated under the same name without a UNIQUE constraint violation). The
// failure was additionally returned as a 500, which the Google SDKs retry
// indefinitely, so the test pipeline deadlocked instead of failing fast.
//
// Real BigQuery rejects the delete with a 400 / resourceInUse; the emulator
// must too, and a subsequent DeleteWithContents must actually cascade.
func TestDeleteDatasetCascadeRequiresDeleteContents(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.MemoryStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject("test"); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()
	client, err := bigquery.NewClient(ctx, "test",
		option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ds := client.Dataset("ds341")
	if err := ds.Create(ctx, nil); err != nil {
		t.Fatal(err)
	}
	tbl := ds.Table("t")
	schema := bigquery.Schema{{Name: "n", Type: bigquery.IntegerFieldType}}
	if err := tbl.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		t.Fatal(err)
	}

	// Plain Delete on a non-empty dataset must fail with a 4xx (not a
	// 5xx — the latter causes the Google SDK to retry indefinitely).
	err = ds.Delete(ctx)
	if err == nil {
		t.Fatal("Delete of a non-empty dataset without DeleteContents should fail")
	}
	apiErr, ok := err.(*googleapi.Error)
	if !ok {
		t.Fatalf("expected *googleapi.Error, got %T: %v", err, err)
	}
	if apiErr.Code/100 != 4 {
		t.Fatalf("expected a 4xx status, got %d: %v", apiErr.Code, err)
	}

	// Both the dataset and the table must still exist — the rejected
	// delete must not have partially removed state.
	if _, err := tbl.Metadata(ctx); err != nil {
		t.Fatalf("table should still exist after a rejected delete: %v", err)
	}

	// DeleteWithContents cascades and succeeds.
	if err := ds.DeleteWithContents(ctx); err != nil {
		t.Fatalf("DeleteWithContents: %v", err)
	}

	// Re-creating the dataset and table with the same names must
	// succeed — proof that the previous delete cleared both the
	// dataset metadata and its backing tables (the orphan-table bug
	// surfaced as a UNIQUE constraint violation on table recreate).
	if err := ds.Create(ctx, nil); err != nil {
		t.Fatalf("recreate dataset after DeleteWithContents: %v", err)
	}
	if err := tbl.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		t.Fatalf("recreate table after DeleteWithContents: %v", err)
	}
}

// TestQueryWithDestinationCreateDisposition is a regression test for
// https://github.com/goccy/bigquery-emulator/issues/360: a query with an
// explicit destination table must honour CreateDisposition. CREATE_IF_NEEDED
// (and the default) materializes a missing table; CREATE_NEVER must reject a
// missing destination with a 404 (matching real BigQuery and the existing
// load-job behaviour) rather than silently materializing it.
func TestQueryWithDestinationCreateDisposition(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
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
	client, err := bigquery.NewClient(ctx, "test",
		option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	runWithDispositionInto := func(t *testing.T, tableID string, disp bigquery.TableCreateDisposition) error {
		t.Helper()
		q := client.Query("SELECT id FROM dataset1.table_a")
		q.QueryConfig.Dst = &bigquery.Table{
			ProjectID: "test", DatasetID: "dataset1", TableID: tableID,
		}
		q.QueryConfig.CreateDisposition = disp
		job, err := q.Run(ctx)
		if err != nil {
			return err
		}
		_, err = job.Wait(ctx)
		return err
	}

	t.Run("CREATE_IF_NEEDED creates a missing destination", func(t *testing.T) {
		const dest = "q360_if_needed_dest"
		if err := runWithDispositionInto(t, dest, bigquery.CreateIfNeeded); err != nil {
			t.Fatalf("query: %v", err)
		}
		if _, err := client.Dataset("dataset1").Table(dest).Metadata(ctx); err != nil {
			t.Fatalf("destination table was not created: %v", err)
		}
	})

	t.Run("CREATE_NEVER rejects a missing destination", func(t *testing.T) {
		const dest = "q360_never_missing_dest"
		err := runWithDispositionInto(t, dest, bigquery.CreateNever)
		if err == nil {
			t.Fatal("expected an error when querying into a missing table with CREATE_NEVER")
		}
		apiErr, ok := err.(*googleapi.Error)
		if !ok {
			t.Fatalf("expected *googleapi.Error, got %T: %v", err, err)
		}
		if apiErr.Code != 404 {
			t.Fatalf("expected 404, got %d: %v", apiErr.Code, err)
		}
		// And the missing table must NOT have been created as a side effect.
		if _, err := client.Dataset("dataset1").Table(dest).Metadata(ctx); err == nil {
			t.Fatal("CREATE_NEVER silently materialized the destination table")
		}
	})
}

// TestIssue468CountStarOverEmptyTable is a regression test for
// https://github.com/goccy/bigquery-emulator/issues/468: COUNT(*) over
// an empty table returned NULL instead of the INT64 0, breaking every
// client that scans the column into a non-pointer integer.
//
// COUNT is non-nullable per the GoogleSQL spec; SUM over an empty input
// is legitimately NULL and must stay that way.
func TestIssue468CountStarOverEmptyTable(t *testing.T) {
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(ctx, "test",
		option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ds := client.Dataset("ds468")
	if err := ds.Create(ctx, nil); err != nil {
		t.Fatal(err)
	}
	empty := ds.Table("empty_tbl")
	schema := bigquery.Schema{{Name: "n", Type: bigquery.IntegerFieldType}}
	if err := empty.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		t.Fatal(err)
	}

	// Every COUNT/COUNTIF form over an empty table must scan into a
	// plain int64 as 0 — issue #468 covers the whole family, not only
	// COUNT(*).
	for _, q := range []string{
		"SELECT COUNT(*) AS n FROM ds468.empty_tbl",
		"SELECT COUNT(n) AS n FROM ds468.empty_tbl",
		"SELECT COUNTIF(n > 0) AS n FROM ds468.empty_tbl",
		"SELECT COUNT(*) AS n FROM ds468.empty_tbl WHERE FALSE",
	} {
		t.Run("scan into int64: "+q, func(t *testing.T) {
			it, err := client.Query(q).Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var row struct{ N int64 }
			if err := it.Next(&row); err != nil {
				t.Fatalf("Next: %v", err)
			}
			if row.N != 0 {
				t.Fatalf("%q over empty table = %d; want 0", q, row.N)
			}
		})
	}

	t.Run("COUNT(*) reports a non-NULL value", func(t *testing.T) {
		it, err := client.Query("SELECT COUNT(*) AS n FROM ds468.empty_tbl").Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row struct{ N bigquery.NullInt64 }
		if err := it.Next(&row); err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !row.N.Valid {
			t.Fatal("COUNT(*) over empty table returned NULL; want INT64 0")
		}
		if row.N.Int64 != 0 {
			t.Fatalf("COUNT(*) over empty table = %d; want 0", row.N.Int64)
		}
	})

	t.Run("SUM over empty table stays NULL", func(t *testing.T) {
		it, err := client.Query("SELECT SUM(n) AS n FROM ds468.empty_tbl").Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row struct{ N bigquery.NullInt64 }
		if err := it.Next(&row); err != nil {
			t.Fatalf("Next: %v", err)
		}
		if row.N.Valid {
			t.Fatalf("SUM over empty table = %d; want NULL", row.N.Int64)
		}
	})
}

// TestInUnnestArrayParam is a regression test for a bug introduced in v0.7.0
// where named ARRAY parameters were silently discarded before reaching the
// query engine.  applyNullQueryParameters checked parameterValue.value == nil
// to detect NULL scalars, but array parameters carry their data in
// parameterValue.arrayValues — they never have a value field — so every array
// parameter was incorrectly cleared to nil, causing IN UNNEST(@param) to
// match nothing.  The fix skips clearing when arrayValues or structValues are
// present in the raw JSON.
func TestInUnnestArrayParam(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "people"
	)
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("persona_id", types.STRING),
					types.NewColumn("status", types.STRING),
				},
				types.Data{
					{"persona_id": "persona-aaa", "status": "active"},
					{"persona_id": "persona-bbb", "status": "suspended"},
					{"persona_id": "persona-ccc", "status": "active"},
				},
			),
		),
	))); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Close()
	}()

	client, err := bigquery.NewClient(ctx, projectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Core case: ARRAY parameter must not be wiped by applyNullQueryParameters.
	t.Run("string array matches correct rows", func(t *testing.T) {
		q := client.Query(`
SELECT persona_id
FROM dataset1.people
WHERE persona_id IN UNNEST(@ids)
  AND status = 'active'`)
		q.Parameters = []bigquery.QueryParameter{
			{Name: "ids", Value: []string{"persona-aaa", "persona-bbb"}},
		}
		it, err := q.Read(ctx)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		var got []string
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			got = append(got, fmt.Sprint(row[0]))
		}
		// persona-bbb is suspended, persona-ccc is not in the list.
		if len(got) != 1 || got[0] != "persona-aaa" {
			t.Errorf("got %v, want [persona-aaa]", got)
		}
	})

	// Empty array: the empty array must be preserved (not silently cleared to
	// NULL). ARRAY_LENGTH distinguishes the two: an empty []string{} gives 0,
	// while NULL would give NULL. IN UNNEST([]) also returns no rows, but that
	// assertion holds for either behaviour and would not catch a regression.
	t.Run("empty array is preserved, not cleared to NULL", func(t *testing.T) {
		q := client.Query(`SELECT ARRAY_LENGTH(@ids) AS n`)
		q.Parameters = []bigquery.QueryParameter{
			{Name: "ids", Value: []string{}},
		}
		it, err := q.Read(ctx)
		if err != nil {
			t.Fatalf("empty array query failed: %v", err)
		}
		var row struct{ N bigquery.NullInt64 }
		if err := it.Next(&row); err != nil {
			t.Fatal(err)
		}
		if !row.N.Valid {
			t.Error("ARRAY_LENGTH of empty array returned NULL; want 0 (array was silently cleared)")
		} else if row.N.Int64 != 0 {
			t.Errorf("ARRAY_LENGTH of empty array = %d; want 0", row.N.Int64)
		}
	})

	// STRUCT parameters also use structValues (not value) in the REST JSON and
	// must survive applyNullQueryParameters for the same reason as arrays.
	t.Run("struct param is not cleared", func(t *testing.T) {
		type filter struct {
			Status string
		}
		q := client.Query(`SELECT @f IS NULL AS is_null`)
		q.Parameters = []bigquery.QueryParameter{
			{Name: "f", Value: filter{Status: "active"}},
		}
		it, err := q.Read(ctx)
		if err != nil {
			t.Fatalf("struct param query failed: %v", err)
		}
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			t.Fatal(err)
		}
		if row[0] != false {
			t.Errorf("struct param was cleared: got is_null=%v, want false", row[0])
		}
	})

	// Scalar NULL parameters must still be treated as NULL after the fix
	// (i.e. applyNullQueryParameters must not have broken the scalar path).
	t.Run("null scalar param still observed as NULL", func(t *testing.T) {
		q := client.Query(`SELECT @p IS NULL AS is_null`)
		q.Parameters = []bigquery.QueryParameter{
			{Name: "p", Value: bigquery.NullString{}},
		}
		it, err := q.Read(ctx)
		if err != nil {
			t.Fatalf("null scalar query failed: %v", err)
		}
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			t.Fatal(err)
		}
		if row[0] != true {
			t.Errorf("null scalar: got is_null=%v, want true", row[0])
		}
	})
}
