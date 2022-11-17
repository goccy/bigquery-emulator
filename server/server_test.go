package server_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
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
			fmt.Println("row = ", row)
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
		bqServer.Close()
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
		bqServer.Close()
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
		bqServer.Close()
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
		bqServer.Close()
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
		fmt.Println("row = ", row)
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
		bqServer.Close()
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
		bqServer.Close()
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
		bqServer.Close()
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
		bqServer.Close()
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
		fmt.Println("row = ", row)
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
		bqServer.Close()
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
			fmt.Println("row = ", row)
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
			fmt.Println("row = ", row)
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
		bqServer.Close()
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
		bqServer.Close()
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
	if !reflect.DeepEqual([]*row{
		{ID: 1, Name: "John"},
		{ID: 2, Name: "Joan"},
	}, rows) {
		t.Error("unexpected rows")
	}
}
