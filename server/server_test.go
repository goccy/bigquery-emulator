package server_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

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
