# BigQuery Emulator

![Go](https://github.com/goccy/bigquery-emulator/workflows/Go/badge.svg)
[![GoDoc](https://godoc.org/github.com/goccy/bigquery-emulator?status.svg)](https://pkg.go.dev/github.com/goccy/bigquery-emulator?tab=doc)


BigQuery emulator server implemented by Go

# Features

- If you can choose the Go language, you can launch a BigQuery test server on the same process and test it using the BigQuery client.
- BigQuery emulator can be built as a static single binary and can be launched as a standalone process.

# Synopsis

```go
package main

import (
  "context"
  "fmt"

  "cloud.google.com/go/bigquery"
  "github.com/goccy/bigquery-emulator/server"
  "github.com/goccy/bigquery-emulator/types"
  "google.golang.org/api/iterator"
  "google.golang.org/api/option"
)

func main() {
  ctx := context.Background()
  const (
    projectID = "test"
    datasetID = "dataset1"
    routineID = "routine1"
  )
  bqServer, err := server.New(server.TempStorage)
  if err != nil {
    panic(err)
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
    panic(err)
  }
  if err := bqServer.SetProject(projectID); err != nil {
    panic(err)
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
    panic(err)
  }
  defer client.Close()
  routineName, err := client.Dataset(datasetID).Routine(routineID).Identifier(bigquery.StandardSQLID)
  if err != nil {
    panic(err)
  }
  sql := fmt.Sprintf(`
CREATE FUNCTION %s(
  arr ARRAY<STRUCT<name STRING, val INT64>>
) AS (
  (SELECT SUM(IF(elem.name = "foo",elem.val,null)) FROM UNNEST(arr) AS elem)
)`, routineName)
  job, err := client.Query(sql).Run(ctx)
  if err != nil {
    panic(err)
  }
  status, err := job.Wait(ctx)
  if err != nil {
    panic(err)
  }
  if err := status.Err(); err != nil {
    panic(err)
  }

  it, err := client.Query(fmt.Sprintf(`
SELECT %s([
  STRUCT<name STRING, val INT64>("foo", 10),
  STRUCT<name STRING, val INT64>("bar", 40),
  STRUCT<name STRING, val INT64>("foo", 20)
])`, routineName)).Read(ctx)
  if err != nil {
    panic(err)
  }

  var row []bigquery.Value
  if err := it.Next(&row); err != nil {
    if err == iterator.Done {
        return
    }
    panic(err)
  }
  fmt.Println(row[0]) // 30
}
```


# License

MIT