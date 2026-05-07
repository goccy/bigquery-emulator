# BigQuery Emulator

[![build and test](https://github.com/glassmonkey/bigquery-emulator/actions/workflows/test.yml/badge.svg)](https://github.com/glassmonkey/bigquery-emulator/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/goccy/bigquery-emulator?status.svg)](https://pkg.go.dev/github.com/goccy/bigquery-emulator?tab=doc)


BigQuery emulator server implemented in Go.  
BigQuery emulator provides a way to launch a BigQuery server on your local machine for testing and development.

> **Note: this is a fork of [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator).**
> The SQL analyzer has been swapped from the CGO-based [go-zetasql](https://github.com/goccy/go-zetasql) / [go-zetasqlite](https://github.com/goccy/go-zetasqlite) stack to the pure-Go [zetasql-wasm](https://github.com/glassmonkey/zetasql-wasm) (ZetaSQL compiled to WebAssembly, executed via [wazero](https://github.com/tetratelabs/wazero)). The vendored `internal/zetasqlite` layer replaces the external `go-zetasqlite` dependency. Huge thanks to [@goccy](https://github.com/goccy) for the original work this fork builds on.
>
> Practical differences:
> - `go install` requires no CGO toolchain (no `clang++`, no `CGO_ENABLED=1`, no multi-minute ZetaSQL build).
> - Cross-compilation works out of the box.
> - Some runtime corners are still being filled in — see the test status of `internal/zetasqlite/` for the current gap list.
>
> The Go module path (`github.com/goccy/bigquery-emulator`) is kept unchanged for drop-in compatibility, so `go install` and library `import` paths still read `goccy/...`. Source distribution and issue tracking happen on this fork (`glassmonkey/bigquery-emulator`).

# Features

- If you can choose the Go language as BigQuery client, you can launch a BigQuery emulator on the same process as the testing process by [httptest](https://pkg.go.dev/net/http/httptest) .
- BigQuery emulator can be built as a static single binary and can be launched as a standalone process. So, you can use the BigQuery emulator from programs written in non-Go languages or such as the [bq](https://cloud.google.com/bigquery/docs/bq-command-line-tool) command, by specifying the address of the launched BigQuery emulator.
- BigQuery emulator utilizes SQLite for storage. You can select either memory or file as the data storage destination at startup, and if you set it to file, data can be persisted.
- You can load seeds from a YAML file on startup

# Status

Although this project is still in **beta** version, many features are already available.

## BigQuery API

We've been implemented all the [BigQuery APIs](https://cloud.google.com/bigquery/docs/reference/rest) except the API to manipulate IAM resources. It is possible that some options are not supported, in which case please report them in an Issue.

## Google Cloud Storage linkage

BigQuery emulator supports loading data from Google Cloud Storage and extracting table data. Currently, only CSV and JSON data types can be used for extracting. If you use Google Cloud Storage emulator, please set `STORAGE_EMULATOR_HOST` environment variable.

## BigQuery Storage API

Supports gRPC-based read/write using [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage).
Supports both Apache `Avro` and `Arrow` formats.

## Google Standard SQL

BigQuery emulator supports many of the specifications present in [Google Standard SQL](https://cloud.google.com/bigquery/docs/reference/standard-sql/introduction).
For example, it has the following features.

- 200+ standard functions
- Wildcard table
- Templated Argument Function
- JavaScript UDF

The supported feature set tracks the bundled `internal/zetasqlite` layer, which is built on top of [zetasql-wasm](https://github.com/glassmonkey/zetasql-wasm).

# Goals and Sponsors

The goal of this project is to build a server that behaves exactly like BigQuery from the BigQuery client's perspective. To do so, we need to support all features present in BigQuery ( Model API / Connection API / INFORMATION SCHEMA etc.. ) in addition to evaluating Google Standard SQL.

However, this project is a personal project and I develop it on my days off and after work. I work full time and maintain a lot of OSS. Therefore, the time available for this project is also limited. Of course, I will be adding features and fixing bugs on a regular basis to get us closer to our goals, but if you want me to implement the features you want, please consider sponsoring me. Of course, you can use this project for free, but if you sponsor me, that will be my motivation. Especially if you are part of a commercial company and could use this project, I'd be glad if you could consider sponsoring me at the same time.

# Install

If Go is installed, you can install the latest version with the following command

```console
$ go install github.com/goccy/bigquery-emulator/cmd/bigquery-emulator@latest
```

The BigQuery emulator embeds the SQL analyzer through [zetasql-wasm](https://github.com/glassmonkey/zetasql-wasm), so the install is a pure-Go `go install` — no CGO toolchain or ZetaSQL build is required.

# How to start the standalone server

If you can install the `bigquery-emulator` CLI, you can start the server using the following options.

```console
$ ./bigquery-emulator -h
Usage:
  bigquery-emulator [OPTIONS]

Application Options:
      --project=        specify the project name
      --dataset=        specify the dataset name
      --port=           specify the http port number. this port used by bigquery api (default: 9050)
      --grpc-port=      specify the grpc port number. this port used by bigquery storage api (default: 9060)
      --log-level=      specify the log level (debug/info/warn/error) (default: error)
      --log-format=     specify the log format (console/json) (default: console)
      --database=       specify the database file if required. if not specified, it will be on memory
      --data-from-yaml= specify the path to the YAML file that contains the initial data
  -v, --version         print version

Help Options:
  -h, --help            Show this help message
```

Start the server by specifying the project name

```console
$ ./bigquery-emulator --project=test
[bigquery-emulator] REST server listening at 0.0.0.0:9050
[bigquery-emulator] gRPC server listening at 0.0.0.0:9060
```

## How to use from bq client

### 1. Start the standalone server

```console
$ ./bigquery-emulator --project=test --data-from-yaml=./server/testdata/data.yaml
[bigquery-emulator] REST server listening at 0.0.0.0:9050
[bigquery-emulator] gRPC server listening at 0.0.0.0:9060
```

* `server/testdata/data.yaml` is [here](https://github.com/glassmonkey/bigquery-emulator/blob/main/server/testdata/data.yaml)

### 2. Call endpoint from bq client

```console
$ bq --api http://0.0.0.0:9050 query --project_id=test "SELECT * FROM dataset1.table_a WHERE id = 1"

+----+-------+---------------------------------------------+------------+----------+---------------------+
| id | name  |                  structarr                  |  birthday  | skillNum |     created_at      |
+----+-------+---------------------------------------------+------------+----------+---------------------+
|  1 | alice | [{"key":"profile","value":"{\"age\": 10}"}] | 2012-01-01 |        3 | 2022-01-01 12:00:00 |
+----+-------+---------------------------------------------+------------+----------+---------------------+
```

## How to use from python client

### 1. Start the standalone server

```console
$ ./bigquery-emulator --project=test --dataset=dataset1
[bigquery-emulator] REST server listening at 0.0.0.0:9050
[bigquery-emulator] gRPC server listening at 0.0.0.0:9060
```

### 2. Call endpoint from python client

Create ClientOptions with api_endpoint option and use AnonymousCredentials to disable authentication.

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig

client_options = ClientOptions(api_endpoint="http://0.0.0.0:9050")
client = bigquery.Client(
  "test",
  client_options=client_options,
  credentials=AnonymousCredentials(),
)
client.query(query="...", job_config=QueryJobConfig())
```

If you use a DataFrame as the download destination for the query results,
You must either disable the BigQueryStorage client with `create_bqstorage_client=False` or
create a BigQueryStorage client that references the local grpc port (default 9060).

https://cloud.google.com/bigquery/docs/samples/bigquery-query-results-dataframe?hl=en

```python
result = client.query(sql).to_dataframe(create_bqstorage_client=False)
```

or

```python
from google.cloud import bigquery_storage

client_options = ClientOptions(api_endpoint="0.0.0.0:9060")
read_client = bigquery_storage.BigQueryReadClient(client_options=client_options)
result = client.query(sql).to_dataframe(bqstorage_client=read_client)
``` 

# Synopsis

If you use the Go language as a BigQuery client, you can launch the BigQuery emulator on the same process as the testing process.  
Please imports `github.com/goccy/bigquery-emulator/server` ( and `github.com/goccy/bigquery-emulator/types` ) and you can use `server.New` API to create the emulator server instance.

See the API reference for more information: https://pkg.go.dev/github.com/goccy/bigquery-emulator

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

# How it works

## BigQuery Emulator Architecture Overview

After receiving a ZetaSQL query via the REST API from `bq` or a client SDK, the bundled `internal/zetasqlite` layer (built on [zetasql-wasm](https://github.com/glassmonkey/zetasql-wasm)) parses and analyzes the query to produce an AST. The AST is lowered into a SQLite query, which is then executed through go-sqlite3 against the SQLite database.

<img width="600px" src="https://user-images.githubusercontent.com/209884/196145011-e35c2df4-5f5d-43ce-b7df-08cd130b5d31.png"></img>

> Diagram credit: original by [@goccy](https://github.com/goccy) for the upstream `go-zetasqlite`-based architecture. The boxes labelled "go-zetasqlite" / "go-zetasql" map onto `internal/zetasqlite` and `zetasql-wasm` in this fork; the surrounding data flow is unchanged.

## Type Conversion Flow

BigQuery has a number of types that do not exist in SQLite (e.g. ARRAY and STRUCT).
In order to handle them in SQLite, `internal/zetasqlite` encodes every type except `INT64` / `FLOAT64` / `BOOL` as a `(type info, data)` pair and stores the encoded blob in SQLite.
When the encoded data is read back, a custom function registered with go-sqlite3 decodes it before use.

<img width="600px" src="https://user-images.githubusercontent.com/209884/196145033-aa032878-7e01-4ec7-9a23-b174b87e1a24.png"></img>

> Diagram credit: original by [@goccy](https://github.com/goccy); the encoding strategy is unchanged in this fork.


# Reference

Regarding the story of bigquery-emulator, there are the following articles.
- [How to create a BigQuery Emulator](https://docs.google.com/presentation/d/1j5TPCpXiE9CvBjq78W8BWz-cGxU8djW1qy9Y6eBHso8/edit?usp=sharing) ( Japanese )


# License

MIT
