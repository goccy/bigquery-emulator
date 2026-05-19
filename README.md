# BigQuery Emulator

[![build and test](https://github.com/goccy/bigquery-emulator/actions/workflows/test.yml/badge.svg)](https://github.com/goccy/bigquery-emulator/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/goccy/bigquery-emulator?status.svg)](https://pkg.go.dev/github.com/goccy/bigquery-emulator?tab=doc)
[![Sponsor](https://img.shields.io/badge/Sponsor-%E2%9D%A4-db61a2)](https://github.com/sponsors/goccy)


The only open-source emulator for Google BigQuery — run a BigQuery-compatible server on your local machine for testing and development, with no cloud project or credentials required.

# Quick start

```console
$ docker run -it -p 9050:9050 -p 9060:9060 ghcr.io/goccy/bigquery-emulator:latest --project=test
$ bq --api http://0.0.0.0:9050 query --project_id=test "SELECT 1"
```

See [Install](#install) for `go install`, prebuilt binaries and packages, and [How to start the standalone server](#how-to-start-the-standalone-server) for the full set of options and client examples.

# Features

- If you can choose the Go language as BigQuery client, you can launch a BigQuery emulator on the same process as the testing process by [httptest](https://pkg.go.dev/net/http/httptest) .
- BigQuery emulator can be built as a static single binary and can be launched as a standalone process. So, you can use the BigQuery emulator from programs written in non-Go languages or such as the [bq](https://cloud.google.com/bigquery/docs/bq-command-line-tool) command, by specifying the address of the launched BigQuery emulator.
- BigQuery emulator utilizes SQLite for storage. You can select either memory or file as the data storage destination at startup, and if you set it to file, data can be persisted.
- You can load seeds from a YAML file on startup

# Status

This project is still in **beta**, but a large part of BigQuery already works from the official client libraries. The multi-client conformance suite ([`test/e2e`](https://github.com/goccy/bigquery-emulator/tree/main/test/e2e)) exercises the official Python, Ruby, PHP, Node.js and Java client libraries plus the `bq` CLI against the emulator over a shared query corpus, and currently passes for every client.

BigQuery is a large product, so rather than scatter caveats across this README, the emulator's coverage is tracked feature by feature in a single MECE (mutually exclusive, collectively exhaustive) matrix:

### 📋 [BigQuery feature support matrix](./docs/feature-support.md)

At a glance, the emulator supports dataset / table / job / tabledata management, GoogleSQL query execution, batch load and extract jobs (including loads from Google Cloud Storage), streaming inserts, the gRPC BigQuery Storage read/write APIs, and logical and materialized views. IAM policy management, row access policies, copy jobs, external tables, table snapshots and BigQuery ML are not implemented yet. See the matrix for the complete, categorized breakdown.

## GoogleSQL

Query execution is powered by [googlesqlite](https://github.com/goccy/googlesqlite), which implements almost all of [GoogleSQL](https://cloud.google.com/bigquery/docs/reference/standard-sql/introduction): as of googlesqlite v0.1.0 its spec-driven support matrix reports 523 / 529 GoogleSQL functions and 55 / 59 BigQuery-specific functions implemented — roughly 570 built-in functions — along with 16 / 18 data types. Beyond functions, it also supports:

- Wildcard tables
- Templated-argument functions
- JavaScript UDF

For the authoritative, per-function and per-type support matrix, see the [googlesqlite status](https://github.com/goccy/googlesqlite#status).

# Goals

The goal of this project is to build a server that behaves exactly like BigQuery from the BigQuery client's perspective. To do so, we need to support all features present in BigQuery ( Model API / Connection API / INFORMATION SCHEMA etc.. ) in addition to evaluating GoogleSQL.

# Sponsorship

This is a personal project. It receives no support of any kind from Google — no sponsorship, no contributions, no promotion.

For example, Google has had a request for a BigQuery emulator open on its Issue Tracker ([issue 129248927](https://issuetracker.google.com/issues/129248927)) for seven years without taking any action. Unable to watch that any longer, in 2022 I built `bigquery-emulator` — the only BigQuery emulator in the world — and have maintained it ever since.

I do not use BigQuery in my own job, however. I simply happen to have the skills to build this, noticed how many people are struggling without it, and so I spend my personal time and money on it. I develop it on my days off and after work, while working full time and maintaining a lot of other OSS, so the time available for this project is limited.

Because of that, keeping this project alive needs your help. Emulating BigQuery locally brings many benefits to development and can significantly cut development costs. Could you return a small part of those savings to me? I believe doing so leads to a better future for both me and your company. Especially if you are part of a commercial company and could use this project, I'd be glad if you could consider sponsoring me.

# Install

If Go is installed, you can install the latest version with the following command

```console
$ go install github.com/goccy/bigquery-emulator/cmd/bigquery-emulator@latest
```

The SQL backend is pure Go, so installation is an ordinary `go install` with
no native build step.

You can also download the docker image with the following command

```console
$ docker pull ghcr.io/goccy/bigquery-emulator:latest
```

The image is a multi-arch manifest, so the same tag works on both `linux/amd64` and `linux/arm64`.

You can also download prebuilt binaries (darwin/linux/windows, amd64/arm64) and `deb`/`rpm`/`apk` packages directly from [releases](https://github.com/goccy/bigquery-emulator/releases).

Both the release archives and the container image ship a signed [GitHub build-provenance attestation](https://docs.github.com/en/actions/security-guides/using-artifact-attestations-to-establish-provenance-for-builds). Verify them with the GitHub CLI:

```console
# release archive
$ gh attestation verify bigquery-emulator_v0.0.0_linux_amd64.tar.gz --repo goccy/bigquery-emulator

# container image
$ gh attestation verify oci://ghcr.io/goccy/bigquery-emulator:latest --repo goccy/bigquery-emulator
```

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

If you want to use docker image to start emulator, specify like the following.

```console
$ docker run -it -p 9050:9050 -p 9060:9060 ghcr.io/goccy/bigquery-emulator:latest --project=test
```

* If you are using an M1 Mac ( and Docker Desktop ) you may get a warning. In that case please use `--platform linux/x86_64` option.

## How to use from bq client

### 1. Start the standalone server

```console
$ ./bigquery-emulator --project=test --data-from-yaml=./server/testdata/data.yaml
[bigquery-emulator] REST server listening at 0.0.0.0:9050
[bigquery-emulator] gRPC server listening at 0.0.0.0:9060
```

* `server/testdata/data.yaml` is [here](https://github.com/goccy/bigquery-emulator/blob/main/server/testdata/data.yaml)

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

# Debugging

If you have specified a database file when starting `bigquery-emulator`, the file is an ordinary SQLite database and can be inspected with any SQLite tool.

# How it works

## BigQuery Emulator Architecture Overview

After receiving a GoogleSQL query via the REST API from bq or a client SDK, the googlesqlite driver parses and analyzes the query — using [go-googlesql](https://github.com/goccy/go-googlesql), a GoogleSQL (ZetaSQL) parser and analyzer written in Go — and executes it against an embedded SQLite database.

<img width="600px" src="https://user-images.githubusercontent.com/209884/196145011-e35c2df4-5f5d-43ce-b7df-08cd130b5d31.png"></img>



## Type Conversion Flow

BigQuery has a number of types that do not exist in SQLite (e.g. ARRAY and STRUCT).
In order to handle them in SQLite, googlesqlite encodes all types except `INT64` / `FLOAT64` / `BOOL` with the type information and data combination and stores them in SQLite.
When using the encoded data, decode the data via a custom function registered with the SQLite driver before use.

<img width="600px" src="https://user-images.githubusercontent.com/209884/196145033-aa032878-7e01-4ec7-9a23-b174b87e1a24.png"></img>


# Reference

Regarding the story of bigquery-emulator, there are the following articles.
- [How to create a BigQuery Emulator](https://docs.google.com/presentation/d/1j5TPCpXiE9CvBjq78W8BWz-cGxU8djW1qy9Y6eBHso8/edit?usp=sharing) ( Japanese )


# License

MIT
