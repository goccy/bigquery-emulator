# BigQuery feature support matrix

This document tracks, feature by feature, what [`bigquery-emulator`](https://github.com/goccy/bigquery-emulator) supports.

BigQuery is a large product, so this matrix is organized to be **MECE** (mutually
exclusive, collectively exhaustive): every BigQuery feature area appears in
exactly one section, and the sections together cover the whole product surface.
The top-level grouping follows the structure of the official
[BigQuery documentation](https://docs.cloud.google.com/bigquery/docs) and the
[BigQuery REST API reference](https://docs.cloud.google.com/bigquery/docs/reference/rest).

If a feature you need is missing or wrong here, please open an
[Issue](https://github.com/goccy/bigquery-emulator/issues).

**Legend**

| Mark | Meaning |
| --- | --- |
| ✅ | Supported |
| 🟡 | Partially supported — see the note |
| ❌ | Not supported yet |

*Last reviewed: 2026-05-18, against `googlesqlite` v0.1.0.*

---

## 1. REST API

The emulator implements the [BigQuery API v2](https://docs.cloud.google.com/bigquery/docs/reference/rest)
REST surface. The tables below list every documented resource and method.

### 1.1 `datasets`

| Method | Status | Notes |
| --- | --- | --- |
| `get` | ✅ | |
| `insert` | ✅ | |
| `list` | ✅ | |
| `patch` | ✅ | |
| `update` | ✅ | |
| `delete` | ✅ | |
| `undelete` | ❌ | Dataset time travel / undelete is not implemented. |

### 1.2 `tables`

| Method | Status | Notes |
| --- | --- | --- |
| `get` | ✅ | |
| `insert` | ✅ | |
| `list` | ✅ | |
| `patch` | ✅ | |
| `update` | ❌ | Use `patch` instead. |
| `delete` | ✅ | |
| `getIamPolicy` | ❌ | IAM policy management is not implemented. |
| `setIamPolicy` | ❌ | IAM policy management is not implemented. |
| `testIamPermissions` | ❌ | IAM policy management is not implemented. |

### 1.3 `tabledata`

| Method | Status | Notes |
| --- | --- | --- |
| `insertAll` | ✅ | Streaming inserts. Unknown fields are reported as errors. |
| `list` | ✅ | |

### 1.4 `jobs`

| Method | Status | Notes |
| --- | --- | --- |
| `query` | ✅ | |
| `getQueryResults` | ✅ | Honors `maxResults` paging. |
| `get` | ✅ | |
| `list` | ✅ | |
| `insert` | 🟡 | Query, load and extract configurations only — see [section 2](#2-jobs). |
| `cancel` | ✅ | |
| `delete` | ✅ | |

### 1.5 `models`

| Method | Status | Notes |
| --- | --- | --- |
| `get` | 🟡 | Metadata only; the emulator does not train or store models. |
| `list` | 🟡 | Metadata only. |
| `patch` | 🟡 | Metadata only. |
| `delete` | ✅ | |

There is no `models.insert` in the BigQuery API — models are created with
`CREATE MODEL`, which is not supported (see [section 8](#8-bigquery-ml)).

### 1.6 `routines`

| Method | Status | Notes |
| --- | --- | --- |
| `insert` | ✅ | Create SQL functions, procedures and table functions. |
| `get` | ❌ | |
| `list` | ❌ | |
| `update` | ❌ | |
| `delete` | ❌ | |
| `getIamPolicy` | ❌ | |
| `setIamPolicy` | ❌ | |
| `testIamPermissions` | ❌ | |

### 1.7 `projects`

| Method | Status | Notes |
| --- | --- | --- |
| `list` | ✅ | |
| `getServiceAccount` | ✅ | Returns a placeholder service account. |

### 1.8 `rowAccessPolicies`

| Method | Status | Notes |
| --- | --- | --- |
| `list` | ❌ | Row-level security is not implemented. |
| `get` | ❌ | |
| `insert` | ❌ | |
| `update` | ❌ | |
| `delete` | ❌ | |
| `batchDelete` | ❌ | |
| `getIamPolicy` | ❌ | |
| `testIamPermissions` | ❌ | |

---

## 2. Jobs

[Job types](https://docs.cloud.google.com/bigquery/docs/jobs-overview) accepted by `jobs.insert`.

| Job type | Status | Notes |
| --- | --- | --- |
| Query | ✅ | See [section 6](#6-query-language-googlesql). Exposes the anonymous results table as `destinationTable`. |
| Load | ✅ | See [section 4](#4-data-ingestion). |
| Extract | ✅ | See [section 5](#5-data-export). |
| Copy | ❌ | Table copy jobs are not implemented. |

---

## 3. Storage and table types

| Feature | Status | Notes |
| --- | --- | --- |
| Standard tables | ✅ | |
| Logical views | ✅ | Created via DDL or `tables.insert`; view schemas are hydrated. |
| Materialized views | ✅ | Registered and queryable. |
| External tables | ❌ | The table type exists but is not implemented. |
| Table snapshots | ❌ | |
| Table clones | ❌ | |
| Partitioned tables | 🟡 | Tables with partitioning metadata are accepted, but partition pruning / `_PARTITIONTIME` semantics are not emulated. |
| Clustered tables | 🟡 | Clustering metadata is accepted but does not affect execution. |
| Storage backend | ✅ | Embedded SQLite — in-memory or a persisted file (see the README). |

---

## 4. Data ingestion

| Feature | Status | Notes |
| --- | --- | --- |
| Streaming inserts (`tabledata.insertAll`) | ✅ | |
| Storage Write API | ✅ | gRPC; see [section 9](#9-bigquery-storage-api). |
| Load from Google Cloud Storage | ✅ | Set `STORAGE_EMULATOR_HOST` to point at a GCS emulator. |
| Load from a local file (multipart / resumable upload) | ✅ | |
| Load format: CSV | ✅ | Schema autodetect is supported. |
| Load format: JSON (newline-delimited) | ✅ | |
| Load format: Parquet | ✅ | |
| Load format: Avro | ❌ | |
| Load format: ORC | ❌ | |
| Write disposition: `WRITE_APPEND` | ✅ | Default. |
| Write disposition: `WRITE_TRUNCATE` | ✅ | |
| Write disposition: `WRITE_EMPTY` | ✅ | |
| BigQuery Data Transfer Service | ❌ | |

---

## 5. Data export

| Feature | Status | Notes |
| --- | --- | --- |
| Extract to Google Cloud Storage | ✅ | |
| Extract format: CSV | ✅ | |
| Extract format: JSON (newline-delimited) | ✅ | |
| Extract format: Avro | ❌ | |
| Extract format: Parquet | ❌ | |
| Storage Read API | ✅ | gRPC; see [section 9](#9-bigquery-storage-api). |

---

## 6. Query language (GoogleSQL)

Query execution is delegated to [`googlesqlite`](https://github.com/goccy/googlesqlite),
which parses and analyzes [GoogleSQL](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/introduction)
and runs it against the embedded SQLite database. The table below records what
the **emulator** wires up; the authoritative, per-function and per-type matrix
lives in the [`googlesqlite` status](https://github.com/goccy/googlesqlite#status).

| Feature | Status | Notes |
| --- | --- | --- |
| `SELECT` / query statements | ✅ | |
| DDL (`CREATE` / `ALTER` / `DROP` for tables, views, materialized views, functions) | ✅ | DDL-created tables and views are registered as metadata. |
| DML (`INSERT` / `UPDATE` / `DELETE` / `MERGE` / `TRUNCATE`) | ✅ | Subject to `googlesqlite` coverage. |
| Scripting and multi-statement queries | ✅ | Subject to `googlesqlite` coverage. |
| Stored procedures | ✅ | Created via `routines.insert` or DDL. |
| Built-in functions (~570) | ✅ | `googlesqlite` v0.1.0: 523/529 GoogleSQL + 55/59 BigQuery-specific. |
| Data types (16 / 18) | ✅ | `NUMERIC` / `BIGNUMERIC` map to Arrow decimal types. |
| SQL UDFs | ✅ | |
| JavaScript UDFs | ✅ | Inline `CREATE ... LANGUAGE js` functions; persisting a JS routine via `routines.insert` is not supported. |
| Wildcard tables | ✅ | |
| Templated-argument functions | ✅ | |
| Query parameters (named and positional) | ✅ | Empty / absent numeric, temporal and string parameters are treated as typed `NULL`s. |
| Table-valued functions | ✅ | |
| `INFORMATION_SCHEMA` views | 🟡 | `googlesqlite` implements `SCHEMATA`, `TABLES`, `TABLE_OPTIONS` and `COLUMNS`; other views (e.g. `JOBS`, `VIEWS`, `ROUTINES`, `PARTITIONS`) are not implemented. |
| Time travel (`FOR SYSTEM_TIME AS OF`) | ❌ | |
| Sessions / multi-statement transactions over the REST API | ❌ | |
| BigQuery ML statements (`CREATE MODEL`, `ML.*`) | ❌ | See [section 8](#8-bigquery-ml). |

---

## 7. Security and governance

| Feature | Status | Notes |
| --- | --- | --- |
| Authentication | 🟡 | The emulator does not authenticate requests; clients connect with anonymous credentials. |
| IAM policy management | ❌ | `getIamPolicy` / `setIamPolicy` / `testIamPermissions` are not implemented. |
| Row-level security (`rowAccessPolicies`) | ❌ | |
| Column-level security / policy tags | ❌ | |
| Dynamic data masking | ❌ | |
| Authorized views / authorized routines / authorized datasets | ❌ | |
| Customer-managed encryption keys (CMEK) | ❌ | Not applicable to a local emulator. |

---

## 8. BigQuery ML

| Feature | Status | Notes |
| --- | --- | --- |
| `CREATE MODEL` and `ML.*` functions | ❌ | Model training and inference are not emulated. |
| Models REST resource | 🟡 | Metadata-only stubs — see [section 1.5](#15-models). |

---

## 9. BigQuery Storage API

The gRPC [BigQuery Storage API](https://docs.cloud.google.com/bigquery/docs/reference/storage).

| Feature | Status | Notes |
| --- | --- | --- |
| Storage Read API | ✅ | |
| Storage Write API | ✅ | |
| Apache Avro format | ✅ | |
| Apache Arrow format | ✅ | |

---

## 10. Integrations

| Feature | Status | Notes |
| --- | --- | --- |
| Google Cloud Storage (load / extract) | ✅ | Works against a GCS emulator via `STORAGE_EMULATOR_HOST`. |
| BigQuery Data Transfer Service | ❌ | |
| Federated queries / external data sources | ❌ | |
| Connections API (`Connection` resource) | ❌ | |
| BI Engine | ❌ | |
| Analytics Hub | ❌ | |
| BigQuery Omni | ❌ | |
| Dataform | ❌ | |
| Reservations / capacity management | ❌ | Not applicable to a local emulator. |

---

## 11. Emulator-specific features

Capabilities that are not part of the BigQuery product but are provided by this
emulator for local development and testing.

| Feature | Status | Notes |
| --- | --- | --- |
| In-process Go test server (`server.New` / `httptest`) | ✅ | See the README synopsis. |
| Standalone server binary / Docker image | ✅ | |
| YAML seed loader (`--data-from-yaml`) | ✅ | |
| In-memory storage | ✅ | |
| Persisted file storage | ✅ | An ordinary SQLite database file. |
| Multi-client conformance suite (`test/e2e`) | ✅ | Python, Ruby, PHP, Node.js, Java clients and the `bq` CLI. |
