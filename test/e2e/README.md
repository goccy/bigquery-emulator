# Multi-client conformance suite

This suite runs the **official BigQuery client libraries for Python, Ruby, PHP,
Node.js and Java, plus the `bq` command-line tool**, against a locally started
emulator and checks that every client agrees with a shared query corpus.
BigQuery has many clients and they decode result values differently; this
suite is how we catch those divergences.

## How it works

```
  go test ./test/e2e/...
        │
        ├─ builds one multi-stage image with every client
        │
        └─ for each language:
             ├─ starts an in-process emulator on the host (fresh per language)
             ├─ runs the client container, which executes cases/cases.json
             │  and writes a per-case report
             └─ surfaces each query as a Go subtest  →  language × query matrix
```

* **Emulator** — runs in-process on the host (not containerized). Each language
  gets its own fresh instance, so client runs are isolated.
* **Clients** — a single multi-stage `Dockerfile` installs each client library
  in its own build stage; the runtime stage carries only the interpreters/JRE
  and resolved libraries.
* **Corpus** — `cases/cases.json` holds the shared, language-neutral queries.
  The SQL is identical for every client; the comparison code is written *per
  language* (`clients/<lang>/`) because each client maps BigQuery values to
  different native types — exercising that mapping is the point of the suite.

## Running

```sh
make test/e2e                 # or: go test -v ./test/e2e/...
```

A running Docker daemon is required. Without one the suite skips itself, so it
is safe to leave in `go test ./...`. The Docker endpoint is auto-detected from
the active `docker` context (Docker Desktop, colima, …).

## Adding a query

1. Add an entry to `cases/cases.json` (`sql`, optional `params`, `expected`).
   Expected values use the canonical encoding documented at the top of that
   file. Derive expected values from the BigQuery documentation, not from a
   client's output.
2. Each client runner is data-driven, so no per-language code change is needed
   for an ordinary scalar/array/struct case.

## Known issues

`knownIssues` in `e2e_test.go` lists matrix cells that currently diverge due to
a tracked emulator or client bug. Those cells are reported as skipped so the
suite stays green for everything else. Removing an entry once its fix lands
re-arms the regression guard for that cell.
