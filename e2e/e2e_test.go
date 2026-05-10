//go:build e2e

// Smoke tests an already-running bigquery-emulator container.
// Assumes the emulator is reachable on http://localhost:9050 — bring
// it up with `make docker/up` before running. Iterates every
// testdata/query/*.sql, posts it to the BigQuery REST query
// endpoint, renders the response as TSV, and diffs against the
// paired .golden.tsv.
//
// testdata layout:
//
//	e2e/testdata/
//	├── fixture/                   pre-condition state (empty for now;
//	│                              future seed data / schema YAML can
//	│                              live here and be wired into compose)
//	└── query/
//	    ├── <name>.sql             query to POST
//	    └── <name>.golden.tsv      expected response, line 1 = column
//	                               names (\t-joined), line 2..N = row
//	                               values (\t-joined)
//
// Run via the Makefile (the `make e2e` target wires the
// docker/healthcheck gate so the test fails fast with a clear
// message if the emulator is not up):
//
//	make docker/up
//	make e2e
//	make docker/down
//
// Or directly:
//
//	go test -tags=e2e -count=1 -v ./e2e/...
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	composeFile = "compose.yml"
	project     = "test"
	httpAddr    = "http://localhost:9050"
)

// TestSmoke posts every testdata/query/*.sql to the running
// emulator and diffs the rendered TSV against the paired
// .golden.tsv. The container's lifecycle is the user's
// responsibility (see `make docker/up` / `make docker/down`).
func TestSmoke(t *testing.T) {
	cases, err := filepath.Glob("testdata/query/*.sql")
	if err != nil {
		t.Fatalf("glob testdata/query: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("no testdata/query/*.sql files found")
	}

	for _, sqlPath := range cases {
		sqlPath := sqlPath
		name := strings.TrimSuffix(filepath.Base(sqlPath), ".sql")
		goldenPath := strings.TrimSuffix(sqlPath, ".sql") + ".golden.tsv"

		t.Run(name, func(t *testing.T) {
			sqlBytes, err := os.ReadFile(sqlPath)
			if err != nil {
				t.Fatalf("read %s: %v", sqlPath, err)
			}
			sql := strings.TrimSpace(string(sqlBytes))

			// Act
			resp := postQuery(t, sql)
			got := renderTSV(resp)

			// Assert
			assertGolden(t, goldenPath, got)
		})
	}
}

// queryResponse decodes the synchronous query endpoint shape we
// care about. The full response carries more fields
// (jobReference, totalRows, ...) but the smoke harness only needs
// the schema field names and the row values to render TSV.
type queryResponse struct {
	Schema struct {
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields"`
	} `json:"schema"`
	Rows []struct {
		F []struct {
			V string `json:"v"`
		} `json:"f"`
	} `json:"rows"`
}

// renderTSV turns a query response into a tab-separated string with
// the column-name header on the first line and row values on the
// subsequent lines. Always ends with a trailing newline so the
// golden file's editor-friendly trailing newline is preserved on a
// byte-for-byte compare.
func renderTSV(resp queryResponse) string {
	var sb strings.Builder
	headers := make([]string, 0, len(resp.Schema.Fields))
	for _, f := range resp.Schema.Fields {
		headers = append(headers, f.Name)
	}
	sb.WriteString(strings.Join(headers, "\t"))
	sb.WriteByte('\n')
	for _, row := range resp.Rows {
		values := make([]string, 0, len(row.F))
		for _, c := range row.F {
			values = append(values, c.V)
		}
		sb.WriteString(strings.Join(values, "\t"))
		sb.WriteByte('\n')
	}
	return sb.String()
}

// assertGolden compares `got` against the golden file at path.
// On the first run for a freshly-added query the file does not
// exist yet — in that case the helper writes `got` as the new
// golden, logs the creation, and returns without flagging the
// test as failed. Subsequent runs hit the diff branch.
//
// Callers do not need to distinguish "created" from "matched"
// since both outcomes are a passing test from the test case's
// point of view; that bookkeeping is the helper's responsibility.
func assertGolden(t *testing.T, path, got string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("create golden file %s: %v", path, err)
		}
		t.Logf("created %s — review and commit; next run will diff against it", path)
		return
	}
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if want := string(raw); got != want {
		t.Errorf("response mismatch (%s)\n--- got\n%s\n--- want\n%s", path, got, want)
	}
}

// postQuery POSTs the SQL to /projects/<project>/queries and
// returns the parsed response. Fails the test on transport, HTTP,
// or JSON-decode errors. Dumps the last 50 lines of the
// container's logs alongside the failure so a single test run
// carries the diagnostic trail.
func postQuery(t *testing.T, sql string) queryResponse {
	t.Helper()
	url := fmt.Sprintf("%s/projects/%s/queries", httpAddr, project)
	body, err := json.Marshal(map[string]any{
		"query":        sql,
		"useLegacySql": false,
	})
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		dumpLogs(t)
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		dumpLogs(t)
		t.Fatalf("POST %s: HTTP %d, body=%s", url, resp.StatusCode, raw)
	}

	var got queryResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, raw)
	}
	return got
}

// dumpLogs prints the last 50 lines of the emulator's container
// logs to the test output. Best-effort: if `docker compose` is not
// on PATH or the compose stack is not running, the failure stays
// silent (the higher-level assertion already reported the cause).
func dumpLogs(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-f", composeFile, "logs", "--tail=50", "bigquery-emulator")
	cmd.Stdout = testLogWriter{t}
	cmd.Stderr = testLogWriter{t}
	_ = cmd.Run()
}

// testLogWriter routes a child process's stdout / stderr into the
// test's structured log so every line is attributable to the test
// run.
type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
