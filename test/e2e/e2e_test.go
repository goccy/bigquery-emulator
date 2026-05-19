// Package e2e runs the official BigQuery client libraries (Python, Ruby, PHP,
// Java) against a locally started emulator and asserts that every client
// agrees with the shared query corpus in cases/cases.json.
//
// The emulator runs in-process on the host; only the clients are containized.
// A single multi-stage image (Dockerfile) bundles all four client libraries.
// For each language the test starts a fresh emulator, runs the client image
// once, and surfaces the per-query result as a Go subtest, giving a
// language x query matrix under `go test ./test/e2e/...`.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// clientImage is the tag of the multi-language client image built by TestMain.
const clientImage = "bigquery-emulator-e2e-clients:latest"

// clientLanguages is the set of client languages exercised by the matrix.
var clientLanguages = []string{"python", "ruby", "php", "node", "bq", "java"}

// knownIssues records matrix cells (language/case, or language/* for a whole
// language) that currently diverge because of a tracked emulator or client
// bug. Listed cells are reported as skipped so the suite stays green and keeps
// working as a regression guard for everything else. When a fix lands, delete
// the entry: a cell with an exact (non-wildcard) entry that starts passing
// fails the suite, prompting the entry's removal.
//
// These were the first findings of the conformance suite itself.
var knownIssues = map[string]string{}

// knownIssue reports whether a language/case cell is a tracked divergence.
func knownIssue(lang, name string) (reason string, exact, known bool) {
	if r, ok := knownIssues[lang+"/"+name]; ok {
		return r, true, true
	}
	if r, ok := knownIssues[lang+"/*"]; ok {
		return r, false, true
	}
	return "", false, false
}

// dockerUp records whether a usable Docker daemon was found at startup.
var dockerUp bool

type caseResult struct {
	Name   string `json:"name"`
	Status string `json:"status"` // "pass", "fail" or "error"
	Detail string `json:"detail"`
}

type report struct {
	Language string       `json:"language"`
	Cases    []caseResult `json:"cases"`
}

func TestMain(m *testing.M) {
	dockerUp = exec.Command("docker", "info").Run() == nil
	if dockerUp {
		configureDockerEnv()
		if err := buildClientImage(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to build e2e client image: %v\n", err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

// configureDockerEnv points testcontainers-go at the same Docker endpoint the
// `docker` CLI uses. testcontainers-go does not always auto-detect non-default
// providers (colima, Docker contexts), so the endpoint is resolved explicitly.
func configureDockerEnv() {
	if os.Getenv("DOCKER_HOST") == "" {
		out, err := exec.Command("docker", "context", "inspect",
			"--format", "{{.Endpoints.docker.Host}}").Output()
		if err == nil {
			if host := strings.TrimSpace(string(out)); host != "" {
				os.Setenv("DOCKER_HOST", host)
			}
		}
	}
	// Ryuk (the resource reaper) bind-mounts the daemon socket, which is
	// awkward under colima. This test terminates its own containers, so the
	// reaper is not needed.
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
}

// buildClientImage builds the multi-language client image from the Dockerfile
// in this directory (the test runs with its package directory as cwd).
func buildClientImage() error {
	cmd := exec.Command("docker", "build", "-t", clientImage, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("docker build: %w\n%s", err, out)
	}
	return nil
}

func TestClientConformance(t *testing.T) {
	if !dockerUp {
		t.Skip("docker is not available; skipping multi-client e2e suite")
	}
	for _, lang := range clientLanguages {
		t.Run(lang, func(t *testing.T) {
			runLanguage(t, lang)
		})
	}
}

// runLanguage starts a fresh emulator, runs the client image for one language,
// and turns the client's per-case report into Go subtests.
func runLanguage(t *testing.T, lang string) {
	ctx := context.Background()
	emulatorPort := startEmulator(t)

	// The emulator listens on the host; the container reaches it through the
	// Docker host gateway exposed as host.docker.internal.
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: clientImage,
			Cmd:   []string{lang},
			Env: map[string]string{
				"EMULATOR_HOST": fmt.Sprintf("http://host.docker.internal:%d", emulatorPort),
				"PROJECT_ID":    "test",
			},
			ExtraHosts: []string{"host.docker.internal:host-gateway"},
			WaitingFor: wait.ForExit().WithExitTimeout(3 * time.Minute),
		},
		Started: true,
	}
	container, err := testcontainers.GenericContainer(ctx, req)
	if container != nil {
		defer testcontainers.CleanupContainer(t, container)
	}
	if err != nil {
		t.Fatalf("start %s client container: %v\n--- container logs ---\n%s",
			lang, err, containerLogs(ctx, container))
	}

	rep, err := readReport(ctx, container)
	if err != nil {
		t.Fatalf("read report from %s client: %v\n--- container logs ---\n%s",
			lang, err, containerLogs(ctx, container))
	}
	if len(rep.Cases) == 0 {
		t.Fatalf("%s client produced an empty report\n--- container logs ---\n%s",
			lang, containerLogs(ctx, container))
	}
	for _, c := range rep.Cases {
		t.Run(c.Name, func(t *testing.T) {
			reason, exact, known := knownIssue(lang, c.Name)
			if known {
				if c.Status == "pass" && exact {
					t.Fatalf("case is listed in knownIssues but now passes; "+
						"remove %q from knownIssues", lang+"/"+c.Name)
				}
				t.Skipf("known issue: %s [%s: %s]", reason, c.Status, firstLine(c.Detail))
			}
			if c.Status != "pass" {
				t.Errorf("%s: %s", c.Status, c.Detail)
			}
		})
	}
}

// firstLine returns the first non-empty line of s, for compact skip messages.
func firstLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

// corpusCase is the subset of a cases/cases.json entry the harness itself
// needs. The optional `setup` block names a table that must exist before the
// client runs; the client then streams `rows` into it through insertAll.
type corpusCase struct {
	Name  string `json:"name"`
	Setup *struct {
		Dataset string `json:"dataset"`
		Table   string `json:"table"`
		Schema  []struct {
			Name string `json:"name"`
			Type string `json:"type"`
			Mode string `json:"mode"`
		} `json:"schema"`
	} `json:"setup"`
}

// crudPreloadProject builds the emulator's "test" project, preloading every
// table named by a case's `setup` block (schema only, no rows). This models a
// production emulator started with --data-from-yaml: the table exists up
// front and each client runner streams rows into it through
// tabledata.insertAll — the path issue #470 regressed. Keeping the corpus the
// single source of truth means a new CRUD case needs no harness change.
func crudPreloadProject(t *testing.T) *types.Project {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("cases", "cases.json"))
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	var corpus struct {
		Cases []corpusCase `json:"cases"`
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatalf("parse corpus: %v", err)
	}
	tablesByDataset := map[string][]*types.Table{}
	var datasetOrder []string
	for _, c := range corpus.Cases {
		if c.Setup == nil {
			continue
		}
		cols := make([]*types.Column, 0, len(c.Setup.Schema))
		for _, f := range c.Setup.Schema {
			var opts []types.ColumnOption
			if f.Mode != "" {
				opts = append(opts, types.ColumnMode(types.Mode(f.Mode)))
			}
			cols = append(cols, types.NewColumn(f.Name, types.Type(f.Type), opts...))
		}
		if _, seen := tablesByDataset[c.Setup.Dataset]; !seen {
			datasetOrder = append(datasetOrder, c.Setup.Dataset)
		}
		tablesByDataset[c.Setup.Dataset] = append(tablesByDataset[c.Setup.Dataset],
			types.NewTable(c.Setup.Table, cols, nil))
	}
	datasets := make([]*types.Dataset, 0, len(datasetOrder))
	for _, name := range datasetOrder {
		datasets = append(datasets, types.NewDataset(name, tablesByDataset[name]...))
	}
	return types.NewProject("test", datasets...)
}

// startEmulator boots an in-process emulator with a single project ("test")
// through its real Serve entrypoint, listening on all interfaces so a
// container can reach it via the Docker host gateway. Using Serve (rather than
// hand-serving the handler) is what a real embedder does and ensures internal
// state such as the HTTP server reference is populated. It returns the host
// REST port and is torn down when the test finishes.
func startEmulator(t *testing.T) int {
	t.Helper()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatalf("create emulator: %v", err)
	}
	if err := bqServer.Load(server.StructSource(crudPreloadProject(t))); err != nil {
		t.Fatalf("load emulator project: %v", err)
	}
	httpPort, grpcPort := freePort(t), freePort(t)
	go func() {
		_ = bqServer.Serve(
			context.Background(),
			fmt.Sprintf("0.0.0.0:%d", httpPort),
			fmt.Sprintf("0.0.0.0:%d", grpcPort),
		)
	}()
	t.Cleanup(func() { _ = bqServer.Stop(context.Background()) })
	waitForListen(t, httpPort)
	return httpPort
}

// freePort reserves an OS-assigned free TCP port and releases it for reuse.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// waitForListen blocks until the emulator accepts connections on port.
func waitForListen(t *testing.T, port int) {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("emulator did not start listening on port %d", port)
}

// readReport copies the per-case report the client runner wrote inside the
// container and decodes it.
func readReport(ctx context.Context, c testcontainers.Container) (*report, error) {
	rc, err := c.CopyFileFromContainer(ctx, "/work/report.json")
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var rep report
	if err := json.Unmarshal(data, &rep); err != nil {
		return nil, fmt.Errorf("decode report: %w (raw: %s)", err, data)
	}
	return &rep, nil
}

func containerLogs(ctx context.Context, c testcontainers.Container) string {
	if c == nil {
		return "(no container)"
	}
	r, err := c.Logs(ctx)
	if err != nil {
		return fmt.Sprintf("(could not read container logs: %v)", err)
	}
	defer r.Close()
	b, _ := io.ReadAll(r)
	return string(b)
}
