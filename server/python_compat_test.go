package server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/go-json"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TestDiscoveryEndpointUsesCurrentServerAddress(t *testing.T) {
	ctx := context.Background()

	createServer := func(t *testing.T) (*server.Server, *server.TestServer) {
		t.Helper()
		bqServer, err := server.New(server.TempStorage)
		if err != nil {
			t.Fatal(err)
		}
		if err := bqServer.SetProject("test"); err != nil {
			t.Fatal(err)
		}
		testServer := bqServer.TestServer()
		return bqServer, testServer
	}

	bqServer1, testServer1 := createServer(t)
	defer func() {
		testServer1.Close()
		_ = bqServer1.Stop(ctx)
	}()

	bqServer2, testServer2 := createServer(t)
	defer func() {
		testServer2.Close()
		_ = bqServer2.Stop(ctx)
	}()

	resp1, err := http.Get(fmt.Sprintf("%s/discovery/v1/apis/bigquery/v2/rest", testServer1.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer resp1.Body.Close()
	body1, err := io.ReadAll(resp1.Body)
	if err != nil {
		t.Fatal(err)
	}
	var discovery1 map[string]interface{}
	if err := json.Unmarshal(body1, &discovery1); err != nil {
		t.Fatal(err)
	}

	resp2, err := http.Get(fmt.Sprintf("%s/discovery/v1/apis/bigquery/v2/rest", testServer2.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	body2, err := io.ReadAll(resp2.Body)
	if err != nil {
		t.Fatal(err)
	}
	var discovery2 map[string]interface{}
	if err := json.Unmarshal(body2, &discovery2); err != nil {
		t.Fatal(err)
	}

	if got := discovery1["rootUrl"]; got != testServer1.URL {
		t.Fatalf("unexpected rootUrl for server1: got=%v want=%s", got, testServer1.URL)
	}
	if got := discovery2["rootUrl"]; got != testServer2.URL {
		t.Fatalf("unexpected rootUrl for server2: got=%v want=%s", got, testServer2.URL)
	}
}

func TestJobsInsertAcceptsMissingJobReference(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject("test"); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		_ = bqServer.Stop(ctx)
	}()

	body, err := json.Marshal(&bigqueryv2.Job{
		Configuration: &bigqueryv2.JobConfiguration{
			Query: &bigqueryv2.JobConfigurationQuery{
				Query: "SELECT 1",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(
		fmt.Sprintf("%s/projects/test/jobs", testServer.URL),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status: %d body=%s", resp.StatusCode, string(responseBody))
	}
	var job bigqueryv2.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	if job.JobReference == nil {
		t.Fatal("expected jobReference to be generated")
	}
	if job.JobReference.JobId == "" {
		t.Fatal("expected non-empty generated job id")
	}
	if job.JobReference.ProjectId != "test" {
		t.Fatalf("unexpected project id: %s", job.JobReference.ProjectId)
	}
}

func TestJobsInsertMarksInvalidQueryAsInvalidQuery(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject("test"); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		_ = bqServer.Stop(ctx)
	}()

	body, err := json.Marshal(&bigqueryv2.Job{
		JobReference: &bigqueryv2.JobReference{
			ProjectId: "test",
			JobId:     "invalid_query_job",
		},
		Configuration: &bigqueryv2.JobConfiguration{
			Query: &bigqueryv2.JobConfigurationQuery{
				Query: "SELECT 'unterminated",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(
		fmt.Sprintf("%s/projects/test/jobs", testServer.URL),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status: %d body=%s", resp.StatusCode, string(responseBody))
	}
	var job bigqueryv2.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	if job.Status == nil || job.Status.ErrorResult == nil {
		t.Fatalf("expected query to fail, got status=%+v", job.Status)
	}
	if got, want := job.Status.ErrorResult.Reason, "invalidQuery"; got != want {
		t.Fatalf("unexpected reason: got=%s want=%s message=%s", got, want, job.Status.ErrorResult.Message)
	}
}
