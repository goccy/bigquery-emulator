package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/goccy/bigquery-emulator/server"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

// TestFailedQueryReportsInvalidQuery checks the error reason of a query
// that fails on its own terms. BigQuery reports it as invalidQuery
// (https://cloud.google.com/bigquery/docs/error-messages). The emulator
// reported jobInternalError, which the Python client lists in its
// job_retry_reasons, so a single failing query (a failed ASSERT, a
// SELECT ERROR(...)) was re-run until the client's 600s deadline instead
// of failing once.
func TestFailedQueryReportsInvalidQuery(t *testing.T) {
	ctx := context.Background()
	const project = "test"
	const query = "SELECT ERROR('boom')"

	bqServer, err := server.New(server.MemoryStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(project); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()
	base := fmt.Sprintf("%s/bigquery/v2/projects/%s", testServer.URL, project)

	post := func(url string, body any) *http.Response {
		t.Helper()
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		res, err := http.Post(url, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	errorReason := func(res *http.Response) (int, string) {
		t.Helper()
		defer res.Body.Close()
		var body struct {
			Error struct {
				Errors []struct {
					Reason string `json:"reason"`
				} `json:"errors"`
			} `json:"error"`
		}
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Error.Errors) == 0 {
			return res.StatusCode, ""
		}
		return res.StatusCode, body.Error.Errors[0].Reason
	}

	t.Run("jobs.insert status", func(t *testing.T) {
		res := post(base+"/jobs", &bigqueryv2.Job{
			JobReference:  &bigqueryv2.JobReference{ProjectId: project, JobId: "failing_job"},
			Configuration: &bigqueryv2.JobConfiguration{Query: &bigqueryv2.JobConfigurationQuery{Query: query}},
		})
		defer res.Body.Close()
		var job bigqueryv2.Job
		if err := json.NewDecoder(res.Body).Decode(&job); err != nil {
			t.Fatal(err)
		}
		if job.Status == nil || job.Status.ErrorResult == nil {
			t.Fatalf("job status has no errorResult: %+v", job.Status)
		}
		if got := job.Status.ErrorResult.Reason; got != "invalidQuery" {
			t.Fatalf("errorResult.reason = %q; want invalidQuery", got)
		}
	})

	t.Run("jobs.getQueryResults", func(t *testing.T) {
		res, err := http.Get(base + "/queries/failing_job")
		if err != nil {
			t.Fatal(err)
		}
		if code, reason := errorReason(res); code != http.StatusBadRequest || reason != "invalidQuery" {
			t.Fatalf("getQueryResults = %d %q; want 400 invalidQuery", code, reason)
		}
	})

	t.Run("jobs.query", func(t *testing.T) {
		res := post(base+"/queries", &bigqueryv2.QueryRequest{Query: query, UseLegacySql: new(bool)})
		if code, reason := errorReason(res); code != http.StatusBadRequest || reason != "invalidQuery" {
			t.Fatalf("jobs.query = %d %q; want 400 invalidQuery", code, reason)
		}
	})
}
