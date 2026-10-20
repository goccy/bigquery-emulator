package server_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
)

// The resumable upload session URL must point at the host the client used,
// not at the listen address: with --host=0.0.0.0 the client would otherwise
// PUT to http://0.0.0.0:9050, which Windows rejects (WinError 10049).
func TestResumableUploadLocationUsesRequestHost(t *testing.T) {
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test", types.NewDataset("ds")))); err != nil {
		t.Fatal(err)
	}
	ts := bqServer.TestServer()
	t.Cleanup(func() {
		ts.Close()
		bqServer.Stop(ctx)
	})
	baseURL := ts.URL
	for _, tc := range []struct {
		name    string
		headers map[string]string
		want    string
	}{
		{name: "host header", want: "http://localhost:9050"},
		{name: "forwarded https", headers: map[string]string{"X-Forwarded-Proto": "https"}, want: "https://localhost:9050"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jobID := "location_" + strings.ReplaceAll(tc.name, " ", "_")
			body := `{"jobReference":{"projectId":"test","jobId":"` + jobID + `"},"configuration":{"load":{"destinationTable":{"projectId":"test","datasetId":"ds","tableId":"t"}}}}`
			req, err := http.NewRequest(http.MethodPost, baseURL+"/upload/bigquery/v2/projects/test/jobs?uploadType=resumable", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Host = "localhost:9050"
			req.Header.Set("Content-Type", "application/json")
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("start resumable upload: %d", resp.StatusCode)
			}
			want := tc.want + "/upload/bigquery/v2/projects/test/jobs?uploadType=resumable&upload_id=" + jobID
			if got := resp.Header.Get("Location"); got != want {
				t.Fatalf("Location = %q, want %q", got, want)
			}
		})
	}
}
