package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/go-json"
)

func TestGenerateUUID(t *testing.T) {
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	query := `
SELECT
  dummy.id, GENERATE_UUID() AS UUID
FROM (
    SELECT 1 AS id
  UNION ALL
    SELECT 2 AS id
) dummy
`
	values := url.Values{}
	values.Set("q", query)

	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("%s/bigquery/v2/projects/test/queries", testServer.URL),
		strings.NewReader(fmt.Sprintf(`{"query": %q}`, query)),
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK, got %v", resp.Status)
	}

	var result struct {
		Rows []struct {
			F []struct {
				V interface{} `json:"v"`
			} `json:"f"`
		} `json:"rows"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	uuid1 := result.Rows[0].F[1].V.(string)
	uuid2 := result.Rows[1].F[1].V.(string)

	if uuid1 == uuid2 {
		t.Errorf("expected different UUIDs, got %s and %s", uuid1, uuid2)
	}
}
