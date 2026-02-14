package server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-json"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
)

func setupInsertAllTest(t *testing.T) (context.Context, *server.Server, *server.TestServer, *bigquery.Client) {
	t.Helper()
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		"test",
		types.NewDataset(
			"dataset1",
			types.NewTable(
				"table_insert_all",
				[]*types.Column{
					types.NewColumn("name", types.STRING),
				},
				nil,
			),
		),
	)
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	client, err := bigquery.NewClient(
		ctx,
		"test",
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, bqServer, testServer, client
}

func countInsertAllRows(t *testing.T, ctx context.Context, client *bigquery.Client) int64 {
	t.Helper()
	it, err := client.Query("SELECT COUNT(*) FROM dataset1.table_insert_all").Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		t.Fatal(err)
	}
	return row[0].(int64)
}

func callInsertAll(
	t *testing.T,
	serverURL string,
	body map[string]interface{},
) *bigqueryv2.TableDataInsertAllResponse {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(
		fmt.Sprintf("%s/projects/test/datasets/dataset1/tables/table_insert_all/insertAll", serverURL),
		"application/json",
		bytes.NewReader(b),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
	var insertRes bigqueryv2.TableDataInsertAllResponse
	if err := json.NewDecoder(resp.Body).Decode(&insertRes); err != nil {
		t.Fatal(err)
	}
	return &insertRes
}

func TestInsertAllSkipInvalidRows(t *testing.T) {
	ctx, bqServer, testServer, client := setupInsertAllTest(t)
	defer func() {
		client.Close()
		testServer.Close()
		_ = bqServer.Stop(ctx)
	}()

	insertRes := callInsertAll(t, testServer.URL, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"insertId": "valid",
				"json": map[string]interface{}{
					"name": "ok",
				},
			},
			{
				"insertId": "invalid",
				"json": map[string]interface{}{
					"name": map[string]interface{}{"nested": "invalid"},
				},
			},
		},
	})
	if got, want := len(insertRes.InsertErrors), 1; got != want {
		t.Fatalf("unexpected insert errors length: got %d want %d", got, want)
	}
	if got, want := insertRes.InsertErrors[0].Index, int64(1); got != want {
		t.Fatalf("unexpected insert error index: got %d want %d", got, want)
	}
	if got, want := countInsertAllRows(t, ctx, client), int64(0); got != want {
		t.Fatalf("unexpected row count without skipInvalidRows: got %d want %d", got, want)
	}

	insertRes = callInsertAll(t, testServer.URL, map[string]interface{}{
		"skipInvalidRows": true,
		"rows": []map[string]interface{}{
			{
				"insertId": "valid",
				"json": map[string]interface{}{
					"name": "ok",
				},
			},
			{
				"insertId": "invalid",
				"json": map[string]interface{}{
					"name": map[string]interface{}{"nested": "invalid"},
				},
			},
		},
	})
	if got, want := len(insertRes.InsertErrors), 1; got != want {
		t.Fatalf("unexpected insert errors length with skipInvalidRows: got %d want %d", got, want)
	}
	if got, want := countInsertAllRows(t, ctx, client), int64(1); got != want {
		t.Fatalf("unexpected row count with skipInvalidRows: got %d want %d", got, want)
	}
}

func TestInsertAllIgnoreUnknownValues(t *testing.T) {
	ctx, bqServer, testServer, client := setupInsertAllTest(t)
	defer func() {
		client.Close()
		testServer.Close()
		_ = bqServer.Stop(ctx)
	}()

	insertRes := callInsertAll(t, testServer.URL, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"insertId": "unknown-field",
				"json": map[string]interface{}{
					"name":  "ok",
					"extra": "ignored?",
				},
			},
		},
	})
	if got, want := len(insertRes.InsertErrors), 1; got != want {
		t.Fatalf("unexpected insert errors length without ignoreUnknownValues: got %d want %d", got, want)
	}
	if got, want := countInsertAllRows(t, ctx, client), int64(0); got != want {
		t.Fatalf("unexpected row count without ignoreUnknownValues: got %d want %d", got, want)
	}

	insertRes = callInsertAll(t, testServer.URL, map[string]interface{}{
		"ignoreUnknownValues": true,
		"rows": []map[string]interface{}{
			{
				"insertId": "unknown-field",
				"json": map[string]interface{}{
					"name":  "ok",
					"extra": "ignored",
				},
			},
		},
	})
	if got := len(insertRes.InsertErrors); got != 0 {
		t.Fatalf("unexpected insert errors with ignoreUnknownValues: got %d", got)
	}
	if got, want := countInsertAllRows(t, ctx, client), int64(1); got != want {
		t.Fatalf("unexpected row count with ignoreUnknownValues: got %d want %d", got, want)
	}
}
