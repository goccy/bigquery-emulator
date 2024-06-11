package server_test

import (
    "context"
    "testing"

    "cloud.google.com/go/bigquery"
    "google.golang.org/api/iterator"
    "google.golang.org/api/option"

    "github.com/goccy/bigquery-emulator/server"
    "github.com/goccy/bigquery-emulator/types"
)

func TestUDF(t *testing.T) {
    ctx := context.Background()

    const (
        projectName = "test"
    )

    bqServer, err := server.New(server.TempStorage)
    if err != nil {
        t.Fatal(err)
    }
    if err := bqServer.Load(
        server.StructSource(
            types.NewProject(
                projectName,
                types.NewDataset(
                    "dataset1",
                    types.NewTable(
                        "table_a",
                        []*types.Column{
                            types.NewColumn("id", types.INTEGER),
                            types.NewColumn("name", types.STRING),
                        },
                        types.Data{
                            {
                                "id":   1,
                                "name": "alice",
                            },
                            {
                                "id":   2,
                                "name": "bob",
                            },
                        },
                    ),
                ),
            ),
        ),
    ); err != nil {
        t.Fatal(err)
    }
    testServer := bqServer.TestServer()
    defer func() {
        testServer.Close()
        bqServer.Close()
    }()

    client, err := bigquery.NewClient(
        ctx,
        projectName,
        option.WithEndpoint(testServer.URL),
        option.WithoutAuthentication(),
    )
    if err != nil {
        t.Fatal(err)
    }
    defer client.Close()

    query := client.Query(`
CREATE TEMP FUNCTION DoubleFn(x INT64)
RETURNS INT64
AS (
	x + x
);
SELECT
	id
FROM
	dataset1.table_a;
`)

    it, err := query.Read(ctx)
    if err != nil {
        t.Fatal(err)
    }
    for {
        var row []bigquery.Value
        if err := it.Next(&row); err != nil {
            if err == iterator.Done {
                break
            }
            t.Fatal(err)
        }
        t.Log("row = ", row)
    }
}
