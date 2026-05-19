package server_test

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/option"
)

// TestIssue468CountStarOverEmptyTable is a regression test for
// https://github.com/goccy/bigquery-emulator/issues/468: COUNT(*) over
// an empty table returned NULL instead of the INT64 0, breaking every
// client that scans the column into a non-pointer integer.
//
// COUNT is non-nullable per the GoogleSQL spec; SUM over an empty input
// is legitimately NULL and must stay that way.
func TestIssue468CountStarOverEmptyTable(t *testing.T) {
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

	client, err := bigquery.NewClient(ctx, "test",
		option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ds := client.Dataset("ds468")
	if err := ds.Create(ctx, nil); err != nil {
		t.Fatal(err)
	}
	empty := ds.Table("empty_tbl")
	schema := bigquery.Schema{{Name: "n", Type: bigquery.IntegerFieldType}}
	if err := empty.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		t.Fatal(err)
	}

	// Every COUNT/COUNTIF form over an empty table must scan into a
	// plain int64 as 0 — issue #468 covers the whole family, not only
	// COUNT(*).
	for _, q := range []string{
		"SELECT COUNT(*) AS n FROM ds468.empty_tbl",
		"SELECT COUNT(n) AS n FROM ds468.empty_tbl",
		"SELECT COUNTIF(n > 0) AS n FROM ds468.empty_tbl",
		"SELECT COUNT(*) AS n FROM ds468.empty_tbl WHERE FALSE",
	} {
		t.Run("scan into int64: "+q, func(t *testing.T) {
			it, err := client.Query(q).Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var row struct{ N int64 }
			if err := it.Next(&row); err != nil {
				t.Fatalf("Next: %v", err)
			}
			if row.N != 0 {
				t.Fatalf("%q over empty table = %d; want 0", q, row.N)
			}
		})
	}

	t.Run("COUNT(*) reports a non-NULL value", func(t *testing.T) {
		it, err := client.Query("SELECT COUNT(*) AS n FROM ds468.empty_tbl").Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row struct{ N bigquery.NullInt64 }
		if err := it.Next(&row); err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !row.N.Valid {
			t.Fatal("COUNT(*) over empty table returned NULL; want INT64 0")
		}
		if row.N.Int64 != 0 {
			t.Fatalf("COUNT(*) over empty table = %d; want 0", row.N.Int64)
		}
	})

	t.Run("SUM over empty table stays NULL", func(t *testing.T) {
		it, err := client.Query("SELECT SUM(n) AS n FROM ds468.empty_tbl").Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row struct{ N bigquery.NullInt64 }
		if err := it.Next(&row); err != nil {
			t.Fatalf("Next: %v", err)
		}
		if row.N.Valid {
			t.Fatalf("SUM over empty table = %d; want NULL", row.N.Int64)
		}
	})
}
