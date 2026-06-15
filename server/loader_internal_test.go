package server

import (
	"context"
	"testing"

	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/types"
)

func TestAddProjectNormalizesRepeatedRecordData(t *testing.T) {
	s, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := logger.WithLogger(context.Background(), s.logger)

	project := types.NewProject("test",
		types.NewDataset("dataset",
			types.NewTable("repro", []*types.Column{
				types.NewColumn("items", types.RECORD,
					types.ColumnMode(types.RepeatedMode),
					types.ColumnFields(
						types.NewColumn("id", types.INTEGER),
						types.NewColumn("name", types.STRING),
						types.NewColumn("skip", types.BOOLEAN),
						types.NewColumn("enabled", types.BOOLEAN),
					),
				),
			}, types.Data{{
				"items": []map[string]interface{}{{"id": 1, "name": "item-1", "skip": false, "enabled": true}},
			}}),
		),
	)
	if err := s.addProject(ctx, project); err != nil {
		t.Fatal(err)
	}

	conn, err := s.connMgr.Connection(ctx, "test", "dataset")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackIfNotCommitted()

	res, err := s.contentRepo.Query(ctx, tx, "test", "dataset", `
SELECT item.id, item.name, item.skip, item.enabled
FROM repro
LEFT JOIN UNNEST(items) AS item
`, nil)

	if err != nil {
		t.Fatal(err)
	}
	if res.TotalRows != 1 || len(res.Rows) != 1 || len(res.Rows[0].F) != 4 {
		t.Fatalf("rows = %#v; want 1 row with 4 fields", res.Rows)
	}
	want := []string{"1", "item-1", "false", "true"}
	for i, want := range want {
		if got := res.Rows[0].F[i].V; got != want {
			t.Errorf("field %d = %q; want %q", i, got, want)
		}
	}
}
