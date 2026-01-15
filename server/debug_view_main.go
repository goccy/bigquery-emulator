//go:build ignore

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/goccy/go-zetasqlite"
)

func main() {
	ctx := context.Background()

	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create base table
	_, err = db.ExecContext(ctx, "CREATE TABLE base_table (id INT64, name STRING, value INT64)")
	if err != nil {
		log.Fatal("create table:", err)
	}

	// Insert data
	_, err = db.ExecContext(ctx, "INSERT INTO base_table (id, name, value) VALUES (1, 'test', 100)")
	if err != nil {
		log.Fatal("insert:", err)
	}

	// Create initial view with 2 columns
	_, err = db.ExecContext(ctx, "CREATE VIEW test_view AS SELECT id, name FROM base_table")
	if err != nil {
		log.Fatal("create view:", err)
	}

	// Query initial view
	rows, err := db.QueryContext(ctx, "SELECT * FROM test_view")
	if err != nil {
		log.Fatal("query initial view:", err)
	}
	cols, _ := rows.Columns()
	fmt.Printf("Initial view columns: %v\n", cols)
	rows.Close()

	// Query the SQLite view directly to see what's in it
	sqliteRows, err := db.QueryContext(ctx, "SELECT sql FROM sqlite_master WHERE type='view' AND name='test_view'")
	if err != nil {
		fmt.Printf("Error querying sqlite_master: %v\n", err)
	} else {
		if sqliteRows.Next() {
			var sqlDef string
			sqliteRows.Scan(&sqlDef)
			fmt.Printf("Initial SQLite view definition: %s\n", sqlDef)
		}
		sqliteRows.Close()
	}

	// Replace view with 3 columns using CREATE OR REPLACE
	_, err = db.ExecContext(ctx, "CREATE OR REPLACE VIEW test_view AS SELECT id, name, value FROM base_table")
	if err != nil {
		log.Fatal("replace view:", err)
	}

	// Query the SQLite view directly after replacement
	sqliteRows, err = db.QueryContext(ctx, "SELECT sql FROM sqlite_master WHERE type='view' AND name='test_view'")
	if err != nil {
		fmt.Printf("Error querying sqlite_master after replace: %v\n", err)
	} else {
		if sqliteRows.Next() {
			var sqlDef string
			sqliteRows.Scan(&sqlDef)
			fmt.Printf("Replaced SQLite view definition: %s\n", sqlDef)
		}
		sqliteRows.Close()
	}

	// Query replaced view
	rows, err = db.QueryContext(ctx, "SELECT * FROM test_view")
	if err != nil {
		log.Fatal("query replaced view:", err)
	}
	cols, _ = rows.Columns()
	fmt.Printf("Replaced view columns: %v\n", cols)

	// Also check actual data
	if rows.Next() {
		var id, value int64
		var name string
		// Try to scan 3 columns
		err = rows.Scan(&id, &name, &value)
		if err != nil {
			fmt.Printf("Error scanning 3 columns: %v\n", err)
		} else {
			fmt.Printf("Data from replaced view: id=%d, name=%s, value=%d\n", id, name, value)
		}
	}
	rows.Close()

	fmt.Println("Test completed successfully!")
}
