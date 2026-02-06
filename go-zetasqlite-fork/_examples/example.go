package main

import (
	"database/sql"
	"fmt"

	_ "github.com/goccy/go-zetasqlite"
)

func main() {
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT * FROM UNNEST([?, ?, ?])`, 1, 2, 3)
	if err != nil {
		panic(err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			panic(err)
		}
		ids = append(ids, id)
	}
	fmt.Println(ids) // [1 2 3]
}
