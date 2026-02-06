package zetasqlite_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/go-cmp/cmp"

	zetasqlite "github.com/goccy/go-zetasqlite"
)

func TestDriver(t *testing.T) {
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS Singers (
  SingerId   INT64 NOT NULL,
  FirstName  STRING(1024),
  LastName   STRING(1024),
  SingerInfo BYTES(MAX)
)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT Singers (SingerId, FirstName, LastName) VALUES (1, 'John', 'Titor')`); err != nil {
		t.Fatal(err)
	}
	row := db.QueryRow("SELECT SingerID, FirstName, LastName FROM Singers WHERE SingerId = @id", 1)
	if row.Err() != nil {
		t.Fatal(row.Err())
	}
	var (
		singerID  int64
		firstName string
		lastName  string
	)
	if err := row.Scan(&singerID, &firstName, &lastName); err != nil {
		t.Fatal(err)
	}
	if singerID != 1 || firstName != "John" || lastName != "Titor" {
		t.Fatalf("failed to find row %v %v %v", singerID, firstName, lastName)
	}
	if _, err := db.Exec(`
CREATE VIEW IF NOT EXISTS SingerNames AS SELECT FirstName || ' ' || LastName AS Name FROM Singers`); err != nil {
		t.Fatal(err)
	}

	viewRow := db.QueryRow("SELECT Name FROM SingerNames LIMIT 1")
	if viewRow.Err() != nil {
		t.Fatal(viewRow.Err())
	}

	var name string

	if err := viewRow.Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "John Titor" {
		t.Fatalf("failed to find view row")
	}
}

func TestRegisterCustomDriver(t *testing.T) {
	sql.Register("zetasqlite-custom", &zetasqlite.ZetaSQLiteDriver{
		ConnectHook: func(conn *zetasqlite.ZetaSQLiteConn) error {
			return conn.SetNamePath([]string{"project-id", "datasetID"})
		},
	})
	db, err := sql.Open("zetasqlite-custom", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS tableID (Id INT64 NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT `project-id`.datasetID.tableID (Id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	row := db.QueryRow("SELECT * FROM project-id.datasetID.tableID WHERE Id = ?", 1)
	if row.Err() != nil {
		t.Fatal(row.Err())
	}
	var id int64
	if err := row.Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("failed to find row %v", id)
	}
}

func TestChangedCatalog(t *testing.T) {
	t.Run("table", func(t *testing.T) {
		db, err := sql.Open("zetasqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		result, err := db.Exec(`
CREATE TABLE IF NOT EXISTS Singers (
  SingerId   INT64 NOT NULL,
  FirstName  STRING(1024),
  LastName   STRING(1024),
  SingerInfo BYTES(MAX)
)`)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := db.Query(`DROP TABLE Singers`)
		if err != nil {
			t.Fatal(err)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		resultCatalog, err := zetasqlite.ChangedCatalogFromResult(result)
		if err != nil {
			t.Fatal(err)
		}
		if !resultCatalog.Changed() {
			t.Fatal("failed to get changed catalog")
		}
		if len(resultCatalog.Table.Added) != 1 {
			t.Fatal("failed to get created table spec")
		}
		if diff := cmp.Diff(resultCatalog.Table.Added[0].NamePath, []string{"Singers"}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
		rowsCatalog, err := zetasqlite.ChangedCatalogFromRows(rows)
		if err != nil {
			t.Fatal(err)
		}
		if !rowsCatalog.Changed() {
			t.Fatal("failed to get changed catalog")
		}
		if len(rowsCatalog.Table.Deleted) != 1 {
			t.Fatal("failed to get deleted table spec")
		}
		if diff := cmp.Diff(rowsCatalog.Table.Deleted[0].NamePath, []string{"Singers"}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
	t.Run("function", func(t *testing.T) {
		db, err := sql.Open("zetasqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		result, err := db.ExecContext(context.Background(), `CREATE FUNCTION ANY_ADD(x ANY TYPE, y ANY TYPE) AS ((x + 4) / y)`)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(context.Background(), `DROP FUNCTION ANY_ADD`)
		if err != nil {
			t.Fatal(err)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		resultCatalog, err := zetasqlite.ChangedCatalogFromResult(result)
		if err != nil {
			t.Fatal(err)
		}
		if !resultCatalog.Changed() {
			t.Fatal("failed to get changed catalog")
		}
		if len(resultCatalog.Function.Added) != 1 {
			t.Fatal("failed to get created function spec")
		}
		if diff := cmp.Diff(resultCatalog.Function.Added[0].NamePath, []string{"ANY_ADD"}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
		rowsCatalog, err := zetasqlite.ChangedCatalogFromRows(rows)
		if err != nil {
			t.Fatal(err)
		}
		if !rowsCatalog.Changed() {
			t.Fatal("failed to get changed catalog")
		}
		if len(rowsCatalog.Function.Deleted) != 1 {
			t.Fatal("failed to get deleted function spec")
		}
		if diff := cmp.Diff(rowsCatalog.Function.Deleted[0].NamePath, []string{"ANY_ADD"}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
}

func TestPreparedStatements(t *testing.T) {
	t.Run("prepared select", func(t *testing.T) {
		db, err := sql.Open("zetasqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS Singers (
  SingerId   INT64 NOT NULL,
  FirstName  STRING(1024),
  LastName   STRING(1024),
  SingerInfo BYTES(MAX)
)`); err != nil {
			t.Fatal(err)
		}
		stmt, err := db.Prepare("SELECT * FROM Singers WHERE SingerId = ?")
		if err != nil {
			t.Fatal(err)
		}
		rows, err := stmt.Query("123")
		if err != nil {
			t.Fatal(err)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if rows.Next() {
			t.Fatal("found unexpected row; expected no rows")
		}
	})
	t.Run("prepared insert", func(t *testing.T) {
		db, err := sql.Open("zetasqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS Items (ItemId   INT64 NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("INSERT `Items` (`ItemId`) VALUES (123)"); err != nil {
			t.Fatal(err)
		}

		// Test that executing without args fails
		_, err = db.Exec("INSERT `Items` (`ItemId`) VALUES (?)")
		if err == nil {
			t.Fatal("expected error when inserting without args; got no error")
		}

		stmt, err := db.Prepare("INSERT `Items` (`ItemId`) VALUES (?)")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stmt.Exec(456); err != nil {
			t.Fatal(err)
		}

		stmt, err = db.PrepareContext(context.Background(), "INSERT `Items` (`ItemId`) VALUES (?)")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stmt.Exec(456); err != nil {
			t.Fatal(err)
		}

		rows, err := db.Query("SELECT * FROM Items WHERE ItemId = 456")
		if err != nil {
			t.Fatal(err)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatal("expected no rows; expected one row")
		}
	})
}
