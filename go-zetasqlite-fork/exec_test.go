package zetasqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	zetasqlite "github.com/goccy/go-zetasqlite"
)

func TestExec(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, test := range []struct {
		name        string
		query       string
		args        []interface{}
		expectedErr bool
	}{
		{
			name: "create table with all types",
			query: `
CREATE TABLE _table_a (
 intValue        INT64,
 boolValue       BOOL,
 doubleValue     DOUBLE,
 floatValue      FLOAT,
 stringValue     STRING,
 bytesValue      BYTES,
 numericValue    NUMERIC,
 bignumericValue BIGNUMERIC,
 intervalValue   INTERVAL,
 dateValue       DATE,
 datetimeValue   DATETIME,
 timeValue       TIME,
 timestampValue  TIMESTAMP
)`,
		},
		{
			name: "create table as select",
			query: `
CREATE TABLE foo ( id STRING PRIMARY KEY NOT NULL, name STRING );
CREATE TABLE bar ( id STRING, name STRING, PRIMARY KEY (id, name) );
CREATE OR REPLACE TABLE new_table_as_select AS (
  SELECT t1.id, t2.name FROM foo t1 JOIN bar t2 ON t1.id = t2.id
);
`,
		},
		{
			name: "recreate table",
			query: `
CREATE OR REPLACE TABLE recreate_table ( a string );
DROP TABLE recreate_table;
CREATE TABLE recreate_table ( b string );
INSERT recreate_table (b) VALUES ('hello');
`,
		},
		{
			name: "insert select",
			query: `
CREATE OR REPLACE TABLE TableA(product string, quantity int64);
INSERT TableA (product, quantity) SELECT 'top load washer', 10;
INSERT INTO TableA (product, quantity) SELECT * FROM UNNEST([('microwave', 20), ('dishwasher', 30)]);
`,
		},
		{
			name: "create view",
			query: `
CREATE VIEW _view_a AS SELECT * FROM TableA
`,
		},
		{
			name: "drop view",
			query: `
DROP VIEW IF EXISTS _view_a
`,
		},
		{
			name: "transaction",
			query: `
CREATE OR REPLACE TABLE Inventory
(
 product string,
 quantity int64,
 supply_constrained bool
);

CREATE OR REPLACE TABLE NewArrivals
(
 product string,
 quantity int64,
 warehouse string
);

INSERT Inventory (product, quantity)
VALUES('top load washer', 10),
     ('front load washer', 20),
     ('dryer', 30),
     ('refrigerator', 10),
     ('microwave', 20),
     ('dishwasher', 30);

INSERT NewArrivals (product, quantity, warehouse)
VALUES('top load washer', 100, 'warehouse #1'),
     ('dryer', 200, 'warehouse #2'),
     ('oven', 300, 'warehouse #1');

BEGIN TRANSACTION;

CREATE TEMP TABLE tmp AS SELECT * FROM NewArrivals WHERE warehouse = 'warehouse #1';
DELETE NewArrivals WHERE warehouse = 'warehouse #1';
MERGE Inventory AS I
USING tmp AS T
ON I.product = T.product
WHEN NOT MATCHED THEN
 INSERT(product, quantity, supply_constrained)
 VALUES(product, quantity, false)
WHEN MATCHED THEN
 UPDATE SET quantity = I.quantity + T.quantity;

TRUNCATE TABLE tmp;

COMMIT TRANSACTION;
`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, test.query); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNestedStructFieldAccess(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
CREATE TABLE table (
  id INT64,
  value STRUCT<fieldA STRING, fieldB STRUCT<fieldX STRING, fieldY STRING>>
)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		`INSERT table (id, value) VALUES (?, ?)`,
		123,
		map[string]interface{}{
			"fieldB": map[string]interface{}{
				"fieldY": "bar",
			},
		},
	); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, "SELECT value, value.fieldB, value.fieldB.fieldY FROM table")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type queryRow struct {
		Value  interface{}
		FieldB []map[string]interface{}
		FieldY string
	}
	var results []*queryRow
	for rows.Next() {
		var (
			value  interface{}
			fieldB []map[string]interface{}
			fieldY string
		)
		if err := rows.Scan(&value, &fieldB, &fieldY); err != nil {
			t.Fatal(err)
		}
		results = append(results, &queryRow{
			Value:  value,
			FieldB: fieldB,
			FieldY: fieldY,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("failed to get results")
	}
	if results[0].FieldY != "bar" {
		t.Fatalf("failed to get fieldY")
	}
}

func TestCreateTempTable(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE TEMP TABLE tmp_table (id INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TEMP TABLE tmp_table (id INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE tmp_table (id INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE tmp_table (id INT64)"); err == nil {
		t.Fatal("expected error")
	}
}

func TestWildcardTable(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.table_a` AS SELECT specialName FROM UNNEST (['alice_a', 'bob_a']) as specialName",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.table_b` AS SELECT name FROM UNNEST(['alice_b', 'bob_b']) as name",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.table_c` AS SELECT name FROM UNNEST(['alice_c', 'bob_c']) as name",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE `project.dataset.other_d` AS SELECT name FROM UNNEST(['alice_d', 'bob_d']) as name",
	); err != nil {
		t.Fatal(err)
	}
	t.Run("with first identifier", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "SELECT name, _TABLE_SUFFIX FROM `project.dataset.table_*` WHERE name LIKE 'alice%' OR name IS NULL")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		type queryRow struct {
			Name   *string
			Suffix string
		}
		var results []*queryRow
		for rows.Next() {
			var (
				name   *string
				suffix string
			)
			if err := rows.Scan(&name, &suffix); err != nil {
				t.Fatal(err)
			}
			results = append(results, &queryRow{
				Name:   name,
				Suffix: suffix,
			})
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		stringPtr := func(v string) *string { return &v }
		if diff := cmp.Diff(results, []*queryRow{
			{Name: stringPtr("alice_c"), Suffix: "c"},
			{Name: stringPtr("alice_b"), Suffix: "b"},
			{Name: nil, Suffix: "a"},
			{Name: nil, Suffix: "a"},
		}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
	t.Run("without first identifier", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "SELECT name, _TABLE_SUFFIX FROM `dataset.table_*` WHERE name LIKE 'alice%' OR name IS NULL")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		type queryRow struct {
			Name   *string
			Suffix string
		}
		var results []*queryRow
		for rows.Next() {
			var (
				name   *string
				suffix string
			)
			if err := rows.Scan(&name, &suffix); err != nil {
				t.Fatal(err)
			}
			results = append(results, &queryRow{
				Name:   name,
				Suffix: suffix,
			})
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		stringPtr := func(v string) *string { return &v }
		if diff := cmp.Diff(results, []*queryRow{
			{Name: stringPtr("alice_c"), Suffix: "c"},
			{Name: stringPtr("alice_b"), Suffix: "b"},
			{Name: nil, Suffix: "a"},
			{Name: nil, Suffix: "a"},
		}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
}

func TestTemplatedArgFunc(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Run("simple any arguments", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`CREATE FUNCTION ANY_ADD(x ANY TYPE, y ANY TYPE) AS ((x + 4) / y)`,
		); err != nil {
			t.Fatal(err)
		}
		t.Run("int64", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "SELECT ANY_ADD(3, 4)")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			rows.Next()
			var num float64
			if err := rows.Scan(&num); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(num) != "1.75" {
				t.Fatalf("failed to get max number. got %f", num)
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
		})
		t.Run("float64", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "SELECT ANY_ADD(18.22, 11.11)")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			rows.Next()
			var num float64
			if err := rows.Scan(&num); err != nil {
				t.Fatal(err)
			}
			if num != 2.0 {
				t.Fatalf("failed to get max number. got %f", num)
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
		})
	})
	t.Run("array any arguments", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`CREATE FUNCTION MAX_FROM_ARRAY(arr ANY TYPE) as (( SELECT MAX(x) FROM UNNEST(arr) as x ))`,
		); err != nil {
			t.Fatal(err)
		}
		t.Run("int64", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "SELECT MAX_FROM_ARRAY([1, 4, 2, 3])")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			rows.Next()
			var num int64
			if err := rows.Scan(&num); err != nil {
				t.Fatal(err)
			}
			if num != 4 {
				t.Fatalf("failed to get max number. got %d", num)
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
		})
		t.Run("float64", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "SELECT MAX_FROM_ARRAY([1.234, 3.456, 4.567, 2.345])")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			rows.Next()
			var num float64
			if err := rows.Scan(&num); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(num) != "4.567" {
				t.Fatalf("failed to get max number. got %f", num)
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
		})
	})
}

func TestJavaScriptUDF(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Run("operation", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`
CREATE FUNCTION multiplyInputs(x FLOAT64, y FLOAT64)
RETURNS FLOAT64
LANGUAGE js
AS r"""
  return x*y;
"""`,
		); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(ctx, `
WITH numbers AS
  (SELECT 1 AS x, 5 as y UNION ALL SELECT 2 AS x, 10 as y UNION ALL SELECT 3 as x, 15 as y)
  SELECT x, y, multiplyInputs(x, y) AS product FROM numbers`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		results := [][]float64{}
		for rows.Next() {
			var (
				x, y, retVal float64
			)
			if err := rows.Scan(&x, &y, &retVal); err != nil {
				t.Fatal(err)
			}
			results = append(results, []float64{x, y, retVal})
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if diff := cmp.Diff(results, [][]float64{
			{1, 5, 5},
			{2, 10, 20},
			{3, 15, 45},
		}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
	t.Run("function", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`
CREATE FUNCTION SumFieldsNamedFoo(json_row STRING)
RETURNS FLOAT64
LANGUAGE js
AS r"""
  function SumFoo(obj) {
    var sum = 0;
    for (var field in obj) {
      if (obj.hasOwnProperty(field) && obj[field] != null) {
        if (typeof obj[field] == "object") {
          sum += SumFoo(obj[field]);
        } else if (field == "foo") {
          sum += obj[field];
        }
      }
    }
    return sum;
  }
  var row = JSON.parse(json_row);
  return SumFoo(row);
"""`,
		); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(ctx, `
WITH Input AS (
  SELECT
    STRUCT(1 AS foo, 2 AS bar, STRUCT('foo' AS x, 3.14 AS foo) AS baz) AS s,
    10 AS foo
  UNION ALL
  SELECT
    NULL,
    4 AS foo
  UNION ALL
  SELECT
    STRUCT(NULL, 2 AS bar, STRUCT('fizz' AS x, 1.59 AS foo) AS baz) AS s,
    NULL AS foo
) SELECT TO_JSON_STRING(t), SumFieldsNamedFoo(TO_JSON_STRING(t)) FROM Input AS t`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		type queryRow struct {
			JSONRow string
			Sum     float64
		}
		results := []*queryRow{}
		for rows.Next() {
			var (
				jsonRow string
				sum     float64
			)
			if err := rows.Scan(&jsonRow, &sum); err != nil {
				t.Fatal(err)
			}
			results = append(results, &queryRow{JSONRow: jsonRow, Sum: sum})
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if diff := cmp.Diff(results, []*queryRow{
			{JSONRow: `{"s":{"foo":1,"bar":2,"baz":{"x":"foo","foo":3.14}},"foo":10}`, Sum: 14.14},
			{JSONRow: `{"s":null,"foo":4}`, Sum: 4},
			{JSONRow: `{"s":{"foo":null,"bar":2,"baz":{"x":"fizz","foo":1.59}},"foo":null}`, Sum: 1.59},
		}); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
	t.Run("multibytes", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`
CREATE FUNCTION JS_JOIN(v ARRAY<STRING>)
RETURNS STRING
LANGUAGE js
AS r"""
  return v.join(' ');
"""`,
		); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(ctx, `SELECT JS_JOIN(['あいうえお', 'かきくけこ'])`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("failed to get result")
		}
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if v != "あいうえお かきくけこ" {
			t.Fatalf("got %s", v)
		}
	})
	t.Run("struct", func(t *testing.T) {
		if _, err := db.ExecContext(
			ctx,
			`
CREATE FUNCTION structToArray(obj STRUCT<idx INT64, name STRING>)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  let result = []

  result.push(obj["idx"])
  result.push(obj["name"])
  return result;
""";
`,
		); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(ctx, `SELECT * FROM UNNEST(structToArray(STRUCT(1,"A")))`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		var results []string
		for i := 0; i < 2; i++ {
			if !rows.Next() {
				t.Fatal("failed to get result")
			}
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			results = append(results, v)
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if !reflect.DeepEqual(results, []string{"1", "A"}) {
			t.Fatalf("failed to get results")
		}
	})
}
