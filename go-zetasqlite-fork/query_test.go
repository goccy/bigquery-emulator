package zetasqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	zetasqlite "github.com/goccy/go-zetasqlite"
)

func TestQuery(t *testing.T) {
	t.Setenv("TZ", "UTC")
	now := time.Now()
	ctx := context.Background()
	ctx = zetasqlite.WithCurrentTime(ctx, now)
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	floatCmpOpt := cmp.Comparer(func(x, y float64) bool {
		if x == y {
			return true
		}
		delta := math.Abs(x - y)
		mean := math.Abs(x+y) / 2.0
		return delta/mean < 0.00001
	})
	for _, test := range []struct {
		name         string
		query        string
		args         []interface{}
		expectedRows [][]interface{}
		expectedErr  string
	}{
		// Regression test for https://github.com/goccy/go-zetasqlite/issues/191
		{
			name: "distinct union",
			query: `WITH toks AS (SELECT true AS x, 1 AS y)
					SELECT DISTINCT x, x as y FROM toks`,
			expectedRows: [][]interface{}{{true, true}},
		},
		{
			name: "with scan union all",
			query: `(WITH toks AS (SELECT 1 AS x) SELECT x FROM toks)
UNION ALL
(WITH toks2 AS (SELECT 2 AS x) SELECT x FROM toks2)`,
			expectedRows: [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name: "having with union all",
			query: `(WITH toks AS (SELECT 1 AS x) SELECT COUNT(x) AS total_rows FROM toks WHERE x > 0 HAVING total_rows >= 0)
UNION ALL
(WITH toks2 AS (SELECT 2 AS x) SELECT COUNT(x) AS total_rows FROM toks2 WHERE x > 0 HAVING total_rows >= 0)`,
			expectedRows: [][]interface{}{{int64(1)}, {int64(1)}},
		},
		// priority 2 operator
		{
			name:         "unary plus operator",
			query:        "SELECT +1",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "unary minus operator",
			query:        "SELECT -2",
			expectedRows: [][]interface{}{{int64(-2)}},
		},
		{
			name:         "bit not operator",
			query:        "SELECT ~1",
			expectedRows: [][]interface{}{{int64(-2)}},
		},
		// priority 3 operator
		{
			name:         "mul operator",
			query:        "SELECT 2 * 3",
			expectedRows: [][]interface{}{{int64(6)}},
		},
		{
			name:         "div operator",
			query:        "SELECT 10 / 2",
			expectedRows: [][]interface{}{{float64(5)}},
		},
		{
			name:         "concat string operator",
			query:        `SELECT "a" || "b"`,
			expectedRows: [][]interface{}{{"ab"}},
		},
		{
			name:         "concat array operator",
			query:        `SELECT [1, 2] || [3, 4]`,
			expectedRows: [][]interface{}{{[]interface{}{int64(1), int64(2), int64(3), int64(4)}}},
		},

		// priority 4 operator
		{
			name:         "add operator",
			query:        "SELECT 1 + 1",
			expectedRows: [][]interface{}{{int64(2)}},
		},
		{
			name:         "sub operator",
			query:        "SELECT 1 - 2",
			expectedRows: [][]interface{}{{int64(-1)}},
		},
		// priority 5 operator
		{
			name:         "left shift operator",
			query:        "SELECT 1 << 2",
			expectedRows: [][]interface{}{{int64(4)}},
		},
		{
			name:         "right shift operator",
			query:        "SELECT 4 >> 1",
			expectedRows: [][]interface{}{{int64(2)}},
		},
		// priority 6 operator
		{
			name:         "bit and operator",
			query:        "SELECT 3 & 1",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		// priority 7 operator
		{
			name:         "bit xor operator",
			query:        "SELECT 10 ^ 12",
			expectedRows: [][]interface{}{{int64(6)}},
		},
		// priority 8 operator
		{
			name:         "bit or operator",
			query:        "SELECT 1 | 2",
			expectedRows: [][]interface{}{{int64(3)}},
		},
		// priority 9 operator
		{
			name:         "eq operator",
			query:        "SELECT 100 = 100",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "lt operator",
			query:        "SELECT 10 < 100",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "gt operator",
			query:        "SELECT 100 > 10",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "lte operator",
			query:        "SELECT 10 <= 10",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "gte operator",
			query:        "SELECT 10 >= 10",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "ne operator",
			query:        "SELECT 100 != 10",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "ne operator2",
			query:        "SELECT 100 <> 10",
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "like operator",
			query:        `SELECT "abcd" LIKE "a%d"`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "like operator - special regex characters",
			query:        `SELECT 'my*string' LIKE '%%*%%', 'my*string' LIKE '%%([][%';`,
			expectedRows: [][]interface{}{{true, false}},
		},
		{
			name:         "like operator2",
			query:        `SELECT "abcd" LIKE "%a%"`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "like operator3",
			query:        `SELECT "abcd" LIKE "%b%"`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "like operator4",
			query:        `SELECT "dog" LIKE "o%"`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "not like operator",
			query:        `SELECT "abcd" NOT LIKE "a%d"`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "between operator",
			query:        `SELECT DATE "2022-09-10" BETWEEN "2022-09-01" and "2022-10-01"`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "not between operator",
			query:        `SELECT DATE "2020-09-10" NOT BETWEEN "2022-09-01" and "2022-10-01"`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:  "in operator",
			query: `SELECT 3 IN (1, 2, 3, 4), null IN (1), null IN (null)`,
			// When left-hand side is null, null is always returned
			expectedRows: [][]interface{}{{true, nil, nil}},
		},
		{
			name:  "not in operator",
			query: `SELECT 5 NOT IN (1, 2, 3, 4), null NOT IN (1), null NOT IN (null)`,
			// When left-hand side is null, null is always returned
			expectedRows: [][]interface{}{{true, nil, nil}},
		},
		{
			name:         "is null operator",
			query:        `SELECT NULL IS NULL`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is not null operator",
			query:        `SELECT 1 IS NOT NULL`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is true operator",
			query:        `SELECT true IS TRUE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is not true operator",
			query:        `SELECT false IS NOT TRUE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is false operator",
			query:        `SELECT false IS FALSE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is not false operator",
			query:        `SELECT true IS NOT FALSE`,
			expectedRows: [][]interface{}{{true}},
		},
		// priority 10 operator
		{
			name:         "not operator",
			query:        `SELECT NOT 1 = 2`,
			expectedRows: [][]interface{}{{true}},
		},
		// priority 11 operator
		{
			name:         "and operator",
			query:        `SELECT 1 = 1 AND 2 = 2`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "and operator with true and true",
			query:        `SELECT TRUE AND TRUE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "and operator with true and false",
			query:        `SELECT TRUE AND FALSE`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "and operator with true and null",
			query:        `SELECT TRUE AND NULL`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "and operator with false and true",
			query:        `SELECT FALSE AND TRUE`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "and operator with false and false",
			query:        `SELECT FALSE AND FALSE`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "and operator with false and null",
			query:        `SELECT FALSE AND NULL`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "and operator with null and true",
			query:        `SELECT NULL AND TRUE`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "and operator with null and false",
			query:        `SELECT NULL AND FALSE`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "and operator with null and null",
			query:        `SELECT NULL AND NULL`,
			expectedRows: [][]interface{}{{nil}},
		},

		// priority 12 operator
		{
			name:         "or operator",
			query:        `SELECT 1 = 2 OR 1 = 1`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "or operator with true and true",
			query:        `SELECT TRUE OR TRUE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "or operator with true and false",
			query:        `SELECT TRUE OR FALSE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "or operator with true and null",
			query:        `SELECT TRUE OR NULL`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "or operator with false and true",
			query:        `SELECT FALSE OR TRUE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "or operator with false and false",
			query:        `SELECT FALSE OR FALSE`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "or operator with false and null",
			query:        `SELECT FALSE OR NULL`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "or operator with null and true",
			query:        `SELECT NULL OR TRUE`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "or operator with null and false",
			query:        `SELECT NULL OR FALSE`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "or operator with null and null",
			query:        `SELECT NULL OR NULL`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "exists",
			query:        `SELECT EXISTS ( SELECT val FROM UNNEST([1, 2, 3]) AS val WHERE val = 1 )`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "not exists",
			query:        `SELECT EXISTS ( SELECT val FROM UNNEST([1, 2, 3]) AS val WHERE val = 4 )`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "is distinct from with 1 and 2",
			query:        `SELECT 1 IS DISTINCT FROM 2`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is distinct from with 1 and null",
			query:        `SELECT 1 IS DISTINCT FROM NULL`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is not distinct from with 1 and 1",
			query:        `SELECT 1 IS NOT DISTINCT FROM 1`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is not distinct from with null and null",
			query:        `SELECT NULL IS NOT DISTINCT FROM NULL`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "is distinct from with null and null",
			query:        `SELECT NULL IS DISTINCT FROM NULL`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "is distinct from with 1 and 1",
			query:        `SELECT 1 IS DISTINCT FROM 1`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "is not distinct from with 1 and 2",
			query:        `SELECT 1 IS NOT DISTINCT FROM 2`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "is not distinct from with 1 and null",
			query:        `SELECT 1 IS NOT DISTINCT FROM NULL`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name: "case-when",
			query: `
SELECT
  val,
  CASE val
    WHEN 1 THEN 'one'
    WHEN 2 THEN 'two'
    WHEN 3 THEN 'three'
    ELSE 'four'
    END
FROM UNNEST([1, 2, 3, 4]) AS val`,
			expectedRows: [][]interface{}{
				{int64(1), "one"},
				{int64(2), "two"},
				{int64(3), "three"},
				{int64(4), "four"},
			},
		},
		{
			name: "case-when with compare",
			query: `
SELECT
  val,
  CASE
    WHEN val > 3 THEN 'four'
    WHEN val > 2 THEN 'three'
    WHEN val > 1 THEN 'two'
    ELSE 'one'
    END
FROM UNNEST([1, 2, 3, 4]) AS val`,
			expectedRows: [][]interface{}{
				{int64(1), "one"},
				{int64(2), "two"},
				{int64(3), "three"},
				{int64(4), "four"},
			},
		},
		{
			name:         "coalesce",
			query:        `SELECT COALESCE('A', 'B', 'C')`,
			expectedRows: [][]interface{}{{"A"}},
		},
		{
			name:         "coalesce with null",
			query:        `SELECT COALESCE(NULL, 'B', 'C')`,
			expectedRows: [][]interface{}{{"B"}},
		},
		{
			name:         "coalesce with all nulls",
			query:        `SELECT COALESCE(NULL, NULL, NULL)`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "if return int64",
			query:        `SELECT IF("a" = "b", 1, 2)`,
			expectedRows: [][]interface{}{{int64(2)}},
		},
		{
			name:         "if return string",
			query:        `SELECT IF("a" = "a", "true", "false")`,
			expectedRows: [][]interface{}{{"true"}},
		},
		{
			name:         "if with case that causes errors",
			query:        `SELECT IF(FALSE, ERROR("error case!"), "false")`,
			expectedRows: [][]interface{}{{"false"}},
		},
		{
			name:         "ifnull with case that causes errors",
			query:        `SELECT IFNULL("STRING", ERROR("error case!"))`,
			expectedRows: [][]interface{}{{"STRING"}},
		},
		{
			name:         "case with case that causes errors",
			query:        `SELECT CASE WHEN FALSE THEN ERROR("error case!") ELSE "false" END`,
			expectedRows: [][]interface{}{{"false"}},
		},
		{
			name:         "ifnull",
			query:        `SELECT IFNULL(10, 0)`,
			expectedRows: [][]interface{}{{int64(10)}},
		},
		{
			name:         "ifnull with null",
			query:        `SELECT IFNULL(NULL, 0)`,
			expectedRows: [][]interface{}{{int64(0)}},
		},
		{
			name:         "nullif true",
			query:        `SELECT NULLIF(0, 0)`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "nullif false",
			query:        `SELECT NULLIF(10, 0)`,
			expectedRows: [][]interface{}{{int64(10)}},
		},
		{
			name:         "nullif null",
			query:        `SELECT NULLIF(null, 0)`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "rounding",
			query:        `SELECT ROUND(2.0), ROUND(2.3), ROUND(2.8), ROUND(2.5), ROUND(-2.3), ROUND(-2.8), ROUND(-2.5)`,
			expectedRows: [][]interface{}{{float64(2.0), float64(2.0), float64(3.0), float64(3.0), float64(-2.0), float64(-3.0), float64(-3.0)}},
		},
		{
			name:         "rounding precision",
			query:        `SELECT ROUND(123.7, -1), ROUND(1.235, 2)`,
			expectedRows: [][]interface{}{{float64(120.0), float64(1.24)}},
		},
		{
			name: "with clause",
			query: `
WITH sub1 AS (SELECT ["a", "b"]),
     sub2 AS (SELECT ["c", "d"])
SELECT * FROM sub1
UNION ALL
SELECT * FROM sub2`,
			expectedRows: [][]interface{}{
				{[]interface{}{"a", "b"}},
				{[]interface{}{"c", "d"}},
			},
		},
		{
			name: "field access operator",
			query: `
WITH orders AS (
  SELECT STRUCT(STRUCT('Yonge Street' AS street, 'Canada' AS country) AS address) AS customer
)
SELECT t.customer.address.country FROM orders AS t`,
			expectedRows: [][]interface{}{{"Canada"}},
		},
		{
			name:         "struct with bool",
			query:        `SELECT CURRENT_TIMESTAMP() AS ts, STRUCT(NULL AS a, FALSE AS b).b AS b`,
			expectedRows: [][]interface{}{{createTimestampFormatFromTime(now.UTC()), false}},
		},
		{
			name: "array index access operator",
			query: `
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array,
  item_array[OFFSET(1)] AS item_offset,
  item_array[ORDINAL(1)] AS item_ordinal,
  item_array[SAFE_OFFSET(6)] AS item_safe_offset,
FROM Items`,
			expectedRows: [][]interface{}{{
				[]interface{}{"coffee", "tea", "milk"},
				"tea",
				"coffee",
				nil,
			}},
		},
		{
			name: "array boundary indexing test",
			query: `WITH toks AS (SELECT ['one', 'two'] AS digits)
					SELECT digits[SAFE_ORDINAL(2)], digits[ORDINAL(2)], digits[OFFSET(1)], digits[SAFE_OFFSET(1)] FROM toks`,
			expectedRows: [][]interface{}{{"two", "two", "two", "two"}},
		},
		{
			name: "create function",
			query: `
CREATE FUNCTION customfunc(
  arr ARRAY<STRUCT<name STRING, val INT64>>
) AS (
  (SELECT SUM(IF(elem.name = "foo",elem.val,null)) FROM UNNEST(arr) AS elem)
)`,
			expectedRows: [][]interface{}{},
		},
		{
			name: "use function",
			query: `
SELECT customfunc([
  STRUCT<name STRING, val INT64>("foo", 10),
  STRUCT<name STRING, val INT64>("bar", 40),
  STRUCT<name STRING, val INT64>("foo", 20)
])`,
			expectedRows: [][]interface{}{{int64(30)}},
		},
		{
			name: "out of range error",
			query: `
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array[OFFSET(6)] AS item_offset
FROM Items`,
			expectedRows: [][]interface{}{},
			expectedErr:  "OFFSET(6) is out of range",
		},
		// INVALID_ARGUMENT: Subscript access using [INT64] is not supported on values of type JSON [at 2:34]
		// {
		//	name: "json",
		//	query: `
		//	SELECT json_value.class.students[0]['name'] AS first_student
		//	FROM
		//	  UNNEST(
		//	    [
		//	      JSON '{"class" : {"students" : [{"name" : "Jane"}]}}',
		//	      JSON '{"class" : {"students" : []}}',
		//	      JSON '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'])
		//	    AS json_value`,
		//	expectedRows: [][]interface{}{
		//		{"Jane"},
		//		{nil},
		//		{"John"},
		//	},
		// },
		{
			name:         "date operator",
			query:        `SELECT DATE "2020-09-22" + 1 AS day_later, DATE "2020-09-22" - 7 AS week_ago`,
			expectedRows: [][]interface{}{{"2020-09-23", "2020-09-15"}},
		},

		// aggregate functions
		{
			name:         "any_value",
			query:        `SELECT ANY_VALUE(fruit) FROM UNNEST(["apple", "banana", "pear"]) as fruit`,
			expectedRows: [][]interface{}{{"apple"}},
		},
		{
			name:  "any_value with window",
			query: `SELECT fruit, ANY_VALUE(fruit) OVER (ORDER BY LENGTH(fruit) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM UNNEST(["apple", "banana", "pear"]) as fruit`,
			expectedRows: [][]interface{}{
				{"pear", "pear"},
				{"apple", "pear"},
				{"banana", "apple"},
			},
		},
		{
			name:  "array_agg",
			query: `SELECT ARRAY_AGG(x) AS array_agg FROM UNNEST([2, 1,-2, 3, -2, 1, 2]) AS x`,
			expectedRows: [][]interface{}{{
				[]interface{}{int64(2), int64(1), int64(-2), int64(3), int64(-2), int64(1), int64(2)},
			}},
		},
		{
			name:  "array_agg with distinct",
			query: `SELECT ARRAY_AGG(DISTINCT x) AS array_agg FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x`,
			expectedRows: [][]interface{}{{
				[]interface{}{int64(2), int64(1), int64(-2), int64(3)},
			}},
		},
		{
			name:  "array_agg with limit",
			query: `SELECT ARRAY_AGG(x LIMIT 5) AS array_agg FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x`,
			expectedRows: [][]interface{}{{
				[]interface{}{int64(2), int64(1), int64(-2), int64(3), int64(-2)},
			}},
		},
		{
			name:        "array_agg with nulls",
			query:       `SELECT ARRAY_AGG(x) AS array_agg FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x`,
			expectedErr: "ARRAY_AGG: input value must be not null",
		},
		{
			name:  "array_agg with null in order by",
			query: `WITH toks AS (SELECT '1' AS x, '1' as y UNION ALL SELECT '2', null) SELECT ARRAY_AGG(x ORDER BY y) FROM toks`,
			expectedRows: [][]interface{}{{
				[]interface{}{"2", "1"},
			}},
		},
		{
			name:        "array_agg with struct",
			query:       `SELECT b, ARRAY_AGG(a) FROM UNNEST([STRUCT(1 AS a, 2 AS b), STRUCT(NULL AS a, 2 AS b)]) GROUP BY b`,
			expectedErr: "ARRAY_AGG: input value must be not null",
		},
		{
			name:  "array_agg with ignore nulls",
			query: `SELECT ARRAY_AGG(x IGNORE NULLS) AS array_agg FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x`,
			expectedRows: [][]interface{}{{
				[]interface{}{int64(1), int64(-2), int64(3), int64(-2), int64(1)},
			}},
		},
		{
			name:         "array_agg with ignore nulls and struct",
			query:        `SELECT b, ARRAY_AGG(a IGNORE NULLS) FROM UNNEST([STRUCT(NULL AS a, 2 AS b), STRUCT(1 AS a, 2 AS b)]) GROUP BY b`,
			expectedRows: [][]interface{}{{int64(2), []interface{}{int64(1)}}},
		},
		{
			name:  "array_agg with abs",
			query: `SELECT ARRAY_AGG(x ORDER BY ABS(x)) AS array_agg FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x`,
			expectedRows: [][]interface{}{{
				[]interface{}{int64(1), int64(1), int64(2), int64(-2), int64(-2), int64(2), int64(3)},
			}},
		},
		{
			name:  "array_agg with window",
			query: `SELECT x, ARRAY_AGG(x) OVER (ORDER BY ABS(x)) FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x`,
			expectedRows: [][]interface{}{
				{int64(1), []interface{}{int64(1), int64(1)}},
				{int64(1), []interface{}{int64(1), int64(1)}},
				{int64(2), []interface{}{int64(1), int64(1), int64(2), int64(-2), int64(-2), int64(2)}},
				{int64(-2), []interface{}{int64(1), int64(1), int64(2), int64(-2), int64(-2), int64(2)}},
				{int64(-2), []interface{}{int64(1), int64(1), int64(2), int64(-2), int64(-2), int64(2)}},
				{int64(2), []interface{}{int64(1), int64(1), int64(2), int64(-2), int64(-2), int64(2)}},
				{int64(3), []interface{}{int64(1), int64(1), int64(2), int64(-2), int64(-2), int64(2), int64(3)}},
			},
		},
		{
			name: "array_concat_agg",
			query: `
SELECT ARRAY_CONCAT_AGG(x) AS array_concat_agg FROM (
  SELECT [NULL, 1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
)`,
			expectedRows: [][]interface{}{{
				[]interface{}{nil, int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8), int64(9)},
			}},
		},
		{
			name:  "array_concat_agg with null in order by",
			query: `WITH toks AS (SELECT ['1'] AS x, '1' as y UNION ALL SELECT ['2', '3'], null) SELECT ARRAY_CONCAT_AGG(x ORDER BY y) FROM toks`,
			expectedRows: [][]interface{}{{
				[]interface{}{"2", "3", "1"},
			}},
		},
		{
			name:  "array_concat_agg with limt",
			query: `WITH toks AS (SELECT ['1'] AS x, '1' as y UNION ALL SELECT ['2', '3'], null) SELECT ARRAY_CONCAT_AGG(x ORDER BY y LIMIT 1) FROM toks`,
			expectedRows: [][]interface{}{{
				[]interface{}{"2", "3"},
			}},
		},
		{
			name: "array_concat_agg with format",
			query: `SELECT FORMAT("%T", ARRAY_CONCAT_AGG(x)) AS array_concat_agg FROM (
  SELECT [NULL, 1, 2, 3, 4] AS x
  UNION ALL SELECT NULL
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
)`,
			expectedRows: [][]interface{}{
				{"[NULL, 1, 2, 3, 4, 5, 6, 7, 8, 9]"},
			},
		},
		{
			name:         "avg",
			query:        `SELECT AVG(x) as avg FROM UNNEST([0, 2, 4, 4, 5]) as x`,
			expectedRows: [][]interface{}{{float64(3)}},
		},
		{
			name:         "avg with distinct",
			query:        `SELECT AVG(DISTINCT x) AS avg FROM UNNEST([0, 2, 4, 4, 5]) AS x`,
			expectedRows: [][]interface{}{{float64(2.75)}},
		},
		{
			name:  "avg with window",
			query: `SELECT x, AVG(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM UNNEST([0, 2, NULL, 4, 4, 5]) AS x`,
			expectedRows: [][]interface{}{
				{nil, nil},
				{int64(0), float64(0)},
				{int64(2), float64(1)},
				{int64(4), float64(3)},
				{int64(4), float64(4)},
				{int64(5), float64(4.5)},
			},
		},
		{
			name:         "bit_and",
			query:        `SELECT BIT_AND(x) as bit_and FROM UNNEST([0xF001, 0x00A1]) as x`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "bit_or",
			query:        `SELECT BIT_OR(x) as bit_or FROM UNNEST([0xF001, 0x00A1]) as x`,
			expectedRows: [][]interface{}{{int64(61601)}},
		},
		{
			name:         "bit_xor",
			query:        `SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([5678, 1234]) AS x`,
			expectedRows: [][]interface{}{{int64(4860)}},
		},
		{
			name:         "bit_xor 2",
			query:        `SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x`,
			expectedRows: [][]interface{}{{int64(5678)}},
		},
		{
			name:         "bit_xor 3",
			query:        `SELECT BIT_XOR(DISTINCT x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x`,
			expectedRows: [][]interface{}{{int64(4860)}},
		},
		{
			name:         "count star and distinct",
			query:        `SELECT COUNT(*) AS count_star, COUNT(DISTINCT x) AS count_dist_x FROM UNNEST([1, 4, 4, 5]) AS x`,
			expectedRows: [][]interface{}{{int64(4), int64(3)}},
		},
		{
			name:         "count with null",
			query:        `SELECT COUNT(x) FROM UNNEST([NULL]) AS x`,
			expectedRows: [][]interface{}{{int64(0)}},
		},
		{
			name:         "count with if",
			query:        `SELECT COUNT(DISTINCT IF(x > 0, x, NULL)) AS distinct_positive FROM UNNEST([1, -2, 4, 1, -5, 4, 1, 3, -6, 1]) AS x`,
			expectedRows: [][]interface{}{{int64(3)}},
		},
		{
			name:  "count with window",
			query: `SELECT x, COUNT(*) OVER (PARTITION BY MOD(x, 3)), COUNT(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) FROM UNNEST([1, 4, 4, 5]) AS x`,
			expectedRows: [][]interface{}{
				{int64(1), int64(3), int64(2)},
				{int64(4), int64(3), int64(2)},
				{int64(4), int64(3), int64(2)},
				{int64(5), int64(1), int64(1)},
			},
		},
		{
			name:         "countif",
			query:        `SELECT COUNTIF(x<0) AS num_negative, COUNTIF(x>0) AS num_positive FROM UNNEST([5, -2, 3, 6, -10, -7, 4, 0]) AS x`,
			expectedRows: [][]interface{}{{int64(3), int64(4)}},
		},
		{
			name:         "countif with null",
			query:        `SELECT COUNTIF(x<0) FROM UNNEST([NULL]) AS x`,
			expectedRows: [][]interface{}{{int64(0)}},
		},
		{
			name:  "countif with window",
			query: `SELECT x, COUNTIF(x<0) OVER (ORDER BY ABS(x) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM UNNEST([5, -2, 3, 6, -10, NULL, -7, 4, 0]) AS x`,
			expectedRows: [][]interface{}{
				{nil, int64(0)},
				{int64(0), int64(1)},
				{int64(-2), int64(1)},
				{int64(3), int64(1)},
				{int64(4), int64(0)},
				{int64(5), int64(0)},
				{int64(6), int64(1)},
				{int64(-7), int64(2)},
				{int64(-10), int64(2)},
			},
		},

		{
			name:         "logical_and",
			query:        `SELECT LOGICAL_AND(x) AS logical_and FROM UNNEST([true, false, true]) AS x`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name: "logical_and null",
			query: ` WITH toks AS (SELECT FALSE AS x UNION ALL SELECT FALSE UNION ALL SELECT NULL)
SELECT LOGICAL_AND(x) AS logical_or FROM toks`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "logical_or",
			query:        `SELECT LOGICAL_OR(x) AS logical_or FROM UNNEST([true, false, true]) AS x`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name: "logical_or null",
			query: ` WITH toks AS (SELECT FALSE AS x UNION ALL SELECT FALSE UNION ALL SELECT NULL)
SELECT LOGICAL_OR(x) AS logical_or FROM toks`,
			expectedRows: [][]interface{}{{false}},
		},
		{
			name:         "max from int group",
			query:        `SELECT MAX(x) AS max FROM UNNEST([8, 37, 4, 55]) AS x`,
			expectedRows: [][]interface{}{{int64(55)}},
		},
		{
			name:         "max from date group",
			query:        `SELECT MAX(x) AS max FROM UNNEST(['2022-01-01', '2022-02-01', '2022-01-02', '2021-03-01']) AS x`,
			expectedRows: [][]interface{}{{"2022-02-01"}},
		},
		{
			name:         "max window from date group",
			query:        `SELECT MAX(x) OVER() AS max FROM UNNEST(['2022-01-01', '2022-02-01', '2022-01-02', '2021-03-01']) AS x`,
			expectedRows: [][]interface{}{{"2022-02-01"}, {"2022-02-01"}, {"2022-02-01"}, {"2022-02-01"}},
		},
		{
			name:         "min from int group",
			query:        `SELECT MIN(x) AS min FROM UNNEST([8, 37, 4, 55]) AS x`,
			expectedRows: [][]interface{}{{int64(4)}},
		},
		{
			name:         "min from date group",
			query:        `SELECT MIN(x) AS min FROM UNNEST(['2022-01-01', '2022-02-01', '2022-01-02', '2021-03-01']) AS x`,
			expectedRows: [][]interface{}{{"2021-03-01"}},
		},
		{
			name:         "min window from date group",
			query:        `SELECT MIN(x) OVER() AS max FROM UNNEST(['2022-01-01', '2022-02-01', '2022-01-02', '2021-03-01']) AS x`,
			expectedRows: [][]interface{}{{"2021-03-01"}, {"2021-03-01"}, {"2021-03-01"}, {"2021-03-01"}},
		},
		{
			name:         "string_agg",
			query:        `SELECT STRING_AGG(fruit) AS string_agg FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"apple,pear,banana,pear"}},
		},
		{
			name:         "string_agg with length 0",
			query:        `SELECT STRING_AGG(fruit) FROM UNNEST(ARRAY<STRING>[]) fruit;`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "string_agg with null",
			query:        `SELECT STRING_AGG(null) FROM UNNEST(ARRAY<STRING>[]);`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "string_agg with delimiter",
			query:        `SELECT STRING_AGG(fruit, " & ") AS string_agg FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"apple & pear & banana & pear"}},
		},
		{
			name:         "string_agg with distinct",
			query:        `SELECT STRING_AGG(DISTINCT fruit, " & ") AS string_agg FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"apple & pear & banana"}},
		},
		{
			name:         "string_agg with order by",
			query:        `SELECT STRING_AGG(fruit, " & " ORDER BY LENGTH(fruit)) AS string_agg FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"pear & pear & apple & banana"}},
		},
		{
			name:         "string_agg with limit",
			query:        `SELECT STRING_AGG(fruit, " & " LIMIT 2) AS string_agg FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"apple & pear"}},
		},
		{
			name:         "string_agg with distinct and order by and limit",
			query:        `SELECT STRING_AGG(DISTINCT fruit, " & " ORDER BY fruit DESC LIMIT 2) AS string_agg FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{{"pear & banana"}},
		},
		{
			// TODO: add NULL back to the unnest once ORDER BY does not crash on NULL
			name:  "string_agg with window",
			query: `SELECT fruit, STRING_AGG(fruit, " & ") OVER (ORDER BY LENGTH(fruit)) FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit`,
			expectedRows: [][]interface{}{
				{"pear", "pear & pear"},
				{"pear", "pear & pear"},
				{"apple", "pear & pear & apple"},
				{"banana", "pear & pear & apple & banana"},
			},
		},
		{
			name:         "sum",
			query:        `SELECT SUM(x) AS sum FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x`,
			expectedRows: [][]interface{}{{int64(25)}},
		},
		{
			name:         "sum with distinct",
			query:        `SELECT SUM(DISTINCT x) AS sum FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x`,
			expectedRows: [][]interface{}{{int64(15)}},
		},
		{
			name:  "sum with window",
			query: `SELECT x, SUM(x) OVER (PARTITION BY MOD(x, 3)) FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x`,
			expectedRows: [][]interface{}{
				{int64(3), int64(6)},
				{int64(3), int64(6)},
				{int64(1), int64(10)},
				{int64(4), int64(10)},
				{int64(4), int64(10)},
				{int64(1), int64(10)},
				{int64(2), int64(9)},
				{int64(5), int64(9)},
				{int64(2), int64(9)},
			},
		},
		{
			name:  "sum with window and distinct",
			query: `SELECT x, SUM(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x`,
			expectedRows: [][]interface{}{
				{int64(3), int64(3)},
				{int64(3), int64(3)},
				{int64(1), int64(5)},
				{int64(4), int64(5)},
				{int64(4), int64(5)},
				{int64(1), int64(5)},
				{int64(2), int64(7)},
				{int64(5), int64(7)},
				{int64(2), int64(7)},
			},
		},
		{
			name:         "sum null",
			query:        `SELECT SUM(x) AS sum FROM UNNEST([]) AS x`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:        "safe sum",
			query:       `SELECT SAFE.SUM(x) AS sum FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x`,
			expectedErr: "SAFE is not supported for function SUM",
		},
		{
			name:         "approx_count_distinct",
			query:        `SELECT APPROX_COUNT_DISTINCT(x) FROM UNNEST([0, 1, 1, 2, 3, 5]) as x`,
			expectedRows: [][]interface{}{{int64(5)}},
		},
		{
			name:         "approx_quantiles",
			query:        `SELECT APPROX_QUANTILES(x, 2) FROM UNNEST([1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x`,
			expectedRows: [][]interface{}{{[]interface{}{int64(1), int64(5), int64(10)}}},
		},
		{
			name:         "approx_quantiles 2",
			query:        `SELECT APPROX_QUANTILES(x, 100)[OFFSET(90)] FROM UNNEST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) AS x`,
			expectedRows: [][]interface{}{{int64(9)}},
		},
		{
			name:         "approx_quantiles with distinct",
			query:        `SELECT APPROX_QUANTILES(DISTINCT x, 2) FROM UNNEST([1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x`,
			expectedRows: [][]interface{}{{[]interface{}{int64(1), int64(6), int64(10)}}},
		},
		{
			name:         "approx_quantiles with null",
			query:        `SELECT APPROX_QUANTILES(x, 2 RESPECT NULLS) FROM UNNEST([NULL, NULL, 1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x`,
			expectedRows: [][]interface{}{{[]interface{}{nil, int64(4), int64(10)}}},
		},
		{
			name:         "approx_quantiles with respect nulls",
			query:        `SELECT APPROX_QUANTILES(DISTINCT x, 2 RESPECT NULLS) FROM UNNEST([NULL, NULL, 1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x`,
			expectedRows: [][]interface{}{{[]interface{}{nil, int64(6), int64(10)}}},
		},
		{
			name:  "approx_top_count",
			query: `SELECT APPROX_TOP_COUNT(x, 2) FROM UNNEST(["apple", "apple", "pear", "pear", "pear", "banana"]) as x`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						[]map[string]interface{}{
							{
								"value": "pear",
							},
							{
								"count": int64(3),
							},
						},
						[]map[string]interface{}{
							{
								"value": "apple",
							},
							{
								"count": int64(2),
							},
						},
					},
				},
			},
		},
		{
			name:  "approx_top_count with null",
			query: `SELECT APPROX_TOP_COUNT(x, 2) FROM UNNEST([NULL, "pear", "pear", "pear", "apple", NULL]) as x`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						[]map[string]interface{}{
							{
								"value": "pear",
							},
							{
								"count": int64(3),
							},
						},
						[]map[string]interface{}{
							{
								"value": nil,
							},
							{
								"count": int64(2),
							},
						},
					},
				},
			},
		},
		{
			name: "approx_top_sum",
			query: `
SELECT APPROX_TOP_SUM(x, weight, 2) FROM UNNEST([
  STRUCT("apple" AS x, 3 AS weight),
  ("pear", 2),
  ("apple", 0),
  ("banana", 5),
  ("pear", 4)
])`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						[]map[string]interface{}{
							{
								"value": "pear",
							},
							{
								"sum": int64(6),
							},
						},
						[]map[string]interface{}{
							{
								"value": "banana",
							},
							{
								"sum": int64(5),
							},
						},
					},
				},
			},
		},
		{
			name:  "approx_top_sum with null",
			query: `SELECT APPROX_TOP_SUM(x, weight, 2) FROM UNNEST([STRUCT("apple" AS x, NULL AS weight), ("pear", 0), ("pear", NULL)])`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						[]map[string]interface{}{
							{
								"value": "pear",
							},
							{
								"sum": int64(0),
							},
						},
						[]map[string]interface{}{
							{
								"value": "apple",
							},
							{
								"sum": nil,
							},
						},
					},
				},
			},
		},
		{
			name:  "approx_top_sum with null 2",
			query: `SELECT APPROX_TOP_SUM(x, weight, 2) FROM UNNEST([STRUCT("apple" AS x, 0 AS weight), (NULL, 2)])`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						[]map[string]interface{}{
							{
								"value": nil,
							},
							{
								"sum": int64(2),
							},
						},
						[]map[string]interface{}{
							{
								"value": "apple",
							},
							{
								"sum": int64(0),
							},
						},
					},
				},
			},
		},
		{
			name:  "approx_top_sum with null 3",
			query: `SELECT APPROX_TOP_SUM(x, weight, 2) FROM UNNEST([STRUCT("apple" AS x, 0 AS weight), (NULL, NULL)])`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						[]map[string]interface{}{
							{
								"value": "apple",
							},
							{
								"sum": int64(0),
							},
						},
						[]map[string]interface{}{
							{
								"value": nil,
							},
							{
								"sum": nil,
							},
						},
					},
				},
			},
		},

		// hyperloglog++ function
		{
			name: "hll_count.init",
			query: `
SELECT
  country,
  HLL_COUNT.INIT(customer_id, 10)
    AS hll_sketch
FROM
  UNNEST(
    ARRAY<STRUCT<country STRING, customer_id STRING, invoice_id STRING>>[
      ('UA', 'customer_id_1', 'invoice_id_11'),
      ('CZ', 'customer_id_2', 'invoice_id_22'),
      ('CZ', 'customer_id_2', 'invoice_id_23'),
      ('BR', 'customer_id_3', 'invoice_id_31'),
      ('UA', 'customer_id_2', 'invoice_id_24')])
GROUP BY country`,
			expectedRows: [][]interface{}{
				{"BR", "Eu9/P61VrRgkBrk="},
				{"CZ", "Eu9/TliDjbmhVEA="},
				{"UA", "Eu9/Ol8Q5++jVjNOWIONuaFUQA=="},
			},
		},
		{
			name: "hll_count.merge",
			query: `
SELECT HLL_COUNT.MERGE(hll_sketch) AS distinct_customers_with_open_invoice
FROM
(
    SELECT
      country,
      HLL_COUNT.INIT(customer_id) AS hll_sketch
    FROM
      UNNEST(
        ARRAY<STRUCT<country STRING, customer_id STRING, invoice_id STRING>>[
          ('UA', 'customer_id_1', 'invoice_id_11'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('CZ', 'customer_id_2', 'invoice_id_22'),
          ('CZ', 'customer_id_2', 'invoice_id_23'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('UA', 'customer_id_2', 'invoice_id_24')])
    GROUP BY country
)`,
			expectedRows: [][]interface{}{{int64(3)}},
		},
		{
			name: "hll_count.merge_partial",
			query: `
SELECT HLL_COUNT.MERGE_PARTIAL(HLL_sketch) AS distinct_customers_with_open_invoice
FROM
  (
    SELECT
      country,
      HLL_COUNT.INIT(customer_id) AS hll_sketch
    FROM
      UNNEST(
        ARRAY<STRUCT<country STRING, customer_id STRING, invoice_id STRING>>[
          ('UA', 'customer_id_1', 'invoice_id_11'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('CZ', 'customer_id_2', 'invoice_id_22'),
          ('CZ', 'customer_id_2', 'invoice_id_23'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('UA', 'customer_id_2', 'invoice_id_24')])
    GROUP BY country
  )`,
			expectedRows: [][]interface{}{{"Eu9/Ol8Q5++jVjM/rVWtGCQGuU5Yg425oVRA"}},
		},
		{
			name: "hll_count.extract",
			query: `
SELECT
  country,
  HLL_COUNT.EXTRACT(HLL_sketch) AS distinct_customers_with_open_invoice
FROM
  (
    SELECT
      country,
      HLL_COUNT.INIT(customer_id) AS hll_sketch
    FROM
      UNNEST(
        ARRAY<STRUCT<country STRING, customer_id STRING, invoice_id STRING>>[
          ('UA', 'customer_id_1', 'invoice_id_11'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('CZ', 'customer_id_2', 'invoice_id_22'),
          ('CZ', 'customer_id_2', 'invoice_id_23'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('UA', 'customer_id_2', 'invoice_id_24')])
    GROUP BY country
  )`,
			expectedRows: [][]interface{}{
				{"BR", int64(1)},
				{"CZ", int64(1)},
				{"UA", int64(2)},
			},
		},

		{
			name:         "null",
			query:        `SELECT NULL`,
			expectedRows: [][]interface{}{{nil}},
		},

		// window function
		{
			name: `window total`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, SUM(purchases)
  OVER () AS total_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"kale", int64(23), "vegetable", int64(54)},
				{"banana", int64(2), "fruit", int64(54)},
				{"cabbage", int64(9), "vegetable", int64(54)},
				{"apple", int64(8), "fruit", int64(54)},
				{"leek", int64(2), "vegetable", int64(54)},
				{"lettuce", int64(10), "vegetable", int64(54)},
			},
		},
		{
			name: `window subtotal`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, SUM(purchases)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS total_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", int64(10)},
				{"apple", int64(8), "fruit", int64(10)},
				{"leek", int64(2), "vegetable", int64(44)},
				{"cabbage", int64(9), "vegetable", int64(44)},
				{"lettuce", int64(10), "vegetable", int64(44)},
				{"kale", int64(23), "vegetable", int64(44)},
			},
		},
		{
			name: `window cumulative`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, SUM(purchases)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS total_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", int64(2)},
				{"apple", int64(8), "fruit", int64(10)},
				{"leek", int64(2), "vegetable", int64(2)},
				{"cabbage", int64(9), "vegetable", int64(11)},
				{"lettuce", int64(10), "vegetable", int64(21)},
				{"kale", int64(23), "vegetable", int64(44)},
			},
		},
		{
			name: `window cumulative omit current row`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, SUM(purchases)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS UNBOUNDED PRECEDING
  ) AS total_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", int64(2)},
				{"apple", int64(8), "fruit", int64(10)},
				{"leek", int64(2), "vegetable", int64(2)},
				{"cabbage", int64(9), "vegetable", int64(11)},
				{"lettuce", int64(10), "vegetable", int64(21)},
				{"kale", int64(23), "vegetable", int64(44)},
			},
		},

		{
			name: `window offset`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, SUM(purchases)
  OVER (
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING
  ) AS total_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", nil},
				{"leek", int64(2), "vegetable", nil},
				{"apple", int64(8), "fruit", int64(2)},
				{"cabbage", int64(9), "vegetable", int64(4)},
				{"lettuce", int64(10), "vegetable", int64(12)},
				{"kale", int64(23), "vegetable", int64(21)},
			},
		},
		{
			name: `window avg`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, AVG(purchases)
  OVER (
    ORDER BY purchases
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS avg_purchases
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", float64(2)},
				{"leek", int64(2), "vegetable", float64(4)},
				{"apple", int64(8), "fruit", float64(6.333333333333333)},
				{"cabbage", int64(9), "vegetable", float64(9)},
				{"lettuce", int64(10), "vegetable", float64(14)},
				{"kale", int64(23), "vegetable", float64(16.5)},
			},
		},
		{
			name: `window first_value`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, FIRST_VALUE(item)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS most_popular
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", "banana"},
				{"apple", int64(8), "fruit", "banana"},
				{"leek", int64(2), "vegetable", "leek"},
				{"cabbage", int64(9), "vegetable", "leek"},
				{"lettuce", int64(10), "vegetable", "leek"},
				{"kale", int64(23), "vegetable", "leek"},
			},
		},
		{
			name: `window last_value`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS most_popular
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", "apple"},
				{"apple", int64(8), "fruit", "apple"},
				{"leek", int64(2), "vegetable", "kale"},
				{"cabbage", int64(9), "vegetable", "kale"},
				{"lettuce", int64(10), "vegetable", "kale"},
				{"kale", int64(23), "vegetable", "kale"},
			},
		},
		{
			name: `window last_value with offset`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS most_popular
FROM Produce`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", "apple"},
				{"apple", int64(8), "fruit", "apple"},
				{"leek", int64(2), "vegetable", "cabbage"},
				{"cabbage", int64(9), "vegetable", "lettuce"},
				{"lettuce", int64(10), "vegetable", "kale"},
				{"kale", int64(23), "vegetable", "kale"},
			},
		},
		{
			name: `window last_value with named window`,
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (
    item_window
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS most_popular
FROM Produce
WINDOW item_window AS (
  PARTITION BY category
  ORDER BY purchases)`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "fruit", "apple"},
				{"apple", int64(8), "fruit", "apple"},
				{"leek", int64(2), "vegetable", "cabbage"},
				{"cabbage", int64(9), "vegetable", "lettuce"},
				{"lettuce", int64(10), "vegetable", "kale"},
				{"kale", int64(23), "vegetable", "kale"},
			},
		},
		{
			name: `nth_value`,
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41+00', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58+00', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14+00', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  FORMAT_TIMESTAMP('%X', fastest_time) AS fastest_time,
  FORMAT_TIMESTAMP('%X', second_fastest) AS second_fastest
FROM (
  SELECT name,
  finish_time,
  division,finishers,
  FIRST_VALUE(finish_time)
    OVER w1 AS fastest_time,
  NTH_VALUE(finish_time, 2)
    OVER w1 as second_fastest
  FROM finishers
  WINDOW w1 AS (
    PARTITION BY division ORDER BY finish_time ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))`,
			expectedRows: [][]interface{}{
				{"Carly Forte", "03:08:58", "F25-29", "03:08:58", nil},
				{"Sophia Liu", "02:51:45", "F30-34", "02:51:45", "02:59:01"},
				{"Nikki Leith", "02:59:01", "F30-34", "02:51:45", "02:59:01"},
				{"Jen Edwards", "03:06:36", "F30-34", "02:51:45", "02:59:01"},
				{"Meghan Lederer", "03:07:41", "F30-34", "02:51:45", "02:59:01"},
				{"Lauren Reasoner", "03:10:14", "F30-34", "02:51:45", "02:59:01"},
				{"Lisa Stelzner", "02:54:11", "F35-39", "02:54:11", "03:01:17"},
				{"Lauren Matthews", "03:01:17", "F35-39", "02:54:11", "03:01:17"},
				{"Desiree Berry", "03:05:42", "F35-39", "02:54:11", "03:01:17"},
				{"Suzy Slane", "03:06:24", "F35-39", "02:54:11", "03:01:17"},
			},
		},
		{
			name: `lead`,
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41+00', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58+00', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14+00', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  LEAD(name)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS followed_by
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Carly Forte", "03:08:58", "F25-29", nil},
				{"Sophia Liu", "02:51:45", "F30-34", "Nikki Leith"},
				{"Nikki Leith", "02:59:01", "F30-34", "Jen Edwards"},
				{"Jen Edwards", "03:06:36", "F30-34", "Meghan Lederer"},
				{"Meghan Lederer", "03:07:41", "F30-34", "Lauren Reasoner"},
				{"Lauren Reasoner", "03:10:14", "F30-34", nil},
				{"Lisa Stelzner", "02:54:11", "F35-39", "Lauren Matthews"},
				{"Lauren Matthews", "03:01:17", "F35-39", "Desiree Berry"},
				{"Desiree Berry", "03:05:42", "F35-39", "Suzy Slane"},
				{"Suzy Slane", "03:06:24", "F35-39", nil},
			},
		},
		{
			name: `lead with offset`,
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41+00', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58+00', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14+00', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  LEAD(name, 2)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_back
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Carly Forte", "03:08:58", "F25-29", nil},
				{"Sophia Liu", "02:51:45", "F30-34", "Jen Edwards"},
				{"Nikki Leith", "02:59:01", "F30-34", "Meghan Lederer"},
				{"Jen Edwards", "03:06:36", "F30-34", "Lauren Reasoner"},
				{"Meghan Lederer", "03:07:41", "F30-34", nil},
				{"Lauren Reasoner", "03:10:14", "F30-34", nil},
				{"Lisa Stelzner", "02:54:11", "F35-39", "Desiree Berry"},
				{"Lauren Matthews", "03:01:17", "F35-39", "Suzy Slane"},
				{"Desiree Berry", "03:05:42", "F35-39", nil},
				{"Suzy Slane", "03:06:24", "F35-39", nil},
			},
		},
		{
			name: `lead with default`,
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41+00', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58+00', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14+00', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  LEAD(name, 2, 'Nobody')
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_back
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Carly Forte", "03:08:58", "F25-29", "Nobody"},
				{"Sophia Liu", "02:51:45", "F30-34", "Jen Edwards"},
				{"Nikki Leith", "02:59:01", "F30-34", "Meghan Lederer"},
				{"Jen Edwards", "03:06:36", "F30-34", "Lauren Reasoner"},
				{"Meghan Lederer", "03:07:41", "F30-34", "Nobody"},
				{"Lauren Reasoner", "03:10:14", "F30-34", "Nobody"},
				{"Lisa Stelzner", "02:54:11", "F35-39", "Desiree Berry"},
				{"Lauren Matthews", "03:01:17", "F35-39", "Suzy Slane"},
				{"Desiree Berry", "03:05:42", "F35-39", "Nobody"},
				{"Suzy Slane", "03:06:24", "F35-39", "Nobody"},
			},
		},
		{
			name: `window order by`,
			query: `WITH toks AS (
			SELECT DATE '2024-01-01' AS dt, 'c' AS letter
			UNION ALL SELECT DATE '2024-02-01', 'b'
			UNION ALL SELECT DATE '2024-02-01', 'c'
			UNION ALL SELECT DATE '2024-03-01', 'a'
)
SELECT dt, letter, ROW_NUMBER() OVER (ORDER BY dt, letter) AS rn FROM toks
`,
			expectedRows: [][]interface{}{
				{"2024-01-01", "c", int64(1)},
				{"2024-02-01", "b", int64(2)},
				{"2024-02-01", "c", int64(3)},
				{"2024-03-01", "a", int64(4)},
			},
		},
		{
			name: `window order by handles nil`,
			query: `WITH toks AS (
			SELECT DATE '2024-01-01' AS dt, 'c' AS letter
			UNION ALL SELECT DATE '2024-02-01', null
)
SELECT dt, letter, ROW_NUMBER() OVER (ORDER BY dt, letter) AS rn FROM toks
`,
			expectedRows: [][]interface{}{
				{"2024-01-01", "c", int64(1)},
				{"2024-02-01", nil, int64(2)},
			},
		},
		{
			name: `percentile_cont`,
			query: `
SELECT
  PERCENTILE_CONT(x, 0) OVER() AS min,
  PERCENTILE_CONT(x, 0.01) OVER() AS percentile1,
  PERCENTILE_CONT(x, 0.5) OVER() AS median,
  PERCENTILE_CONT(x, 0.9) OVER() AS percentile90,
  PERCENTILE_CONT(x, 1) OVER() AS max
FROM UNNEST([0, 3, NULL, 1, 2]) AS x LIMIT 1`,
			expectedRows: [][]interface{}{
				{float64(0), float64(0.03), float64(1.5), float64(2.7), float64(3)},
			},
		},
		{
			name: `percentile_cont_non_zero_min_sorted`,
			query: `
WITH cte AS
	(SELECT 20 as x UNION ALL SELECT 30 as x UNION ALL SELECT 40 as x)
SELECT
  PERCENTILE_CONT(x, 0) OVER() AS min,
  PERCENTILE_CONT(x, 0.01) OVER() AS percentile1,
  PERCENTILE_CONT(x, 0.5) OVER() AS median,
  PERCENTILE_CONT(x, 0.9) OVER() AS percentile90,
  PERCENTILE_CONT(x, 1) OVER() AS max
FROM cte LIMIT 1`,
			expectedRows: [][]interface{}{
				{float64(20), float64(20.2), float64(30.0), float64(38), float64(40)},
			},
		},
		{
			name: `percentile_cont_non_zero_min_unsorted`,
			query: `
WITH cte AS
	(SELECT 500 as x UNION ALL SELECT 50 as x UNION ALL SELECT 100 as x)
SELECT
  PERCENTILE_CONT(x, 0) OVER() AS min,
  PERCENTILE_CONT(x, 0.01) OVER() AS percentile1,
  PERCENTILE_CONT(x, 0.5) OVER() AS median,
  PERCENTILE_CONT(x, 0.9) OVER() AS percentile90,
  PERCENTILE_CONT(x, 1) OVER() AS max
FROM cte LIMIT 1`,
			expectedRows: [][]interface{}{
				{float64(50.0), float64(51), float64(100), float64(420), float64(500)},
			},
		},
		// TODO: support RESPECT NULLS
		//		{
		//			name: `percentile_cont with respect nulls`,
		//			query: `
		// SELECT
		//  PERCENTILE_CONT(x, 0 RESPECT NULLS) OVER() AS min,
		//  PERCENTILE_CONT(x, 0.01 RESPECT NULLS) OVER() AS percentile1,
		//  PERCENTILE_CONT(x, 0.5 RESPECT NULLS) OVER() AS median,
		//  PERCENTILE_CONT(x, 0.9 RESPECT NULLS) OVER() AS percentile90,
		//  PERCENTILE_CONT(x, 1 RESPECT NULLS) OVER() AS max
		// FROM UNNEST([0, 3, NULL, 1, 2]) AS x LIMIT 1`,
		//			expectedRows: [][]interface{}{
		//				{nil, float64(0), float64(1), float64(2.6), float64(3)},
		//			},
		//		},
		{
			name: `percentile_disc`,
			query: `
SELECT
  x,
  PERCENTILE_DISC(x, 0) OVER() AS min,
  PERCENTILE_DISC(x, 0.5) OVER() AS median,
  PERCENTILE_DISC(x, 1) OVER() AS max
FROM UNNEST(['c', NULL, 'b', 'a']) AS x`,
			expectedRows: [][]interface{}{
				{"c", "a", "b", "c"},
				{nil, "a", "b", "c"},
				{"b", "a", "b", "c"},
				{"a", "a", "b", "c"},
			},
		},
		{
			name: `percentile_disc with respect nulls`,
			query: `
SELECT
  x,
  PERCENTILE_DISC(x, 0 RESPECT NULLS) OVER() AS min,
  PERCENTILE_DISC(x, 0.5 RESPECT NULLS) OVER() AS median,
  PERCENTILE_DISC(x, 1 RESPECT NULLS) OVER() AS max
FROM UNNEST(['c', NULL, 'b', 'a']) AS x`,
			expectedRows: [][]interface{}{
				{"c", nil, "a", "c"},
				{nil, nil, "a", "c"},
				{"b", nil, "a", "c"},
				{"a", nil, "a", "c"},
			},
		},
		{
			name: "window range",
			query: `
WITH Farm AS
 (SELECT 'cat' as animal, 23 as population, 'mammal' as category
  UNION ALL SELECT 'duck', 3, 'bird'
  UNION ALL SELECT 'dog', 2, 'mammal'
  UNION ALL SELECT 'goose', 1, 'bird'
  UNION ALL SELECT 'ox', 2, 'mammal'
  UNION ALL SELECT 'goat', 2, 'mammal')
SELECT animal, population, category, COUNT(*)
  OVER (
    ORDER BY population
    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS similar_population
FROM Farm`,
			expectedRows: [][]interface{}{
				{"goose", int64(1), "bird", int64(4)},
				{"dog", int64(2), "mammal", int64(5)},
				{"ox", int64(2), "mammal", int64(5)},
				{"goat", int64(2), "mammal", int64(5)},
				{"duck", int64(3), "bird", int64(4)},
				{"cat", int64(23), "mammal", int64(1)},
			},
		},
		{
			name: "date type",
			query: `
WITH Employees AS
 (SELECT 'Isabella' as name, 2 as department, DATE(1997, 09, 28) as start_date
  UNION ALL SELECT 'Anthony', 1, DATE(1995, 11, 29)
  UNION ALL SELECT 'Daniel', 2, DATE(2004, 06, 24)
  UNION ALL SELECT 'Andrew', 1, DATE(1999, 01, 23)
  UNION ALL SELECT 'Jacob', 1, DATE(1990, 07, 11)
  UNION ALL SELECT 'Jose', 2, DATE(2013, 03, 17))
SELECT * FROM Employees`,
			expectedRows: [][]interface{}{
				{"Isabella", int64(2), "1997-09-28"},
				{"Anthony", int64(1), "1995-11-29"},
				{"Daniel", int64(2), "2004-06-24"},
				{"Andrew", int64(1), "1999-01-23"},
				{"Jacob", int64(1), "1990-07-11"},
				{"Jose", int64(2), "2013-03-17"},
			},
		},
		{
			name: "window rank",
			query: `
WITH Employees AS
 (SELECT 'Isabella' as name, 2 as department, DATE(1997, 09, 28) as start_date
  UNION ALL SELECT 'Anthony', 1, DATE(1995, 11, 29)
  UNION ALL SELECT 'Daniel', 2, DATE(2004, 06, 24)
  UNION ALL SELECT 'Andrew', 1, DATE(1999, 01, 23)
  UNION ALL SELECT 'Jacob', 1, DATE(1990, 07, 11)
  UNION ALL SELECT 'Jose', 2, DATE(2013, 03, 17))
SELECT name, department, start_date,
  RANK() OVER (PARTITION BY department ORDER BY start_date) AS rank
FROM Employees`,
			expectedRows: [][]interface{}{
				{"Jacob", int64(1), "1990-07-11", int64(1)},
				{"Anthony", int64(1), "1995-11-29", int64(2)},
				{"Andrew", int64(1), "1999-01-23", int64(3)},
				{"Isabella", int64(2), "1997-09-28", int64(1)},
				{"Daniel", int64(2), "2004-06-24", int64(2)},
				{"Jose", int64(2), "2013-03-17", int64(3)},
			},
		},
		{
			name: "rank with same order",
			query: `
WITH Numbers AS
 (SELECT 1 as x
  UNION ALL SELECT 2
  UNION ALL SELECT 2
  UNION ALL SELECT 5
  UNION ALL SELECT 8
  UNION ALL SELECT 10
  UNION ALL SELECT 10
)
SELECT x,
  RANK() OVER (ORDER BY x ASC) AS rank
FROM Numbers`,
			expectedRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(2), int64(2)},
				{int64(2), int64(2)},
				{int64(5), int64(4)},
				{int64(8), int64(5)},
				{int64(10), int64(6)},
				{int64(10), int64(6)},
			},
		},
		{
			name: "window dense_rank",
			query: `
WITH Numbers AS
 (SELECT 1 as x
  UNION ALL SELECT 2
  UNION ALL SELECT 2
  UNION ALL SELECT 5
  UNION ALL SELECT 8
  UNION ALL SELECT 10
  UNION ALL SELECT 10
)
SELECT x,
  DENSE_RANK() OVER (ORDER BY x ASC) AS dense_rank
FROM Numbers`,
			expectedRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(2), int64(2)},
				{int64(2), int64(2)},
				{int64(5), int64(3)},
				{int64(8), int64(4)},
				{int64(10), int64(5)},
				{int64(10), int64(5)},
			},
		},
		{
			name: "window dense_rank with group",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  DENSE_RANK() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers
`,
			expectedRows: [][]interface{}{
				{"Sophia Liu", createTimestampFormatFromString("2016-10-18 09:51:45+00"), "F30-34", int64(1)},
				{"Nikki Leith", createTimestampFormatFromString("2016-10-18 09:59:01+00"), "F30-34", int64(2)},
				{"Meghan Lederer", createTimestampFormatFromString("2016-10-18 09:59:01+00"), "F30-34", int64(2)},
				{"Jen Edwards", createTimestampFormatFromString("2016-10-18 10:06:36+00"), "F30-34", int64(3)},
				{"Lisa Stelzner", createTimestampFormatFromString("2016-10-18 09:54:11+00"), "F35-39", int64(1)},
				{"Lauren Matthews", createTimestampFormatFromString("2016-10-18 10:01:17+00"), "F35-39", int64(2)},
				{"Desiree Berry", createTimestampFormatFromString("2016-10-18 10:05:42+00"), "F35-39", int64(3)},
				{"Suzy Slane", createTimestampFormatFromString("2016-10-18 10:06:24+00"), "F35-39", int64(4)},
			},
		},
		{
			name: "percent_rank",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  PERCENT_RANK() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Sophia Liu", "02:51:45", "F30-34", float64(0)},
				{"Nikki Leith", "02:59:01", "F30-34", float64(0.33333333333333331)},
				{"Meghan Lederer", "02:59:01", "F30-34", float64(0.33333333333333331)},
				{"Jen Edwards", "03:06:36", "F30-34", float64(1)},
				{"Lisa Stelzner", "02:54:11", "F35-39", float64(0)},
				{"Lauren Matthews", "03:01:17", "F35-39", float64(0.33333333333333331)},
				{"Desiree Berry", "03:05:42", "F35-39", float64(0.66666666666666663)},
				{"Suzy Slane", "03:06:24", "F35-39", float64(1)},
			},
		},
		{
			name: "cume_dist",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  CUME_DIST() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Sophia Liu", "02:51:45", "F30-34", float64(0.25)},
				// FIXME: care same ordered value.
				{"Nikki Leith", "02:59:01", "F30-34", float64(0.5)},
				{"Meghan Lederer", "02:59:01", "F30-34", float64(0.75)},
				{"Jen Edwards", "03:06:36", "F30-34", float64(1)},
				{"Lisa Stelzner", "02:54:11", "F35-39", float64(0.25)},
				{"Lauren Matthews", "03:01:17", "F35-39", float64(0.5)},
				{"Desiree Berry", "03:05:42", "F35-39", float64(0.75)},
				{"Suzy Slane", "03:06:24", "F35-39", float64(1)},
			},
		},
		{
			name: "ntile",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:00+00', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  NTILE(3) OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Sophia Liu", "02:51:45", "F30-34", int64(1)},
				{"Meghan Lederer", "02:59:00", "F30-34", int64(1)},
				{"Nikki Leith", "02:59:01", "F30-34", int64(2)},
				{"Jen Edwards", "03:06:36", "F30-34", int64(3)},
				{"Lisa Stelzner", "02:54:11", "F35-39", int64(1)},
				{"Lauren Matthews", "03:01:17", "F35-39", int64(1)},
				{"Desiree Berry", "03:05:42", "F35-39", int64(2)},
				{"Suzy Slane", "03:06:24", "F35-39", int64(3)},
			},
		},
		{
			name: "window row_number",
			query: `
WITH Numbers AS
 (SELECT 1 as x
  UNION ALL SELECT 2
  UNION ALL SELECT 2
  UNION ALL SELECT 5
  UNION ALL SELECT 8
  UNION ALL SELECT 10
  UNION ALL SELECT 10
)
SELECT x,
  ROW_NUMBER() OVER (ORDER BY x) AS row_num
FROM Numbers`,
			expectedRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(2), int64(2)},
				{int64(2), int64(3)},
				{int64(5), int64(4)},
				{int64(8), int64(5)},
				{int64(10), int64(6)},
				{int64(10), int64(7)},
			},
		},
		{
			name: "row_number nest",
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
, Numbers AS (
  SELECT item, purchases, category, ROW_NUMBER() OVER(PARTITION BY category) AS num
    FROM Produce
) SELECT p.item, p.category, p.purchases, n.num,
    ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY p.purchases ASC) AS num2
    FROM Produce p JOIN Numbers n ON p.item = n.item AND p.category = n.category
`,
			expectedRows: [][]interface{}{
				{"banana", "fruit", int64(2), int64(1), int64(1)},
				{"apple", "fruit", int64(8), int64(2), int64(2)},
				{"leek", "vegetable", int64(2), int64(3), int64(1)},
				{"cabbage", "vegetable", int64(9), int64(2), int64(2)},
				{"lettuce", "vegetable", int64(10), int64(4), int64(3)},
				{"kale", "vegetable", int64(23), int64(1), int64(4)},
			},
		},
		{
			name: "window lag",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41+00', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58+00', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14+00', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS preceding_runner
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Carly Forte", createTimestampFormatFromString("2016-10-18 03:08:58+00"), "F25-29", nil},
				{"Sophia Liu", createTimestampFormatFromString("2016-10-18 02:51:45+00"), "F30-34", nil},
				{"Nikki Leith", createTimestampFormatFromString("2016-10-18 02:59:01+00"), "F30-34", "Sophia Liu"},
				{"Jen Edwards", createTimestampFormatFromString("2016-10-18 03:06:36+00"), "F30-34", "Nikki Leith"},
				{"Meghan Lederer", createTimestampFormatFromString("2016-10-18 03:07:41+00"), "F30-34", "Jen Edwards"},
				{"Lauren Reasoner", createTimestampFormatFromString("2016-10-18 03:10:14+00"), "F30-34", "Meghan Lederer"},
				{"Lisa Stelzner", createTimestampFormatFromString("2016-10-18 02:54:11+00"), "F35-39", nil},
				{"Lauren Matthews", createTimestampFormatFromString("2016-10-18 03:01:17+00"), "F35-39", "Lisa Stelzner"},
				{"Desiree Berry", createTimestampFormatFromString("2016-10-18 03:05:42+00"), "F35-39", "Lauren Matthews"},
				{"Suzy Slane", createTimestampFormatFromString("2016-10-18 03:06:24+00"), "F35-39", "Desiree Berry"},
			},
		},
		// Regression test for https://github.com/goccy/go-zetasqlite/issues/160
		{
			name: "window partitions are distinct from each other",
			query: `
WITH inventory AS (
  SELECT 'banana' AS item, 'fruit' AS kind, 2 AS purchases
  UNION ALL SELECT 'onion', 'vegetable', 3
  UNION ALL SELECT 'orange', 'fruit', 4
  ORDER BY item ASC
)
SELECT
  item,
  purchases,
  LEAD(item) OVER (PARTITION BY kind ORDER BY item ASC) AS next_in_kind,
  LAG(item) OVER (ORDER BY purchases ASC) AS next_best_seller
FROM inventory`,
			expectedRows: [][]interface{}{
				{"banana", int64(2), "orange", nil},
				{"onion", int64(3), nil, "banana"},
				{"orange", int64(4), nil, "onion"},
			},
		},
		{
			name: "lag with option",
			query: `
WITH segments AS (
  SELECT "2020-08-01" AS created_at, 10 AS rank UNION ALL
  SELECT "2020-08-01" AS created_at, 20 AS rank
) SELECT LAG(rank + 1, 1, 0) OVER(PARTITION BY created_at ORDER BY rank) FROM segments`,
			expectedRows: [][]interface{}{
				{int64(0)},
				{int64(11)},
			},
		},
		{
			name: "window lag with offset",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41+00', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58+00', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14+00', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name, 2)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_ahead
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Carly Forte", createTimestampFormatFromString("2016-10-18 03:08:58+00"), "F25-29", nil},
				{"Sophia Liu", createTimestampFormatFromString("2016-10-18 02:51:45+00"), "F30-34", nil},
				{"Nikki Leith", createTimestampFormatFromString("2016-10-18 02:59:01+00"), "F30-34", nil},
				{"Jen Edwards", createTimestampFormatFromString("2016-10-18 03:06:36+00"), "F30-34", "Sophia Liu"},
				{"Meghan Lederer", createTimestampFormatFromString("2016-10-18 03:07:41+00"), "F30-34", "Nikki Leith"},
				{"Lauren Reasoner", createTimestampFormatFromString("2016-10-18 03:10:14+00"), "F30-34", "Jen Edwards"},
				{"Lisa Stelzner", createTimestampFormatFromString("2016-10-18 02:54:11+00"), "F35-39", nil},
				{"Lauren Matthews", createTimestampFormatFromString("2016-10-18 03:01:17+00"), "F35-39", nil},
				{"Desiree Berry", createTimestampFormatFromString("2016-10-18 03:05:42+00"), "F35-39", "Lisa Stelzner"},
				{"Suzy Slane", createTimestampFormatFromString("2016-10-18 03:06:24+00"), "F35-39", "Lauren Matthews"},
			},
		},
		{
			name: "window lag with offset and default value",
			query: `
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45+00' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11+00', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01+00', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17+00', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42+00', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24+00', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36+00', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41+00', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58+00', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14+00', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name, 2, 'NoBody')
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_ahead
FROM finishers`,
			expectedRows: [][]interface{}{
				{"Carly Forte", createTimestampFormatFromString("2016-10-18 03:08:58+00"), "F25-29", "NoBody"},
				{"Sophia Liu", createTimestampFormatFromString("2016-10-18 02:51:45+00"), "F30-34", "NoBody"},
				{"Nikki Leith", createTimestampFormatFromString("2016-10-18 02:59:01+00"), "F30-34", "NoBody"},
				{"Jen Edwards", createTimestampFormatFromString("2016-10-18 03:06:36+00"), "F30-34", "Sophia Liu"},
				{"Meghan Lederer", createTimestampFormatFromString("2016-10-18 03:07:41+00"), "F30-34", "Nikki Leith"},
				{"Lauren Reasoner", createTimestampFormatFromString("2016-10-18 03:10:14+00"), "F30-34", "Jen Edwards"},
				{"Lisa Stelzner", createTimestampFormatFromString("2016-10-18 02:54:11+00"), "F35-39", "NoBody"},
				{"Lauren Matthews", createTimestampFormatFromString("2016-10-18 03:01:17+00"), "F35-39", "NoBody"},
				{"Desiree Berry", createTimestampFormatFromString("2016-10-18 03:05:42+00"), "F35-39", "Lisa Stelzner"},
				{"Suzy Slane", createTimestampFormatFromString("2016-10-18 03:06:24+00"), "F35-39", "Lauren Matthews"},
			},
		},
		{
			name:  "sign",
			query: `SELECT SIGN(25) UNION ALL SELECT SIGN(0) UNION ALL SELECT SIGN(-25)`,
			expectedRows: [][]interface{}{
				{int64(1)}, {int64(0)}, {int64(-1)},
			},
		},

		{
			name: "bit_count",
			query: `
SELECT a, BIT_COUNT(a) AS a_bits, FORMAT("%T", b) as b, BIT_COUNT(b) AS b_bits
FROM UNNEST([
  STRUCT(0 AS a, b'' AS b), (0, b'\x00'), (5, b'\x05'), (8, b'\x00\x08'),
  (0xFFFF, b'\xFF\xFF'), (-2, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE'),
  (-1, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
  (NULL, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF')
]) AS x`,
			expectedRows: [][]interface{}{
				{int64(0), int64(0), `b""`, int64(0)},
				{int64(0), int64(0), `b"\x00"`, int64(0)},
				{int64(5), int64(2), `b"\x05"`, int64(2)},
				{int64(8), int64(1), `b"\x00\x08"`, int64(1)},
				{int64(65535), int64(16), `b"\xff\xff"`, int64(16)},
				{int64(-2), int64(63), `b"\xff\xff\xff\xff\xff\xff\xff\xfe"`, int64(63)},
				{int64(-1), int64(64), `b"\xff\xff\xff\xff\xff\xff\xff\xff"`, int64(64)},
				{nil, nil, `b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"`, int64(80)},
			},
		},

		// array functions
		{
			name:         "make_array",
			query:        `SELECT a, b FROM UNNEST([STRUCT(DATE(2022, 1, 1) AS a, 1 AS b)])`,
			expectedRows: [][]interface{}{{"2022-01-01", int64(1)}},
		},
		{
			name: "unnest with offset",
			query: `SELECT *
FROM UNNEST(['foo', 'bar', 'baz'])
  AS element
WITH OFFSET AS offset
ORDER BY offset DESC;`,
			expectedRows: [][]interface{}{{"baz", int64(2)}, {"bar", int64(1)}, {"foo", int64(0)}},
		},
		{
			name:  "array function",
			query: `SELECT ARRAY (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS new_array`,
			expectedRows: [][]interface{}{
				{[]interface{}{int64(1), int64(2), int64(3)}},
			},
		},
		// Regression tests for goccy/go-zetasqlite#176
		{
			name: "array scan left outer join",
			query: `WITH produce AS (select 'lettuce' AS item UNION ALL SELECT 'banana')
SELECT item, in_stock_items is not null AS item_in_stock FROM produce
LEFT OUTER JOIN unnest(['lettuce']) in_stock_items ON in_stock_items = item;`,
			expectedRows: [][]interface{}{
				{"lettuce", true},
				{"banana", false},
			},
		},
		{
			name: "array scan inner join",
			query: `WITH produce AS (select 'lettuce' AS item UNION ALL SELECT 'banana')
SELECT item, in_stock_items is not null AS item_in_stock FROM produce
INNER JOIN unnest(['lettuce']) in_stock_items ON in_stock_items = item;`,
			expectedRows: [][]interface{}{
				{"lettuce", true},
			},
		},
		{
			name:  "array function with struct",
			query: `SELECT ARRAY (SELECT AS STRUCT 1, 2, 3 UNION ALL SELECT AS STRUCT 4, 5, 6) AS new_array`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						[]map[string]interface{}{
							{
								"": float64(1),
							},
							{
								"": float64(2),
							},
							{
								"": float64(3),
							},
						},
						[]map[string]interface{}{
							{
								"": float64(4),
							},
							{
								"": float64(5),
							},
							{
								"": float64(6),
							},
						},
					},
				},
			},
		},
		{
			name:  "array function with multiple array",
			query: `SELECT ARRAY (SELECT AS STRUCT [1, 2, 3] UNION ALL SELECT AS STRUCT [4, 5, 6]) AS new_array`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						[]map[string]interface{}{
							{
								"": []interface{}{
									float64(1),
									float64(2),
									float64(3),
								},
							},
						},
						[]map[string]interface{}{
							{
								"": []interface{}{
									float64(4),
									float64(5),
									float64(6),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "array function with other column",
			query: `
SELECT ARRAY (
	SELECT 1
) AS new_array,
1 as new_column
`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						int64(1),
					},
					int64(1),
				},
			},
		}, {
			name:  "array_concat function",
			query: `SELECT ARRAY_CONCAT([1, 2], [3, 4], [5, 6]) as count_to_six`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)},
				},
			},
		},
		{
			name:         "array_length function",
			query:        `SELECT ARRAY_LENGTH([1, 2, 3, 4]) as length`,
			expectedRows: [][]interface{}{{int64(4)}},
		},
		{
			name: "array_to_string function",
			query: `
WITH items AS
  (SELECT ['coffee', 'tea', 'milk' ] as list
  UNION ALL
  SELECT ['cake', 'pie', NULL] as list)
SELECT ARRAY_TO_STRING(list, '--') AS text FROM items`,
			expectedRows: [][]interface{}{
				{"coffee--tea--milk"},
				{"cake--pie"},
			},
		},
		{
			name: "array_to_string function with null text",
			query: `
WITH items AS
  (SELECT ['coffee', 'tea', 'milk' ] as list
  UNION ALL
  SELECT ['cake', 'pie', NULL] as list)

SELECT ARRAY_TO_STRING(list, '--', 'MISSING') AS text FROM items`,
			expectedRows: [][]interface{}{
				{"coffee--tea--milk"},
				{"cake--pie--MISSING"},
			},
		},
		{
			name:         "generate_array function",
			query:        `SELECT GENERATE_ARRAY(1, 5) AS example_array`,
			expectedRows: [][]interface{}{{[]interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}}},
		},
		{
			name:         "generate_array function with step",
			query:        `SELECT GENERATE_ARRAY(0, 10, 3) AS example_array`,
			expectedRows: [][]interface{}{{[]interface{}{int64(0), int64(3), int64(6), int64(9)}}},
		},
		{
			name:         "generate_array function with negative step value",
			query:        `SELECT GENERATE_ARRAY(10, 0, -3) AS example_array`,
			expectedRows: [][]interface{}{{[]interface{}{int64(10), int64(7), int64(4), int64(1)}}},
		},
		{
			name:         "generate_array function with large step value",
			query:        `SELECT GENERATE_ARRAY(4, 4, 10) AS example_array`,
			expectedRows: [][]interface{}{{[]interface{}{int64(4)}}},
		},
		{
			name:         "generate_array function with over step value",
			query:        `SELECT GENERATE_ARRAY(10, 0, 3) AS example_array`,
			expectedRows: [][]interface{}{{[]interface{}{}}},
		},
		{
			name:         "generate_array function with null",
			query:        `SELECT GENERATE_ARRAY(5, NULL, 1) AS example_array`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:  "generate_array function for generate multiple array",
			query: `SELECT GENERATE_ARRAY(start, 5) AS example_array FROM UNNEST([3, 4, 5]) AS start`,
			expectedRows: [][]interface{}{
				{[]interface{}{int64(3), int64(4), int64(5)}},
				{[]interface{}{int64(4), int64(5)}},
				{[]interface{}{int64(5)}},
			},
		},
		{
			name:  "generate_date_array function",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08') AS example`,
			expectedRows: [][]interface{}{
				{[]interface{}{"2016-10-05", "2016-10-06", "2016-10-07", "2016-10-08"}},
			},
		},
		{
			name:  "generate_date_array function with step",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-09', INTERVAL 2 DAY) AS example`,
			expectedRows: [][]interface{}{
				{[]interface{}{"2016-10-05", "2016-10-07", "2016-10-09"}},
			},
		},
		{
			name:  "generate_date_array function with negative step",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-01', INTERVAL -3 DAY) AS example`,
			expectedRows: [][]interface{}{
				{[]interface{}{"2016-10-05", "2016-10-02"}},
			},
		},
		{
			name:  "generate_date_array function with same value",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-05', INTERVAL 8 DAY) AS example`,
			expectedRows: [][]interface{}{
				{[]interface{}{"2016-10-05"}},
			},
		},
		{
			name:  "generate_date_array function with over step",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-01', INTERVAL 1 DAY) AS example`,
			expectedRows: [][]interface{}{
				{[]interface{}{}},
			},
		},
		{
			name:  "generate_date_array function with null",
			query: `SELECT GENERATE_DATE_ARRAY('2016-10-05', NULL) AS example`,
			expectedRows: [][]interface{}{
				{nil},
			},
		},
		{
			name:  "generate_date_array function with month",
			query: `SELECT GENERATE_DATE_ARRAY('2016-01-01', '2016-12-31', INTERVAL 2 MONTH) AS example`,
			expectedRows: [][]interface{}{
				{[]interface{}{"2016-01-01", "2016-03-01", "2016-05-01", "2016-07-01", "2016-09-01", "2016-11-01"}},
			},
		},
		{
			name: "generate_date_array function with variable",
			query: `
SELECT GENERATE_DATE_ARRAY(date_start, date_end, INTERVAL 1 WEEK) AS date_range
FROM (
  SELECT DATE '2016-01-01' AS date_start, DATE '2016-01-31' AS date_end
  UNION ALL SELECT DATE "2016-04-01", DATE "2016-04-30"
  UNION ALL SELECT DATE "2016-07-01", DATE "2016-07-31"
  UNION ALL SELECT DATE "2016-10-01", DATE "2016-10-31"
) AS items`,
			expectedRows: [][]interface{}{
				{[]interface{}{"2016-01-01", "2016-01-08", "2016-01-15", "2016-01-22", "2016-01-29"}},
				{[]interface{}{"2016-04-01", "2016-04-08", "2016-04-15", "2016-04-22", "2016-04-29"}},
				{[]interface{}{"2016-07-01", "2016-07-08", "2016-07-15", "2016-07-22", "2016-07-29"}},
				{[]interface{}{"2016-10-01", "2016-10-08", "2016-10-15", "2016-10-22", "2016-10-29"}},
			},
		},
		{
			name:  "generate_timestamp_array function",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '2016-10-05 00:00:00+00', '2016-10-07 00:00:00+00', INTERVAL 1 DAY) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						createTimestampFormatFromString("2016-10-05 00:00:00+00"),
						createTimestampFormatFromString("2016-10-06 00:00:00+00"),
						createTimestampFormatFromString("2016-10-07 00:00:00+00"),
					},
				},
			},
		},
		{
			name:  "generate_timestamp_array function interval 1 second",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00+00', '2016-10-05 00:00:02+00', INTERVAL 1 SECOND) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						createTimestampFormatFromString("2016-10-05 00:00:00+00"),
						createTimestampFormatFromString("2016-10-05 00:00:01+00"),
						createTimestampFormatFromString("2016-10-05 00:00:02+00"),
					},
				},
			},
		},
		{
			name:  "generate_timestamp_array function negative interval",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00+00', '2016-10-01 00:00:00+00', INTERVAL -2 DAY) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						createTimestampFormatFromString("2016-10-06 00:00:00+00"),
						createTimestampFormatFromString("2016-10-04 00:00:00+00"),
						createTimestampFormatFromString("2016-10-02 00:00:00+00"),
					},
				},
			},
		},
		{
			name:  "generate_timestamp_array function same value",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00+00', '2016-10-05 00:00:00+00', INTERVAL 1 HOUR) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						createTimestampFormatFromString("2016-10-05 00:00:00+00"),
					},
				},
			},
		},
		{
			name:  "generate_timestamp_array function over step",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00+00', '2016-10-05 00:00:00+00', INTERVAL 1 HOUR) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{[]interface{}{}},
			},
		},
		{
			name:  "generate_timestamp_array function with null",
			query: `SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00+00', NULL, INTERVAL 1 HOUR) AS timestamp_array`,
			expectedRows: [][]interface{}{
				{nil},
			},
		},
		{
			name: "generate_timestamp_array function with variable",
			query: `
SELECT GENERATE_TIMESTAMP_ARRAY(start_timestamp, end_timestamp, INTERVAL 1 HOUR)
  AS timestamp_array
FROM
  (SELECT
    TIMESTAMP '2016-10-05 00:00:00+00' AS start_timestamp,
    TIMESTAMP '2016-10-05 02:00:00+00' AS end_timestamp
   UNION ALL
   SELECT
    TIMESTAMP '2016-10-05 12:00:00+00' AS start_timestamp,
    TIMESTAMP '2016-10-05 14:00:00+00' AS end_timestamp
   UNION ALL
   SELECT
    TIMESTAMP '2016-10-05 23:59:00+00' AS start_timestamp,
    TIMESTAMP '2016-10-06 01:59:00+00' AS end_timestamp)`,
			expectedRows: [][]interface{}{
				{
					[]interface{}{
						createTimestampFormatFromString("2016-10-05 00:00:00+00"),
						createTimestampFormatFromString("2016-10-05 01:00:00+00"),
						createTimestampFormatFromString("2016-10-05 02:00:00+00"),
					},
				},
				{
					[]interface{}{
						createTimestampFormatFromString("2016-10-05 12:00:00+00"),
						createTimestampFormatFromString("2016-10-05 13:00:00+00"),
						createTimestampFormatFromString("2016-10-05 14:00:00+00"),
					},
				},
				{
					[]interface{}{
						createTimestampFormatFromString("2016-10-05 23:59:00+00"),
						createTimestampFormatFromString("2016-10-06 00:59:00+00"),
						createTimestampFormatFromString("2016-10-06 01:59:00+00"),
					},
				},
			},
		},
		// Regression test for goccy/go-zetasqlite#179
		{
			name: "null array scan",
			query: `
WITH file AS (
  SELECT 1 AS file_id, ARRAY<STRING>["r", "w"] AS modes
  UNION ALL SELECT 2, ARRAY<STRING>["w"]
)
SELECT id,
(SELECT mode FROM UNNEST(modes) AS mode WHERE mode = 'w') IS NOT NULL AS write_mode,
(SELECT mode FROM UNNEST(modes) AS mode WHERE mode = 'r') IS NOT NULL AS read_mode
FROM UNNEST([1, 2, 3]) id
LEFT JOIN file on file.file_id = id`,
			expectedRows: [][]interface{}{{int64(1), true, true}, {int64(2), true, false}, {int64(3), false, false}},
		},

		{
			name: "array_reverse function",
			query: `
WITH example AS (
  SELECT [1, 2, 3] AS arr UNION ALL
  SELECT [4, 5] AS arr UNION ALL
  SELECT [] AS arr
) SELECT ARRAY_REVERSE(arr) AS reverse_arr FROM example`,
			expectedRows: [][]interface{}{
				{[]interface{}{int64(3), int64(2), int64(1)}},
				{[]interface{}{int64(5), int64(4)}},
				{[]interface{}{}},
			},
		},
		{
			name: "group by",
			query: `
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  day,
  SUM(price) AS total
FROM Sales
GROUP BY day`,
			expectedRows: [][]interface{}{
				{int64(1), float64(23.54)},
				{int64(2), float64(9.99)},
				{int64(3), float64(6.24)},
			},
		},
		{
			name: "group by rollup with one column",
			query: `
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  day,
  SUM(price) AS total
FROM Sales
GROUP BY ROLLUP(day)`,
			expectedRows: [][]interface{}{
				{nil, float64(39.77)},
				{int64(1), float64(23.54)},
				{int64(2), float64(9.99)},
				{int64(3), float64(6.24)},
			},
		},
		{
			name: "group by rollup with two columns",
			query: `
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  sku,
  day,
  SUM(price) AS total
FROM Sales
GROUP BY ROLLUP(sku, day)
ORDER BY sku, day`,
			expectedRows: [][]interface{}{
				{nil, nil, float64(39.77)},
				{int64(123), nil, float64(28.97)},
				{int64(123), int64(1), float64(18.98)},
				{int64(123), int64(2), float64(9.99)},
				{int64(456), nil, float64(8.81)},
				{int64(456), int64(1), float64(4.56)},
				{int64(456), int64(3), float64(4.25)},
				{int64(789), nil, float64(1.99)},
				{int64(789), int64(3), float64(1.99)},
			},
		},
		{
			name: "group by having",
			query: `
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 2, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  day,
  SUM(price) AS total
FROM Sales
GROUP BY day HAVING SUM(price) > 10`,
			expectedRows: [][]interface{}{
				{int64(1), float64(23.54)},
				{int64(2), float64(10.99)},
			},
		},
		{
			name:  "order by",
			query: `SELECT x, y FROM (SELECT 1 AS x, true AS y UNION ALL SELECT 9, true UNION ALL SELECT NULL, false) ORDER BY x`,
			expectedRows: [][]interface{}{
				{nil, false},
				{int64(1), true},
				{int64(9), true},
			},
		},
		{
			name:  "order by with nulls last",
			query: `SELECT x, y FROM (SELECT 1 AS x, true AS y UNION ALL SELECT 9, true UNION ALL SELECT NULL, false) ORDER BY x NULLS LAST`,
			expectedRows: [][]interface{}{
				{int64(1), true},
				{int64(9), true},
				{nil, false},
			},
		},
		{
			name:  "order by desc",
			query: `SELECT x, y FROM (SELECT 1 AS x, true AS y UNION ALL SELECT 9, true UNION ALL SELECT NULL, false) ORDER BY x DESC`,
			expectedRows: [][]interface{}{
				{int64(9), true},
				{int64(1), true},
				{nil, false},
			},
		},
		{
			name:  "order by nulls first",
			query: `SELECT x, y FROM (SELECT 1 AS x, true AS y UNION ALL SELECT 9, true UNION ALL SELECT NULL, false) ORDER BY x DESC NULLS FIRST`,
			expectedRows: [][]interface{}{
				{nil, false},
				{int64(9), true},
				{int64(1), true},
			},
		},
		{
			name: "inner join with using",
			query: `
WITH Roster AS
 (SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL
  SELECT 'Buchanan', 52 UNION ALL
  SELECT 'Coolidge', 52 UNION ALL
  SELECT 'Davis', 51 UNION ALL
  SELECT 'Eisenhower', 77),
 TeamMascot AS
 (SELECT 50 as SchoolID, 'Jaguars' as Mascot UNION ALL
  SELECT 51, 'Knights' UNION ALL
  SELECT 52, 'Lakers' UNION ALL
  SELECT 53, 'Mustangs')
SELECT * FROM Roster INNER JOIN TeamMascot USING (SchoolID)
`,
			expectedRows: [][]interface{}{
				{int64(50), "Adams", "Jaguars"},
				{int64(52), "Buchanan", "Lakers"},
				{int64(52), "Coolidge", "Lakers"},
				{int64(51), "Davis", "Knights"},
			},
		},
		{
			name: "left join",
			query: `
WITH Roster AS
 (SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL
  SELECT 'Buchanan', 52 UNION ALL
  SELECT 'Coolidge', 52 UNION ALL
  SELECT 'Davis', 51 UNION ALL
  SELECT 'Eisenhower', 77),
 TeamMascot AS
 (SELECT 50 as SchoolID, 'Jaguars' as Mascot UNION ALL
  SELECT 51, 'Knights' UNION ALL
  SELECT 52, 'Lakers' UNION ALL
  SELECT 53, 'Mustangs')
SELECT Roster.LastName, TeamMascot.Mascot FROM Roster LEFT JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID
`,
			expectedRows: [][]interface{}{
				{"Adams", "Jaguars"},
				{"Buchanan", "Lakers"},
				{"Coolidge", "Lakers"},
				{"Davis", "Knights"},
				{"Eisenhower", nil},
			},
		},
		{
			name: "right join",
			query: `
WITH Roster AS
 (SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL
  SELECT 'Buchanan', 52 UNION ALL
  SELECT 'Coolidge', 52 UNION ALL
  SELECT 'Davis', 51 UNION ALL
  SELECT 'Eisenhower', 77),
 TeamMascot AS
 (SELECT 50 as SchoolID, 'Jaguars' as Mascot UNION ALL
  SELECT 51, 'Knights' UNION ALL
  SELECT 52, 'Lakers' UNION ALL
  SELECT 53, 'Mustangs')
SELECT Roster.LastName, TeamMascot.Mascot FROM Roster RIGHT JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID
`,
			expectedRows: [][]interface{}{
				{"Adams", "Jaguars"},
				{"Buchanan", "Lakers"},
				{"Coolidge", "Lakers"},
				{"Davis", "Knights"},
				{nil, "Mustangs"},
			},
		},
		{
			name: "full join",
			query: `
WITH Roster AS
 (SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL
  SELECT 'Buchanan', 52 UNION ALL
  SELECT 'Coolidge', 52 UNION ALL
  SELECT 'Davis', 51 UNION ALL
  SELECT 'Eisenhower', 77),
 TeamMascot AS
 (SELECT 50 as SchoolID, 'Jaguars' as Mascot UNION ALL
  SELECT 51, 'Knights' UNION ALL
  SELECT 52, 'Lakers' UNION ALL
  SELECT 53, 'Mustangs')
SELECT Roster.LastName, TeamMascot.Mascot FROM Roster FULL JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID
`,
			expectedRows: [][]interface{}{
				{"Adams", "Jaguars"},
				{"Buchanan", "Lakers"},
				{"Coolidge", "Lakers"},
				{"Davis", "Knights"},
				{"Eisenhower", nil},
				{nil, "Mustangs"},
			},
		},
		{
			name: "qualify",
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT
  item,
  RANK() OVER (PARTITION BY category ORDER BY purchases DESC) as rank
FROM Produce WHERE Produce.category = 'vegetable' QUALIFY rank <= 3`,
			expectedRows: [][]interface{}{
				{"kale", int64(1)},
				{"lettuce", int64(2)},
				{"cabbage", int64(3)},
			},
		},
		// Regression test goccy/go-zetasqlite#123
		{
			name: "qualify without group by / where / having",
			query: `WITH toks AS (SELECT 1 AS x UNION ALL SELECT 2 AS x)
			SELECT x FROM toks QUALIFY MAX(x) OVER (PARTITION BY x) > 1`,
			expectedRows: [][]interface{}{
				{int64(2)},
			},
		},
		// Regression test goccy/go-zetasqlite#150
		{
			name: "qualify group",
			query: `
				WITH produce AS (
					SELECT 'kale' AS item, 23 AS purchases
				)
				SELECT item, sum(purchases)
				FROM produce
				GROUP BY item
				QUALIFY ROW_NUMBER() OVER (PARTITION BY item ORDER BY item) = 1
			`,
			expectedRows: [][]interface{}{{"kale", int64(23)}},
		},
		// Regression test goccy/go-zetasqlite#147
		{
			name: "subselect qualifier",
			query: `
				WITH produce AS (SELECT 'banana' AS item, 3 AS purchases),
				toks AS (
					SELECT item FROM (
						SELECT * FROM produce
						WHERE item = 'banana'
					) sub
					WHERE purchases = 3
				)
				SELECT * FROM toks;`,
			expectedRows: [][]interface{}{{"banana"}},
		},
		{
			name: "qualify direct",
			query: `
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT item FROM Produce WHERE Produce.category = 'vegetable' QUALIFY RANK() OVER (PARTITION BY category ORDER BY purchases DESC) <= 3`,
			expectedRows: [][]interface{}{
				{"kale"},
				{"lettuce"},
				{"cabbage"},
			},
		},
		{
			name:        "invalid cast",
			query:       `SELECT CAST("apple" AS INT64) AS not_a_number`,
			expectedErr: `failed to analyze: INVALID_ARGUMENT: Could not cast literal "apple" to type INT64 [at 1:13]`,
		},
		// Regression test for goccy/go-zetasqlite#175
		{
			name:        "cast integer to datetime",
			query:       `WITH toks AS (SELECT "20100317" AS dt) SELECT CAST(dt AS DATETIME) FROM toks;`,
			expectedErr: "failed to convert 20100317 to time.Time type",
		},
		{
			name:         "safe cast",
			query:        `SELECT SAFE_CAST(x AS STRING) FROM UNNEST([1, 2, 3]) AS x`,
			expectedRows: [][]interface{}{{"1"}, {"2"}, {"3"}},
		},
		{
			name:         "safe cast for invalid cast",
			query:        `SELECT SAFE_CAST("apple" AS INT64) AS not_a_number`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "cast string to int64",
			query:        `SELECT CAST('0x87a' as INT64), CAST(CONCAT('0x', '87a') as INT64), CAST(SUBSTR('q0x87a', 2) as INT64), CAST(s AS INT64) FROM (SELECT CONCAT('0x', '87a') AS s)`,
			expectedRows: [][]interface{}{{int64(2170), int64(2170), int64(2170), int64(2170)}},
		},
		{
			name: "cast string to int64 - leading zeros",
			query: `WITH toks AS (
				SELECT "000800" AS x
				UNION ALL SELECT "-0900"
				UNION ALL SELECT "+000100"
				UNION ALL SELECT "0"
				UNION ALL SELECT "0000"
			)
			SELECT ARRAY_AGG(CAST(x AS INT64)) FROM toks`,
			expectedRows: [][]interface{}{{[]any{int64(800), int64(-900), int64(100), int64(0), int64(0)}}},
		},

		// hash functions
		{
			name: "farm_fingerprint",
			query: `
WITH example AS (
  SELECT 1 AS x, "foo" AS y, true AS z UNION ALL
  SELECT 2 AS x, "apple" AS y, false AS z UNION ALL
  SELECT 3 AS x, "" AS y, true AS z
) SELECT *, FARM_FINGERPRINT(CONCAT(CAST(x AS STRING), y, CAST(z AS STRING))) FROM example`,
			expectedRows: [][]interface{}{
				{int64(1), "foo", true, int64(-1541654101129638711)},
				{int64(2), "apple", false, int64(2794438866806483259)},
				{int64(3), "", true, int64(-4880158226897771312)},
			},
		},
		{
			name:         "md5",
			query:        `SELECT MD5("Hello World")`,
			expectedRows: [][]interface{}{{"sQqNsWTgdUEFt6mb5y4/5Q=="}},
		},
		{
			name:         "sha1",
			query:        `SELECT SHA1("Hello World")`,
			expectedRows: [][]interface{}{{"Ck1VqNd45QIvq3AZd8XYQLvEhtA="}},
		},
		{
			name:         "sha256",
			query:        `SELECT SHA256("Hello World")`,
			expectedRows: [][]interface{}{{"pZGm1Av0IEBKARczz7exkNYsZb8LzaMrV7J32a2fFG4="}},
		},
		{
			name:         "sha512",
			query:        `SELECT SHA512("Hello World")`,
			expectedRows: [][]interface{}{{"LHT9F+2v2A6ER7DUZ0HuJDt+t03SFJoKsbkkb7MDgvJ+hT2FhXGeDmfL2g2qj1FnEGRhXWRa4nrLFb+xRH9Fmw=="}},
		},

		// string functions
		{
			name:         "ascii",
			query:        `SELECT ASCII('abcd'), ASCII('a'), ASCII(''), ASCII(NULL)`,
			expectedRows: [][]interface{}{{int64(97), int64(97), int64(0), nil}},
		},
		{
			name: "byte_length",
			query: `
WITH example AS (SELECT 'абвгд' AS characters, b'абвгд' AS bytes)
SELECT characters, BYTE_LENGTH(characters), bytes, BYTE_LENGTH(bytes) FROM example`,
			expectedRows: [][]interface{}{{"абвгд", int64(10), "0LDQsdCy0LPQtA==", int64(10)}},
		},
		{
			name:         "byte_length null",
			query:        `SELECT BYTE_LENGTH(NULL)`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name: "char_length",
			query: `
WITH example AS (SELECT 'абвгд' AS characters)
SELECT characters, CHAR_LENGTH(characters) FROM example`,
			expectedRows: [][]interface{}{{"абвгд", int64(5)}},
		},
		{
			name:         "char_length null",
			query:        `SELECT CHAR_LENGTH(NULL)`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name: "character_length",
			query: `
WITH example AS (SELECT 'абвгд' AS characters)
SELECT characters, CHARACTER_LENGTH(characters) FROM example`,
			expectedRows: [][]interface{}{{"абвгд", int64(5)}},
		},
		{
			name:         "chr",
			query:        `SELECT CHR(65), CHR(255), CHR(513), CHR(1024), CHR(97), CHR(0xF9B5), CHR(0), CHR(NULL)`,
			expectedRows: [][]interface{}{{"A", "ÿ", "ȁ", "Ѐ", "a", "例", "", nil}},
		},
		{
			name:         "code_points_to_bytes",
			query:        `SELECT CODE_POINTS_TO_BYTES([65, 98, 67, 100]), CODE_POINTS_TO_BYTES(NULL)`,
			expectedRows: [][]interface{}{{"QWJDZA==", nil}},
		},
		{
			name:         "code_points_to_string",
			query:        `SELECT CODE_POINTS_TO_STRING([65, 255, 513, 1024]), CODE_POINTS_TO_STRING([97, 0, 0xF9B5]), CODE_POINTS_TO_STRING([65, 255, NULL, 1024]), CODE_POINTS_TO_STRING(NULL)`,
			expectedRows: [][]interface{}{{"AÿȁЀ", "a例", nil, nil}},
		},
		// TODO: currently collate function is unsupported.
		// {
		//	name: "collate",
		//	query: `
		// WITH Words AS (
		//  SELECT COLLATE('a', 'und:ci') AS char1, COLLATE('Z', 'und:ci') AS char2
		// ) SELECT (Words.char1 < Words.char2) FROM Words`,
		//	expectedRows: [][]interface{}{{true}},
		// },
		{
			name:         "concat",
			query:        `SELECT CONCAT('T.P.', ' ', 'Bar'), CONCAT('Summer', ' ', 1923), CONCAT("abc"), CONCAT(1), CONCAT('A', NULL, 'C'), CONCAT(NULL)`,
			expectedRows: [][]interface{}{{"T.P. Bar", "Summer 1923", "abc", "1", nil, nil}},
		},
		// TODO: currently unsupported CONTAINS_SUBSTR function because ZetaSQL library doesn't support it.
		// {
		//	name:         "contains_substr true",
		//	query:        `SELECT CONTAINS_SUBSTR('the blue house', 'Blue house')`,
		//	expectedRows: [][]interface{}{{true}},
		// },
		// {
		//	name:         "contains_substr false",
		//	query:        `SELECT CONTAINS_SUBSTR('the red house', 'blue')`,
		//	expectedRows: [][]interface{}{{false}},
		// },
		// {
		//	name:         "contains_substr normalize",
		//	query:        `SELECT '\u2168 day' AS a, 'IX' AS b, CONTAINS_SUBSTR('\u2168', 'IX')`,
		//	expectedRows: [][]interface{}{{"Ⅸ day", "IX", true}},
		// },
		// {
		//	name:         "contains_substr struct_field",
		//	query:        `SELECT CONTAINS_SUBSTR((23, 35, 41), '35')`,
		//	expectedRows: [][]interface{}{{true}},
		// },
		// {
		//	name:         "contains_substr recursive",
		//	query:        `SELECT CONTAINS_SUBSTR(('abc', ['def', 'ghi', 'jkl'], 'mno'), 'jk')`,
		//	expectedRows: [][]interface{}{{true}},
		// },
		// {
		//	name:         "contains_substr struct with null",
		//	query:        `SELECT CONTAINS_SUBSTR((23, NULL, 41), '41')`,
		//	expectedRows: [][]interface{}{{true}},
		// },
		// {
		//	name:         "contains_substr struct with null2",
		//	query:        `SELECT CONTAINS_SUBSTR((23, NULL, 41), '35')`,
		//	expectedRows: [][]interface{}{{nil}},
		// },
		// {
		//	name:        "contains_substr nil",
		//	query:       `SELECT CONTAINS_SUBSTR('hello', NULL)`,
		//	expectedErr: true,
		// },
		// {
		//	name: "contains_substr for table all rows",
		//	query: `
		// WITH Recipes AS (
		//  SELECT 'Blueberry pancakes' as Breakfast, 'Egg salad sandwich' as Lunch, 'Potato dumplings' as Dinner UNION ALL
		//  SELECT 'Potato pancakes', 'Toasted cheese sandwich', 'Beef stroganoff' UNION ALL
		//  SELECT 'Ham scramble', 'Steak avocado salad', 'Tomato pasta' UNION ALL
		//  SELECT 'Avocado toast', 'Tomato soup', 'Blueberry salmon' UNION ALL
		//  SELECT 'Corned beef hash', 'Lentil potato soup', 'Glazed ham'
		// ) SELECT * FROM Recipes WHERE CONTAINS_SUBSTR(Recipes, 'toast')`,
		//	expectedRows: [][]interface{}{
		//	{"Potato pancakes", "Toasted cheese sandwich", "Beef stroganoff"},
		//	{"Avocado toast", "Tomato soup", "Blueberry samon"},
		//	},
		//	},
		// {
		//	name: "contains_substr for table specified rows",
		//	query: `
		// WITH Recipes AS (
		//  SELECT 'Blueberry pancakes' as Breakfast, 'Egg salad sandwich' as Lunch, 'Potato dumplings' as Dinner UNION ALL
		//  SELECT 'Potato pancakes', 'Toasted cheese sandwich', 'Beef stroganoff' UNION ALL
		//  SELECT 'Ham scramble', 'Steak avocado salad', 'Tomato pasta' UNION ALL
		//  SELECT 'Avocado toast', 'Tomato soup', 'Blueberry salmon' UNION ALL
		//  SELECT 'Corned beef hash', 'Lentil potato soup', 'Glazed ham'
		// ) SELECT * FROM Recipes WHERE CONTAINS_SUBSTR((Lunch, Dinner), 'potato')`,
		//	expectedRows: [][]interface{}{
		//		{"Bluberry pancakes", "Egg salad sandwich", "Potato dumplings"},
		//		{"Corned beef hash", "Lentil potato soup", "Glazed ham"},
		//	},
		// },
		// {
		//	name: "contains_substr for table except",
		//	query: `
		// WITH Recipes AS (
		//  SELECT 'Blueberry pancakes' as Breakfast, 'Egg salad sandwich' as Lunch, 'Potato dumplings' as Dinner UNION ALL
		//  SELECT 'Potato pancakes', 'Toasted cheese sandwich', 'Beef stroganoff' UNION ALL
		//  SELECT 'Ham scramble', 'Steak avocado salad', 'Tomato pasta' UNION ALL
		//  SELECT 'Avocado toast', 'Tomato soup', 'Blueberry salmon' UNION ALL
		//  SELECT 'Corned beef hash', 'Lentil potato soup', 'Glazed ham'
		// ) SELECT * FROM Recipes WHERE CONTAINS_SUBSTR((SELECT AS STRUCT Recipes.* EXCEPT (Lunch, Dinner)), 'potato')`,
		//	expectedRows: [][]interface{}{
		//		{"Potato pancakes", "Toasted cheese sandwich", "Beef stroganoff"},
		//	},
		// },
		{
			name:         "ends_with",
			query:        `SELECT ENDS_WITH('apple', 'e'), ENDS_WITH('banana', 'e'), ENDS_WITH('orange', 'e'), ENDS_WITH('foo', NULL), ENDS_WITH(NULL, 'foo')`,
			expectedRows: [][]interface{}{{true, false, true, nil, nil}},
		},
		{
			name:         "format %d",
			query:        `SELECT FORMAT('%d %i %o %x %X', 10, 11, 10, 255, 255)`,
			expectedRows: [][]interface{}{{"10 11 12 ff FF"}},
		},
		{
			name:         "format |%10d|",
			query:        `SELECT FORMAT('|%10d|', 11)`,
			expectedRows: [][]interface{}{{"|        11|"}},
		},
		{
			name:         "format +%010d+",
			query:        `SELECT FORMAT('+%010d+', 12)`,
			expectedRows: [][]interface{}{{"+0000000012+"}},
		},
		{
			name:         "format %'d",
			query:        `SELECT FORMAT("%'d", 123456789)`,
			expectedRows: [][]interface{}{{"123,456,789"}},
		},
		{
			name:         "format %s",
			query:        `SELECT FORMAT('-%s-', 'abcd efg'), FORMAT('-%s-', CAST(NULL AS STRING)), FORMAT('-%s %s-', 'x', CAST(NULL AS STRING))`,
			expectedRows: [][]interface{}{{"-abcd efg-", nil, nil}},
		},
		{
			name:         "format %f %E",
			query:        `SELECT FORMAT('%f %E', 1.1, 2.2)`,
			expectedRows: [][]interface{}{{"1.100000 2.200000E+00"}},
		},
		{
			name:         "format date with %t",
			query:        `SELECT FORMAT('%t', date '2015-09-01')`,
			expectedRows: [][]interface{}{{"2015-09-01"}},
		},
		{
			name:         "format timestamp with %t",
			query:        `SELECT FORMAT('%t', timestamp '2015-09-01 12:34:56 America/Los_Angeles')`,
			expectedRows: [][]interface{}{{"2015-09-01 19:34:56+00"}},
		},
		// This fails in ZetaSQL base code.
		// {
		// 	name:         "format null",
		// 	query:        `SELECT FORMAT(NULL, 'abc')`,
		// 	expectedRows: [][]interface{}{{nil}},
		// },

		{
			name:         "from_base32",
			query:        `SELECT FROM_BASE32('MFRGGZDF74======'), FROM_BASE32(NULL)`,
			expectedRows: [][]interface{}{{"YWJjZGX/", nil}},
		},
		{
			name:         "from_base64",
			query:        `SELECT FROM_BASE64('/+A='), FROM_BASE64(NULL)`,
			expectedRows: [][]interface{}{{"/+A=", nil}},
		},
		{
			name:         "from_hex",
			query:        `SELECT FROM_HEX('00010203aaeeefff'), FROM_HEX('0AF'), FROM_HEX('666f6f626172'), FROM_HEX(NULL)`,
			expectedRows: [][]interface{}{{"AAECA6ru7/8=", "AK8=", "Zm9vYmFy", nil}},
		},
		{
			name: "initcap",
			query: `
WITH example AS
(
  SELECT 'Hello World-everyone!' AS value UNION ALL
  SELECT 'tHe dog BARKS loudly+friendly' AS value UNION ALL
  SELECT 'apples&oranges;&pears' AS value UNION ALL
  SELECT 'καθίσματα ταινιών' AS value UNION ALL
  SELECT NULL as value
)
SELECT value, INITCAP(value) AS initcap_value FROM example`,
			expectedRows: [][]interface{}{
				{"Hello World-everyone!", "Hello World-Everyone!"},
				{"tHe dog BARKS loudly+friendly", "The Dog Barks Loudly+Friendly"},
				{"apples&oranges;&pears", "Apples&Oranges;&Pears"},
				{"καθίσματα ταινιών", "Καθίσματα Ταινιών"},
				{nil, nil},
			},
		},
		{
			name: "initcap with delimiters",
			query: `
WITH example AS
(
  SELECT 'hello WORLD!' AS value, '' AS delimiters UNION ALL
  SELECT 'καθίσματα ταιντιώ@ν' AS value, 'τ@' AS delimiters UNION ALL
  SELECT 'Apples1oranges2pears' AS value, '12' AS delimiters UNION ALL
  SELECT 'tHisEisEaESentence' AS value, 'E' AS delimiters UNION ALL
  SELECT NULL AS value, '' AS delimiters UNION ALL
  SELECT 'foo' AS value, NULL AS delimiters
)
SELECT value, delimiters, INITCAP(value, delimiters) AS initcap_value FROM example`,
			expectedRows: [][]interface{}{
				{"hello WORLD!", "", "Hello world!"},
				{"καθίσματα ταιντιώ@ν", "τ@", "ΚαθίσματΑ τΑιντΙώ@Ν"},
				{"Apples1oranges2pears", "12", "Apples1Oranges2Pears"},
				{"tHisEisEaESentence", "E", "ThisEIsEAESentence"},
				{nil, "", nil},
				{"foo", nil, nil},
			},
		},
		{
			name: "instr",
			query: `
WITH example AS
(
 SELECT 'banana' as source_value, 'an' as search_value, 1 as position, 1 as occurrence UNION ALL
 SELECT 'banana' as source_value, 'an' as search_value, 1 as position, 2 as occurrence UNION ALL
 SELECT 'banana' as source_value, 'an' as search_value, 1 as position, 3 as occurrence UNION ALL
 SELECT 'banana' as source_value, 'an' as search_value, 3 as position, 1 as occurrence UNION ALL
 SELECT 'banana' as source_value, 'an' as search_value, -1 as position, 1 as occurrence UNION ALL
 SELECT 'banana' as source_value, 'an' as search_value, -3 as position, 1 as occurrence UNION ALL
 SELECT 'banana' as source_value, 'ann' as search_value, 1 as position, 1 as occurrence UNION ALL
 SELECT 'helloooo' as source_value, 'oo' as search_value, 1 as position, 1 as occurrence UNION ALL
 SELECT 'helloooo' as source_value, 'oo' as search_value, 1 as position, 2 as occurrence UNION ALL
 SELECT NULL as source_value, 'oo' as search_value, 1 as position, 1 as occurrence UNION ALL
 SELECT 'helloooo' as source_value, NULL as search_value, 1 as position, 1 as occurrence UNION ALL
 SELECT 'helloooo' as source_value, 'oo' as search_value, NULL as position, 1 as occurrence UNION ALL
 SELECT 'helloooo' as source_value, 'oo' as search_value, 1 as position, NULL as occurrence
) SELECT source_value, search_value, position, occurrence, INSTR(source_value, search_value, position, occurrence) FROM example`,
			expectedRows: [][]interface{}{
				{"banana", "an", int64(1), int64(1), int64(2)},
				{"banana", "an", int64(1), int64(2), int64(4)},
				{"banana", "an", int64(1), int64(3), int64(0)},
				{"banana", "an", int64(3), int64(1), int64(4)},
				{"banana", "an", int64(-1), int64(1), int64(4)},
				{"banana", "an", int64(-3), int64(1), int64(4)},
				{"banana", "ann", int64(1), int64(1), int64(0)},
				{"helloooo", "oo", int64(1), int64(1), int64(5)},
				{"helloooo", "oo", int64(1), int64(2), int64(6)},
				{nil, "oo", int64(1), int64(1), nil},
				{"helloooo", nil, int64(1), int64(1), nil},
				{"helloooo", "oo", nil, int64(1), nil},
				{"helloooo", "oo", int64(1), nil, nil},
			},
		},
		{
			name:         "left with string value",
			query:        `SELECT LEFT('apple', 3), LEFT('banana', 3), LEFT('абвгд', 3), LEFT(NULL, 3), LEFT('apple', NULL)`,
			expectedRows: [][]interface{}{{"app", "ban", "абв", nil, nil}},
		},
		{
			name:         "left with bytes value",
			query:        `SELECT LEFT(b'apple', 3), LEFT(b'banana', 3), LEFT(b'\xab\xcd\xef\xaa\xbb', 3)`,
			expectedRows: [][]interface{}{{"YXBw", "YmFu", "q83v"}},
		},
		{
			name:         "length",
			query:        `SELECT LENGTH('абвгд'), LENGTH(CAST('абвгд' AS BYTES)), LENGTH(NULL)`,
			expectedRows: [][]interface{}{{int64(5), int64(10), nil}},
		},
		{
			name:         "lpad string without pattern",
			query:        `SELECT LPAD(t, len) FROM UNNEST([STRUCT('abc' AS t, 5 AS len),('abc', 2),('例子', 4),(NULL, 2),('abc', NULL)])`,
			expectedRows: [][]interface{}{{"  abc"}, {"ab"}, {"  例子"}, {nil}, {nil}},
		},
		{
			name:         "lpad string with pattern",
			query:        `SELECT LPAD(t, len, pattern) FROM UNNEST([STRUCT('abc' AS t, 8 AS len, 'def' AS pattern),('abc', 5, '-'),('例子', 5, '中文'),('abc', 5, NULL)])`,
			expectedRows: [][]interface{}{{"defdeabc"}, {"--abc"}, {"中文中例子"}, {nil}},
		},
		{
			name:         "lpad bytes without pattern",
			query:        `SELECT LPAD(t, len) FROM UNNEST([STRUCT(b'abc' AS t, 5 AS len),(b'abc', 2),(b'\xab\xcd\xef', 4)])`,
			expectedRows: [][]interface{}{{"ICBhYmM="}, {"YWI="}, {"IKvN7w=="}},
		},
		{
			name:         "lpad bytes with pattern",
			query:        `SELECT LPAD(t, len, pattern) FROM UNNEST([STRUCT(b'abc' AS t, 8 AS len, b'def' AS pattern),(b'abc', 5, b'-'),(b'\xab\xcd\xef', 5, b'\x00')])`,
			expectedRows: [][]interface{}{{"ZGVmZGVhYmM="}, {"LS1hYmM="}, {"AACrze8="}},
		},
		{
			name:         "lower",
			query:        `SELECT LOWER('FOO'), LOWER('BAR'), LOWER('BAZ'), LOWER(NULL)`,
			expectedRows: [][]interface{}{{"foo", "bar", "baz", nil}},
		},
		{
			name:         "ltrim",
			query:        `SELECT LTRIM('   apple   '), LTRIM('***apple***', '*'), LTRIM(NULL), LTRIM(' . ', NULL)`,
			expectedRows: [][]interface{}{{"apple   ", "apple***", nil, nil}},
		},
		{
			name:         "normalize",
			query:        `SELECT a, b, a = b FROM (SELECT NORMALIZE('\u00ea') as a, NORMALIZE('\u0065\u0302') as b)`,
			expectedRows: [][]interface{}{{"ê", "ê", true}},
		},
		{
			name:         "normalize null",
			query:        `SELECT NORMALIZE(NULL)`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name: "normalize with nfkc",
			query: `
WITH EquivalentNames AS (
  SELECT name
  FROM UNNEST([
      'Jane\u2004Doe',
      'John\u2004Smith',
      'Jane\u2005Doe',
      'Jane\u2006Doe',
      'John Smith']) AS name
) SELECT NORMALIZE(name, NFKC) AS normalized_name, COUNT(*) AS name_count FROM EquivalentNames GROUP BY 1`,
			expectedRows: [][]interface{}{
				{"Jane Doe", int64(3)},
				{"John Smith", int64(2)},
			},
		},
		{
			name:         "normalize_and_casefold",
			query:        `SELECT a, b, NORMALIZE(a) = NORMALIZE(b), NORMALIZE_AND_CASEFOLD(a) = NORMALIZE_AND_CASEFOLD(b) FROM (SELECT 'The red barn' AS a, 'The Red Barn' AS b)`,
			expectedRows: [][]interface{}{{"The red barn", "The Red Barn", false, true}},
		},
		{
			name: "normalize_and_casefold with params",
			query: `
WITH Strings AS (
  SELECT '\u2168' AS a, 'IX' AS b UNION ALL
  SELECT '\u0041\u030A', '\u00C5'
)
SELECT a, b,
  NORMALIZE_AND_CASEFOLD(a, NFD)=NORMALIZE_AND_CASEFOLD(b, NFD) AS nfd,
  NORMALIZE_AND_CASEFOLD(a, NFC)=NORMALIZE_AND_CASEFOLD(b, NFC) AS nfc,
  NORMALIZE_AND_CASEFOLD(a, NFKD)=NORMALIZE_AND_CASEFOLD(b, NFKD) AS nkfd,
  NORMALIZE_AND_CASEFOLD(a, NFKC)=NORMALIZE_AND_CASEFOLD(b, NFKC) AS nkfc
FROM Strings`,
			expectedRows: [][]interface{}{
				{"Ⅸ", "IX", false, false, true, true},
				{"Å", "Å", true, true, true, true},
			},
		},
		{
			name: "octet_length",
			query: `
WITH example AS (SELECT 'абвгд' AS characters, b'абвгд' AS bytes)
SELECT characters, OCTET_LENGTH(characters), bytes, OCTET_LENGTH(bytes) FROM example`,
			expectedRows: [][]interface{}{{"абвгд", int64(10), "0LDQsdCy0LPQtA==", int64(10)}},
		},
		{
			name:         "octet_length null",
			query:        `SELECT OCTET_LENGTH(NULL)`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name: "regexp_contains",
			query: `
SELECT email, REGEXP_CONTAINS(email, r'@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')
 FROM (SELECT ['foo@example.com', 'bar@example.org', 'www.example.net', NULL] AS addresses),
 UNNEST(addresses) AS email`,
			expectedRows: [][]interface{}{{"foo@example.com", true}, {"bar@example.org", true}, {"www.example.net", false}, {nil, nil}},
		},
		{
			name: "regexp_contains2",
			query: `
SELECT email,
  REGEXP_CONTAINS(email, r'^([\w.+-]+@foo\.com|[\w.+-]+@bar\.org)$'),
  REGEXP_CONTAINS(email, r'^[\w.+-]+@foo\.com|[\w.+-]+@bar\.org$')
FROM
  (SELECT ['a@foo.com', 'a@foo.computer', 'b@bar.org', '!b@bar.org', 'c@buz.net'] AS addresses),
  UNNEST(addresses) AS email`,
			expectedRows: [][]interface{}{
				{"a@foo.com", true, true},
				{"a@foo.computer", false, true},
				{"b@bar.org", true, true},
				{"!b@bar.org", false, true},
				{"c@buz.net", false, false},
			},
		},
		{
			name:         "regexp_contains null pattern",
			query:        `SELECT REGEXP_CONTAINS('abc', NULL)`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "regexp_extract",
			query:        `SELECT email, REGEXP_EXTRACT(email, r'^[a-zA-Z0-9_.+-]+') FROM UNNEST(['foo@example.com', 'bar@example.com', 'baz@example.net', NULL]) email`,
			expectedRows: [][]interface{}{{"foo@example.com", "foo"}, {"bar@example.com", "bar"}, {"baz@example.net", "baz"}, {nil, nil}},
		},
		{
			name:         "regexp_extract null pattern",
			query:        `SELECT REGEXP_EXTRACT('abc', NULL)`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name: "regexp_extract with capture",
			query: `
WITH email_addresses AS (
  SELECT 'foo@example.com' as email UNION ALL SELECT 'bar@example.org' as email UNION ALL SELECT 'baz@example.net' as email
) SELECT REGEXP_EXTRACT(email, r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.([a-zA-Z0-9-.]+$)') FROM email_addresses`,
			expectedRows: [][]interface{}{{"com"}, {"org"}, {"net"}},
		},
		{
			name: "regexp_extract with position and occurrence",
			query: `
WITH example AS
 (
   SELECT 'Hello Helloo and Hellooo' AS value, 'H?ello+' AS regex, 1 as position, 1 AS occurrence UNION ALL
   SELECT 'Hello Helloo and Hellooo', 'H?ello+', 1, 2 UNION ALL
   SELECT 'Hello Helloo and Hellooo', 'H?ello+', 1, 3 UNION ALL
   SELECT 'Hello Helloo and Hellooo', 'H?ello+', 1, 4 UNION ALL
   SELECT 'Hello Helloo and Hellooo', 'H?ello+', 2, 1 UNION ALL
   SELECT 'Hello Helloo and Hellooo', 'H?ello+', 3, 1 UNION ALL
   SELECT 'Hello Helloo and Hellooo', 'H?ello+', 3, 2 UNION ALL
   SELECT 'Hello Helloo and Hellooo', 'H?ello+', 3, 3 UNION ALL
   SELECT 'Hello Helloo and Hellooo', 'H?ello+', 20, 1 UNION ALL
   SELECT 'cats&dogs&rabbits', '\\w+&', 1, 2 UNION ALL
   SELECT 'cats&dogs&rabbits', '\\w+&', 2, 3 UNION ALL
   SELECT NULL,'\\w+&', 1, 2 UNION ALL
   SELECT 'cats&dogs&rabbits', NULL, 1, 2 UNION ALL
   SELECT 'cats&dogs&rabbits', '\\w+&', NULL, 2 UNION ALL
   SELECT 'cats&dogs&rabbits', '\\w+&', 1, NULL
) SELECT value, regex, position, occurrence, REGEXP_EXTRACT(value, regex, position, occurrence) FROM example`,
			expectedRows: [][]interface{}{
				{"Hello Helloo and Hellooo", "H?ello+", int64(1), int64(1), "Hello"},
				{"Hello Helloo and Hellooo", "H?ello+", int64(1), int64(2), "Helloo"},
				{"Hello Helloo and Hellooo", "H?ello+", int64(1), int64(3), "Hellooo"},
				{"Hello Helloo and Hellooo", "H?ello+", int64(1), int64(4), nil},
				{"Hello Helloo and Hellooo", "H?ello+", int64(2), int64(1), "ello"},
				{"Hello Helloo and Hellooo", "H?ello+", int64(3), int64(1), "Helloo"},
				{"Hello Helloo and Hellooo", "H?ello+", int64(3), int64(2), "Hellooo"},
				{"Hello Helloo and Hellooo", "H?ello+", int64(3), int64(3), nil},
				{"Hello Helloo and Hellooo", "H?ello+", int64(20), int64(1), nil},
				{"cats&dogs&rabbits", `\w+&`, int64(1), int64(2), "dogs&"},
				{"cats&dogs&rabbits", `\w+&`, int64(2), int64(3), nil},
				{nil, `\w+&`, int64(1), int64(2), nil},
				{"cats&dogs&rabbits", nil, int64(1), int64(2), nil},
				{"cats&dogs&rabbits", `\w+&`, nil, int64(2), nil},
				{"cats&dogs&rabbits", `\w+&`, int64(1), nil, nil},
			},
		},
		{
			name:         "regexp_extract_all",
			query:        "WITH code_markdown AS (SELECT 'Try `function(x)` or `function(y)`' as code) SELECT REGEXP_EXTRACT_ALL(code, '`(.+?)`') FROM code_markdown",
			expectedRows: [][]interface{}{{[]interface{}{"function(x)", "function(y)"}}},
		},
		{
			name:         "regexp_extract_all null",
			query:        "SELECT REGEXP_EXTRACT_ALL(NULL, '`(.+?)`'), REGEXP_EXTRACT_ALL('abc123', NULL)",
			expectedRows: [][]interface{}{{nil, nil}},
		},
		{
			name: "regexp_instr",
			query: `
WITH example AS (
  SELECT 'ab@gmail.com' AS source_value, '@[^.]*' AS regexp UNION ALL
  SELECT 'ab@mail.com', '@[^.]*' UNION ALL
  SELECT 'abc@gmail.com', '@[^.]*' UNION ALL
  SELECT 'abc.com', '@[^.]*' UNION ALL
  SELECT NULL, '@[^.]*' UNION ALL
  SELECT 'abc.com', NULL
) SELECT source_value, regexp, REGEXP_INSTR(source_value, regexp) FROM example`,
			expectedRows: [][]interface{}{
				{"ab@gmail.com", "@[^.]*", int64(3)},
				{"ab@mail.com", "@[^.]*", int64(3)},
				{"abc@gmail.com", "@[^.]*", int64(4)},
				{"abc.com", "@[^.]*", int64(0)},
				{nil, "@[^.]*", nil},
				{"abc.com", nil, nil},
			},
		},
		{
			name: "regexp_instr with position",
			query: `
WITH example AS (
  SELECT 'a@gmail.com b@gmail.com' AS source_value, '@[^.]*' AS regexp, 1 AS position UNION ALL
  SELECT 'a@gmail.com b@gmail.com', '@[^.]*', 2 UNION ALL
  SELECT 'a@gmail.com b@gmail.com', '@[^.]*', 3 UNION ALL
  SELECT 'a@gmail.com b@gmail.com', '@[^.]*', 4 UNION ALL
  SELECT 'a@gmail.com b@gmail.com', '@[^.]*', NULL
) SELECT source_value, regexp, position, REGEXP_INSTR(source_value, regexp, position) FROM example`,
			expectedRows: [][]interface{}{
				{"a@gmail.com b@gmail.com", "@[^.]*", int64(1), int64(2)},
				{"a@gmail.com b@gmail.com", "@[^.]*", int64(2), int64(2)},
				{"a@gmail.com b@gmail.com", "@[^.]*", int64(3), int64(14)},
				{"a@gmail.com b@gmail.com", "@[^.]*", int64(4), int64(14)},
				{"a@gmail.com b@gmail.com", "@[^.]*", nil, nil},
			},
		},
		{
			name: "regexp_instr with occurrence",
			query: `
WITH example AS (
  SELECT 'a@gmail.com b@gmail.com c@gmail.com' AS source_value, '@[^.]*' AS regexp, 1 AS position, 1 AS occurrence UNION ALL
  SELECT 'a@gmail.com b@gmail.com c@gmail.com', '@[^.]*', 1, 2 UNION ALL
  SELECT 'a@gmail.com b@gmail.com c@gmail.com', '@[^.]*', 1, 3 UNION ALL
  SELECT 'a@gmail.com b@gmail.com c@gmail.com', '@[^.]*', 1, NULL
) SELECT source_value, regexp, position, occurrence, REGEXP_INSTR(source_value, regexp, position, occurrence) FROM example`,
			expectedRows: [][]interface{}{
				{"a@gmail.com b@gmail.com c@gmail.com", "@[^.]*", int64(1), int64(1), int64(2)},
				{"a@gmail.com b@gmail.com c@gmail.com", "@[^.]*", int64(1), int64(2), int64(14)},
				{"a@gmail.com b@gmail.com c@gmail.com", "@[^.]*", int64(1), int64(3), int64(26)},
				{"a@gmail.com b@gmail.com c@gmail.com", "@[^.]*", int64(1), nil, nil},
			},
		},
		{
			name: "regexp_instr with occurrence position",
			query: `
WITH example AS (
  SELECT 'a@gmail.com' AS source_value, '@[^.]*' AS regexp, 1 AS position, 1 AS occurrence, 0 AS o_position UNION ALL
  SELECT 'a@gmail.com', '@[^.]*', 1, 1, 1 UNION ALL
  SELECT 'a@gmail.com', '@[^.]*', 1, 1, NULL
) SELECT source_value, regexp, position, occurrence, o_position, REGEXP_INSTR(source_value, regexp, position, occurrence, o_position) FROM example`,
			expectedRows: [][]interface{}{
				{"a@gmail.com", "@[^.]*", int64(1), int64(1), int64(0), int64(2)},
				{"a@gmail.com", "@[^.]*", int64(1), int64(1), int64(1), int64(8)},
				{"a@gmail.com", "@[^.]*", int64(1), int64(1), nil, nil},
			},
		},
		{
			name: "regexp_replace",
			query: `
WITH markdown AS (
  SELECT '# Heading' as heading UNION ALL
  SELECT '# Another heading' as heading
) SELECT REGEXP_REPLACE(heading, r'^# ([a-zA-Z0-9\s]+$)', '<h1>\\1</h1>') FROM markdown`,
			expectedRows: [][]interface{}{
				{"<h1>Heading</h1>"},
				{"<h1>Another heading</h1>"},
			},
		},
		// Regression tests for goccy/go-zetasqlite#178
		{
			name:  "regexp_replace quoted",
			query: `SELECT REGEXP_REPLACE('"quote123"', r'["\d]', '')`,
			expectedRows: [][]interface{}{
				{"quote"},
			},
		},

		{
			name:         "regexp_replace null",
			query:        `SELECT REGEXP_REPLACE(NULL, r'\:\d\d\d', ''), REGEXP_REPLACE('abc', NULL, ''), REGEXP_REPLACE('abc', r'\:\d\d\d', NULL)`,
			expectedRows: [][]interface{}{{nil, nil, nil}},
		},
		{
			name: "regexp_substr",
			query: `
WITH example AS (
  SELECT 'Hello World Helloo' AS value, 'H?ello+' AS regex, 1 AS position, 1 AS occurrence UNION ALL
  SELECT NULL, 'H?ello+', 1, 1 UNION ALL
  SELECT 'Hello World Helloo', NULL, 1, 1 UNION ALL
  SELECT 'Hello World Helloo', 'H?ello+', NULL, 1 UNION ALL
  SELECT 'Hello World Helloo', 'H?ello+', 1, NULL
) SELECT value, regex, position, occurrence, REGEXP_SUBSTR(value, regex, position, occurrence) FROM example`,
			expectedRows: [][]interface{}{
				{"Hello World Helloo", "H?ello+", int64(1), int64(1), "Hello"},
				{nil, "H?ello+", int64(1), int64(1), nil},
				{"Hello World Helloo", nil, int64(1), int64(1), nil},
				{"Hello World Helloo", "H?ello+", nil, int64(1), nil},
				{"Hello World Helloo", "H?ello+", int64(1), nil, nil},
			},
		},
		{
			name: "replace",
			query: `
WITH desserts AS (
  SELECT 'apple pie' as dessert UNION ALL
  SELECT 'blackberry pie' as dessert UNION ALL
  SELECT 'cherry pie' as dessert
) SELECT REPLACE (dessert, 'pie', 'cobbler') FROM desserts`,
			expectedRows: [][]interface{}{
				{"apple cobbler"},
				{"blackberry cobbler"},
				{"cherry cobbler"},
			},
		},
		{
			name:         "replace null",
			query:        `SELECT REPLACE(NULL, 'foo', ''), REPLACE('abc', NULL, ''), REPLACE('abc', 'foo', NULL)`,
			expectedRows: [][]interface{}{{nil, nil, nil}},
		},
		{
			name:  "repeat",
			query: `SELECT t, n, REPEAT(t, n) FROM UNNEST([STRUCT('abc' AS t, 3 AS n),('例子', 2),('abc', null),(null, 3)])`,
			expectedRows: [][]interface{}{
				{"abc", int64(3), "abcabcabc"},
				{"例子", int64(2), "例子例子"},
				{"abc", nil, nil},
				{nil, int64(3), nil},
			},
		},
		{
			name: "reverse",
			query: `
WITH example AS (
  SELECT 'foo' AS sample_string, b'bar' AS sample_bytes UNION ALL
  SELECT 'абвгд', b'123' UNION ALL
  SELECT CAST(NULL AS STRING), CAST(NULL AS BYTES)
) SELECT sample_string, REVERSE(sample_string), sample_bytes, REVERSE(sample_bytes) FROM example`,
			expectedRows: [][]interface{}{
				{"foo", "oof", "YmFy", "cmFi"},
				{"абвгд", "дгвба", "MTIz", "MzIx"},
				{nil, nil, nil, nil},
			},
		},
		{
			name: "right string",
			query: `
WITH examples AS (
  SELECT 'apple' as example UNION ALL
  SELECT 'banana' as example UNION ALL
  SELECT 'абвгд' as example UNION ALL
  SELECT NULL as example
) SELECT example, RIGHT(example, 3) FROM examples`,
			expectedRows: [][]interface{}{
				{"apple", "ple"},
				{"banana", "ana"},
				{"абвгд", "вгд"},
				{nil, nil},
			},
		},
		{
			name: "right bytes",
			query: `
WITH examples AS (
  SELECT b'apple' as example UNION ALL
  SELECT b'banana' as example UNION ALL
  SELECT b'\xab\xcd\xef\xaa\xbb' as example
) SELECT example, RIGHT(example, 3) FROM examples`,
			expectedRows: [][]interface{}{
				{"YXBwbGU=", "cGxl"},
				{"YmFuYW5h", "YW5h"},
				{"q83vqrs=", "76q7"},
			},
		},
		{
			name:  "rpad string",
			query: `SELECT t, len, FORMAT('%T', RPAD(t, len)) FROM UNNEST([STRUCT('abc' AS t, 5 AS len),('abc', 2),('例子', 4),(NULL, 2),('abc', NULL)])`,
			expectedRows: [][]interface{}{
				{"abc", int64(5), `"abc  "`},
				{"abc", int64(2), `"ab"`},
				{"例子", int64(4), `"例子  "`},
				{nil, int64(2), nil},
				{"abc", nil, nil},
			},
		},
		{
			name: "rpad string with pattern",
			query: `SELECT t, len, pattern, FORMAT('%T', RPAD(t, len, pattern)) FROM UNNEST([
  STRUCT('abc' AS t, 8 AS len, 'def' AS pattern),
  ('abc', 5, '-'),
  ('abc', 5, NULL),
  ('例子', 5, '中文')])`,
			expectedRows: [][]interface{}{
				{"abc", int64(8), "def", `"abcdefde"`},
				{"abc", int64(5), "-", `"abc--"`},
				{"abc", int64(5), nil, nil},
				{"例子", int64(5), "中文", `"例子中文中"`},
			},
		},
		{
			name: "rpad bytes",
			query: `SELECT FORMAT('%T', t) AS t, len, FORMAT('%T', RPAD(t, len)) FROM UNNEST([
  STRUCT(b'abc' AS t, 5 AS len),
  (b'abc', 2),
  (b'\xab\xcd\xef', 4)])`,
			expectedRows: [][]interface{}{
				{`b"abc"`, int64(5), `b"abc  "`},
				{`b"abc"`, int64(2), `b"ab"`},
				{`b"\xab\xcd\xef"`, int64(4), `b"\xab\xcd\xef "`},
			},
		},
		{
			name: "rpad bytes with pattern",
			query: `SELECT FORMAT('%T', t) AS t, len, FORMAT('%T', pattern) AS pattern, FORMAT('%T', RPAD(t, len, pattern)) FROM UNNEST([
  STRUCT(b'abc' AS t, 8 AS len, b'def' AS pattern),
  (b'abc', 5, b'-'),
  (b'\xab\xcd\xef', 5, b'\x00')])`,
			expectedRows: [][]interface{}{
				{`b"abc"`, int64(8), `b"def"`, `b"abcdefde"`},
				{`b"abc"`, int64(5), `b"-"`, `b"abc--"`},
				{`b"\xab\xcd\xef"`, int64(5), `b"\x00"`, `b"\xab\xcd\xef\x00\x00"`},
			},
		},
		{
			name: "rtrim",
			query: `
WITH items AS (
  SELECT '***apple***' as item UNION ALL
  SELECT '***banana***' as item UNION ALL
  SELECT '***orange***' as item UNION ALL
  SELECT NULL as item
) SELECT RTRIM(item, '*') FROM items`,
			expectedRows: [][]interface{}{
				{"***apple"},
				{"***banana"},
				{"***orange"},
				{nil},
			},
		},
		{
			name: "rtrim2",
			query: `
WITH items AS (
  SELECT 'applexxx' as item UNION ALL
  SELECT 'bananayyy' as item UNION ALL
  SELECT 'orangezzz' as item UNION ALL
  SELECT 'pearxyz' as item
) SELECT RTRIM(item, 'xyz') FROM items`,
			expectedRows: [][]interface{}{
				{"apple"},
				{"banana"},
				{"orange"},
				{"pear"},
			},
		},
		{
			name:         "safe_convert_bytes_to_string",
			query:        `SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\xc2'), SAFE_CONVERT_BYTES_TO_STRING(NULL)`,
			expectedRows: [][]interface{}{{"�", nil}},
		},
		{
			name: "soundex",
			query: `
WITH example AS (
  SELECT 'Ashcraft' AS value UNION ALL
  SELECT 'Raven' AS value UNION ALL
  SELECT 'Ribbon' AS value UNION ALL
  SELECT 'apple' AS value UNION ALL
  SELECT 'Hello world!' AS value UNION ALL
  SELECT '  H3##!@llo w00orld!' AS value UNION ALL
  SELECT '#1' AS value UNION ALL
  SELECT NULL AS value
) SELECT value, SOUNDEX(value) FROM example`,
			expectedRows: [][]interface{}{
				{"Ashcraft", "A261"},
				{"Raven", "R150"},
				{"Ribbon", "R150"},
				{"apple", "a140"},
				{"Hello world!", "H464"},
				{"  H3##!@llo w00orld!", "H464"},
				{"#1", ""},
				{nil, nil},
			},
		},
		{
			name: "split",
			query: `
WITH letters AS (
  SELECT '' as letter_group UNION ALL
  SELECT 'a' as letter_group UNION ALL
  SELECT 'b c d' as letter_group UNION ALL
  SELECT NULL as letter_group
) SELECT SPLIT(letter_group, ' ') FROM letters`,
			expectedRows: [][]interface{}{
				{[]interface{}{""}},
				{[]interface{}{"a"}},
				{[]interface{}{"b", "c", "d"}},
				{[]interface{}{}},
			},
		}, {
			name:         "split null delimiter",
			query:        `SELECT SPLIT('abc', NULL), SPLIT(b'\xab\xcd\xef\xaa\xbb', NULL)`,
			expectedRows: [][]interface{}{{[]interface{}{}, []interface{}{}}},
		},
		{
			name:         "starts_with",
			query:        `SELECT STARTS_WITH('foo', 'b'), STARTS_WITH('bar', 'b'), STARTS_WITH('baz', 'b'), STARTS_WITH(NULL, 'a'), STARTS_WITH('a', NULL)`,
			expectedRows: [][]interface{}{{false, true, true, nil, nil}},
		},
		{
			name:         "strpos",
			query:        `SELECT STRPOS('foo@example.com', '@'), STRPOS('foobar@example.com', '@'), STRPOS('foobarbaz@example.com', '@'), STRPOS('quxexample.com', '@'), STRPOS(NULL, 'a'), STRPOS('a', NULL)`,
			expectedRows: [][]interface{}{{int64(4), int64(7), int64(10), int64(0), nil, nil}},
		},
		{
			name:         "substr",
			query:        `SELECT SUBSTR('apple', 2), SUBSTR('apple', 2, 2), SUBSTR('apple', -2), SUBSTR('apple', 1, 123), SUBSTR('apple', 123), SUBSTR(NULL, 1, 1), SUBSTR('foo', NULL, 1), SUBSTR('foo', 1, NULL)`,
			expectedRows: [][]interface{}{{"pple", "pp", "le", "apple", "", nil, nil, nil}},
		},
		{
			name:         "substring",
			query:        `SELECT SUBSTRING('apple', 2), SUBSTRING('apple', 2, 2), SUBSTRING('apple', -2), SUBSTRING('apple', 1, 123), SUBSTRING('apple', 123)`,
			expectedRows: [][]interface{}{{"pple", "pp", "le", "apple", ""}},
		},
		{
			name:         "to_base32",
			query:        `SELECT TO_BASE32(b'abcde\xFF'), TO_BASE32(NULL)`,
			expectedRows: [][]interface{}{{"MFRGGZDF74======", nil}},
		},
		{
			name:         "to_base64",
			query:        `SELECT TO_BASE64(b'\377\340'), TO_BASE64(NULL)`,
			expectedRows: [][]interface{}{{"/+A=", nil}},
		},
		{
			name:  "to_code_points with string value",
			query: `SELECT word, TO_CODE_POINTS(word) FROM UNNEST(['foo', 'bar', 'baz', 'giraffe', 'llama', NULL]) AS word`,
			expectedRows: [][]interface{}{
				{"foo", []interface{}{int64(102), int64(111), int64(111)}},
				{"bar", []interface{}{int64(98), int64(97), int64(114)}},
				{"baz", []interface{}{int64(98), int64(97), int64(122)}},
				{"giraffe", []interface{}{int64(103), int64(105), int64(114), int64(97), int64(102), int64(102), int64(101)}},
				{"llama", []interface{}{int64(108), int64(108), int64(97), int64(109), int64(97)}},
				{nil, []interface{}{}},
			},
		},
		{
			name:  "to_code_points with bytes value",
			query: `SELECT word, TO_CODE_POINTS(word) FROM UNNEST([b'\x00\x01\x10\xff', b'\x66\x6f\x6f']) AS word`,
			expectedRows: [][]interface{}{
				{"AAEQ/w==", []interface{}{int64(0), int64(1), int64(16), int64(255)}},
				{"Zm9v", []interface{}{int64(102), int64(111), int64(111)}},
			},
		},
		{
			name:  "to_code_points compare string and bytes",
			query: `SELECT TO_CODE_POINTS(b'Ā'), TO_CODE_POINTS('Ā')`,
			expectedRows: [][]interface{}{
				{[]interface{}{int64(196), int64(128)}, []interface{}{int64(256)}},
			},
		},
		{
			name:         "to_hex",
			query:        `SELECT TO_HEX(b'\x00\x01\x02\x03\xAA\xEE\xEF\xFF'), TO_HEX(b'foobar'), TO_HEX(NULL)`,
			expectedRows: [][]interface{}{{"00010203aaeeefff", "666f6f626172", nil}},
		},
		{
			name: "translate",
			query: `
WITH example AS (
  SELECT 'This is a cookie' AS expression, 'sco' AS source_characters, 'zku' AS target_characters UNION ALL
  SELECT 'A coaster' AS expression, 'co' AS source_characters, 'k' as target_characters UNION ALL
  SELECT NULL, 'co', 'k' UNION ALL
  SELECT 'A coaster', NULL, 'k' UNION ALL
  SELECT 'A coaster', 'co', NULL
) SELECT expression, source_characters, target_characters, TRANSLATE(expression, source_characters, target_characters) FROM example`,
			expectedRows: [][]interface{}{
				{"This is a cookie", "sco", "zku", "Thiz iz a kuukie"},
				{"A coaster", "co", "k", "A kaster"},
				{nil, "co", "k", nil},
				{"A coaster", nil, "k", nil},
				{"A coaster", "co", nil, nil},
			},
		},
		{
			name:         "trim",
			query:        `SELECT TRIM('   apple   '), TRIM('***apple***', '*'), TRIM(NULL), TRIM('abc', NULL)`,
			expectedRows: [][]interface{}{{"apple", "apple", nil, nil}},
		},
		{
			name:         "unicode",
			query:        `SELECT UNICODE('âbcd'), UNICODE('â'), UNICODE(''), UNICODE(NULL)`,
			expectedRows: [][]interface{}{{int64(226), int64(226), int64(0), nil}},
		},
		{
			name:         "upper",
			query:        `SELECT UPPER('foo'), UPPER('bar'), UPPER('baz'), UPPER(NULL)`,
			expectedRows: [][]interface{}{{"FOO", "BAR", "BAZ", nil}},
		},

		// Regression tests for goccy/go-zetasqlite#177
		{
			name:         "least greatest between string",
			query:        `SELECT LEAST("a", "b"), GREATEST("a", "b"), "b" BETWEEN "a" AND "c";`,
			expectedRows: [][]interface{}{{"a", "b", true}},
		},
		{
			name:         "least greatest between integer",
			query:        `SELECT LEAST(1, 2), GREATEST(1, 2), 2 BETWEEN 1 AND 3;`,
			expectedRows: [][]interface{}{{int64(1), int64(2), true}},
		},
		{
			name:         "least greatest date",
			query:        `SELECT LEAST(DATE '2024-02-27', DATE '2024-02-28'), GREATEST(DATE '2024-02-27', DATE '2024-02-28');`,
			expectedRows: [][]interface{}{{"2024-02-27", "2024-02-28"}},
		},

		// date functions
		{
			name:  "current_date",
			query: `SELECT CURRENT_DATE()`,
			expectedRows: [][]interface{}{
				{now.Format("2006-01-02")},
			},
		},
		{
			name:         "date_add",
			query:        `SELECT DATE_ADD('2023-01-29', INTERVAL 1 MONTH)`,
			expectedRows: [][]interface{}{{"2023-02-28"}},
		},
		{

			name:         "date_add quarter",
			query:        `SELECT DATE_ADD('2023-01-01', INTERVAL 1 QUARTER), DATE_ADD('2023-11-30', INTERVAL 1 QUARTER)`,
			expectedRows: [][]interface{}{{"2023-04-01", "2024-02-29"}},
		},
		{
			name:         "date_trunc with quarter",
			query:        `SELECT DATE_TRUNC(DATE "2017-01-05", QUARTER), DATE_TRUNC(DATE "2017-02-05", QUARTER), DATE_TRUNC(DATE "2017-08-05", QUARTER), DATE_TRUNC(DATE "2017-11-05", QUARTER), DATE_TRUNC(DATE "2017-12-31", QUARTER)`,
			expectedRows: [][]interface{}{{"2017-01-01", "2017-01-01", "2017-07-01", "2017-10-01", "2017-10-01"}},
		},

		{
			name:         "datetime_trunc with quarter",
			query:        `SELECT DATETIME_TRUNC(DATETIME "2017-01-05", QUARTER), DATETIME_TRUNC(DATETIME "2017-02-05", QUARTER), DATETIME_TRUNC(DATETIME "2017-08-05", QUARTER), DATETIME_TRUNC(DATETIME "2017-11-05", QUARTER), DATETIME_TRUNC(DATETIME "2017-12-31", QUARTER)`,
			expectedRows: [][]interface{}{{"2017-01-01T00:00:00", "2017-01-01T00:00:00", "2017-07-01T00:00:00", "2017-10-01T00:00:00", "2017-10-01T00:00:00"}},
		},
		{
			name:  "timestamp_trunc with quarter",
			query: `SELECT TIMESTAMP_TRUNC(TIMESTAMP "2017-01-05", QUARTER, "Pacific/Auckland"), TIMESTAMP_TRUNC(TIMESTAMP "2017-02-05", QUARTER), TIMESTAMP_TRUNC(TIMESTAMP "2024-02-29", QUARTER), TIMESTAMP_TRUNC(TIMESTAMP "2017-08-05", QUARTER), TIMESTAMP_TRUNC(TIMESTAMP "2017-12-31", QUARTER)`,
			expectedRows: [][]interface{}{{
				createTimestampFormatFromString("2016-12-31 11:00:00+00"),
				createTimestampFormatFromString("2017-01-01 00:00:00+00"),
				createTimestampFormatFromString("2024-01-01 00:00:00+00"),
				createTimestampFormatFromString("2017-07-01 00:00:00+00"),
				createTimestampFormatFromString("2017-10-01 00:00:00+00"),
			}},
		},
		{
			name:         "datetime_trunc with day weekday",
			query:        `SELECT DATETIME_TRUNC(DATETIME "2024-03-29", WEEK(MONDAY))`,
			expectedRows: [][]interface{}{{"2024-03-25T00:00:00"}},
		},
		{
			name: "datetime_trunc isoyear",
			query: `SELECT
  DATETIME_TRUNC('2015-06-15', ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM DATE '2015-06-15') AS isoyear_number;
`,
			expectedRows: [][]interface{}{{"2014-12-29T00:00:00", int64(2015)}},
		},
		{
			name: "PIVOT",
			query: `
WITH produce AS (
	SELECT 'Kale' AS product, 51 AS sales, 'Q1' AS quarter, 2020 AS year UNION ALL
	SELECT 'Kale', 23, 'Q2', 2020 UNION ALL
	SELECT 'Kale', 45, 'Q3', 2020 UNION ALL
	SELECT 'Kale', 3, 'Q4', 2020 UNION ALL
	SELECT 'Kale', 70, 'Q1', 2021 UNION ALL
	SELECT 'Kale', 85, 'Q2', 2021 UNION ALL
	SELECT 'Apple', 77, 'Q1', 2020 UNION ALL
	SELECT 'Apple', 0, 'Q2', 2020 UNION ALL
	SELECT 'Apple', 1, 'Q1', 2021
)
SELECT * FROM
  Produce
  PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
`,
			expectedRows: [][]interface{}{
				{"Apple", int64(2020), int64(77), int64(0), nil, nil},
				{"Apple", int64(2021), int64(1), nil, nil, nil},
				{"Kale", int64(2020), int64(51), int64(23), int64(45), int64(3)},
				{"Kale", int64(2021), int64(70), int64(85), nil, nil},
			},
		},
		{
			name: "UNPIVOT",
			query: `
WITH Produce AS (
  SELECT 'Kale' as product, 51 as Q1, 23 as Q2, 45 as Q3, 3 as Q4 UNION ALL
  SELECT 'Apple', 77, 0, 25, 2)
SELECT * FROM Produce
UNPIVOT(sales FOR quarter IN (Q1, Q2, Q3, Q4))
`,
			expectedRows: [][]interface{}{
				{"Kale", int64(51), "Q1"},
				{"Kale", int64(23), "Q2"},
				{"Kale", int64(45), "Q3"},
				{"Kale", int64(3), "Q4"},
				{"Apple", int64(77), "Q1"},
				{"Apple", int64(0), "Q2"},
				{"Apple", int64(25), "Q3"},
				{"Apple", int64(2), "Q4"},
			},
		},
		{
			name:         "date_sub",
			query:        `SELECT DATE_SUB('2023-03-31', INTERVAL 1 MONTH)`,
			expectedRows: [][]interface{}{{"2023-02-28"}},
		},
		{
			name:  "current_date",
			query: `SELECT CURRENT_DATE()`,
			expectedRows: [][]interface{}{
				{now.Format("2006-01-02")},
			},
		},
		{
			name:  "base date is epoch",
			query: `SELECT PARSE_DATE("%m", "03")`,
			expectedRows: [][]interface{}{
				{"1970-03-01"},
			},
		},
		{
			name:  "base date is epoch julian",
			query: `SELECT PARSE_DATE("%j", "001")`,
			expectedRows: [][]interface{}{
				{"1970-01-01"},
			},
		},
		{
			name:  "base datetime is epoch julian",
			query: `SELECT PARSE_DATETIME("%j", "001")`,
			expectedRows: [][]interface{}{
				{"1970-01-01T00:00:00"},
			},
		},
		{
			name:  "base date is epoch julian different day",
			query: `SELECT PARSE_DATE("%j", "002")`,
			expectedRows: [][]interface{}{
				{"1970-01-02"},
			},
		},
		{
			name:  "parse date with two digit year and julian day",
			query: `SELECT PARSE_DATE("%y%j", "70002")`,
			expectedRows: [][]interface{}{
				{"1970-01-02"},
			},
		},
		{
			name:  "parse date with two digit year before 2000 and julian day",
			query: `SELECT PARSE_DATE("%y%j", "95033")`,
			expectedRows: [][]interface{}{
				{"1995-02-02"},
			},
		},
		{
			name:  "parse datetime with two digit year before 2000 and julian day",
			query: `SELECT PARSE_DATETIME("%y%j%H%M%S", "95033101010")`,
			expectedRows: [][]interface{}{
				{"1995-02-02T10:10:10"},
			},
		},
		{
			name:  "parse date with two digit year after 2000 and julian day",
			query: `SELECT PARSE_DATE("%y%j", "22120")`,
			expectedRows: [][]interface{}{
				{"2022-04-30"},
			},
		},
		{
			name:  "parse datetime with two digit year after 2000 and julian day",
			query: `SELECT PARSE_DATETIME("%y%j-%H:%M:%S", "22120-10:10:10")`,
			expectedRows: [][]interface{}{
				{"2022-04-30T10:10:10"},
			},
		},
		{
			name:  "parse date with two digit year after 2000 and julian day leap year",
			query: `SELECT PARSE_DATE("%y%j", "24120")`,
			expectedRows: [][]interface{}{
				{"2024-04-29"},
			},
		},
		{
			name:  "parse datetime with two digit year after 2000 and julian day leap year",
			query: `SELECT PARSE_DATETIME("%y%j %H:%M", "24120 02:04")`,
			expectedRows: [][]interface{}{
				{"2024-04-29T02:04:00"},
			},
		},
		{
			name: "extract date",
			query: `
SELECT date, EXTRACT(ISOYEAR FROM date), EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date),
       EXTRACT(ISOWEEK FROM date), EXTRACT(WEEK FROM date), EXTRACT(DAY FROM date) FROM UNNEST([DATE '2015-12-23']) AS date`,
			expectedRows: [][]interface{}{{"2015-12-23", int64(2015), int64(2015), int64(12), int64(52), int64(51), int64(23)}},
		},
		{
			name:         "date_diff with week",
			query:        `SELECT DATE_DIFF(DATE '2017-10-17', DATE '2017-10-12', WEEK) AS weeks_diff`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:  "date_diff with week day",
			query: `SELECT DATE_DIFF(DATE '2024-03-19', DATE '2024-03-24', WEEK(MONDAY))`,
			expectedRows: [][]interface{}{{
				// No Mondays occurred between 2024-03-24 abd 2024-03-19
				int64(0),
			}},
		},
		{
			name: "date_diff with week day",
			query: `SELECT 
  DATE_DIFF(DATE '2024-03-19', DATE '2024-03-24', WEEK(SUNDAY)),
  DATE_DIFF(DATE '2024-03-19', DATE '2024-03-24', WEEK(MONDAY)),
  DATE_DIFF(DATE '2024-03-25', DATE '2024-03-19', WEEK(SUNDAY)),
  DATE_DIFF(DATE '2024-03-19', DATE '2024-03-25', WEEK(MONDAY)),
  DATE_DIFF(DATE '2024-03-19', DATE '2017-10-25', WEEK(MONDAY)),
  DATE_DIFF('0001-01-01', '9999-12-31', WEEK(SUNDAY))`,
			expectedRows: [][]interface{}{{
				// 1 Sunday occurred between 2024-03-19 and 2024-03-24
				int64(-1),
				// No Mondays occurred between 2024-03-24 abd 2024-03-19
				int64(0),
				// 1 Monday occurred between 2024-03-25 and 2024-03-19
				int64(1),
				// -1 Monday occurred between 2024-03-19 and 2024-03-25
				int64(-1),
				int64(334),
				int64(-521722),
			}},
		},
		{
			name: "datetime_diff with week day",
			query: `SELECT 
  DATETIME_DIFF(DATETIME '2024-03-19', DATETIME '2024-03-24', WEEK(SUNDAY)),
  DATETIME_DIFF(DATETIME '2024-03-19', DATETIME '2024-03-24', WEEK(MONDAY)),
  DATETIME_DIFF(DATETIME '2024-03-25', DATETIME '2024-03-19', WEEK(SUNDAY)),
  DATETIME_DIFF(DATETIME '2024-03-19', DATETIME '2024-03-25', WEEK(MONDAY)),
  DATETIME_DIFF(DATETIME '2024-03-19', DATETIME '2017-10-25', WEEK(MONDAY)),
	DATETIME_DIFF(DATETIME '2024-02-21', DATETIME '2024-02-29', WEEK(MONDAY))`,
			expectedRows: [][]interface{}{{
				// 1 Sunday occurred between 2024-03-19 and 2024-03-24
				int64(-1),
				// No Mondays occurred between 2024-03-24 abd 2024-03-19
				int64(0),
				// 1 Monday occurred between 2024-03-25 and 2024-03-19
				int64(1),
				// -1 Monday occurred between 2024-03-19 and 2024-03-25
				int64(-1),
				int64(334),
				int64(-1),
			}},
		},
		{
			name: "datetime_diff with week day 1 week",
			query: `SELECT 
	DATETIME_DIFF(DATETIME '2024-02-21', DATETIME '2024-02-29', WEEK(MONDAY))`,
			expectedRows: [][]interface{}{{
				int64(-1),
			}},
		},
		{
			name:  "datetime_diff with week day",
			query: `SELECT DATETIME_DIFF(DATETIME '2024-03-19', DATETIME '2024-03-25', WEEK(MONDAY))`,
			expectedRows: [][]interface{}{{
				// -1 Monday occurred between 2024-03-19 and 2024-03-25
				int64(-1),
			}},
		},
		{
			name: "timestamp diff with week day",
			query: `SELECT 
  TIMESTAMP_DIFF(DATETIME '2024-03-19', DATETIME '2024-03-24', WEEK(SUNDAY)),
  TIMESTAMP_DIFF(DATETIME '2024-03-19', DATETIME '2024-03-24', WEEK(MONDAY)),
  TIMESTAMP_DIFF(DATETIME '2024-03-25', DATETIME '2024-03-19', WEEK(SUNDAY)),
  TIMESTAMP_DIFF(DATETIME '2024-03-19', DATETIME '2024-03-25', WEEK(MONDAY)),
  TIMESTAMP_DIFF(DATETIME '2024-03-19', DATETIME '2017-10-25', WEEK(MONDAY))`,
			expectedRows: [][]interface{}{{
				// 1 Sunday occurred between 2024-03-19 and 2024-03-24
				int64(-1),
				// No Mondays occurred between 2024-03-24 abd 2024-03-19
				int64(0),
				// 1 Monday occurred between 2024-03-25 and 2024-03-19
				int64(1),
				// -1 Monday occurred between 2024-03-19 and 2024-03-25
				int64(-1),
				int64(334),
			}},
		},

		{
			name:         "date_diff with month",
			query:        `SELECT DATE_DIFF(DATE '2018-01-01', DATE '2017-10-30', MONTH) AS months_diff`,
			expectedRows: [][]interface{}{{int64(3)}},
		},
		{
			name:         "date_diff with day",
			query:        `SELECT DATE_DIFF(DATE '2021-06-06', DATE '2017-11-12', DAY) AS days_diff`,
			expectedRows: [][]interface{}{{int64(1302)}},
		},
		{
			name:         "date_from_unix_date",
			query:        `SELECT DATE_FROM_UNIX_DATE(14238) AS date_from_epoch`,
			expectedRows: [][]interface{}{{"2008-12-25"}},
		},
		{
			name:         "date_trunc with day",
			query:        `SELECT DATE_TRUNC(DATE "2008-12-25", DAY)`,
			expectedRows: [][]interface{}{{"2008-12-25"}},
		},
		{
			name:         "date_trunc with week",
			query:        `SELECT DATE_TRUNC(DATE "2017-11-07", WEEK)`,
			expectedRows: [][]interface{}{{"2017-11-05"}},
		},
		{
			name:         "date_trunc with month",
			query:        `SELECT DATE_TRUNC(DATE "2017-11-05", MONTH)`,
			expectedRows: [][]interface{}{{"2017-11-01"}},
		},
		{
			name:         "date_trunc with year",
			query:        `SELECT DATE_TRUNC(DATE "2017-11-05", YEAR)`,
			expectedRows: [][]interface{}{{"2017-01-01"}},
		},
		{
			name:         "format_date with %x",
			query:        `SELECT FORMAT_DATE("%x", DATE "2008-12-25")`,
			expectedRows: [][]interface{}{{"12/25/08"}},
		},
		{
			name:         "format_date with %y",
			query:        `SELECT FORMAT_DATE("%y", DATE "2008-12-25"), FORMAT_DATE("%y", DATE "2012-12-25")`,
			expectedRows: [][]interface{}{{"08", "12"}},
		},
		{
			name:         "format_date with %b-%d-%Y",
			query:        `SELECT FORMAT_DATE("%b-%d-%Y", DATE "2008-12-25")`,
			expectedRows: [][]interface{}{{"Dec-25-2008"}},
		},
		{
			name:         "format_date with %b %Y",
			query:        `SELECT FORMAT_DATE("%b %Y", DATE "2008-12-25")`,
			expectedRows: [][]interface{}{{"Dec 2008"}},
		},
		{
			name:         "format_date with %E4Y",
			query:        `SELECT FORMAT_DATE("%E4Y", DATE "2008-12-25")`,
			expectedRows: [][]interface{}{{"2008"}},
		},

		{
			name:         "last_day",
			query:        `SELECT LAST_DAY(DATE '2008-11-25') AS last_day`,
			expectedRows: [][]interface{}{{"2008-11-30"}},
		},
		{
			name:         "last_day with month",
			query:        `SELECT LAST_DAY(DATE '2008-11-25', MONTH) AS last_day`,
			expectedRows: [][]interface{}{{"2008-11-30"}},
		},
		{
			name:         "last_day with year",
			query:        `SELECT LAST_DAY(DATE '2008-11-25', YEAR) AS last_day`,
			expectedRows: [][]interface{}{{"2008-12-31"}},
		},
		{
			name:         "last_day with week(sunday)",
			query:        `SELECT LAST_DAY(DATE '2008-11-10', WEEK(SUNDAY)) AS last_day`,
			expectedRows: [][]interface{}{{"2008-11-15"}},
		},
		{
			name:         "last_day with week(monday)",
			query:        `SELECT LAST_DAY(DATE '2008-11-10', WEEK(MONDAY)) AS last_day`,
			expectedRows: [][]interface{}{{"2008-11-16"}},
		},
		// date parsing out of range values
		{
			name:        "parse date exceeding month maximum",
			query:       `SELECT PARSE_DATE("%m", "14")`,
			expectedErr: "error parsing [14] with format [%m]: could not parse month: part [14] is greater than maximum value [12]",
		},
		{
			name:        "parse date beneath month minimum",
			query:       `SELECT PARSE_DATE("%m", "0")`,
			expectedErr: "error parsing [0] with format [%m]: could not parse month: part [0] is less than minimum value [1]",
		},
		{
			name:        "parse date exceeding day maximum",
			query:       `SELECT PARSE_DATE("%d", "32")`,
			expectedErr: "error parsing [32] with format [%d]: could not parse day number: part [32] is greater than maximum value [31]",
		},
		{
			name:        "parse date beneath day minimum",
			query:       `SELECT PARSE_DATE("%d", "0")`,
			expectedErr: "error parsing [0] with format [%d]: could not parse day number: part [0] is less than minimum value [1]",
		},
		{
			name:        "parse date exceeding day of year maximum",
			query:       `SELECT PARSE_DATE("%j", "367")`,
			expectedErr: "error parsing [367] with format [%j]: could not parse day of year number: part [367] is greater than maximum value [366]",
		},
		{
			name:        "parse date beneath day of year minimum",
			query:       `SELECT PARSE_DATE("%j", "0")`,
			expectedErr: "error parsing [0] with format [%j]: could not parse day of year number: part [0] is less than minimum value [1]",
		},
		{
			name:         "parse date with single-digit month %m",
			query:        `SELECT PARSE_DATE("%m", "03"), PARSE_DATE("%m", "3"), PARSE_DATE("%m%Y", "032024")`,
			expectedRows: [][]interface{}{{"1970-03-01", "1970-03-01", "2024-03-01"}},
		},
		{
			name:         "parse_date with %y",
			query:        `SELECT PARSE_DATE("%y", '1'), PARSE_DATE("%y", '67'), PARSE_DATE("%y", '69')`,
			expectedRows: [][]interface{}{{"2001-01-01", "2067-01-01", "1969-01-01"}},
		},
		{
			name:         "parse date with %A %b %e %Y",
			query:        `SELECT PARSE_DATE("%A %b %e %Y", "Thursday Dec 25 2008")`,
			expectedRows: [][]interface{}{{"2008-12-25"}},
		},
		{
			name:         "parse date with %Y%m%d",
			query:        `SELECT PARSE_DATE("%Y%m%d", "20081225") AS parsed`,
			expectedRows: [][]interface{}{{"2008-12-25"}},
		},
		{
			name:         "parse date with %e",
			query:        `SELECT PARSE_DATE('%e', ' 3'), PARSE_DATE('%e', '20');`,
			expectedRows: [][]interface{}{{"1970-01-03", "1970-01-20"}},
		},
		{
			name:         "parse date with %e - leading space allows multiple digits",
			query:        `SELECT PARSE_DATE('%e', ' 20');`,
			expectedRows: [][]interface{}{{"1970-01-20"}},
		},
		{
			name:        "parse date with %F no day field",
			query:       `SELECT PARSE_DATE("%F", "2008-01") AS parsed`,
			expectedErr: "error parsing [2008-01] with format [%F]: could not parse year-month-day format: [-] not found after [2008-01]",
		},
		{
			name:        "parse date with %F no month field",
			query:       `SELECT PARSE_DATE("%F", "2008") AS parsed`,
			expectedErr: "error parsing [2008] with format [%F]: could not parse year-month-day format: [-] not found after [2008]",
		},
		{
			name:        "parse date with %F separator but no month",
			query:       `SELECT PARSE_DATE("%F", "2008-") AS parsed`,
			expectedErr: "error parsing [2008-] with format [%F]: could not parse year-month-day format: could not parse month: empty text after [2008-]",
		},
		{
			name:         "parse date with %F",
			query:        `SELECT PARSE_DATE("%F", "2008-12-25") AS parsed`,
			expectedRows: [][]interface{}{{"2008-12-25"}},
		},
		{
			name:         "parse date with %x",
			query:        `SELECT PARSE_DATE("%x", "12/25/08") AS parsed`,
			expectedRows: [][]interface{}{{"2008-12-25"}},
		},
		{
			name:        "parse date ( the year element is in different locations )",
			query:       `SELECT PARSE_DATE("%Y %A %b %e", "Thursday Dec 25 2008")`,
			expectedErr: "error parsing [Thursday Dec 25 2008] with format [%Y %A %b %e]: could not parse year: leading character is not a digit",
		},
		{
			name:         "safe parse date ( the year element is in different locations )",
			query:        `SELECT SAFE.PARSE_DATE("%Y %A %b %e", "Thursday Dec 25 2008")`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:        "parse date ( one of the year elements is missing )",
			query:       `SELECT PARSE_DATE("%A %b %e", "Thursday Dec 25 2008")`,
			expectedErr: `error parsing [Thursday Dec 25 2008] with format [%A %b %e]: found unparsed text [ 2008]`,
		},
		{
			name:         "unix_date",
			query:        `SELECT UNIX_DATE(DATE "2008-12-25") AS days_from_epoch`,
			expectedRows: [][]interface{}{{int64(14238)}},
		},

		// datetime functions
		{
			name:  "current_datetime",
			query: `SELECT CURRENT_DATETIME()`,
			expectedRows: [][]interface{}{
				{now.Format("2006-01-02T15:04:05.999999")},
			},
		},
		{
			name:  "datetime",
			query: `SELECT DATETIME(2008, 12, 25, 05, 30, 00), DATETIME(TIMESTAMP "2008-12-25 05:30:00+00", "America/Los_Angeles")`,
			expectedRows: [][]interface{}{
				{"2008-12-25T05:30:00", "2008-12-24T21:30:00"},
			},
		},
		{
			name:  "datetime_add",
			query: `SELECT DATETIME "2008-12-25 15:30:00", DATETIME_ADD(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE)`,
			expectedRows: [][]interface{}{
				{"2008-12-25T15:30:00", "2008-12-25T15:40:00"},
			},
		},
		{
			name:         "datetime_add",
			query:        `SELECT DATETIME_ADD(DATETIME '2023-01-29 00:00:00', INTERVAL 1 MONTH)`,
			expectedRows: [][]interface{}{{"2023-02-28T00:00:00"}},
		},
		{
			name:  "datetime_sub",
			query: `SELECT DATETIME "2008-12-25 15:30:00", DATETIME_SUB(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE)`,
			expectedRows: [][]interface{}{
				{"2008-12-25T15:30:00", "2008-12-25T15:20:00"},
			},
		},
		{
			name:         "datetime_sub",
			query:        `SELECT DATETIME_SUB(DATETIME '2023-03-31 00:00:00', INTERVAL 1 MONTH)`,
			expectedRows: [][]interface{}{{"2023-02-28T00:00:00"}},
		},
		{
			name:         "datetime_diff with day",
			query:        `SELECT DATETIME_DIFF(DATETIME "2010-07-07 10:20:00", DATETIME "2008-12-25 15:30:00", DAY)`,
			expectedRows: [][]interface{}{{int64(559)}},
		},
		{
			name:         "datetime_diff with week",
			query:        `SELECT DATETIME_DIFF(DATETIME '2017-10-15 00:00:00', DATETIME '2017-10-14 00:00:00', WEEK)`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "datetime_diff with year, ISOYEAR",
			query:        `SELECT DATETIME_DIFF('2017-12-30 00:00:00', '2014-12-30 00:00:00', YEAR), DATETIME_DIFF('2017-12-30 00:00:00', '2014-12-30 00:00:00', ISOYEAR)`,
			expectedRows: [][]interface{}{{int64(3), int64(2)}},
		},
		{
			name:         "datetime_diff with isoweek",
			query:        `SELECT DATETIME_DIFF('2017-12-18', '2017-12-17', WEEK), DATETIME_DIFF('2017-12-18', '2017-12-17', WEEK(MONDAY)), DATETIME_DIFF('2017-12-18', '2017-12-17', ISOWEEK)`,
			expectedRows: [][]interface{}{{int64(0), int64(1), int64(1)}},
		},
		{
			name:         "datetime_trunc with day",
			query:        `SELECT DATETIME_TRUNC(DATETIME "2008-12-25 15:30:00", DAY)`,
			expectedRows: [][]interface{}{{"2008-12-25T00:00:00"}},
		},
		{
			name:         "datetime_trunc with weekday(monday)",
			query:        `SELECT DATETIME_TRUNC(DATETIME "2017-11-05 00:00:00", WEEK(MONDAY))`,
			expectedRows: [][]interface{}{{"2017-10-30T00:00:00"}},
		},
		{
			name:         "datetime_trunc with isoyear",
			query:        `SELECT DATETIME_TRUNC('2015-06-15 00:00:00', ISOYEAR)`,
			expectedRows: [][]interface{}{{"2014-12-29T00:00:00"}},
		},
		{
			name:         "format_datetime with %c",
			query:        `SELECT FORMAT_DATETIME("%c", DATETIME "2008-12-25 15:30:00")`,
			expectedRows: [][]interface{}{{"Thu Dec 25 15:30:00 2008"}},
		},
		{
			name:         "format_datetime with %b-%d-%Y",
			query:        `SELECT FORMAT_DATETIME("%b-%d-%Y", DATETIME "2008-12-25 15:30:00")`,
			expectedRows: [][]interface{}{{"Dec-25-2008"}},
		},
		{
			name:         "format_datetime with %b %Y",
			query:        `SELECT FORMAT_DATETIME("%b %Y", DATETIME "2008-12-25 15:30:00")`,
			expectedRows: [][]interface{}{{"Dec 2008"}},
		},
		{
			name:         "format_datetime with %E3S",
			query:        `SELECT FORMAT_DATETIME("%E3S", DATETIME "2008-12-25 15:30:12.345678")`,
			expectedRows: [][]interface{}{{"12.345"}},
		},
		{
			name:         "format_datetime with %E*S",
			query:        `SELECT FORMAT_DATETIME("%E*S", DATETIME "2008-12-25 15:30:12.345678")`,
			expectedRows: [][]interface{}{{"12.345678"}},
		},
		{
			name:         "format_datetime with %E4Y",
			query:        `SELECT FORMAT_DATETIME("%E4Y", DATETIME "2008-12-25 15:30:12.345678")`,
			expectedRows: [][]interface{}{{"2008"}},
		},
		{
			name:         "parse datetime",
			query:        `SELECT PARSE_DATETIME("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008")`,
			expectedRows: [][]interface{}{{"2008-12-25T07:30:00"}},
		},
		{
			name:         "parse datetime with %c",
			query:        `SELECT PARSE_DATETIME("%c", "Thu Dec 25 07:30:00 2008")`,
			expectedRows: [][]interface{}{{"2008-12-25T07:30:00"}},
		},
		{
			name:        "parse datetime ( the year element is in different locations )",
			query:       `SELECT PARSE_DATETIME("%a %b %e %Y %I:%M:%S", "Thu Dec 25 07:30:00 2008")`,
			expectedErr: "error parsing [Thu Dec 25 07:30:00 2008] with format [%a %b %e %Y %I:%M:%S]: could not parse hour number: leading character is not a digit",
		},
		{
			name:        "parse datetime ( one of the year elements is missing )",
			query:       `SELECT PARSE_DATETIME("%a %b %e %I:%M:%S", "Thu Dec 25 07:30:00 2008")`,
			expectedErr: `error parsing [Thu Dec 25 07:30:00 2008] with format [%a %b %e %I:%M:%S]: found unparsed text [ 2008]`,
		},
		{
			name:         "parse datetime %F respectfully consuming digits",
			query:        `SELECT PARSE_DATETIME("%F", "03-1-1"), PARSE_DATETIME("%F", "003-01-1"), PARSE_DATETIME("%F", "0003-1-11")`,
			expectedRows: [][]interface{}{{"0003-01-01T00:00:00", "0003-01-01T00:00:00", "0003-01-11T00:00:00"}},
		},

		// time functions
		{
			name:  "current_time",
			query: `SELECT CURRENT_TIME()`,
			expectedRows: [][]interface{}{
				{now.Format("15:04:05.999999")},
			},
		},
		{
			name:  "time",
			query: `SELECT TIME(15, 30, 00), TIME(TIMESTAMP "2008-12-25 15:30:00+08", "America/Los_Angeles")`,
			expectedRows: [][]interface{}{
				{"15:30:00", "23:30:00"},
			},
		},
		{
			name:         "time from datetime",
			query:        `SELECT TIME(DATETIME "2008-12-25 15:30:00.000000")`,
			expectedRows: [][]interface{}{{"15:30:00"}},
		},
		{
			name:         "time_add",
			query:        `SELECT TIME_ADD(TIME "15:30:00", INTERVAL 10 MINUTE)`,
			expectedRows: [][]interface{}{{"15:40:00"}},
		},
		{
			name:         "time_sub",
			query:        `SELECT TIME_SUB(TIME "15:30:00", INTERVAL 10 MINUTE)`,
			expectedRows: [][]interface{}{{"15:20:00"}},
		},
		{
			name:         "time_diff",
			query:        `SELECT TIME_DIFF(TIME "15:30:00", TIME "14:35:00", MINUTE)`,
			expectedRows: [][]interface{}{{int64(55)}},
		},
		{
			name:         "time_trunc",
			query:        `SELECT TIME_TRUNC(TIME "15:30:00", HOUR)`,
			expectedRows: [][]interface{}{{"15:00:00"}},
		},
		{
			name:         "parse_time with %R",
			query:        `SELECT PARSE_TIME("%R", "14:30")`,
			expectedRows: [][]interface{}{{"14:30:00"}},
		},
		{
			name:        "parse_time with %R without minute element",
			query:       `SELECT PARSE_TIME("%R", "14")`,
			expectedErr: "error parsing [14] with format [%R]: could not parse hour:minute format: [:] not found after [14]",
		},

		{
			name:        "parse_time with %R without separator",
			query:       `SELECT PARSE_TIME("%R", "14")`,
			expectedErr: "error parsing [14] with format [%R]: could not parse hour:minute format: [:] not found after [14]",
		},
		{
			name:         "format_time with %k %l",
			query:        `SELECT FORMAT_TIME("%k", TIME "15:30:00"), FORMAT_TIME("%l", TIME "15:30:00");`,
			expectedRows: [][]interface{}{{"15", " 3"}},
		},
		{
			name:         "format_time with %R",
			query:        `SELECT FORMAT_TIME("%R", TIME "15:30:00")`,
			expectedRows: [][]interface{}{{"15:30"}},
		},
		{
			name:         "format_time with %E3S",
			query:        `SELECT FORMAT_TIME("%E3S", TIME "15:30:12.345678")`,
			expectedRows: [][]interface{}{{"12.345"}},
		},
		{
			name:         "format_time with %E*S",
			query:        `SELECT FORMAT_TIME("%E*S", TIME "15:30:12.345678")`,
			expectedRows: [][]interface{}{{"12.345678"}},
		},
		{
			name:         "parse time with %I:%M:%S",
			query:        `SELECT PARSE_TIME("%I:%M:%S", "07:30:00")`,
			expectedRows: [][]interface{}{{"07:30:00"}},
		},
		{
			name:         "parse time with %T",
			query:        `SELECT PARSE_TIME("%T", "07:30:00")`,
			expectedRows: [][]interface{}{{"07:30:00"}},
		},
		{
			name:        "parse time ( the seconds element is in different locations )",
			query:       `SELECT PARSE_TIME("%S:%I:%M", "07:30:00")`,
			expectedErr: "error parsing [07:30:00] with format [%S:%I:%M]: could not parse hour number: part [30] is greater than maximum value [12]",
		},
		{
			name:        "parse time ( one of the seconds elements is missing )",
			query:       `SELECT PARSE_TIME("%I:%M", "07:30:00")`,
			expectedErr: `error parsing [07:30:00] with format [%I:%M]: found unparsed text [:00]`,
		},

		// timestamp functions
		{
			name:  "current_timestamp",
			query: `SELECT CURRENT_TIMESTAMP()`,
			expectedRows: [][]interface{}{
				{createTimestampFormatFromTime(now.UTC())},
			},
		},

		{
			name:  "minimum / maximum date value",
			query: `SELECT DATE '0001-01-01', DATE '9999-12-31'`,
			expectedRows: [][]interface{}{
				{
					"0001-01-01", "9999-12-31",
				},
			},
		},
		{
			name:  "minimum / maximum timestamp value uses microsecond precision and range",
			query: `SELECT TIMESTAMP '0001-01-01 00:00:00.000000+00', TIMESTAMP '9999-12-31 23:59:59.999999+00'`,
			expectedRows: [][]interface{}{
				{
					createTimestampFormatFromTime(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)),
					createTimestampFormatFromTime(time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)),
				},
			},
		},
		{
			name:         "string",
			query:        `SELECT STRING(TIMESTAMP "2008-12-25 15:30:00+00", "UTC")`,
			expectedRows: [][]interface{}{{"2008-12-25 15:30:00+00"}},
		},
		{
			name:         "timestamp",
			query:        `SELECT TIMESTAMP("2008-12-25 15:30:00+00")`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 15:30:00+00")}},
		},
		{
			name:         "timestamp with zone",
			query:        `SELECT TIMESTAMP("2008-12-25 15:30:00", "America/Los_Angeles")`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 23:30:00+00")}},
		},
		{
			name:         "timestamp in zone",
			query:        `SELECT TIMESTAMP("2008-12-25 15:30:00 UTC")`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 15:30:00+00")}},
		},
		{
			name:         "timestamp from datetime",
			query:        `SELECT TIMESTAMP(DATETIME "2008-12-25 15:30:00")`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 15:30:00+00")}},
		},
		{
			name:         "timestamp from date",
			query:        `SELECT TIMESTAMP(DATE "2008-12-25")`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 00:00:00+00")}},
		},
		{
			name:         "timestamp_add",
			query:        `SELECT TIMESTAMP_ADD(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE)`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 15:40:00+00")}},
		},
		{
			name:         "timestamp_sub",
			query:        `SELECT TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE)`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 15:20:00+00")}},
		},
		{
			name:         "timestamp_diff",
			query:        `SELECT TIMESTAMP_DIFF(TIMESTAMP "2010-07-07 10:20:00+00", TIMESTAMP "2008-12-25 15:30:00+00", HOUR)`,
			expectedRows: [][]interface{}{{int64(13410)}},
		},
		{
			name:  "timestamp_trunc with day",
			query: `SELECT TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "UTC"), TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "America/Los_Angeles")`,
			expectedRows: [][]interface{}{
				{createTimestampFormatFromString("2008-12-25 00:00:00+00"), createTimestampFormatFromString("2008-12-25 08:00:00+00")},
			},
		},
		{
			name: "timestamp_trunc with week",
			query: `SELECT timestamp_value AS timestamp_value,
					                    TIMESTAMP_TRUNC(timestamp_value, WEEK(MONDAY), "UTC"),
					                    TIMESTAMP_TRUNC(timestamp_value, WEEK(MONDAY), "Pacific/Auckland")
					                    FROM (SELECT TIMESTAMP("2017-11-06 00:00:00+12") AS timestamp_value)`,
			expectedRows: [][]interface{}{
				{
					createTimestampFormatFromString("2017-11-05 12:00:00+00"),
					createTimestampFormatFromString("2017-10-30 00:00:00+00"),
					createTimestampFormatFromString("2017-11-05 11:00:00+00"),
				},
			},
		},
		{
			name:  "timestamp_trunc with year",
			query: `SELECT TIMESTAMP_TRUNC("2015-06-15 00:00:00+00", ISOYEAR)`,
			expectedRows: [][]interface{}{
				{createTimestampFormatFromString("2014-12-29 00:00:00+00")},
			},
		},
		{
			name:         "format_timestamp with %c",
			query:        `SELECT FORMAT_TIMESTAMP("%c", TIMESTAMP "2008-12-25 15:30:00+00", "UTC")`,
			expectedRows: [][]interface{}{{"Thu Dec 25 15:30:00 2008"}},
		},
		{
			name:         "format_timestamp with %b-%d-%Y",
			query:        `SELECT FORMAT_TIMESTAMP("%b-%d-%Y", TIMESTAMP "2008-12-25 15:30:00+00")`,
			expectedRows: [][]interface{}{{"Dec-25-2008"}},
		},
		{
			name:         "format_timestamp with %b %Y",
			query:        `SELECT FORMAT_TIMESTAMP("%b %Y", TIMESTAMP "2008-12-25 15:30:00+00")`,
			expectedRows: [][]interface{}{{"Dec 2008"}},
		},
		{
			name:         "format_timestamp with %Y-%m-%d %H:%M:%S",
			query:        `SELECT FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP "2008-12-25 15:30:21+00", "Asia/Tokyo")`,
			expectedRows: [][]interface{}{{"2008-12-26 00:30:21"}},
		},
		{
			name:         "format_timestamp with %E3S",
			query:        `SELECT FORMAT_TIMESTAMP("%E3S", TIMESTAMP "2008-12-25 15:30:12.345678+00")`,
			expectedRows: [][]interface{}{{"12.345"}},
		},
		{
			name:         "format_timestamp with %E*S",
			query:        `SELECT FORMAT_TIMESTAMP("%E*S", TIMESTAMP "2008-12-25 15:30:12.345678+00")`,
			expectedRows: [][]interface{}{{"12.345678"}},
		},
		{
			name:         "format_timestamp with %E4Y",
			query:        `SELECT FORMAT_TIMESTAMP("%E4Y", TIMESTAMP "2008-12-25 15:30:12.345678+00")`,
			expectedRows: [][]interface{}{{"2008"}},
		},
		{
			name:         "format_timestamp with %Ez",
			query:        `SELECT FORMAT_TIMESTAMP("%Ez", TIMESTAMP "2008-12-25 15:30:12.345678+00")`,
			expectedRows: [][]interface{}{{"+00:00"}},
		},
		{
			name:         "parse timestamp with %a %b %e %I:%M:%S %Y",
			query:        `SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008")`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 07:30:00+00")}},
		},
		{
			name:         "parse timestamp with %c",
			query:        `SELECT PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008")`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 07:30:00+00")}},
		},
		{
			name:         "parse timestamp with %k",
			query:        `SELECT PARSE_TIMESTAMP("%k", " 9");`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("1970-01-01 09:00:00+00")}},
		},
		{
			name:         "parse timestamp with %k",
			query:        `SELECT PARSE_TIMESTAMP("%k", " 9");`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("1970-01-01 09:00:00+00")}},
		},
		{name: "parse_timestamp with %D",
			query:        `SELECT PARSE_TIMESTAMP("%D", "02/02/99");`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("1999-02-02 00:00:00+00")}},
		},
		{
			name:  "parse timestamp with %p",
			query: `SELECT PARSE_TIMESTAMP("%I%p", "9am"), PARSE_TIMESTAMP("%I%p", "12am"), PARSE_TIMESTAMP("%l%p", " 12pm"), PARSE_TIMESTAMP("%I%p", "10PM");`,
			expectedRows: [][]interface{}{{
				createTimestampFormatFromString("1970-01-01 09:00:00+00"),
				createTimestampFormatFromString("1970-01-01 00:00:00+00"),
				createTimestampFormatFromString("1970-01-01 12:00:00+00"),
				createTimestampFormatFromString("1970-01-01 22:00:00+00"),
			}},
		},
		{
			name:         "parse timestamp with extra whitespace ",
			query:        `SELECT PARSE_TIMESTAMP("%m/%d/%Y  %H:%M:%S", "7/2/2020    09:24:28")`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2020-07-02 9:24:28+00")}},
		},
		{
			name:         "parse timestamp with %Y-%m-%d %H:%M:%S%Ez",
			query:        `SELECT PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S%Ez", "2020-06-02 23:58:40+09:00")`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2020-06-02 14:58:40+00")}},
		},
		{
			name:         "parse timestamp with %Y-%m-%d %H:%M:%E*S%Ez",
			query:        `SELECT PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E*S%Ez", "2020-06-02 23:58:40.123+09:00")`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2020-06-02 14:58:40.123+00")}},
		},
		{
			name:        "parse timestamp ( the year element is in different locations )",
			query:       `SELECT PARSE_TIMESTAMP("%a %b %e %Y %I:%M:%S", "Thu Dec 25 07:30:00 2008")`,
			expectedErr: "error parsing [Thu Dec 25 07:30:00 2008] with format [%a %b %e %Y %I:%M:%S]: could not parse hour number: leading character is not a digit",
		},
		{
			name:        "parse timestamp ( one of the year elements is missing )",
			query:       `SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S", "Thu Dec 25 07:30:00 2008")`,
			expectedErr: `error parsing [Thu Dec 25 07:30:00 2008] with format [%a %b %e %I:%M:%S]: found unparsed text [ 2008]`,
		},
		{
			name:         "timestamp_seconds",
			query:        `SELECT TIMESTAMP_SECONDS(1230219000)`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 15:30:00+00")}},
		},
		{
			name:         "timestamp_millis",
			query:        `SELECT TIMESTAMP_MILLIS(1230219000000)`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 15:30:00+00")}},
		},
		{
			name:         "timestamp_micros",
			query:        `SELECT TIMESTAMP_MICROS(1230219000000000)`,
			expectedRows: [][]interface{}{{createTimestampFormatFromString("2008-12-25 15:30:00+00")}},
		},
		{
			name:         "unix_seconds",
			query:        `SELECT UNIX_SECONDS(TIMESTAMP "2008-12-25 15:30:00+00")`,
			expectedRows: [][]interface{}{{int64(1230219000)}},
		},
		{
			name:         "unix_millis",
			query:        `SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00+00")`,
			expectedRows: [][]interface{}{{int64(1230219000000)}},
		},
		{
			name:         "unix_micros",
			query:        `SELECT UNIX_MICROS(TIMESTAMP "2008-12-25 15:30:00+00")`,
			expectedRows: [][]interface{}{{int64(1230219000000000)}},
		},
		{
			name: "extract from timestamp",
			query: `
WITH Input AS (SELECT TIMESTAMP("2008-12-25 05:30:00+00") AS timestamp_value)
SELECT
  EXTRACT(DAY FROM timestamp_value AT TIME ZONE "UTC"),
  EXTRACT(DAY FROM timestamp_value AT TIME ZONE "America/Los_Angeles"),
  EXTRACT(DATE FROM timestamp_value)
FROM Input`,
			expectedRows: [][]interface{}{
				{int64(25), int64(24), "2008-12-25"},
			},
		},

		// interval functions
		{
			name:         "interval operator",
			query:        `SELECT DATE "2020-09-22" + val FROM UNNEST([INTERVAL 1 DAY,INTERVAL -1 DAY,INTERVAL 2 YEAR,CAST('1-2 3 18:1:55' AS INTERVAL)]) as val`,
			expectedRows: [][]interface{}{{"2020-09-23T00:00:00"}, {"2020-09-21T00:00:00"}, {"2022-09-22T00:00:00"}, {"2021-11-25T18:01:55"}},
		},
		{
			name: "interval from sub operator",
			query: `
SELECT
  DATE "2021-05-20" - DATE "2020-04-19",
  DATETIME "2021-06-01 12:34:56.789" - DATETIME "2021-05-31 00:00:00",
  TIMESTAMP "2021-06-01 12:34:56.789" - TIMESTAMP "2021-05-31 00:00:00"`,
			expectedRows: [][]interface{}{
				{"0-0 396 0:0:0", "0-0 0 36:34:56.789", "0-0 0 36:34:56.789"},
			},
		},
		{
			name:         "make interval",
			query:        `SELECT MAKE_INTERVAL(1, 6, 15), MAKE_INTERVAL(hour => 10, second => 20), MAKE_INTERVAL(1, minute => 5, day => 2)`,
			expectedRows: [][]interface{}{{"1-6 15 0:0:0", "0-0 0 10:0:20", "1-0 2 0:5:0"}},
		},
		{
			name: "extract from interval",
			query: `SELECT
  EXTRACT(YEAR FROM i), EXTRACT(MONTH FROM i), EXTRACT(DAY FROM i),
  EXTRACT(HOUR FROM i),  EXTRACT(MINUTE FROM i),  EXTRACT(SECOND FROM i),  EXTRACT(MILLISECOND FROM i),  EXTRACT(MICROSECOND FROM i)
  FROM UNNEST([INTERVAL '1-2 3 4:5:6.789999' YEAR TO SECOND, INTERVAL '0-13 370 48:61:61' YEAR TO SECOND]) AS i`,
			expectedRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(789), int64(789999)},
				{int64(1), int64(1), int64(370), int64(49), int64(2), int64(1), int64(0), int64(0)},
			},
		},
		{
			name:         "justify_days",
			query:        `SELECT JUSTIFY_DAYS(INTERVAL 29 DAY), JUSTIFY_DAYS(INTERVAL -30 DAY), JUSTIFY_DAYS(INTERVAL 31 DAY), JUSTIFY_DAYS(INTERVAL -65 DAY), JUSTIFY_DAYS(INTERVAL 370 DAY)`,
			expectedRows: [][]interface{}{{"0-0 29 0:0:0", "-0-1 0 0:0:0", "0-1 1 0:0:0", "-0-2 -5 0:0:0", "1-0 10 0:0:0"}},
		},
		{
			name:         "justify_hours",
			query:        `SELECT JUSTIFY_HOURS(INTERVAL 23 HOUR), JUSTIFY_HOURS(INTERVAL -24 HOUR), JUSTIFY_HOURS(INTERVAL 47 HOUR), JUSTIFY_HOURS(INTERVAL -12345 MINUTE)`,
			expectedRows: [][]interface{}{{"0-0 0 23:0:0", "0-0 -1 0:0:0", "0-0 1 23:0:0", "0-0 -8 -13:45:0"}},
		},
		{
			name:         "justify_interval",
			query:        `SELECT JUSTIFY_INTERVAL(INTERVAL '29 49:00:00' DAY TO SECOND)`,
			expectedRows: [][]interface{}{{"0-1 1 1:0:0"}},
		},

		// numeric/bignumeric
		{
			name:         "cast numeric and bignumeric",
			query:        `SELECT cast('12.4E17' as NUMERIC) numeric, cast('12.4E37' as BIGNUMERIC) bignumeric`,
			expectedRows: [][]interface{}{{"1240000000000000000", "124000000000000000000000000000000000000"}},
		},
		{
			name:         "parse_numeric",
			query:        `SELECT PARSE_NUMERIC("123.45"), PARSE_NUMERIC("12.34E27"), PARSE_NUMERIC("1.0123456789")`,
			expectedRows: [][]interface{}{{"123.45", "12340000000000000000000000000", "1.012345679"}},
		},
		{
			name:         "parse_bignumeric",
			query:        `SELECT PARSE_BIGNUMERIC("123.45"), PARSE_BIGNUMERIC("123.456E37"), PARSE_BIGNUMERIC("1.123456789012345678901234567890123456789")`,
			expectedRows: [][]interface{}{{"123.45", "1234560000000000000000000000000000000000", "1.12345678901234567890123456789012345679"}},
		},
		{
			name:         "cast numeric and bignumeric to string",
			query:        `SELECT cast(PARSE_NUMERIC("123.456") as STRING), cast(PARSE_BIGNUMERIC("123.456") as STRING)`,
			expectedRows: [][]interface{}{{"123.456", "123.456"}},
		},

		// security functions
		{
			name:         "session_user",
			query:        `SELECT SESSION_USER()`,
			expectedRows: [][]interface{}{{"dummy"}},
		},

		// uuid functions
		{
			name:         "generate_uuid",
			query:        `SELECT LENGTH(GENERATE_UUID())`,
			expectedRows: [][]interface{}{{int64(36)}},
		},

		// debugging functions
		{
			name: "error",
			query: `
SELECT
  CASE
    WHEN value = 'foo' THEN 'Value is foo.'
    WHEN value = 'bar' THEN 'Value is bar.'
    ELSE ERROR(CONCAT('Found unexpected value: ', value))
  END AS new_value
FROM (
  SELECT 'foo' AS value UNION ALL
  SELECT 'bar' AS value UNION ALL
  SELECT 'baz' AS value)`,
			expectedRows: [][]interface{}{{"Value is foo."}, {"Value is bar."}},
			expectedErr:  "Found unexpected value: baz",
		},

		// begin-end
		{
			name: "begin-end",
			query: `
BEGIN
  SELECT 1;
END;`,
			expectedRows: [][]interface{}{{int64(1)}},
		},

		// create temp function
		{
			name: "create temp function",
			query: `
CREATE TEMP FUNCTION Add(x INT64, y INT64) AS (x + y);
SELECT Add(3, 4);
`,
			expectedRows: [][]interface{}{{int64(7)}},
		},

		// except
		{
			name:         "except",
			query:        `WITH orders AS (SELECT 5 as order_id, "sprocket" as item_name, 200 as quantity) SELECT * EXCEPT (order_id) FROM orders`,
			expectedRows: [][]interface{}{{"sprocket", int64(200)}},
		},
		{
			name:         "except",
			query:        `SELECT * FROM UNNEST(ARRAY<int64>[1, 2, 3]) AS number EXCEPT DISTINCT SELECT 1`,
			expectedRows: [][]interface{}{{int64(2)}, {int64(3)}},
		},

		// replace
		{
			name:         "replace",
			query:        `WITH orders AS (SELECT 5 as order_id, "sprocket" as item_name, 200 as quantity) SELECT * REPLACE ("widget" AS item_name) FROM orders`,
			expectedRows: [][]interface{}{{int64(5), "widget", int64(200)}},
		},
		{
			name:         "replace",
			query:        `WITH orders AS (SELECT 5 as order_id, "sprocket" as item_name, 200 as quantity) SELECT * REPLACE (quantity/2 AS quantity) FROM orders`,
			expectedRows: [][]interface{}{{int64(5), "sprocket", float64(100)}},
		},

		// json
		{
			name: "json value subscript operator",
			query: `
SELECT json_value.class.students[0]['name'] AS first_student
FROM
  UNNEST(
    [
      JSON '{"class" : {"students" : [{"name" : "Jane"}]}}',
      JSON '{"class" : {"students" : []}}',
      JSON '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'])
    AS json_value`,
			expectedRows: [][]interface{}{{`"Jane"`}, {nil}, {`"John"`}},
		},
		{
			name:         "json_extract",
			query:        `SELECT JSON_EXTRACT(JSON '{"class":{"students":[{"id":5},{"id":12}]}}', '$.class')`,
			expectedRows: [][]interface{}{{`{"students":[{"id":5},{"id":12}]}`}},
		},
		{
			name: "json_extract for format",
			query: `
SELECT JSON_EXTRACT(json_text, '$') AS json_text_string
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
]) AS json_text`,
			expectedRows: [][]interface{}{
				{`{"class":{"students":[{"name":"Jane"}]}}`},
				{`{"class":{"students":[]}}`},
				{`{"class":{"students":[{"name":"John"},{"name":"Jamie"}]}}`},
			},
		},
		{
			name: "json_extract with array",
			query: `
SELECT JSON_EXTRACT(json_text, '$.class.students[0]') AS first_student
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
]) AS json_text`,
			expectedRows: [][]interface{}{{`{"name":"Jane"}`}, {nil}, {`{"name":"John"}`}},
		},
		{
			name: "json_extract for name",
			query: `
SELECT JSON_EXTRACT(json_text, '$.class.students[1].name') AS second_student_name
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name" : null}]}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
]) AS json_text`,
			expectedRows: [][]interface{}{{nil}, {nil}, {nil}, {`"Jamie"`}},
		},
		{
			name: "json_extract with escape",
			query: `
SELECT JSON_EXTRACT(json_text, "$.class['students']") AS student_names
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
]) AS json_text`,
			expectedRows: [][]interface{}{{`[{"name":"Jane"}]`}, {`[]`}, {`[{"name":"John"},{"name":"Jamie"}]`}},
		},
		{
			name: "json_extract and null",
			query: `
SELECT
  JSON_EXTRACT('{"a":null}', "$.a"),
  JSON_EXTRACT('{"a":null}', "$.b"),
  JSON_EXTRACT(JSON '{"a":null}', "$.a"),
  JSON_EXTRACT(JSON '{"a":null}', "$.b")`,
			expectedRows: [][]interface{}{{nil, nil, nil, nil}},
		},
		{
			name:         "json_query",
			query:        `SELECT JSON_QUERY(JSON '{"class":{"students":[{"id":5},{"id":12}]}}', '$.class')`,
			expectedRows: [][]interface{}{{`{"students":[{"id":5},{"id":12}]}`}},
		},
		{
			name: "json_query for format",
			query: `
SELECT JSON_QUERY(json_text, '$') AS json_text_string
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
]) AS json_text`,
			expectedRows: [][]interface{}{
				{`{"class":{"students":[{"name":"Jane"}]}}`},
				{`{"class":{"students":[]}}`},
				{`{"class":{"students":[{"name":"John"},{"name":"Jamie"}]}}`},
			},
		},
		{
			name: "json_query with array",
			query: `
SELECT JSON_QUERY(json_text, '$.class.students[0]') AS first_student
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
]) AS json_text`,
			expectedRows: [][]interface{}{{`{"name":"Jane"}`}, {nil}, {`{"name":"John"}`}},
		},
		{
			name: "json_query for name",
			query: `
SELECT JSON_QUERY(json_text, '$.class.students[1].name') AS second_student_name
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name" : null}]}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
]) AS json_text`,
			expectedRows: [][]interface{}{{nil}, {nil}, {nil}, {`"Jamie"`}},
		},
		{
			name: "json_query with escape",
			query: `
SELECT JSON_QUERY(json_text, '$.class."students"') AS student_names
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
]) AS json_text`,
			expectedRows: [][]interface{}{{`[{"name":"Jane"}]`}, {`[]`}, {`[{"name":"John"},{"name":"Jamie"}]`}},
		},
		{
			name: "json_query and null",
			query: `
SELECT
  JSON_QUERY('{"a":null}', "$.a"),
  JSON_QUERY('{"a":null}', "$.b"),
  JSON_QUERY(JSON '{"a":null}', "$.a"),
  JSON_QUERY(JSON '{"a":null}', "$.b")`,
			expectedRows: [][]interface{}{{nil, nil, nil, nil}},
		},
		{
			name:         "json_extract_scalar with number",
			query:        `SELECT JSON_EXTRACT_SCALAR(JSON '{ "name" : "Jakob", "age" : "6" }', '$.age')`,
			expectedRows: [][]interface{}{{`6`}},
		},
		{
			name:         "json_extract_scalar with string",
			query:        `SELECT JSON_EXTRACT_SCALAR('{ "name" : "Jakob", "age" : "6" }', '$.name')`,
			expectedRows: [][]interface{}{{`Jakob`}},
		},
		{
			name:         "json_extract_scalar with array",
			query:        `SELECT JSON_EXTRACT_SCALAR('{"fruits": ["apple", "banana"]}', '$.fruits')`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "json_extract_scalar with escape",
			query:        `SELECT JSON_EXTRACT_SCALAR('{"a.b": {"c": "world"}}', "$['a.b'].c")`,
			expectedRows: [][]interface{}{{"world"}},
		},
		{
			name:         "json_value with number",
			query:        `SELECT JSON_VALUE(JSON '{ "name" : "Jakob", "age" : "6" }', '$.age')`,
			expectedRows: [][]interface{}{{`6`}},
		},
		{
			name:         "json_value with string",
			query:        `SELECT JSON_VALUE('{ "name" : "Jakob", "age" : "6" }', '$.name')`,
			expectedRows: [][]interface{}{{`Jakob`}},
		},
		{
			name:         "json_value with array",
			query:        `SELECT JSON_VALUE('{"fruits": ["apple", "banana"]}', '$.fruits')`,
			expectedRows: [][]interface{}{{nil}},
		},
		{
			name:         "json_value with escape",
			query:        `SELECT JSON_VALUE('{"a.b": {"c": "world"}}', '$."a.b".c')`,
			expectedRows: [][]interface{}{{"world"}},
		},
		{
			name:         "json_value with null",
			query:        `SELECT JSON_VALUE(JSON 'null'), JSON_VALUE(NULL), JSON_VALUE(JSON '{}', '$.does_not_exist')`,
			expectedRows: [][]interface{}{{nil, nil, nil}},
		},
		{
			name:  "json_extract_array",
			query: `SELECT JSON_EXTRACT_ARRAY(JSON '{"fruits":["apples","oranges","grapes"]}','$.fruits')`,
			expectedRows: [][]interface{}{
				{[]interface{}{`"apples"`, `"oranges"`, `"grapes"`}},
			},
		},
		{
			name:  "json_extract_array with integer",
			query: `SELECT JSON_EXTRACT_ARRAY('[1,2,3]')`,
			expectedRows: [][]interface{}{
				{[]interface{}{"1", "2", "3"}},
			},
		},
		{
			name:  "json_extract_array with integer cast",
			query: `SELECT ARRAY(SELECT CAST(integer_element AS INT64) FROM UNNEST(JSON_EXTRACT_ARRAY('[1,2,3]','$')) AS integer_element)`,
			expectedRows: [][]interface{}{
				{[]interface{}{int64(1), int64(2), int64(3)}},
			},
		},
		{
			name:  "json_extract_array format",
			query: `SELECT JSON_EXTRACT_ARRAY('["apples","oranges","grapes"]', '$')`,
			expectedRows: [][]interface{}{
				{[]interface{}{`"apples"`, `"oranges"`, `"grapes"`}},
			},
		},
		{
			name:  "json_extract_array filter",
			query: `SELECT JSON_EXTRACT_ARRAY('{"fruit":[{"apples":5,"oranges":10},{"apples":2,"oranges":4}],"vegetables":[{"lettuce":7,"kale": 8}]}', '$.fruit')`,
			expectedRows: [][]interface{}{
				{[]interface{}{`{"apples":5,"oranges":10}`, `{"apples":2,"oranges":4}`}},
			},
		},
		{
			name:         "json_extract_array with escape",
			query:        `SELECT JSON_EXTRACT_ARRAY('{"a.b": {"c": ["world"]}}', "$['a.b'].c")`,
			expectedRows: [][]interface{}{{[]interface{}{`"world"`}}},
		},
		{
			name:         "json_extract_array with null",
			query:        `SELECT JSON_EXTRACT_ARRAY('{"a":"foo"}','$.a'), JSON_EXTRACT_ARRAY('{"a":"foo"}','$.b'), JSON_EXTRACT_ARRAY(JSON 'null', '$')`,
			expectedRows: [][]interface{}{{nil, nil, nil}},
		},
		{
			name:         "json_extract_array with empty array",
			query:        `SELECT JSON_EXTRACT_ARRAY('{"a":"foo","b":[]}','$.b')`,
			expectedRows: [][]interface{}{{[]interface{}{}}},
		},
		{
			name:  "json_query_array",
			query: `SELECT JSON_QUERY_ARRAY(JSON '{"fruits":["apples","oranges","grapes"]}','$.fruits')`,
			expectedRows: [][]interface{}{
				{[]interface{}{`"apples"`, `"oranges"`, `"grapes"`}},
			},
		},
		{
			name:  "json_query_array with integer",
			query: `SELECT JSON_QUERY_ARRAY('[1,2,3]')`,
			expectedRows: [][]interface{}{
				{[]interface{}{"1", "2", "3"}},
			},
		},
		{
			name:  "json_query_array with integer cast",
			query: `SELECT ARRAY(SELECT CAST(integer_element AS INT64) FROM UNNEST(JSON_QUERY_ARRAY('[1,2,3]','$')) AS integer_element)`,
			expectedRows: [][]interface{}{
				{[]interface{}{int64(1), int64(2), int64(3)}},
			},
		},
		{
			name:  "json_query_array format",
			query: `SELECT JSON_QUERY_ARRAY('["apples","oranges","grapes"]', '$')`,
			expectedRows: [][]interface{}{
				{[]interface{}{`"apples"`, `"oranges"`, `"grapes"`}},
			},
		},
		{
			name:  "json_query_array filter",
			query: `SELECT JSON_QUERY_ARRAY('{"fruit":[{"apples":5,"oranges":10},{"apples":2,"oranges":4}],"vegetables":[{"lettuce":7,"kale": 8}]}', '$.fruit')`,
			expectedRows: [][]interface{}{
				{[]interface{}{`{"apples":5,"oranges":10}`, `{"apples":2,"oranges":4}`}},
			},
		},
		{
			name:         "json_query_array with escape",
			query:        `SELECT JSON_QUERY_ARRAY('{"a.b": {"c": ["world"]}}', '$."a.b".c')`,
			expectedRows: [][]interface{}{{[]interface{}{`"world"`}}},
		},
		{
			name:         "json_query_array with null",
			query:        `SELECT JSON_QUERY_ARRAY('{"a":"foo"}','$.a'), JSON_QUERY_ARRAY('{"a":"foo"}','$.b'), JSON_QUERY_ARRAY(JSON 'null', '$')`,
			expectedRows: [][]interface{}{{nil, nil, nil}},
		},
		{
			name:         "json_query_array with empty array",
			query:        `SELECT JSON_QUERY_ARRAY('{"a":"foo","b":[]}','$.b')`,
			expectedRows: [][]interface{}{{[]interface{}{}}},
		},
		{
			name:  "json_extract_string_array",
			query: `SELECT JSON_EXTRACT_STRING_ARRAY(JSON '{"fruits":["apples","oranges","grapes"]}','$.fruits')`,
			expectedRows: [][]interface{}{
				{[]interface{}{`apples`, `oranges`, `grapes`}},
			},
		},
		{
			name:  "json_extract_string_array with root only",
			query: `SELECT JSON_EXTRACT_STRING_ARRAY('["foo","bar","baz"]','$')`,
			expectedRows: [][]interface{}{
				{[]interface{}{`foo`, `bar`, `baz`}},
			},
		},
		{
			name:  "json_extract_string_array with integer cast",
			query: `SELECT ARRAY(SELECT CAST(integer_element AS INT64) FROM UNNEST(JSON_EXTRACT_STRING_ARRAY('[1,2,3]','$')) AS integer_element)`,
			expectedRows: [][]interface{}{
				{[]interface{}{int64(1), int64(2), int64(3)}},
			},
		},
		{
			name:  "json_extract_string_array with escape",
			query: `SELECT JSON_EXTRACT_STRING_ARRAY('{"a.b": {"c": ["world"]}}', "$['a.b'].c")`,
			expectedRows: [][]interface{}{
				{[]interface{}{"world"}},
			},
		},
		{
			name: "json_extract_string_array with null",
			query: `
SELECT
  JSON_EXTRACT_STRING_ARRAY('}}','$'),
  JSON_EXTRACT_STRING_ARRAY(NULL,'$'),
  JSON_EXTRACT_STRING_ARRAY('{"a":["foo","bar","baz"]}','$.b'),
  JSON_EXTRACT_STRING_ARRAY('{"a":"foo"}','$'),
  JSON_EXTRACT_STRING_ARRAY('{"a":[{"b":"foo","c":1},{"b":"bar","c":2}],"d":"baz"}','$.a'),
  JSON_EXTRACT_STRING_ARRAY('{"a":[10, {"b": 20}]','$.a'),
  JSON_EXTRACT_STRING_ARRAY(JSON 'null', '$')`,
			expectedRows: [][]interface{}{{nil, nil, nil, nil, nil, nil, nil}},
		},
		{
			name:         "json_extract_string_array with empty array",
			query:        `SELECT JSON_EXTRACT_STRING_ARRAY('{"a":"foo","b":[]}','$.b')`,
			expectedRows: [][]interface{}{{[]interface{}{}}},
		},
		{
			name:  "json_value_array",
			query: `SELECT JSON_VALUE_ARRAY(JSON '{"fruits":["apples","oranges","grapes"]}','$.fruits')`,
			expectedRows: [][]interface{}{
				{[]interface{}{`apples`, `oranges`, `grapes`}},
			},
		},
		{
			name:  "json_value_array with root only",
			query: `SELECT JSON_VALUE_ARRAY('["foo","bar","baz"]','$')`,
			expectedRows: [][]interface{}{
				{[]interface{}{`foo`, `bar`, `baz`}},
			},
		},
		{
			name:  "json_value_array with integer cast",
			query: `SELECT ARRAY(SELECT CAST(integer_element AS INT64) FROM UNNEST(JSON_VALUE_ARRAY('[1,2,3]','$')) AS integer_element)`,
			expectedRows: [][]interface{}{
				{[]interface{}{int64(1), int64(2), int64(3)}},
			},
		},
		{
			name:  "json_value_array with escape",
			query: `SELECT JSON_VALUE_ARRAY('{"a.b": {"c": ["world"]}}', '$."a.b".c')`,
			expectedRows: [][]interface{}{
				{[]interface{}{"world"}},
			},
		},
		{
			name: "json_value_array with null",
			query: `
SELECT
  JSON_VALUE_ARRAY('}}','$'),
  JSON_VALUE_ARRAY(NULL,'$'),
  JSON_VALUE_ARRAY('{"a":["foo","bar","baz"]}','$.b'),
  JSON_VALUE_ARRAY('{"a":"foo"}','$'),
  JSON_VALUE_ARRAY('{"a":[{"b":"foo","c":1},{"b":"bar","c":2}],"d":"baz"}','$.a'),
  JSON_VALUE_ARRAY('{"a":[10, {"b": 20}]','$.a'),
  JSON_VALUE_ARRAY(JSON 'null', '$')`,
			expectedRows: [][]interface{}{{nil, nil, nil, nil, nil, nil, nil}},
		},
		{
			name:         "json_value_array with empty array",
			query:        `SELECT JSON_VALUE_ARRAY('{"a":"foo","b":[]}','$.b')`,
			expectedRows: [][]interface{}{{[]interface{}{}}},
		},
		{
			name:         "parse_json",
			query:        `SELECT PARSE_JSON('{"coordinates":[10,20],"id":1}')`,
			expectedRows: [][]interface{}{{`{"coordinates":[10,20],"id":1}`}},
		},

		{
			name: "to_json",
			query: `
With CoordinatesTable AS (
    (SELECT 1 AS id, [10,20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30,40] AS coordinates) UNION ALL
    (SELECT 3 AS id, [50,60] AS coordinates))
SELECT TO_JSON(t) AS json_objects FROM CoordinatesTable AS t`,
			expectedRows: [][]interface{}{
				{`{"id":1,"coordinates":[10,20]}`},
				{`{"id":2,"coordinates":[30,40]}`},
				{`{"id":3,"coordinates":[50,60]}`},
			},
		},
		{
			name:         "to_json with struct",
			query:        `SELECT TO_JSON(STRUCT("foo" AS a, TO_JSON(STRUCT("bar" AS c)) AS b))`,
			expectedRows: [][]interface{}{{`{"a":"foo","b":{"c":"bar"}}`}},
		},
		{
			name: "to_json_string",
			query: `
With CoordinatesTable AS (
    (SELECT 1 AS id, [10,20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30,40] AS coordinates) UNION ALL
    (SELECT 3 AS id, [50,60] AS coordinates))
SELECT id, coordinates, TO_JSON_STRING(t) AS json_data
FROM CoordinatesTable AS t`,
			expectedRows: [][]interface{}{
				{int64(1), []interface{}{int64(10), int64(20)}, `{"id":1,"coordinates":[10,20]}`},
				{int64(2), []interface{}{int64(30), int64(40)}, `{"id":2,"coordinates":[30,40]}`},
				{int64(3), []interface{}{int64(50), int64(60)}, `{"id":3,"coordinates":[50,60]}`},
			},
		},
		{
			name:         "json_string",
			query:        `SELECT STRING(JSON '"purple"') AS color`,
			expectedRows: [][]interface{}{{"purple"}},
		},
		{
			name:         "json_bool",
			query:        `SELECT BOOL(JSON 'true') AS vacancy`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name:         "json_int64",
			query:        `SELECT INT64(JSON '2005') AS flight_number`,
			expectedRows: [][]interface{}{{int64(2005)}},
		},
		{
			name:         "json_float64",
			query:        `SELECT FLOAT64(JSON '9.8') AS velocity`,
			expectedRows: [][]interface{}{{float64(9.8)}},
		},
		{
			name: "json_type",
			query: `
SELECT json_val, JSON_TYPE(json_val) AS type
FROM
  UNNEST(
    [
      JSON '"apple"',
      JSON '10',
      JSON '3.14',
      JSON 'null',
      JSON '{"city": "New York", "State": "NY"}',
      JSON '["apple", "banana"]',
      JSON 'false'
    ]
  ) AS json_val`,
			expectedRows: [][]interface{}{
				{`"apple"`, "string"},
				{"10", "number"},
				{"3.14", "number"},
				{"null", "null"},
				{`{"State":"NY","city":"New York"}`, "object"},
				{`["apple","banana"]`, "array"},
				{"false", "boolean"},
			},
		},

		// subquery expr
		{
			name:         "subquery expr with scalar type at SELECT",
			query:        "SELECT (SELECT 1)",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "subquery expr with scalar type at WHERE",
			query:        "SELECT * FROM UNNEST([1, 2, 3]) AS val WHERE val = (SELECT 1)",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "subquery expr with scalar type at HAVING",
			query:        "SELECT * FROM UNNEST([1, 2, 3]) AS val GROUP BY val HAVING val = (SELECT 1)",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "subquery expr with scalar type at function call",
			query:        "SELECT ABS((SELECT 1))",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "subquery expr with array type",
			query:        "SELECT ARRAY(SELECT * FROM UNNEST([1, 2, 3]))",
			expectedRows: [][]interface{}{{[]interface{}{int64(1), int64(2), int64(3)}}},
		},
		{
			name:         "subquery expr with in type",
			query:        "SELECT * FROM UNNEST([1, 2, 3]) AS val WHERE val IN (SELECT 1)",
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "subquery expr with exists type",
			query:        `SELECT EXISTS ( SELECT val FROM UNNEST([1, 2, 3]) AS val WHERE val = 1 )`,
			expectedRows: [][]interface{}{{true}},
		},
		{
			name: "subquery with",
			query: `
WITH tmp as (
  SELECT * FROM (
    WITH A AS (
      SELECT * FROM (SELECT 1 AS id)
    )  SELECT * FROM A
  )
) SELECT id FROM tmp`,
			expectedRows: [][]interface{}{{int64(1)}},
		},
		{
			name:         "nested with",
			query:        `WITH output AS ( WITH sub AS ( SELECT val FROM UNNEST([1, 2, 3]) as val ) SELECT * FROM sub WHERE val > 1 ) SELECT * FROM output`,
			expectedRows: [][]interface{}{{int64(2)}, {int64(3)}},
		},
		{
			name: "join nested with",
			query: `select * from (SELECT 1 AS id) as A LEFT JOIN (with tmp as (select 1 as id)  select * from tmp) as b on a.id = b.id
`,
			expectedRows: [][]interface{}{{int64(1), int64(1)}},
		},
		{
			name: "nested with 2",
			query: `
WITH tmp as (
  WITH A AS (
    SELECT * FROM (SELECT 1 AS id)
  ), B AS (
    SELECT * FROM (SELECT "hello" AS name)
  ) SELECT * FROM A CROSS JOIN B
) SELECT * FROM tmp
`,
			expectedRows: [][]interface{}{{int64(1), "hello"}},
		},

		// net
		{
			name: "net_host",
			query: `
SELECT
  NET.HOST(url),
  NET.PUBLIC_SUFFIX(url),
  NET.REG_DOMAIN(url)
FROM (
  SELECT "" AS url UNION ALL
  SELECT "http://abc.xyz" UNION ALL
  SELECT "http://abc..xyz" UNION ALL
  SELECT "//user:password@a.b:80/path?query" UNION ALL
  SELECT "https://[::1]:80" UNION ALL
  SELECT "http://例子.卷筒纸.中国" UNION ALL
  SELECT "    www.Example.Co.UK    " UNION ALL
  SELECT "amazon.co.uk"
)`,
			expectedRows: [][]interface{}{
				{nil, nil, nil},
				{"abc.xyz", "xyz", "abc.xyz"},
				{"abc..xyz", nil, nil},
				{"a.b", nil, nil},
				{"[::1]", nil, nil},
				{"例子.卷筒纸.中国", "中国", "卷筒纸.中国"},
				{"www.Example.Co.UK", "Co.UK", "Example.Co.UK"},
				{"amazon.co.uk", "co.uk", "amazon.co.uk"},
			},
		},
		{
			name: "net_ip_from_string",
			query: `
SELECT
  FORMAT("%T", NET.IP_FROM_STRING(ip))
FROM (
  SELECT "48.49.50.51" AS ip UNION ALL
  SELECT "::1" UNION ALL
  SELECT "3031:3233:3435:3637:3839:4041:4243:4445" UNION ALL
  SELECT "::ffff:192.0.2.128" UNION ALL
  SELECT NULL
)`,
			expectedRows: [][]interface{}{
				{`b"0123"`},
				{`b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01"`},
				{`b"0123456789@ABCDE"`},
				{`b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80"`},
				{nil},
			},
		},
		{
			name: "net_ip_net_mask",
			query: `
SELECT FORMAT("%T", NET.IP_NET_MASK(4, 0)) UNION ALL
SELECT FORMAT("%T", NET.IP_NET_MASK(4, 20)) UNION ALL
SELECT FORMAT("%T", NET.IP_NET_MASK(4, 32)) UNION ALL
SELECT FORMAT("%T", NET.IP_NET_MASK(16, 0)) UNION ALL
SELECT FORMAT("%T", NET.IP_NET_MASK(16, 1)) UNION ALL
SELECT FORMAT("%T", NET.IP_NET_MASK(16, 128))`,
			expectedRows: [][]interface{}{
				{`b"\x00\x00\x00\x00"`},
				{`b"\xff\xff\xf0\x00"`},
				{`b"\xff\xff\xff\xff"`},
				{`b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"`},
				{`b"\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"`},
				{`b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"`},
			},
		},
		{
			name:        "net_ip_net_mask with invalid output bytes",
			query:       `SELECT NET.IP_NET_MASK(1, 0)`,
			expectedErr: "NET.IP_NET_MASK: the first argument must be either 4 or 16",
		},
		{
			name:        "net_ip_net_mask with invalid prefix length",
			query:       `SELECT NET.IP_NET_MASK(4, 33)`,
			expectedErr: "NET.IP_NET_MASK: the second argument must be in the range from 0 to 32",
		},
		{
			name: "net_ip_to_string",
			query: `
SELECT
  NET.IP_TO_STRING(bin)
FROM (
  SELECT b"0123" AS bin UNION ALL
  SELECT b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" UNION ALL
  SELECT b"0123456789@ABCDE" UNION ALL
  SELECT b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80"
)`,
			expectedRows: [][]interface{}{
				{"48.49.50.51"},
				{"::1"},
				{"3031:3233:3435:3637:3839:4041:4243:4445"},
				{"::ffff:192.0.2.128"},
			},
		},
		{
			name: "net_ip_trunc",
			query: `
SELECT
  FORMAT("%T", NET.IP_TRUNC(x, prefix_length))
FROM (
  SELECT b"\xAA\xBB\xCC\xDD" as x, 0 as prefix_length UNION ALL
  SELECT b"\xAA\xBB\xCC\xDD", 11 UNION ALL
  SELECT b"\xAA\xBB\xCC\xDD", 12 UNION ALL
  SELECT b"\xAA\xBB\xCC\xDD", 24 UNION ALL
  SELECT b"\xAA\xBB\xCC\xDD", 32 UNION ALL
  SELECT b'0123456789@ABCDE', 80
)`,
			expectedRows: [][]interface{}{
				{`b"\x00\x00\x00\x00"`},
				{`b"\xaa\xa0\x00\x00"`},
				{`b"\xaa\xb0\x00\x00"`},
				{`b"\xaa\xbb\xcc\x00"`},
				{`b"\xaa\xbb\xcc\xdd"`},
				{`b"0123456789\x00\x00\x00\x00\x00\x00"`},
			},
		},
		{
			name:        "net_ip_trunc with invalid ip address",
			query:       `SELECT NET.IP_TRUNC(b"\xAA\xbb\xCC", 0)`,
			expectedErr: "NET.IP_TRUNC: length of the first argument must be either 4 or 16",
		},
		{
			name:        "net_ip_trunc with invalid length",
			query:       `SELECT NET.IP_TRUNC(b"\xAA\xbb\xCC\xDD", 33)`,
			expectedErr: "NET.IP_TRUNC: length must be in the range from 0 to 32",
		},
		{
			name: "net_ipv4_from_int64",
			query: `
SELECT
  FORMAT("%T", NET.IPV4_FROM_INT64(v))
FROM (
  SELECT 0 AS v UNION ALL
  SELECT 11259375 UNION ALL
  SELECT 4294967295 UNION ALL
  SELECT -1 UNION ALL
  SELECT -2
)`,
			expectedRows: [][]interface{}{
				{`b"\x00\x00\x00\x00"`},
				{`b"\x00\xab\xcd\xef"`},
				{`b"\xff\xff\xff\xff"`},
				{`b"\xff\xff\xff\xff"`},
				{`b"\xff\xff\xff\xfe"`},
			},
		},
		{
			name: "net_ipv4_to_int64",
			query: `
SELECT
  FORMAT("0x%X", NET.IPV4_TO_INT64(v))
FROM (
 SELECT b"\x00\x00\x00\x00" AS v UNION ALL
 SELECT b"\x00\xab\xcd\xef" UNION ALL
 SELECT b"\xff\xff\xff\xff"
)`,
			expectedRows: [][]interface{}{
				{"0x0"},
				{"0xABCDEF"},
				{"0xFFFFFFFF"},
			},
		},
		{
			name: "net_safe_if_from_string",
			query: `
SELECT
  FORMAT("%T", NET.SAFE_IP_FROM_STRING(v))
FROM (
  SELECT "48.49.50.51" AS v UNION ALL
  SELECT "::1" UNION ALL
  SELECT "3031:3233:3435:3637:3839:4041:4243:4445" UNION ALL
  SELECT "::ffff:192.0.2.128" UNION ALL
  SELECT "48.49.50.51/32" UNION ALL
  SELECT "48.49.50" UNION ALL
  SELECT "::wxyz"
)`,
			expectedRows: [][]interface{}{
				{`b"0123"`},
				{`b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01"`},
				{`b"0123456789@ABCDE"`},
				{`b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80"`},
				{nil},
				{nil},
				{nil},
			},
		},

		{
			name: "single statement with named params",
			query: `
SELECT @a + @b;
`,
			args: []interface{}{
				sql.NamedArg{Name: "a", Value: 1},
				sql.NamedArg{Name: "b", Value: 2},
			},
			expectedRows: [][]interface{}{{int64(3)}},
		},
		{
			name: "not enough named params given",
			query: `
SELECT @a + @b;
`,
			args:        []interface{}{sql.NamedArg{Name: "a", Value: 1}},
			expectedErr: "not enough query arguments",
		},
		{
			name: "multiple statements with named params",
			query: `
CREATE TEMP TABLE t1 AS SELECT @a c1;
SELECT c1 * @b * @c FROM t1;
`,
			args: []interface{}{
				sql.NamedArg{Name: "a", Value: 1},
				sql.NamedArg{Name: "b", Value: 2},
				sql.NamedArg{Name: "c", Value: 3},
			},
			expectedRows: [][]interface{}{{int64(6)}},
		},

		{
			name: "single statement with positional params",
			query: `
SELECT ? + ?;
`,
			args:         []interface{}{int64(1), int64(2)},
			expectedRows: [][]interface{}{{int64(3)}},
		},
		{
			name: "not enough positional params given",
			query: `
SELECT ? + ?;
`,
			args:        []interface{}{int64(1)},
			expectedErr: "not enough query arguments",
		},
		{
			name: "multiple statements with positional params",
			query: `
CREATE TEMP TABLE t1 AS SELECT ? c1;
SELECT c1 * ? * ? FROM t1;
`,
			args:         []interface{}{int64(1), int64(2), int64(3)},
			expectedRows: [][]interface{}{{int64(6)}},
		},
		{
			name: "create table as select with column list",
			query: `
CREATE TABLE table1 (field_a STRING NOT NULL);
INSERT INTO table1 (field_a) VALUES ("test");
CREATE TEMP TABLE table2 (field_x STRING NOT NULL)
AS (SELECT field_a FROM table1);
SELECT * FROM table2;
`,
			expectedRows: [][]interface{}{{"test"}},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			rows, err := db.QueryContext(ctx, test.query, test.args...)
			if err != nil {
				if test.expectedErr == "" {
					t.Fatal(err)
				} else {
					return
				}
			}
			defer rows.Close()
			columns, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			columnNum := len(columns)
			args := []interface{}{}
			for i := 0; i < columnNum; i++ {
				var v interface{}
				args = append(args, &v)
			}
			rowNum := 0
			for rows.Next() {
				if err := rows.Scan(args...); err != nil {
					t.Fatal(err)
				}
				derefArgs := []interface{}{}
				for i := 0; i < len(args); i++ {
					value := reflect.ValueOf(args[i]).Elem().Interface()
					derefArgs = append(derefArgs, value)
				}
				if len(test.expectedRows) <= rowNum {
					t.Fatalf("unexpected row %v. expected row num %d but got next row", derefArgs, len(test.expectedRows))
				}
				expectedRow := test.expectedRows[rowNum]
				if len(derefArgs) != len(expectedRow) {
					t.Fatalf("failed to get columns. expected %d but got %d", len(expectedRow), len(derefArgs))
				}
				if diff := cmp.Diff(expectedRow, derefArgs, floatCmpOpt); diff != "" {
					t.Errorf("[%d]: (-want +got):\n%s", rowNum, diff)
				}
				rowNum++
			}
			rowsErr := rows.Err()
			if test.expectedErr != "" {
				if test.expectedErr != rowsErr.Error() {
					t.Fatalf("unexpected error message: expected [%s] but got [%s]", test.expectedErr, rowsErr.Error())
				}
			} else {
				if rowsErr != nil {
					t.Fatal(rowsErr)
				}
			}
			if len(test.expectedRows) != rowNum {
				t.Fatalf("failed to get rows. expected %d but got %d", len(test.expectedRows), rowNum)
			}
		})
	}
	os.Unsetenv("TZ")
}

func createTimestampFormatFromTime(t time.Time) string {
	unixmicro := t.UnixMicro()
	sec := unixmicro / int64(time.Millisecond)
	nsec := unixmicro - sec*int64(time.Millisecond)
	return fmt.Sprintf("%d.%d", sec, nsec)
}

func createTimestampFormatFromString(v string) string {
	t, _ := time.Parse("2006-01-02 15:04:05.999999+00", v)
	return createTimestampFormatFromTime(t)
}
