# go-zetasqlite

![Go](https://github.com/goccy/go-zetasqlite/workflows/Go/badge.svg)
[![GoDoc](https://godoc.org/github.com/goccy/go-zetasqlite?status.svg)](https://pkg.go.dev/github.com/goccy/go-zetasqlite?tab=doc)
[![codecov](https://codecov.io/gh/goccy/go-zetasqlite/branch/main/graph/badge.svg)](https://codecov.io/gh/goccy/go-zetasqlite)

A database driver library that interprets ZetaSQL queries and runs them using SQLite3

# Features

`go-zetasqlite` supports `database/sql` driver interface.
So, you can use ZetaSQL queries just by importing `github.com/goccy/go-zetasqlite`.
Also, go-zetasqlite uses SQLite3 as the database engine.
Since we are using [go-sqlite3](https://github.com/mattn/go-sqlite3), we can use the options ( like `:memory:` ) supported by `go-sqlite3` ( see [details](https://pkg.go.dev/github.com/mattn/go-sqlite3#readme-connection-string) ).
ZetaSQL functionality is provided by [go-zetasql](https://github.com/goccy/go-zetasql)

# Installation

```
go get github.com/goccy/go-zetasqlite
```

## **NOTE**

Since this library uses go-zetasql, the following environment variables must be enabled in order to build. See [here](https://github.com/goccy/go-zetasql#prerequisites) for details.

```
CGO_ENABLED=1
CXX=clang++
```

# Synopsis

You can pass ZetaSQL queries to Query/Exec function of database/sql package.

```go
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
```

# Tools

## ZetaSQLite CLI

You can execute ZetaSQL queries interactively by using the tools provided by `cmd/zetasqlite-cli`. See [here](https://github.com/goccy/go-zetasqlite/tree/main/cmd/zetasqlite-cli#readme) for details

# Status

A list of ZetaSQL ( Google Standard SQL ) specifications and features supported by go-zetasqlite.

## Types

- [x] INT64 ( `INT`, `SMALLINT`, `INTEGER`, `BIGINT`, `TINYINT`, `BYTEINT` )
- [x] NUMERIC ( `DECIMAL` )
- [x] BIGNUMERIC ( `BIGDECIMAL` )
- [x] FLOAT64 ( `FLOAT` )
- [x] BOOL ( `BOOLEAN` )
- [x] STRING
- [x] BYTES
- [x] DATE
- [x] TIME
- [x] DATETIME
- [x] TIMESTAMP
- [x] INTERVAL
- [x] ARRAY
- [x] STRUCT
- [x] JSON
- [x] RECORD
- [ ] GEOGRAPHY

## Expressions

### Operators

- [x] Field access operator
- [x] Array subscript operator
- [x] JSON subscript operator
- [x] Unary operators ( `+`, `-`, `~` )
- [x] Multiplication ( `*` )
- [x] Division ( `/` )
- [x] Concatenation operator ( `||` )
- [x] Addition ( `+` )
- [x] Subtraction ( `-` )
- [x] Bitwise operators ( `<<`, `>>`, `&`, `|` )
- [x] Comparison operators ( `=`, `<`, `>`, `<=`, `>=`, `!=`, `<>`)
- [x] [NOT] LIKE
- [x] [NOT] BETWEEN
- [x] [NOT] IN
- [x] IS [NOT] NULL
- [x] IS [NOT] TRUE
- [x] IS [NOT] FALSE
- [x] NOT
- [x] AND
- [x] OR
- [x] [NOT] EXISTS
- [x] IS [NOT] DISTINCT FROM

### Conditional Expressions

- [x] CASE expr
- [x] CASE
- [x] COALESCE
- [x] IFNULL
- [x] NULLIF

### Subqueries

- [x] Expression subqueries
  - [x] Scalar subqueries
  - [x] ARRAY subqueries
  - [x] IN subqueries
  - [x] EXISTS subqueries
- [x] Table subqueries
- [x] Correlated subqueries
- [x] Volatile subqueries

## Query

- [x] SELECT statement
  - [x] SELECT *
  - [x] SELECT expression
  - [x] SELECT expression.*
  - [x] SELECT * EXCEPT
  - [x] SELECT * REPLACE
  - [x] SELECT DISTINCT
  - [x] SELECT ALL
  - [x] SELECT AS STRUCT
  - [x] SELECT AS VALUE
- [x] FROM clause
- [x] UNNEST operator
  - [x] UNNEST and STRUCTs
  - [ ] Explicit and implicit UNNEST
  - [ ] UNNEST and NULLs
  - [x] UNNEST and WITH OFFSET
- [x] PIVOT operator
- [x] UNPIVOT operator
- [ ] TABLESAMPLE operator
- [x] JOIN operation
  - [x] INNER JOIN
  - [x] CROSS JOIN
  - [x] Comma cross join (,)
  - [x] FULL OUTER JOIN
  - [x] LEFT OUTER JOIN
  - [x] RIGHT OUTER JOIN
  - [x] ON clause
  - [x] USING clause
  - [x] ON and USING equivalency
  - [ ] Join operations in a sequence
  - [ ] Correlated join operation
- [x] WHERE clause
- [x] GROUP BY clause
- [x] HAVING clause
  - [x] Mandatory aggregation
- [x] ORDER BY clause
- [x] QUALIFY clause
- [x] WINDOW clause
- [x] Set operators
  - [x] UNION
  - [x] INTERSECT
  - [x] EXCEPT
- [x] LIMIT and OFFSET clauses
- [x] WITH clause
  - [ ] RECURSIVE keyword
  - [x] Non-recursive CTEs
  - [ ] Recursive CTEs
  - [x] CTE rules and constraints
  - [x] CTE visibility
- [x] Using aliases
  - [x] Explicit aliases
  - [x] Implicit aliases
  - [x] Alias visibility
  - [x] Duplicate aliases
  - [x] Ambiguous aliases
  - [x] Range variables
- [x] Value tables
  - [x] Return query results as a value table
  - [x] Create a table with a value table
  - [ ] Use a set operation on a value table
- [x] Queries for wildcard table

## Statements

### DDL ( Data Definition Language )

- [ ] CREATE SCHEMA
- [x] CREATE TABLE
- [ ] CREATE TABLE LIKE
- [ ] CREATE TABLE COPY
- [ ] CREATE SNAPSHOT TABLE
- [ ] CREATE TABLE CLONE
- [x] CREATE VIEW
- [ ] CREATE MATERIALIZED VIEW
- [ ] CREATE EXTERNAL TABLE
- [x] CREATE FUNCTION
- [ ] CREATE TABLE FUNCTION
- [ ] CREATE PROCEDURE
- [ ] CREATE ROW ACCESS POLICY
- [ ] CREATE CAPACITY
- [ ] CREATE RESERVATION
- [ ] CREATE ASSIGNMENT
- [ ] CREATE SEARCH INDEX
- [ ] ALTER SCHEMA SET DEFAULT COLLATE
- [ ] ALTER SCHEMA SET OPTIONS
- [ ] ALTER TABLE SET OPTIONS
- [ ] ALTER TABLE ADD COLUMN
- [ ] ALTER TABLE RENAME TO
- [ ] ALTER TABLE RENAME COLUMN
- [ ] ALTER TABLE DROP COLUMN
- [ ] ALTER TABLE SET DEFAULT COLLATE
- [ ] ALTER COLUMN SET OPTIONS
- [ ] ALTER COLUMN DROP NOT NULL
- [ ] ALTER COLUMN SET DATA TYPE
- [ ] ALTER COLUMN SET DEFAULT
- [ ] ALTER COLUMN DROP DEFAULT
- [ ] ALTER VIEW SET OPTIONS
- [ ] ALTER MATERIALIZED VIEW SET OPTIONS
- [ ] ALTER ORGANIZATION SET OPTIONS
- [ ] ALTER PROJECT SET OPTIONS
- [ ] ALTER BI_CAPACITY SET OPTIONS
- [ ] DROP SCHEMA
- [x] DROP TABLE
- [ ] DROP SNAPSHOT TABLE
- [ ] DROP EXTERNAL TABLE
- [x] DROP VIEW
- [ ] DROP MATERIALIZED VIEW
- [x] DROP FUNCTION
- [ ] DROP TABLE FUNCTION
- [ ] DROP PROCEDURE
- [ ] DROP ROW ACCESS POLICY
- [ ] DROP CAPACITY
- [ ] DROP RESERVATION
- [ ] DROP ASSIGNMENT
- [ ] DROP SEARCH INDEX

### DML ( Data Manipulation Language )

- [x] INSERT
- [x] DELETE
- [x] TRUNCATE TABLE
- [x] UPDATE
- [x] MERGE

### DCL ( Data Control Language )

- [ ] GRANT
- [ ] REVOKE

### Procedural Language

- [ ] DECLARE
- [ ] SET
- [ ] EXECUTE IMMEDIATE
- [x] BEGIN...END
- [ ] BEGIN...EXCEPTION...END
- [x] CASE
- [x] CASE search_expression
- [x] IF
- [ ] Labels
- [ ] Loops
  - [ ] LOOP
  - [ ] REPEATE
  - [ ] WHILE
  - [ ] BREAK
  - [ ] LEAVE
  - [ ] CONTINUE
  - [ ] ITERATE
  - [ ] FOR...IN
- [ ] Transactions
  - [x] BEGIN TRANSACTION
  - [x] COMMIT TRANSACTION
  - [ ] ROLLBACK TRANSACTION
- [ ] RAISE
- [ ] RETURN
- [ ] CALL

### Debugging Statements

- [ ] ASSERT

### Other Statements

- [ ] EXPORT DATA
- [ ] LOAD DATA


## User Defined Functions

- [x] User Defined Function
- [x] Templated Argument Function
  - If the return type is not specified, templated argument function supports only some types of patterns.
    - `ANY` -> `ANY`
    - `ARRAY<ANY>` -> `ANY`
    - `ANY` -> `ARRAY<ANY>`
    - If the return type is always fixed, only some types are supported, such as `INT64` / `DOUBLE`

- [x] JavaScript UDF

## Functions

### Aggregate functions

- [x] ANY_VALUE
- [x] ARRAY_AGG
- [x] ARRAY_CONCAT_AGG
- [x] AVG
- [x] BIT_AND
- [x] BIT_OR
- [x] BIT_XOR
- [x] COUNT
- [x] COUNTIF
- [x] LOGICAL_AND
- [x] LOGICAL_OR
- [x] MAX
- [x] MIN
- [x] STRING_AGG
- [x] SUM

### Statistical aggregate functions

- [x] CORR
- [x] COVAR_POP
- [x] COVAR_SAMP
- [x] STDDEV_POP
- [x] STDDEV_SAMP
- [x] STDDEV
- [x] VAR_POP
- [x] VAR_SAMP
- [x] VARIANCE

### Approximate aggregate functions

- [x] APPROX_COUNT_DISTINCT
- [x] APPROX_QUANTILES
- [x] APPROX_TOP_COUNT
- [x] APPROX_TOP_SUM

### HyperLogLog++ functions

- [x] HLL_COUNT.INIT
- [x] HLL_COUNT.MERGE
- [x] HLL_COUNT.MERGE_PARTIAL
- [x] HLL_COUNT.EXTRACT

### Numbering functions

- [x] RANK
- [x] DENSE_RANK
- [x] PERCENT_RANK
- [x] CUME_DIST
- [x] NTILE
- [x] ROW_NUMBER

### Bit functions

- [x] BIT_COUNT

### Conversion functions

- [x] CAST AS ARRAY
- [x] CAST AS BIGNUMERIC
- [x] CAST AS BOOL
- [x] CAST AS BYTES
- [x] CAST AS DATE
- [x] CAST AS DATETIME
- [x] CAST AS FLOAT64
- [x] CAST AS INT64
- [x] CAST AS INTERVAL
- [x] CAST AS NUMERIC
- [x] CAST AS STRING
- [x] CAST AS STRUCT
- [x] CAST AS TIME
- [x] CAST AS TIMESTAMP
- [x] PARSE_BIGNUMERIC
- [x] PARSE_NUMERIC
- [x] SAFE_CAST
- [ ] Format clause for CAST

### Mathematical functions

- [x] ABS
- [x] SIGN
- [x] IS_INF
- [x] IS_NAN
- [x] IEEE_DIVIDE
- [x] RAND
- [x] SQRT
- [x] POW
- [x] POWER
- [x] EXP
- [x] LN
- [x] LOG
- [x] LOG10
- [x] GREATEST
- [x] LEAST
- [x] DIV
- [x] SAFE_DIVIDE
- [x] SAFE_MULTIPLY
- [x] SAFE_NEGATE
- [x] SAFE_ADD
- [x] SAFE_SUBTRACT
- [x] MOD
- [x] ROUND
- [x] TRUNC
- [x] CEIL
- [x] CEILING
- [x] FLOOR
- [x] COS
- [x] COSH
- [x] ACOS
- [x] ACOSH
- [x] SIN
- [x] SINH
- [x] ASIN
- [x] ASINH
- [x] TAN
- [x] TANH
- [x] ATAN
- [x] ATANH
- [x] ATAN2
- [x] RANGE_BUCKET

### Navigation functions

- [x] FIRST_VALUE
- [x] LAST_VALUE
- [x] NTH_VALUE
- [x] LEAD
- [x] LAG
- [x] PERCENTILE_CONT
- [x] PERCENTILE_DISC

### Hash functions

- [x] FARM_FINGERPRINT
- [x] MD5
- [x] SHA1
- [x] SHA256
- [x] SHA512

### String functions

- [x] ASCII
- [x] BYTE_LENGTH
- [x] CHAR_LENGTH
- [x] CHARACTER_LENGTH
- [x] CHR
- [x] CODE_POINTS_TO_BYTES
- [x] CODE_POINTS_TO_STRING
- [ ] COLLATE
- [x] CONCAT
- [ ] CONTAINS_SUBSTR
- [x] ENDS_WITH
- [x] FORMAT
- [x] FROM_BASE32
- [x] FROM_BASE64
- [x] FROM_HEX
- [x] INITCAP
- [x] INSTR
- [x] LEFT
- [x] LENGTH
- [x] LPAD
- [x] LOWER
- [x] LTRIM
- [x] NORMALIZE
- [x] NORMALIZE_AND_CASEFOLD
- [x] OCTET_LENGTH
- [x] REGEXP_CONTAINS
- [x] REGEXP_EXTRACT
- [x] REGEXP_EXTRACT_ALL
- [x] REGEXP_INSTR
- [x] REGEXP_REPLACE
- [x] REGEXP_SUBSTR
- [x] REPLACE
- [x] REPEAT
- [x] REVERSE
- [x] RIGHT
- [x] RPAD
- [x] RTRIM
- [x] SAFE_CONVERT_BYTES_TO_STRING
- [x] SOUNDEX
- [x] SPLIT
- [x] STARTS_WITH
- [x] STRPOS
- [x] SUBSTR
- [x] SUBSTRING
- [x] TO_BASE32
- [x] TO_BASE64
- [x] TO_CODE_POINTS
- [x] TO_HEX
- [x] TRANSALTE
- [x] TRIM
- [x] UNICODE
- [x] UPPER

### JSON functions

- [x] JSON_EXTRACT
- [x] JSON_QUERY
- [x] JSON_EXTRACT_SCALAR
- [x] JSON_VALUE
- [x] JSON_EXTRACT_ARRAY
- [x] JSON_QUERY_ARRAY
- [x] JSON_EXTRACT_STRING_ARRAY
- [x] JSON_VALUE_ARRAY
- [x] PARSE_JSON
- [x] TO_JSON
- [x] TO_JSON_STRING
- [x] STRING
- [x] BOOL
- [x] INT64
- [x] FLOAT64
- [x] JSON_TYPE

### Array functions

- [x] ARRAY
- [x] ARRAY_CONCAT
- [x] ARRAY_LENGTH
- [x] ARRAY_TO_STRING
- [x] GENERATE_ARRAY
- [x] GENERATE_DATE_ARRAY
- [x] GENERATE_TIMESTAMP_ARRAY
- [x] ARRAY_REVERSE

### Date functions

- [x] CURRENT_DATE
- [x] EXTRACT
- [x] DATE
- [x] DATE_ADD
- [x] DATE_SUB
- [x] DATE_DIFF
- [x] DATE_TRUNC
- [x] DATE_FROM_UNIX_DATE
- [x] FORMAT_DATE
- [x] LAST_DAY
- [x] PARSE_DATE
- [x] UNIX_DATE

### Datetime functions

- [x] CURRENT_DATETIME
- [x] DATETIME
- [x] EXTRACT
- [x] DATETIME_ADD
- [x] DATETIME_SUB
- [x] DATETIME_DIFF
- [x] DATETIME_TRUNC
- [x] FORMAT_DATETIME
- [x] LAST_DAY
- [x] PARSE_DATETIME

### Time functions

- [x] CURRENT_TIME
- [x] TIME
- [x] EXTRACT
- [x] TIME_ADD
- [x] TIME_SUB
- [x] TIME_DIFF
- [x] TIME_TRUNC
- [x] FORMAT_TIME
- [x] PARSE_TIME

### Timestamp functions

- [x] CURRENT_TIMESTAMP
- [x] EXTRACT
- [x] STRING
- [x] TIMESTAMP
- [x] TIMESTAMP_ADD
- [x] TIMESTAMP_SUB
- [x] TIMESTAMP_DIFF
- [x] TIMESTAMP_TRUNC
- [x] FORMAT_TIMESTAMP
- [x] PARSE_TIMESTAMP
- [x] TIMESTAMP_SECONDS
- [x] TIMESTAMP_MILLIS
- [x] TIMEATAMP_MICROS
- [x] UNIX_SECONDS
- [x] UNIX_MILLIS
- [x] UNIX_MICROS

### Interval functions

- [x] MAKE_INTERVAL
- [x] EXTRACT
- [x] JUSTIFY_DAYS
- [x] JUSTIFY_HOURS
- [x] JUSTIFY_INTERVAL

### Geography types

- [x] Point
- [ ] LineString
- [ ] Polygon
- [ ] MultiPoint
- [ ] MultiLineString
- [ ] MultiPolygon
- [ ] GeometryCollection

### Geography functions

- [ ] S2_CELLIDFROMPOINT
- [ ] S2_COVERINGCELLIDS
- [ ] ST_ANGLE
- [ ] ST_AREA
- [ ] ST_ASBINARY
- [ ] ST_ASGEOJSON
- [ ] ST_ASTEXT
- [ ] ST_AZIMUTH
- [ ] ST_BOUNDARY
- [ ] ST_BOUNDINGBOX
- [ ] ST_BUFFER
- [ ] ST_BUFFERWITHTOLERANCE
- [ ] ST_CENTROID
- [ ] ST_CENTROID_AGG
- [ ] ST_CLOSESTPOINT
- [ ] ST_CLUSTERDBSCAN
- [ ] ST_CONTAINS
- [ ] ST_CONVEXHULL
- [ ] ST_COVEREDBY
- [ ] ST_COVERS
- [ ] ST_DIFFERENCE
- [ ] ST_DIMENSION
- [ ] ST_DISJOINT
- [x] ST_DISTANCE
- [ ] ST_DUMP
- [ ] ST_DWITHIN
- [ ] ST_ENDPOINT
- [ ] ST_EQUALS
- [ ] ST_EXTENT
- [ ] ST_EXTERIORRING
- [ ] ST_GEOGFROM
- [ ] ST_GEOGFROMGEOJSON
- [x] ST_GEOGFROMTEXT
- [ ] ST_GEOGFROMWKB
- [x] ST_GEOGPOINT
- [ ] ST_GEOGPOINTFROMGEOHASH
- [ ] ST_GEOHASH
- [ ] ST_GEOMETRYTYPE
- [ ] ST_INTERIORRINGS
- [ ] ST_INTERSECTION
- [ ] ST_INTERSECTS
- [ ] ST_INTERSECTSBOX
- [ ] ST_ISCLOSED
- [ ] ST_ISCOLLECTION
- [ ] ST_ISEMPTY
- [ ] ST_ISRING
- [ ] ST_LENGTH
- [ ] ST_MAKELINE
- [ ] ST_MAKEPOLYGON
- [ ] ST_MAKEPOLYGONORIENTED
- [ ] ST_MAXDISTANCE
- [ ] ST_NPOINTS
- [ ] ST_NUMGEOMETRIES
- [ ] ST_NUMPOINTS
- [ ] ST_PERIMETER
- [ ] ST_POINTN
- [ ] ST_SIMPLIFY
- [ ] ST_SNAPTOGRID
- [ ] ST_STARTPOINT
- [ ] ST_TOUCHES
- [ ] ST_UNION
- [ ] ST_UNION_AGG
- [ ] ST_WITHIN
- [ ] ST_X
- [ ] ST_Y

### Security functions

- [x] SESSION_USER

### UUID functions

- [x] GENERATE_UUID

### Net functions

- [x] NET.IP_FROM_STRING
- [x] NET.SAFE_IP_FROM_STRING
- [x] NET.IP_TO_STRING
- [x] NET.IP_NET_MASK
- [x] NET.IP_TRUNC
- [x] NET.IPV4_FROM_INT64
- [x] NET.IPV4_TO_INT64
- [x] NET.HOST
- [x] NET.PUBLIC_SUFFIX
- [x] NET.REG_DOMAIN

### Debugging functions

- [x] ERROR

### AEAD encryption functions

- [ ] KEYS.NEW_KEYSET
- [ ] KEYS.ADD_KEY_FROM_RAW_BYTES
- [ ] AEAD.DECRYPT_BYTES
- [ ] AEAD.DECRYPT_STRING
- [ ] AEAD.ENCRYPT
- [ ] DETERMINISTIC_DECRYPT_BYTES
- [ ] DETERMINISTIC_DECRYPT_STRING
- [ ] DETERMINISTIC_ENCRYPT
- [ ] KEYS.KEYSET_CHAIN
- [ ] KEYS.KEYSET_FROM_JSON
- [ ] KEYS.KEYSET_TO_JSON
- [ ] KEYS.ROTATE_KEYSET
- [ ] KEYS.KEYSET_LENGTH

# License

MIT
