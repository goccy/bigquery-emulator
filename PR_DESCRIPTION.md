# fix: use load.FieldDelimiter for CSV parsing in uploadContentHandler

## Related Issue

Fixes https://github.com/goccy/bigquery-emulator/issues/329

## Problem

When using the BigQuery client library to load CSV data with a custom field delimiter (e.g. semicolon, tab, pipe), the `field_delimiter` parameter is correctly parsed from the job configuration and stored in `load.FieldDelimiter`, but the emulator **never applies it** to the actual CSV parser.

In `server/handler.go`, the `uploadContentHandler.Handle` method creates a `csv.NewReader` on line 439 without setting the `Comma` field, so Go's `encoding/csv` package always defaults to comma (`,`). This makes it impossible to load any CSV file that uses a delimiter other than comma.

### Example (Python BigQuery client)

```python
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    schema=table.schema,
    field_delimiter=";",       # <-- this is ignored by the emulator
    max_bad_records=0,
    skip_leading_rows=0,
    allow_jagged_rows=True,
)
load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
```

Any CSV with `;` delimiters would fail or produce incorrect data because the emulator splits on `,` instead.

## Solution

In the `"CSV"` branch of `uploadContentHandler.Handle` (`server/handler.go`), the fix:

1. Creates the `csv.Reader` as a named variable instead of an inline one-liner.
2. Checks if `load.FieldDelimiter` is non-empty.
3. If set, assigns the first character of `FieldDelimiter` to `csvReader.Comma` before calling `ReadAll()`.

This matches the BigQuery API behavior where `fieldDelimiter` is documented as a single-character string that defaults to comma.

### Before

```go
records, err := csv.NewReader(r.reader).ReadAll()
```

### After

```go
csvReader := csv.NewReader(r.reader)
if load.FieldDelimiter != "" {
    csvReader.Comma = rune(load.FieldDelimiter[0])
}
records, err := csvReader.ReadAll()
```

## Files Changed

| File | Change |
|------|--------|
| `server/handler.go` | Apply `load.FieldDelimiter` to the CSV reader before parsing |
| `server/server_test.go` | Add `TestLoadCSVWithCustomFieldDelimiter` test |

## Test

Added `TestLoadCSVWithCustomFieldDelimiter` which:

1. Starts the emulator with a project and empty dataset.
2. Creates a CSV payload using semicolon (`;`) as the field delimiter:
   ```
   ID;Name
   1;Alice
   2;Bob
   3;Charlie
   ```
3. Loads it via `table.LoaderFrom(source)` with `source.FieldDelimiter = ";"`.
4. Queries the table and asserts all 3 rows were correctly parsed with the right column values.

This test would **fail without the fix** because the CSV reader would treat `ID;Name` as a single column value instead of splitting on `;`.

## Backward Compatibility

- When `FieldDelimiter` is empty (the default), the behavior is unchanged — Go's `csv.Reader` defaults to comma.
- No API or configuration changes are required.
