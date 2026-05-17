package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

// csvNullTokens are the textual values BigQuery's CSV autodetect treats as
// NULL; such values do not constrain a column's inferred type.
var csvNullTokens = map[string]struct{}{"": {}, "null": {}}

func isCSVNull(s string) bool {
	_, ok := csvNullTokens[strings.ToLower(s)]
	return ok
}

func csvLooksBool(s string) bool {
	switch strings.ToLower(s) {
	case "true", "false":
		return true
	}
	return false
}

func csvLooksInt(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func csvLooksFloat(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func csvLooksDate(s string) bool {
	_, err := time.Parse("2006-01-02", s)
	return err == nil
}

func csvLooksTimestamp(s string) bool {
	for _, layout := range []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999",
		time.RFC3339,
		time.RFC3339Nano,
	} {
		if _, err := time.Parse(layout, s); err == nil {
			return true
		}
	}
	return false
}

// inferCSVColumnType picks the narrowest BigQuery type that fits every
// non-null value in a column, in BigQuery's preference order
// (BOOL > INT64 > FLOAT64 > DATE > TIMESTAMP), and falls back to STRING. A
// column with no non-null values is typed STRING.
func inferCSVColumnType(values []string) string {
	allBool, allInt, allFloat, allDate, allTimestamp := true, true, true, true, true
	seen := false
	for _, v := range values {
		if isCSVNull(v) {
			continue
		}
		seen = true
		allBool = allBool && csvLooksBool(v)
		allInt = allInt && csvLooksInt(v)
		allFloat = allFloat && csvLooksFloat(v)
		allDate = allDate && csvLooksDate(v)
		allTimestamp = allTimestamp && csvLooksTimestamp(v)
	}
	switch {
	case !seen:
		return "STRING"
	case allBool:
		return "BOOLEAN"
	case allInt:
		return "INTEGER"
	case allFloat:
		return "FLOAT"
	case allDate:
		return "DATE"
	case allTimestamp:
		return "TIMESTAMP"
	default:
		return "STRING"
	}
}

// inferCSVSchema infers a table schema from CSV records, taking the first row
// as the header (column names) and the remaining rows as sample data. It backs
// the load job's autodetect option.
func inferCSVSchema(records [][]string) (*bigqueryv2.TableSchema, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("cannot autodetect schema: the CSV has no rows")
	}
	header := records[0]
	dataRows := records[1:]
	fields := make([]*bigqueryv2.TableFieldSchema, len(header))
	for i, name := range header {
		column := make([]string, 0, len(dataRows))
		for _, row := range dataRows {
			if i < len(row) {
				column = append(column, row[i])
			}
		}
		fields[i] = &bigqueryv2.TableFieldSchema{
			Name: name,
			Type: inferCSVColumnType(column),
			Mode: "NULLABLE",
		}
	}
	return &bigqueryv2.TableSchema{Fields: fields}, nil
}
