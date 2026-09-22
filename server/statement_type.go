package server

import (
	"strings"

	"github.com/goccy/googlesqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

// statementType returns the value BigQuery reports in
// statistics.query.statementType for query. The classification is
// lexical: comments and string/identifier literals are skipped, and only
// the leading keywords of the statement (plus a top-level AS for CREATE
// TABLE) are inspected. A query with more than one statement, or one
// that starts with a procedural keyword, is a SCRIPT.
func statementType(query string) string {
	stmts := splitStatements(query)
	if len(stmts) != 1 {
		if len(stmts) == 0 {
			return "SELECT"
		}
		return "SCRIPT"
	}
	toks := stmts[0]
	kw := func(i int) string {
		if i < len(toks) {
			return toks[i]
		}
		return ""
	}
	switch kw(0) {
	case "SELECT", "WITH", "(", "FROM", "VALUES":
		return "SELECT"
	case "INSERT", "UPDATE", "DELETE", "MERGE", "CALL":
		return kw(0)
	case "TRUNCATE":
		return "TRUNCATE_TABLE"
	case "EXPORT":
		if kw(1) == "MODEL" {
			return "EXPORT_MODEL"
		}
		return "EXPORT_DATA"
	case "LOAD":
		return "LOAD_DATA"
	case "CREATE":
		return createStatementType(toks[1:])
	case "DROP":
		return dropStatementType(toks[1:])
	case "ALTER":
		switch kw(1) {
		case "VIEW":
			return "ALTER_VIEW"
		case "MATERIALIZED":
			return "ALTER_MATERIALIZED_VIEW"
		case "SCHEMA":
			return "ALTER_SCHEMA"
		}
		return "ALTER_TABLE"
	}
	// DECLARE, SET, BEGIN, IF, LOOP, ... are procedural.
	return "SCRIPT"
}

func createStatementType(toks []string) string {
	i := 0
	if i+1 < len(toks) && toks[i] == "OR" && toks[i+1] == "REPLACE" {
		i += 2
	}
	if i < len(toks) && (toks[i] == "TEMP" || toks[i] == "TEMPORARY") {
		i++
	}
	at := func(j int) string {
		if i+j < len(toks) {
			return toks[i+j]
		}
		return ""
	}
	switch at(0) {
	case "TABLE":
		if at(1) == "FUNCTION" {
			return "CREATE_TABLE_FUNCTION"
		}
		// CREATE TABLE ... AS query. OPTIONS(...) and the column list
		// are parenthesised, so a top-level AS is the query separator.
		for _, t := range toks[i+1:] {
			if t == "AS" {
				return "CREATE_TABLE_AS_SELECT"
			}
		}
		return "CREATE_TABLE"
	case "FUNCTION", "AGGREGATE":
		return "CREATE_FUNCTION"
	case "VIEW":
		return "CREATE_VIEW"
	case "MATERIALIZED":
		return "CREATE_MATERIALIZED_VIEW"
	case "PROCEDURE":
		return "CREATE_PROCEDURE"
	case "SCHEMA":
		return "CREATE_SCHEMA"
	case "EXTERNAL":
		return "CREATE_EXTERNAL_TABLE"
	case "SNAPSHOT":
		return "CREATE_SNAPSHOT_TABLE"
	case "MODEL":
		return "CREATE_MODEL"
	case "SEARCH":
		return "CREATE_SEARCH_INDEX"
	case "ROW":
		return "CREATE_ROW_ACCESS_POLICY"
	}
	return "CREATE_TABLE"
}

func dropStatementType(toks []string) string {
	at := func(j int) string {
		if j < len(toks) {
			return toks[j]
		}
		return ""
	}
	switch at(0) {
	case "TABLE":
		if at(1) == "FUNCTION" {
			return "DROP_TABLE_FUNCTION"
		}
		return "DROP_TABLE"
	case "EXTERNAL":
		return "DROP_EXTERNAL_TABLE"
	case "VIEW":
		return "DROP_VIEW"
	case "MATERIALIZED":
		return "DROP_MATERIALIZED_VIEW"
	case "FUNCTION":
		return "DROP_FUNCTION"
	case "PROCEDURE":
		return "DROP_PROCEDURE"
	case "SCHEMA":
		return "DROP_SCHEMA"
	case "SNAPSHOT":
		return "DROP_SNAPSHOT_TABLE"
	case "MODEL":
		return "DROP_MODEL"
	case "SEARCH":
		return "DROP_SEARCH_INDEX"
	case "ROW", "ALL":
		return "DROP_ROW_ACCESS_POLICY"
	}
	return "DROP_TABLE"
}

// splitStatements tokenizes query into upper-cased keywords per
// top-level statement. Only bare words at parenthesis depth zero are
// kept (plus a leading "("), which is all statementType needs.
func splitStatements(query string) [][]string {
	var (
		stmts [][]string
		cur   []string
		depth int
	)
	flush := func() {
		if len(cur) > 0 {
			stmts = append(stmts, cur)
		}
		cur = nil
	}
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '-' && i+1 < len(query) && query[i+1] == '-', c == '#':
			for i < len(query) && query[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(query) && query[i+1] == '*':
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				i = len(query)
			} else {
				i += end + 4
			}
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(query, i)
		case c == '(':
			if depth == 0 && len(cur) == 0 {
				cur = append(cur, "(")
			}
			depth++
			i++
		case c == ')':
			if depth > 0 {
				depth--
			}
			i++
		case c == ';' && depth == 0:
			flush()
			i++
		case isWordByte(c):
			start := i
			for i < len(query) && isWordByte(query[i]) {
				i++
			}
			if depth == 0 {
				cur = append(cur, strings.ToUpper(query[start:i]))
			}
		default:
			i++
		}
	}
	flush()
	return stmts
}

// skipQuoted returns the index just past the literal starting at i,
// handling triple-quoted strings and backslash escapes.
func skipQuoted(query string, i int) int {
	q := query[i]
	if q != '`' && strings.HasPrefix(query[i:], strings.Repeat(string(q), 3)) {
		delim := strings.Repeat(string(q), 3)
		end := strings.Index(query[i+3:], delim)
		if end < 0 {
			return len(query)
		}
		return i + 3 + end + 3
	}
	for j := i + 1; j < len(query); j++ {
		switch query[j] {
		case '\\':
			j++
		case q:
			return j + 1
		}
	}
	return len(query)
}

func isWordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// ddlTargetTable returns the table a CREATE TABLE statement created or
// replaced, from the catalog changes the statement produced.
func ddlTargetTable(cat *googlesqlite.ChangedCatalog) *bigqueryv2.TableReference {
	if cat == nil || cat.Table == nil {
		return nil
	}
	ref := func(spec *googlesqlite.TableSpec) *bigqueryv2.TableReference {
		if spec == nil || len(spec.NamePath) != 3 {
			return nil
		}
		return &bigqueryv2.TableReference{
			ProjectId: spec.NamePath[0],
			DatasetId: spec.NamePath[1],
			TableId:   spec.NamePath[2],
		}
	}
	for _, spec := range append(cat.Table.Added, cat.Table.Updated...) {
		if r := ref(spec); r != nil {
			return r
		}
	}
	return nil
}

// applyStatementStatistics fills in the statement type and, for CREATE
// TABLE statements, the DDL target of a finished query job. BigQuery
// sets configuration.query.destinationTable to the created table for a
// CREATE TABLE AS SELECT; clients such as dbt-bigquery read the row
// count through it.
func applyStatementStatistics(job *bigqueryv2.Job, query string, cat *googlesqlite.ChangedCatalog) {
	if job == nil || job.Statistics == nil || job.Statistics.Query == nil {
		return
	}
	st := statementType(query)
	job.Statistics.Query.StatementType = st
	if st != "CREATE_TABLE" && st != "CREATE_TABLE_AS_SELECT" {
		return
	}
	target := ddlTargetTable(cat)
	if target == nil {
		return
	}
	job.Statistics.Query.DdlTargetTable = target
	if st == "CREATE_TABLE_AS_SELECT" && job.Configuration != nil && job.Configuration.Query != nil &&
		job.Configuration.Query.DestinationTable == nil {
		job.Configuration.Query.DestinationTable = target
	}
}
