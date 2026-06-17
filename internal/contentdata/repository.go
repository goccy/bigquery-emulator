package contentdata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/googlesqlite"
	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/logger"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
)

type Repository struct{}

func NewRepository() *Repository {
	return &Repository{}
}

// escapeIdent escapes a SQL identifier so it can be safely embedded between
// backticks. BigQuery (and googlesqlite) quote identifiers with backticks, so
// any backtick contained in the identifier itself is doubled. This prevents a
// crafted project/dataset/table/column name from terminating the quoted
// region and injecting arbitrary SQL.
func escapeIdent(ident string) string {
	return strings.ReplaceAll(ident, "`", "``")
}

// validateIdent rejects identifiers that cannot be embedded safely in a
// backtick-quoted SQL identifier.
//
// Escaping alone is not sufficient here: a query first passes through the
// GoogleSQL (ZetaSQL) parser and is then re-emitted for the SQLite backend,
// and the two layers disagree on how a backtick is escaped (GoogleSQL uses a
// backslash, SQLite doubles the backtick). A name containing a backtick or a
// backslash can therefore survive one layer's escaping and break out of the
// quoted region in the other, allowing arbitrary SQL to be injected (CWE-89).
//
// Neither character is valid in a BigQuery project, dataset, table, view,
// routine or column name, so rejecting them closes the injection vector
// without affecting any legitimate identifier.
func validateIdent(kind, ident string) error {
	if i := strings.IndexAny(ident, "`\\"); i >= 0 {
		return fmt.Errorf(
			"invalid %s %q: character %q is not allowed in an identifier",
			kind, ident, ident[i],
		)
	}
	return nil
}

func (r *Repository) tablePath(projectID, datasetID, tableID string) (string, error) {
	var tablePath []string
	if projectID != "" {
		if err := validateIdent("project", projectID); err != nil {
			return "", err
		}
		tablePath = append(tablePath, escapeIdent(projectID))
	}
	if datasetID != "" {
		if err := validateIdent("dataset", datasetID); err != nil {
			return "", err
		}
		tablePath = append(tablePath, escapeIdent(datasetID))
	}
	if err := validateIdent("table", tableID); err != nil {
		return "", err
	}
	tablePath = append(tablePath, escapeIdent(tableID))
	return strings.Join(tablePath, "."), nil
}

func (r *Repository) routinePath(projectID, datasetID, routineID string) (string, error) {
	var routinePath []string
	if projectID != "" {
		if err := validateIdent("project", projectID); err != nil {
			return "", err
		}
		routinePath = append(routinePath, escapeIdent(projectID))
	}
	if datasetID != "" {
		if err := validateIdent("dataset", datasetID); err != nil {
			return "", err
		}
		routinePath = append(routinePath, escapeIdent(datasetID))
	}
	if err := validateIdent("routine", routineID); err != nil {
		return "", err
	}
	routinePath = append(routinePath, escapeIdent(routineID))
	return strings.Join(routinePath, "."), nil
}

func (r *Repository) CreateTable(ctx context.Context, tx *connection.Tx, table *bigqueryv2.Table) error {
	if err := tx.ContentRepoMode(); err != nil {
		return err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()
	ref := table.TableReference
	if ref == nil {
		return fmt.Errorf("TableReference is nil")
	}
	fields := make([]string, 0, len(table.Schema.Fields))
	for _, field := range table.Schema.Fields {
		if err := validateIdent("column", field.Name); err != nil {
			return err
		}
		fields = append(fields, fmt.Sprintf("`%s` %s", escapeIdent(field.Name), r.encodeSchemaField(field)))
	}
	tablePath, err := r.tablePath(ref.ProjectId, ref.DatasetId, ref.TableId)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("CREATE TABLE `%s` (%s)", tablePath, strings.Join(fields, ","))
	if _, err := tx.Tx().ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create table %s: %w", query, err)
	}
	return nil
}

func (r *Repository) CreateView(ctx context.Context, tx *connection.Tx, table *bigqueryv2.Table) error {
	if err := tx.ContentRepoMode(); err != nil {
		return err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()
	ref := table.TableReference
	if ref == nil {
		return fmt.Errorf("TableReference is nil")
	}
	var viewQuery string
	switch {
	case table.View != nil:
		viewQuery = table.View.Query
	case table.MaterializedView != nil:
		// The emulator does not materialize results; a materialized view is
		// served as an ordinary view.
		viewQuery = table.MaterializedView.Query
	default:
		return fmt.Errorf("view definition is nil")
	}
	tablePath, err := r.tablePath(ref.ProjectId, ref.DatasetId, ref.TableId)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("CREATE VIEW `%s` AS (%s)", tablePath, viewQuery)
	if _, err := tx.Tx().ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create view %s: %w", query, err)
	}
	return nil
}

func (r *Repository) encodeSchemaField(field *bigqueryv2.TableFieldSchema) string {
	var elem string
	if field.Type == "RECORD" {
		types := make([]string, 0, len(field.Fields))
		for _, f := range field.Fields {
			types = append(types, fmt.Sprintf("%s %s", f.Name, r.encodeSchemaField(f)))
		}
		elem = fmt.Sprintf("STRUCT<%s>", strings.Join(types, ","))
	} else {
		elem = types.Type(field.Type).TypeKind().String()
	}
	if field.Mode == "REPEATED" {
		return fmt.Sprintf("ARRAY<%s>", elem)
	}
	return elem
}

func (r *Repository) Query(ctx context.Context, tx *connection.Tx, projectID, datasetID, query string, params []*bigqueryv2.QueryParameter) (*internaltypes.QueryResponse, error) {
	tx.SetProjectAndDataset(projectID, datasetID)
	if err := tx.ContentRepoMode(); err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()

	values := []interface{}{}
	for _, param := range params {
		value, err := r.queryParameterValueToGoValue(param.ParameterType, param.ParameterValue)
		if err != nil {
			return nil, err
		}
		// The BigQuery REST API encodes every scalar parameter as a
		// JSON string, regardless of its declared type. The accompanying
		// ParameterType.Type ("INT64", "BOOL", "FLOAT64", ...) is the
		// only signal of what Go type the analyzer needs. Predecessor
		// drivers were lenient about implicit STRING -> INT64 coercion;
		// googlesqlite follows the GoogleSQL spec and refuses, so coerce
		// here instead.
		if param.ParameterType != nil {
			value = coerceScalarParameterValue(value, param.ParameterType.Type)
		}
		if param.Name != "" {
			values = append(values, sql.Named(param.Name, value))
		} else {
			values = append(values, value)
		}
	}
	fields := []*bigqueryv2.TableFieldSchema{}
	logger.Logger(ctx).Info(
		"",
		zap.String("query", query),
		zap.Any("values", values),
	)
	rows, err := tx.Tx().QueryContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	changedCatalog, err := googlesqlite.ChangedCatalogFromRows(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to get changed catalog: %w", err)
	}
	colNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}
	tableRows := []*internaltypes.TableRow{}
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}
	for i := 0; i < len(columnTypes); i++ {
		typ, err := googlesqlite.UnmarshalDatabaseTypeName(columnTypes[i].DatabaseTypeName())
		if err != nil {
			return nil, fmt.Errorf("failed to get type from database type name: %w", err)
		}
		fields = append(fields, types.TableFieldSchemaFromColumnType(colNames[i], typ))
	}

	var (
		totalBytes int64
		result     = [][]interface{}{}
	)
	for rows.Next() {
		values := make([]interface{}, 0, len(columnTypes))
		for i := 0; i < len(columnTypes); i++ {
			var v interface{}
			values = append(values, &v)
		}
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}
		cells := make([]*internaltypes.TableCell, 0, len(values))
		resultValues := make([]interface{}, 0, len(values))
		for idx, value := range values {
			v := reflect.ValueOf(value).Elem().Interface()
			if v == nil && fields[idx].Mode == string(types.RepeatedMode) {
				// GoogleSQL for BigQuery translates a NULL array into an empty array in the query result
				v = []interface{}{}
			}
			cell, err := r.convertValueToCell(v, fields[idx])
			if err != nil {
				return nil, fmt.Errorf("failed to convert value to cell: %w", err)
			}
			cell.Name = colNames[idx]
			cells = append(cells, cell)
			totalBytes += cell.Bytes
			resultValues = append(resultValues, v)
		}
		result = append(result, resultValues)
		tableRows = append(tableRows, &internaltypes.TableRow{
			F: cells,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan rows: %w", err)
	}
	logger.Logger(ctx).Debug("query result", zap.Any("rows", result))
	return &internaltypes.QueryResponse{
		Schema: &bigqueryv2.TableSchema{
			Fields: fields,
		},
		TotalRows:      uint64(len(tableRows)),
		JobComplete:    true,
		Rows:           tableRows,
		TotalBytes:     totalBytes,
		ChangedCatalog: changedCatalog,
	}, nil
}

// emptyTypedArray returns a typed empty Go slice for the given BigQuery array
// element type. Using a typed slice (e.g. []string{}) rather than
// []interface{}{} lets googlesqlite infer the correct ARRAY<T> declaration
// even when there are no elements to reflect on.
func emptyTypedArray(arrayType *bigqueryv2.QueryParameterType) interface{} {
	if arrayType == nil {
		return []interface{}{}
	}
	switch strings.ToUpper(arrayType.Type) {
	case "STRING", "BYTES":
		return []string{}
	case "INT64", "INTEGER", "INT":
		return []int64{}
	case "FLOAT64", "FLOAT", "DOUBLE":
		return []float64{}
	case "BOOL", "BOOLEAN":
		return []bool{}
	default:
		return []interface{}{}
	}
}

// coerceScalarParameterValue converts the JSON-string representation
// of a scalar query parameter into the Go type that matches the
// declared BigQuery parameter type. Returns the original value
// untouched for non-scalar shapes (already converted by
// queryParameterValueToGoValue) and for unknown / unhandled type
// names so the caller can fall back to the analyzer's undeclared
// inference.
func coerceScalarParameterValue(value interface{}, paramType string) interface{} {
	s, ok := value.(string)
	if !ok {
		return value
	}
	upper := strings.ToUpper(paramType)
	// An empty scalar value for a numeric or temporal parameter denotes NULL:
	// those types have no valid empty representation. An empty STRING or BYTES,
	// by contrast, is a legitimate value and is left untouched. Passing nil
	// lets the query observe a real NULL instead of a coerced zero value.
	if s == "" {
		switch upper {
		case "INT64", "INTEGER", "INT",
			"FLOAT64", "FLOAT", "DOUBLE",
			"BOOL", "BOOLEAN",
			"NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL",
			"DATE", "TIME", "DATETIME", "TIMESTAMP":
			return nil
		}
	}
	switch upper {
	case "INT64", "INTEGER", "INT":
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
	case "FLOAT64", "FLOAT", "DOUBLE":
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	case "BOOL", "BOOLEAN":
		if b, err := strconv.ParseBool(s); err == nil {
			return b
		}
	case "TIMESTAMP":
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999Z07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
		} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UTC()
			}
		}
	case "DATETIME":
		for _, layout := range []string{
			"2006-01-02T15:04:05.999999999",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
		} {
			if t, err := time.Parse(layout, s); err == nil {
				return t
			}
		}
	}
	return value
}

func (r *Repository) queryParameterValueToGoValue(ptype *bigqueryv2.QueryParameterType, value *bigqueryv2.QueryParameterValue) (interface{}, error) {
	// A nil parameter value denotes an explicit NULL: the handler clears it
	// when the request JSON carried `null` (or no value) for the parameter.
	if value == nil {
		return nil, nil
	}
	// When the declared type is known, use it as the primary signal. This
	// handles the empty-array and empty-struct cases where ArrayValues /
	// StructValues are nil due to JSON omitempty on the bigqueryv2 fields.
	if ptype != nil {
		switch strings.ToUpper(ptype.Type) {
		case "ARRAY":
			if len(value.ArrayValues) == 0 {
				return emptyTypedArray(ptype.ArrayType), nil
			}
			arr := make([]interface{}, 0, len(value.ArrayValues))
			for _, v := range value.ArrayValues {
				elem, err := r.queryParameterValueToGoValue(ptype.ArrayType, v)
				if err != nil {
					return nil, err
				}
				arr = append(arr, elem)
			}
			return arr, nil
		case "STRUCT":
			fieldTypes := make(map[string]*bigqueryv2.QueryParameterType, len(ptype.StructTypes))
			for _, ft := range ptype.StructTypes {
				fieldTypes[ft.Name] = ft.Type
			}
			st := make(map[string]interface{}, len(value.StructValues))
			for k, v := range value.StructValues {
				vCopy := v
				elem, err := r.queryParameterValueToGoValue(fieldTypes[k], &vCopy)
				if err != nil {
					return nil, err
				}
				st[k] = elem
			}
			return st, nil
		}
	}
	// Scalar fallback: type is absent or is a scalar type. Fall back to
	// data-driven detection for safety (e.g. recursive calls where ptype
	// was not propagated).
	switch {
	case len(value.ArrayValues) != 0:
		arr := make([]interface{}, 0, len(value.ArrayValues))
		for _, v := range value.ArrayValues {
			elem, err := r.queryParameterValueToGoValue(nil, v)
			if err != nil {
				return nil, err
			}
			arr = append(arr, elem)
		}
		return arr, nil
	case len(value.StructValues) != 0:
		st := make(map[string]interface{}, len(value.StructValues))
		for k, v := range value.StructValues {
			vCopy := v
			elem, err := r.queryParameterValueToGoValue(nil, &vCopy)
			if err != nil {
				return nil, err
			}
			st[k] = elem
		}
		return st, nil
	}
	return value.Value, nil
}

// convertValueToCell renders one googlesqlite row value as a
// TableCell. The shape it sees depends on the column's TableFieldSchema:
//
//   - schema.Mode == "REPEATED": value is a Go slice (ARRAY); recurse
//     into each element with the schema's Mode forced to "NULLABLE"
//     (the element type is the un-repeated form).
//   - schema.Type == "RECORD" (and not REPEATED): value is a positional
//     `[]any` STRUCT from googlesqlite, whose element i corresponds to
//     schema.Fields[i]. Recurse element-wise and stamp each cell.Name
//     from the matching field schema.
//   - everything else: render the scalar via fmt.Sprint.
//
// googlesqlite returns STRUCT values as positional `[]any`. Field
// names live on the column type, not in the row value, so we read
// them off the BigQuery schema instead. A STRUCT value arrives as a
// reflect.Slice with reflect.Interface element kind, indistinguishable
// from ARRAY at the value level; only the column schema tells them apart.
func (r *Repository) convertValueToCell(value interface{}, schema *bigqueryv2.TableFieldSchema) (*internaltypes.TableCell, error) {
	if value == nil {
		return &internaltypes.TableCell{V: nil}, nil
	}

	// REPEATED — the value is an ARRAY at this level. Strip the
	// REPEATED-ness off the schema for the elements and recurse.
	if schema != nil && schema.Mode == string(types.RepeatedMode) {
		rv := reflect.ValueOf(value)
		kind := rv.Type().Kind()
		if kind != reflect.Slice && kind != reflect.Array {
			v := fmt.Sprint(value)
			return &internaltypes.TableCell{V: v, Bytes: int64(len(v))}, nil
		}
		elemSchema := *schema
		elemSchema.Mode = string(types.NullableMode)
		cells := []*internaltypes.TableCell{}
		var totalBytes int64
		for i := 0; i < rv.Len(); i++ {
			cell, err := r.convertValueToCell(rv.Index(i).Interface(), &elemSchema)
			if err != nil {
				return nil, err
			}
			totalBytes += cell.Bytes
			cells = append(cells, cell)
		}
		return &internaltypes.TableCell{V: cells, Bytes: totalBytes}, nil
	}

	// RECORD (= STRUCT) at this level. googlesqlite returns a positional
	// []any whose element i is the value of schema.Fields[i].
	if schema != nil && schema.Type == string(types.RECORD) {
		rv := reflect.ValueOf(value)
		kind := rv.Type().Kind()
		if kind != reflect.Slice && kind != reflect.Array {
			v := fmt.Sprint(value)
			return &internaltypes.TableCell{V: v, Bytes: int64(len(v))}, nil
		}
		cells := []*internaltypes.TableCell{}
		var totalBytes int64
		for i := 0; i < rv.Len(); i++ {
			var fieldSchema *bigqueryv2.TableFieldSchema
			if i < len(schema.Fields) {
				fieldSchema = schema.Fields[i]
			}
			cell, err := r.convertValueToCell(rv.Index(i).Interface(), fieldSchema)
			if err != nil {
				return nil, err
			}
			if fieldSchema != nil {
				cell.Name = fieldSchema.Name
			}
			totalBytes += cell.Bytes
			cells = append(cells, cell)
		}
		return &internaltypes.TableCell{V: internaltypes.TableRow{F: cells}, Bytes: totalBytes}, nil
	}

	// Scalar. Render via fmt.Sprint — matches the legacy behaviour
	// for non-RECORD, non-REPEATED columns.
	v := fmt.Sprint(value)
	return &internaltypes.TableCell{V: v, Bytes: int64(len(v))}, nil
}

func (r *Repository) CreateOrReplaceTable(ctx context.Context, tx *connection.Tx, projectID, datasetID string, table *types.Table) error {
	tx.SetProjectAndDataset(projectID, datasetID)
	if err := tx.MetadataRepoMode(); err != nil {
		return err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()

	columns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		if err := validateIdent("column", column.Name); err != nil {
			return err
		}
		columns = append(columns,
			fmt.Sprintf("`%s` %s", escapeIdent(column.Name), column.FormatType()),
		)
	}
	tablePath, err := r.tablePath(projectID, datasetID, table.ID)
	if err != nil {
		return err
	}
	ddl := fmt.Sprintf(
		"CREATE OR REPLACE TABLE `%s` (%s)",
		tablePath, strings.Join(columns, ","),
	)

	if _, err := tx.Tx().ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("failed to execute DDL %s: %w", ddl, err)
	}
	return nil
}

func (r *Repository) AddTableData(ctx context.Context, tx *connection.Tx, projectID, datasetID string, table *types.Table) error {
	if len(table.Data) == 0 {
		return nil
	}
	tx.SetProjectAndDataset(projectID, datasetID)
	if err := tx.MetadataRepoMode(); err != nil {
		return err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()

	var columns []*types.Column
	for _, col := range table.Columns {
		columns = append(columns, col)
	}

	placeholders := make([]string, 0, len(columns))
	columnsWithEscape := make([]string, 0, len(columns))
	for _, col := range columns {
		if err := validateIdent("column", col.Name); err != nil {
			return err
		}
		if col.Type == types.JSON {
			// A JSON column cannot take a raw bound string: SQLite would store
			// it as a JSON string literal (double-encoded). PARSE_JSON turns
			// the bound JSON text into a proper JSON value.
			placeholders = append(placeholders, "PARSE_JSON(?)")
		} else {
			placeholders = append(placeholders, "?")
		}
		columnsWithEscape = append(columnsWithEscape, fmt.Sprintf("`%s`", escapeIdent(col.Name)))
	}

	tablePath, err := r.tablePath(projectID, datasetID, table.ID)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(
		"INSERT `%s` (%s) VALUES (%s)",
		tablePath,
		strings.Join(columnsWithEscape, ","),
		strings.Join(placeholders, ","),
	)

	stmt, err := tx.Tx().PrepareContext(ctx, query)
	if err != nil {
		return err
	}

	for _, data := range table.Data {
		values := make([]interface{}, 0, len(table.Columns))

		for _, column := range columns {
			value, found := data[column.Name]
			if !found || value == nil {
				values = append(values, nil)
				continue
			}

			if column.Type == types.JSON {
				// The PARSE_JSON(?) placeholder expects JSON text. A string
				// value is already JSON text (clients json-encode it); any
				// other Go value is marshaled to its JSON representation.
				jsonText, err := jsonColumnText(value)
				if err != nil {
					return fmt.Errorf("failed to encode JSON value for column %s: %w", column.Name, err)
				}
				values = append(values, jsonText)
				continue
			}

			inputString, isInputString := value.(string)
			if isInputString && column.Type == types.TIMESTAMP {
				parsedTimestamp, err := googlesqlite.TimeFromTimestampValue(inputString)
				// If we could parse the timestamp, use it when inserting, otherwise fallback to the supplied value
				if err == nil {
					values = append(values, parsedTimestamp)
					continue
				}
			}

			values = append(values, value)
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return err
		}
	}

	return nil
}

// jsonColumnText returns the JSON text for a value bound to a JSON column.
// A string is assumed to already hold JSON text (the form clients send for
// JSON columns); any other value is marshaled to its JSON representation.
func jsonColumnText(value interface{}) (string, error) {
	if s, ok := value.(string); ok {
		return s, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// TableDeletion identifies one table or view to drop. A view must be dropped
// with DROP VIEW; DROP TABLE does not apply to it.
type TableDeletion struct {
	ID     string
	IsView bool
}

func (r *Repository) DeleteTables(ctx context.Context, tx *connection.Tx, projectID, datasetID string, tables []TableDeletion) error {
	tx.SetProjectAndDataset(projectID, datasetID)
	if err := tx.ContentRepoMode(); err != nil {
		return err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()

	for _, table := range tables {
		tablePath, err := r.tablePath(projectID, datasetID, table.ID)
		if err != nil {
			return err
		}
		logger.Logger(ctx).Debug("delete table", zap.String("table", tablePath))
		stmt := "DROP TABLE"
		if table.IsView {
			stmt = "DROP VIEW"
		}
		query := fmt.Sprintf("%s `%s`", stmt, tablePath)
		if _, err := tx.Tx().ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to delete table %s: %w", query, err)
		}
	}
	return nil
}

// TruncateTable removes every row from a table, implementing the
// WRITE_TRUNCATE write disposition.
func (r *Repository) TruncateTable(ctx context.Context, tx *connection.Tx, projectID, datasetID, tableID string) error {
	tx.SetProjectAndDataset(projectID, datasetID)
	if err := tx.ContentRepoMode(); err != nil {
		return err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()
	tablePath, err := r.tablePath(projectID, datasetID, tableID)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("DELETE FROM `%s` WHERE true", tablePath)
	if _, err := tx.Tx().ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to truncate table %s: %w", query, err)
	}
	return nil
}

// CountTableRows returns the number of rows in a table, used to enforce the
// WRITE_EMPTY write disposition.
func (r *Repository) CountTableRows(ctx context.Context, tx *connection.Tx, projectID, datasetID, tableID string) (int64, error) {
	tx.SetProjectAndDataset(projectID, datasetID)
	if err := tx.ContentRepoMode(); err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()
	tablePath, err := r.tablePath(projectID, datasetID, tableID)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tablePath)
	var count int64
	if err := tx.Tx().QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count rows of %s: %w", query, err)
	}
	return count, nil
}

// ViewSchema returns the column schema of a view by analyzing its definition.
// The view must already exist (visible to tx). It mirrors BigQuery, which
// records a view's resolved schema at creation time.
func (r *Repository) ViewSchema(ctx context.Context, tx *connection.Tx, projectID, datasetID, viewID string) (*bigqueryv2.TableSchema, error) {
	tablePath, err := r.tablePath(projectID, datasetID, viewID)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("SELECT * FROM `%s` LIMIT 0", tablePath)
	response, err := r.Query(ctx, tx, projectID, datasetID, query, nil)
	if err != nil {
		return nil, err
	}
	return response.Schema, nil
}

type RoutineType string

const (
	ScalarFunctionType      RoutineType = "SCALAR_FUNCTION"
	ProcedureType           RoutineType = "PROCEDURE"
	TableValuedFunctionType RoutineType = "TABLE_VALUED_FUNCTION"
)

type RoutineLanguageType string

const (
	LanguageTypeSQL        RoutineLanguageType = "SQL"
	LanguageTypeJavaScript RoutineLanguageType = "JavaScript"
)

func (r *Repository) AddRoutineByMetaData(ctx context.Context, tx *connection.Tx, routine *bigqueryv2.Routine) error {
	ref := routine.RoutineReference
	tx.SetProjectAndDataset(ref.ProjectId, ref.DatasetId)
	if err := tx.ContentRepoMode(); err != nil {
		return err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()

	var routineType string
	switch RoutineType(routine.RoutineType) {
	case ScalarFunctionType:
		routineType = "CREATE FUNCTION"
	case ProcedureType:
		routineType = "CREATE PROCEDURE"
	case TableValuedFunctionType:
		routineType = "CREATE TABLE FUNCTION"
	default:
		return fmt.Errorf("invalid routine type %s", routine.RoutineType)
	}
	switch RoutineLanguageType(routine.Language) {
	case LanguageTypeSQL:
	case LanguageTypeJavaScript:
		return fmt.Errorf("unsupported language: JavaScript")
	default:
		return fmt.Errorf("invalid language %s", routine.Language)
	}
	args := make([]string, 0, len(routine.Arguments))
	for _, arg := range routine.Arguments {
		if arg.Name == "" {
			return fmt.Errorf("invalid argument: missing name of argument")
		}
		if arg.DataType == nil {
			return fmt.Errorf("invalid argument: missing data type for %s", arg.Name)
		}
		args = append(args, fmt.Sprintf("%s %s", arg.Name, arg.DataType.TypeKind))
	}
	var retType string
	if routine.ReturnType != nil {
		retType = fmt.Sprintf(" RETURNS %s", routine.ReturnType.TypeKind)
	}
	if routine.DefinitionBody == "" {
		return fmt.Errorf("invalid body: missing function body")
	}
	routinePath, err := r.routinePath(ref.ProjectId, ref.DatasetId, ref.RoutineId)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(
		"%s `%s`(%s)%s AS (%s)",
		routineType,
		routinePath,
		strings.Join(args, ", "),
		retType,
		routine.DefinitionBody,
	)
	if _, err := tx.Tx().ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create function %s: %w", query, err)
	}
	return nil
}
