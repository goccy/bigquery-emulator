package contentdata

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/goccy/go-zetasqlite"
	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/logger"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{
		db: db,
	}
}

func (r *Repository) getConnection(ctx context.Context, projectID, datasetID string) (*sql.Conn, error) {
	if projectID == "" {
		return nil, fmt.Errorf("invalid projectID. projectID is empty")
	}
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	if err := conn.Raw(func(c interface{}) error {
		zetasqliteConn, ok := c.(*zetasqlite.ZetaSQLiteConn)
		if !ok {
			return fmt.Errorf("failed to get ZetaSQLiteConn from %T", c)
		}
		if datasetID == "" {
			_ = zetasqliteConn.SetNamePath([]string{projectID})
		} else {
			_ = zetasqliteConn.SetNamePath([]string{projectID, datasetID})
		}
		const maxNamePath = 3 // projectID and datasetID and tableID
		zetasqliteConn.SetMaxNamePath(maxNamePath)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to setup connection: %w", err)
	}
	return conn, nil
}

func (r *Repository) tablePath(projectID, datasetID, tableID string) string {
	var tablePath []string
	if projectID != "" {
		tablePath = append(tablePath, projectID)
	}
	if datasetID != "" {
		tablePath = append(tablePath, datasetID)
	}
	tablePath = append(tablePath, tableID)
	return strings.Join(tablePath, ".")
}

func (r *Repository) routinePath(projectID, datasetID, routineID string) string {
	var routinePath []string
	if projectID != "" {
		routinePath = append(routinePath, projectID)
	}
	if datasetID != "" {
		routinePath = append(routinePath, datasetID)
	}
	routinePath = append(routinePath, routineID)
	return strings.Join(routinePath, ".")
}

func stringReference(ref *bigqueryv2.TableReference) string {
	return fmt.Sprintf("%s:%s.%s", ref.ProjectId, ref.DatasetId, ref.TableId)
}

func (r *Repository) AlterTable(ctx context.Context, tx *connection.Tx, table *bigqueryv2.Table, newSchema *bigqueryv2.TableSchema) *ContentDataError {
	if newSchema == nil {
		return nil
	}

	if err := tx.ContentRepoMode(); err != nil {
		return ErrorWithCause(Unknown, err)
	}

	defer func() {
		_ = tx.MetadataRepoMode()
	}()

	ref := table.TableReference
	if ref == nil {
		return ErrorWithMessage(Unknown, "TableReference is nil")
	}
	tablePath := r.tablePath(ref.ProjectId, ref.DatasetId, ref.TableId)

	alterStatements := make([]string, 0)
	oldFieldMap := make(map[string]*bigqueryv2.TableFieldSchema)
	for _, field := range table.Schema.Fields {
		oldFieldMap[field.Name] = field
	}

	for _, newField := range newSchema.Fields {
		oldField, exists := oldFieldMap[newField.Name]
		if !exists {
			alterStatements = append(alterStatements, fmt.Sprintf("ADD COLUMN `%s` %s", newField.Name, r.encodeSchemaField(newField)))
		} else if !reflect.DeepEqual(oldField, newField) {
			oldFieldAfterUpdates := *oldField
			if oldField.Description != newField.Description {
				// it's ok, ignore
				oldFieldAfterUpdates.Description = newField.Description
				//alterStatements = append(alterStatements, fmt.Sprintf("ALTER COLUMN `%s` SET DEFAULT %s", newField.Name, newField.DefaultValueExpression))
				//oldFieldAfterUpdates.Description = newField.Description
				//if !reflect.DeepEqual(&oldFieldAfterUpdates, newField) {
				//	message := fmt.Sprintf(
				//		"(Todo) one change allowed at a time",
				//	)
				//	return ErrorWithMessage(Unknown, message)
				//}
			} else if oldField.DefaultValueExpression != newField.DefaultValueExpression {
				alterStatements = append(alterStatements, fmt.Sprintf("ALTER COLUMN `%s` SET DEFAULT %s", newField.Name, newField.DefaultValueExpression))
				oldFieldAfterUpdates.DefaultValueExpression = newField.DefaultValueExpression
				if !reflect.DeepEqual(&oldFieldAfterUpdates, newField) {
					message := fmt.Sprintf(
						"(Todo) one change allowed at a time",
					)
					return ErrorWithMessage(Unknown, message)
				}
			} else {
				return ErrorWithMessage(Unknown, "unknown")
			}
			// FIXME: what about field description?

		}
		delete(oldFieldMap, newField.Name)
	}

	if len(oldFieldMap) > 0 {
		return Error(AlterTableExistingFieldRemoved)
	}

	if len(alterStatements) > 0 {
		query := fmt.Sprintf("ALTER TABLE `%s` %s", tablePath, strings.Join(alterStatements, ", "))
		if _, err := tx.Tx().ExecContext(ctx, query); err != nil {
			return ErrorWithCause(Unknown, fmt.Errorf("failed to alter table %s: %w", query, err))
		}
	}

	return nil
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
		fields = append(fields, fmt.Sprintf("`%s` %s", field.Name, r.encodeSchemaField(field)))
	}
	tablePath := r.tablePath(ref.ProjectId, ref.DatasetId, ref.TableId)
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
	viewDefinition := table.View
	if viewDefinition == nil {
		return fmt.Errorf("ViewDefinition is nil")
	}
	tablePath := r.tablePath(ref.ProjectId, ref.DatasetId, ref.TableId)
	query := fmt.Sprintf("CREATE VIEW `%s` AS (%s)", tablePath, viewDefinition.Query)
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
		elem = types.Type(field.Type).ZetaSQLTypeKind().String()
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
		value, err := r.queryParameterValueToGoValue(param.ParameterValue)
		if err != nil {
			return nil, err
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
	changedCatalog, err := zetasqlite.ChangedCatalogFromRows(rows)
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
		typ, err := zetasqlite.UnmarshalDatabaseTypeName(columnTypes[i].DatabaseTypeName())
		if err != nil {
			return nil, fmt.Errorf("failed to get type from database type name: %w", err)
		}
		zetasqlType, err := typ.ToZetaSQLType()
		if err != nil {
			return nil, err
		}
		fields = append(fields, types.TableFieldSchemaFromZetaSQLType(colNames[i], zetasqlType))
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
			cell, err := r.convertValueToCell(v)
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

func (r *Repository) queryParameterValueToGoValue(value *bigqueryv2.QueryParameterValue) (interface{}, error) {
	switch {
	case len(value.ArrayValues) != 0:
		arr := make([]interface{}, 0, len(value.ArrayValues))
		for _, v := range value.ArrayValues {
			elem, err := r.queryParameterValueToGoValue(v)
			if err != nil {
				return nil, err
			}
			arr = append(arr, elem)
		}
		return arr, nil
	case len(value.StructValues) != 0:
		st := make(map[string]interface{}, len(value.StructValues))
		for k, v := range value.StructValues {
			elem, err := r.queryParameterValueToGoValue(&v)
			if err != nil {
				return nil, err
			}
			st[k] = elem
		}
		return st, nil
	}
	return value.Value, nil
}

// zetasqlite returns []map[string]interface{} value as struct value, also returns []interface{} value as array value.
// we need to convert them to specifically TableRow and TableCell type.
func (r *Repository) convertValueToCell(value interface{}) (*internaltypes.TableCell, error) {
	if value == nil {
		return &internaltypes.TableCell{V: nil}, nil
	}
	rv := reflect.ValueOf(value)
	kind := rv.Type().Kind()
	if kind != reflect.Slice && kind != reflect.Array {
		v := fmt.Sprint(value)
		return &internaltypes.TableCell{V: v, Bytes: int64(len(v))}, nil
	}
	elemType := rv.Type().Elem()
	if elemType.Kind() == reflect.Map {
		// value is struct type
		var (
			cells      []*internaltypes.TableCell
			totalBytes int64
		)
		for i := 0; i < rv.Len(); i++ {
			fieldV := rv.Index(i)
			keys := fieldV.MapKeys()
			if len(keys) != 1 {
				return nil, fmt.Errorf("unexpected key number of field map value. expected 1 but got %d", len(keys))
			}
			cell, err := r.convertValueToCell(fieldV.MapIndex(keys[0]).Interface())
			if err != nil {
				return nil, err
			}
			cell.Name = keys[0].Interface().(string)
			totalBytes += cell.Bytes
			cells = append(cells, cell)
		}
		return &internaltypes.TableCell{V: internaltypes.TableRow{F: cells}, Bytes: totalBytes}, nil
	}
	// array type
	var (
		cells            = []*internaltypes.TableCell{}
		totalBytes int64 = 0
	)
	for i := 0; i < rv.Len(); i++ {
		cell, err := r.convertValueToCell(rv.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		totalBytes += cell.Bytes
		cells = append(cells, cell)
	}
	return &internaltypes.TableCell{V: cells, Bytes: totalBytes}, nil
}

func (r *Repository) CreateOrReplaceTable(ctx context.Context, tx *connection.Tx, projectID, datasetID string, table *types.Table) error {
	tx.SetProjectAndDataset(projectID, datasetID)
	if err := tx.ContentRepoMode(); err != nil {
		return err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()

	columns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		columns = append(columns,
			fmt.Sprintf("`%s` %s", column.Name, column.FormatType()),
		)
	}
	ddl := fmt.Sprintf(
		"CREATE OR REPLACE TABLE `%s` (%s)",
		r.tablePath(projectID, datasetID, table.ID), strings.Join(columns, ","),
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
	if err := tx.ContentRepoMode(); err != nil {
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
		placeholders = append(placeholders, "?")
		columnsWithEscape = append(columnsWithEscape, fmt.Sprintf("`%s`", col.Name))
	}

	query := fmt.Sprintf(
		"INSERT `%s` (%s) VALUES (%s)",
		r.tablePath(projectID, datasetID, table.ID),
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
			if value, found := data[column.Name]; found {
				isTimestampColumn := column.Type == types.TIMESTAMP
				inputString, isInputString := value.(string)

				if isInputString && isTimestampColumn {
					parsedTimestamp, err := zetasqlite.TimeFromTimestampValue(inputString)
					// If we could parse the timestamp, use it when inserting, otherwise fallback to the supplied value
					if err == nil {
						values = append(values, parsedTimestamp)
						continue
					}
				}

				values = append(values, value)
			} else {
				values = append(values, nil)
			}
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return err
		}
	}

	return nil
}

func (r *Repository) DeleteTables(ctx context.Context, tx *connection.Tx, projectID, datasetID string, tableIDs []string) error {
	tx.SetProjectAndDataset(projectID, datasetID)
	if err := tx.ContentRepoMode(); err != nil {
		return err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
	}()

	for _, tableID := range tableIDs {
		tablePath := r.tablePath(projectID, datasetID, tableID)
		logger.Logger(ctx).Debug("delete table", zap.String("table", tablePath))
		query := fmt.Sprintf("DROP TABLE `%s`", tablePath)
		if _, err := tx.Tx().ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to delete table %s: %w", query, err)
		}
	}
	return nil
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
	query := fmt.Sprintf(
		"%s `%s`(%s)%s AS (%s)",
		routineType,
		r.routinePath(ref.ProjectId, ref.DatasetId, ref.RoutineId),
		strings.Join(args, ", "),
		retType,
		routine.DefinitionBody,
	)
	if _, err := tx.Tx().ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create function %s: %w", query, err)
	}
	return nil
}
