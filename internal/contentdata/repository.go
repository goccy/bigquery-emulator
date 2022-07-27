package contentdata

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"

	"github.com/goccy/bigquery-emulator/internal/connection"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
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
			zetasqliteConn.SetNamePath([]string{projectID})
		} else {
			zetasqliteConn.SetNamePath([]string{projectID, datasetID})
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to setup connection: %w", err)
	}
	return conn, nil
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
		fields = append(fields, fmt.Sprintf("`%s` %s", field.Name, types.Type(field.Type).ZetaSQLTypeKind()))
	}
	query := fmt.Sprintf("CREATE TABLE `%s` (%s)", ref.TableId, strings.Join(fields, ","))
	if _, err := tx.Tx().ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create table %s: %w", query, err)
	}
	return nil
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
		// param.Name
		values = append(values, param.ParameterValue.Value)
	}
	fields := []*bigqueryv2.TableFieldSchema{}
	log.Printf("query: %s. values: %v", query, values)
	rows, err := tx.Tx().QueryContext(ctx, query, values...)
	if err != nil {
		log.Printf("query error: %+v", err)
		return nil, err
	}
	defer rows.Close()
	tableRows := []*internaltypes.TableRow{}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}
	colNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %w", err)
	}
	for i := 0; i < len(colTypes); i++ {
		fields = append(fields, &bigqueryv2.TableFieldSchema{
			Name: colNames[i],
			Type: string(types.Type(colTypes[i].DatabaseTypeName()).FieldType()),
		})
	}

	result := [][]interface{}{}
	for rows.Next() {
		values := make([]interface{}, 0, len(colNames))
		for i := 0; i < len(colNames); i++ {
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
		for _, value := range values {
			v := reflect.ValueOf(value).Elem().Interface()
			if v == nil {
				cells = append(cells, &internaltypes.TableCell{V: nil})
			} else {
				cells = append(cells, &internaltypes.TableCell{V: fmt.Sprint(v)})
			}
			resultValues = append(resultValues, v)
		}
		result = append(result, resultValues)
		tableRows = append(tableRows, &internaltypes.TableRow{
			F: cells,
		})
	}
	log.Printf("rows: %v", result)
	return &internaltypes.QueryResponse{
		Schema: &bigqueryv2.TableSchema{
			Fields: fields,
		},
		TotalRows:   uint64(len(tableRows)),
		JobComplete: true,
		Rows:        tableRows,
	}, nil
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
			fmt.Sprintf("`%s` %s", column.Name, column.Type.ZetaSQLTypeKind()),
		)
	}
	ddl := fmt.Sprintf(
		"CREATE OR REPLACE TABLE `%s` (%s)",
		table.ID, strings.Join(columns, ","),
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
	if err := tx.SetParameterMode(zetasql.ParameterPositional); err != nil {
		return err
	}
	defer func() {
		_ = tx.MetadataRepoMode()
		_ = tx.SetParameterMode(zetasql.ParameterNamed)
	}()

	var columns []string
	for col := range table.Data[0] {
		columns = append(columns, col)
	}
	sort.Strings(columns)
	rows := make([]string, 0, len(table.Data))
	values := make([]interface{}, 0, len(table.Data)*len(columns))
	for _, data := range table.Data {
		placeholders := make([]string, 0, len(data))
		for _, col := range columns {
			values = append(values, data[col])
			placeholders = append(placeholders, "?")
		}
		rows = append(rows, fmt.Sprintf("(%s)", strings.Join(placeholders, ",")))
	}
	query := fmt.Sprintf(
		"INSERT `%s` (%s) VALUES %s",
		table.ID,
		strings.Join(columns, ","),
		strings.Join(rows, ","),
	)
	if _, err := tx.Tx().ExecContext(ctx, query, values...); err != nil {
		return err
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
		log.Printf("delete table %s", tableID)
		query := fmt.Sprintf("DROP TABLE `%s`", tableID)
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
		ref.RoutineId,
		strings.Join(args, ", "),
		retType,
		routine.DefinitionBody,
	)
	if _, err := tx.Tx().ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create function %s: %w", query, err)
	}
	return nil
}
