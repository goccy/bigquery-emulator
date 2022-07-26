package contentdata

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/goccy/bigquery-emulator/types"
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

func (r *Repository) CreateTable(ctx context.Context, tx *sql.Tx, table *bigqueryv2.Table) error {
	ref := table.TableReference
	if ref == nil {
		return fmt.Errorf("TableReference is nil")
	}
	fields := make([]string, 0, len(table.Schema.Fields))
	for _, field := range table.Schema.Fields {
		fields = append(fields, fmt.Sprintf("`%s` %s", field.Name, types.Type(field.Type).ZetaSQLTypeKind()))
	}
	query := fmt.Sprintf("CREATE TABLE `%s` (%s)", ref.TableId, strings.Join(fields, ","))
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create table %s: %w", query, err)
	}
	return nil
}

func (r *Repository) Query(ctx context.Context, tx *sql.Tx, projectID, datasetID, query string, params []*bigqueryv2.QueryParameter) (*bigqueryv2.QueryResponse, error) {
	values := []interface{}{}
	for _, param := range params {
		// param.Name
		values = append(values, param.ParameterValue.Value)
	}
	fields := []*bigqueryv2.TableFieldSchema{}
	log.Printf("query: %s. values: %v", query, values)
	rows, err := tx.QueryContext(ctx, query, values...)
	if err != nil {
		log.Printf("query error: %+v", err)
		return nil, err
	}
	defer rows.Close()
	tableRows := []*bigqueryv2.TableRow{}
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
		cells := make([]*bigqueryv2.TableCell, 0, len(values))
		resultValues := make([]interface{}, 0, len(values))
		for _, value := range values {
			v := reflect.ValueOf(value).Elem().Interface()
			if v == nil {
				cells = append(cells, nil)
			} else {
				cells = append(cells, &bigqueryv2.TableCell{V: fmt.Sprint(v)})
			}
			resultValues = append(resultValues, v)
		}
		result = append(result, resultValues)
		tableRows = append(tableRows, &bigqueryv2.TableRow{
			F: cells,
		})
	}
	log.Printf("rows: %v", result)
	return &bigqueryv2.QueryResponse{
		Rows: tableRows,
		Schema: &bigqueryv2.TableSchema{
			Fields: fields,
		},
		TotalRows:   uint64(len(tableRows)),
		JobComplete: true,
	}, nil
}

func (r *Repository) CreateOrReplaceTable(ctx context.Context, tx *sql.Tx, projectID, datasetID string, table *types.Table) error {
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
	if _, err := tx.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("failed to execute DDL %s: %w", ddl, err)
	}
	return nil
}

func (r *Repository) AddTableData(ctx context.Context, tx *sql.Tx, projectID, datasetID string, table *types.Table) error {
	for _, data := range table.Data {
		columns := make([]string, 0, len(data))
		values := make([]interface{}, 0, len(data))
		params := make([]string, 0, len(data))
		for k, v := range data {
			columns = append(columns, fmt.Sprintf("`%s`", k))
			values = append(values, v)
			params = append(params, fmt.Sprintf("@%s", k))
		}
		query := fmt.Sprintf(
			"INSERT `%s` (%s) VALUES (%s)",
			table.ID,
			strings.Join(columns, ","),
			strings.Join(params, ","),
		)
		if _, err := tx.ExecContext(ctx, query, values...); err != nil {
			return fmt.Errorf("failed to insert data %s: %w", query, err)
		}
	}
	return nil
}

func (r *Repository) DeleteTables(ctx context.Context, tx *sql.Tx, projectID, datasetID string, tableIDs []string) error {
	for _, tableID := range tableIDs {
		log.Printf("delete table %s", tableID)
		query := fmt.Sprintf("DROP TABLE `%s`", tableID)
		if _, err := tx.ExecContext(ctx, query); err != nil {
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

func (r *Repository) AddRoutineByMetaData(ctx context.Context, tx *sql.Tx, routine *bigqueryv2.Routine) error {
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
	ref := routine.RoutineReference
	query := fmt.Sprintf(
		"%s `%s`(%s)%s AS (%s)",
		routineType,
		ref.RoutineId,
		strings.Join(args, ", "),
		retType,
		routine.DefinitionBody,
	)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create function %s: %w", query, err)
	}
	return nil
}
