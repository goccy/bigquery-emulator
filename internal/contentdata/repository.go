package contentdata

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/go-zetasqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type Repository struct {
	storagePath                 string
	projectIDToConn             map[string]*zetasqlite.ZetaSQLiteConn
	projectIDAndDatasetIDToConn map[string]*zetasqlite.ZetaSQLiteConn
}

func NewRepository(storagePath string) *Repository {
	return &Repository{
		storagePath:                 storagePath,
		projectIDToConn:             map[string]*zetasqlite.ZetaSQLiteConn{},
		projectIDAndDatasetIDToConn: map[string]*zetasqlite.ZetaSQLiteConn{},
	}
}

func (r *Repository) Close() error {
	for _, c := range r.projectIDToConn {
		c.Close()
	}
	for _, c := range r.projectIDAndDatasetIDToConn {
		c.Close()
	}
	return nil
}

func (r *Repository) exec(conn *zetasqlite.ZetaSQLiteConn, query string, args []driver.Value) error {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	if _, err := stmt.Exec(args); err != nil {
		return err
	}
	return nil
}

func (r *Repository) getConnection(projectID, datasetID string) (*zetasqlite.ZetaSQLiteConn, error) {
	if datasetID == "" {
		return r.getConnectionByProjectID(projectID)
	}
	return r.getConnectionByProjectIDAndDatasetID(projectID, datasetID)
}

func (r *Repository) getConnectionByProjectID(projectID string) (*zetasqlite.ZetaSQLiteConn, error) {
	if projectID == "" {
		return nil, fmt.Errorf("invalid projectID. projectID is empty")
	}
	conn, exists := r.projectIDToConn[projectID]
	if exists {
		return conn, nil
	}

	driver := &zetasqlite.ZetaSQLiteDriver{
		ConnectHook: func(conn *zetasqlite.ZetaSQLiteConn) error {
			conn.SetNamePath([]string{projectID})
			return nil
		},
	}
	driverConn, err := driver.Open(r.storagePath)
	if err != nil {
		return nil, err
	}
	conn = driverConn.(*zetasqlite.ZetaSQLiteConn)
	r.projectIDToConn[projectID] = conn
	return conn, nil
}

func (r *Repository) getConnectionByProjectIDAndDatasetID(projectID, datasetID string) (*zetasqlite.ZetaSQLiteConn, error) {
	if projectID == "" {
		return nil, fmt.Errorf("invalid projectID. projectID is empty")
	}
	if datasetID == "" {
		return nil, fmt.Errorf("invalid datasetID: datasetID is empty")
	}

	connName := fmt.Sprintf("project_%s_dataset_%s", projectID, datasetID)

	conn, exists := r.projectIDAndDatasetIDToConn[connName]
	if exists {
		return conn, nil
	}

	driver := &zetasqlite.ZetaSQLiteDriver{
		ConnectHook: func(conn *zetasqlite.ZetaSQLiteConn) error {
			conn.SetNamePath([]string{projectID, datasetID})
			return nil
		},
	}
	driverConn, err := driver.Open(r.storagePath)
	if err != nil {
		return nil, err
	}
	conn = driverConn.(*zetasqlite.ZetaSQLiteConn)
	r.projectIDAndDatasetIDToConn[connName] = conn
	return conn, nil
}

func (r *Repository) Query(projectID, datasetID, query string, params []*bigqueryv2.QueryParameter) (*bigqueryv2.QueryResponse, error) {
	conn, err := r.getConnection(projectID, datasetID)
	if err != nil {
		return nil, err
	}
	stmt, err := conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	values := []driver.Value{}
	for _, param := range params {
		// param.Name
		values = append(values, param.ParameterValue.Value)
	}
	queryStmt, ok := stmt.(*zetasqlite.QueryStmt)
	if !ok {
		if _, err := stmt.Exec(values); err != nil {
			return nil, err
		}
		return &bigqueryv2.QueryResponse{
			DmlStats:    &bigqueryv2.DmlStatistics{},
			JobComplete: true,
		}, nil
	}

	fields := []*bigqueryv2.TableFieldSchema{}
	rows, err := stmt.Query(values)
	if err != nil {
		return nil, err
	}
	for _, column := range queryStmt.OutputColumns() {
		fields = append(fields, &bigqueryv2.TableFieldSchema{
			Name: column.Name,
			Type: string(types.TypeFromKind(column.Type.Kind).FieldType()),
		})
	}
	tableRows := []*bigqueryv2.TableRow{}
	for {
		values := make([]driver.Value, len(rows.Columns()))
		if err := rows.Next(values); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		cells := make([]*bigqueryv2.TableCell, 0, len(values))
		for _, value := range values {
			cells = append(cells, &bigqueryv2.TableCell{V: fmt.Sprint(value)})
		}
		tableRows = append(tableRows, &bigqueryv2.TableRow{
			F: cells,
		})
	}
	return &bigqueryv2.QueryResponse{
		Rows: tableRows,
		Schema: &bigqueryv2.TableSchema{
			Fields: fields,
		},
		TotalRows:   uint64(len(tableRows)),
		JobComplete: true,
	}, nil
}

func (r *Repository) CreateOrReplaceTable(ctx context.Context, projectID, datasetID string, table *types.Table) error {
	conn, err := r.getConnection(projectID, datasetID)
	if err != nil {
		return err
	}
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
	if err := r.exec(conn, ddl, nil); err != nil {
		return err
	}
	return nil
}

func (r *Repository) AddTableData(ctx context.Context, projectID, datasetID string, table *types.Table) error {
	conn, err := r.getConnection(projectID, datasetID)
	if err != nil {
		return err
	}
	for _, data := range table.Data {
		columns := make([]string, 0, len(data))
		values := make([]driver.Value, 0, len(data))
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
		if err := r.exec(conn, query, values); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) DeleteTables(ctx context.Context, projectID, datasetID string, tableIDs []string) error {
	conn, err := r.getConnection(projectID, datasetID)
	if err != nil {
		return err
	}
	for _, tableID := range tableIDs {
		log.Printf("delete table %s", tableID)
		if err := r.exec(conn, fmt.Sprintf("DROP TABLE `%s`", tableID), nil); err != nil {
			return err
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

func (r *Repository) AddRoutineByMetaData(ctx context.Context, routine *bigqueryv2.Routine) error {
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
	conn, err := r.getConnection(ref.ProjectId, ref.DatasetId)
	if err != nil {
		return err
	}
	if err := r.exec(conn, query, nil); err != nil {
		return err
	}
	return nil
}
