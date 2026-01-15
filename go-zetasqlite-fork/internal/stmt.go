package internal

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	ast "github.com/goccy/go-zetasql/resolved_ast"
)

var (
	_ driver.Stmt = &CreateTableStmt{}
	_ driver.Stmt = &CreateFunctionStmt{}
	_ driver.Stmt = &DMLStmt{}
	_ driver.Stmt = &QueryStmt{}
)

type CreateTableStmt struct {
	stmt    *sql.Stmt
	conn    *Conn
	catalog *Catalog
	spec    *TableSpec
}

type CreateViewStmt struct {
	stmt    *sql.Stmt
	conn    *Conn
	catalog *Catalog
	spec    *TableSpec
}

func (s *CreateTableStmt) Close() error {
	return s.stmt.Close()
}

func (s *CreateTableStmt) NumInput() int {
	return 0
}

func (s *CreateTableStmt) Exec(args []driver.Value) (driver.Result, error) {
	if _, err := s.stmt.Exec(args); err != nil {
		return nil, err
	}
	if err := s.catalog.AddNewTableSpec(context.Background(), s.conn, s.spec); err != nil {
		return nil, fmt.Errorf("failed to add new table spec: %w", err)
	}
	return nil, nil
}

func (s *CreateTableStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("failed to query for CreateTableStmt")
}

func newCreateTableStmt(stmt *sql.Stmt, conn *Conn, catalog *Catalog, spec *TableSpec) *CreateTableStmt {
	return &CreateTableStmt{
		stmt:    stmt,
		conn:    conn,
		catalog: catalog,
		spec:    spec,
	}
}

func newCreateViewStmt(stmt *sql.Stmt, conn *Conn, catalog *Catalog, spec *TableSpec) *CreateViewStmt {
	return &CreateViewStmt{
		stmt:    stmt,
		conn:    conn,
		catalog: catalog,
		spec:    spec,
	}
}

func (s *CreateViewStmt) Close() error {
	return s.stmt.Close()
}

func (s *CreateViewStmt) NumInput() int {
	return 0
}

func (s *CreateViewStmt) Exec(args []driver.Value) (driver.Result, error) {
	if _, err := s.stmt.Exec(args); err != nil {
		return nil, err
	}
	if err := s.catalog.AddNewTableSpec(context.Background(), s.conn, s.spec); err != nil {
		return nil, fmt.Errorf("failed to add new table spec: %w", err)
	}
	return nil, nil
}

func (s *CreateViewStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("failed to query for CreateViewStmt")
}

type CreateFunctionStmt struct {
	conn    *Conn
	catalog *Catalog
	spec    *FunctionSpec
}

func (s *CreateFunctionStmt) Close() error {
	return nil
}

func (s *CreateFunctionStmt) NumInput() int {
	return 0
}

func (s *CreateFunctionStmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.catalog.AddNewFunctionSpec(context.Background(), s.conn, s.spec); err != nil {
		return nil, fmt.Errorf("failed to add new function spec: %w", err)
	}
	return nil, nil
}

func (s *CreateFunctionStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("failed to query for CreateFunctionStmt")
}

func newCreateFunctionStmt(conn *Conn, catalog *Catalog, spec *FunctionSpec) *CreateFunctionStmt {
	return &CreateFunctionStmt{
		conn:    conn,
		catalog: catalog,
		spec:    spec,
	}
}

type DMLStmt struct {
	stmt           *sql.Stmt
	args           []*ast.ParameterNode
	formattedQuery string
}

func newDMLStmt(stmt *sql.Stmt, args []*ast.ParameterNode, formattedQuery string) *DMLStmt {
	return &DMLStmt{
		stmt:           stmt,
		args:           args,
		formattedQuery: formattedQuery,
	}
}

func (s *DMLStmt) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}

func (s *DMLStmt) Close() error {
	return s.stmt.Close()
}

func (s *DMLStmt) NumInput() int {
	return len(s.args)
}

func (s *DMLStmt) Exec(args []driver.Value) (driver.Result, error) {
	values := make([]interface{}, 0, len(args))
	for _, arg := range args {
		values = append(values, arg)
	}
	newArgs, err := EncodeGoValues(values, s.args)
	if err != nil {
		return nil, err
	}
	result, err := s.stmt.Exec(newArgs...)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to execute query %s: args %v: %w",
			s.formattedQuery,
			newArgs,
			err,
		)
	}
	return result, nil
}

func (s *DMLStmt) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return nil, fmt.Errorf("unimplemented ExecContext for DMLStmt")
}

func (s *DMLStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("unsupported query for DMLStmt")
}

func (s *DMLStmt) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return nil, fmt.Errorf("unsupported query for DMLStmt")
}

type QueryStmt struct {
	stmt           *sql.Stmt
	args           []*ast.ParameterNode
	formattedQuery string
	outputColumns  []*ColumnSpec
}

func newQueryStmt(stmt *sql.Stmt, args []*ast.ParameterNode, formattedQuery string, outputColumns []*ColumnSpec) *QueryStmt {
	return &QueryStmt{
		stmt:           stmt,
		args:           args,
		formattedQuery: formattedQuery,
		outputColumns:  outputColumns,
	}
}

func (s *QueryStmt) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}

func (s *QueryStmt) Close() error {
	return s.stmt.Close()
}

func (s *QueryStmt) NumInput() int {
	return len(s.args)
}

func (s *QueryStmt) OutputColumns() []*ColumnSpec {
	return s.outputColumns
}

func (s *QueryStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("unsupported exec for QueryStmt")
}

func (s *QueryStmt) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return nil, fmt.Errorf("unsupported exec for QueryStmt")
}

func (s *QueryStmt) Query(args []driver.Value) (driver.Rows, error) {
	values := make([]interface{}, 0, len(args))
	for _, arg := range args {
		values = append(values, arg)
	}
	newArgs, err := EncodeGoValues(values, s.args)
	if err != nil {
		return nil, err
	}
	rows, err := s.stmt.Query(newArgs...)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to query %s: args: %v: %w",
			s.formattedQuery,
			newArgs,
			err,
		)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"failed to query %s: args: %v: %w",
			s.formattedQuery,
			newArgs,
			err,
		)
	}
	return &Rows{rows: rows, columns: s.outputColumns}, nil
}

func (s *QueryStmt) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return nil, fmt.Errorf("unimplemented QueryContext for QueryStmt")
}
