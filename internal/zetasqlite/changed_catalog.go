package zetasqlite

import (
	"database/sql"
	"fmt"
	"reflect"
)

// Type aliases that the upstream go-zetasqlite root package re-exported are
// now no-ops because everything lives in the same package — the impl types
// (ChangedCatalog, TableSpec, FunctionSpec, NameWithType, ColumnSpec, Type)
// are already visible. Only the *FromRows / *FromResult helpers, which use
// unsafe.Pointer to peek inside database/sql internals, stay here.

// ChangedCatalogFromRows retrieve modified catalog information from sql.Rows.
// NOTE: This API relies on the internal structure of sql.Rows, so not will work for all Go versions.
func ChangedCatalogFromRows(rows *sql.Rows) (*ChangedCatalog, error) {
	if rows == nil {
		return nil, fmt.Errorf("zetasqlite: sql.Rows instance required not nil")
	}
	rv := reflect.ValueOf(rows)
	rowsi := rv.Elem().FieldByName("rowsi")
	if !rowsi.IsValid() {
		return nil, fmt.Errorf("zetasqlite: unexpected sql.Rows layout")
	}
	driverValue := rowsi.Elem()
	if driverValue.Type() != reflect.TypeOf(new(Rows)) {
		return nil, fmt.Errorf("zetasqlite: sql.Rows must be an instance created using the zetasqlite database driver")
	}
	zetasqliteRows := (*Rows)(driverValue.UnsafePointer())
	return zetasqliteRows.ChangedCatalog(), nil
}

// ChangedCatalogFromResult retrieve modified catalog information from sql.Result.
// NOTE: This API relies on the internal structure of sql.Result, so not will work for all Go versions.
func ChangedCatalogFromResult(result sql.Result) (*ChangedCatalog, error) {
	rv := reflect.ValueOf(result)
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("zetasqlite: unexpected sql.Result layout. expected sql.Result type is struct but got %T", result)
	}
	resi := rv.FieldByName("resi")
	if !resi.IsValid() {
		return nil, fmt.Errorf("zetasqlite: unexpected sql.Result layout")
	}
	driverValue := resi.Elem()
	if driverValue.Type() != reflect.TypeOf(new(Result)) {
		return nil, fmt.Errorf("zetasqlite: sql.Result must be an instance created using the zetasqlite database driver")
	}
	zetasqliteResult := (*Result)(driverValue.UnsafePointer())
	return zetasqliteResult.ChangedCatalog(), nil
}
