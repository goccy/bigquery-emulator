package internal

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/goccy/go-zetasql/types"
)

var (
	createCatalogTableQuery = `
CREATE TABLE IF NOT EXISTS zetasqlite_catalog(
  name STRING NOT NULL PRIMARY KEY,
  kind STRING NOT NULL,
  spec STRING NOT NULL,
  updatedAt TIMESTAMP NOT NULL,
  createdAt TIMESTAMP NOT NULL
)
`
	upsertCatalogQuery = `
INSERT INTO zetasqlite_catalog (
  name,
  kind,
  spec,
  updatedAt,
  createdAt
) VALUES (
  @name,
  @kind,
  @spec,
  @updatedAt,
  @createdAt
) ON CONFLICT(name) DO UPDATE SET
  spec = @spec,
  updatedAt = @updatedAt
`
	deleteCatalogQuery = `
DELETE FROM zetasqlite_catalog WHERE name = @name
`
)

type CatalogSpecKind string

const (
	TableSpecKind    CatalogSpecKind = "table"
	ViewSpecKind     CatalogSpecKind = "view"
	FunctionSpecKind CatalogSpecKind = "function"
	catalogName                      = "zetasqlite"
)

type Catalog struct {
	db           *sql.DB
	lastSyncedAt time.Time
	mu           sync.Mutex
	tables       []*TableSpec
	functions    []*FunctionSpec
	catalog      *types.SimpleCatalog
	tableMap     map[string]*TableSpec
	funcMap      map[string]*FunctionSpec
}

func newSimpleCatalog(name string) *types.SimpleCatalog {
	catalog := types.NewSimpleCatalog(name)
	catalog.AddZetaSQLBuiltinFunctions(nil)
	return catalog
}

func NewCatalog(db *sql.DB) *Catalog {
	return &Catalog{
		db:       db,
		catalog:  newSimpleCatalog(catalogName),
		tableMap: map[string]*TableSpec{},
		funcMap:  map[string]*FunctionSpec{},
	}
}

func (c *Catalog) FullName() string {
	return c.catalog.FullName()
}

func (c *Catalog) FindTable(path []string) (types.Table, error) {
	if c.isWildcardTable(path) {
		return c.createWildcardTable(path)
	}
	return c.catalog.FindTable(path)
}

func (c *Catalog) FindModel(path []string) (types.Model, error) {
	return c.catalog.FindModel(path)
}

func (c *Catalog) FindConnection(path []string) (types.Connection, error) {
	return c.catalog.FindConnection(path)
}

func (c *Catalog) FindFunction(path []string) (*types.Function, error) {
	return c.catalog.FindFunction(path)
}

func (c *Catalog) FindTableValuedFunction(path []string) (types.TableValuedFunction, error) {
	return c.catalog.FindTableValuedFunction(path)
}

func (c *Catalog) FindProcedure(path []string) (*types.Procedure, error) {
	return c.catalog.FindProcedure(path)
}

func (c *Catalog) FindType(path []string) (types.Type, error) {
	return c.catalog.FindType(path)
}

func (c *Catalog) FindConstant(path []string) (types.Constant, int, error) {
	return c.catalog.FindConstant(path)
}

func (c *Catalog) FindConversion(from, to types.Type) (types.Conversion, error) {
	return c.catalog.FindConversion(from, to)
}

func (c *Catalog) ExtendedTypeSuperTypes(typ types.Type) (*types.TypeListView, error) {
	return c.catalog.ExtendedTypeSuperTypes(typ)
}

func (c *Catalog) SuggestTable(mistypedPath []string) string {
	return c.catalog.SuggestTable(mistypedPath)
}

func (c *Catalog) SuggestModel(mistypedPath []string) string {
	return c.catalog.SuggestModel(mistypedPath)
}

func (c *Catalog) SuggestFunction(mistypedPath []string) string {
	return c.catalog.SuggestFunction(mistypedPath)
}

func (c *Catalog) SuggestTableValuedFunction(mistypedPath []string) string {
	return c.catalog.SuggestTableValuedFunction(mistypedPath)
}

func (c *Catalog) SuggestConstant(mistypedPath []string) string {
	return c.catalog.SuggestConstant(mistypedPath)
}

func (c *Catalog) formatNamePath(path []string) string {
	return strings.Join(path, "_")
}

func (c *Catalog) getFunctions(namePath *NamePath) []*FunctionSpec {
	if namePath.empty() {
		return c.functions
	}
	key := c.formatNamePath(namePath.path)
	specs := make([]*FunctionSpec, 0, len(c.functions))
	for _, fn := range c.functions {
		if len(fn.NamePath) == 1 {
			// function name only
			specs = append(specs, fn)
			continue
		}
		pathPrefixKey := c.formatNamePath(c.trimmedLastPath(fn.NamePath))
		if strings.Contains(pathPrefixKey, key) {
			specs = append(specs, fn)
		}
	}
	return specs
}

func (c *Catalog) Sync(ctx context.Context, conn *Conn) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.createCatalogTablesIfNotExists(ctx, conn); err != nil {
		return fmt.Errorf("failed to create catalog tables: %w", err)
	}
	now := time.Now()
	rows, err := conn.QueryContext(
		ctx,
		`SELECT name, kind, spec FROM zetasqlite_catalog WHERE updatedAt >= @lastUpdatedAt`,
		c.lastSyncedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to query load catalog: %w", err)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			name string
			kind CatalogSpecKind
			spec string
		)
		if err := rows.Scan(&name, &kind, &spec); err != nil {
			return fmt.Errorf("failed to scan catalog values: %w", err)
		}
		switch kind {
		case TableSpecKind, ViewSpecKind:
			if err := c.loadTableSpec(spec); err != nil {
				return fmt.Errorf("failed to load table spec: %w", err)
			}
		case FunctionSpecKind:
			if err := c.loadFunctionSpec(spec); err != nil {
				return fmt.Errorf("failed to load function spec: %w", err)
			}
		default:
			return fmt.Errorf("unknown catalog spec kind %s", kind)
		}
	}
	c.lastSyncedAt = now
	return nil
}

func (c *Catalog) AddNewTableSpec(ctx context.Context, conn *Conn, spec *TableSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.addTableSpec(spec); err != nil {
		return err
	}
	if !spec.IsTemp {
		if err := c.saveTableSpec(ctx, conn, spec); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) AddNewFunctionSpec(ctx context.Context, conn *Conn, spec *FunctionSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.addFunctionSpec(spec); err != nil {
		return err
	}
	if !spec.IsTemp {
		if err := c.saveFunctionSpec(ctx, conn, spec); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) DeleteTableSpec(ctx context.Context, conn *Conn, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.deleteTableSpecByName(name); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, deleteCatalogQuery, sql.Named("name", name)); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) DeleteFunctionSpec(ctx context.Context, conn *Conn, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.deleteFunctionSpecByName(name); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, deleteCatalogQuery, sql.Named("name", name)); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) deleteTableSpecByName(name string) error {
	spec, exists := c.tableMap[name]
	if !exists {
		return fmt.Errorf("failed to find table spec from map by %s", name)
	}
	tables := make([]*TableSpec, 0, len(c.tables))
	specName := c.formatNamePath(spec.NamePath)
	for _, table := range c.tables {
		if specName == c.formatNamePath(table.NamePath) {
			continue
		}
		tables = append(tables, table)
	}
	if err := c.resetCatalog(tables, c.functions); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) deleteFunctionSpecByName(name string) error {
	spec, exists := c.funcMap[name]
	if !exists {
		return fmt.Errorf("failed to find function spec from map by %s", name)
	}
	functions := make([]*FunctionSpec, 0, len(c.functions))
	specName := c.formatNamePath(spec.NamePath)
	for _, function := range c.functions {
		if specName == c.formatNamePath(function.NamePath) {
			continue
		}
		functions = append(functions, function)
	}
	if err := c.resetCatalog(c.tables, functions); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) resetCatalog(tables []*TableSpec, functions []*FunctionSpec) error {
	c.catalog = newSimpleCatalog(catalogName)
	c.tables = []*TableSpec{}
	c.functions = []*FunctionSpec{}
	c.tableMap = map[string]*TableSpec{}
	c.funcMap = map[string]*FunctionSpec{}
	for _, spec := range tables {
		if err := c.addTableSpec(spec); err != nil {
			return err
		}
	}
	for _, spec := range functions {
		if err := c.addFunctionSpec(spec); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) saveTableSpec(ctx context.Context, conn *Conn, spec *TableSpec) error {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to encode table spec: %w", err)
	}
	now := time.Now()
	kind := string(TableSpecKind)
	if spec.IsView {
		kind = string(ViewSpecKind)
	}
	if _, err := conn.ExecContext(
		ctx,
		upsertCatalogQuery,
		sql.Named("name", spec.TableName()),
		sql.Named("kind", kind),
		sql.Named("spec", string(encoded)),
		sql.Named("updatedAt", now),
		sql.Named("createdAt", now),
	); err != nil {
		return fmt.Errorf("failed to save a new table spec: %w", err)
	}
	return nil
}

func (c *Catalog) saveFunctionSpec(ctx context.Context, conn *Conn, spec *FunctionSpec) error {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to encode function spec: %w", err)
	}
	now := time.Now()
	if _, err := conn.ExecContext(
		ctx,
		upsertCatalogQuery,
		sql.Named("name", spec.FuncName()),
		sql.Named("kind", string(FunctionSpecKind)),
		sql.Named("spec", string(encoded)),
		sql.Named("updatedAt", now),
		sql.Named("createdAt", now),
	); err != nil {
		return fmt.Errorf("failed to save a new function spec: %w", err)
	}
	return nil
}

func (c *Catalog) createCatalogTablesIfNotExists(ctx context.Context, conn *Conn) error {
	if _, err := conn.ExecContext(ctx, createCatalogTableQuery); err != nil {
		return fmt.Errorf("failed to create catalog table: %w", err)
	}
	return nil
}

func (c *Catalog) loadTableSpec(spec string) error {
	var v TableSpec
	if err := json.Unmarshal([]byte(spec), &v); err != nil {
		return fmt.Errorf("failed to decode table spec: %w", err)
	}
	if err := c.addTableSpec(&v); err != nil {
		return fmt.Errorf("failed to add table spec to catalog: %w", err)
	}
	return nil
}

func (c *Catalog) loadFunctionSpec(spec string) error {
	var v FunctionSpec
	if err := json.Unmarshal([]byte(spec), &v); err != nil {
		return fmt.Errorf("failed to decode function spec: %w", err)
	}
	if err := c.addFunctionSpec(&v); err != nil {
		return fmt.Errorf("failed to add function spec to catalog: %w", err)
	}
	return nil
}

func (c *Catalog) trimmedLastPath(path []string) []string {
	if len(path) == 0 {
		return path
	}
	return path[:len(path)-1]
}

func (c *Catalog) addFunctionSpec(spec *FunctionSpec) error {
	funcName := spec.FuncName()
	if _, exists := c.funcMap[funcName]; exists {
		c.funcMap[funcName] = spec // update current spec
		return nil
	}
	c.functions = append(c.functions, spec)
	c.funcMap[funcName] = spec
	if err := c.addFunctionSpecRecursive(c.catalog, spec); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) addTableSpec(spec *TableSpec) error {
	tableName := spec.TableName()
	if _, exists := c.tableMap[tableName]; exists {
		// Table already exists, need to update the SimpleCatalog as well
		// First, update the spec in the map
		c.tableMap[tableName] = spec

		// Update the spec in the tables slice
		for i, t := range c.tables {
			if t.TableName() == tableName {
				c.tables[i] = spec
				break
			}
		}

		// Rebuild the catalog to reflect the updated schema
		if err := c.resetCatalog(c.tables, c.functions); err != nil {
			return err
		}
		return nil
	}
	c.tables = append(c.tables, spec)
	c.tableMap[tableName] = spec
	if err := c.addTableSpecRecursive(c.catalog, spec); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) addTableSpecRecursive(cat *types.SimpleCatalog, spec *TableSpec) error {
	if len(spec.NamePath) > 1 {
		subCatalogName := spec.NamePath[0]
		subCatalog, _ := cat.Catalog(subCatalogName)
		if subCatalog == nil {
			subCatalog = newSimpleCatalog(subCatalogName)
			cat.AddCatalog(subCatalog)
		}
		fullTableName := strings.Join(spec.NamePath, ".")
		if !c.existsTable(cat, fullTableName) {
			table, err := c.createSimpleTable(fullTableName, spec)
			if err != nil {
				return err
			}
			cat.AddTable(table)
		}
		newNamePath := spec.NamePath[1:]
		// add sub catalog to root catalog
		if err := c.addTableSpecRecursive(cat, c.copyTableSpec(spec, newNamePath)); err != nil {
			return fmt.Errorf("failed to add table spec to root catalog: %w", err)
		}
		// add sub catalog to parent catalog
		if err := c.addTableSpecRecursive(subCatalog, c.copyTableSpec(spec, newNamePath)); err != nil {
			return fmt.Errorf("failed to add table spec to parent catalog: %w", err)
		}
		return nil
	}
	if len(spec.NamePath) == 0 {
		return fmt.Errorf("table name is not found")
	}

	tableName := spec.NamePath[0]
	if c.existsTable(cat, tableName) {
		return nil
	}
	table, err := c.createSimpleTable(tableName, spec)
	if err != nil {
		return err
	}
	cat.AddTable(table)
	return nil
}

func (c *Catalog) createSimpleTable(tableName string, spec *TableSpec) (*types.SimpleTable, error) {
	columns := []types.Column{}
	for _, column := range spec.Columns {
		typ, err := column.Type.ToZetaSQLType()
		if err != nil {
			return nil, err
		}
		columns = append(columns, types.NewSimpleColumn(
			tableName, column.Name, typ,
		))
	}
	return types.NewSimpleTable(tableName, columns), nil
}

func (c *Catalog) addFunctionSpecRecursive(cat *types.SimpleCatalog, spec *FunctionSpec) error {
	if len(spec.NamePath) > 1 {
		subCatalogName := spec.NamePath[0]
		subCatalog, _ := cat.Catalog(subCatalogName)
		if subCatalog == nil {
			subCatalog = newSimpleCatalog(subCatalogName)
			cat.AddCatalog(subCatalog)
		}
		newNamePath := spec.NamePath[1:]
		// add sub catalog to root catalog
		if err := c.addFunctionSpecRecursive(cat, c.copyFunctionSpec(spec, newNamePath)); err != nil {
			return fmt.Errorf("failed to add function spec to root catalog: %w", err)
		}
		// add sub catalog to parent catalog
		if err := c.addFunctionSpecRecursive(subCatalog, c.copyFunctionSpec(spec, newNamePath)); err != nil {
			return fmt.Errorf("failed to add function spec to parent catalog: %w", err)
		}
		return nil
	}
	if len(spec.NamePath) == 0 {
		return fmt.Errorf("function name is not found")
	}

	funcName := spec.NamePath[0]
	if c.existsFunction(cat, funcName) {
		return nil
	}
	argTypes := []*types.FunctionArgumentType{}
	for _, arg := range spec.Args {
		argType, err := arg.FunctionArgumentType()
		if err != nil {
			return err
		}
		argTypes = append(argTypes, argType)
	}
	retType, err := spec.Return.FunctionArgumentType()
	if err != nil {
		return err
	}
	sig := types.NewFunctionSignature(retType, argTypes)
	newFunc := types.NewFunction([]string{funcName}, "", types.ScalarMode, []*types.FunctionSignature{sig})
	cat.AddFunction(newFunc)
	return nil
}

func (c *Catalog) existsTable(cat *types.SimpleCatalog, name string) bool {
	foundTable, _ := cat.FindTable([]string{name})
	return !c.isNilTable(foundTable)
}

func (c *Catalog) existsFunction(cat *types.SimpleCatalog, name string) bool {
	foundFunc, _ := cat.FindFunction([]string{name})
	return foundFunc != nil
}

func (c *Catalog) isNilTable(t types.Table) bool {
	v := reflect.ValueOf(t)
	if !v.IsValid() {
		return true
	}
	return v.IsNil()
}

func (c *Catalog) copyTableSpec(spec *TableSpec, newNamePath []string) *TableSpec {
	return &TableSpec{
		NamePath:   newNamePath,
		Columns:    spec.Columns,
		CreateMode: spec.CreateMode,
	}
}

func (c *Catalog) copyFunctionSpec(spec *FunctionSpec, newNamePath []string) *FunctionSpec {
	return &FunctionSpec{
		NamePath: newNamePath,
		Language: spec.Language,
		Args:     spec.Args,
		Return:   spec.Return,
		Code:     spec.Code,
		Body:     spec.Body,
	}
}
