package zetasqlite

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	ast "github.com/glassmonkey/zetasql-wasm/resolved_ast"
	"github.com/glassmonkey/zetasql-wasm/types"
	"github.com/goccy/go-json"
)

type NameWithType struct {
	Name string `json:"name"`
	Type *Type  `json:"type"`
}

func (t *NameWithType) FunctionArgumentType() (*types.FunctionArgumentType, error) {
	if t.Type.SignatureKind != types.ArgTypeFixed {
		arg := types.NewTemplatedFunctionArgumentType(t.Type.SignatureKind)
		arg.Options = &types.FunctionArgumentTypeOptions{Cardinality: types.RequiredCardinality}
		return arg, nil
	}
	typ, err := t.Type.ToZetaSQLType()
	if err != nil {
		return nil, err
	}
	arg := types.NewFunctionArgumentType(typ)
	arg.Options = &types.FunctionArgumentTypeOptions{Cardinality: types.RequiredCardinality}
	// TODO(zetasql-wasm-migration): argument name (t.Name) is not currently
	// propagated; FunctionArgumentTypeOptions in zetasql-wasm only exposes
	// Cardinality. Add an ArgumentName field upstream when needed.
	_ = t.Name
	return arg, nil
}

type FunctionSpec struct {
	IsTemp    bool            `json:"isTemp"`
	NamePath  []string        `json:"name"`
	Language  string          `json:"language"`
	Args      []*NameWithType `json:"args"`
	Return    *Type           `json:"return"`
	Body      string          `json:"body"`
	Code      string          `json:"code"`
	UpdatedAt time.Time       `json:"updatedAt"`
	CreatedAt time.Time       `json:"createdAt"`
}

func (s *FunctionSpec) FuncName() string {
	return formatPath(s.NamePath)
}

func (s *FunctionSpec) SQL() string {
	args := []string{}
	for _, arg := range s.Args {
		t, _ := arg.Type.ToZetaSQLType()
		args = append(args, fmt.Sprintf("%s %s", arg.Name, t.Kind()))
	}
	retType, _ := s.Return.ToZetaSQLType()
	return fmt.Sprintf(
		"CREATE FUNCTION `%s`(%s) RETURNS %s AS (%s)",
		s.FuncName(),
		strings.Join(args, ", "),
		retType.Kind(),
		s.Body,
	)
}

func (s *FunctionSpec) CallSQL(ctx context.Context, callNode ast.BaseFunctionCall, argValues []string) (string, error) {
	args := callNode.ArgumentList()
	var body string
	if s.Body == "" {
		// templated argument func
		definedArgs := make([]string, 0, len(args))
		for idx, arg := range args {
			argType, err := types.TypeFromProto(ast.ExprType(arg))
			if err != nil {
				return "", err
			}
			typeName := newType(argType).FormatType()
			definedArgs = append(
				definedArgs,
				fmt.Sprintf("%s %s", s.Args[idx].Name, typeName),
			)
		}
		funcName := strings.Join(s.NamePath, ".")
		runtimeDefinedFunc := fmt.Sprintf(
			"CREATE FUNCTION `%s`(%s) as (%s)",
			funcName,
			strings.Join(definedArgs, ","),
			s.Code,
		)
		analyzer := analyzerFromContext(ctx)
		runtimeSpec, err := analyzer.analyzeTemplatedFunctionWithRuntimeArgument(ctx, runtimeDefinedFunc)
		if err != nil {
			return "", err
		}
		body = runtimeSpec.Body
	} else {
		body = s.Body
	}
	for i := 0; i < len(s.Args); i++ {
		argRef := fmt.Sprintf("@%s", s.Args[i].Name)
		value := argValues[i]
		body = strings.Replace(body, argRef, value, -1)
	}
	return fmt.Sprintf("( %s )", body), nil
}

type TableSpec struct {
	IsTemp     bool           `json:"isTemp"`
	IsView     bool           `json:"isView"`
	NamePath   []string       `json:"namePath"`
	Columns    []*ColumnSpec  `json:"columns"`
	PrimaryKey []string       `json:"primaryKey"`
	CreateMode ast.CreateMode `json:"createMode"`
	Query      string         `json:"query"`
	UpdatedAt  time.Time      `json:"updatedAt"`
	CreatedAt  time.Time      `json:"createdAt"`
}

func (s *TableSpec) Column(name string) *ColumnSpec {
	for _, col := range s.Columns {
		if col.Name == name {
			return col
		}
	}
	return nil
}

func (s *TableSpec) TableName() string {
	return formatPath(s.NamePath)
}

func (s *TableSpec) SQLiteSchema() string {
	if s.IsView {
		return viewSQLiteSchema(s)
	}
	if s.Query != "" {
		return fmt.Sprintf("CREATE TABLE `%s` AS %s", s.TableName(), s.Query)
	}
	columns := []string{}
	for _, c := range s.Columns {
		columns = append(columns, c.SQLiteSchema())
	}
	if len(s.PrimaryKey) != 0 {
		columns = append(
			columns,
			fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(s.PrimaryKey, ",")),
		)
	}
	var stmt string
	switch s.CreateMode {
	case ast.CreateDefaultMode:
		stmt = "CREATE TABLE"
	case ast.CreateOrReplaceMode:
		stmt = "CREATE TABLE"
	case ast.CreateIfNotExistsMode:
		stmt = "CREATE TABLE IF NOT EXISTS"
	}
	return fmt.Sprintf("%s `%s` (%s)", stmt, s.TableName(), strings.Join(columns, ","))
}

func viewSQLiteSchema(s *TableSpec) string {
	var stmt string
	switch s.CreateMode {
	case ast.CreateDefaultMode:
		stmt = "CREATE VIEW"
	case ast.CreateOrReplaceMode:
		stmt = "CREATE VIEW"
	case ast.CreateIfNotExistsMode:
		stmt = "CREATE VIEW IF NOT EXISTS"
	}
	return fmt.Sprintf("%s `%s` AS %s", stmt, s.TableName(), s.Query)
}

type ColumnSpec struct {
	Name      string `json:"name"`
	Type      *Type  `json:"type"`
	IsNotNull bool   `json:"isNotNull"`
}

type Type struct {
	Name          string                      `json:"name"`
	Kind          types.TypeKind              `json:"kind"`
	SignatureKind types.SignatureArgumentKind `json:"signatureKind"`
	ElementType   *Type                       `json:"elementType"`
	FieldTypes    []*NameWithType             `json:"fieldTypes"`
}

func (t *Type) FunctionArgumentType() (*types.FunctionArgumentType, error) {
	if t.SignatureKind != types.ArgTypeFixed {
		arg := types.NewTemplatedFunctionArgumentType(t.SignatureKind)
		arg.Options = &types.FunctionArgumentTypeOptions{Cardinality: types.RequiredCardinality}
		return arg, nil
	}
	typ, err := t.ToZetaSQLType()
	if err != nil {
		return nil, err
	}
	arg := types.NewFunctionArgumentType(typ)
	arg.Options = &types.FunctionArgumentTypeOptions{Cardinality: types.RequiredCardinality}
	return arg, nil
}

func (t *Type) IsArray() bool {
	return t.Kind == types.Array
}

func (t *Type) IsStruct() bool {
	return t.Kind == types.Struct
}

func (t *Type) AvailableAutoIndex() bool {
	switch t.Kind {
	case types.Bytes, types.Json, types.Array, types.Struct,
		types.Geography, types.Proto, types.Extended:
		return false
	}
	return true
}

func (t *Type) GoReflectType() (reflect.Type, error) {
	switch t.Kind {
	case types.Int32, types.Int64, types.Uint32, types.Uint64:
		return reflect.TypeOf(int64(0)), nil
	case types.Bool:
		return reflect.TypeOf(false), nil
	case types.Float, types.Double:
		return reflect.TypeOf(float64(0)), nil
	case types.Bytes, types.String, types.Numeric, types.BigNumeric,
		types.Date, types.Datetime, types.Time, types.Timestamp, types.Interval, types.Json:
		return reflect.TypeOf(""), nil
	case types.Array:
		elem, err := t.ElementType.GoReflectType()
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elem), nil
	case types.Struct:
		return reflect.TypeOf(map[string]interface{}{}), nil
	}
	return nil, fmt.Errorf("cannot convert %s to reflect.Type", t.Name)
}

func (t *Type) ToZetaSQLType() (types.Type, error) {
	switch types.TypeKind(t.Kind) {
	case types.Array:
		typ, err := t.ElementType.ToZetaSQLType()
		if err != nil {
			return nil, err
		}
		return types.NewArrayType(typ)
	case types.Struct:
		var fields []*types.StructField
		for _, field := range t.FieldTypes {
			typ, err := field.Type.ToZetaSQLType()
			if err != nil {
				return nil, err
			}
			fields = append(fields, types.NewStructField(field.Name, typ))
		}
		return types.NewStructType(fields)
	}
	return types.TypeFromKind(t.Kind), nil
}

func (t *Type) FormatType() string {
	switch t.Kind {
	case types.Struct:
		formatTypes := make([]string, 0, len(t.FieldTypes))
		for _, field := range t.FieldTypes {
			formatTypes = append(formatTypes, fmt.Sprintf("`%s` %s", field.Name, field.Type.FormatType()))
		}
		return fmt.Sprintf("STRUCT<%s>", strings.Join(formatTypes, ","))
	case types.Array:
		return fmt.Sprintf("ARRAY<%s>", t.ElementType.FormatType())
	}
	return t.Kind.String()
}

func (s *ColumnSpec) SQLiteSchema() string {
	var typ string
	switch s.Type.Kind {
	case types.Int32, types.Int64, types.Uint32, types.Uint64:
		typ = "INT"
	case types.Enum:
		typ = "INT"
	case types.Bool:
		typ = "BOOLEAN"
	case types.Float:
		typ = "FLOAT"
	case types.Bytes:
		typ = "BLOB"
	case types.Double:
		typ = "DOUBLE"
	case types.Json:
		typ = "JSON"
	case types.String:
		typ = "TEXT"
	case types.Date:
		typ = "TEXT"
	case types.Timestamp:
		typ = "TEXT"
	case types.Array:
		typ = "TEXT"
	case types.Struct:
		typ = "TEXT"
	case types.Proto:
		typ = "TEXT"
	case types.Time:
		typ = "TEXT"
	case types.Datetime:
		typ = "TEXT"
	case types.Geography:
		typ = "TEXT"
	case types.Numeric:
		typ = "TEXT"
	case types.BigNumeric:
		typ = "TEXT"
	case types.Extended:
		typ = "TEXT"
	case types.Interval:
		typ = "TEXT"
	default:
		typ = "UNKNOWN"
	}
	schema := fmt.Sprintf("`%s` %s", s.Name, typ)
	if s.IsNotNull {
		schema += " NOT NULL"
	}
	return schema
}

func newTypeFromFunctionArgumentType(t *types.FunctionArgumentType) *Type {
	if t.Kind != types.ArgTypeFixed {
		return &Type{SignatureKind: t.Kind}
	}
	return newType(t.Type)
}

func newFunctionSpec(ctx context.Context, namePath *NamePath, stmt *ast.CreateFunctionStmtNode) (*FunctionSpec, error) {
	args := []*NameWithType{}
	signature := stmt.Signature()
	for _, arg := range signature.Arguments {
		args = append(args, &NameWithType{
			Name: argumentName(arg),
			Type: newTypeFromArgumentType(arg),
		})
	}

	returnType, err := types.TypeFromProto(stmt.ReturnType())
	if err != nil {
		return nil, err
	}
	var body string
	language := stmt.Language()
	switch language {
	case "js":
		code, err := EncodeGoValue(types.StringType(), stmt.Code())
		if err != nil {
			return nil, err
		}
		encodedType, err := json.Marshal(newType(returnType))
		if err != nil {
			return nil, err
		}
		retType, err := EncodeGoValue(types.StringType(), string(encodedType))
		if err != nil {
			return nil, err
		}
		argParams := make([]string, 0, len(args))
		argNames := make([]string, 0, len(args))
		for _, arg := range args {
			argParams = append(argParams, fmt.Sprintf("@%s", arg.Name))
			argNames = append(argNames, arg.Name)
		}
		if len(argParams) == 0 {
			body = fmt.Sprintf("zetasqlite_eval_javascript('%s', '%s')", code, retType)
		} else {
			stringArrayType, err := types.NewArrayType(types.StringType())
			if err != nil {
				return nil, err
			}
			arr, err := EncodeGoValue(stringArrayType, argNames)
			if err != nil {
				return nil, err
			}
			body = fmt.Sprintf(
				"zetasqlite_eval_javascript('%s', '%s', '%s', %s)",
				code, retType, arr,
				strings.Join(argParams, ","),
			)
		}
	default:
		funcExpr := stmt.FunctionExpression()
		if funcExpr != nil {
			bodyQuery, err := newNode(funcExpr).FormatSQL(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to format function expression: %w", err)
			}
			body = bodyQuery
		}
	}
	now := time.Now()
	return &FunctionSpec{
		IsTemp:    stmt.CreateScope() == ast.CreateTempScope,
		NamePath:  namePath.mergePath(stmt.NamePath()),
		Args:      args,
		Return:    newType(returnType),
		Code:      stmt.Code(),
		Body:      body,
		Language:  language,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// newTypeFromArgumentType rebuilds the fork's *Type from a wrapped
// FunctionArgumentType, treating templated kinds as the wrapped
// SignatureArgumentKind and fixed kinds as the resolved Type.
func newTypeFromArgumentType(arg *types.FunctionArgumentType) *Type {
	if arg.Kind != types.ArgTypeFixed {
		return &Type{SignatureKind: arg.Kind}
	}
	return newType(arg.Type)
}

// newTypeFromArgumentTypeByRealType reflects the realised type's shape
// (array vs scalar) into the templated SignatureArgumentKind; for fixed
// arguments it just unwraps the wrapped argument type.
func newTypeFromArgumentTypeByRealType(arg *types.FunctionArgumentType, realType types.Type) *Type {
	if arg.Kind != types.ArgTypeFixed {
		if realType != nil && realType.IsArray() {
			return &Type{SignatureKind: types.ArgArrayTypeAny1}
		}
		return &Type{SignatureKind: types.ArgTypeAny1}
	}
	return newType(arg.Type)
}

// argumentName returns the declared parameter name for a function argument,
// or "" when no Options block is attached.
func argumentName(arg *types.FunctionArgumentType) string {
	if arg.Options == nil {
		return ""
	}
	return arg.Options.ArgumentName
}

func newTemplatedFunctionSpec(ctx context.Context, namePath *NamePath, stmt *ast.CreateFunctionStmtNode, realStmts []*ast.CreateFunctionStmtNode) (*FunctionSpec, error) {
	signature := stmt.Signature()
	arguments := signature.Arguments
	realStmt := realStmts[0]
	realSignature := realStmt.Signature()
	realArguments := realSignature.Arguments
	resultType := newType(realSignature.ReturnType.Type)
	resultTypeName := resultType.FormatType()

	allSameResultType := true
	for _, stmt := range realStmts {
		t := stmt.Signature().ReturnType.Type
		if newType(t).FormatType() != resultTypeName {
			allSameResultType = false
			break
		}
	}
	var retType *Type
	if allSameResultType {
		retType = resultType
	} else {
		retType = newTypeFromArgumentTypeByRealType(
			signature.ReturnType,
			realSignature.ReturnType.Type,
		)
	}
	args := []*NameWithType{}
	for i := 0; i < len(arguments); i++ {
		args = append(args, &NameWithType{
			Name: argumentName(arguments[i]),
			Type: newTypeFromArgumentTypeByRealType(arguments[i], realArguments[i].Type),
		})
	}
	funcExpr := stmt.FunctionExpression()
	var body string
	if funcExpr != nil {
		bodyQuery, err := newNode(funcExpr).FormatSQL(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to format function expression: %w", err)
		}
		body = bodyQuery
	}
	now := time.Now()
	return &FunctionSpec{
		IsTemp:    stmt.CreateScope() == ast.CreateTempScope,
		NamePath:  namePath.mergePath(stmt.NamePath()),
		Args:      args,
		Return:    retType,
		Code:      stmt.Code(),
		Body:      body,
		Language:  stmt.Language(),
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func newColumnsFromDef(def []*ast.ColumnDefinitionNode) ([]*ColumnSpec, error) {
	columns := []*ColumnSpec{}
	for _, columnNode := range def {
		annotation := columnNode.Annotations()
		var isNotNull bool
		if annotation != nil {
			params := annotation.TypeParameters()
			if params != nil {
				//TODO: get type param from params
				_ = params
			}
			isNotNull = annotation.NotNull()
		}
		colType, err := types.TypeFromProto(columnNode.Type())
		if err != nil {
			return nil, err
		}
		columns = append(columns, &ColumnSpec{
			Name:      columnNode.Name(),
			Type:      newType(colType),
			IsNotNull: isNotNull,
		})
	}
	return columns, nil
}

func newColumnsFromOutputColumns(def []*ast.OutputColumnNode) ([]*ColumnSpec, error) {
	columns := []*ColumnSpec{}
	for _, columnNode := range def {
		column := columnNode.Column()
		colType, err := types.TypeFromProto(column.Type)
		if err != nil {
			return nil, err
		}
		columns = append(columns, &ColumnSpec{
			Name: columnNode.Name(),
			Type: newType(colType),
		})
	}
	return columns, nil
}

func newPrimaryKey(key *ast.PrimaryKeyNode) []string {
	if key == nil {
		return nil
	}
	return key.ColumnNameList()
}

func newTableSpec(namePath *NamePath, stmt *ast.CreateTableStmtNode) (*TableSpec, error) {
	now := time.Now()
	cols, err := newColumnsFromDef(stmt.ColumnDefinitionList())
	if err != nil {
		return nil, err
	}
	return &TableSpec{
		IsTemp:     stmt.CreateScope() == ast.CreateTempScope,
		NamePath:   namePath.mergePath(stmt.NamePath()),
		Columns:    cols,
		PrimaryKey: newPrimaryKey(stmt.PrimaryKey()),
		CreateMode: stmt.CreateMode(),
		UpdatedAt:  now,
		CreatedAt:  now,
	}, nil
}

func newTableAsViewSpec(namePath *NamePath, query string, stmt *ast.CreateViewStmtNode) (*TableSpec, error) {
	var outputColumns []string
	for _, column := range stmt.OutputColumnList() {
		colName := column.Name()
		refColumnName := column.Column().Name
		colID := column.Column().ID
		outputColumns = append(
			outputColumns,
			fmt.Sprintf("`%s#%d` AS `%s`", refColumnName, colID, colName),
		)
	}
	cols, err := newColumnsFromOutputColumns(stmt.OutputColumnList())
	if err != nil {
		return nil, err
	}
	now := time.Now()
	return &TableSpec{
		IsTemp:     stmt.CreateScope() == ast.CreateTempScope,
		IsView:     true,
		NamePath:   namePath.mergePath(stmt.NamePath()),
		Columns:    cols,
		CreateMode: stmt.CreateMode(),
		Query:      fmt.Sprintf("SELECT %s FROM (%s)", strings.Join(outputColumns, ","), query),
		UpdatedAt:  now,
		CreatedAt:  now,
	}, nil
}

func newTableAsSelectSpec(namePath *NamePath, query string, stmt *ast.CreateTableAsSelectStmtNode) (*TableSpec, error) {
	var outputColumns []string
	for _, column := range stmt.OutputColumnList() {
		colName := column.Name()
		refColumnName := column.Column().Name
		colID := column.Column().ID
		outputColumns = append(
			outputColumns,
			fmt.Sprintf("`%s#%d` AS `%s`", refColumnName, colID, colName),
		)
	}
	cols, err := newColumnsFromDef(stmt.ColumnDefinitionList())
	if err != nil {
		return nil, err
	}
	now := time.Now()
	return &TableSpec{
		IsTemp:     stmt.CreateScope() == ast.CreateTempScope,
		NamePath:   namePath.mergePath(stmt.NamePath()),
		Columns:    cols,
		PrimaryKey: newPrimaryKey(stmt.PrimaryKey()),
		CreateMode: stmt.CreateMode(),
		Query:      fmt.Sprintf("SELECT %s FROM (%s)", strings.Join(outputColumns, ","), query),
		UpdatedAt:  now,
		CreatedAt:  now,
	}, nil
}

func newType(t types.Type) *Type {
	if t == nil {
		return nil
	}
	kind := t.Kind()
	var (
		elem       *Type
		fieldTypes []*NameWithType
	)
	switch kind {
	case types.Array:
		elem = newType(t.AsArray().ElementType)
	case types.Struct:
		for _, field := range t.AsStruct().Fields {
			fieldTypes = append(fieldTypes, &NameWithType{
				Name: field.Name,
				Type: newType(field.Type),
			})
		}
	}
	return &Type{
		// TODO(zetasql-wasm-migration): types.Type doesn't expose a
		// TypeName(productMode) method (go-zetasql gave a "STRING" /
		// "ARRAY<INT64>" form). KindString returns the proto-name
		// "TYPE_STRING"; the proper name reconstruction lives on the
		// fork's *Type.FormatType, so this Name field is only used as
		// a debugging tag for now.
		Name:        kind.String(),
		Kind:        kind,
		ElementType: elem,
		FieldTypes:  fieldTypes,
	}
}
