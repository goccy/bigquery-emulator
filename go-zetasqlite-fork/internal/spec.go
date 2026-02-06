package internal

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/goccy/go-json"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

type NameWithType struct {
	Name string `json:"name"`
	Type *Type  `json:"type"`
}

func (t *NameWithType) FunctionArgumentType() (*types.FunctionArgumentType, error) {
	if t.Type.SignatureKind != types.ArgTypeFixed {
		return types.NewTemplatedFunctionArgumentType(
			t.Type.SignatureKind,
			types.NewFunctionArgumentTypeOptions(types.RequiredArgumentCardinality),
		), nil
	}
	typ, err := t.Type.ToZetaSQLType()
	if err != nil {
		return nil, err
	}
	opt := types.NewFunctionArgumentTypeOptions(types.RequiredArgumentCardinality)
	opt.SetArgumentName(t.Name)
	return types.NewFunctionArgumentType(typ, opt), nil
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

func (s *FunctionSpec) CallSQL(ctx context.Context, callNode *ast.BaseFunctionCallNode, argValues []string) (string, error) {
	args := callNode.ArgumentList()
	var body string
	if s.Body == "" {
		// templated argument func
		definedArgs := make([]string, 0, len(args))
		for idx, arg := range args {
			typeName := newType(arg.Type()).FormatType()
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
	Kind          int                         `json:"kind"`
	SignatureKind types.SignatureArgumentKind `json:"signatureKind"`
	ElementType   *Type                       `json:"elementType"`
	FieldTypes    []*NameWithType             `json:"fieldTypes"`
}

func (t *Type) FunctionArgumentType() (*types.FunctionArgumentType, error) {
	if t.SignatureKind != types.ArgTypeFixed {
		return types.NewTemplatedFunctionArgumentType(
			t.SignatureKind,
			types.NewFunctionArgumentTypeOptions(types.RequiredArgumentCardinality),
		), nil
	}
	typ, err := t.ToZetaSQLType()
	if err != nil {
		return nil, err
	}
	opt := types.NewFunctionArgumentTypeOptions(types.RequiredArgumentCardinality)
	return types.NewFunctionArgumentType(typ, opt), nil
}

func (t *Type) IsArray() bool {
	return t.Kind == types.ARRAY
}

func (t *Type) IsStruct() bool {
	return t.Kind == types.STRUCT
}

func (t *Type) AvailableAutoIndex() bool {
	switch t.Kind {
	case types.BYTES, types.JSON, types.ARRAY, types.STRUCT,
		types.GEOGRAPHY, types.PROTO, types.EXTENDED:
		return false
	}
	return true
}

func (t *Type) GoReflectType() (reflect.Type, error) {
	switch t.Kind {
	case types.INT32, types.INT64, types.UINT32, types.UINT64:
		return reflect.TypeOf(int64(0)), nil
	case types.BOOL:
		return reflect.TypeOf(false), nil
	case types.FLOAT, types.DOUBLE:
		return reflect.TypeOf(float64(0)), nil
	case types.BYTES, types.STRING, types.NUMERIC, types.BIG_NUMERIC,
		types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP, types.INTERVAL, types.JSON:
		return reflect.TypeOf(""), nil
	case types.ARRAY:
		elem, err := t.ElementType.GoReflectType()
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elem), nil
	case types.STRUCT:
		return reflect.TypeOf(map[string]interface{}{}), nil
	}
	return nil, fmt.Errorf("cannot convert %s to reflect.Type", t.Name)
}

func (t *Type) ToZetaSQLType() (types.Type, error) {
	switch types.TypeKind(t.Kind) {
	case types.ARRAY:
		typ, err := t.ElementType.ToZetaSQLType()
		if err != nil {
			return nil, err
		}
		return types.NewArrayType(typ)
	case types.STRUCT:
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
	return types.TypeFromKind(types.TypeKind(t.Kind)), nil
}

func (t *Type) FormatType() string {
	switch t.Kind {
	case types.STRUCT:
		formatTypes := make([]string, 0, len(t.FieldTypes))
		for _, field := range t.FieldTypes {
			formatTypes = append(formatTypes, fmt.Sprintf("`%s` %s", field.Name, field.Type.FormatType()))
		}
		return fmt.Sprintf("STRUCT<%s>", strings.Join(formatTypes, ","))
	case types.ARRAY:
		return fmt.Sprintf("ARRAY<%s>", t.ElementType.FormatType())
	}
	return types.TypeKind(t.Kind).String()
}

func (s *ColumnSpec) SQLiteSchema() string {
	var typ string
	switch types.TypeKind(s.Type.Kind) {
	case types.INT32, types.INT64, types.UINT32, types.UINT64:
		typ = "INT"
	case types.ENUM:
		typ = "INT"
	case types.BOOL:
		typ = "BOOLEAN"
	case types.FLOAT:
		typ = "FLOAT"
	case types.BYTES:
		typ = "BLOB"
	case types.DOUBLE:
		typ = "DOUBLE"
	case types.JSON:
		typ = "JSON"
	case types.STRING:
		typ = "TEXT"
	case types.DATE:
		typ = "TEXT"
	case types.TIMESTAMP:
		typ = "TEXT"
	case types.ARRAY:
		typ = "TEXT"
	case types.STRUCT:
		typ = "TEXT"
	case types.PROTO:
		typ = "TEXT"
	case types.TIME:
		typ = "TEXT"
	case types.DATETIME:
		typ = "TEXT"
	case types.GEOGRAPHY:
		typ = "TEXT"
	case types.NUMERIC:
		typ = "TEXT"
	case types.BIG_NUMERIC:
		typ = "TEXT"
	case types.EXTENDED:
		typ = "TEXT"
	case types.INTERVAL:
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
	if t.IsTemplated() {
		return &Type{SignatureKind: t.Kind()}
	}
	return newType(t.Type())
}

func newFunctionSpec(ctx context.Context, namePath *NamePath, stmt *ast.CreateFunctionStmtNode) (*FunctionSpec, error) {
	args := []*NameWithType{}
	signature := stmt.Signature()
	for _, arg := range signature.Arguments() {
		args = append(args, &NameWithType{
			Name: arg.ArgumentName(),
			Type: newTypeFromFunctionArgumentType(arg),
		})
	}

	var body string
	language := stmt.Language()
	switch language {
	case "js":
		code, err := EncodeGoValue(types.StringType(), stmt.Code())
		if err != nil {
			return nil, err
		}
		encodedType, err := json.Marshal(newType(stmt.ReturnType()))
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
			arr, err := EncodeGoValue(types.StringArrayType(), argNames)
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
		IsTemp:    stmt.CreateScope() == ast.CreateScopeTemp,
		NamePath:  namePath.mergePath(stmt.NamePath()),
		Args:      args,
		Return:    newType(stmt.ReturnType()),
		Code:      stmt.Code(),
		Body:      body,
		Language:  language,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func newTypeFromFunctionArgumentTypeByRealType(t *types.FunctionArgumentType, realType types.Type) *Type {
	if t.IsTemplated() {
		if realType.IsArray() {
			return &Type{SignatureKind: types.ArgArrayTypeAny1}
		}
		return &Type{SignatureKind: types.ArgTypeAny1}
	}
	return newType(t.Type())
}

func newTemplatedFunctionSpec(ctx context.Context, namePath *NamePath, stmt *ast.CreateFunctionStmtNode, realStmts []*ast.CreateFunctionStmtNode) (*FunctionSpec, error) {
	signature := stmt.Signature()
	arguments := signature.Arguments()
	realStmt := realStmts[0]
	realSignature := realStmt.Signature()
	realArguments := realSignature.Arguments()
	resultType := newType(realSignature.ResultType().Type())
	resultTypeName := resultType.FormatType()

	allSameResultType := true
	for _, stmt := range realStmts {
		if newType(stmt.Signature().ResultType().Type()).FormatType() != resultTypeName {
			allSameResultType = false
			break
		}
	}
	var retType *Type
	if allSameResultType {
		retType = resultType
	} else {
		retType = newTypeFromFunctionArgumentTypeByRealType(
			signature.ResultType(),
			realSignature.ResultType().Type(),
		)
	}
	args := []*NameWithType{}
	for i := 0; i < len(arguments); i++ {
		args = append(args, &NameWithType{
			Name: arguments[i].ArgumentName(),
			Type: newTypeFromFunctionArgumentTypeByRealType(
				arguments[i],
				realArguments[i].Type(),
			),
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
		IsTemp:    stmt.CreateScope() == ast.CreateScopeTemp,
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

func newColumnsFromDef(def []*ast.ColumnDefinitionNode) []*ColumnSpec {
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
		columns = append(columns, &ColumnSpec{
			Name:      columnNode.Name(),
			Type:      newType(columnNode.Type()),
			IsNotNull: isNotNull,
		})
	}
	return columns
}

func newColumnsFromOutputColumns(def []*ast.OutputColumnNode) []*ColumnSpec {
	columns := []*ColumnSpec{}
	for _, columnNode := range def {
		column := columnNode.Column()

		columns = append(columns, &ColumnSpec{
			Name: columnNode.Name(),
			Type: newType(column.Type()),
		})
	}
	return columns
}

func newPrimaryKey(key *ast.PrimaryKeyNode) []string {
	if key == nil {
		return nil
	}
	return key.ColumnNameList()
}

func newTableSpec(namePath *NamePath, stmt *ast.CreateTableStmtNode) *TableSpec {
	now := time.Now()
	return &TableSpec{
		IsTemp:     stmt.CreateScope() == ast.CreateScopeTemp,
		NamePath:   namePath.mergePath(stmt.NamePath()),
		Columns:    newColumnsFromDef(stmt.ColumnDefinitionList()),
		PrimaryKey: newPrimaryKey(stmt.PrimaryKey()),
		CreateMode: stmt.CreateMode(),
		UpdatedAt:  now,
		CreatedAt:  now,
	}
}

func newTableAsViewSpec(namePath *NamePath, query string, stmt *ast.CreateViewStmtNode) *TableSpec {
	var outputColumns []string
	for _, column := range stmt.OutputColumnList() {
		colName := column.Name()
		refColumnName := column.Column().Name()
		colID := column.Column().ColumnID()
		outputColumns = append(
			outputColumns,
			fmt.Sprintf("`%s#%d` AS `%s`", refColumnName, colID, colName),
		)
	}
	now := time.Now()
	return &TableSpec{
		IsTemp:     stmt.CreateScope() == ast.CreateScopeTemp,
		IsView:     true,
		NamePath:   namePath.mergePath(stmt.NamePath()),
		Columns:    newColumnsFromOutputColumns(stmt.OutputColumnList()),
		CreateMode: stmt.CreateMode(),
		Query:      fmt.Sprintf("SELECT %s FROM (%s)", strings.Join(outputColumns, ","), query),
		UpdatedAt:  now,
		CreatedAt:  now,
	}
}

func newTableAsSelectSpec(namePath *NamePath, query string, stmt *ast.CreateTableAsSelectStmtNode) *TableSpec {
	var outputColumns []string
	for _, column := range stmt.OutputColumnList() {
		colName := column.Name()
		refColumnName := column.Column().Name()
		colID := column.Column().ColumnID()
		outputColumns = append(
			outputColumns,
			fmt.Sprintf("`%s#%d` AS `%s`", refColumnName, colID, colName),
		)
	}
	now := time.Now()
	return &TableSpec{
		IsTemp:     stmt.CreateScope() == ast.CreateScopeTemp,
		NamePath:   namePath.mergePath(stmt.NamePath()),
		Columns:    newColumnsFromDef(stmt.ColumnDefinitionList()),
		PrimaryKey: newPrimaryKey(stmt.PrimaryKey()),
		CreateMode: stmt.CreateMode(),
		Query:      fmt.Sprintf("SELECT %s FROM (%s)", strings.Join(outputColumns, ","), query),
		UpdatedAt:  now,
		CreatedAt:  now,
	}
}

func newType(t types.Type) *Type {
	kind := t.Kind()
	var (
		elem       *Type
		fieldTypes []*NameWithType
	)
	switch kind {
	case types.ARRAY:
		elem = newType(t.AsArray().ElementType())
	case types.STRUCT:
		for _, field := range t.AsStruct().Fields() {
			fieldTypes = append(fieldTypes, &NameWithType{
				Name: field.Name(),
				Type: newType(field.Type()),
			})
		}
	}
	return &Type{
		Name:        t.TypeName(types.ProductInternal),
		Kind:        int(kind),
		ElementType: elem,
		FieldTypes:  fieldTypes,
	}
}
