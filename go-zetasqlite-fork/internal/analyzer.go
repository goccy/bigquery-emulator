package internal

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/goccy/go-zetasql"
	parsed_ast "github.com/goccy/go-zetasql/ast"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

type Analyzer struct {
	namePath        *NamePath
	isAutoIndexMode bool
	isExplainMode   bool
	catalog         *Catalog
	opt             *zetasql.AnalyzerOptions
}

func NewAnalyzer(catalog *Catalog) (*Analyzer, error) {
	opt, err := newAnalyzerOptions()
	if err != nil {
		return nil, err
	}
	return &Analyzer{
		catalog:  catalog,
		opt:      opt,
		namePath: &NamePath{},
	}, nil
}

func newAnalyzerOptions() (*zetasql.AnalyzerOptions, error) {
	langOpt := zetasql.NewLanguageOptions()
	langOpt.SetNameResolutionMode(zetasql.NameResolutionDefault)
	langOpt.SetProductMode(types.ProductInternal)
	langOpt.SetEnabledLanguageFeatures([]zetasql.LanguageFeature{
		zetasql.FeatureAnalyticFunctions,
		zetasql.FeatureNamedArguments,
		zetasql.FeatureNumericType,
		zetasql.FeatureBignumericType,
		zetasql.FeatureV13DecimalAlias,
		zetasql.FeatureCreateTableNotNull,
		zetasql.FeatureParameterizedTypes,
		zetasql.FeatureTablesample,
		zetasql.FeatureTimestampNanos,
		zetasql.FeatureV11HavingInAggregate,
		zetasql.FeatureV11NullHandlingModifierInAggregate,
		zetasql.FeatureV11NullHandlingModifierInAnalytic,
		zetasql.FeatureV11OrderByCollate,
		zetasql.FeatureV11SelectStarExceptReplace,
		zetasql.FeatureV12SafeFunctionCall,
		zetasql.FeatureJsonType,
		zetasql.FeatureJsonArrayFunctions,
		zetasql.FeatureJsonStrictNumberParsing,
		zetasql.FeatureV13IsDistinct,
		zetasql.FeatureV13FormatInCast,
		zetasql.FeatureV13DateArithmetics,
		zetasql.FeatureV11OrderByInAggregate,
		zetasql.FeatureV11LimitInAggregate,
		zetasql.FeatureV13DateTimeConstructors,
		zetasql.FeatureV13ExtendedDateTimeSignatures,
		zetasql.FeatureV12CivilTime,
		zetasql.FeatureV12WeekWithWeekday,
		zetasql.FeatureIntervalType,
		zetasql.FeatureGroupByRollup,
		zetasql.FeatureV13NullsFirstLastInOrderBy,
		zetasql.FeatureV13Qualify,
		zetasql.FeatureV13AllowDashesInTableName,
		zetasql.FeatureGeography,
		zetasql.FeatureV13ExtendedGeographyParsers,
		zetasql.FeatureTemplateFunctions,
		zetasql.FeatureV11WithOnSubquery,
		zetasql.FeatureV13Pivot,
		zetasql.FeatureV13Unpivot,
		zetasql.FeatureCreateTableAsSelectColumnList,
	})
	langOpt.SetSupportedStatementKinds([]ast.Kind{
		ast.BeginStmt,
		ast.CommitStmt,
		ast.MergeStmt,
		ast.QueryStmt,
		ast.InsertStmt,
		ast.UpdateStmt,
		ast.DeleteStmt,
		ast.DropStmt,
		ast.TruncateStmt,
		ast.CreateTableStmt,
		ast.CreateTableAsSelectStmt,
		ast.CreateProcedureStmt,
		ast.CreateFunctionStmt,
		ast.CreateTableFunctionStmt,
		ast.CreateViewStmt,
		ast.DropFunctionStmt,
	})
	// Enable QUALIFY without WHERE
	// https://github.com/google/zetasql/issues/124
	if err := langOpt.EnableReservableKeyword("QUALIFY", true); err != nil {
		return nil, err
	}
	opt := zetasql.NewAnalyzerOptions()
	opt.SetAllowUndeclaredParameters(true)
	opt.SetLanguage(langOpt)
	opt.SetParseLocationRecordType(zetasql.ParseLocationRecordFullNodeScope)
	return opt, nil
}

func (a *Analyzer) SetAutoIndexMode(enabled bool) {
	a.isAutoIndexMode = enabled
}

func (a *Analyzer) SetExplainMode(enabled bool) {
	a.isExplainMode = enabled
}

func (a *Analyzer) NamePath() []string {
	return a.namePath.path
}

func (a *Analyzer) SetNamePath(path []string) error {
	return a.namePath.setPath(path)
}

func (a *Analyzer) SetMaxNamePath(num int) {
	a.namePath.setMaxNum(num)
}

func (a *Analyzer) MaxNamePath() int {
	return a.namePath.maxNum
}

func (a *Analyzer) AddNamePath(path string) error {
	return a.namePath.addPath(path)
}

func (a *Analyzer) parseScript(query string) ([]parsed_ast.StatementNode, error) {
	loc := zetasql.NewParseResumeLocation(query)
	var stmts []parsed_ast.StatementNode
	for {
		stmt, isEnd, err := zetasql.ParseNextScriptStatement(loc, a.opt.ParserOptions())
		if err != nil {
			return nil, fmt.Errorf("failed to parse statement: %w", err)
		}
		switch s := stmt.(type) {
		case *parsed_ast.BeginEndBlockNode:
			stmts = append(stmts, s.StatementList()...)
		default:
			stmts = append(stmts, s)
		}
		if isEnd {
			break
		}
	}
	return stmts, nil
}

func (a *Analyzer) getParameterMode(stmt parsed_ast.StatementNode) (zetasql.ParameterMode, error) {
	var (
		enabledNamedParameter      bool
		enabledPositionalParameter bool
	)
	_ = parsed_ast.Walk(stmt, func(node parsed_ast.Node) error {
		switch n := node.(type) {
		case *parsed_ast.ParameterExprNode:
			if n.Position() > 0 {
				enabledPositionalParameter = true
			}
			if n.Name() != nil {
				enabledNamedParameter = true
			}
		}
		return nil
	})
	if enabledNamedParameter && enabledPositionalParameter {
		return zetasql.ParameterNone, fmt.Errorf("named parameter and positional parameter cannot be used together")
	}
	if enabledPositionalParameter {
		return zetasql.ParameterPositional, nil
	}
	return zetasql.ParameterNamed, nil
}

type StmtActionFunc func() (StmtAction, error)

func (a *Analyzer) Analyze(ctx context.Context, conn *Conn, query string, args []driver.NamedValue) ([]StmtActionFunc, error) {
	if err := a.catalog.Sync(ctx, conn); err != nil {
		return nil, fmt.Errorf("failed to sync catalog: %w", err)
	}
	stmts, err := a.parseScript(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse statements: %w", err)
	}
	funcMap := map[string]*FunctionSpec{}
	for _, spec := range a.catalog.getFunctions(a.namePath) {
		funcMap[spec.FuncName()] = spec
	}
	actionFuncs := make([]StmtActionFunc, 0, len(stmts))
	for _, stmt := range stmts {
		stmt := stmt
		actionFuncs = append(actionFuncs, func() (StmtAction, error) {
			mode, err := a.getParameterMode(stmt)
			if err != nil {
				return nil, err
			}
			a.opt.SetParameterMode(mode)
			out, err := zetasql.AnalyzeStatementFromParserAST(
				query,
				stmt,
				a.catalog,
				a.opt,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to analyze: %w", err)
			}
			stmtNode := out.Statement()
			ctx = a.context(ctx, funcMap, stmtNode, stmt)
			action, err := a.newStmtAction(ctx, query, args, stmtNode)
			if err != nil {
				return nil, err
			}
			if mode == zetasql.ParameterPositional {
				args = args[len(action.Args()):]
			}
			return action, nil
		})
	}
	return actionFuncs, nil
}

func (a *Analyzer) context(
	ctx context.Context,
	funcMap map[string]*FunctionSpec,
	stmtNode ast.StatementNode,
	stmt parsed_ast.StatementNode) context.Context {
	ctx = withAnalyzer(ctx, a)
	ctx = withNamePath(ctx, a.namePath)
	ctx = withColumnRefMap(ctx, map[string]string{})
	ctx = withTableNameToColumnListMap(ctx, map[string][]*ast.Column{})
	ctx = withFuncMap(ctx, funcMap)
	ctx = withAnalyticOrderColumnNames(ctx, &analyticOrderColumnNames{})
	ctx = withNodeMap(ctx, zetasql.NewNodeMap(stmtNode, stmt))
	return ctx
}

func (a *Analyzer) analyzeTemplatedFunctionWithRuntimeArgument(ctx context.Context, query string) (*FunctionSpec, error) {
	out, err := zetasql.AnalyzeStatement(query, a.catalog, a.opt)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze: %w", err)
	}
	node := out.Statement()
	stmt, ok := node.(*ast.CreateFunctionStmtNode)
	if !ok {
		return nil, fmt.Errorf("unexpected create function query %s", query)
	}
	spec, err := newFunctionSpec(ctx, a.namePath, stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to create function spec: %w", err)
	}
	return spec, nil
}

func (a *Analyzer) newStmtAction(ctx context.Context, query string, args []driver.NamedValue, node ast.StatementNode) (StmtAction, error) {
	switch node.Kind() {
	case ast.CreateTableStmt:
		return a.newCreateTableStmtAction(ctx, query, args, node.(*ast.CreateTableStmtNode))
	case ast.CreateTableAsSelectStmt:
		ctx = withUseColumnID(ctx)
		return a.newCreateTableAsSelectStmtAction(ctx, query, args, node.(*ast.CreateTableAsSelectStmtNode))
	case ast.CreateFunctionStmt:
		return a.newCreateFunctionStmtAction(ctx, query, args, node.(*ast.CreateFunctionStmtNode))
	case ast.CreateViewStmt:
		ctx = withUseColumnID(ctx)
		return a.newCreateViewStmtAction(ctx, query, args, node.(*ast.CreateViewStmtNode))
	case ast.DropStmt:
		return a.newDropStmtAction(ctx, query, args, node.(*ast.DropStmtNode))
	case ast.DropFunctionStmt:
		return a.newDropFunctionStmtAction(ctx, query, args, node.(*ast.DropFunctionStmtNode))
	case ast.InsertStmt, ast.UpdateStmt, ast.DeleteStmt:
		return a.newDMLStmtAction(ctx, query, args, node)
	case ast.TruncateStmt:
		return a.newTruncateStmtAction(ctx, query, args, node.(*ast.TruncateStmtNode))
	case ast.MergeStmt:
		ctx = withUseColumnID(ctx)
		return a.newMergeStmtAction(ctx, query, args, node.(*ast.MergeStmtNode))
	case ast.QueryStmt:
		ctx = withUseColumnID(ctx)
		return a.newQueryStmtAction(ctx, query, args, node.(*ast.QueryStmtNode))
	case ast.BeginStmt:
		return a.newBeginStmtAction(ctx, query, args, node)
	case ast.CommitStmt:
		return a.newCommitStmtAction(ctx, query, args, node)
	}
	return nil, fmt.Errorf("unsupported stmt %s", node.DebugString())
}

func (a *Analyzer) newCreateTableStmtAction(_ context.Context, query string, args []driver.NamedValue, node *ast.CreateTableStmtNode) (*CreateTableStmtAction, error) {
	spec := newTableSpec(a.namePath, node)
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	return &CreateTableStmtAction{
		query:           query,
		spec:            spec,
		args:            queryArgs,
		catalog:         a.catalog,
		isAutoIndexMode: a.isAutoIndexMode,
	}, nil
}

func (a *Analyzer) newCreateTableAsSelectStmtAction(ctx context.Context, _ string, args []driver.NamedValue, node *ast.CreateTableAsSelectStmtNode) (*CreateTableStmtAction, error) {
	query, err := newNode(node.Query()).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	spec := newTableAsSelectSpec(a.namePath, query, node)
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	return &CreateTableStmtAction{
		query:           query,
		spec:            spec,
		args:            queryArgs,
		catalog:         a.catalog,
		isAutoIndexMode: a.isAutoIndexMode,
	}, nil
}

func (a *Analyzer) newCreateFunctionStmtAction(ctx context.Context, query string, _ []driver.NamedValue, node *ast.CreateFunctionStmtNode) (*CreateFunctionStmtAction, error) {
	var spec *FunctionSpec
	if a.resultTypeIsTemplatedType(node.Signature()) {
		realStmts, err := a.inferTemplatedTypeByRealType(query, node)
		if err != nil {
			return nil, err
		}
		templatedFuncSpec, err := newTemplatedFunctionSpec(ctx, a.namePath, node, realStmts)
		if err != nil {
			return nil, err
		}
		spec = templatedFuncSpec
	} else {
		funcSpec, err := newFunctionSpec(ctx, a.namePath, node)
		if err != nil {
			return nil, fmt.Errorf("failed to create function spec: %w", err)
		}
		spec = funcSpec
	}
	return &CreateFunctionStmtAction{
		spec:    spec,
		catalog: a.catalog,
		funcMap: funcMapFromContext(ctx),
	}, nil
}

func (a *Analyzer) newCreateViewStmtAction(ctx context.Context, _ string, _ []driver.NamedValue, node *ast.CreateViewStmtNode) (*CreateViewStmtAction, error) {
	query, err := newNode(node.Query()).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	spec := newTableAsViewSpec(a.namePath, query, node)
	return &CreateViewStmtAction{
		query:   query,
		spec:    spec,
		catalog: a.catalog,
	}, nil
}

func (a *Analyzer) resultTypeIsTemplatedType(sig *types.FunctionSignature) bool {
	if !sig.IsTemplated() {
		return false
	}
	return sig.ResultType().IsTemplated()
}

var inferTypes = []string{
	"INT64", "DOUBLE", "BOOL", "STRING", "BYTES",
	"JSON", "DATE", "DATETIME", "TIME", "TIMESTAMP",
	"INTERVAL", "GEOGRAPHY",
	"STRUCT<>",
}

func (a *Analyzer) inferTemplatedTypeByRealType(query string, node *ast.CreateFunctionStmtNode) ([]*ast.CreateFunctionStmtNode, error) {
	var stmts []*ast.CreateFunctionStmtNode
	for _, typ := range inferTypes {
		if out, err := zetasql.AnalyzeStatement(a.buildScalarTypeFuncFromTemplatedFunc(node, typ), a.catalog, a.opt); err == nil {
			stmts = append(stmts, out.Statement().(*ast.CreateFunctionStmtNode))
		}
	}
	if len(stmts) != 0 {
		return stmts, nil
	}
	for _, typ := range inferTypes {
		if out, err := zetasql.AnalyzeStatement(a.buildArrayTypeFuncFromTemplatedFunc(node, typ), a.catalog, a.opt); err == nil {
			stmts = append(stmts, out.Statement().(*ast.CreateFunctionStmtNode))
		}
	}
	if len(stmts) != 0 {
		return stmts, nil
	}
	return nil, fmt.Errorf("failed to infer templated function result type for %s", query)
}

func (a *Analyzer) buildScalarTypeFuncFromTemplatedFunc(node *ast.CreateFunctionStmtNode, realType string) string {
	signature := node.Signature()
	var args []string
	for _, arg := range signature.Arguments() {
		typ := realType
		if !arg.IsTemplated() {
			typ = newType(arg.Type()).FormatType()
		}
		args = append(args, fmt.Sprintf("%s %s", arg.ArgumentName(), typ))
	}
	return fmt.Sprintf(
		"CREATE TEMP FUNCTION __zetasqlite_func__(%s) as (%s)",
		strings.Join(args, ","),
		node.Code(),
	)
}

func (a *Analyzer) buildArrayTypeFuncFromTemplatedFunc(node *ast.CreateFunctionStmtNode, realType string) string {
	signature := node.Signature()
	var args []string
	for _, arg := range signature.Arguments() {
		typ := fmt.Sprintf("ARRAY<%s>", realType)
		if !arg.IsTemplated() {
			typ = newType(arg.Type()).FormatType()
		}
		args = append(args, fmt.Sprintf("%s %s", arg.ArgumentName(), typ))
	}
	return fmt.Sprintf(
		"CREATE TEMP FUNCTION __zetasqlite_func__(%s) as (%s)",
		strings.Join(args, ","),
		node.Code(),
	)
}

func (a *Analyzer) newDropStmtAction(ctx context.Context, query string, args []driver.NamedValue, node *ast.DropStmtNode) (*DropStmtAction, error) {
	formattedQuery, err := newNode(node).FormatSQL(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to format query %s: %w", query, err)
	}
	if formattedQuery == "" {
		return nil, fmt.Errorf("failed to format query %s", query)
	}
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	objectType := node.ObjectType()
	name := a.namePath.format(node.NamePath())
	return &DropStmtAction{
		name:           name,
		objectType:     objectType,
		funcMap:        funcMapFromContext(ctx),
		catalog:        a.catalog,
		query:          query,
		formattedQuery: formattedQuery,
		args:           queryArgs,
	}, nil
}

func (a *Analyzer) newDropFunctionStmtAction(ctx context.Context, query string, args []driver.NamedValue, node *ast.DropFunctionStmtNode) (*DropStmtAction, error) {
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	name := a.namePath.format(node.NamePath())
	return &DropStmtAction{
		name:       name,
		objectType: "FUNCTION",
		funcMap:    funcMapFromContext(ctx),
		catalog:    a.catalog,
		query:      query,
		args:       queryArgs,
	}, nil
}

func (a *Analyzer) newDMLStmtAction(ctx context.Context, query string, args []driver.NamedValue, node ast.Node) (*DMLStmtAction, error) {
	formattedQuery, err := newNode(node).FormatSQL(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to format query %s: %w", query, err)
	}
	if formattedQuery == "" {
		return nil, fmt.Errorf("failed to format query %s", query)
	}
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	return &DMLStmtAction{
		query:          query,
		params:         params,
		args:           queryArgs,
		formattedQuery: formattedQuery,
	}, nil
}

func (a *Analyzer) newQueryStmtAction(ctx context.Context, query string, args []driver.NamedValue, node *ast.QueryStmtNode) (*QueryStmtAction, error) {
	outputColumns := []*ColumnSpec{}
	for _, col := range node.OutputColumnList() {
		outputColumns = append(outputColumns, &ColumnSpec{
			Name: col.Name(),
			Type: newType(col.Column().Type()),
		})
	}
	formattedQuery, err := newNode(node).FormatSQL(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to format query %s: %w", query, err)
	}
	if formattedQuery == "" {
		return nil, fmt.Errorf("failed to format query %s", query)
	}
	params := getParamsFromNode(node)
	queryArgs, err := getArgsFromParams(args, params)
	if err != nil {
		return nil, err
	}
	return &QueryStmtAction{
		query:          query,
		params:         params,
		args:           queryArgs,
		formattedQuery: formattedQuery,
		outputColumns:  outputColumns,
		isExplainMode:  a.isExplainMode,
	}, nil
}

func (a *Analyzer) newBeginStmtAction(ctx context.Context, query string, args []driver.NamedValue, node ast.Node) (*BeginStmtAction, error) {
	return &BeginStmtAction{}, nil
}

func (a *Analyzer) newCommitStmtAction(ctx context.Context, query string, args []driver.NamedValue, node ast.Node) (*CommitStmtAction, error) {
	return &CommitStmtAction{}, nil
}

//nolint:unparam
func (a *Analyzer) newTruncateStmtAction(_ context.Context, _ string, _ []driver.NamedValue, node *ast.TruncateStmtNode) (*TruncateStmtAction, error) {
	table := node.TableScan().Table().Name()
	return &TruncateStmtAction{query: fmt.Sprintf("DELETE FROM `%s`", table)}, nil
}

func (a *Analyzer) newMergeStmtAction(ctx context.Context, _ string, args []driver.NamedValue, node *ast.MergeStmtNode) (*MergeStmtAction, error) {
	targetTable, err := newNode(node.TableScan()).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	sourceTable, err := newNode(node.FromScan()).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	expr, err := newNode(node.MergeExpr()).FormatSQL(ctx)
	if err != nil {
		return nil, err
	}
	fn, ok := node.MergeExpr().(*ast.FunctionCallNode)
	if !ok {
		return nil, fmt.Errorf("currently MERGE expression is supported equal expression only")
	}
	if fn.Function().FullName(false) != "$equal" {
		return nil, fmt.Errorf("currently MERGE expression is supported equal expression only")
	}
	argList := fn.ArgumentList()
	if len(argList) != 2 {
		return nil, fmt.Errorf("unexpected MERGE expression column num. expected 2 column but specified %d column", len(args))
	}
	colA, ok := argList[0].(*ast.ColumnRefNode)
	if !ok {
		return nil, fmt.Errorf("unexpected MERGE expression. expected column reference but got %T", argList[0])
	}
	colB, ok := argList[1].(*ast.ColumnRefNode)
	if !ok {
		return nil, fmt.Errorf("unexpected MERGE expression. expected column reference but got %T", argList[1])
	}
	var (
		sourceColumn *ast.Column
		targetColumn *ast.Column
	)
	if strings.Contains(sourceTable, colA.Column().TableName()) {
		sourceColumn = colA.Column()
		targetColumn = colB.Column()
	} else {
		sourceColumn = colB.Column()
		targetColumn = colA.Column()
	}
	mergedTableSourceColumnName := fmt.Sprintf("`%s`", uniqueColumnName(ctx, sourceColumn))
	mergedTableTargetColumnName := fmt.Sprintf("`%s`", uniqueColumnName(ctx, targetColumn))
	mergedTableOutputColumns := []string{
		mergedTableTargetColumnName,
		mergedTableSourceColumnName,
	}
	var stmts []string
	stmts = append(stmts, fmt.Sprintf(
		"CREATE TABLE zetasqlite_merged_table AS SELECT DISTINCT * FROM (SELECT * FROM %[1]s LEFT JOIN %[2]s ON %[3]s UNION ALL SELECT * FROM %[2]s LEFT JOIN %[1]s ON %[3]s)",
		sourceTable, targetTable, expr,
	))

	// exists target table and source table
	matchedFromStmt := fmt.Sprintf(
		"FROM zetasqlite_merged_table WHERE %[2]s = %[1]s AND %[3]s = %[1]s",
		targetColumn.Name(),
		mergedTableSourceColumnName,
		mergedTableTargetColumnName,
	)

	// exists target table but not exists source table
	notMatchedBySourceFromStmt := fmt.Sprintf(
		"FROM zetasqlite_merged_table WHERE %[2]s = `%[1]s` AND %[3]s IS NULL",
		targetColumn.Name(),
		mergedTableTargetColumnName,
		mergedTableSourceColumnName,
	)

	// exists source table but not exists target table
	notMatchedByTargetFromStmt := fmt.Sprintf(
		"FROM zetasqlite_merged_table WHERE %[2]s = `%[1]s` AND %[3]s IS NULL",
		sourceColumn.Name(),
		mergedTableSourceColumnName,
		mergedTableTargetColumnName,
	)
	for _, when := range node.WhenClauseList() {
		var fromStmt string
		switch when.MatchType() {
		case ast.MatchTypeMatched:
			fromStmt = matchedFromStmt
		case ast.MatchTypeNotMatchedBySource:
			fromStmt = notMatchedBySourceFromStmt
		case ast.MatchTypeNotMatchedByTarget:
			fromStmt = notMatchedByTargetFromStmt
		}
		whereStmt := fmt.Sprintf(
			"WHERE EXISTS(SELECT %s %s)",
			strings.Join(mergedTableOutputColumns, ","),
			fromStmt,
		)
		switch when.ActionType() {
		case ast.ActionTypeInsert:
			var columns []string
			for _, col := range when.InsertColumnList() {
				columns = append(columns, fmt.Sprintf("`%s`", col.Name()))
			}
			row, err := newNode(when.InsertRow()).FormatSQL(unuseColumnID(ctx))
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, fmt.Sprintf(
				"INSERT INTO `%[1]s`(%[2]s) SELECT %[3]s FROM (SELECT * FROM `%[4]s` %[5]s)",
				targetColumn.TableName(),
				strings.Join(columns, ","),
				row,
				sourceColumn.TableName(),
				whereStmt,
			))
		case ast.ActionTypeUpdate:
			var items []string
			for _, item := range when.UpdateItemList() {
				sql, err := newNode(item).FormatSQL(ctx)
				if err != nil {
					return nil, err
				}
				items = append(items, sql)
			}
			stmts = append(stmts, fmt.Sprintf(
				"UPDATE `%s` SET %s %s",
				targetColumn.TableName(),
				strings.Join(items, ","),
				fromStmt,
			))
		case ast.ActionTypeDelete:
			stmts = append(stmts, fmt.Sprintf(
				"DELETE FROM `%s` %s",
				targetColumn.TableName(),
				whereStmt,
			))
		}
	}
	stmts = append(stmts, "DROP TABLE zetasqlite_merged_table")
	return &MergeStmtAction{stmts: stmts}, nil
}

func getParamsFromNode(node ast.Node) []*ast.ParameterNode {
	var (
		params       []*ast.ParameterNode
		paramNameMap = map[string]struct{}{}
	)
	_ = ast.Walk(node, func(n ast.Node) error {
		param, ok := n.(*ast.ParameterNode)
		if ok {
			name := param.Name()
			if name != "" {
				if _, exists := paramNameMap[name]; !exists {
					params = append(params, param)
					paramNameMap[name] = struct{}{}
				}
			} else {
				params = append(params, param)
			}
		}
		return nil
	})
	return params
}

func getArgsFromParams(values []driver.NamedValue, params []*ast.ParameterNode) ([]interface{}, error) {
	if values == nil {
		return nil, nil
	}
	argNum := len(params)
	if len(values) < argNum {
		return nil, fmt.Errorf("not enough query arguments")
	}
	namedValuesMap := map[string]driver.NamedValue{}
	for _, value := range values {
		// Name() value of ast.ParameterNode always returns lowercase name.
		namedValuesMap[strings.ToLower(value.Name)] = value
	}
	var namedValues []driver.NamedValue
	for idx, param := range params {
		name := param.Name()
		if name != "" {
			value, exists := namedValuesMap[name]
			if exists {
				namedValues = append(namedValues, value)
			} else {
				namedValues = append(namedValues, values[idx])
			}
		} else {
			namedValues = append(namedValues, values[idx])
		}
	}
	newNamedValues, err := EncodeNamedValues(namedValues, params)
	if err != nil {
		return nil, err
	}
	args := make([]interface{}, 0, argNum)
	for _, newNamedValue := range newNamedValues {
		args = append(args, newNamedValue)
	}
	return args, nil
}
