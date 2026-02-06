package internal

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/goccy/go-json"
	parsed_ast "github.com/goccy/go-zetasql/ast"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

type Formatter interface {
	FormatSQL(context.Context) (string, error)
}

func New(node ast.Node) Formatter {
	return newNode(node)
}

func getTableName(ctx context.Context, n ast.Node) (string, error) {
	nodeMap := nodeMapFromContext(ctx)
	found := nodeMap.FindNodeFromResolvedNode(n)
	if len(found) == 0 {
		return "", fmt.Errorf("failed to find path node from table node %T", n)
	}
	path, err := getPathFromNode(found[0])
	if err != nil {
		return "", fmt.Errorf("failed to find path: %w", err)
	}
	namePath := namePathFromContext(ctx)
	return namePath.format(path), nil
}

func getFuncName(ctx context.Context, n ast.Node) (string, error) {
	nodeMap := nodeMapFromContext(ctx)
	found := nodeMap.FindNodeFromResolvedNode(n)
	if len(found) == 0 {
		return "", fmt.Errorf("failed to find path node from function node %T", n)
	}
	var foundCallNode *parsed_ast.FunctionCallNode
	for _, node := range found {
		fcallNode, ok := node.(*parsed_ast.FunctionCallNode)
		if !ok {
			continue
		}
		foundCallNode = fcallNode
		break
	}
	if foundCallNode == nil {
		return "", fmt.Errorf("failed to find function call node from %T", n)
	}
	path, err := getPathFromNode(foundCallNode.Function())
	if err != nil {
		return "", fmt.Errorf("failed to find path: %w", err)
	}
	namePath := namePathFromContext(ctx)
	return namePath.format(path), nil
}

func getPathFromNode(n parsed_ast.Node) ([]string, error) {
	var path []string
	switch node := n.(type) {
	case *parsed_ast.IdentifierNode:
		path = append(path, node.Name())
	case *parsed_ast.PathExpressionNode:
		for _, name := range node.Names() {
			path = append(path, name.Name())
		}
	case *parsed_ast.TablePathExpressionNode:
		switch {
		case node.PathExpr() != nil:
			for _, name := range node.PathExpr().Names() {
				path = append(path, name.Name())
			}
		}
	default:
		return nil, fmt.Errorf("found unknown path node: %T", node)
	}
	return path, nil
}

func uniqueColumnName(ctx context.Context, col *ast.Column) string {
	colName := col.Name()
	if useTableNameForColumn(ctx) {
		return fmt.Sprintf("%s.%s", col.TableName(), colName)
	}
	if useColumnID(ctx) {
		colID := col.ColumnID()
		return fmt.Sprintf("%s#%d", colName, colID)
	}
	return colName
}

type InputPattern int

const (
	InputKeep      InputPattern = 0
	InputNeedsWrap InputPattern = 1
	InputNeedsFrom InputPattern = 2
)

func getInputPattern(input string) InputPattern {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return InputKeep
	}
	if strings.HasPrefix(trimmed, "FROM") {
		return InputKeep
	}
	if strings.HasPrefix(trimmed, "SELECT") {
		return InputNeedsWrap
	}
	if strings.HasPrefix(trimmed, "WITH") {
		return InputNeedsWrap
	}
	return InputNeedsFrom
}

func formatInput(input string) (string, error) {
	switch getInputPattern(input) {
	case InputKeep:
		return input, nil
	case InputNeedsWrap:
		return fmt.Sprintf("FROM (%s)", input), nil
	case InputNeedsFrom:
		return fmt.Sprintf("FROM %s", input), nil
	}
	return "", fmt.Errorf("unexpected input pattern: %s", input)
}

func getFuncNameAndArgs(ctx context.Context, node *ast.BaseFunctionCallNode, isWindowFunc bool) (string, []string, error) {
	args := []string{}
	for _, a := range node.ArgumentList() {
		arg, err := newNode(a).FormatSQL(ctx)
		if err != nil {
			return "", nil, err
		}
		args = append(args, arg)
	}
	funcName := node.Function().FullName(false)
	funcName = strings.Replace(funcName, ".", "_", -1)

	_, existsCurrentTimeFunc := currentTimeFuncMap[funcName]
	_, existsNormalFunc := normalFuncMap[funcName]
	_, existsAggregateFunc := aggregateFuncMap[funcName]
	_, existsWindowFunc := windowFuncMap[funcName]
	currentTime := CurrentTime(ctx)

	funcPrefix := "zetasqlite"
	if node.ErrorMode() == ast.SafeErrorMode {
		if !existsNormalFunc {
			return "", nil, fmt.Errorf("SAFE is not supported for function %s", funcName)
		}
		funcPrefix = "zetasqlite_safe"
	}

	if strings.HasPrefix(funcName, "$") {
		if isWindowFunc {
			funcName = fmt.Sprintf("%s_window_%s", funcPrefix, funcName[1:])
		} else {
			funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName[1:])
		}
	} else if existsCurrentTimeFunc {
		if currentTime != nil {
			args = append(
				args,
				fmt.Sprint(currentTime.UnixNano()),
			)
		}
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if existsNormalFunc {
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if !isWindowFunc && existsAggregateFunc {
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if isWindowFunc && existsWindowFunc {
		funcName = fmt.Sprintf("%s_window_%s", funcPrefix, funcName)
	} else {
		if node.Function().IsZetaSQLBuiltin() {
			return "", nil, fmt.Errorf("%s function is unimplemented", funcName)
		}
		fname, err := getFuncName(ctx, node)
		if err != nil {
			return "", nil, err
		}
		funcName = fname
	}
	return funcName, args, nil
}

func (n *LiteralNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return LiteralFromZetaSQLValue(n.node.Value())
}

func (n *ParameterNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node.Name() == "" {
		return "?", nil
	}
	return fmt.Sprintf("@%s", n.node.Name()), nil
}

func (n *ExpressionColumnNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ColumnRefNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	columnMap := columnRefMap(ctx)
	col := n.node.Column()
	colName := uniqueColumnName(ctx, col)
	if ref, exists := columnMap[colName]; exists {
		delete(columnMap, colName)
		return ref, nil
	}
	return fmt.Sprintf("`%s`", colName), nil
}

func (n *ConstantNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SystemVariableNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *InlineLambdaNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FilterFieldArgNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FilterFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FunctionCallNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	funcName, args, err := getFuncNameAndArgs(ctx, n.node.BaseFunctionCallNode, false)
	if err != nil {
		return "", err
	}
	switch funcName {
	case "zetasqlite_ifnull":
		return fmt.Sprintf(
			"CASE WHEN %s IS NULL THEN %s ELSE %s END",
			args[0],
			args[1],
			args[0],
		), nil
	case "zetasqlite_if":
		return fmt.Sprintf(
			"CASE WHEN %s THEN %s ELSE %s END",
			args[0],
			args[1],
			args[2],
		), nil
	case "zetasqlite_case_no_value":
		var whenStmts []string
		for i := 0; i < len(args)-1; i += 2 {
			whenStmts = append(whenStmts, fmt.Sprintf("WHEN %s THEN %s", args[i], args[i+1]))
		}
		stmt := fmt.Sprintf("CASE %s", strings.Join(whenStmts, " "))
		// if args length is odd number, else statement exists.
		if len(args) > (len(args)/2)*2 {
			stmt += fmt.Sprintf(" ELSE %s", args[len(args)-1])
		}
		stmt += " END"
		return stmt, nil
	case "zetasqlite_case_with_value":
		if len(args) < 2 {
			return "", fmt.Errorf("not enough arguments for case with value")
		}
		val := args[0]
		args = args[1:]
		var whenStmts []string
		for i := 0; i < len(args)-1; i += 2 {
			whenStmts = append(whenStmts, fmt.Sprintf("WHEN %s THEN %s", args[i], args[i+1]))
		}
		stmt := fmt.Sprintf("CASE %s %s", val, strings.Join(whenStmts, " "))
		// if args length is odd number, else statement exists.
		if len(args) > (len(args)/2)*2 {
			stmt += fmt.Sprintf(" ELSE %s", args[len(args)-1])
		}
		stmt += " END"
		return stmt, nil
	}
	funcMap := funcMapFromContext(ctx)
	if spec, exists := funcMap[funcName]; exists {
		return spec.CallSQL(ctx, n.node.BaseFunctionCallNode, args)
	}
	return fmt.Sprintf(
		"%s(%s)",
		funcName,
		strings.Join(args, ","),
	), nil
}

func (n *AggregateFunctionCallNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	funcName, args, err := getFuncNameAndArgs(ctx, n.node.BaseFunctionCallNode, false)
	if err != nil {
		return "", err
	}
	funcMap := funcMapFromContext(ctx)
	if spec, exists := funcMap[funcName]; exists {
		return spec.CallSQL(ctx, n.node.BaseFunctionCallNode, args)
	}
	var opts []string
	for _, item := range n.node.OrderByItemList() {
		columnRef := item.ColumnRef()
		colName := uniqueColumnName(ctx, columnRef.Column())
		if item.IsDescending() {
			opts = append(opts, fmt.Sprintf("zetasqlite_order_by(`%s`, false)", colName))
		} else {
			opts = append(opts, fmt.Sprintf("zetasqlite_order_by(`%s`, true)", colName))
		}
	}
	if n.node.Distinct() {
		opts = append(opts, "zetasqlite_distinct()")
	}
	if n.node.Limit() != nil {
		limitValue, err := newNode(n.node.Limit()).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		opts = append(opts, fmt.Sprintf("zetasqlite_limit(%s)", limitValue))
	}
	switch n.node.NullHandlingModifier() {
	case ast.IgnoreNulls:
		opts = append(opts, "zetasqlite_ignore_nulls()")
	case ast.RespectNulls:
	}
	args = append(args, opts...)
	return fmt.Sprintf(
		"%s(%s)",
		funcName,
		strings.Join(args, ","),
	), nil
}

func (n *AnalyticFunctionCallNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	orderColumnNames := analyticOrderColumnNamesFromContext(ctx)
	orderColumns := orderColumnNames.values
	funcName, args, err := getFuncNameAndArgs(ctx, n.node.BaseFunctionCallNode, true)
	if err != nil {
		return "", err
	}
	var opts []string
	if n.node.Distinct() {
		opts = append(opts, "zetasqlite_distinct()")
	}
	switch n.node.NullHandlingModifier() {
	case ast.RespectNulls:
		// do nothing
	default:
		opts = append(opts, "zetasqlite_ignore_nulls()")
	}
	args = append(args, opts...)
	for _, column := range analyticPartitionColumnNamesFromContext(ctx) {
		args = append(args, getWindowPartitionOptionFuncSQL(column))
	}
	for _, col := range orderColumns {
		args = append(args, getWindowOrderByOptionFuncSQL(col.column, col.isAsc))
	}
	windowFrame := n.node.WindowFrame()
	if windowFrame != nil {
		args = append(args, getWindowFrameUnitOptionFuncSQL(windowFrame.FrameUnit()))
		startSQL, err := n.getWindowBoundaryOptionFuncSQL(ctx, windowFrame.StartExpr(), true)
		if err != nil {
			return "", err
		}
		endSQL, err := n.getWindowBoundaryOptionFuncSQL(ctx, windowFrame.EndExpr(), false)
		if err != nil {
			return "", err
		}
		args = append(args, startSQL, endSQL)
	}
	args = append(args, getWindowRowIDOptionFuncSQL())
	input := analyticInputScanFromContext(ctx)
	funcMap := funcMapFromContext(ctx)
	if spec, exists := funcMap[funcName]; exists {
		return spec.CallSQL(ctx, n.node.BaseFunctionCallNode, args)
	}
	return fmt.Sprintf(
		"( SELECT %s(%s) %s )",
		funcName,
		strings.Join(args, ","),
		input,
	), nil
}

func (n *AnalyticFunctionCallNode) getWindowBoundaryOptionFuncSQL(ctx context.Context, expr *ast.WindowFrameExprNode, isStart bool) (string, error) {
	typ := expr.BoundaryType()
	switch typ {
	case ast.UnboundedPrecedingType, ast.CurrentRowType, ast.UnboundedFollowingType:
		if isStart {
			return getWindowBoundaryStartOptionFuncSQL(typ, ""), nil
		}
		return getWindowBoundaryEndOptionFuncSQL(typ, ""), nil
	case ast.OffsetPrecedingType, ast.OffsetFollowingType:
		literal, err := newNode(expr.Expression()).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		if isStart {
			return getWindowBoundaryStartOptionFuncSQL(typ, literal), nil
		}
		return getWindowBoundaryEndOptionFuncSQL(typ, literal), nil
	}
	return "", fmt.Errorf("unexpected boundary type %d", typ)
}

func (n *ExtendedCastElementNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExtendedCastNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CastNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	fromType := newType(n.node.Expr().Type())
	jsonEncodedFromType, err := json.Marshal(fromType)
	if err != nil {
		return "", err
	}
	toType := newType(n.node.Type())
	jsonEncodedToType, err := json.Marshal(toType)
	if err != nil {
		return "", err
	}
	encodedFromType, err := EncodeGoValue(types.StringType(), string(jsonEncodedFromType))
	if err != nil {
		return "", err
	}
	encodedToType, err := EncodeGoValue(types.StringType(), string(jsonEncodedToType))
	if err != nil {
		return "", err
	}
	expr, err := newNode(n.node.Expr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"zetasqlite_cast(%s, '%s', '%s', %t)",
		expr, encodedFromType, encodedToType, n.node.ReturnNullOnError(),
	), nil
}

func (n *MakeStructNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	typ := n.node.Type().AsStruct()
	fieldNum := typ.NumFields()
	fields := n.node.FieldList()
	args := make([]string, 0, fieldNum*2)
	for i := 0; i < fieldNum; i++ {
		fieldName := typ.Field(i).Name()
		key, err := LiteralFromValue(StringValue(fieldName))
		if err != nil {
			return "", err
		}
		args = append(args, key)
		field, err := newNode(fields[i]).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		args = append(args, field)
	}
	return fmt.Sprintf("zetasqlite_make_struct(%s)", strings.Join(args, ",")), nil
}

func (n *MakeProtoNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *MakeProtoFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GetStructFieldNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	expr, err := newNode(n.node.Expr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	idx := n.node.FieldIdx()
	return fmt.Sprintf("zetasqlite_get_struct_field(%s, %d)", expr, idx), nil
}

func (n *GetProtoFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GetJsonFieldNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	expr, err := newNode(n.node.Expr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	name := n.node.FieldName()
	encodedName, err := EncodeGoValue(types.StringType(), name)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("zetasqlite_get_json_field(%s, '%s')", expr, encodedName), nil
}

func (n *FlattenNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FlattenedArgNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ReplaceFieldItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ReplaceFieldNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SubqueryExprNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	columnNames := &arraySubqueryColumnNames{}
	ctx = withArraySubqueryColumnName(ctx, columnNames)
	sql, err := newNode(n.node.Subquery()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	switch n.node.SubqueryType() {
	case ast.SubqueryTypeScalar:
	case ast.SubqueryTypeArray:
		if len(n.node.Subquery().ColumnList()) == 0 {
			return "", fmt.Errorf("failed to find computed column names for array subquery")
		}
		colName := uniqueColumnName(ctx, n.node.Subquery().ColumnList()[0])
		return fmt.Sprintf("(SELECT zetasqlite_array(`%s`) FROM (%s))", colName, sql), nil
	case ast.SubqueryTypeExists:
		return fmt.Sprintf("EXISTS (%s)", sql), nil
	case ast.SubqueryTypeIn:
		expr, err := newNode(n.node.InExpr()).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s IN (%s)", expr, sql), nil
	case ast.SubqueryTypeLikeAny:
	case ast.SubqueryTypeLikeAll:
	}
	return fmt.Sprintf("(%s)", sql), nil
}

func (n *LetExprNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ModelNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ConnectionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DescriptorNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SingleRowScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *TableScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	var columns []string
	for _, col := range n.node.ColumnList() {
		columns = append(
			columns,
			fmt.Sprintf("`%s` AS `%s`", col.Name(), uniqueColumnName(ctx, col)),
		)
	}

	table := n.node.Table()
	wildcardTable, ok := table.(*WildcardTable)
	if ok {
		query, err := wildcardTable.FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(SELECT %s FROM (%s))", strings.Join(columns, ","), query), nil
	}
	tableName, err := getTableName(ctx, n.node)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("(SELECT %s FROM `%s`)", strings.Join(columns, ","), tableName), nil
}

func (n *JoinScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	left, err := newNode(n.node.LeftScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	right, err := newNode(n.node.RightScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	if getInputPattern(left) == InputNeedsWrap {
		left = fmt.Sprintf("(%s)", left)
	}
	if getInputPattern(right) == InputNeedsWrap {
		right = fmt.Sprintf("(%s)", right)
	}
	if n.node.JoinExpr() == nil {
		return fmt.Sprintf("%s CROSS JOIN %s", left, right), nil
	}
	joinExpr, err := newNode(n.node.JoinExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	switch n.node.JoinType() {
	case ast.JoinTypeInner:
		return fmt.Sprintf("%s JOIN %s ON %s", left, right, joinExpr), nil
	case ast.JoinTypeLeft:
		return fmt.Sprintf("%s LEFT JOIN %s ON %s", left, right, joinExpr), nil
	case ast.JoinTypeRight:
		return fmt.Sprintf("%s RIGHT JOIN %s ON %s", left, right, joinExpr), nil
	case ast.JoinTypeFull:
		return fmt.Sprintf("%s FULL OUTER JOIN %s ON %s", left, right, joinExpr), nil
	}
	return "", fmt.Errorf("unexpected join type %d", n.node.JoinType())
}

func (n *ArrayScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	arrayExpr, err := newNode(n.node.ArrayExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	colName := uniqueColumnName(ctx, n.node.ElementColumn())
	columns := []string{fmt.Sprintf("json_each.value AS `%s`", colName)}

	if offsetColumn := n.node.ArrayOffsetColumn(); offsetColumn != nil {
		offsetColName := uniqueColumnName(ctx, offsetColumn.Column())
		columns = append(columns, fmt.Sprintf("json_each.key AS `%s`", offsetColName))
	}
	if n.node.InputScan() != nil {
		input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		formattedInput, err := formatInput(input)
		if err != nil {
			return "", err
		}

		array := fmt.Sprintf("json_each(zetasqlite_decode_array(%s))", arrayExpr)
		var arrayJoinExpr string
		if n.node.JoinExpr() != nil {
			arrayJoinExpr, err = newNode(n.node.JoinExpr()).FormatSQL(ctx)
			if err != nil {
				return "", err
			}
			// RIGHT JOINs on array expressions are not supported by BigQuery
			var joinMode string
			if n.node.IsOuter() {
				joinMode = "LEFT OUTER JOIN"
			} else {
				joinMode = "INNER JOIN"
			}
			arrayJoinExpr = fmt.Sprintf("%s %s ON %s",
				joinMode,
				array,
				arrayJoinExpr,
			)
		} else {
			// If there is no join expression, use a CROSS JOIN
			arrayJoinExpr = fmt.Sprintf(", %s", array)
		}

		return fmt.Sprintf(
			"SELECT *, %s %s %s",
			strings.Join(columns, ","),
			formattedInput,
			arrayJoinExpr,
		), nil
	}
	return fmt.Sprintf(
		"SELECT %s FROM json_each(zetasqlite_decode_array(%s))",
		strings.Join(columns, ","),
		arrayExpr,
	), nil
}

func (n *ColumnHolderNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

var tokensAfterFromClause = [...]string{"WHERE", "GROUP BY", "HAVING", "QUALIFY", "WINDOW", "ORDER BY", "COLLATE"}
var removeExpressions = regexp.MustCompile(`\(.+?\)`)

func (n *FilterScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	filter, err := newNode(n.node.FilterExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	currentQuery := removeExpressions.ReplaceAllString(input, "")

	// Qualify the statement if the input is not wrapped in parens
	queryWrappedInParens := currentQuery == ""
	containsTokens := false
	// and the input contains a token that would result in a syntax error
	for _, token := range tokensAfterFromClause {
		containsTokens = containsTokens || strings.Contains(currentQuery, token)
	}

	if !queryWrappedInParens && containsTokens {
		return fmt.Sprintf("( %s ) WHERE %s", input, filter), nil
	}
	return fmt.Sprintf("%s WHERE %s", input, filter), nil
}

func (n *GroupingSetNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AggregateScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	for _, agg := range n.node.AggregateList() {
		// assign sql to column ref map
		if _, err := newNode(agg).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	groupByColumns := []string{}
	groupByColumnMap := map[string]struct{}{}
	for _, col := range n.node.GroupByList() {
		if _, err := newNode(col).FormatSQL(ctx); err != nil {
			return "", err
		}
		colName := uniqueColumnName(ctx, col.Column())
		groupByColumns = append(groupByColumns, fmt.Sprintf("`%s`", colName))
		groupByColumnMap[colName] = struct{}{}
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	columnNames := []string{}
	for _, col := range n.node.ColumnList() {
		colName := uniqueColumnName(ctx, col)
		columnNames = append(columnNames, colName)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(columns, fmt.Sprintf("`%s`", colName))
		}
	}
	if len(n.node.GroupingSetList()) != 0 {
		columnPatterns := [][]string{}
		groupByColumnPatterns := [][]string{}
		for _, set := range n.node.GroupingSetList() {
			groupBySetColumns := []string{}
			groupBySetColumnMap := map[string]struct{}{}
			for _, col := range set.GroupByColumnList() {
				colName := uniqueColumnName(ctx, col.Column())
				groupBySetColumns = append(groupBySetColumns, fmt.Sprintf("`%s`", colName))
				groupBySetColumnMap[colName] = struct{}{}
			}
			nullColumnNameMap := map[string]struct{}{}
			for colName := range groupByColumnMap {
				if _, exists := groupBySetColumnMap[colName]; !exists {
					nullColumnNameMap[colName] = struct{}{}
				}
			}
			groupBySetColumnPattern := []string{}
			for idx, col := range columnNames {
				if _, exists := nullColumnNameMap[col]; exists {
					groupBySetColumnPattern = append(groupBySetColumnPattern, fmt.Sprintf("NULL AS `%s`", col))
				} else {
					groupBySetColumnPattern = append(groupBySetColumnPattern, columns[idx])
				}
			}
			columnPatterns = append(columnPatterns, groupBySetColumnPattern)
			annotatedGroupBySetColumns := make([]string, 0, len(groupBySetColumns))
			for _, column := range groupBySetColumns {
				annotatedGroupBySetColumns = append(
					annotatedGroupBySetColumns,
					fmt.Sprintf("zetasqlite_group_by(%s)", column),
				)
			}
			groupByColumnPatterns = append(groupByColumnPatterns, annotatedGroupBySetColumns)
		}
		stmts := []string{}
		for i := 0; i < len(columnPatterns); i++ {
			var groupBy string
			if len(groupByColumnPatterns[i]) != 0 {
				groupBy = fmt.Sprintf("GROUP BY %s", strings.Join(groupByColumnPatterns[i], ","))
			}
			formattedColumns := strings.Join(columnPatterns[i], ",")
			switch getInputPattern(input) {
			case InputKeep:
				stmts = append(stmts, fmt.Sprintf("SELECT %s %s %s", formattedColumns, input, groupBy))
			case InputNeedsWrap:
				stmts = append(stmts, fmt.Sprintf("SELECT %s FROM (%s) %s", formattedColumns, input, groupBy))
			case InputNeedsFrom:
				stmts = append(stmts, fmt.Sprintf("SELECT %s FROM %s %s", formattedColumns, input, groupBy))
			}
		}
		groupByWithCollates := make([]string, 0, len(groupByColumns))
		for _, groupByColumn := range groupByColumns {
			groupByWithCollates = append(
				groupByWithCollates,
				fmt.Sprintf("%s COLLATE zetasqlite_collate", groupByColumn),
			)
		}
		return fmt.Sprintf(
			"%s ORDER BY %s",
			strings.Join(stmts, " UNION ALL "),
			strings.Join(groupByWithCollates, ","),
		), nil
	}
	var groupBy string
	if len(groupByColumns) > 0 {
		annotatedGroupByColumns := make([]string, 0, len(groupByColumns))
		for _, groupByColumn := range groupByColumns {
			annotatedGroupByColumns = append(
				annotatedGroupByColumns,
				fmt.Sprintf("zetasqlite_group_by(%s)", groupByColumn),
			)
		}
		groupBy = fmt.Sprintf("GROUP BY %s", strings.Join(annotatedGroupByColumns, ","))
	}
	formattedColumns := strings.Join(columns, ",")
	switch getInputPattern(input) {
	case InputKeep:
		return fmt.Sprintf("SELECT %s %s %s", formattedColumns, input, groupBy), nil
	case InputNeedsWrap:
		return fmt.Sprintf("SELECT %s FROM (%s) %s", formattedColumns, input, groupBy), nil
	case InputNeedsFrom:
		return fmt.Sprintf("SELECT %s FROM %s %s", formattedColumns, input, groupBy), nil
	}
	return "", fmt.Errorf("unexpected input pattern: %s", input)
}

func (n *AnonymizedAggregateScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetOperationItemNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return newNode(n.node.Scan()).FormatSQL(ctx)
}

func (n *SetOperationScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	var opType string
	switch n.node.OpType() {
	case ast.SetOperationTypeUnionAll:
		opType = "UNION ALL"
	case ast.SetOperationTypeUnionDistinct:
		opType = "UNION"
	case ast.SetOperationTypeIntersectAll:
		opType = "INTERSECT ALL"
	case ast.SetOperationTypeIntersectDistinct:
		opType = "INTERSECT"
	case ast.SetOperationTypeExceptAll:
		opType = "EXCEPT ALL"
	case ast.SetOperationTypeExceptDistinct:
		opType = "EXCEPT"
	default:
		opType = "UNKNOWN"
	}
	var queries []string
	for _, item := range n.node.InputItemList() {
		var outputColumns []string
		for _, outputColumn := range item.OutputColumnList() {
			outputColumns = append(outputColumns, fmt.Sprintf("`%s`", uniqueColumnName(ctx, outputColumn)))
		}
		query, err := newNode(item).FormatSQL(ctx)
		if err != nil {
			return "", err
		}

		formattedInput, err := formatInput(query)
		if err != nil {
			return "", err
		}

		queries = append(
			queries,
			fmt.Sprintf("SELECT %s %s",
				strings.Join(outputColumns, ", "),
				formattedInput,
			),
		)
	}
	columnMaps := []string{}
	if len(n.node.InputItemList()) != 0 {
		for idx, col := range n.node.InputItemList()[0].OutputColumnList() {
			columnMaps = append(
				columnMaps,
				fmt.Sprintf(
					"`%s` AS `%s`",
					uniqueColumnName(ctx, col),
					uniqueColumnName(ctx, n.node.ColumnList()[idx]),
				),
			)
		}
	}
	return fmt.Sprintf(
		"SELECT %s FROM (%s)",
		strings.Join(columnMaps, ","),
		strings.Join(queries, fmt.Sprintf(" %s ", opType)),
	), nil
}

func (n *OrderByScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range n.node.ColumnList() {
		colName := uniqueColumnName(ctx, col)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(
				columns,
				fmt.Sprintf("`%s`", colName),
			)
		}
	}
	orderByColumns := []string{}
	for _, item := range n.node.OrderByItemList() {
		colName := uniqueColumnName(ctx, item.ColumnRef().Column())
		switch item.NullOrder() {
		case ast.NullOrderModeNullsFirst:
			orderByColumns = append(
				orderByColumns,
				fmt.Sprintf("(`%s` IS NOT NULL)", colName),
			)
		case ast.NullOrderModeNullsLast:
			orderByColumns = append(
				orderByColumns,
				fmt.Sprintf("(`%s` IS NULL)", colName),
			)
		}
		if item.IsDescending() {
			orderByColumns = append(orderByColumns, fmt.Sprintf("`%s` COLLATE zetasqlite_collate DESC", colName))
		} else {
			orderByColumns = append(orderByColumns, fmt.Sprintf("`%s` COLLATE zetasqlite_collate", colName))
		}
	}
	formattedInput, err := formatInput(input)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SELECT %s %s ORDER BY %s",
		strings.Join(columns, ","),
		formattedInput,
		strings.Join(orderByColumns, ","),
	), nil
}

func (n *LimitOffsetScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range n.node.ColumnList() {
		colName := uniqueColumnName(ctx, col)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(
				columns,
				fmt.Sprintf("`%s`", colName),
			)
		}
	}
	formattedInput, err := formatInput(input)
	if err != nil {
		return "", err
	}
	var limitExpr string
	if n.node.Limit() != nil {
		expr, err := newNode(n.node.Limit()).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		limitExpr = fmt.Sprintf("LIMIT %s", expr)
	}
	var offsetExpr string
	if n.node.Offset() != nil {
		expr, err := newNode(n.node.Offset()).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		offsetExpr = fmt.Sprintf("OFFSET %s", expr)
	}
	return fmt.Sprintf(
		"SELECT %s %s %s %s",
		strings.Join(columns, ","),
		formattedInput,
		limitExpr,
		offsetExpr,
	), nil
}

func (n *WithRefScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	tableName := n.node.WithQueryName()
	tableToColumnListMap := tableNameToColumnListMap(ctx)
	columnDefs := tableToColumnListMap[tableName]
	columns := n.node.ColumnList()
	if len(columnDefs) != len(columns) {
		return "", fmt.Errorf(
			"column num mismatch. defined column num is %d but used %d column",
			len(columnDefs), len(columns),
		)
	}
	formattedColumns := []string{}
	for i := 0; i < len(columnDefs); i++ {
		formattedColumns = append(
			formattedColumns,
			fmt.Sprintf("`%s` AS `%s`", uniqueColumnName(ctx, columnDefs[i]), uniqueColumnName(ctx, columns[i])),
		)
	}
	return fmt.Sprintf("(SELECT %s FROM `%s`)", strings.Join(formattedColumns, ","), tableName), nil
}

func (n *AnalyticScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	formattedInput, err := formatInput(input)
	if err != nil {
		return "", err
	}
	ctx = withAnalyticInputScan(ctx, formattedInput)
	orderColumnNames := analyticOrderColumnNamesFromContext(ctx)
	var scanOrderBy []*analyticOrderBy
	for _, group := range n.node.FunctionGroupList() {
		scanOrderBy = []*analyticOrderBy{}

		if group.PartitionBy() != nil {
			var partitionColumns []string
			for _, columnRef := range group.PartitionBy().PartitionByList() {
				colName := fmt.Sprintf("`%s`", uniqueColumnName(ctx, columnRef.Column()))
				partitionColumns = append(
					partitionColumns,
					colName,
				)
				order := &analyticOrderBy{
					column: colName,
					isAsc:  true,
				}
				orderColumnNames.values = append(orderColumnNames.values, order)
				scanOrderBy = append(scanOrderBy, order)
			}
			ctx = withAnalyticPartitionColumnNames(ctx, partitionColumns)
		}
		if group.OrderBy() != nil {
			for _, item := range group.OrderBy().OrderByItemList() {
				colName := uniqueColumnName(ctx, item.ColumnRef().Column())
				formattedColName := fmt.Sprintf("`%s`", colName)
				order := &analyticOrderBy{
					column: formattedColName,
					isAsc:  !item.IsDescending(),
				}
				orderColumnNames.values = append(orderColumnNames.values, order)
				scanOrderBy = append(scanOrderBy, order)
			}
		}
		if _, err := newNode(group).FormatSQL(ctx); err != nil {
			return "", err
		}

		// Reset context after each analytic function group
		orderColumnNames.values = []*analyticOrderBy{}
		ctx = withAnalyticPartitionColumnNames(ctx, nil)
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range n.node.ColumnList() {
		colName := uniqueColumnName(ctx, col)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(
				columns,
				fmt.Sprintf("`%s`", colName),
			)
		}
	}
	var orderColumnFormattedNames []string
	for _, col := range scanOrderBy {
		if col.isAsc {
			orderColumnFormattedNames = append(
				orderColumnFormattedNames,
				fmt.Sprintf("%s COLLATE zetasqlite_collate", col.column),
			)
		} else {
			orderColumnFormattedNames = append(
				orderColumnFormattedNames,
				fmt.Sprintf("%s COLLATE zetasqlite_collate DESC", col.column),
			)
		}
	}
	var orderBy string
	if len(orderColumnFormattedNames) != 0 {
		orderBy = fmt.Sprintf("ORDER BY %s", strings.Join(orderColumnFormattedNames, ","))
	}
	orderColumnNames.values = []*analyticOrderBy{}
	return fmt.Sprintf(
		"SELECT %s FROM (SELECT *, ROW_NUMBER() OVER() AS `row_id` %s) %s",
		strings.Join(columns, ","),
		formattedInput,
		orderBy,
	), nil
}

func (n *SampleScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ComputedColumnNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	expr, err := newNode(n.node.Expr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	col := n.node.Column()
	uniqueName := uniqueColumnName(ctx, col)
	query := fmt.Sprintf("%s AS `%s`", expr, uniqueColumnName(ctx, col))
	columnMap := columnRefMap(ctx)
	columnMap[uniqueName] = query
	arraySubqueryColumnNames := arraySubqueryColumnNameFromContext(ctx)
	if arraySubqueryColumnNames != nil {
		arraySubqueryColumnNames.names = append(arraySubqueryColumnNames.names, fmt.Sprintf("`%s`", col.Name()))
	}
	return query, nil
}

func (n *OrderByItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ColumnAnnotationsNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GeneratedColumnInfoNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ColumnDefaultValueNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ColumnDefinitionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *PrimaryKeyNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ForeignKeyNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CheckConstraintNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *OutputColumnNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	columnMap := columnRefMap(ctx)
	col := n.node.Column()
	uniqueName := uniqueColumnName(ctx, col)
	if ref, exists := columnMap[uniqueName]; exists {
		return ref, nil
	}
	return fmt.Sprintf("`%s`", col.Name()), nil
}

func (n *ProjectScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	for _, col := range n.node.ExprList() {
		// assign expr to columnRefMap
		if _, err := newNode(col).FormatSQL(ctx); err != nil {
			return "", err
		}
	}
	input, err := newNode(n.node.InputScan()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	columns := []string{}
	columnMap := columnRefMap(ctx)
	for _, col := range n.node.ColumnList() {
		colName := uniqueColumnName(ctx, col)
		if ref, exists := columnMap[colName]; exists {
			columns = append(columns, ref)
			delete(columnMap, colName)
		} else {
			columns = append(
				columns,
				fmt.Sprintf("`%s`", colName),
			)
		}
	}
	formattedInput, err := formatInput(input)
	if err != nil {
		return "", err
	}
	formattedColumns := strings.Join(columns, ",")
	return fmt.Sprintf("SELECT %s %s", formattedColumns, formattedInput), nil
}

func (n *TVFScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GroupRowsScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FunctionArgumentNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExplainStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

// FormatSQL Formats the outermost query statement that runs and produces rows of output, like a SELECT
// The node's `OutputColumnList()` gives user-visible column names that should be returned. There may be duplicate names,
// and multiple output columns may reference the same column from `Query()`
// https://github.com/google/zetasql/blob/master/docs/resolved_ast.md#ResolvedQueryStmt
func (n *QueryStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	input, err := newNode(n.node.Query()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}

	var columns []string
	for _, outputColumnNode := range n.node.OutputColumnList() {
		columns = append(
			columns,
			fmt.Sprintf("`%s` AS `%s`",
				uniqueColumnName(ctx, outputColumnNode.Column()),
				outputColumnNode.Name(),
			),
		)
	}

	return fmt.Sprintf(
		"SELECT %s FROM (%s)",
		strings.Join(columns, ", "),
		input,
	), nil
}

func (n *CreateDatabaseStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *IndexItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *UnnestItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateIndexStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateSchemaStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateTableAsSelectStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateModelStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WithPartitionColumnsNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateSnapshotTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateExternalTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExportModelStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExportDataStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DefineTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DescribeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ShowStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *BeginStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetTransactionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CommitStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RollbackStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *StartBatchStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RunBatchStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AbortBatchStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	namePath := namePathFromContext(ctx)
	tableName := namePath.format(n.node.NamePath())
	objectType := n.node.ObjectType()
	if n.node.IsIfExists() {
		return fmt.Sprintf("DROP %s IF EXISTS `%s`", objectType, tableName), nil
	}
	return fmt.Sprintf("DROP %s `%s`", objectType, tableName), nil
}

func (n *DropMaterializedViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropSnapshotTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RecursiveRefScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RecursiveScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WithScanNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	queries := []string{}
	for _, entry := range n.node.WithEntryList() {
		sql, err := newNode(entry).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		queries = append(queries, sql)
	}
	query, err := newNode(n.node.Query()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"WITH %s %s",
		strings.Join(queries, ", "),
		query,
	), nil
}

func (n *WithEntryNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	queryName := n.node.WithQueryName()
	subquery, err := newNode(n.node.WithSubquery()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	tableToColumnList := tableNameToColumnListMap(ctx)
	tableToColumnList[queryName] = n.node.WithSubquery().ColumnList()
	return fmt.Sprintf("%s AS ( %s )", queryName, subquery), nil
}

func (n *OptionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WindowPartitioningNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WindowOrderingNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *WindowFrameNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AnalyticFunctionGroupNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}

	var queries []string
	for _, column := range n.node.AnalyticFunctionList() {
		sql, err := newNode(column).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		queries = append(queries, sql)
	}
	return strings.Join(queries, ","), nil
}

func (n *WindowFrameExprNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DMLValueNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	return newNode(n.node.Value()).FormatSQL(ctx)
}

func (n *DMLDefaultNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AssertStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AssertRowsModifiedNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *InsertRowNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	values := []string{}
	for _, value := range n.node.ValueList() {
		sql, err := newNode(value).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		values = append(values, sql)
	}
	return strings.Join(values, ","), nil
}

func (n *InsertStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	table, err := getTableName(ctx, n.node.TableScan())
	if err != nil {
		return "", err
	}
	columns := []string{}
	for _, col := range n.node.InsertColumnList() {
		columns = append(columns, fmt.Sprintf("`%s`", col.Name()))
	}
	query := n.node.Query()
	if query != nil {
		stmt, err := newNode(query).FormatSQL(withUseColumnID(ctx))
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("INSERT INTO `%s` (%s) %s",
			table,
			strings.Join(columns, ","),
			stmt,
		), nil
	}
	rows := []string{}
	for _, row := range n.node.RowList() {
		sql, err := newNode(row).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		rows = append(rows, fmt.Sprintf("(%s)", sql))
	}
	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		table,
		strings.Join(columns, ","),
		strings.Join(rows, ","),
	), nil
}

func (n *DeleteStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	table, err := getTableName(ctx, n.node.TableScan())
	if err != nil {
		return "", err
	}
	where, err := newNode(n.node.WhereExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"DELETE FROM `%s` WHERE %s",
		table,
		where,
	), nil
}

func (n *UpdateItemNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	target, err := newNode(n.node.Target()).FormatSQL(unuseColumnID(withoutUseTableNameForColumn(ctx)))
	if err != nil {
		return "", err
	}
	setValue, err := newNode(n.node.SetValue()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s=%s", target, setValue), nil
}

func (n *UpdateArrayItemNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *UpdateStmtNode) FormatSQL(ctx context.Context) (string, error) {
	if n == nil {
		return "", nil
	}
	table, err := getTableName(ctx, n.node.TableScan())
	if err != nil {
		return "", err
	}
	updateItems := []string{}
	for _, item := range n.node.UpdateItemList() {
		sql, err := newNode(item).FormatSQL(ctx)
		if err != nil {
			return "", err
		}
		updateItems = append(updateItems, sql)
	}
	where, err := newNode(n.node.WhereExpr()).FormatSQL(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"UPDATE `%s` SET %s WHERE %s",
		table,
		strings.Join(updateItems, ","),
		where,
	), nil
}

func (n *MergeWhenNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *MergeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *TruncateStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ObjectUnitNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *PrivilegeNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GrantStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RevokeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterDatabaseStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterMaterializedViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterSchemaStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterTableStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetOptionsActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AddColumnActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AddConstraintActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropConstraintActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropPrimaryKeyActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterColumnOptionsActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterColumnDropNotNullActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterColumnSetDataTypeActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterColumnSetDefaultActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterColumnDropDefaultActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropColumnActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RenameColumnActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetAsActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *SetCollateClauseNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterTableSetOptionsStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RenameStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreatePrivilegeRestrictionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateRowAccessPolicyStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropPrivilegeRestrictionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropRowAccessPolicyStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropSearchIndexStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *GrantToActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RestrictToActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AddToRestricteeListActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RemoveFromRestricteeListActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FilterUsingActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RevokeFromActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RenameToActionNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterPrivilegeRestrictionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterRowAccessPolicyStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterAllRowAccessPoliciesStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateConstantStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ArgumentDefNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ArgumentRefNode) FormatSQL(ctx context.Context) (string, error) {
	if n.node == nil {
		return "", nil
	}
	return fmt.Sprintf("@%s", n.node.Name()), nil
}

func (n *CreateTableFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *RelationArgumentScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ArgumentListNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *FunctionSignatureHolderNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *DropTableFunctionStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CallStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ImportStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ModuleStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AggregateHavingModifierNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateMaterializedViewStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateProcedureStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExecuteImmediateArgumentNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ExecuteImmediateStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AssignmentStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CreateEntityStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AlterEntityStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *PivotColumnNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *PivotScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *ReturningClauseNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *UnpivotArgNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *UnpivotScanNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *CloneDataStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *TableAndColumnInfoNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AnalyzeStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}

func (n *AuxLoadDataStmtNode) FormatSQL(ctx context.Context) (string, error) {
	return "", nil
}
