package zetasqlite

import (
	"context"
	"time"

	"github.com/glassmonkey/zetasql-wasm"
	parsed_ast "github.com/glassmonkey/zetasql-wasm/ast"
	"github.com/glassmonkey/zetasql-wasm/resolved_ast"
	ast "github.com/glassmonkey/zetasql-wasm/resolved_ast"
)

type (
	analyzerKey                     struct{}
	namePathKey                     struct{}
	nodeMapKey                      struct{}
	columnRefMapKey                 struct{}
	funcMapKey                      struct{}
	analyticOrderColumnNamesKey     struct{}
	analyticPartitionColumnNamesKey struct{}
	analyticInputScanKey            struct{}
	arraySubqueryColumnNameKey      struct{}
	currentTimeKey                  struct{}
	tableNameToColumnListMapKey     struct{}
	useColumnIDKey                  struct{}
	useTableNameForColumnKey        struct{}
)

func analyzerFromContext(ctx context.Context) *Analyzer {
	value := ctx.Value(analyzerKey{})
	if value == nil {
		return nil
	}
	return value.(*Analyzer)
}

func withAnalyzer(ctx context.Context, analyzer *Analyzer) context.Context {
	return context.WithValue(ctx, analyzerKey{}, analyzer)
}

func namePathFromContext(ctx context.Context) *NamePath {
	value := ctx.Value(namePathKey{})
	if value == nil {
		return nil
	}
	return value.(*NamePath)
}

func withNamePath(ctx context.Context, namePath *NamePath) context.Context {
	return context.WithValue(ctx, namePathKey{}, namePath)
}

// nodeMap is a fork-side wrapper over zetasql-wasm's NodeMap. It exists
// so the formatter can use the historical FindNodeFromResolvedNode shape
// (returning a slice of parsed AST nodes) while delegating the actual
// lookup to zetasql-wasm's FindParsedNodes.
type nodeMap struct {
	resolved *zetasql.NodeMap
}

// FindNodeFromResolvedNode returns the parsed AST nodes whose source
// range overlaps the given resolved node, by delegating to zetasql-wasm's
// FindParsedNodes (added in v0.7.0).
func (m *nodeMap) FindNodeFromResolvedNode(n resolved_ast.Node) []parsed_ast.Node {
	if m == nil || m.resolved == nil {
		return nil
	}
	return m.resolved.FindParsedNodes(n)
}

func withNodeMap(ctx context.Context, m *nodeMap) context.Context {
	return context.WithValue(ctx, nodeMapKey{}, m)
}

func nodeMapFromContext(ctx context.Context) *nodeMap {
	value := ctx.Value(nodeMapKey{})
	if value == nil {
		return nil
	}
	return value.(*nodeMap)
}

func withColumnRefMap(ctx context.Context, m map[string]string) context.Context {
	return context.WithValue(ctx, columnRefMapKey{}, m)
}

func columnRefMap(ctx context.Context) map[string]string {
	value := ctx.Value(columnRefMapKey{})
	if value == nil {
		return nil
	}
	return value.(map[string]string)
}

func withFuncMap(ctx context.Context, m map[string]*FunctionSpec) context.Context {
	return context.WithValue(ctx, funcMapKey{}, m)
}

func funcMapFromContext(ctx context.Context) map[string]*FunctionSpec {
	value := ctx.Value(funcMapKey{})
	if value == nil {
		return nil
	}
	return value.(map[string]*FunctionSpec)
}

type analyticOrderBy struct {
	column string
	isAsc  bool
}

type analyticOrderColumnNames struct {
	values []*analyticOrderBy
}

func withAnalyticOrderColumnNames(ctx context.Context, v *analyticOrderColumnNames) context.Context {
	return context.WithValue(ctx, analyticOrderColumnNamesKey{}, v)
}

func analyticOrderColumnNamesFromContext(ctx context.Context) *analyticOrderColumnNames {
	value := ctx.Value(analyticOrderColumnNamesKey{})
	if value == nil {
		return nil
	}
	return value.(*analyticOrderColumnNames)
}

func withAnalyticPartitionColumnNames(ctx context.Context, names []string) context.Context {
	return context.WithValue(ctx, analyticPartitionColumnNamesKey{}, names)
}

func analyticPartitionColumnNamesFromContext(ctx context.Context) []string {
	value := ctx.Value(analyticPartitionColumnNamesKey{})
	if value == nil {
		return nil
	}
	return value.([]string)
}

func withAnalyticInputScan(ctx context.Context, input string) context.Context {
	return context.WithValue(ctx, analyticInputScanKey{}, input)
}

func analyticInputScanFromContext(ctx context.Context) string {
	value := ctx.Value(analyticInputScanKey{})
	if value == nil {
		return ""
	}
	return value.(string)
}

type arraySubqueryColumnNames struct {
	names []string
}

func withArraySubqueryColumnName(ctx context.Context, v *arraySubqueryColumnNames) context.Context {
	return context.WithValue(ctx, arraySubqueryColumnNameKey{}, v)
}

func arraySubqueryColumnNameFromContext(ctx context.Context) *arraySubqueryColumnNames {
	value := ctx.Value(arraySubqueryColumnNameKey{})
	if value == nil {
		return nil
	}
	return value.(*arraySubqueryColumnNames)
}

func withUseColumnID(ctx context.Context) context.Context {
	return context.WithValue(ctx, useColumnIDKey{}, true)
}

func useColumnID(ctx context.Context) bool {
	value := ctx.Value(useColumnIDKey{})
	if value == nil {
		return false
	}
	return value.(bool)
}

func unuseColumnID(ctx context.Context) context.Context {
	return context.WithValue(ctx, useColumnIDKey{}, false)
}

func withoutUseTableNameForColumn(ctx context.Context) context.Context {
	return context.WithValue(ctx, useTableNameForColumnKey{}, false)
}

func useTableNameForColumn(ctx context.Context) bool {
	value := ctx.Value(useTableNameForColumnKey{})
	if value == nil {
		return false
	}
	return value.(bool)
}

func withTableNameToColumnListMap(ctx context.Context, v map[string][]*ast.Column) context.Context {
	return context.WithValue(ctx, tableNameToColumnListMapKey{}, v)
}

func tableNameToColumnListMap(ctx context.Context) map[string][]*ast.Column {
	value := ctx.Value(tableNameToColumnListMapKey{})
	if value == nil {
		return nil
	}
	return value.(map[string][]*ast.Column)
}

func WithCurrentTime(ctx context.Context, now time.Time) context.Context {
	return context.WithValue(ctx, currentTimeKey{}, &now)
}

func CurrentTime(ctx context.Context) *time.Time {
	value := ctx.Value(currentTimeKey{})
	if value == nil {
		return nil
	}
	return value.(*time.Time)
}
