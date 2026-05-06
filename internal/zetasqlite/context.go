package zetasqlite

import (
	"context"
	"time"

	"github.com/glassmonkey/zetasql-wasm"
	parsed_ast "github.com/glassmonkey/zetasql-wasm/ast"
	"github.com/glassmonkey/zetasql-wasm/resolved_ast"
	"github.com/glassmonkey/zetasql-wasm/wasm/generated"
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

// nodeMap is a fork-side wrapper over zetasql-wasm's parse-location-indexed
// NodeMap that adds the resolved->parsed reverse lookup go-zetasqlite
// formatters depend on (e.g. to recover the original table/function path
// text from a resolved node). The reverse direction is not yet implemented
// — for now the wrapper delegates the forward direction and stubs the
// reverse with an empty result.
type nodeMap struct {
	resolved *zetasql.NodeMap
}

// FindNodeFromResolvedNode would return the parsed AST nodes whose source
// range overlaps the given resolved node. zetasql-wasm doesn't expose a
// parsed-side NodeMap yet, so this returns nil. Callers must tolerate an
// empty slice (the formatter currently surfaces it as an error).
//
// TODO(zetasql-wasm-migration): once parser-AST location indexing is in
// place upstream, build the reverse map at NewNodeMap time and consult it
// here.
func (m *nodeMap) FindNodeFromResolvedNode(_ resolved_ast.Node) []parsed_ast.Node {
	return nil
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

func withTableNameToColumnListMap(ctx context.Context, v map[string][]*generated.ResolvedColumnProto) context.Context {
	return context.WithValue(ctx, tableNameToColumnListMapKey{}, v)
}

func tableNameToColumnListMap(ctx context.Context) map[string][]*generated.ResolvedColumnProto {
	value := ctx.Value(tableNameToColumnListMapKey{})
	if value == nil {
		return nil
	}
	return value.(map[string][]*generated.ResolvedColumnProto)
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
