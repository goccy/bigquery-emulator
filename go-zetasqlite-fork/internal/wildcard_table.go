package internal

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-zetasql/types"
)

const tableSuffixColumnName = "_TABLE_SUFFIX"

func (c *Catalog) isWildcardTable(path []string) bool {
	if len(path) == 0 {
		return false
	}
	lastPath := path[len(path)-1]
	if lastPath == "" {
		return false
	}
	lastChar := lastPath[len(lastPath)-1]
	return lastChar == '*'
}

type WildcardTable struct {
	spec   *TableSpec
	tables []*TableSpec
	prefix string
}

func (t *WildcardTable) existsColumn(table *TableSpec, column string) bool {
	for _, col := range table.Columns {
		if col.Name == column {
			return true
		}
	}
	return false
}

func (t *WildcardTable) FormatSQL(ctx context.Context) (string, error) {
	queries := make([]string, 0, len(t.tables))
	for _, table := range t.tables {
		var columns []string
		for _, column := range t.spec.Columns {
			if column.Name == tableSuffixColumnName {
				continue
			}
			if t.existsColumn(table, column.Name) {
				columns = append(columns, fmt.Sprintf("`%s`", column.Name))
			} else {
				columns = append(columns, fmt.Sprintf("NULL as %s", column.Name))
			}
		}
		fullName := strings.Join(table.NamePath, ".")
		if len(fullName) <= len(t.prefix) {
			return "", fmt.Errorf("failed to find table suffix from %s", fullName)
		}
		tableSuffix := fullName[len(t.prefix):]
		encodedSuffix, err := EncodeGoValue(types.StringType(), tableSuffix)
		if err != nil {
			return "", err
		}
		queries = append(queries,
			fmt.Sprintf(
				"SELECT %s, '%s' as _TABLE_SUFFIX FROM `%s`",
				strings.Join(columns, ","),
				encodedSuffix,
				table.TableName(),
			),
		)
	}

	return strings.Join(queries, " UNION ALL "), nil
}

func (t *WildcardTable) Name() string {
	return strings.Join(t.spec.NamePath, ".")
}

func (t *WildcardTable) FullName() string {
	return t.spec.TableName()
}

func (t *WildcardTable) NumColumns() int {
	return len(t.spec.Columns)
}

func (t *WildcardTable) Column(idx int) types.Column {
	column := t.spec.Columns[idx]
	typ, err := column.Type.ToZetaSQLType()
	if err != nil {
		return nil
	}
	return types.NewSimpleColumn(
		strings.Join(t.spec.NamePath, "."), column.Name, typ,
	)
}

func (t *WildcardTable) PrimaryKey() []int {
	return nil
}

func (t *WildcardTable) FindColumnByName(name string) types.Column {
	for _, col := range t.spec.Columns {
		if col.Name == name {
			typ, err := col.Type.ToZetaSQLType()
			if err != nil {
				return nil
			}
			return types.NewSimpleColumn(
				t.spec.TableName(), col.Name, typ,
			)
		}
	}
	return nil
}

func (t *WildcardTable) IsValueTable() bool {
	return false
}

func (t *WildcardTable) SerializationID() int64 {
	return 0
}

func (t *WildcardTable) CreateEvaluatorTableIterator(columnIdxs []int) (*types.EvaluatorTableIterator, error) {
	return nil, nil
}

func (t *WildcardTable) AnonymizationInfo() *types.AnonymizationInfo {
	return nil
}

func (t *WildcardTable) SupportsAnonymization() bool {
	return false
}

func (t *WildcardTable) TableTypeName(mode types.ProductMode) string {
	return ""
}

func (c *Catalog) createWildcardTable(path []string) (types.Table, error) {
	name := strings.Join(path, "_")
	name = strings.TrimRight(name, "*")
	re, err := regexp.Compile(name)
	if err != nil {
		return nil, fmt.Errorf("failed to compile %s: %w", name, err)
	}
	matchedSpecs := make([]*TableSpec, 0, len(c.tableMap))
	for name, spec := range c.tableMap {
		if re.MatchString(name) {
			matchedSpecs = append(matchedSpecs, spec)
		}
	}
	sort.Slice(matchedSpecs, func(i, j int) bool {
		return matchedSpecs[i].CreatedAt.UnixNano() > matchedSpecs[j].CreatedAt.UnixNano()
	})
	if len(matchedSpecs) == 0 {
		return nil, fmt.Errorf("failed to find matched tables by wildcard")
	}

	spec := matchedSpecs[0]
	wildcardTable := new(TableSpec)
	*wildcardTable = *spec
	wildcardTable.NamePath = append([]string{}, spec.NamePath...)
	wildcardTable.Columns = append(wildcardTable.Columns, &ColumnSpec{
		Name: tableSuffixColumnName,
		Type: &Type{Kind: types.STRING},
	})
	lastNamePath := spec.NamePath[len(spec.NamePath)-1]
	lastNamePath = lastNamePath[:len(path)-1]
	wildcardTable.NamePath[len(spec.NamePath)-1] = fmt.Sprintf(
		"%s_wildcard_%d", lastNamePath, time.Now().Unix(),
	)

	// firstIdentifier may be omitted, so we need to check it.
	prefix := name
	firstIdentifier := spec.NamePath[0]
	if !strings.HasPrefix(prefix, firstIdentifier+".") {
		prefix = firstIdentifier + "." + prefix
	}

	return &WildcardTable{
		spec:   wildcardTable,
		tables: matchedSpecs,
		prefix: prefix,
	}, nil
}
