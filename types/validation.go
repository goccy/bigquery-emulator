package types

import (
	"sort"
	"strings"

	"github.com/go-playground/validator/v10"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

// ValidateRowFields returns, sorted, the names of the fields present in an
// insertAll row that are not declared in the table schema. An empty result
// means every field in the row is known. BigQuery rejects rows with unknown
// fields unless ignoreUnknownValues is set.
func ValidateRowFields[V any](schema *bigqueryv2.TableSchema, row map[string]V) []string {
	if schema == nil {
		return nil
	}
	known := make(map[string]struct{}, len(schema.Fields))
	for _, field := range schema.Fields {
		known[field.Name] = struct{}{}
	}
	var unknown []string
	for name := range row {
		if _, ok := known[name]; !ok {
			unknown = append(unknown, name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func TypeValidation(fl validator.FieldLevel) bool {
	return Type(fl.Field().String()).TypeKind().String() != ""
}

func ModeValidation(fl validator.FieldLevel) bool {
	mode := fl.Field().String()
	if mode == "" {
		return true
	}
	switch strings.ToLower(mode) {
	case strings.ToLower(string(NullableMode)):
		return true
	case strings.ToLower(string(RequiredMode)):
		return true
	case strings.ToLower(string(RepeatedMode)):
		return true
	}
	return false
}

func RegisterTypeValidation(v *validator.Validate) {
	v.RegisterValidation("type", TypeValidation)
	v.RegisterValidation("mode", ModeValidation)
}
