package main

import (
	"bytes"
	"context"
	"fmt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"log"
	"math"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
)

const (
	envProjID      = "GCLOUD_TESTS_GOLANG_PROJECT_ID"
	envPrivateKey  = "GCLOUD_TESTS_GOLANG_KEY"
	envImpersonate = "GCLOUD_TESTS_IMPERSONATE_CREDENTIALS"
)

// ProjID returns the project ID to use in integration tests, or the empty
// string if none is configured.
func ProjID() string {
	return os.Getenv(envProjID)
}

// Credentials returns the credentials to use in integration tests, or nil if
// none is configured. It uses the standard environment variable for tests in
// this repo.
func Credentials(ctx context.Context, scopes ...string) *google.Credentials {
	return CredentialsEnv(ctx, envPrivateKey, scopes...)
}

// CredentialsEnv returns the credentials to use in integration tests, or nil
// if none is configured. If the environment variable is unset, CredentialsEnv
// will try to find 'Application Default Credentials'. Else, CredentialsEnv
// will return nil. CredentialsEnv will log.Fatal if the token source is
// specified but missing or invalid.
func CredentialsEnv(ctx context.Context, envVar string, scopes ...string) *google.Credentials {
	key := os.Getenv(envVar)
	if key == "" { // Try for application default credentials.
		creds, err := google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			log.Println("No 'Application Default Credentials' found.")
			return nil
		}
		return creds
	}

	data, err := os.ReadFile(key)
	if err != nil {
		log.Fatal(err)
	}

	creds, err := google.CredentialsFromJSON(ctx, data, scopes...)
	if err != nil {
		log.Fatal(err)
	}
	return creds
}

// TokenSource returns the OAuth2 token source to use in integration tests,
// or nil if none is configured. It uses the standard environment variable
// for tests in this repo.
func TokenSource(ctx context.Context, scopes ...string) oauth2.TokenSource {
	return TokenSourceEnv(ctx, envPrivateKey, scopes...)
}

// TokenSourceEnv returns the OAuth2 token source to use in integration tests. or nil
// if none is configured. It tries to get credentials from the filename in the
// environment variable envVar. If the environment variable is unset, TokenSourceEnv
// will try to find 'Application Default Credentials'. Else, TokenSourceEnv will
// return nil. TokenSourceEnv will log.Fatal if the token source is specified but
// missing or invalid.
func TokenSourceEnv(ctx context.Context, envVar string, scopes ...string) oauth2.TokenSource {
	key := os.Getenv(envVar)
	if key == "" { // Try for application default credentials.
		ts, err := google.DefaultTokenSource(ctx, scopes...)
		if err != nil {
			log.Println("No 'Application Default Credentials' found.")
			return nil
		}
		return ts
	}
	conf, err := jwtConfigFromFile(key, scopes)
	if err != nil {
		log.Fatal(err)
	}
	return conf.TokenSource(ctx)
}

// jwtConfigFromFile reads the given JSON private key file, and returns the
// jwt.Config it contains.
// If the filename is empty, it returns (nil, nil).
func jwtConfigFromFile(filename string, scopes []string) (*jwt.Config, error) {
	if filename == "" {
		return nil, nil
	}
	jsonKey, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot read the JSON key file, err: %v", err)
	}
	conf, err := google.JWTConfigFromJSON(jsonKey, scopes...)
	if err != nil {
		return nil, fmt.Errorf("google.JWTConfigFromJSON: %v", err)
	}
	return conf, nil
}

// validateTableConstraints is used to validate properties of a table by computing stats using the query engine.
func validateTableConstraints(ctx context.Context, t *testing.T, client *bigquery.Client, table *bigquery.Table, description string, opts ...constraintOption) {
	vi := &validationInfo{
		constraints: make(map[string]*constraint),
	}

	for _, o := range opts {
		o(vi)
	}

	if len(vi.constraints) == 0 {
		t.Errorf("%q: no constraints were specified", description)
		return
	}

	sql := new(bytes.Buffer)
	sql.WriteString("SELECT\n")
	var i int
	for _, c := range vi.constraints {
		if i > 0 {
			sql.WriteString(",\n")
		}
		sql.WriteString(c.projection)
		i++
	}
	sql.WriteString(fmt.Sprintf("\nFROM `%s`.%s.%s", table.ProjectID, table.DatasetID, table.TableID))
	q := client.Query(sql.String())
	it, err := q.Read(ctx)
	if err != nil {
		t.Errorf("%q: failed to issue validation query: %v\nSQL: %s", description, err, sql.String())
		return
	}
	var resultrow []bigquery.Value
	err = it.Next(&resultrow)
	if err != nil {
		t.Errorf("%q: failed to get result row: %v", description, err)
		return
	}

	for colname, con := range vi.constraints {
		off := -1
		for k, v := range it.Schema {
			if v.Name == colname {
				off = k
				break
			}
		}
		if off == -1 {
			t.Errorf("%q: missing constraint %q from results", description, colname)
			continue
		}
		val, ok := resultrow[off].(int64)
		if !ok {
			t.Errorf("%q: constraint %q type mismatch", description, colname)
		}
		if con.allowedError == 0 {
			if val != con.expectedValue {
				t.Errorf("%q: constraint %q mismatch, got %d want %d (%s)", description, colname, val, con.expectedValue, it.SourceJob().ID())
			}
			continue
		}
		res := val - con.expectedValue
		if res < 0 {
			res = -res
		}
		if res > con.allowedError {
			t.Errorf("%q: constraint %q outside error bound %d, got %d want %d", description, colname, con.allowedError, val, con.expectedValue)
		}
	}
}

// constraint is a specific table constraint.
type constraint struct {
	// sql fragment that projects a result value
	projection string

	// all validation constraints must eval as int64.
	expectedValue int64

	// if nonzero, the constraint value must be within allowedError distance of expectedValue.
	allowedError int64
}

// validationInfo is keyed by the result column name.
type validationInfo struct {
	constraints map[string]*constraint
}

// constraintOption is for building validation rules.
type constraintOption func(*validationInfo)

// withExactRowCount asserts the exact total row count of the table.
func withExactRowCount(totalRows int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := "total_rows"
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("COUNT(1) AS `%s`", resultCol),
			expectedValue: totalRows,
		}
	}
}

// withNullCount asserts the number of null values in a column.
func withNullCount(colname string, nullCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("nullcol_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("SUM(IF(`%s` IS NULL,1,0)) AS `%s`", colname, resultCol),
			expectedValue: nullCount,
		}
	}
}

// withNonNullCount asserts the number of non null values in a column.
func withNonNullCount(colname string, nonNullCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("nonnullcol_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("SUM(IF(`%s` IS NOT NULL,1,0)) AS `%s`", colname, resultCol),
			expectedValue: nonNullCount,
		}
	}
}

// withDistinctValues validates the exact cardinality of a column.
func withDistinctValues(colname string, distinctVals int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("distinct_count_%s", strings.ReplaceAll(colname, ".", "_"))
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("COUNT(DISTINCT %s) AS `%s`", colname, resultCol),
			expectedValue: distinctVals,
		}
	}
}

// withApproxDistinctValues validates the approximate cardinality of a column with an error bound.
func withApproxDistinctValues(colname string, approxValues int64, errorBound int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("distinct_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("APPROX_COUNT_DISTINCT(`%s`) AS `%s`", colname, resultCol),
			expectedValue: approxValues,
			allowedError:  errorBound,
		}
	}
}

// withIntegerValueCount validates how many values in the column have a given integer value.
func withIntegerValueCount(colname string, wantValue int64, valueCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("integer_value_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("COUNTIF(`%s` = %d) AS `%s`", colname, wantValue, resultCol),
			expectedValue: valueCount,
		}
	}
}

// withStringValueCount validates how many values in the column have a given string value.
func withStringValueCount(colname string, wantValue string, valueCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("string_value_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("COUNTIF(`%s` = \"%s\") AS `%s`", colname, wantValue, resultCol),
			expectedValue: valueCount,
		}
	}
}

// withBoolValueCount validates how many values in the column have a given boolean value.
func withBoolValueCount(colname string, wantValue bool, valueCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("bool_value_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("COUNTIF(`%s` = %t) AS `%s`", colname, wantValue, resultCol),
			expectedValue: valueCount,
		}
	}
}

// withBytesValueCount validates how many values in the column have a given bytes value.
func withBytesValueCount(colname string, wantValue []byte, valueCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("bytes_value_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("COUNTIF(`%s` = B\"%s\") AS `%s`", colname, wantValue, resultCol),
			expectedValue: valueCount,
		}
	}
}

// withFloatValueCount validates how many values in the column have a given floating point value, with a
// reasonable error bound due to precision loss.
func withFloatValueCount(colname string, wantValue float64, valueCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("float_value_count_%s", colname)
		projection := fmt.Sprintf("COUNTIF((ABS(`%s`) - ABS(%f))/ABS(%f) < 0.0001) AS `%s`", colname, wantValue, wantValue, resultCol)
		switch wantValue {
		case math.Inf(0):
			// special case for infinities.
			projection = fmt.Sprintf("COUNTIF(IS_INF(`%s`)) as `%s`", colname, resultCol)
		case math.NaN():
			projection = fmt.Sprintf("COUNTIF(IS_NAN(%s)) as `%s`", colname, resultCol)
		case 0:
			projection = fmt.Sprintf("COUNTIF(SIGN(`%s`) = 0) as `%s`", colname, resultCol)
		}
		vi.constraints[resultCol] = &constraint{
			projection:    projection,
			expectedValue: valueCount,
		}
	}
}

// withArrayLength validates how many rows in an ARRAY column have a given length.
func withArrayLength(colname string, wantLen int64, wantCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("arraylength_value_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("COUNTIF(ARRAY_LENGTH(`%s`) = %d) as `%s`", colname, wantLen, resultCol),
			expectedValue: wantCount,
		}
	}
}

// withDistinctArrayValues validates how many elements of an ARRAY column have a given cardinality.
func withDistinctArrayValues(colname string, distinctVals, wantCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("distinct_array_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("COUNTIF(ARRAY_LENGTH(ARRAY(SELECT DISTINCT element FROM UNNEST(`%s`) as element)) = %d) AS `%s`", colname, distinctVals, resultCol),
			expectedValue: wantCount,
		}
	}
}

// withIntegerArraySum validates the total sum of values in an ARRAY<INT64?> column.
func withIntegerArraySum(colname string, arraySum int64, wantCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("arraysum_int64_value_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("COUNTIF((SELECT SUM(elem) FROM UNNEST(`%s`) as elem) = %d) as `%s`", colname, arraySum, resultCol),
			expectedValue: wantCount,
		}
	}
}

// withFloatArraySum validates how many rows in an an ARRAY<INT64?> column have a given sum, within an error bound.
func withFloatArraySum(colname string, floatSum float64, wantCount int64) constraintOption {
	return func(vi *validationInfo) {
		resultCol := fmt.Sprintf("arraysum_float_value_count_%s", colname)
		vi.constraints[resultCol] = &constraint{
			projection:    fmt.Sprintf("COUNTIF(((SELECT ABS(SUM(elem)) FROM UNNEST(`%s`) as elem) - ABS(%f)) / ABS(%f) < 0.0001) as `%s`", colname, floatSum, floatSum, resultCol),
			expectedValue: wantCount,
		}
	}
}
