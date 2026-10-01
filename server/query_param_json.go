package server

import (
	"bytes"

	"github.com/goccy/go-json"
)

// normalizeQueryParameterJSON rewrites every scalar "value" that a
// parameterValue tree in a request body carries as a JSON number into its
// string form, so that the body decodes into bigqueryv2.QueryParameterValue,
// whose Value is a string. The Python client sends FLOAT64 values as JSON
// numbers, at the top level and inside ARRAY and STRUCT parameters. The
// body is returned unchanged when it holds no such value.
func normalizeQueryParameterJSON(body []byte) []byte {
	if !bytes.Contains(body, []byte(`"parameterValue"`)) {
		return body
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var root any
	if err := dec.Decode(&root); err != nil {
		return body
	}
	if !normalizeParameterValues(root) {
		return body
	}
	out, err := json.Marshal(root)
	if err != nil {
		return body
	}
	return out
}

// normalizeParameterValues finds every "parameterValue" object under node
// and normalizes it, reporting whether anything changed.
func normalizeParameterValues(node any) bool {
	changed := false
	switch n := node.(type) {
	case map[string]any:
		for k, v := range n {
			if pv, ok := v.(map[string]any); ok && k == "parameterValue" {
				changed = stringifyParameterValue(pv) || changed
				continue
			}
			changed = normalizeParameterValues(v) || changed
		}
	case []any:
		for _, v := range n {
			changed = normalizeParameterValues(v) || changed
		}
	}
	return changed
}

// stringifyParameterValue converts a JSON number "value" in pv, and in
// every arrayValues / structValues entry below it, into a string.
func stringifyParameterValue(pv map[string]any) bool {
	changed := false
	if n, ok := pv["value"].(json.Number); ok {
		pv["value"] = n.String()
		changed = true
	}
	if arr, ok := pv["arrayValues"].([]any); ok {
		for _, e := range arr {
			if m, ok := e.(map[string]any); ok {
				changed = stringifyParameterValue(m) || changed
			}
		}
	}
	if st, ok := pv["structValues"].(map[string]any); ok {
		for _, e := range st {
			if m, ok := e.(map[string]any); ok {
				changed = stringifyParameterValue(m) || changed
			}
		}
	}
	return changed
}
