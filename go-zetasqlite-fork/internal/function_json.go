package internal

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/goccy/go-json"
)

func JSON_FIELD(v, fieldName string) (Value, error) {
	p, err := json.CreatePath(fmt.Sprintf(`$.%q`, fieldName))
	if err != nil {
		return nil, err
	}
	extracted, err := p.Extract([]byte(v))
	if err != nil {
		return nil, err
	}
	if len(extracted) == 0 {
		return nil, nil
	}
	return JsonValue(string(extracted[0])), nil
}

func JSON_SUBSCRIPT(v string, field Value) (Value, error) {
	var path *json.Path
	switch field.(type) {
	case IntValue:
		index, err := field.ToInt64()
		if err != nil {
			return nil, err
		}
		p, err := json.CreatePath(fmt.Sprintf(`$[%d]`, index))
		if err != nil {
			return nil, err
		}
		path = p
	case StringValue:
		name, err := field.ToString()
		if err != nil {
			return nil, err
		}
		p, err := json.CreatePath(fmt.Sprintf(`$.%q`, name))
		if err != nil {
			return nil, err
		}
		path = p
	}
	extracted, err := path.Extract([]byte(v))
	if err != nil {
		return nil, err
	}
	if len(extracted) == 0 {
		return nil, nil
	}
	return JsonValue(string(extracted[0])), nil
}

func JSON_EXTRACT(v, path string) (Value, error) {
	p, err := json.CreatePath(path)
	if err != nil {
		return nil, err
	}
	if p.UsedDoubleQuotePathSelector() {
		return nil, fmt.Errorf("JSON_EXTRACT: doesn't use double quote path selector")
	}
	extracted, err := p.Extract([]byte(v))
	if err != nil {
		return nil, err
	}
	if len(extracted) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, extracted[0]); err != nil {
		return nil, fmt.Errorf("failed to format json %q: %w", extracted[0], err)
	}
	jsonValue := buf.String()
	if jsonValue == "null" {
		return nil, nil
	}
	return JsonValue(jsonValue), nil
}

func JSON_EXTRACT_SCALAR(v, path string) (Value, error) {
	p, err := json.CreatePath(path)
	if err != nil {
		return nil, err
	}
	if p.UsedDoubleQuotePathSelector() {
		return nil, fmt.Errorf("JSON_EXTRACT_SCALAR: doesn't use double quote path selector")
	}
	var values []interface{}
	if err := p.Unmarshal([]byte(v), &values); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	value := values[0]
	switch reflect.ValueOf(value).Type().Kind() {
	case reflect.Map, reflect.Slice:
		return nil, nil
	}
	return StringValue(fmt.Sprint(value)), nil
}

func JSON_EXTRACT_ARRAY(v, path string) (Value, error) {
	p, err := json.CreatePath(path)
	if err != nil {
		return nil, err
	}
	if p.UsedDoubleQuotePathSelector() {
		return nil, fmt.Errorf("JSON_EXTRACT_ARRAY: doesn't use double quote path selector")
	}
	extracted, err := p.Extract([]byte(v))
	if err != nil {
		return nil, err
	}
	if len(extracted) == 0 {
		return nil, nil
	}
	content := bytes.TrimLeft(extracted[0], " ")
	if len(content) != 0 && content[0] != '[' {
		// not array content
		return nil, nil
	}
	var values []json.RawMessage
	if err := json.Unmarshal(content, &values); err != nil {
		return nil, err
	}
	ret := &ArrayValue{}
	for _, value := range values {
		jsonValue := string(value)
		if jsonValue == "null" {
			ret.values = append(ret.values, nil)
		} else {
			ret.values = append(ret.values, JsonValue(jsonValue))
		}
	}
	return ret, nil
}

func JSON_EXTRACT_STRING_ARRAY(v, path string) (Value, error) {
	p, err := json.CreatePath(path)
	if err != nil {
		return nil, err
	}
	if p.UsedDoubleQuotePathSelector() {
		return nil, fmt.Errorf("JSON_EXTRACT_STRING_ARRAY: doesn't use double quote path selector")
	}
	var values []interface{}
	if err := p.Unmarshal([]byte(v), &values); err != nil {
		// invalid json content is ignored.
		return nil, nil
	}
	if len(values) == 0 {
		return nil, nil
	}
	value := values[0]
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Type().Kind() != reflect.Slice {
		return nil, nil
	}
	ret := &ArrayValue{}
	for i := 0; i < rv.Len(); i++ {
		elem := rv.Index(i).Interface()
		elemV := reflect.ValueOf(elem)
		elemKind := elemV.Type().Kind()
		if elemKind == reflect.Map || elemKind == reflect.Slice {
			return nil, nil
		}
		jsonValue := fmt.Sprint(elem)
		if jsonValue == "null" {
			ret.values = append(ret.values, nil)
		} else {
			ret.values = append(ret.values, StringValue(jsonValue))
		}
	}
	return ret, nil
}

func JSON_QUERY(v, path string) (Value, error) {
	p, err := json.CreatePath(path)
	if err != nil {
		return nil, err
	}
	if p.UsedSingleQuotePathSelector() {
		return nil, fmt.Errorf("JSON_QUERY: doesn't use single quote path selector")
	}
	extracted, err := p.Extract([]byte(v))
	if err != nil {
		return nil, err
	}
	if len(extracted) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, extracted[0]); err != nil {
		return nil, fmt.Errorf("failed to format json %q: %w", extracted[0], err)
	}
	jsonValue := buf.String()
	if jsonValue == "null" {
		return nil, nil
	}
	return JsonValue(jsonValue), nil
}

func JSON_VALUE(v, path string) (Value, error) {
	p, err := json.CreatePath(path)
	if err != nil {
		return nil, err
	}
	if p.UsedSingleQuotePathSelector() {
		return nil, fmt.Errorf("JSON_VALUE: doesn't use single quote path selector")
	}
	var values []interface{}
	if err := p.Unmarshal([]byte(v), &values); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	value := values[0]
	if !reflect.ValueOf(value).IsValid() {
		return nil, nil
	}
	switch reflect.ValueOf(value).Type().Kind() {
	case reflect.Map, reflect.Slice:
		return nil, nil
	}
	return StringValue(fmt.Sprint(value)), nil
}

func JSON_QUERY_ARRAY(v, path string) (Value, error) {
	p, err := json.CreatePath(path)
	if err != nil {
		return nil, err
	}
	if p.UsedSingleQuotePathSelector() {
		return nil, fmt.Errorf("JSON_QUERY_ARRAY: doesn't use single quote path selector")
	}
	extracted, err := p.Extract([]byte(v))
	if err != nil {
		return nil, err
	}
	if len(extracted) == 0 {
		return nil, nil
	}
	content := bytes.TrimLeft(extracted[0], " ")
	if len(content) != 0 && content[0] != '[' {
		// not array content
		return nil, nil
	}
	var values []json.RawMessage
	if err := json.Unmarshal(content, &values); err != nil {
		return nil, err
	}
	ret := &ArrayValue{}
	for _, value := range values {
		jsonValue := string(value)
		if jsonValue == "null" {
			ret.values = append(ret.values, nil)
		} else {
			ret.values = append(ret.values, JsonValue(jsonValue))
		}
	}
	return ret, nil
}

func JSON_VALUE_ARRAY(v, path string) (Value, error) {
	p, err := json.CreatePath(path)
	if err != nil {
		return nil, err
	}
	if p.UsedSingleQuotePathSelector() {
		return nil, fmt.Errorf("JSON_VALUE_ARRAY: doesn't use single quote path selector")
	}
	var values []interface{}
	if err := p.Unmarshal([]byte(v), &values); err != nil {
		// invalid json content is ignored.
		return nil, nil
	}
	if len(values) == 0 {
		return nil, nil
	}
	value := values[0]
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Type().Kind() != reflect.Slice {
		return nil, nil
	}
	ret := &ArrayValue{}
	for i := 0; i < rv.Len(); i++ {
		elem := rv.Index(i).Interface()
		elemV := reflect.ValueOf(elem)
		elemKind := elemV.Type().Kind()
		if elemKind == reflect.Map || elemKind == reflect.Slice {
			return nil, nil
		}
		jsonValue := fmt.Sprint(elem)
		if jsonValue == "null" {
			ret.values = append(ret.values, nil)
		} else {
			ret.values = append(ret.values, StringValue(jsonValue))
		}
	}
	return ret, nil
}

func PARSE_JSON(expr, mode string) (Value, error) {
	var v interface{}
	if err := json.Unmarshal([]byte(expr), &v); err != nil {
		return nil, err
	}
	return JsonValue(expr), nil
}

func TO_JSON(v Value, stringifyWideNumbers bool) (Value, error) {
	s, err := v.ToJSON()
	if err != nil {
		return nil, err
	}
	return JsonValue(s), nil
}

func TO_JSON_STRING(v Value, prettyPrint bool) (Value, error) {
	s, err := v.ToJSON()
	if err != nil {
		return nil, err
	}
	return StringValue(s), nil
}

func JSON_TYPE(v JsonValue) (Value, error) {
	return StringValue(v.Type()), nil
}
