package internal

type ValueType string

const (
	IntValueType        ValueType = "int64"
	StringValueType     ValueType = "string"
	BytesValueType      ValueType = "bytes"
	FloatValueType      ValueType = "float"
	NumericValueType    ValueType = "numeric"
	BigNumericValueType ValueType = "bignumeric"
	BoolValueType       ValueType = "bool"
	JsonValueType       ValueType = "json"
	ArrayValueType      ValueType = "array"
	StructValueType     ValueType = "struct"
	DateValueType       ValueType = "date"
	DatetimeValueType   ValueType = "datetime"
	TimeValueType       ValueType = "time"
	TimestampValueType  ValueType = "timestamp"
	IntervalValueType   ValueType = "interval"
	GeographyValueType  ValueType = "geography"
)

type ValueLayout struct {
	Header ValueType `json:"header"`
	Body   string    `json:"body"`
}

type StructValueLayout struct {
	Keys   []string      `json:"keys"`
	Values []interface{} `json:"values"`
}
