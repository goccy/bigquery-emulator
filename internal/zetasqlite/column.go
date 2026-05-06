package zetasqlite

import (
	"github.com/goccy/go-json"
)

type ColumnType = Type

func UnmarshalDatabaseTypeName(typ string) (*ColumnType, error) {
	var v ColumnType
	if err := json.Unmarshal([]byte(typ), &v); err != nil {
		return nil, err
	}
	return &v, nil
}
