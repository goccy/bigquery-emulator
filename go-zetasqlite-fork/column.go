package zetasqlite

import (
	"github.com/goccy/go-json"

	internal "github.com/goccy/go-zetasqlite/internal"
)

type ColumnType = internal.Type

func UnmarshalDatabaseTypeName(typ string) (*ColumnType, error) {
	var v ColumnType
	if err := json.Unmarshal([]byte(typ), &v); err != nil {
		return nil, err
	}
	return &v, nil
}
