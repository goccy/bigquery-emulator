package internal

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/goccy/go-json"
	"github.com/goccy/go-zetasql/types"
)

type Rows struct {
	rows    *sql.Rows
	conn    *Conn
	columns []*ColumnSpec
	actions []StmtAction
}

func (r *Rows) ChangedCatalog() *ChangedCatalog {
	return r.conn.cc
}

func (r *Rows) SetActions(actions []StmtAction) {
	r.actions = actions
}

func (r *Rows) Columns() []string {
	colNames := make([]string, 0, len(r.columns))
	for _, col := range r.columns {
		colNames = append(colNames, col.Name)
	}
	return colNames
}

func (r *Rows) ColumnTypeDatabaseTypeName(i int) string {
	encodedType, _ := json.Marshal(r.columns[i].Type)
	return string(encodedType)
}

func (r *Rows) Close() (e error) {
	defer func() {
		eg := new(ErrorGroup)
		eg.Add(e)
		for _, action := range r.actions {
			eg.Add(action.Cleanup(context.Background(), r.conn))
		}
		if eg.HasError() {
			e = eg
		}
	}()
	if r.rows == nil {
		return nil
	}
	return r.rows.Close()
}

func (r *Rows) columnTypes() []*Type {
	ret := make([]*Type, 0, len(r.columns))
	for _, col := range r.columns {
		ret = append(ret, col.Type)
	}
	return ret
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.rows == nil {
		return io.EOF
	}
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	if err := r.rows.Err(); err != nil {
		return err
	}
	colTypes := r.columnTypes()
	values := make([]interface{}, 0, len(dest))
	for i := 0; i < len(dest); i++ {
		var v interface{}
		values = append(values, &v)
	}
	retErr := r.rows.Scan(values...)
	destV := reflect.ValueOf(dest)
	for idx, colType := range colTypes {
		src := reflect.ValueOf(values[idx]).Elem().Interface()
		dst := destV.Index(idx)
		if err := r.assignValue(src, dst, colType); err != nil {
			return err
		}
	}
	return retErr
}

func (r *Rows) assignValue(src interface{}, dst reflect.Value, typ *Type) error {
	if src == nil {
		dst.Set(reflect.New(dst.Type()).Elem())
		return nil
	}
	decodedValue, err := DecodeValue(src)
	if err != nil {
		return err
	}
	t, err := typ.ToZetaSQLType()
	if err != nil {
		return err
	}
	value, err := CastValue(t, decodedValue)
	if err != nil {
		return err
	}
	kind := dst.Type().Kind()
	switch kind {
	case reflect.Int:
		i64, err := value.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(int(i64)))
	case reflect.Int8:
		i64, err := value.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(int8(i64)))
	case reflect.Int16:
		i64, err := value.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(int16(i64)))
	case reflect.Int32:
		i64, err := value.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(int32(i64)))
	case reflect.Int64:
		i64, err := value.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(i64))
	case reflect.Uint:
		i64, err := value.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(uint(i64)))
	case reflect.Uint8:
		i64, err := value.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(uint8(i64)))
	case reflect.Uint16:
		i64, err := value.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(uint16(i64)))
	case reflect.Uint32:
		i64, err := value.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(uint32(i64)))
	case reflect.Uint64:
		i64, err := value.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(uint64(i64)))
	case reflect.Float32:
		f64, err := value.ToFloat64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(float32(f64)))
	case reflect.Float64:
		f64, err := value.ToFloat64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(f64))
	case reflect.String:
		s, err := value.ToString()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
	case reflect.Bool:
		b, err := value.ToBool()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(b))
	case reflect.Interface:
		return r.assignInterfaceValue(value, dst, typ)
	default:
		return fmt.Errorf("unexpected destination type %s for %T", kind, value)
	}
	return nil
}

func (r *Rows) assignInterfaceValue(src Value, dst reflect.Value, typ *Type) error {
	switch types.TypeKind(typ.Kind) {
	case types.INT32, types.INT64, types.UINT32, types.UINT64:
		i64, err := src.ToInt64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(i64))
	case types.BOOL:
		b, err := src.ToBool()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(b))
	case types.FLOAT, types.DOUBLE:
		f64, err := src.ToFloat64()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(f64))
	case types.BYTES:
		s, err := src.ToString()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
	case types.STRING:
		s, err := src.ToString()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
	case types.NUMERIC:
		s, err := src.ToString()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
	case types.BIG_NUMERIC:
		s, err := src.ToString()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
	case types.DATE:
		date, err := src.ToJSON()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(date))
	case types.DATETIME:
		datetime, err := src.ToJSON()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(datetime))
	case types.TIME:
		t, err := src.ToJSON()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(t))
	case types.TIMESTAMP:
		t, err := src.ToTime()
		if err != nil {
			return err
		}
		unixmicro := t.UnixMicro()
		sec := unixmicro / int64(time.Millisecond)
		nsec := unixmicro - sec*int64(time.Millisecond)
		dst.Set(reflect.ValueOf(fmt.Sprintf("%d.%d", sec, nsec)))
	case types.INTERVAL:
		s, err := src.ToString()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
	case types.JSON:
		v, err := src.ToJSON()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(v))
	case types.STRUCT:
		s, err := src.ToStruct()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s.Interface()))
	case types.ARRAY:
		array, err := src.ToArray()
		if err != nil {
			return err
		}
		sliceType := reflect.SliceOf(reflect.TypeOf((*interface{})(nil)).Elem())
		sliceRef := reflect.New(sliceType)
		sliceRef.Elem().Set(reflect.MakeSlice(sliceType, 0, len(array.values)))
		for _, v := range array.values {
			refV := reflect.New(sliceType.Elem())
			if v == nil {
				sliceRef.Elem().Set(reflect.Append(sliceRef.Elem(), refV.Elem()))
				continue
			}
			if err := r.assignInterfaceValue(v, refV.Elem(), typ.ElementType); err != nil {
				return err
			}
			sliceRef.Elem().Set(reflect.Append(sliceRef.Elem(), refV.Elem()))
		}
		dst.Set(sliceRef.Elem())
	case types.GEOGRAPHY:
		s, err := src.ToString()
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
	}
	return nil
}
