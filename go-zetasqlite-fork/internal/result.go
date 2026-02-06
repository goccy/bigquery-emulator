package internal

import "database/sql/driver"

type Result struct {
	conn   *Conn
	result driver.Result
}

func (r *Result) ChangedCatalog() *ChangedCatalog {
	return r.conn.cc
}

func (r *Result) LastInsertId() (int64, error) {
	if r.result == nil {
		return 0, nil
	}
	return r.result.LastInsertId()
}

func (r *Result) RowsAffected() (int64, error) {
	if r.result == nil {
		return 0, nil
	}
	return r.result.RowsAffected()
}
