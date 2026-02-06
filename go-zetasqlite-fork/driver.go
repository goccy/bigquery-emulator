package zetasqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/mattn/go-sqlite3"

	internal "github.com/goccy/go-zetasqlite/internal"
)

var (
	_ driver.Driver = &ZetaSQLiteDriver{}
	_ driver.Conn   = &ZetaSQLiteConn{}
	_ driver.Tx     = &ZetaSQLiteTx{}
)

var (
	nameToCatalogMap = map[string]*internal.Catalog{}
	nameToDBMap      = map[string]*sql.DB{}
	nameToValueMapMu sync.Mutex
)

func init() {
	sql.Register("zetasqlite", &ZetaSQLiteDriver{})
	sql.Register("zetasqlite_sqlite3", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if err := internal.RegisterFunctions(conn); err != nil {
				return err
			}
			conn.SetLimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, -1)
			return nil
		},
	})
}

func newDBAndCatalog(name string) (*sql.DB, *internal.Catalog, error) {
	nameToValueMapMu.Lock()
	defer nameToValueMapMu.Unlock()
	db, exists := nameToDBMap[name]
	if exists {
		return db, nameToCatalogMap[name], nil
	}
	db, err := sql.Open("zetasqlite_sqlite3", name)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open database by %s: %w", name, err)
	}
	catalog := internal.NewCatalog(db)
	nameToDBMap[name] = db
	nameToCatalogMap[name] = catalog
	return db, catalog, nil
}

type ZetaSQLiteDriver struct {
	ConnectHook func(*ZetaSQLiteConn) error
}

func (d *ZetaSQLiteDriver) Open(name string) (driver.Conn, error) {
	db, catalog, err := newDBAndCatalog(name)
	if err != nil {
		return nil, err
	}
	conn, err := newZetaSQLiteConn(db, catalog)
	if err != nil {
		return nil, err
	}
	if d.ConnectHook != nil {
		if err := d.ConnectHook(conn); err != nil {
			return nil, err
		}
	}
	return conn, nil
}

type ZetaSQLiteConn struct {
	conn     *sql.Conn
	tx       *sql.Tx
	analyzer *internal.Analyzer
}

func newZetaSQLiteConn(db *sql.DB, catalog *internal.Catalog) (*ZetaSQLiteConn, error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get sqlite3 connection: %w", err)
	}
	analyzer, err := internal.NewAnalyzer(catalog)
	if err != nil {
		return nil, fmt.Errorf("failed to create analyzer: %w", err)
	}
	return &ZetaSQLiteConn{
		conn:     conn,
		analyzer: analyzer,
	}, nil
}

func (c *ZetaSQLiteConn) SetAutoIndexMode(enabled bool) {
	c.analyzer.SetAutoIndexMode(enabled)
}

func (c *ZetaSQLiteConn) SetExplainMode(enabled bool) {
	c.analyzer.SetExplainMode(enabled)
}

// SetMaxNamePath specifies the maximum value of name path.
// If the name path in the query is the maximum value, the name path set as prefix is not used.
// Effective only when a value greater than zero is specified ( default zero ).
func (c *ZetaSQLiteConn) SetMaxNamePath(num int) {
	c.analyzer.SetMaxNamePath(num)
}

// MaxNamePath returns maximum value of name path.
func (c *ZetaSQLiteConn) MaxNamePath() int {
	return c.analyzer.MaxNamePath()
}

// SetNamePath set path to name path to be set as prefix.
// If max name path is specified, an error is returned if the number is exceeded.
func (c *ZetaSQLiteConn) SetNamePath(path []string) error {
	return c.analyzer.SetNamePath(path)
}

// NamePath returns path to name path to be set as prefix.
func (c *ZetaSQLiteConn) NamePath() []string {
	return c.analyzer.NamePath()
}

// AddNamePath add path to name path to be set as prefix.
// If max name path is specified, an error is returned if the number is exceeded.
func (c *ZetaSQLiteConn) AddNamePath(path string) error {
	return c.analyzer.AddNamePath(path)
}

func (s *ZetaSQLiteConn) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}

func (c *ZetaSQLiteConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.PrepareContext(context.Background(), query)
	return stmt, err
}

func (c *ZetaSQLiteConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	conn := internal.NewConn(c.conn, c.tx)
	actionFuncs, err := c.analyzer.Analyze(ctx, conn, query, nil)
	if err != nil {
		return nil, err
	}
	var stmt driver.Stmt
	for _, actionFunc := range actionFuncs {
		action, err := actionFunc()
		if err != nil {
			return nil, err
		}
		s, err := action.Prepare(ctx, conn)
		if err != nil {
			return nil, err
		}
		stmt = s
	}
	return stmt, nil
}

func (c *ZetaSQLiteConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, e error) {
	conn := internal.NewConn(c.conn, c.tx)
	actionFuncs, err := c.analyzer.Analyze(ctx, conn, query, args)
	if err != nil {
		return nil, err
	}
	var actions []internal.StmtAction
	defer func() {
		eg := new(internal.ErrorGroup)
		eg.Add(e)
		for _, action := range actions {
			eg.Add(action.Cleanup(ctx, conn))
		}
		if eg.HasError() {
			e = eg
		}
	}()

	var result driver.Result
	for _, actionFunc := range actionFuncs {
		action, err := actionFunc()
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)
		r, err := action.ExecContext(ctx, conn)
		if err != nil {
			return nil, err
		}
		result = r
	}
	return result, nil
}

func (c *ZetaSQLiteConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Rows, e error) {
	conn := internal.NewConn(c.conn, c.tx)
	actionFuncs, err := c.analyzer.Analyze(ctx, conn, query, args)
	if err != nil {
		return nil, err
	}
	var (
		actions []internal.StmtAction
		rows    *internal.Rows
	)
	defer func() {
		if rows != nil {
			// If we call cleanup action at the end of QueryContext function,
			// there is a possibility that the deleted table will be referenced when scanning from Rows,
			// so cleanup action should be executed in the Close() process of Rows.
			// For that, let Rows have a reference to actions ( and connection ).
			rows.SetActions(actions)
		}
	}()
	for _, actionFunc := range actionFuncs {
		action, err := actionFunc()
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)
		queryRows, err := action.QueryContext(ctx, conn)
		if err != nil {
			return nil, err
		}
		rows = queryRows
	}
	return rows, nil
}

func (c *ZetaSQLiteConn) Close() error {
	return c.conn.Close()
}

func (c *ZetaSQLiteConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(opts.Isolation),
		ReadOnly:  opts.ReadOnly,
	})
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return &ZetaSQLiteTx{
		tx:   tx,
		conn: c,
	}, nil
}

func (c *ZetaSQLiteConn) Begin() (driver.Tx, error) {
	tx, err := c.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return &ZetaSQLiteTx{
		tx:   tx,
		conn: c,
	}, nil
}

type ZetaSQLiteTx struct {
	tx   *sql.Tx
	conn *ZetaSQLiteConn
}

func (tx *ZetaSQLiteTx) Commit() error {
	defer func() {
		tx.conn.tx = nil
	}()
	return tx.tx.Commit()
}

func (tx *ZetaSQLiteTx) Rollback() error {
	defer func() {
		tx.conn.tx = nil
	}()
	return tx.tx.Rollback()
}
