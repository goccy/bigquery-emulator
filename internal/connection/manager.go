package connection

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/goccy/googlesqlite"
)

type Manager struct {
	db *sql.DB
}

func NewManager(db *sql.DB) *Manager {
	return &Manager{db: db}
}

func (m *Manager) Connection(ctx context.Context, projectID, datasetID string) (*Conn, error) {
	if projectID == "" {
		return nil, fmt.Errorf("invalid projectID. projectID is empty")
	}
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	return &Conn{
		ProjectID: projectID,
		DatasetID: datasetID,
		Conn:      conn,
	}, nil
}

type Tx struct {
	tx        *sql.Tx
	conn      *Conn
	committed bool
}

func (t *Tx) Tx() *sql.Tx {
	return t.tx
}

func (t *Tx) RollbackIfNotCommitted() error {
	if t.committed {
		return nil
	}
	defer t.conn.Conn.Close()
	return t.tx.Rollback()
}

func (t *Tx) Commit() error {
	if err := t.tx.Commit(); err != nil {
		return err
	}
	t.committed = true
	t.conn.Conn.Close()
	return nil
}

func (t *Tx) SetProjectAndDataset(projectID, datasetID string) {
	t.conn.ProjectID = projectID
	t.conn.DatasetID = datasetID
}

func (t *Tx) MetadataRepoMode() error {
	if err := t.conn.Conn.Raw(func(c interface{}) error {
		gsqlConn, ok := c.(*googlesqlite.Conn)
		if !ok {
			return fmt.Errorf("failed to get *googlesqlite.Conn from %T", c)
		}
		_ = gsqlConn.SetNamePath([]string{})
		return nil
	}); err != nil {
		return fmt.Errorf("failed to setup connection: %w", err)
	}
	return nil
}

func (t *Tx) ContentRepoMode() error {
	if err := t.conn.Conn.Raw(func(c interface{}) error {
		gsqlConn, ok := c.(*googlesqlite.Conn)
		if !ok {
			return fmt.Errorf("failed to get *googlesqlite.Conn from %T", c)
		}
		namePath := []string{}
		if t.conn.ProjectID != "" {
			namePath = append(namePath, t.conn.ProjectID)
		}
		if t.conn.DatasetID != "" {
			namePath = append(namePath, t.conn.DatasetID)
		}
		_ = gsqlConn.SetNamePath(namePath)

		const maxNamePath = 3 // projectID and datasetID and tableID
		gsqlConn.SetMaxNamePath(maxNamePath)
		return nil
	}); err != nil {
		return fmt.Errorf("failed to setup connection: %w", err)
	}
	return nil
}

type Conn struct {
	ProjectID string
	DatasetID string
	Conn      *sql.Conn
}

func (c *Conn) Begin(ctx context.Context) (*Tx, error) {
	tx, err := c.Conn.BeginTx(ctx, nil)
	if err != nil {
		// The pooled connection is owned by the Tx once BeginTx succeeds and
		// is released by Commit/RollbackIfNotCommitted. When BeginTx fails no
		// Tx is created, so the connection must be returned to the pool here
		// or it leaks for the lifetime of the process.
		_ = c.Conn.Close()
		return nil, err
	}
	return &Tx{tx: tx, conn: c}, nil
}
