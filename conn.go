package cubridsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/hexacluster/gocubrid/cubrid"
)

// conn adapts one native connection to database/sql. The pool serializes
// use, matching the native contract (one *cubrid.Conn = one socket, not
// goroutine-safe).
type conn struct {
	nc *cubrid.Conn
}

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
)

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	ns, err := c.nc.Prepare(ctx, query)
	if err != nil {
		return nil, mapErr(err)
	}
	return &stmt{ns: ns}, nil
}

// ExecContext prepares, executes, and closes in one go (CUBRID has no
// direct-execute protocol op; the close is one cheap round-trip).
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	vals, err := bindArgs(args)
	if err != nil {
		return nil, err
	}
	ns, err := c.nc.Prepare(ctx, query)
	if err != nil {
		return nil, mapErr(err)
	}
	defer ns.Close(context.Background())
	res, err := ns.Exec(ctx, vals...)
	if err != nil {
		return nil, mapErr(err)
	}
	return result{affected: res.AffectedRows}, nil
}

// QueryContext prepares and executes; the returned rows own the statement
// and release its server handle on Close.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	vals, err := bindArgs(args)
	if err != nil {
		return nil, err
	}
	ns, err := c.nc.Prepare(ctx, query)
	if err != nil {
		return nil, mapErr(err)
	}
	nr, err := ns.Query(ctx, vals...)
	if err != nil {
		ns.Close(context.Background())
		return nil, mapErr(err)
	}
	return &rows{ctx: ctx, nr: nr, cols: nr.Columns(), ownedStmt: ns}, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx maps database/sql isolation levels onto the native API and
// disables autocommit for the duration of the transaction. Non-default
// isolation is set via SET_DB_PARAMETER before the transaction starts
// (CUBRID applies it from the next transaction).
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, errors.New("cubridsql: read-only transactions are not supported by CUBRID")
	}
	lv, ok := nativeIsolation(opts.Isolation)
	if !ok {
		return nil, fmt.Errorf("cubridsql: unsupported isolation level %s (CUBRID supports default, read committed, repeatable read, serializable)",
			sql.IsolationLevel(opts.Isolation))
	}
	if lv != 0 {
		if err := c.nc.SetIsolationLevel(ctx, lv); err != nil {
			return nil, mapErr(err)
		}
	}
	c.nc.SetAutoCommit(false)
	return &tx{nc: c.nc}, nil
}

// nativeIsolation maps a database/sql isolation level to the native one;
// 0 means "leave the session level alone" (sql.LevelDefault).
func nativeIsolation(lv driver.IsolationLevel) (cubrid.IsolationLevel, bool) {
	switch sql.IsolationLevel(lv) {
	case sql.LevelDefault:
		return 0, true
	case sql.LevelReadCommitted:
		return cubrid.IsolationReadCommitted, true
	case sql.LevelRepeatableRead:
		return cubrid.IsolationRepeatableRead, true
	case sql.LevelSerializable:
		return cubrid.IsolationSerializable, true
	}
	return 0, false
}

// Ping checks broker liveness with CHECK_CAS.
func (c *conn) Ping(ctx context.Context) error {
	return mapErr(c.nc.Ping(ctx))
}

// IsValid reports pool-reusability without a wire operation.
func (c *conn) IsValid() bool { return c.nc.Valid() }

// ResetSession restores the connection's pool-neutral state: any
// transaction left open is rolled back and autocommit re-enabled.
func (c *conn) ResetSession(ctx context.Context) error {
	if !c.nc.Valid() {
		return driver.ErrBadConn
	}
	if !c.nc.AutoCommit() {
		if err := c.nc.Rollback(ctx); err != nil {
			return driver.ErrBadConn
		}
		c.nc.SetAutoCommit(true)
	}
	return nil
}

// CheckNamedValue admits the native bind specials (*cubrid.Lob and []any
// collections) that the default converter would reject, rejects named
// parameters (CUBRID has only ? ordinals), and defers everything else to
// the default converter (which also resolves driver.Valuer types).
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if nv.Name != "" {
		return fmt.Errorf("cubridsql: named parameter %q is not supported (CUBRID binds by ? ordinal only)", nv.Name)
	}
	switch nv.Value.(type) {
	case *cubrid.Lob, []any:
		return nil
	}
	return driver.ErrSkip
}

// Close sends CON_CLOSE best-effort and releases the socket.
func (c *conn) Close() error { return c.nc.Close() }
