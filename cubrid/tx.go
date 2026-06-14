package cubrid

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
)

const (
	tranCommit   = 1
	tranRollback = 2
)

// IsolationLevel is a CUBRID transaction isolation level, using the wire
// values of the server's TRAN_* constants. 10.0+ (MVCC) servers support
// exactly these three; 9.x servers (broker protocol < V7) additionally
// accept the legacy lock-based levels 1 to 3, which share codes 4 to 6's wire
// shape but have different names and semantics there.
type IsolationLevel int32

const (
	IsolationReadCommitted  IsolationLevel = 4
	IsolationRepeatableRead IsolationLevel = 5
	IsolationSerializable   IsolationLevel = 6
)

// String returns the SQL-standard level name ("READ COMMITTED", ...).
func (l IsolationLevel) String() string {
	switch l {
	case IsolationReadCommitted:
		return "READ COMMITTED"
	case IsolationRepeatableRead:
		return "REPEATABLE READ"
	case IsolationSerializable:
		return "SERIALIZABLE"
	default:
		return fmt.Sprintf("IsolationLevel(%d)", int32(l))
	}
}

// SetIsolationLevel changes the session's transaction isolation level via
// SET_DB_PARAMETER. The new level applies from the next transaction; call
// it at a transaction boundary. V7+ brokers accept only the three MVCC
// levels, pre-V7 (9.x) also the legacy 1 to 3 (mirroring JDBC's
// isolationLevelMin clamp); out-of-range values are rejected client-side.
func (c *Conn) SetIsolationLevel(ctx context.Context, lv IsolationLevel) error {
	minLevel := IsolationLevel(1)
	if c.version.AtLeast(protocol.ProtoV7) {
		minLevel = IsolationReadCommitted
	}
	if lv < minLevel || lv > IsolationSerializable {
		return fmt.Errorf("cubrid: isolation level %d out of range [%d, %d]", int32(lv), int32(minLevel), int32(IsolationSerializable))
	}
	w := protocol.NewWriter()
	protocol.SetDBParameterRequest(w, protocol.DBParamIsolationLevel, int32(lv))
	r, err := c.roundTrip(ctx, w.Bytes())
	if err != nil {
		return err
	}
	_, err = protocol.ReadStatus(r)
	return err
}

// IsolationLevel reads the session's current transaction isolation level
// from the server via GET_DB_PARAMETER.
func (c *Conn) IsolationLevel(ctx context.Context) (IsolationLevel, error) {
	w := protocol.NewWriter()
	protocol.GetDBParameterRequest(w, protocol.DBParamIsolationLevel)
	r, err := c.roundTrip(ctx, w.Bytes())
	if err != nil {
		return 0, err
	}
	if _, err := protocol.ReadStatus(r); err != nil {
		return 0, err
	}
	v, err := r.Int32()
	if err != nil {
		return 0, err
	}
	return IsolationLevel(v), nil
}

// SetLockTimeout changes how long the session waits for a lock before
// erroring, via SET_DB_PARAMETER. The wire value is in milliseconds
// (sub-millisecond remainders are truncated, durations beyond the int32
// range clamp to MaxInt32 ms ~ 24.8 days); a negative duration means
// wait forever (-1, the server default).
func (c *Conn) SetLockTimeout(ctx context.Context, d time.Duration) error {
	ms := int32(-1)
	if d >= 0 {
		ms = math.MaxInt32
		if v := d / time.Millisecond; v < math.MaxInt32 {
			ms = int32(v)
		}
	}
	w := protocol.NewWriter()
	protocol.SetDBParameterRequest(w, protocol.DBParamLockTimeout, ms)
	r, err := c.roundTrip(ctx, w.Bytes())
	if err != nil {
		return err
	}
	_, err = protocol.ReadStatus(r)
	return err
}

// SetAutoCommit toggles client-side autocommit. With autocommit off, end
// transactions explicitly with Commit or Rollback.
func (c *Conn) SetAutoCommit(on bool) { c.autoCommit = on }

// AutoCommit reports whether client-side autocommit is on (the default).
func (c *Conn) AutoCommit() bool { return c.autoCommit }

func (c *Conn) endTransaction(ctx context.Context, tranType byte) error {
	w := protocol.NewWriter()
	w.RawByte(protocol.FnEndTransaction)
	w.ArgByte(tranType)
	r, err := c.roundTrip(ctx, w.Bytes())
	if err != nil {
		return err
	}
	_, err = protocol.ReadStatus(r)
	return err
}

// Commit ends the current transaction, making its work durable. With
// autocommit off the next statement starts a new transaction. Ending a
// transaction may invalidate open LOB locators and server-side cursors
// (see Lob).
func (c *Conn) Commit(ctx context.Context) error {
	return c.endTransaction(ctx, tranCommit)
}

// Rollback ends the current transaction, undoing its work. See Commit
// for the locator/cursor caveat.
func (c *Conn) Rollback(ctx context.Context) error {
	return c.endTransaction(ctx, tranRollback)
}

// savepointName guards the SQL splice in Savepoint/RollbackToSavepoint:
// savepoint names cannot be bound parameters, so only plain identifiers
// are accepted.
var savepointName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func checkSavepointName(name string) error {
	if !savepointName.MatchString(name) {
		return fmt.Errorf("cubrid: invalid savepoint name %q", name)
	}
	return nil
}

// Savepoint sets a named savepoint in the current transaction. Savepoints
// are SQL-level in CUBRID (the wire protocol's savepoint op is vestigial);
// they require autocommit to be off to be useful.
func (c *Conn) Savepoint(ctx context.Context, name string) error {
	if err := checkSavepointName(name); err != nil {
		return err
	}
	_, err := c.Exec(ctx, "SAVEPOINT "+name)
	return err
}

// RollbackToSavepoint rolls back to a named savepoint without ending the
// transaction: work after the savepoint is undone, work before it stays
// pending.
func (c *Conn) RollbackToSavepoint(ctx context.Context, name string) error {
	if err := checkSavepointName(name); err != nil {
		return err
	}
	_, err := c.Exec(ctx, "ROLLBACK TO SAVEPOINT "+name)
	return err
}
