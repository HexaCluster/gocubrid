package cubrid

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
	"github.com/hexacluster/gocubrid/internal/prototest"
)

func TestCommitSendsEndTransaction(t *testing.T) {
	fb, conn := fakeConn(t)
	conn.SetAutoCommit(false)
	if conn.AutoCommit() {
		t.Fatal("autocommit should be off")
	}
	if err := conn.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := conn.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs := fb.Requests()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d", len(reqs))
	}
	// END_TRANSACTION(1), arg byte 1=commit / 2=rollback.
	// Payload layout: [fn][00 00 00 01][type], so the type byte is index 5.
	if reqs[0][0] != 1 || reqs[0][5] != 1 {
		t.Fatalf("commit frame % X", reqs[0])
	}
	if reqs[1][0] != 1 || reqs[1][5] != 2 {
		t.Fatalf("rollback frame % X", reqs[1])
	}
}

// Savepoints are SQL-level: the protocol op (fn 26) is vestigial (JDBC has
// it commented out), so Savepoint/RollbackToSavepoint must prepare and
// execute SAVEPOINT / ROLLBACK TO SAVEPOINT statements.
func TestSavepointSendsSQL(t *testing.T) {
	fb, conn := fakeConn(t)
	conn.SetAutoCommit(false)
	// Each call runs through Exec: queue a prepare + execute pair.
	for i := 0; i < 2; i++ {
		p := &prototest.RespWriter{}
		p.I32(40 + int32(i)).I32(0).B(0).I32(0).B(0).I32(0) // handle, 0 params, 0 cols
		fb.Queue(2, p.Bytes())
		e := &prototest.RespWriter{}
		e.I32(0).B(0).I32(1).B(0).I32(0).Raw(make([]byte, 8)).I32(0).I32(0).B(0).I32(0)
		fb.Queue(3, e.Bytes())
	}
	ctx := context.Background()
	if err := conn.Savepoint(ctx, "sp_1"); err != nil {
		t.Fatal(err)
	}
	if err := conn.RollbackToSavepoint(ctx, "sp_1"); err != nil {
		t.Fatal(err)
	}
	var sqls []string
	for _, req := range fb.Requests() {
		if req[0] == 2 { // PREPARE: [fn][int32 len incl NUL][sql][NUL]...
			n := binary.BigEndian.Uint32(req[1:5])
			sqls = append(sqls, string(req[5:5+int(n)-1]))
		}
	}
	want := []string{"SAVEPOINT sp_1", "ROLLBACK TO SAVEPOINT sp_1"}
	if len(sqls) != len(want) || sqls[0] != want[0] || sqls[1] != want[1] {
		t.Fatalf("prepared SQL = %q, want %q", sqls, want)
	}
}

// SET_DB_PARAMETER (fn 5) packs [param id int][value int]; isolation is
// param 1 (JDBC UConnection.setIsolationLevel).
func TestSetIsolationLevelWire(t *testing.T) {
	fb, conn := fakeConn(t)
	if err := conn.SetIsolationLevel(context.Background(), IsolationRepeatableRead); err != nil {
		t.Fatal(err)
	}
	reqs := fb.Requests()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d", len(reqs))
	}
	want := []byte{
		protocol.FnSetDBParameter,
		0, 0, 0, 4, 0, 0, 0, 1, // param id: isolation level
		0, 0, 0, 4, 0, 0, 0, 5, // value: REPEATABLE READ
	}
	if !bytes.Equal(reqs[0], want) {
		t.Fatalf("got  % X\nwant % X", reqs[0], want)
	}
}

// GET_DB_PARAMETER (fn 4) packs [param id int]; the response carries the
// value as a raw int32 after the status (JDBC UConnection.getIsolationLevel).
func TestIsolationLevelRoundTrip(t *testing.T) {
	fb, conn := fakeConn(t)
	p := &prototest.RespWriter{}
	p.I32(0).I32(6) // status OK, value SERIALIZABLE
	fb.Queue(protocol.FnGetDBParameter, p.Bytes())
	lv, err := conn.IsolationLevel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lv != IsolationSerializable {
		t.Fatalf("isolation level = %v, want %v", lv, IsolationSerializable)
	}
	reqs := fb.Requests()
	want := []byte{
		protocol.FnGetDBParameter,
		0, 0, 0, 4, 0, 0, 0, 1, // param id: isolation level
	}
	if len(reqs) != 1 || !bytes.Equal(reqs[0], want) {
		t.Fatalf("got  % X\nwant % X", reqs, want)
	}
}

// V7+ brokers accept only 4 to 6 (JDBC clamps isolationLevelMin to
// READ_COMMITTED there); out-of-range levels must be rejected client-side
// before touching the wire.
func TestSetIsolationLevelRejectsOutOfRange(t *testing.T) {
	fb, conn := fakeConn(t) // fake broker negotiates V12
	for _, lv := range []IsolationLevel{0, 1, 2, 3, 7, -1} {
		if err := conn.SetIsolationLevel(context.Background(), lv); err == nil {
			t.Errorf("SetIsolationLevel(%d): want error", lv)
		}
	}
	if n := len(fb.Requests()); n != 0 {
		t.Fatalf("out-of-range levels reached the wire: %d requests", n)
	}
}

// Pre-V7 brokers (9.x) also speak the legacy lock-based levels 1 to 3.
func TestSetIsolationLevelLegacyPreV7(t *testing.T) {
	fb := prototest.Start(t)
	fb.SetProtocolVersion(protocol.ProtoV6)
	cfg, err := ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	if err := conn.SetIsolationLevel(context.Background(), IsolationLevel(2)); err != nil {
		t.Fatalf("legacy level 2 on V6: %v", err)
	}
	if n := len(fb.Requests()); n != 1 {
		t.Fatalf("requests = %d", n)
	}
	for _, lv := range []IsolationLevel{0, 7} {
		if err := conn.SetIsolationLevel(context.Background(), lv); err == nil {
			t.Errorf("SetIsolationLevel(%d) on V6: want error", lv)
		}
	}
	if n := len(fb.Requests()); n != 1 {
		t.Fatalf("out-of-range levels reached the wire: %d requests", n)
	}
}

// Lock timeout is SET_DB_PARAMETER param 2; the wire value is in
// milliseconds and negative durations send -1 (wait forever).
func TestSetLockTimeoutWire(t *testing.T) {
	fb, conn := fakeConn(t)
	if err := conn.SetLockTimeout(context.Background(), 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetLockTimeout(context.Background(), -1); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetLockTimeout(context.Background(), 100*365*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	reqs := fb.Requests()
	if len(reqs) != 3 {
		t.Fatalf("requests = %d", len(reqs))
	}
	want := []byte{
		protocol.FnSetDBParameter,
		0, 0, 0, 4, 0, 0, 0, 2, // param id: lock timeout
		0, 0, 0, 4, 0, 0, 0x05, 0xDC, // 1500 ms
	}
	if !bytes.Equal(reqs[0], want) {
		t.Fatalf("got  % X\nwant % X", reqs[0], want)
	}
	wantInf := []byte{
		protocol.FnSetDBParameter,
		0, 0, 0, 4, 0, 0, 0, 2,
		0, 0, 0, 4, 0xFF, 0xFF, 0xFF, 0xFF, // -1 = infinite wait
	}
	if !bytes.Equal(reqs[1], wantInf) {
		t.Fatalf("got  % X\nwant % X", reqs[1], wantInf)
	}
	wantClamp := []byte{
		protocol.FnSetDBParameter,
		0, 0, 0, 4, 0, 0, 0, 2,
		0, 0, 0, 4, 0x7F, 0xFF, 0xFF, 0xFF, // 100 years clamp to MaxInt32 ms
	}
	if !bytes.Equal(reqs[2], wantClamp) {
		t.Fatalf("got  % X\nwant % X", reqs[2], wantClamp)
	}
}

func TestIsolationLevelString(t *testing.T) {
	cases := map[IsolationLevel]string{
		IsolationReadCommitted:  "READ COMMITTED",
		IsolationRepeatableRead: "REPEATABLE READ",
		IsolationSerializable:   "SERIALIZABLE",
		IsolationLevel(2):       "IsolationLevel(2)",
	}
	for lv, want := range cases {
		if got := lv.String(); got != want {
			t.Errorf("IsolationLevel(%d).String() = %q, want %q", int32(lv), got, want)
		}
	}
}

// Savepoint names are spliced into SQL (they cannot be bound), so anything
// outside ^[A-Za-z_][A-Za-z0-9_]*$ must be rejected before touching the wire.
func TestSavepointRejectsInvalidName(t *testing.T) {
	fb, conn := fakeConn(t)
	conn.SetAutoCommit(false)
	for _, name := range []string{"", "1abc", "a b", "a;DROP TABLE t", "a-b", "naïve", "a'b"} {
		if err := conn.Savepoint(context.Background(), name); err == nil {
			t.Errorf("Savepoint(%q): want error", name)
		}
		if err := conn.RollbackToSavepoint(context.Background(), name); err == nil {
			t.Errorf("RollbackToSavepoint(%q): want error", name)
		}
	}
	if n := len(fb.Requests()); n != 0 {
		t.Fatalf("invalid savepoint names reached the wire: %d requests", n)
	}
}
