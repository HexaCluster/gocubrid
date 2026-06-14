package cubridsql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hexacluster/gocubrid/cubrid"
	"github.com/hexacluster/gocubrid/internal/prototest"
)

// ---- fake-broker scaffolding ----------------------------------------------

func fakeDB(t *testing.T) (*prototest.FakeBroker, *sql.DB) {
	t.Helper()
	fb := prototest.Start(t)
	cfg, err := cubrid.ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb")
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(NewConnector(cfg))
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	return fb, db
}

type fcol struct {
	typ       byte
	coll      byte // collection kind: 1 SET, 2 MULTISET, 3 SEQUENCE
	scale     int16
	precision int32
	name      string
	notNull   bool
}

// prepPayload builds a PREPARE response (handle 33) for the fake broker.
func prepPayload(cmdType byte, params int32, cols ...fcol) []byte {
	p := &prototest.RespWriter{}
	p.I32(33).I32(0).B(cmdType).I32(params).B(0).I32(int32(len(cols)))
	for _, c := range cols {
		if c.coll != 0 {
			p.B(0x80 | c.coll<<5).B(c.typ) // extended two-byte type form
		} else {
			p.B(c.typ)
		}
		p.I16(c.scale).I32(c.precision)
		p.Str(c.name).Str(c.name).Str("t")
		if c.notNull {
			p.B(1)
		} else {
			p.B(0)
		}
		p.Str("")              // default value
		p.Raw(make([]byte, 7)) // flag bytes
	}
	return p.Bytes()
}

// execHeader writes the EXECUTE response prefix shared by SELECT and DML.
func execHeader(rc int32, cmdType byte) *prototest.RespWriter {
	e := &prototest.RespWriter{}
	e.I32(rc)              // affected rows / total row count
	e.B(0)                 // cache reusable
	e.I32(1)               // one result block
	e.B(cmdType)           // statement type
	e.I32(rc)              // result count
	e.Raw(make([]byte, 8)) // result OID
	e.I32(0).I32(0)        // cache times
	e.B(0)                 // include_column_info (V2+)
	e.I32(0)               // shard id (V5+)
	return e
}

// execSelect builds an EXECUTE response with all rows inline; each row is
// a slice of pre-encoded column values (nil = SQL NULL).
func execSelect(rows ...[][]byte) []byte {
	e := execHeader(int32(len(rows)), 21)
	if len(rows) > 0 {
		e.I32(0)                // inline fetch rescode
		e.I32(int32(len(rows))) // tuple count
		for i, row := range rows {
			e.I32(int32(i + 1))    // cursor index
			e.Raw(make([]byte, 8)) // tuple OID
			for _, v := range row {
				if v == nil {
					e.I32(-1)
					continue
				}
				e.I32(int32(len(v))).Raw(v)
			}
		}
		e.B(1) // fetch completed (V5+)
	}
	return e.Bytes()
}

func execUpdate(affected int32) []byte { return execHeader(affected, 20).Bytes() }

// serverError builds a renewed-format error payload.
func serverError(code int32, msg string) []byte {
	e := &prototest.RespWriter{}
	e.I32(code).I32(code).Raw(append([]byte(msg), 0))
	return e.Bytes()
}

func i32b(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func i64b(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func f64b(v float64) []byte { return i64b(int64(math.Float64bits(v))) }

func strb(s string) []byte { return append([]byte(s), 0) }

func dtBytes(t time.Time) []byte {
	b := make([]byte, 14)
	put := func(i, v int) { binary.BigEndian.PutUint16(b[2*i:], uint16(v)) }
	put(0, t.Year())
	put(1, int(t.Month()))
	put(2, t.Day())
	put(3, t.Hour())
	put(4, t.Minute())
	put(5, t.Second())
	put(6, t.Nanosecond()/1e6)
	return b
}

// packedLob builds a packed LOB handle (the result-column wire form).
func packedLob(dbType int32, size int64) []byte {
	loc := append([]byte("file:/lob/unit"), 0)
	b := make([]byte, 16, 16+len(loc))
	binary.BigEndian.PutUint32(b[0:4], uint32(dbType))
	binary.BigEndian.PutUint64(b[4:12], uint64(size))
	binary.BigEndian.PutUint32(b[12:16], uint32(len(loc)))
	return append(b, loc...)
}

func fnRequests(fb *prototest.FakeBroker, fn byte) [][]byte {
	var out [][]byte
	for _, r := range fb.Requests() {
		if len(r) > 0 && r[0] == fn {
			out = append(out, r)
		}
	}
	return out
}

// ---- tests -----------------------------------------------------------------

func TestDriverRegistered(t *testing.T) {
	for _, name := range sql.Drivers() {
		if name == "cubrid" {
			return
		}
	}
	t.Fatalf(`driver "cubrid" not registered, have %v`, sql.Drivers())
}

func TestOpenConnectorBadDSN(t *testing.T) {
	if _, err := (Driver{}).OpenConnector("mysql://nope"); err == nil {
		t.Fatal("want DSN parse error")
	}
}

func TestQueryScalarsAndColumnTypes(t *testing.T) {
	fb, db := fakeDB(t)
	fb.Queue(2, prepPayload(21, 0,
		fcol{typ: 8, precision: 10, name: "id", notNull: true}, // INTEGER
		fcol{typ: 2, precision: 20, name: "name"},              // VARCHAR
		fcol{typ: 7, precision: 10, scale: 2, name: "price"},   // NUMERIC
		fcol{typ: 12, name: "ratio"},                           // DOUBLE
		fcol{typ: 21, name: "big"},                             // BIGINT
		fcol{typ: 22, name: "created"},                         // DATETIME
	))
	created := time.Date(2026, 6, 11, 13, 30, 45, 250e6, time.UTC)
	fb.Queue(3, execSelect([][]byte{
		i32b(7), strb("abc"), strb("12.34"), f64b(1.5), i64b(1 << 40), dtBytes(created),
	}))

	rows, err := db.Query("SELECT id, name, price, ratio, big, created FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{"INTEGER", "VARCHAR", "NUMERIC", "DOUBLE", "BIGINT", "DATETIME"}
	wantScan := []reflect.Type{
		reflect.TypeOf(int64(0)), reflect.TypeOf(""), reflect.TypeOf(""),
		reflect.TypeOf(float64(0)), reflect.TypeOf(int64(0)), reflect.TypeOf(time.Time{}),
	}
	for i, ct := range cts {
		if ct.DatabaseTypeName() != wantNames[i] {
			t.Errorf("col %d type name = %q, want %q", i, ct.DatabaseTypeName(), wantNames[i])
		}
		if ct.ScanType() != wantScan[i] {
			t.Errorf("col %d scan type = %v, want %v", i, ct.ScanType(), wantScan[i])
		}
	}
	if n, ok := cts[0].Nullable(); !ok || n {
		t.Errorf("id nullable = (%v, %v), want (false, true)", n, ok)
	}
	if n, ok := cts[1].Nullable(); !ok || !n {
		t.Errorf("name nullable = (%v, %v), want (true, true)", n, ok)
	}
	if p, s, ok := cts[2].DecimalSize(); !ok || p != 10 || s != 2 {
		t.Errorf("price decimal size = (%d, %d, %v), want (10, 2, true)", p, s, ok)
	}
	if _, _, ok := cts[0].DecimalSize(); ok {
		t.Error("id reports a decimal size")
	}
	if l, ok := cts[1].Length(); !ok || l != 20 {
		t.Errorf("name length = (%d, %v), want (20, true)", l, ok)
	}
	if _, ok := cts[0].Length(); ok {
		t.Error("id reports a variable length")
	}

	if !rows.Next() {
		t.Fatalf("no row: %v", rows.Err())
	}
	var (
		id    int64
		name  string
		price string
		ratio float64
		big   int64
		when  time.Time
	)
	if err := rows.Scan(&id, &name, &price, &ratio, &big, &when); err != nil {
		t.Fatal(err)
	}
	if id != 7 || name != "abc" || price != "12.34" || ratio != 1.5 || big != 1<<40 || !when.Equal(created) {
		t.Fatalf("row = %v %q %q %v %v %v", id, name, price, ratio, big, when)
	}
	if rows.Next() {
		t.Fatal("more than one row")
	}
	rows.Close()
	if got := fnRequests(fb, 6); len(got) != 1 {
		t.Fatalf("statement close requests = %d, want 1 (rows own the statement)", len(got))
	}
}

func TestQueryNullColumn(t *testing.T) {
	fb, db := fakeDB(t)
	fb.Queue(2, prepPayload(21, 0, fcol{typ: 2, precision: 20, name: "name"}))
	fb.Queue(3, execSelect([][]byte{nil}))
	var ns sql.NullString
	if err := db.QueryRow("SELECT name FROM t").Scan(&ns); err != nil {
		t.Fatal(err)
	}
	if ns.Valid {
		t.Fatalf("NULL scanned as %q", ns.String)
	}
}

func TestCollectionColumnScansAsSlice(t *testing.T) {
	fb, db := fakeDB(t)
	fb.Queue(2, prepPayload(21, 0, fcol{typ: 8, coll: 1, name: "s"})) // SET(INT)
	coll := &prototest.RespWriter{}
	coll.B(8).I32(2)
	coll.I32(4).Raw(i32b(7))
	coll.I32(4).Raw(i32b(9))
	fb.Queue(3, execSelect([][]byte{coll.Bytes()}))

	var v any
	if err := db.QueryRow("SELECT s FROM t").Scan(&v); err != nil {
		t.Fatal(err)
	}
	want := []any{int64(7), int64(9)}
	if !reflect.DeepEqual(v, want) {
		t.Fatalf("collection = %#v, want %#v", v, want)
	}
}

func TestExecAffectedRowsAndLastInsertId(t *testing.T) {
	fb, db := fakeDB(t)
	fb.Queue(2, prepPayload(20, 1))
	fb.Queue(3, execUpdate(3))
	res, err := db.Exec("UPDATE t SET n = ? WHERE 1=1", int64(5))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 3 {
		t.Fatalf("RowsAffected = (%d, %v), want (3, nil)", n, err)
	}
	if _, err := res.LastInsertId(); err == nil || !strings.Contains(err.Error(), "LAST_INSERT_ID()") {
		t.Fatalf("LastInsertId error = %v, want unsupported error pointing at SELECT LAST_INSERT_ID()", err)
	}
}

func TestPreparedStmtReuse(t *testing.T) {
	fb, db := fakeDB(t)
	fb.Queue(2, prepPayload(21, 1, fcol{typ: 8, precision: 10, name: "id"}))
	fb.Queue(3, execSelect([][]byte{i32b(7)}))
	fb.Queue(3, execSelect([][]byte{i32b(9)}))

	st, err := db.Prepare("SELECT id FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var a, b int64
	if err := st.QueryRow(int64(1)).Scan(&a); err != nil {
		t.Fatal(err)
	}
	if err := st.QueryRow(int64(2)).Scan(&b); err != nil {
		t.Fatal(err)
	}
	if a != 7 || b != 9 {
		t.Fatalf("rows = %d, %d", a, b)
	}
	if got := fnRequests(fb, 2); len(got) != 1 {
		t.Fatalf("PREPARE requests = %d, want 1 (statement reused)", len(got))
	}
	if got := fnRequests(fb, 3); len(got) != 2 {
		t.Fatalf("EXECUTE requests = %d, want 2", len(got))
	}
}

func TestStmtArgCountEnforced(t *testing.T) {
	fb, db := fakeDB(t)
	fb.Queue(2, prepPayload(20, 1))
	st, err := db.Prepare("INSERT INTO t VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Exec(); err == nil {
		t.Fatal("want arg count error from NumInput")
	}
}

func TestTxCommitRollbackWire(t *testing.T) {
	fb, db := fakeDB(t)
	ctx := context.Background()

	// Committed tx.
	fb.Queue(2, prepPayload(20, 0))
	fb.Queue(3, execUpdate(1))
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Rolled-back tx.
	fb.Queue(2, prepPayload(20, 0))
	fb.Queue(3, execUpdate(1))
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Autocommit statement after both txes.
	fb.Queue(2, prepPayload(20, 0))
	fb.Queue(3, execUpdate(1))
	if _, err := db.Exec("INSERT INTO t VALUES (3)"); err != nil {
		t.Fatal(err)
	}

	ends := fnRequests(fb, 1) // END_TRAN
	if len(ends) != 2 {
		t.Fatalf("END_TRAN requests = %d, want 2", len(ends))
	}
	if want := []byte{1, 0, 0, 0, 1, 1}; !bytes.Equal(ends[0], want) {
		t.Fatalf("commit request = % x, want % x", ends[0], want)
	}
	if want := []byte{1, 0, 0, 0, 1, 2}; !bytes.Equal(ends[1], want) {
		t.Fatalf("rollback request = % x, want % x", ends[1], want)
	}
	// PREPARE's trailing byte is the autocommit flag: off inside a tx,
	// back on for the pooled conn afterwards.
	preps := fnRequests(fb, 2)
	if len(preps) != 3 {
		t.Fatalf("PREPARE requests = %d, want 3", len(preps))
	}
	for i, wantAC := range []byte{0, 0, 1} {
		if got := preps[i][len(preps[i])-1]; got != wantAC {
			t.Errorf("prepare %d autocommit byte = %d, want %d", i, got, wantAC)
		}
	}
}

func TestBeginTxSetsIsolation(t *testing.T) {
	fb, db := fakeDB(t)
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	sets := fnRequests(fb, 5) // SET_DB_PARAMETER
	if len(sets) != 1 {
		t.Fatalf("SET_DB_PARAMETER requests = %d, want 1", len(sets))
	}
	// [fn=5][int32 arg: param id 1][int32 arg: value 6 = SERIALIZABLE]
	want := []byte{5, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 6}
	if !bytes.Equal(sets[0], want) {
		t.Fatalf("request = % x, want % x", sets[0], want)
	}
}

func TestBeginTxRejectsReadOnly(t *testing.T) {
	_, db := fakeDB(t)
	_, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("err = %v, want read-only rejection", err)
	}
}

func TestBeginTxRejectsUnsupportedIsolation(t *testing.T) {
	_, db := fakeDB(t)
	_, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err == nil || !strings.Contains(err.Error(), "isolation") {
		t.Fatalf("err = %v, want unsupported isolation rejection", err)
	}
}

func TestNamedParamsRejected(t *testing.T) {
	_, db := fakeDB(t)
	_, err := db.Exec("INSERT INTO t VALUES (:x)", sql.Named("x", 1))
	if err == nil || !strings.Contains(err.Error(), "named parameter") {
		t.Fatalf("err = %v, want named-parameter rejection", err)
	}
}

func TestCheckNamedValueNativeSpecials(t *testing.T) {
	c := &conn{}
	for _, v := range []any{&cubrid.Lob{}, []any{int64(1), nil}} {
		nv := &driver.NamedValue{Ordinal: 1, Value: v}
		if err := c.CheckNamedValue(nv); err != nil {
			t.Errorf("CheckNamedValue(%T) = %v, want nil (native special)", v, err)
		}
	}
	nv := &driver.NamedValue{Ordinal: 1, Value: int64(1)}
	if err := c.CheckNamedValue(nv); err != driver.ErrSkip {
		t.Errorf("CheckNamedValue(int64) = %v, want driver.ErrSkip (default converter)", err)
	}
}

// TestNumericRoundTrip drives cubrid.Numeric both ways through database/sql:
// as a bind parameter (its Valuer makes the default converter send the exact
// string) and as an opt-in scan target for a NUMERIC column.
func TestNumericRoundTrip(t *testing.T) {
	fb, db := fakeDB(t)
	fb.Queue(2, prepPayload(21, 1, fcol{typ: 7, precision: 10, scale: 2, name: "price"}))
	fb.Queue(3, execSelect([][]byte{strb("12.34")}))

	var got cubrid.Numeric
	if err := db.QueryRow("SELECT price FROM t WHERE price = ?", cubrid.Numeric("12.34")).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != cubrid.Numeric("12.34") {
		t.Fatalf("scanned Numeric = %q, want %q", got, "12.34")
	}
	// The bind went over the wire as the exact decimal string.
	execs := fnRequests(fb, 3)
	if len(execs) != 1 {
		t.Fatalf("execute requests = %d, want 1", len(execs))
	}
	if !bytes.Contains(execs[0], strb("12.34")) {
		t.Fatalf("execute request lacks the bound decimal string: % x", execs[0])
	}
}

func TestServerErrorPassesThroughAndPoolRecovers(t *testing.T) {
	fb, db := fakeDB(t)
	fb.Queue(2, serverError(-493, "Syntax error"))
	_, err := db.Query("SELEC bogus")
	var ce *cubrid.Error
	if !errors.As(err, &ce) || ce.Code != -493 {
		t.Fatalf("err = %v, want *cubrid.Error code -493", err)
	}
	// The session stayed healthy: the pool must reuse it, not dial anew.
	fb.Queue(2, prepPayload(21, 0, fcol{typ: 8, precision: 10, name: "id"}))
	fb.Queue(3, execSelect([][]byte{i32b(1)}))
	var v int64
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil || v != 1 {
		t.Fatalf("recovery query = (%d, %v), want (1, nil)", v, err)
	}
	if closes := fnRequests(fb, 31); len(closes) != 0 {
		t.Fatalf("CON_CLOSE requests = %d, want 0 (healthy conn discarded)", len(closes))
	}
}

func TestContextCancelPoisonsConn(t *testing.T) {
	_, db := fakeDB(t)
	ctx := context.Background()
	sc, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	expired, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)
	if _, err := sc.QueryContext(expired, "SELECT 1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if err := sc.PingContext(ctx); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("ping after poisoning = %v, want driver.ErrBadConn", err)
	}
}

func TestResetSessionRollsBackAndRestoresAutocommit(t *testing.T) {
	fb := prototest.Start(t)
	cfg, err := cubrid.ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dc, err := NewConnector(cfg).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	c := dc.(*conn)

	if !c.IsValid() {
		t.Fatal("fresh adapter conn invalid")
	}
	if _, err := c.BeginTx(ctx, driver.TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.ResetSession(ctx); err != nil {
		t.Fatalf("ResetSession on open tx = %v", err)
	}
	rbs := fnRequests(fb, 1)
	if len(rbs) != 1 || !bytes.Equal(rbs[0], []byte{1, 0, 0, 0, 1, 2}) {
		t.Fatalf("END_TRAN requests = %v, want one rollback", rbs)
	}

	// Poisoned conn: ResetSession must report driver.ErrBadConn.
	expired, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)
	if _, err := c.QueryContext(expired, "SELECT 1", nil); err == nil {
		t.Fatal("want error from expired context")
	}
	if c.IsValid() {
		t.Fatal("IsValid() true on poisoned conn")
	}
	if err := c.ResetSession(ctx); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("ResetSession on poisoned conn = %v, want driver.ErrBadConn", err)
	}
}

func TestLobColumnMaterialized(t *testing.T) {
	fb, db := fakeDB(t)
	fb.Queue(2, prepPayload(21, 0, fcol{typ: 24, name: "doc"})) // CLOB
	fb.Queue(3, execSelect([][]byte{packedLob(24, 5)}))
	r := &prototest.RespWriter{}
	r.I32(5).Raw([]byte("hello"))
	fb.Queue(37, r.Bytes()) // READ_LOB

	var s string
	if err := db.QueryRow("SELECT doc FROM t").Scan(&s); err != nil {
		t.Fatal(err)
	}
	if s != "hello" {
		t.Fatalf("CLOB = %q, want %q", s, "hello")
	}
}

func TestLobColumnOverCapErrors(t *testing.T) {
	fb, db := fakeDB(t)
	fb.Queue(2, prepPayload(21, 0, fcol{typ: 23, name: "bin"})) // BLOB
	fb.Queue(3, execSelect([][]byte{packedLob(23, maxLobBytes+1)}))

	var b []byte
	err := db.QueryRow("SELECT bin FROM t").Scan(&b)
	if err == nil || !strings.Contains(err.Error(), "native API") {
		t.Fatalf("err = %v, want over-cap error pointing at the native API", err)
	}
}
