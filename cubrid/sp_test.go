package cubrid

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/hexacluster/gocubrid/internal/prototest"
)

// prepareCallPayload builds a PREPARE response for a CALL_SP statement:
// handle 44, command type CALL_SP (126), nParams markers, no column
// metadata (the broker sends none for CALL_SP; the client pads the column
// list to nParams+1, mirroring JDBC's columnNumber override).
func prepareCallPayload(nParams int32) []byte {
	p := &prototest.RespWriter{}
	p.I32(44)      // statement handle
	p.I32(0)       // result cache lifetime
	p.B(126)       // command type: CALL_SP
	p.I32(nParams) // parameter count
	p.B(0)         // updatable
	p.I32(0)       // column count
	return p.Bytes()
}

// executeCallPayload builds the EXECUTE response for a CALL_SP statement
// at V12: one result row, no inline fetch block (only SELECT inlines).
func executeCallPayload() []byte {
	e := &prototest.RespWriter{}
	e.I32(1)               // rc: one result row
	e.B(0)                 // cache reusable
	e.I32(1)               // one result block
	e.B(126)               // stmt type CALL_SP
	e.I32(1)               // result count
	e.Raw(make([]byte, 8)) // result OID
	e.I32(0).I32(0)        // server cache times
	e.B(0)                 // include_column_info (V2+)
	e.I32(0)               // shard id (V5+)
	return e.Bytes()
}

// fetchCallRowPayload builds the FETCH response carrying the CALL_SP value
// row [NULL, INT 42, VARCHAR "hi"]: every value is self-described (JDBC
// UStatement.readTypeFromData), here one with the legacy 1-byte prefix and
// one with the extended 2-byte form.
func fetchCallRowPayload() []byte {
	f := &prototest.RespWriter{}
	f.I32(0)                                       // fetch status
	f.I32(1)                                       // tuple count
	f.I32(1)                                       // tuple index
	f.Raw(make([]byte, 8))                         // tuple OID
	f.I32(-1)                                      // value 0 (statement slot): NULL
	f.I32(5).B(8).I32(42)                          // value 1: [type INT][4-byte payload]
	f.I32(5).B(0x84).B(2).Raw([]byte{'h', 'i', 0}) // value 2: extended prefix, VARCHAR "hi"
	f.B(1)                                         // fetch completed (V5+)
	return f.Bytes()
}

func TestPrepareCallSendsFlagAndPadsColumns(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(2, prepareCallPayload(2))
	sql := "CALL spfoo(?, ?)"
	stmt, err := conn.PrepareCall(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
	reqs := fb.Requests()
	req := reqs[len(reqs)-1]
	if req[0] != 2 {
		t.Fatalf("function code = %d, want 2 (PREPARE)", req[0])
	}
	// Layout: [fn][len][sql NUL][len=1][flag][len=1][autocommit].
	flagOff := 1 + 4 + len(sql) + 1 + 4
	if got := req[flagOff]; got != 0x40 {
		t.Fatalf("prepare flag byte = 0x%02x, want 0x40 (PREPARE_CALL)", got)
	}
	if stmt.ParamCount() != 2 {
		t.Fatalf("ParamCount = %d", stmt.ParamCount())
	}
	// CALL_SP result rows are [slot, marker1..N]: cols padded to nParams+1.
	if n := len(stmt.Columns()); n != 3 {
		t.Fatalf("columns = %d, want 3 (param count + 1)", n)
	}
}

// TestPrepareCallRejectsBadParamCount feeds hostile wire parameter counts:
// the count is used as an allocation length (the CALL_SP column padding
// here, RegisterOutParam's marker slice later), so a negative value must
// not panic make and a huge one must not pre-allocate gigabytes. Every
// marker is a literal '?' in the SQL the client itself sent, so a count
// beyond the statement length is provably corrupt.
func TestPrepareCallRejectsBadParamCount(t *testing.T) {
	const sql = "CALL spfoo(?, ?)"
	for _, tc := range []struct {
		name    string
		nParams int32
	}{
		{"negative", -2},
		{"most negative", math.MinInt32},
		{"huge", 1 << 27},
		{"max int32", math.MaxInt32},
		{"exceeds sql length", int32(len(sql)) + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fb, conn := fakeConn(t)
			fb.Queue(2, prepareCallPayload(tc.nParams))
			stmt, err := conn.PrepareCall(context.Background(), sql)
			if err == nil {
				t.Fatalf("PrepareCall accepted parameter count %d: %+v", tc.nParams, stmt)
			}
			if !strings.Contains(err.Error(), "parameter count") {
				t.Fatalf("error %q does not mention parameter count", err)
			}
		})
	}
}

func TestCallExecuteSendsParamModesAndParsesOutValues(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(2, prepareCallPayload(2))
	stmt, err := conn.PrepareCall(context.Background(), "CALL spfoo(?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	if err := stmt.RegisterOutParam(1); err != nil {
		t.Fatal(err)
	}
	if err := stmt.RegisterOutParam(2); err != nil {
		t.Fatal(err)
	}
	fb.Queue(3, executeCallPayload())
	fb.Queue(8, fetchCallRowPayload())
	res, err := stmt.Exec(context.Background(), nil, int32(7))
	if err != nil {
		t.Fatal(err)
	}
	if res.AffectedRows != 1 {
		t.Fatalf("AffectedRows = %d", res.AffectedRows)
	}

	var execReq, fetchReq []byte
	for _, r := range fb.Requests() {
		switch r[0] {
		case 3:
			execReq = r
		case 8:
			fetchReq = r
		}
	}
	if execReq == nil {
		t.Fatal("no EXECUTE request seen")
	}
	// Param-mode slot offset: fn(1) + handle arg(8) + flag arg(5) +
	// max-col arg(8) + max-fetch arg(8) = 30.
	want := []byte{0, 0, 0, 2, 2, 3} // len 2; marker1 OUT (nil arg), marker2 INOUT
	if got := execReq[30 : 30+len(want)]; string(got) != string(want) {
		t.Fatalf("param-mode slot = % x, want % x", got, want)
	}
	if fetchReq == nil {
		t.Fatal("no FETCH request for the CALL result row")
	}
	// FETCH args: [len 4][handle 44][len 4][start index 1].
	wantFetch := []byte{0, 0, 0, 4, 0, 0, 0, 44, 0, 0, 0, 4, 0, 0, 0, 1}
	if string(fetchReq[1:17]) != string(wantFetch) {
		t.Fatalf("fetch request = % x, want % x", fetchReq[1:17], wantFetch)
	}

	got := stmt.OutValues()
	if len(got) != 2 {
		t.Fatalf("OutValues = %#v", got)
	}
	if v, ok := got[0].(int32); !ok || v != 42 {
		t.Fatalf("out 1 = %#v, want int32(42)", got[0])
	}
	if v, ok := got[1].(string); !ok || v != "hi" {
		t.Fatalf("out 2 = %#v, want \"hi\"", got[1])
	}
}

// Query on a CALL statement yields the value row itself.
func TestCallQueryReturnsValueRow(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(2, prepareCallPayload(2))
	stmt, err := conn.PrepareCall(context.Background(), "CALL spfoo(?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	fb.Queue(3, executeCallPayload())
	fb.Queue(8, fetchCallRowPayload())
	rows, err := stmt.Query(context.Background(), nil, "in")
	if err != nil {
		t.Fatal(err)
	}
	row, err := rows.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(row) != 3 || row[0] != nil || row[1].(int32) != 42 || row[2].(string) != "hi" {
		t.Fatalf("call row = %#v", row)
	}
	if _, err := rows.Next(); err == nil {
		t.Fatal("expected EOF after the single CALL row")
	}
}

// A RESULTSET-typed OUT value (server-side cursor, resolved via
// MAKE_OUT_RS in JDBC) is out of scope: it must surface a clear
// unsupported error, not a generic decode failure.
func TestCallResultsetOutValueUnsupported(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(2, prepareCallPayload(1))
	stmt, err := conn.PrepareCall(context.Background(), "CALL spcursor(?)")
	if err != nil {
		t.Fatal(err)
	}
	if err := stmt.RegisterOutParam(1); err != nil {
		t.Fatal(err)
	}
	fb.Queue(3, executeCallPayload())
	f := &prototest.RespWriter{}
	f.I32(0)               // fetch status
	f.I32(1)               // tuple count
	f.I32(1)               // tuple index
	f.Raw(make([]byte, 8)) // tuple OID
	f.I32(-1)              // value 0: NULL
	f.I32(5).B(20).I32(99) // value 1: U_TYPE_RESULTSET, result id 99
	f.B(1)
	fb.Queue(8, f.Bytes())
	_, err = stmt.Exec(context.Background(), nil)
	if err == nil {
		t.Fatal("Exec accepted a RESULTSET OUT value")
	}
	if !strings.Contains(err.Error(), "RESULTSET") {
		t.Fatalf("error %q does not name RESULTSET", err)
	}
}

func TestRegisterOutParamValidation(t *testing.T) {
	fb, conn := fakeConn(t)

	// Not a CALL statement.
	fb.Queue(2, preparePayload()) // SELECT, 1 param
	sel, err := conn.Prepare(context.Background(), "SELECT id FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	if err := sel.RegisterOutParam(1); err == nil {
		t.Fatal("RegisterOutParam accepted a non-CALL statement")
	}

	fb.Queue(2, prepareCallPayload(2))
	call, err := conn.PrepareCall(context.Background(), "CALL spfoo(?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	for _, idx := range []int{0, -1, 3} {
		if err := call.RegisterOutParam(idx); err == nil {
			t.Fatalf("RegisterOutParam accepted index %d of 2", idx)
		}
	}
	if err := call.RegisterOutParam(2); err != nil {
		t.Fatal(err)
	}
}

// OutValues is nil before execution and empty when nothing was registered.
func TestOutValuesBeforeExecute(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(2, prepareCallPayload(1))
	stmt, err := conn.PrepareCall(context.Background(), "CALL spfoo(?)")
	if err != nil {
		t.Fatal(err)
	}
	if got := stmt.OutValues(); got != nil {
		t.Fatalf("OutValues before execute = %#v", got)
	}
}
