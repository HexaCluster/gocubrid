package cubrid

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
	"github.com/hexacluster/gocubrid/internal/prototest"
)

// batchPrepare builds a PREPARE response for a no-column statement with
// the given command type and parameter count.
func batchPrepare(handle int32, cmdType byte, paramCount int32) []byte {
	p := &prototest.RespWriter{}
	p.I32(handle)     // statement handle
	p.I32(0)          // result cache lifetime
	p.B(cmdType)      // command type
	p.I32(paramCount) // parameter count
	p.B(0)            // updatable
	p.I32(0)          // column count
	return p.Bytes()
}

// batchOK appends one successful batch result: result int + 8 reserved
// bytes (int+short+short, JCI 3.0).
func batchOK(p *prototest.RespWriter, affected int32) {
	p.I32(affected).I32(0).I16(0).I16(0)
}

// batchFail appends one failed batch result: negative result, error code,
// length-prefixed message.
func batchFail(p *prototest.RespWriter, code int32, msg string) {
	p.I32(-1).I32(code).Str(msg)
}

func TestExecBatchSendsRowsAndParsesResults(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnPrepare, batchPrepare(11, cmdTypeInsert, 2))

	p := &prototest.RespWriter{}
	p.I32(0) // status
	p.I32(3) // result count
	batchOK(p, 1)
	batchOK(p, 1)
	batchOK(p, 1)
	p.I32(0) // shard id (V5+)
	fb.Queue(protocol.FnExecuteBatchPrep, p.Bytes())

	stmt, err := conn.Prepare(context.Background(), "INSERT INTO t VALUES (?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	rows := [][]any{{int32(1), "a"}, {int32(2), "b"}, {int32(3), "c"}}
	res, err := stmt.ExecBatch(context.Background(), rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 3 {
		t.Fatalf("results = %d, want 3", len(res))
	}
	for i, r := range res {
		if r.AffectedRows != 1 || r.Err != nil {
			t.Fatalf("result %d = %+v", i, r)
		}
	}

	// The request must be the fn 21 header followed by every row's params
	// back-to-back with no separators.
	want := protocol.NewWriter()
	protocol.ExecuteBatchPrepRequest(want, 11, true, protocol.Version(protocol.ProtoV12))
	for _, row := range rows {
		for _, a := range row {
			if err := protocol.EncodeParam(want, a); err != nil {
				t.Fatal(err)
			}
		}
	}
	reqs := fb.Requests()
	got := reqs[len(reqs)-1]
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("request = % X\nwant      % X", got, want.Bytes())
	}
}

// A failed statement in the middle must surface as a per-row *Error while
// its neighbors keep their counts.
func TestExecBatchMixedError(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnPrepare, batchPrepare(12, cmdTypeInsert, 1))

	p := &prototest.RespWriter{}
	p.I32(0)
	p.I32(3)
	batchOK(p, 1)
	batchFail(p, -670, "Operation would have caused one or more unique constraint violations.")
	batchOK(p, 1)
	p.I32(0)
	fb.Queue(protocol.FnExecuteBatchPrep, p.Bytes())

	stmt, err := conn.Prepare(context.Background(), "INSERT INTO t VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	res, err := stmt.ExecBatch(context.Background(), [][]any{{int32(1)}, {int32(1)}, {int32(2)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 3 || res[0].Err != nil || res[2].Err != nil {
		t.Fatalf("results = %+v", res)
	}
	var ce *Error
	if !errors.As(res[1].Err, &ce) || ce.Code != -670 {
		t.Fatalf("middle result err = %v", res[1].Err)
	}
}

func TestExecBatchValidatesRowLength(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnPrepare, batchPrepare(13, cmdTypeInsert, 2))

	stmt, err := conn.Prepare(context.Background(), "INSERT INTO t VALUES (?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	before := len(fb.Requests())
	if _, err := stmt.ExecBatch(context.Background(), [][]any{{int32(1), "a"}, {int32(2)}}); err == nil {
		t.Fatal("want arity error")
	}
	if got := len(fb.Requests()); got != before {
		t.Fatalf("a request was sent despite the arity error (%d -> %d)", before, got)
	}
}

// Zero batch rows: nothing to send, nothing to report.
func TestExecBatchEmpty(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnPrepare, batchPrepare(14, cmdTypeInsert, 1))

	stmt, err := conn.Prepare(context.Background(), "INSERT INTO t VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	before := len(fb.Requests())
	res, err := stmt.ExecBatch(context.Background(), nil)
	if err != nil || len(res) != 0 {
		t.Fatalf("ExecBatch(nil) = %v, %v", res, err)
	}
	if got := len(fb.Requests()); got != before {
		t.Fatal("a request was sent for an empty batch")
	}
}

// The fn 21 wire format carries no row count, the server infers it from
// the parameter count, so a no-parameter statement cannot be batched
// more than once.
func TestExecBatchZeroParamsMultipleRows(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnPrepare, batchPrepare(15, cmdTypeInsert, 0))

	stmt, err := conn.Prepare(context.Background(), "INSERT INTO t VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.ExecBatch(context.Background(), [][]any{{}, {}}); err == nil {
		t.Fatal("want error for multi-row batch without parameters")
	}
}

func TestExecBatchClosedStmt(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnPrepare, batchPrepare(16, cmdTypeInsert, 1))

	stmt, err := conn.Prepare(context.Background(), "INSERT INTO t VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	if err := stmt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.ExecBatch(context.Background(), [][]any{{int32(1)}}); err == nil {
		t.Fatal("want error on closed statement")
	}
}

// The fn 20 response carries an extra statement-type byte per result.
func TestExecBatchSQLMixedResults(t *testing.T) {
	fb, conn := fakeConn(t)

	p := &prototest.RespWriter{}
	p.I32(0) // status
	p.I32(3) // result count
	p.B(20)  // stmt type: INSERT
	batchOK(p, 1)
	p.B(22) // stmt type: UPDATE
	batchFail(p, -494, "Semantic: t2 does not exist.")
	p.B(23) // stmt type: DELETE
	batchOK(p, 4)
	p.I32(0) // shard id
	fb.Queue(protocol.FnExecuteBatch, p.Bytes())

	sqls := []string{"INSERT INTO t VALUES (1)", "UPDATE t2 SET x = 1", "DELETE FROM t"}
	res, err := conn.ExecBatchSQL(context.Background(), sqls)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 3 || res[0].AffectedRows != 1 || res[2].AffectedRows != 4 {
		t.Fatalf("results = %+v", res)
	}
	var ce *Error
	if !errors.As(res[1].Err, &ce) || ce.Code != -494 {
		t.Fatalf("result 1 err = %v", res[1].Err)
	}

	want := protocol.NewWriter()
	protocol.ExecuteBatchSQLRequest(want, true, sqls, protocol.Version(protocol.ProtoV12))
	reqs := fb.Requests()
	got := reqs[len(reqs)-1]
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("request = % X\nwant      % X", got, want.Bytes())
	}
}

func TestExecBatchSQLEmpty(t *testing.T) {
	fb, conn := fakeConn(t)
	before := len(fb.Requests())
	res, err := conn.ExecBatchSQL(context.Background(), nil)
	if err != nil || len(res) != 0 {
		t.Fatalf("ExecBatchSQL(nil) = %v, %v", res, err)
	}
	if got := len(fb.Requests()); got != before {
		t.Fatal("a request was sent for an empty batch")
	}
}

// Version pin: a V4 broker still gets the query timeout (V4+) but its
// response carries no trailing shard id (V5+).
func TestExecBatchV4PinnedNoShardID(t *testing.T) {
	fb := prototest.Start(t)
	fb.SetProtocolVersion(protocol.ProtoV4)
	cfg, err := ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	fb.Queue(protocol.FnPrepare, batchPrepare(17, cmdTypeInsert, 1))
	p := &prototest.RespWriter{}
	p.I32(0) // status
	p.I32(1) // result count
	batchOK(p, 1)
	// no shard id below V5
	fb.Queue(protocol.FnExecuteBatchPrep, p.Bytes())

	stmt, err := conn.Prepare(context.Background(), "INSERT INTO t VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	res, err := stmt.ExecBatch(context.Background(), [][]any{{int32(7)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].AffectedRows != 1 || res[0].Err != nil {
		t.Fatalf("results = %+v", res)
	}

	want := protocol.NewWriter()
	protocol.ExecuteBatchPrepRequest(want, 17, true, protocol.Version(protocol.ProtoV4))
	if err := protocol.EncodeParam(want, int32(7)); err != nil {
		t.Fatal(err)
	}
	reqs := fb.Requests()
	got := reqs[len(reqs)-1]
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("request = % X\nwant      % X", got, want.Bytes())
	}
}
