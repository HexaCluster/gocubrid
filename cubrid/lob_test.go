package cubrid

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
	"github.com/hexacluster/gocubrid/internal/prototest"
)

// lobPacked builds a packed LOB handle as the server sends it.
func lobPacked(dbType int32, size int64, locator string) []byte {
	p := &prototest.RespWriter{}
	p.I32(dbType).I64(size).I32(int32(len(locator) + 1))
	p.Raw([]byte(locator)).B(0)
	return p.Bytes()
}

// newLobPayload builds a NEW_LOB response: status = packed handle byte
// length, payload = the handle.
func newLobPayload(packed []byte) []byte {
	p := &prototest.RespWriter{}
	p.I32(int32(len(packed))).Raw(packed)
	return p.Bytes()
}

func TestNewBlobParsesHandle(t *testing.T) {
	fb, conn := fakeConn(t)
	packed := lobPacked(protocol.TypeBlob, 0, "lob/loc.1")
	fb.Queue(protocol.FnNewLOB, newLobPayload(packed))

	lob, err := conn.NewBlob(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lob.Size() != 0 || lob.IsClob() {
		t.Fatalf("lob = %+v", lob)
	}

	w := protocol.NewWriter()
	protocol.NewLobRequest(w, protocol.TypeBlob)
	reqs := fb.Requests()
	if len(reqs) == 0 || !bytes.Equal(reqs[0], w.Bytes()) {
		t.Fatalf("request = % X\nwant      % X", reqs[0], w.Bytes())
	}
}

func TestNewClobParsesHandle(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnNewLOB, newLobPayload(lobPacked(protocol.TypeClob, 0, "lob/loc.2")))

	lob, err := conn.NewClob(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !lob.IsClob() {
		t.Fatalf("lob = %+v", lob)
	}
	reqs := fb.Requests()
	w := protocol.NewWriter()
	protocol.NewLobRequest(w, protocol.TypeClob)
	if !bytes.Equal(reqs[0], w.Bytes()) {
		t.Fatalf("request = % X\nwant      % X", reqs[0], w.Bytes())
	}
}

// writeLobOK builds a WRITE_LOB response: status = bytes written.
func writeLobOK(n int32) []byte {
	p := &prototest.RespWriter{}
	p.I32(n)
	return p.Bytes()
}

// readLobPayload builds a READ_LOB response: status = bytes read, then
// the raw bytes.
func readLobPayload(b []byte) []byte {
	p := &prototest.RespWriter{}
	p.I32(int32(len(b))).Raw(b)
	return p.Bytes()
}

// chunkData returns deterministic non-repeating test bytes.
func chunkData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i>>8)
	}
	return b
}

// Append must split writes at the 128 KiB chunk cap, advance the offset
// per chunk, and track the size in the handle it sends.
func TestLobAppendChunks(t *testing.T) {
	fb, conn := fakeConn(t)
	packed := lobPacked(protocol.TypeBlob, 0, "lob/loc.3")
	fb.Queue(protocol.FnNewLOB, newLobPayload(packed))
	fb.Queue(protocol.FnWriteLOB, writeLobOK(protocol.LobMaxChunk))
	fb.Queue(protocol.FnWriteLOB, writeLobOK(3))

	lob, err := conn.NewBlob(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data := chunkData(protocol.LobMaxChunk + 3)
	n, err := lob.Append(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) || lob.Size() != int64(len(data)) {
		t.Fatalf("n = %d, size = %d, want %d", n, lob.Size(), len(data))
	}

	reqs := fb.Requests()[1:] // skip NEW_LOB
	if len(reqs) != 2 {
		t.Fatalf("write requests = %d, want 2", len(reqs))
	}
	h1, err := protocol.ParseLobHandle(packed)
	if err != nil {
		t.Fatal(err)
	}
	w1 := protocol.NewWriter()
	protocol.WriteLobRequest(w1, h1, 0, data[:protocol.LobMaxChunk])
	if !bytes.Equal(reqs[0], w1.Bytes()) {
		t.Fatalf("chunk 1 request mismatch (%d bytes vs %d)", len(reqs[0]), len(w1.Bytes()))
	}
	h2, err := protocol.ParseLobHandle(packed)
	if err != nil {
		t.Fatal(err)
	}
	h2.SetSize(protocol.LobMaxChunk) // size advanced by the first chunk
	w2 := protocol.NewWriter()
	protocol.WriteLobRequest(w2, h2, protocol.LobMaxChunk, data[protocol.LobMaxChunk:])
	if !bytes.Equal(reqs[1], w2.Bytes()) {
		t.Fatalf("chunk 2 request mismatch:\ngot  % X\nwant % X", reqs[1], w2.Bytes())
	}
}

// ReadAt must cap requests at 128 KiB, keep looping after SHORT server
// reads (confirmed live: the CAS answers a 128 KiB request with ~80 KiB),
// and treat only a 0-byte reply as end-of-LOB (io.EOF), JDBC
// CUBRIDBlob.getBytes semantics.
func TestLobReadAtChunksAndEOF(t *testing.T) {
	fb, conn := fakeConn(t)
	data := chunkData(protocol.LobMaxChunk + 3)
	packed := lobPacked(protocol.TypeBlob, int64(len(data)), "lob/loc.4")
	fb.Queue(protocol.FnNewLOB, newLobPayload(packed))
	// The server answers short (80000 of 131072), then short again, then
	// completes the tail request exactly.
	fb.Queue(protocol.FnReadLOB, readLobPayload(data[:80000]))
	fb.Queue(protocol.FnReadLOB, readLobPayload(data[80000:120000]))
	fb.Queue(protocol.FnReadLOB, readLobPayload(data[120000:]))

	lob, err := conn.NewBlob(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, len(data))
	n, err := lob.ReadAt(context.Background(), p, 0)
	if err != nil || n != len(data) {
		t.Fatalf("ReadAt = %d, %v", n, err)
	}
	if !bytes.Equal(p, data) {
		t.Fatal("ReadAt reassembled wrong bytes")
	}

	reqs := fb.Requests()[1:]
	if len(reqs) != 3 {
		t.Fatalf("read requests = %d, want 3", len(reqs))
	}
	h, err := protocol.ParseLobHandle(packed)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []struct {
		off int64
		n   int32
	}{
		{0, protocol.LobMaxChunk},         // full first request
		{80000, int32(len(data)) - 80000}, // resumes after the short read
		{120000, int32(len(data)) - 120000} /* tail */} {
		w := protocol.NewWriter()
		protocol.ReadLobRequest(w, h, want.off, want.n)
		if !bytes.Equal(reqs[i], w.Bytes()) {
			t.Fatalf("read request %d:\ngot  % X\nwant % X", i, reqs[i], w.Bytes())
		}
	}

	// Past the end the server reads 0 bytes: io.EOF with n = 0.
	fb.Queue(protocol.FnReadLOB, readLobPayload(nil))
	n, err = lob.ReadAt(context.Background(), p[:1], int64(len(data)))
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt past end = %d, %v; want 0, io.EOF", n, err)
	}
}

func TestClobStringReadsAll(t *testing.T) {
	fb, conn := fakeConn(t)
	text := "héllo, 한국어 CLOB"
	fb.Queue(protocol.FnNewLOB, newLobPayload(lobPacked(protocol.TypeClob, int64(len(text)), "lob/loc.5")))
	fb.Queue(protocol.FnReadLOB, readLobPayload([]byte(text)))

	lob, err := conn.NewClob(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := lob.String(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != text {
		t.Fatalf("String = %q, want %q", got, text)
	}
}

// lobColumnPrepare builds a PREPARE response with a single column of the
// given LOB type.
func lobColumnPrepare(typ byte) []byte {
	p := &prototest.RespWriter{}
	p.I32(40)              // statement handle
	p.I32(0)               // result cache lifetime
	p.B(21)                // command type: SELECT
	p.I32(0)               // parameter count
	p.B(0)                 // updatable
	p.I32(1)               // column count
	p.B(typ)               // legacy single-byte type form
	p.I16(0).I32(0)        // scale, precision
	p.Str("b")             // column label
	p.Str("b")             // attribute name
	p.Str("t")             // table name
	p.B(0)                 // nullable
	p.Str("")              // default value
	p.Raw(make([]byte, 7)) // flag bytes
	return p.Bytes()
}

// lobColumnExecute builds an EXECUTE response whose single tuple value is
// the packed handle (NULL when packed is nil).
func lobColumnExecute(packed []byte) []byte {
	e := &prototest.RespWriter{}
	e.I32(1)               // rc: total row count
	e.B(0)                 // cache reusable
	e.I32(1)               // one query result block
	e.B(21)                // stmt type SELECT
	e.I32(1)               // result count
	e.Raw(make([]byte, 8)) // result OID
	e.I32(0).I32(0)        // server cache times
	e.B(0)                 // include_column_info (V2+)
	e.I32(0)               // shard id (V5+)
	e.I32(1)               // inline fetch rescode
	e.I32(1)               // fetched tuple count
	e.I32(1)               // tuple index
	e.Raw(make([]byte, 8)) // tuple OID
	if packed == nil {
		e.I32(-1) // NULL column value
	} else {
		e.I32(int32(len(packed))).Raw(packed)
	}
	e.B(1) // fetch completed (V5+)
	return e.Bytes()
}

// A CLOB result column must surface as a read-only *Lob bound to the
// connection, not as a decode error.
func TestQueryInterceptsLobColumn(t *testing.T) {
	fb, conn := fakeConn(t)
	packed := lobPacked(protocol.TypeClob, 5, "lob/loc.6")
	fb.Queue(protocol.FnPrepare, lobColumnPrepare(protocol.TypeClob))
	fb.Queue(protocol.FnExecute, lobColumnExecute(packed))

	stmt, err := conn.Prepare(context.Background(), "SELECT b FROM t")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := stmt.Query(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r1, err := rows.Next()
	if err != nil {
		t.Fatal(err)
	}
	lob, ok := r1[0].(*Lob)
	if !ok {
		t.Fatalf("column value = %#v, want *Lob", r1[0])
	}
	if lob.Size() != 5 || !lob.IsClob() {
		t.Fatalf("lob = size %d, clob %v", lob.Size(), lob.IsClob())
	}

	// Result-set locators are read-only.
	if _, err := lob.Append(context.Background(), []byte("x")); err == nil {
		t.Fatal("Append on a result-set LOB must fail")
	}

	// But reads work over the same connection.
	fb.Queue(protocol.FnReadLOB, readLobPayload([]byte("hello")))
	got, err := lob.ReadAll(context.Background())
	if err != nil || string(got) != "hello" {
		t.Fatalf("ReadAll = %q, %v", got, err)
	}
}

func TestQueryNullLobColumn(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnPrepare, lobColumnPrepare(protocol.TypeBlob))
	fb.Queue(protocol.FnExecute, lobColumnExecute(nil))

	stmt, err := conn.Prepare(context.Background(), "SELECT b FROM t")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := stmt.Query(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r1, err := rows.Next()
	if err != nil {
		t.Fatal(err)
	}
	if r1[0] != nil {
		t.Fatalf("NULL LOB column = %#v, want nil", r1[0])
	}
}

// Binding a *Lob must encode the LOB type byte plus the packed handle
// (JDBC UOutputBuffer.addLob); a typed-nil *Lob binds SQL NULL.
func TestBindLobParam(t *testing.T) {
	fb, conn := fakeConn(t)
	packed := lobPacked(protocol.TypeBlob, 9, "lob/loc.7")
	fb.Queue(protocol.FnNewLOB, newLobPayload(packed))

	p := &prototest.RespWriter{}
	p.I32(41) // handle
	p.I32(0)  // cache lifetime
	p.B(20)   // INSERT
	p.I32(2)  // param count
	p.B(0)    // updatable
	p.I32(0)  // no columns
	fb.Queue(protocol.FnPrepare, p.Bytes())

	e := &prototest.RespWriter{}
	e.I32(1)               // rc: affected rows
	e.B(0)                 // cache reusable
	e.I32(1)               // one result
	e.B(20)                // stmt type INSERT
	e.I32(1)               // result count
	e.Raw(make([]byte, 8)) // OID
	e.I32(0).I32(0)        // cache times
	e.B(0)                 // include_column_info
	e.I32(0)               // shard id
	fb.Queue(protocol.FnExecute, e.Bytes())

	lob, err := conn.NewBlob(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.Prepare(context.Background(), "INSERT INTO t VALUES (?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec(context.Background(), lob, (*Lob)(nil)); err != nil {
		t.Fatal(err)
	}

	want := protocol.NewWriter()
	want.ArgByte(protocol.TypeBlob)
	want.ArgBytes(packed) // bound LOB: type byte + packed handle
	want.ArgByte(protocol.TypeNull)
	want.ArgNull() // typed-nil *Lob: SQL NULL
	reqs := fb.Requests()
	last := reqs[len(reqs)-1]
	if !bytes.HasSuffix(last, want.Bytes()) {
		t.Fatalf("execute request = % X\nwant suffix       % X", last, want.Bytes())
	}
}
