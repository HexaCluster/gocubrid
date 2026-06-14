package cubrid

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/hexacluster/gocubrid/internal/prototest"
)

// executeSelectPayload: 2 total rows, inline first batch with int values
// 7 and 9, fetch-completed flag set (broker is V12 => V5+ fields present).
func executeSelectPayload() []byte {
	e := &prototest.RespWriter{}
	e.I32(2)                         // rc: total row count
	e.B(0)                           // cache reusable
	e.I32(1)                         // one query result block
	e.B(21)                          // stmt type SELECT
	e.I32(2)                         // result count
	e.Raw(make([]byte, 8))           // result OID
	e.I32(0).I32(0)                  // server cache times
	e.B(0)                           // include_column_info = 0 (V2+)
	e.I32(0)                         // shard id (V5+)
	e.I32(2)                         // inline fetch rescode (tuple count follows)
	e.I32(2)                         // fetched tuple count
	e.I32(1)                         // tuple 1 index
	e.Raw(make([]byte, 8))           // tuple 1 OID (zero-filled, always present)
	e.I32(4).Raw([]byte{0, 0, 0, 7}) // col value: INT 7
	e.I32(2)                         // tuple 2 index
	e.Raw(make([]byte, 8))           // tuple 2 OID
	e.I32(4).Raw([]byte{0, 0, 0, 9}) // col value: INT 9
	e.B(1)                           // fetch completed (V5+)
	return e.Bytes()
}

func TestQueryReadsInlineBatch(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(2, preparePayload())
	stmt, err := conn.Prepare(context.Background(), "SELECT id FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	fb.Queue(3, executeSelectPayload())
	rows, err := stmt.Query(context.Background(), int32(0))
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for {
		row, err := rows.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, row[0].(int32))
	}
	if len(got) != 2 || got[0] != 7 || got[1] != 9 {
		t.Fatalf("got %v", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}

// A SET(INT) column arrives in PREPARE metadata as collection flag 1 with
// TypeCode demoted to the element type (JDBC UColumnInfo.confirmType);
// the tuple value itself is a collection payload. The decode dispatch must
// route on the collection flag, not the raw TypeCode.
func TestQueryDecodesCollectionColumn(t *testing.T) {
	fb, conn := fakeConn(t)

	p := &prototest.RespWriter{}
	p.I32(35)              // statement handle
	p.I32(0)               // result cache lifetime
	p.B(21)                // command type: SELECT
	p.I32(0)               // parameter count
	p.B(0)                 // updatable
	p.I32(1)               // column count
	p.B(0x80 | 1<<5)       // extended 2-byte type form, collection flag 1 = SET
	p.B(8)                 // element type: INT
	p.I16(0).I32(0)        // scale, precision
	p.Str("s")             // column label
	p.Str("s")             // attribute name
	p.Str("t")             // table name
	p.B(0)                 // nullable
	p.Str("")              // default value
	p.Raw(make([]byte, 7)) // flag bytes
	fb.Queue(2, p.Bytes())

	coll := &prototest.RespWriter{}
	coll.B(8)   // element type: INT
	coll.I32(2) // element count
	coll.I32(4).Raw([]byte{0, 0, 0, 7})
	coll.I32(4).Raw([]byte{0, 0, 0, 9})
	cb := coll.Bytes()

	e := &prototest.RespWriter{}
	e.I32(1)                      // rc: total row count
	e.B(0)                        // cache reusable
	e.I32(1)                      // one query result block
	e.B(21)                       // stmt type SELECT
	e.I32(1)                      // result count
	e.Raw(make([]byte, 8))        // result OID
	e.I32(0).I32(0)               // server cache times
	e.B(0)                        // include_column_info (V2+)
	e.I32(0)                      // shard id (V5+)
	e.I32(1)                      // inline fetch rescode
	e.I32(1)                      // fetched tuple count
	e.I32(1)                      // tuple index
	e.Raw(make([]byte, 8))        // tuple OID
	e.I32(int32(len(cb))).Raw(cb) // col value: SET{7, 9}
	e.B(1)                        // fetch completed (V5+)
	fb.Queue(3, e.Bytes())

	stmt, err := conn.Prepare(context.Background(), "SELECT s FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if c := stmt.Columns()[0]; c.TypeCode != 8 || c.Collection != 1 {
		t.Fatalf("column = %+v", c)
	}
	rows, err := stmt.Query(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	row, err := rows.Next()
	if err != nil {
		t.Fatal(err)
	}
	vs, ok := row[0].([]any)
	if !ok || len(vs) != 2 || vs[0].(int32) != 7 || vs[1].(int32) != 9 {
		t.Fatalf("got %#v, want []any{7, 9}", row[0])
	}
}

func TestExecReportsAffectedRows(t *testing.T) {
	fb, conn := fakeConn(t)

	// Dedicated INSERT prepare payload (no offset-patching of preparePayload).
	p := &prototest.RespWriter{}
	p.I32(34) // handle
	p.I32(0)  // cache lifetime
	p.B(20)   // INSERT
	p.I32(1)  // param count
	p.B(0)    // updatable
	p.I32(0)  // no columns
	fb.Queue(2, p.Bytes())

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
	fb.Queue(3, e.Bytes())

	stmt, err := conn.Prepare(context.Background(), "INSERT INTO t VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	res, err := stmt.Exec(context.Background(), int32(5))
	if err != nil {
		t.Fatal(err)
	}
	if res.AffectedRows != 1 {
		t.Fatalf("affected = %d", res.AffectedRows)
	}
}

func TestExecArgCountMismatch(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(2, preparePayload())
	stmt, err := conn.Prepare(context.Background(), "SELECT id FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Query(context.Background()); err == nil {
		t.Fatal("want error for missing bind arg")
	}
}
