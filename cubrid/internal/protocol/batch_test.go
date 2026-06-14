package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// Request layout per JDBC UStatement.writeExecuteBatchRequest: fn 21,
// [handle int][query timeout int, V4+ only][autocommit byte]; the bind
// parameters for every batch row follow, appended by the caller.
func TestExecuteBatchPrepRequestV12(t *testing.T) {
	w := NewWriter()
	ExecuteBatchPrepRequest(w, 7, true, Version(ProtoV12))

	want := []byte{
		FnExecuteBatchPrep,
		0, 0, 0, 4, 0, 0, 0, 7, // statement handle
		0, 0, 0, 4, 0, 0, 0, 0, // query timeout ms (V4+)
		0, 0, 0, 1, 1, // autocommit on
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

func TestExecuteBatchPrepRequestV3HasNoTimeout(t *testing.T) {
	w := NewWriter()
	ExecuteBatchPrepRequest(w, 9, false, Version(ProtoV3))

	want := []byte{
		FnExecuteBatchPrep,
		0, 0, 0, 4, 0, 0, 0, 9, // statement handle
		0, 0, 0, 1, 0, // autocommit off; no timeout below V4
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

// Request layout per JDBC UConnection.batchExecute: fn 20,
// [autocommit byte][query timeout int, V4+ only] then one NUL-terminated
// string per SQL statement.
func TestExecuteBatchSQLRequestV12(t *testing.T) {
	w := NewWriter()
	ExecuteBatchSQLRequest(w, true, []string{"ab", "c"}, Version(ProtoV12))

	want := []byte{
		FnExecuteBatch,
		0, 0, 0, 1, 1, // autocommit on
		0, 0, 0, 4, 0, 0, 0, 0, // query timeout ms (V4+)
		0, 0, 0, 3, 'a', 'b', 0, // sql 1
		0, 0, 0, 2, 'c', 0, // sql 2
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

func TestExecuteBatchSQLRequestV3HasNoTimeout(t *testing.T) {
	w := NewWriter()
	ExecuteBatchSQLRequest(w, false, []string{"x"}, Version(ProtoV3))

	want := []byte{
		FnExecuteBatch,
		0, 0, 0, 1, 0, // autocommit off; no timeout below V4
		0, 0, 0, 2, 'x', 0, // sql 1
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

// batchBody assembles a batch response body (the bytes after the leading
// status int) for parser tests.
type batchBody struct{ buf []byte }

func (b *batchBody) i32(v int32) *batchBody {
	b.buf = binary.BigEndian.AppendUint32(b.buf, uint32(v))
	return b
}

func (b *batchBody) i16(v int16) *batchBody {
	b.buf = binary.BigEndian.AppendUint16(b.buf, uint16(v))
	return b
}

func (b *batchBody) raw(p ...byte) *batchBody { b.buf = append(b.buf, p...); return b }

func (b *batchBody) ok(affected int32) *batchBody {
	return b.i32(affected).i32(0).i16(0).i16(0) // result + 8 reserved bytes (JCI 3.0)
}

func (b *batchBody) fail(code int32, msg string) *batchBody {
	b.i32(-1).i32(code).i32(int32(len(msg) + 1))
	return b.raw(append([]byte(msg), 0)...)
}

// Response layout per JDBC UStatement.executeBatchInternal: [count int]
// then per result [result int]; negative -> [err code int][err msg
// length-prefixed string]; non-negative -> 8 reserved bytes
// (int+short+short). Trailing [shard id int] on V5+.
func TestParseBatchResponseMixedV12(t *testing.T) {
	b := (&batchBody{}).i32(3)
	b.ok(1)
	b.fail(-670, "constraint violation")
	b.ok(2)
	b.i32(0) // shard id (V5+)

	got, err := ParseBatchResponse(NewReader(b.buf), Version(ProtoV12), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("results = %d, want 3", len(got))
	}
	if got[0].AffectedRows != 1 || got[0].Err != nil {
		t.Fatalf("result 0 = %+v", got[0])
	}
	var pe *Error
	if !errors.As(got[1].Err, &pe) || pe.Code != -670 || pe.Message != "constraint violation" {
		t.Fatalf("result 1 err = %v", got[1].Err)
	}
	if got[1].AffectedRows != 0 {
		t.Fatalf("result 1 affected = %d", got[1].AffectedRows)
	}
	if got[2].AffectedRows != 2 || got[2].Err != nil {
		t.Fatalf("result 2 = %+v", got[2])
	}
}

// The SQL batch (fn 20) response carries one extra statement-type byte
// per result before the result int.
func TestParseBatchResponseWithStmtType(t *testing.T) {
	b := (&batchBody{}).i32(2)
	b.raw(20).ok(1)                 // INSERT
	b.raw(22).fail(-494, "boom: x") // UPDATE
	b.i32(0)                        // shard id

	got, err := ParseBatchResponse(NewReader(b.buf), Version(ProtoV12), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].AffectedRows != 1 || got[0].Err != nil {
		t.Fatalf("results = %+v", got)
	}
	var pe *Error
	if !errors.As(got[1].Err, &pe) || pe.Code != -494 || pe.Message != "boom: x" {
		t.Fatalf("result 1 err = %v", got[1].Err)
	}
}

// Below V5 there is no trailing shard id; the parser must not demand one.
func TestParseBatchResponseV4HasNoShardID(t *testing.T) {
	b := (&batchBody{}).i32(1)
	b.ok(5)

	got, err := ParseBatchResponse(NewReader(b.buf), Version(ProtoV4), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].AffectedRows != 5 || got[0].Err != nil {
		t.Fatalf("results = %+v", got)
	}
}

// A wire-supplied count that cannot fit the remaining bytes must be
// rejected before allocation, like the prepare column-count guard.
func TestParseBatchResponseRejectsBogusCount(t *testing.T) {
	for _, count := range []int32{-1, 1 << 30} {
		b := (&batchBody{}).i32(count)
		b.ok(1)
		if _, err := ParseBatchResponse(NewReader(b.buf), Version(ProtoV12), false); err == nil {
			t.Fatalf("count %d: want error", count)
		}
	}
}

func TestParseBatchResponseShortBuffer(t *testing.T) {
	b := (&batchBody{}).i32(2)
	b.ok(1) // second result missing entirely
	if _, err := ParseBatchResponse(NewReader(b.buf), Version(ProtoV12), false); err == nil {
		t.Fatal("want error for truncated response")
	}
}
