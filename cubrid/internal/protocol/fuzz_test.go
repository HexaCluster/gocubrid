package protocol

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// goldenResponses returns every captured golden response payload under
// testdata/frames/<ver>/<flow>/NNN-res.bin (real broker bytes from all
// five CUBRID versions) for use as fuzz seed corpora.
func goldenResponses(f *testing.F) [][]byte {
	f.Helper()
	pattern := filepath.Join("..", "..", "..", "testdata", "frames", "*", "*", "*-res.bin")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		f.Fatal(err)
	}
	if len(paths) == 0 {
		f.Fatalf("no golden frames match %s; capture them first", pattern)
	}
	out := make([][]byte, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			f.Fatal(err)
		}
		out = append(out, b)
	}
	return out
}

// frame wraps a payload in the 8-byte wire header ReadResponse expects.
func frame(payload []byte) []byte {
	hdr := make([]byte, 8, 8+len(payload))
	binary.BigEndian.PutUint32(hdr, uint32(len(payload)))
	return append(hdr, payload...)
}

// FuzzReadResponse hammers the frame reader with hostile streams. It must
// only ever return an error: no panic, and no allocation driven by a
// forged length header beyond the bytes actually present on the wire.
func FuzzReadResponse(f *testing.F) {
	for _, p := range goldenResponses(f) {
		f.Add(frame(p))
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})             // empty payload
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}) // length > maxPayloadSize
	f.Add([]byte{0x0f, 0xff, 0xff, 0xff, 0, 0, 0, 0}) // huge length, no payload
	f.Fuzz(func(t *testing.T, data []byte) {
		payload, _, err := ReadResponse(bytes.NewReader(data))
		if err != nil {
			return
		}
		want := binary.BigEndian.Uint32(data[0:4])
		if uint32(len(payload)) != want {
			t.Fatalf("payload = %d bytes, header said %d", len(payload), want)
		}
	})
}

// FuzzDecodeValue drives the column-value decoder across every type code,
// including (nested) collections. Guards must turn hostile inputs into
// errors, never a panic, hang, or count-driven huge allocation.
func FuzzDecodeValue(f *testing.F) {
	f.Add(byte(TypeInt), []byte{0, 0, 0, 1})
	f.Add(byte(TypeBigint), []byte{0, 0, 0, 0, 0, 0, 0, 1})
	f.Add(byte(TypeShort), []byte{0, 1})
	f.Add(byte(TypeFloat), []byte{0x3f, 0x80, 0, 0})
	f.Add(byte(TypeDouble), []byte{0x3f, 0xf0, 0, 0, 0, 0, 0, 0})
	f.Add(byte(TypeVarchar), []byte("hello\x00"))
	f.Add(byte(TypeNumeric), []byte("12.34\x00"))
	f.Add(byte(TypeJSON), []byte(`{"a":1}`+"\x00"))
	f.Add(byte(TypeVarbit), []byte{0xde, 0xad})
	f.Add(byte(TypeDate), []byte{0x07, 0xea, 0, 6, 0, 11})
	f.Add(byte(TypeDatetime), []byte{0x07, 0xea, 0, 6, 0, 11, 0, 12, 0, 34, 0, 56, 0x01, 0xf4})
	f.Add(byte(TypeTimestampTZ), append(
		[]byte{0x07, 0xea, 0, 6, 0, 11, 0, 13, 0, 30, 0, 0}, []byte("Asia/Seoul KST\x00")...))
	// SET of two INTs.
	f.Add(byte(TypeSet), []byte{
		TypeInt, 0, 0, 0, 2,
		0, 0, 0, 4, 0, 0, 0, 1,
		0, 0, 0, 4, 0, 0, 0, 2,
	})
	// SEQUENCE nesting a SEQUENCE of one INT (live 11.4 shape).
	f.Add(byte(TypeSequence), []byte{
		TypeSequence, 0, 0, 0, 1,
		0, 0, 0, 13,
		TypeInt, 0, 0, 0, 1,
		0, 0, 0, 4, 0, 0, 0, 7,
	})
	// NULL element (size 0) inside a collection.
	f.Add(byte(TypeMultiset), []byte{
		TypeVarchar, 0, 0, 0, 1,
		0, 0, 0, 0,
	})
	for _, p := range goldenResponses(f) {
		f.Add(byte(TypeVarchar), p)
		f.Add(byte(TypeSequence), p)
	}
	f.Fuzz(func(t *testing.T, typ byte, data []byte) {
		// Errors are fine (and expected for most mutations); the target is
		// panics, hangs, and unbounded allocation in the decode guards.
		_, _ = DecodeValue(typ, data)
	})
}

// FuzzDecodeSelfDescribedValue covers the U_TYPE_NULL-declared column path
// (one-byte and extended two-byte type prefixes).
func FuzzDecodeSelfDescribedValue(f *testing.F) {
	f.Add([]byte{TypeNull})
	f.Add([]byte{TypeInt, 0, 0, 0, 1})
	f.Add([]byte{TypeVarchar, 'h', 'i', 0})
	f.Add([]byte{0x80 | 0x20, 0, TypeInt, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 9}) // extended SET prefix
	f.Add([]byte{0x80, TypeInt, 0, 0, 0, 7})                                   // extended scalar prefix
	for _, p := range goldenResponses(f) {
		f.Add(p)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeSelfDescribedValue(data)
	})
}

// FuzzParseLobHandle attacks the packed-LOB-handle parser (locator length
// header is unauthenticated wire input).
func FuzzParseLobHandle(f *testing.F) {
	valid := make([]byte, 0, 32)
	valid = binary.BigEndian.AppendUint32(valid, uint32(TypeBlob))
	valid = binary.BigEndian.AppendUint64(valid, 5)
	valid = binary.BigEndian.AppendUint32(valid, 9)
	valid = append(valid, []byte("/lob/f.0\x00")...)
	f.Add(valid)
	f.Add([]byte{})
	f.Add(valid[:lobHandleHeader]) // header only, locator missing
	for _, p := range goldenResponses(f) {
		f.Add(p)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		h, err := ParseLobHandle(data)
		if err != nil {
			return
		}
		if got := h.Packed(); !bytes.Equal(got, data) {
			t.Fatalf("Packed() does not round-trip the input: %d vs %d bytes", len(got), len(data))
		}
	})
}

// FuzzParseBatchResponse drives the shared fn 20/21 batch-result parser at
// every protocol version with and without statement-type bytes.
func FuzzParseBatchResponse(f *testing.F) {
	// Two results: success (3 rows + 8 reserved bytes), then a server error.
	body := []byte{
		0, 0, 0, 2,
		0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0,
		0xff, 0xff, 0xff, 0xff, // result -1
		0xff, 0xff, 0xfc, 0x18, // err code -1000
		0, 0, 0, 4, 'b', 'a', 'd', 0,
	}
	f.Add(body, int(ProtoV12), false)
	f.Add(body, int(ProtoV4), false)
	f.Add(body, int(ProtoV0), true)
	f.Add([]byte{0, 0, 0, 0}, int(ProtoV6), false) // zero results
	for _, p := range goldenResponses(f) {
		f.Add(p, int(ProtoV12), false)
		f.Add(p, int(ProtoV6), true)
	}
	f.Fuzz(func(t *testing.T, data []byte, ver int, withStmtType bool) {
		out, err := ParseBatchResponse(NewReader(data), Version(ver), withStmtType)
		if err != nil {
			return
		}
		if len(out)*minBatchResultBytes > len(data) {
			t.Fatalf("%d results out of %d bytes: result count not bounded by input", len(out), len(data))
		}
	})
}

// FuzzParseConnectReply covers the handshake auth-reply parser (broker
// info, CAS id, session id) across forged payloads.
func FuzzParseConnectReply(f *testing.F) {
	valid := make([]byte, 0, 36)
	valid = binary.BigEndian.AppendUint32(valid, 4444)                  // CAS pid
	valid = append(valid, 0, 0, 0, 0, protoIndicator|ProtoV12, 0, 0, 0) // broker info
	valid = binary.BigEndian.AppendUint32(valid, 7)                     // CAS id (V4+)
	valid = append(valid, make([]byte, SessionIDSize)...)               // session id (V3+)
	f.Add(valid)
	f.Add(valid[:12])                                                               // pre-V3: pid + broker info only
	f.Add([]byte{0xff, 0xff, 0xff, 0xfe, 0xff, 0xff, 0xff, 0xf0, 'e', 'r', 'r', 0}) // error status
	for _, p := range goldenResponses(f) {
		f.Add(p)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		rep, err := ParseConnectReply(data)
		if err != nil {
			return
		}
		if rep == nil {
			t.Fatal("nil reply without error")
		}
		if v := int(rep.Version); v < 0 || v > protoVerMask {
			t.Fatalf("parsed protocol version %d out of range", v)
		}
	})
}
