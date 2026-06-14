package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// packedHandle builds a packed LOB handle as the server sends it:
// [db_type int32][lob_size int64][locator_len int32][locator + NUL]
// (JDBC CUBRIDLobHandle.initLob).
func packedHandle(dbType int32, size int64, locator string) []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(dbType))
	b = binary.BigEndian.AppendUint64(b, uint64(size))
	b = binary.BigEndian.AppendUint32(b, uint32(len(locator)+1))
	b = append(b, locator...)
	return append(b, 0)
}

func TestParseLobHandle(t *testing.T) {
	raw := packedHandle(TypeBlob, 42, "ces_temp/ces_382/lob.0001")
	h, err := ParseLobHandle(raw)
	if err != nil {
		t.Fatal(err)
	}
	if h.DBType != TypeBlob || h.Size != 42 || h.Locator != "ces_temp/ces_382/lob.0001" {
		t.Fatalf("handle = %+v", h)
	}
	if !bytes.Equal(h.Packed(), raw) {
		t.Fatalf("packed = % X\nwant     % X", h.Packed(), raw)
	}
}

// ParseLobHandle must copy: mutating the input afterwards must not change
// the handle (result-set values alias the response buffer).
func TestParseLobHandleCopies(t *testing.T) {
	raw := packedHandle(TypeClob, 7, "loc")
	h, err := ParseLobHandle(raw)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = 0xFF
	if h.Packed()[0] == 0xFF {
		t.Fatal("handle aliases the input buffer")
	}
}

func TestSetSizeRewritesPackedBytes(t *testing.T) {
	h, err := ParseLobHandle(packedHandle(TypeBlob, 0, "loc"))
	if err != nil {
		t.Fatal(err)
	}
	h.SetSize(1 << 33) // crosses the int32 boundary: all 8 bytes matter
	if h.Size != 1<<33 {
		t.Fatalf("Size = %d", h.Size)
	}
	if !bytes.Equal(h.Packed(), packedHandle(TypeBlob, 1<<33, "loc")) {
		t.Fatalf("packed after SetSize = % X", h.Packed())
	}
}

func TestParseLobHandleRejectsMalformed(t *testing.T) {
	good := packedHandle(TypeBlob, 0, "loc")
	for name, raw := range map[string][]byte{
		"empty":             {},
		"truncated header":  good[:15],
		"truncated locator": good[:len(good)-1],
		"zero locator len":  packedHandle(TypeBlob, 0, "")[:16],
	} {
		if _, err := ParseLobHandle(raw); err == nil {
			t.Errorf("%s: ParseLobHandle accepted % X", name, raw)
		}
	}
}

func TestNewLobRequest(t *testing.T) {
	w := NewWriter()
	NewLobRequest(w, TypeClob)
	want := []byte{
		FnNewLOB,
		0, 0, 0, 4, 0, 0, 0, 24, // lob type CLOB
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

func TestWriteLobRequest(t *testing.T) {
	h, err := ParseLobHandle(packedHandle(TypeBlob, 3, "L"))
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter()
	WriteLobRequest(w, h, 3, []byte{0xAA, 0xBB})
	want := []byte{FnWriteLOB}
	want = append(want, 0, 0, 0, 18)                        // handle length: 16 + len("L")+1
	want = append(want, h.Packed()...)                      // packed handle
	want = append(want, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3) // offset int64
	want = append(want, 0, 0, 0, 2, 0xAA, 0xBB)             // data
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

func TestReadLobRequest(t *testing.T) {
	h, err := ParseLobHandle(packedHandle(TypeClob, 9, "L"))
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter()
	ReadLobRequest(w, h, 5, 1024)
	want := []byte{FnReadLOB}
	want = append(want, 0, 0, 0, 18)
	want = append(want, h.Packed()...)
	want = append(want, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 5) // offset int64
	want = append(want, 0, 0, 0, 4, 0, 0, 4, 0)             // length int32 = 1024
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}
