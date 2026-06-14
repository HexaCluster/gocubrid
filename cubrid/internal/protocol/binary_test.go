package protocol

import (
	"bytes"
	"testing"
)

func TestWriterArgs(t *testing.T) {
	w := NewWriter()
	w.RawByte(0x02)
	w.ArgInt(7)
	w.ArgByte(0x01)
	w.ArgString("ab")
	w.ArgBytes([]byte{0xDE, 0xAD})
	w.ArgNull()

	want := []byte{
		0x02,
		0, 0, 0, 4, 0, 0, 0, 7,
		0, 0, 0, 1, 0x01,
		0, 0, 0, 3, 'a', 'b', 0x00,
		0, 0, 0, 2, 0xDE, 0xAD,
		0, 0, 0, 0,
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

func TestWriterScalars(t *testing.T) {
	w := NewWriter()
	w.ArgShort(-2)
	w.ArgLong(1)
	w.ArgDouble(1.5)
	w.ArgFloat(2.5)

	want := []byte{
		0, 0, 0, 2, 0xFF, 0xFE,
		0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 8, 0x3F, 0xF8, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 4, 0x40, 0x20, 0, 0,
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

func TestReaderWalksPayload(t *testing.T) {
	payload := []byte{
		0, 0, 0, 42, // int32
		0xFF, 0xFB, // int16 -5
		7,                       // byte
		0, 0, 0, 3, 'h', 'i', 0, // string
		0, 0, 0, 0, 0, 0, 0, 9, // int64
		0xAB, 0xCD, // raw bytes
	}
	r := NewReader(payload)
	if v, err := r.Int32(); err != nil || v != 42 {
		t.Fatalf("Int32 = %d, %v", v, err)
	}
	if v, err := r.Int16(); err != nil || v != -5 {
		t.Fatalf("Int16 = %d, %v", v, err)
	}
	if v, err := r.Byte(); err != nil || v != 7 {
		t.Fatalf("Byte = %d, %v", v, err)
	}
	if v, err := r.String(); err != nil || v != "hi" {
		t.Fatalf("String = %q, %v", v, err)
	}
	if v, err := r.Int64(); err != nil || v != 9 {
		t.Fatalf("Int64 = %d, %v", v, err)
	}
	b, err := r.Bytes(2)
	if err != nil || !bytes.Equal(b, []byte{0xAB, 0xCD}) {
		t.Fatalf("Bytes = % X, %v", b, err)
	}
	if r.Remaining() != 0 {
		t.Fatalf("Remaining = %d, want 0", r.Remaining())
	}
}

func TestReaderShortBuffer(t *testing.T) {
	r := NewReader([]byte{0, 0})
	if _, err := r.Int32(); err == nil {
		t.Fatal("want error on short buffer")
	}
}
