package protocol

import (
	"bytes"
	"testing"
)

func TestWriteRequestFrame(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteRequest(&buf, []byte{0xAA, 0xBB}, [4]byte{0, 0, 0, 1}); err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 0, 0, 2, 0, 0, 0, 1, 0xAA, 0xBB}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("got % X want % X", buf.Bytes(), want)
	}
}

func TestReadResponseFrame(t *testing.T) {
	in := bytes.NewReader([]byte{0, 0, 0, 3, 1, 0, 0, 0x01, 9, 8, 7})
	payload, casInfo, err := ReadResponse(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, []byte{9, 8, 7}) {
		t.Fatalf("payload % X", payload)
	}
	if casInfo != [4]byte{1, 0, 0, 0x01} {
		t.Fatalf("casInfo % X", casInfo)
	}
}

func TestReadResponseRejectsHugePayload(t *testing.T) {
	in := bytes.NewReader([]byte{0x7F, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0})
	if _, _, err := ReadResponse(in); err == nil {
		t.Fatal("want error for oversized payload")
	}
}
