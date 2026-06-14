package prototest

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
)

func TestFakeBrokerHandshakeAndRoundTrip(t *testing.T) {
	fb := Start(t)
	c, err := net.Dial("tcp", fb.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.Write(make([]byte, 10)) // greeting (content not validated by fake)
	var port int32
	binary.Read(c, binary.BigEndian, &port)
	if port != 0 {
		t.Fatalf("greet reply = %d, want 0", port)
	}
	c.Write(make([]byte, 628)) // dbInfo
	hdr := make([]byte, 8)
	io.ReadFull(c, hdr)
	payload := make([]byte, binary.BigEndian.Uint32(hdr[:4]))
	io.ReadFull(c, payload)
	if len(payload) != 4+8+4+20 {
		t.Fatalf("connect reply payload = %d bytes", len(payload))
	}

	// queued response for CHECK_CAS (32)
	rw := &RespWriter{}
	rw.I32(0)
	fb.Queue(32, rw.Bytes())
	req := []byte{0, 0, 0, 1, 0, 0, 0, 0, 32} // header + fn byte
	c.Write(req)
	io.ReadFull(c, hdr)
	resp := make([]byte, binary.BigEndian.Uint32(hdr[:4]))
	io.ReadFull(c, resp)
	if len(resp) != 4 {
		t.Fatalf("resp = % X", resp)
	}
	if got := fb.Requests(); len(got) != 1 || got[0][0] != 32 {
		t.Fatalf("recorded requests: %v", got)
	}
}
