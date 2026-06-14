package protocol

import (
	"bytes"
	"testing"
)

func TestGreeting(t *testing.T) {
	got := Greeting(false)
	want := []byte{'C', 'U', 'B', 'R', 'K', 3, 0x4C, 0xC0, 0, 0}
	if !bytes.Equal(got, want) {
		t.Fatalf("got % X want % X", got, want)
	}
	// SSL variant: only the magic differs (CUBRS), per JDBC driverInfossl.
	wantSSL := []byte{'C', 'U', 'B', 'R', 'S', 3, 0x4C, 0xC0, 0, 0}
	if ssl := Greeting(true); !bytes.Equal(ssl, wantSSL) {
		t.Fatalf("ssl greeting got % X want % X", ssl, wantSSL)
	}
}

func TestDBInfoLayout(t *testing.T) {
	b, err := DBInfo("demodb", "dba", "pw", "cubrid://h/demodb", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 628 {
		t.Fatalf("len = %d", len(b))
	}
	check := func(off int, s string) {
		t.Helper()
		if !bytes.HasPrefix(b[off:], []byte(s)) || b[off+len(s)] != 0 {
			t.Fatalf("offset %d: want %q NUL-padded", off, s)
		}
	}
	check(0, "demodb")
	check(32, "dba")
	check(64, "pw")
	check(96, "cubrid://h/demodb")
}

func TestDBInfoTooLong(t *testing.T) {
	if _, err := DBInfo(string(make([]byte, 33)), "u", "p", "", nil); err == nil {
		t.Fatal("want error for 33-byte dbname")
	}
}

func TestParseConnectReply(t *testing.T) {
	p := []byte{0, 0, 0x10, 0xE2}                    // pid 4322
	p = append(p, 0, 1, 1, 0, 0x4C, 0, 0, 0)         // broker info, V12
	p = append(p, 0, 0, 0, 7)                        // CAS id (V4+)
	p = append(p, bytes.Repeat([]byte{0xAB}, 20)...) // session id (V3+)
	rep, err := ParseConnectReply(p)
	if err != nil {
		t.Fatal(err)
	}
	if rep.CASPid != 4322 || rep.CASID != 7 || rep.Version != ProtoV12 {
		t.Fatalf("got %+v", rep)
	}
	if rep.SessionID[0] != 0xAB || rep.SessionID[19] != 0xAB {
		t.Fatalf("session id % X", rep.SessionID)
	}
}
