package cubrid

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hexacluster/gocubrid/internal/prototest"
)

func fakeConn(t *testing.T) (*prototest.FakeBroker, *Conn) {
	t.Helper()
	fb := prototest.Start(t)
	cfg, err := ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return fb, conn
}

func TestConnectAgainstFakeBroker(t *testing.T) {
	_, conn := fakeConn(t)
	if v := conn.ProtocolVersion(); v != 12 {
		t.Fatalf("protocol version = %d, want 12", v)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestPingEmptyReplyIsSuccess: a healthy live CAS answers CHECK_CAS with an
// EMPTY payload (no status int), so Ping must treat that as success.
func TestPingEmptyReplyIsSuccess(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(32, []byte{}) // CHECK_CAS: empty payload, as live 11.4 sends
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("ping with empty reply payload: %v", err)
	}
}

func TestConnectCancelledContext(t *testing.T) {
	fb := prototest.Start(t)
	cfg, _ := ParseDSN("cubrid://dba@" + fb.Addr() + "/testdb")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Connect(ctx, cfg); err == nil {
		t.Fatal("want error from cancelled context")
	}
}

// TestConnectRedirectFailureClosesRedirectedSocket covers the CAS port
// redirect branch (greeting reply > 0): handshake replaces c.nc with a
// connection to the redirected port, so when the handshake subsequently
// fails Connect must close the *current* socket, not the original one,
// or the redirected fd leaks.
func TestConnectRedirectFailureClosesRedirectedSocket(t *testing.T) {
	// Redirected CAS listener: accept the auth block, answer with a
	// malformed (too short) connect reply, then probe for client close.
	redir, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer redir.Close()
	probe := make(chan error, 1)
	go func() {
		c, err := redir.Accept()
		if err != nil {
			probe <- err
			return
		}
		defer c.Close()
		if _, err := io.ReadFull(c, make([]byte, 628)); err != nil {
			probe <- err
			return
		}
		// Well-formed frame, but the payload is only a 4-byte status:
		// ParseConnectReply needs broker info after it and must fail.
		hdr := make([]byte, 8)
		binary.BigEndian.PutUint32(hdr[:4], 4)
		c.Write(hdr)
		c.Write([]byte{0, 0, 0, 1})
		// The client must close this socket once Connect errors out; a
		// clean close reads as EOF, a leak as a deadline timeout.
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err = c.Read(make([]byte, 1))
		probe <- err
	}()

	// Broker listener: answer the greeting with a redirect to redir.
	broker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	go func() {
		c, err := broker.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		if _, err := io.ReadFull(c, make([]byte, 10)); err != nil {
			return
		}
		port := redir.Addr().(*net.TCPAddr).Port
		binary.Write(c, binary.BigEndian, int32(port))
	}()

	cfg, err := ParseDSN("cubrid://dba@" + broker.Addr().String() + "/testdb")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Connect(context.Background(), cfg); err == nil {
		t.Fatal("want error from malformed connect reply")
	}
	if err := <-probe; !errors.Is(err, io.EOF) {
		t.Fatalf("redirected socket left open after Connect failure: server read err = %v, want EOF", err)
	}
}

func TestConnExecPreparesAndCloses(t *testing.T) {
	fb, conn := fakeConn(t)
	p := &prototest.RespWriter{}
	p.I32(40).I32(0).B(20).I32(0).B(0).I32(0) // INSERT, 0 params, 0 cols
	fb.Queue(2, p.Bytes())
	e := &prototest.RespWriter{}
	e.I32(3).B(0).I32(1).B(20).I32(3).Raw(make([]byte, 8)).I32(0).I32(0).B(0).I32(0)
	fb.Queue(3, e.Bytes())

	res, err := conn.Exec(context.Background(), "DELETE FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if res.AffectedRows != 3 {
		t.Fatalf("affected = %d", res.AffectedRows)
	}
	reqs := fb.Requests()
	last := reqs[len(reqs)-1]
	if last[0] != 6 {
		t.Fatalf("last fn = %d, want CLOSE_USTATEMENT(6)", last[0])
	}
}

// TestRoundTripExpiredContextWriteMapsCtxErr: with an already-expired
// context the guard sets a past socket deadline, so the WRITE fails before
// any bytes go out. roundTrip must surface ctx.Err() (DeadlineExceeded /
// Canceled) on that path too (not a wrapped os.ErrDeadlineExceeded) and
// must still poison the connection.
func TestRoundTripExpiredContextWriteMapsCtxErr(t *testing.T) {
	_, conn := fakeConn(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond) // ensure already expired

	_, err := conn.Prepare(ctx, "SELECT 1 FROM db_root")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	// transport state is undefined mid-frame: conn must be poisoned
	if err := conn.Ping(context.Background()); !errors.Is(err, ErrBadConn) {
		t.Fatalf("want ErrBadConn after expired-context write, got %v", err)
	}
}

func TestServerVersion(t *testing.T) {
	fb, conn := fakeConn(t)
	rw := &prototest.RespWriter{}
	rw.I32(0)
	rw.Raw([]byte("11.4.5.1866\x00"))
	fb.Queue(15, rw.Bytes()) // GET_DB_VERSION
	v, err := conn.ServerVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v != "11.4.5.1866" {
		t.Fatalf("version = %q", v)
	}
	// Request layout per JDBC UConnection.getDatabaseProductVersion:
	// [fn=15][int32 len=1][autocommit flag] (autocommit defaults to on).
	reqs := fb.Requests()
	last := reqs[len(reqs)-1]
	want := []byte{15, 0, 0, 0, 1, 1}
	if !bytes.Equal(last, want) {
		t.Fatalf("request = % x, want % x", last, want)
	}
}

// TestFrameCaptureHook: with CUBRID_FRAME_DIR set, every roundTrip dumps
// its request and response payloads as sequentially numbered .bin files
// (golden-frame capture). The handshake is not captured, only post-connect
// round trips.
func TestFrameCaptureHook(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CUBRID_FRAME_DIR", dir)
	_, conn := fakeConn(t)
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	req, err := os.ReadFile(filepath.Join(dir, "001-req.bin"))
	if err != nil {
		t.Fatalf("captured request: %v", err)
	}
	if !bytes.Equal(req, []byte{32}) { // CHECK_CAS payload is the bare fn byte
		t.Fatalf("captured request = % x, want [20]", req)
	}
	res, err := os.ReadFile(filepath.Join(dir, "002-res.bin"))
	if err != nil {
		t.Fatalf("captured response: %v", err)
	}
	if !bytes.Equal(res, []byte{0, 0, 0, 0}) { // fake broker's generic OK
		t.Fatalf("captured response = % x, want 4-byte OK status", res)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 2 {
		t.Fatalf("capture dir has %d files, want 2 (handshake must not be captured)", len(entries))
	}
}

// TestFrameCaptureDisabledByDefault: without CUBRID_FRAME_DIR no sink is
// attached and roundTrip writes nothing.
func TestFrameCaptureDisabledByDefault(t *testing.T) {
	_, conn := fakeConn(t)
	if conn.frames != nil {
		t.Fatal("frame sink attached without CUBRID_FRAME_DIR")
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestFakeBrokerSetProtocolVersion: replay pins each captured flow at the
// protocol version it was recorded under, so the fake broker must be able
// to advertise an arbitrary version in its handshake reply.
func TestFakeBrokerSetProtocolVersion(t *testing.T) {
	fb := prototest.Start(t)
	fb.SetProtocolVersion(11)
	cfg, err := ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if v := conn.ProtocolVersion(); v != 11 {
		t.Fatalf("protocol version = %d, want 11", v)
	}
}

func TestCloseSendsConClose(t *testing.T) {
	fb, conn := fakeConn(t)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	reqs := fb.Requests()
	if len(reqs) == 0 || reqs[len(reqs)-1][0] != 31 {
		t.Fatalf("last request should be CON_CLOSE, got %v", reqs)
	}
	if err := conn.Close(); err != nil {
		t.Fatal("second Close must be a no-op")
	}
}

// deadAddr returns an address that refuses connections: a listener is
// bound to grab a free port, then closed before the test dials it.
func deadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// TestConnectFailoverToAltHost: with the primary down, Connect must land
// on the altHosts broker and yield a fully usable connection.
func TestConnectFailoverToAltHost(t *testing.T) {
	fb := prototest.Start(t)
	cfg, err := ParseDSN("cubrid://dba:pw@" + deadAddr(t) + "/testdb?altHosts=" + fb.Addr())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failover connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("ping after failover: %v", err)
	}
	if v := conn.ProtocolVersion(); v != 12 {
		t.Fatalf("protocol version = %d, want 12", v)
	}
}

// TestConnectFailoverAllHostsDown: when every host fails, the aggregate
// error must mention each attempted address.
func TestConnectFailoverAllHostsDown(t *testing.T) {
	primary, alt := deadAddr(t), deadAddr(t)
	cfg, err := ParseDSN("cubrid://dba@" + primary + "/testdb?altHosts=" + alt)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Connect(context.Background(), cfg)
	if err == nil {
		t.Fatal("want error with all hosts down")
	}
	for _, addr := range []string{primary, alt} {
		if !strings.Contains(err.Error(), addr) {
			t.Errorf("aggregate error %q does not mention %s", err, addr)
		}
	}
}

// TestConnectFailoverPrimaryPreferred: with the primary healthy, the alt
// broker must never even see a greeting.
func TestConnectFailoverPrimaryPreferred(t *testing.T) {
	fb1 := prototest.Start(t)
	fb2 := prototest.Start(t)
	cfg, err := ParseDSN("cubrid://dba@" + fb1.Addr() + "/testdb?altHosts=" + fb2.Addr())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if g := fb2.Greeting(); len(g) != 0 {
		t.Fatalf("alt broker saw a greeting (% x) although the primary was up", g)
	}
}

// TestConnectFailoverPerAttemptTimeout: connect_timeout bounds EACH
// attempt, not the whole failover sweep. A primary that accepts but never
// answers the greeting must burn only its own attempt budget; the alt
// attempt then starts with a fresh timeout and succeeds. (Under a single
// shared timeout the alt attempt would inherit an expired context.)
func TestConnectFailoverPerAttemptTimeout(t *testing.T) {
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	go func() {
		for {
			c, err := silent.Accept()
			if err != nil {
				return
			}
			defer c.Close() // hold open, never reply
		}
	}()
	fb := prototest.Start(t)
	cfg, err := ParseDSN("cubrid://dba@" + silent.Addr().String() +
		"/testdb?connect_timeout=200ms&altHosts=" + fb.Addr())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	conn, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failover connect: %v", err)
	}
	defer conn.Close()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("failover took %v, want well under 2s (200ms attempt timeout)", elapsed)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestConnectFailoverCancelledContext: a dead outer context must stop the
// sweep instead of marching through every host.
func TestConnectFailoverCancelledContext(t *testing.T) {
	fb := prototest.Start(t)
	cfg, err := ParseDSN("cubrid://dba@" + deadAddr(t) + "/testdb?altHosts=" + fb.Addr())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Connect(ctx, cfg); err == nil {
		t.Fatal("want error from cancelled context")
	}
	if g := fb.Greeting(); len(g) != 0 {
		t.Fatal("alt broker contacted under a cancelled context")
	}
}

// TestValidReflectsConnState: Valid is the no-wire-op liveness check the
// database/sql adapter's driver.Validator relies on, true on a fresh
// conn, false once poisoned, false after Close.
func TestValidReflectsConnState(t *testing.T) {
	_, conn := fakeConn(t)
	if !conn.Valid() {
		t.Fatal("fresh conn reports Valid() == false")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)
	if _, err := conn.Prepare(ctx, "SELECT 1"); err == nil {
		t.Fatal("want error from expired context")
	}
	if conn.Valid() {
		t.Fatal("poisoned conn reports Valid() == true")
	}

	_, conn2 := fakeConn(t)
	if err := conn2.Close(); err != nil {
		t.Fatal(err)
	}
	if conn2.Valid() {
		t.Fatal("closed conn reports Valid() == true")
	}
}
