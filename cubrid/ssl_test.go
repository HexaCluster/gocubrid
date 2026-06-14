package cubrid

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"testing"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
	"github.com/hexacluster/gocubrid/internal/prototest"
)

// TestConnectTLS: with ssl=true the client sends the CUBRS greeting in
// PLAINTEXT, and after the broker's int32 0 reply upgrades the same socket
// to TLS before dbInfo. The fake broker reads the greeting pre-TLS and
// serves everything after the reply through tls.Server, so a passing
// Connect+Ping proves both halves of that sequence.
func TestConnectTLS(t *testing.T) {
	fb := prototest.Start(t)
	fb.EnableTLS()
	cfg, err := ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb?ssl=true")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if v := conn.ProtocolVersion(); v != 12 {
		t.Fatalf("protocol version = %d, want 12", v)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if g := fb.Greeting(); !bytes.Equal(g, protocol.Greeting(true)) {
		t.Fatalf("broker saw greeting % X, want % X", g, protocol.Greeting(true))
	}
}

// TestConnectTLSVerify: ssl_verify=true must actually verify the chain,
// the fake broker's self-signed certificate has no trusted root, so the
// handshake must fail with an x509 verification error.
func TestConnectTLSVerify(t *testing.T) {
	fb := prototest.Start(t)
	fb.EnableTLS()
	cfg, err := ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb?ssl=true&ssl_verify=true")
	if err != nil {
		t.Fatal(err)
	}
	_, err = Connect(context.Background(), cfg)
	var unknownAuth x509.UnknownAuthorityError
	if !errors.As(err, &unknownAuth) {
		t.Fatalf("want x509.UnknownAuthorityError for a self-signed broker cert, got %v", err)
	}
}

// TestConnectSSLRejected: a broker without SSL answers the CUBRS greeting
// with CAS_ER_SSL_TYPE_NOT_ALLOWED (-10103); Connect must surface it as a
// *cubrid.Error, not attempt a TLS handshake against a plaintext socket.
func TestConnectSSLRejected(t *testing.T) {
	fb := prototest.Start(t) // no EnableTLS: a plain broker
	cfg, err := ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb?ssl=true")
	if err != nil {
		t.Fatal(err)
	}
	_, err = Connect(context.Background(), cfg)
	var cerr *Error
	if !errors.As(err, &cerr) {
		t.Fatalf("want *cubrid.Error, got %v", err)
	}
	if cerr.Code != protocol.CASErrSSLTypeNotAllowed {
		t.Fatalf("code = %d, want %d", cerr.Code, protocol.CASErrSSLTypeNotAllowed)
	}
}

// TestConnectPlainBrokerStillWorks: ssl=false against the TLS-enabled fake
// broker keeps the plaintext path (the broker only upgrades when the
// client greets with CUBRS).
func TestConnectPlainGreetingUnaffectedByTLSBroker(t *testing.T) {
	fb := prototest.Start(t)
	fb.EnableTLS()
	cfg, err := ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb")
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
	if g := fb.Greeting(); !bytes.Equal(g, protocol.Greeting(false)) {
		t.Fatalf("broker saw greeting % X, want % X", g, protocol.Greeting(false))
	}
}
