//go:build integration

package cubrid_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	cubrid "github.com/hexacluster/gocubrid/cubrid"
)

// sslCfg parses the DSN of an SSL=ON broker.
// These tests gate on CUBRID_TEST_DSN_SSL, a ssl=true DSN pointing at
// a broker section configured with SSL=ON.
func sslCfg(t *testing.T) *cubrid.Config {
	t.Helper()
	dsn := os.Getenv("CUBRID_TEST_DSN_SSL")
	if dsn == "" {
		t.Skip("CUBRID_TEST_DSN_SSL not set (needs a broker configured with SSL=ON)")
	}
	cfg, err := cubrid.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.SSL {
		t.Fatalf("CUBRID_TEST_DSN_SSL must carry ssl=true, got %q", dsn)
	}
	return cfg
}

// The full protocol runs over TLS: connect (plaintext CUBRS greeting, then
// TLS upgrade, then encrypted auth), and run a real round trip.
func TestIntegrationSSLConnectQuery(t *testing.T) {
	cfg := sslCfg(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := cubrid.Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ver, err := conn.ServerVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ver, "11.") {
		t.Fatalf("server version over TLS = %q, want 11.x", ver)
	}
	stmt, err := conn.Prepare(ctx, "SELECT 40 + 2 FROM db_root")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	rows, err := stmt.Query(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	row, err := rows.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := row[0].(int32); !ok || got != 42 {
		t.Fatalf("SELECT 40+2 over TLS = %#v, want int32(42)", row[0])
	}
}

// Sanity: ssl=true against the plaintext broker (33000) must be refused by
// the broker itself, surfaced as a *cubrid.Error with the broker's
// rejection code, not as a TLS-layer failure.
func TestIntegrationSSLRejectedByPlainBroker(t *testing.T) {
	base := os.Getenv("CUBRID_TEST_DSN_114")
	if base == "" {
		t.Skip("CUBRID_TEST_DSN_114 not set")
	}
	if strings.Contains(base, "?") {
		t.Fatalf("CUBRID_TEST_DSN_114 already has params: %q", base)
	}
	cfg, err := cubrid.ParseDSN(base + "?ssl=true")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = cubrid.Connect(ctx, cfg)
	var cerr *cubrid.Error
	if !errors.As(err, &cerr) {
		t.Fatalf("want *cubrid.Error from plaintext broker, got %v", err)
	}
	if cerr.Code != -10103 { // CAS_ER_SSL_TYPE_NOT_ALLOWED
		t.Fatalf("rejection code = %d, want -10103 (CAS_ER_SSL_TYPE_NOT_ALLOWED)", cerr.Code)
	}
}
