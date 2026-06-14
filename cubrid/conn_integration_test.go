//go:build integration

package cubrid_test

import (
	"context"
	"net/url"
	"os"
	"sort"
	"testing"
	"time"

	cubrid "github.com/hexacluster/gocubrid/cubrid"
)

// dsnConfig resolves a single version's DSN. The matrix tests use
// forEachDSN instead; this remains for single-version flows (capture).
func dsnConfig(t *testing.T, key string) *cubrid.Config {
	t.Helper()
	dsn := os.Getenv("CUBRID_TEST_DSN_" + key)
	if dsn == "" {
		t.Skipf("CUBRID_TEST_DSN_%s not set", key)
	}
	cfg, err := cubrid.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	sweepStaleTables(t, key, cfg)
	return cfg
}

func TestIntegrationConnectPing(t *testing.T) {
	type verProto struct {
		major, proto int
	}
	var seen []verProto
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, c caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		v := conn.ProtocolVersion()
		if v <= 0 {
			t.Errorf("protocol version = %d, want > 0", v)
		}
		seen = append(seen, verProto{c.major, v})
		if err := conn.Ping(ctx); err != nil {
			t.Fatal(err)
		}
	})
	// The negotiated protocol version must be monotone with the server
	// major across the matrix (a 9.x broker never speaks a newer protocol
	// than an 11.x broker). Subtests run sequentially, so `seen` is safe.
	sort.SliceStable(seen, func(i, j int) bool { return seen[i].major < seen[j].major })
	for i := 1; i < len(seen); i++ {
		if seen[i].major > seen[i-1].major && seen[i].proto < seen[i-1].proto {
			t.Errorf("protocol version not monotone with server major: %+v", seen)
		}
	}
}

// TestIntegrationAltHostsFailover (suite-level smoke, 11.4 only): a DSN
// whose primary is a black hole on the lab container network
// (192.0.2.1:33000, RFC 5737 documentation address, SYNs die silently) and whose
// altHosts entry is the real broker must connect within the per-attempt
// connect_timeout instead of hanging on the dead primary.
func TestIntegrationAltHostsFailover(t *testing.T) {
	dsnConfig(t, "114") // skip/sweep bookkeeping; cfg itself comes from the raw DSN below
	u, err := url.Parse(os.Getenv("CUBRID_TEST_DSN_114"))
	if err != nil {
		t.Fatal(err)
	}
	real := u.Host // host:port of the live 11.4 broker
	u.Host = "192.0.2.1:33000"
	q := u.Query()
	q.Set("altHosts", real)
	q.Set("connect_timeout", "3s")
	u.RawQuery = q.Encode()
	cfg, err := cubrid.ParseDSN(u.String())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	conn, err := cubrid.Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("failover connect: %v", err)
	}
	defer conn.Close()
	elapsed := time.Since(start)
	// The black-hole attempt may burn up to its 3s budget; the alt attempt
	// then dials the live broker. Anything near the outer 30s deadline
	// means the per-attempt timeout did not bound the dead primary.
	if elapsed > 10*time.Second {
		t.Errorf("failover took %v, want comfortably under 10s (3s per-attempt timeout)", elapsed)
	}
	t.Logf("failover connect in %v (primary black-holed at 192.0.2.1:33000, alt %s)", elapsed, real)
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping after failover: %v", err)
	}
	if _, err := conn.ServerVersion(ctx); err != nil {
		t.Fatalf("server version after failover: %v", err)
	}
}

func TestIntegrationServerVersion(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, c caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		v, err := conn.ServerVersion(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if v == "" {
			t.Fatal("empty server version")
		}
		// ServerVersion must be stable across connections: the caps the
		// iterator derived must match a fresh read.
		if got := capsOf(v); got != c {
			t.Fatalf("caps of %q = %+v, want %+v from iterator", v, got, c)
		}
		// The matrix floor is 9.3; every release in it has ENUM.
		if c.major < 9 {
			t.Fatalf("server major = %d (version %q), want >= 9", c.major, v)
		}
		if !c.hasENUM() {
			t.Fatalf("caps of %q = %+v, want ENUM on every matrix version", v, c)
		}
		// Capability gates are ordered by release: JSON (10.2) implies TZ (10.0).
		if c.hasJSON() && !c.hasTZ() {
			t.Fatalf("caps of %q = %+v: JSON implies TZ", v, c)
		}
	})
}
