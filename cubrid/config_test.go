package cubrid

import (
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestParseDSN(t *testing.T) {
	cfg, err := ParseDSN("cubrid://dba:secret@db1.example.com:33114/testdb?fetch_size=500&connect_timeout=5s")
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		Host: "db1.example.com", Port: 33114, Database: "testdb",
		User: "dba", Password: "secret",
		FetchSize: 500, ConnectTimeout: 5 * time.Second,
	}
	// AltHosts makes Config non-comparable, hence DeepEqual.
	if !reflect.DeepEqual(*cfg, want) {
		t.Fatalf("got %+v\nwant %+v", *cfg, want)
	}
}

func TestParseDSNDefaults(t *testing.T) {
	cfg, err := ParseDSN("cubrid://localhost/demodb")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 33000 || cfg.User != "public" || cfg.FetchSize != 100 {
		t.Fatalf("defaults wrong: %+v", cfg)
	}
}

func TestParseDSNErrors(t *testing.T) {
	for _, dsn := range []string{
		"mysql://h/db",          // wrong scheme
		"cubrid://host",         // no database
		"cubrid:///db",          // no host
		"cubrid://h/db?bogus=1", // unknown parameter
		"cubrid://h/db?ssl=banana",
		"cubrid://h/db?ssl_verify=2maybe",
		"cubrid://h/db?altHosts=",          // empty entry
		"cubrid://h/db?altHosts=h2:,h3",    // empty port
		"cubrid://h/db?altHosts=h2:banana", // non-numeric port
		"cubrid://h/db?altHosts=h2:1:2",    // too many colons
		"cubrid://h/db?altHosts=:33000",    // empty host
		"cubrid://h/db?altHosts=h2,",       // trailing empty entry
		"cubrid://h/db?altHosts=[]",        // empty bracketed host
		"cubrid://h/db?altHosts=[::1",      // unclosed bracket
		"cubrid://h/db?altHosts=[h]x",      // junk after bracket, no port
		"cubrid://h/db?althosts=h2",        // case-sensitive: exactly altHosts
	} {
		if _, err := ParseDSN(dsn); err == nil {
			t.Errorf("ParseDSN(%q): want error", dsn)
		}
	}
}

// TestParseDSNAltHosts: the JDBC ecosystem's altHosts parameter (kept
// case-sensitive for familiarity) lists standby brokers tried in order
// when the primary is unreachable. Entries normalize to host:port with
// the port defaulting to 33000.
func TestParseDSNAltHosts(t *testing.T) {
	cfg, err := ParseDSN("cubrid://dba@h1:33001/db?altHosts=h2:33002,h3")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"h2:33002", "h3:33000"}
	if !reflect.DeepEqual(cfg.AltHosts, want) {
		t.Fatalf("AltHosts = %v, want %v", cfg.AltHosts, want)
	}
	// Single alt, IPv6 literal with explicit port.
	cfg, err = ParseDSN("cubrid://dba@h1/db?altHosts=[::1]:33099")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.AltHosts, []string{"[::1]:33099"}) {
		t.Fatalf("AltHosts = %v", cfg.AltHosts)
	}
	// Bare bracketed IPv6 literal, no port: the brackets must be shed
	// before normalization so net.JoinHostPort re-brackets exactly once.
	// (Regression: this used to store the undialable "[[::1]]:33000".)
	cfg, err = ParseDSN("cubrid://dba@h1/db?altHosts=[::1]")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.AltHosts, []string{"[::1]:33000"}) {
		t.Fatalf("AltHosts = %v, want [[::1]:33000]", cfg.AltHosts)
	}
	// Absent parameter leaves AltHosts nil (single-host connect path).
	cfg, err = ParseDSN("cubrid://dba@h1/db")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AltHosts != nil {
		t.Fatalf("AltHosts = %v, want nil", cfg.AltHosts)
	}
}

// TestSplitAltHostRoundTrip: every form splitAltHost accepts must survive
// normalization. ParseDSN stores net.JoinHostPort(host, port) and the
// Connect sweep feeds that stored string back through splitAltHost, so
// parse -> join -> parse must be a fixed point, otherwise the dial address
// silently corrupts (the "[::1]" -> "[[::1]]:33000" double-bracket bug).
// Directly-built Configs hit the same path, so this also pins the forms
// the Config.AltHosts doc promises.
func TestSplitAltHostRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		in   string
		host string
		port int
	}{
		{"h2", "h2", DefaultPort},
		{"h2:33002", "h2", 33002},
		{"10.0.0.7", "10.0.0.7", DefaultPort},
		{"[::1]", "::1", DefaultPort},
		{"[::1]:33099", "::1", 33099},
		{"[2001:db8::7]", "2001:db8::7", DefaultPort},
	} {
		host, port, err := splitAltHost(tc.in)
		if err != nil {
			t.Errorf("splitAltHost(%q): %v", tc.in, err)
			continue
		}
		if host != tc.host || port != tc.port {
			t.Errorf("splitAltHost(%q) = (%q, %d), want (%q, %d)",
				tc.in, host, port, tc.host, tc.port)
			continue
		}
		stored := net.JoinHostPort(host, strconv.Itoa(port))
		h2, p2, err := splitAltHost(stored)
		if err != nil {
			t.Errorf("re-parse of stored %q (from %q): %v", stored, tc.in, err)
			continue
		}
		if h2 != host || p2 != port {
			t.Errorf("stored %q re-parses to (%q, %d), want (%q, %d), not a fixed point",
				stored, h2, p2, host, port)
		}
	}
}

func TestParseDSNSSL(t *testing.T) {
	cfg, err := ParseDSN("cubrid://dba@h/db?ssl=true&ssl_verify=true")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.SSL || !cfg.SSLVerify {
		t.Fatalf("ssl flags not set: %+v", cfg)
	}
	// ssl=false and the param's absence are equivalent; ssl_verify
	// defaults to false (accept-all, the JDBC ecosystem behavior).
	cfg, err = ParseDSN("cubrid://dba@h/db?ssl=false")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SSL || cfg.SSLVerify {
		t.Fatalf("ssl flags should default off: %+v", cfg)
	}
}
