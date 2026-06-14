//go:build integration

package cubrid_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cubrid "github.com/hexacluster/gocubrid/cubrid"
)

var tblSeq atomic.Int64

// uniqName returns a table name unique to this test process, so
// back-to-back or concurrent suite runs never contend on DDL locks.
func uniqName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), tblSeq.Add(1))
}

// caps captures server-release-driven capabilities, parsed from the
// Conn.ServerVersion string. Distinct from the broker protocol version:
// these gate features that track CUBRID server releases.
type caps struct{ major, minor int }

func capsOf(version string) caps {
	var c caps
	fmt.Sscanf(version, "%d.%d", &c.major, &c.minor)
	return c
}

func (c caps) hasJSON() bool { return c.major > 10 || (c.major == 10 && c.minor >= 2) }
func (c caps) hasTZ() bool   { return c.major >= 10 }
func (c caps) hasENUM() bool { return c.major >= 9 }
func (c caps) hasMVCC() bool { return c.major >= 10 }

var matrixKeys = []string{"93", "101", "102", "110", "114"}

// forEachDSN runs fn as a subtest for every CUBRID_TEST_DSN_<key> set in
// the environment, providing the parsed config and server capabilities.
func forEachDSN(t *testing.T, fn func(t *testing.T, cfg *cubrid.Config, c caps)) {
	t.Helper()
	ran := false
	for _, key := range matrixKeys {
		dsn := os.Getenv("CUBRID_TEST_DSN_" + key)
		if dsn == "" {
			continue
		}
		ran = true
		cfg, err := cubrid.ParseDSN(dsn)
		if err != nil {
			t.Fatalf("DSN %s: %v", key, err)
		}
		t.Run("v"+key, func(t *testing.T) {
			sweepStaleTables(t, key, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			conn, err := cubrid.Connect(ctx, cfg)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			ver, err := conn.ServerVersion(ctx)
			conn.Close()
			if err != nil {
				t.Fatalf("server version: %v", err)
			}
			fn(t, cfg, capsOf(ver))
		})
	}
	if !ran {
		t.Skip("no CUBRID_TEST_DSN_* set")
	}
}

var sweepOnce sync.Map // DSN key -> *sync.Once

// sweepStaleTables reclaims go_* tables leaked by crashed earlier runs
// (SIGKILL, machine crash, or a `go test -timeout` watchdog panic, none of
// which run the test goroutine's defers). Unique per-run names mean the
// per-test DROP TABLE IF EXISTS can never match a leftover from another
// process, so the first test touching a server sweeps the whole prefix
// instead. Opportunistic: failures are logged, never fatal. Caveat: a
// concurrent suite run against the same database could lose just-created
// tables to this sweep; suites are expected to share a server
// back-to-back, not simultaneously, which is why the Makefile
// integration target passes -p 1 (the root-package suite hits the same
// DSNs from its own test binary).
func sweepStaleTables(t *testing.T, key string, cfg *cubrid.Config) {
	t.Helper()
	once, _ := sweepOnce.LoadOrStore(key, new(sync.Once))
	once.(*sync.Once).Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Logf("stale-table sweep (%s): connect: %v", key, err)
			return
		}
		defer conn.Close()
		stmt, err := conn.Prepare(ctx, "SELECT class_name FROM db_class"+
			" WHERE class_name LIKE 'go!_%' ESCAPE '!'"+
			" AND class_type = 'CLASS' AND is_system_class = 'NO'")
		if err != nil {
			t.Logf("stale-table sweep (%s): prepare: %v", key, err)
			return
		}
		defer stmt.Close(ctx)
		rows, err := stmt.Query(ctx)
		if err != nil {
			t.Logf("stale-table sweep (%s): query: %v", key, err)
			return
		}
		var stale []string
		for {
			row, err := rows.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Logf("stale-table sweep (%s): next: %v", key, err)
				return
			}
			name, ok := row[0].(string)
			if !ok {
				t.Logf("stale-table sweep (%s): class_name = %#v", key, row[0])
				return
			}
			stale = append(stale, name)
		}
		for _, name := range stale {
			if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+name); err != nil {
				t.Logf("stale-table sweep (%s): drop %s: %v", key, name, err)
				continue
			}
			t.Logf("stale-table sweep (%s): dropped leaked table %s", key, name)
		}
	})
}

// dropTableCleanup registers a cleanup that drops tbl on a fresh
// connection with its own deadline. The test's conn and ctx must not be
// used for cleanup: when a test dies of a deadline expiry its ctx is
// already spent and the conn is poisoned (ErrBadConn, see
// TestIntegrationContextCancel), so a deferred drop on them fails
// exactly when cleanup matters most. t.Cleanup runs after the test's
// defers, so the test conn is closed and its locks released by then.
func dropTableCleanup(t *testing.T, cfg *cubrid.Config, tbl string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Logf("cleanup %s: connect: %v", tbl, err)
			return
		}
		defer conn.Close()
		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			t.Logf("cleanup %s: drop: %v", tbl, err)
		}
	})
}
