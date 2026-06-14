//go:build soak

// The soak ring (-tags soak) stresses the database/sql adapter against a
// live broker: a 60-second pooled mixed-traffic storm under -race, a
// 100k-row streaming fetch with bounded memory, broker-restart recovery,
// a query-timeout storm, and an opt-in multi-MiB LOB round-trip. It is
// env-gated like the integration suite and targets 11.4 by default:
// CUBRID_TEST_DSN_114 (or CUBRID_SOAK_DSN to aim elsewhere); skipped when
// neither is set. CUBRID_SOAK_SECONDS overrides the storm duration.
package cubridsql_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cubridsql "github.com/hexacluster/gocubrid"
	"github.com/hexacluster/gocubrid/cubrid"
)

// Helper names are soak-prefixed so a combined -tags "integration soak"
// build does not collide with driver_integration_test.go's copies.

var soakSeq atomic.Int64

func soakUniq(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), soakSeq.Add(1))
}

// soakConfig returns the soak-ring target config, skipping the test when
// no DSN is configured.
func soakConfig(t *testing.T) *cubrid.Config {
	t.Helper()
	dsn := os.Getenv("CUBRID_SOAK_DSN")
	if dsn == "" {
		dsn = os.Getenv("CUBRID_TEST_DSN_114")
	}
	if dsn == "" {
		t.Skip("soak ring needs CUBRID_SOAK_DSN or CUBRID_TEST_DSN_114")
	}
	cfg, err := cubrid.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("soak DSN: %v", err)
	}
	return cfg
}

func soakSeconds() time.Duration {
	if s := os.Getenv("CUBRID_SOAK_SECONDS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 60 * time.Second
}

func soakOpenDB(t *testing.T, cfg *cubrid.Config) *sql.DB {
	t.Helper()
	db := sql.OpenDB(cubridsql.NewConnector(cfg))
	t.Cleanup(func() { db.Close() })
	return db
}

func soakDropTable(t *testing.T, cfg *cubrid.Config, tbl string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		db := sql.OpenDB(cubridsql.NewConnector(cfg))
		defer db.Close()
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			t.Logf("cleanup %s: drop: %v", tbl, err)
		}
	})
}

// isLockContention reports the documented-tolerable storm errors: CUBRID
// kills one victim of a deadlock with ER_LK_UNILATERALLY_ABORTED (-72),
// and lock waits expire with the ER_LK_*_TIMEOUT family (-73..-76).
// Everything else is a soak failure.
func isLockContention(err error) bool {
	var ce *cubrid.Error
	if !errors.As(err, &ce) {
		return false
	}
	switch ce.Code {
	case -72, -73, -74, -75, -76:
		return true
	}
	return false
}

// TestSoakPoolStorm runs 64 goroutines of mixed SELECT/INSERT/transaction
// traffic over a 16-conn pool for CUBRID_SOAK_SECONDS (default 60s).
// Tolerance is zero errors except documented lock contention; committed
// row counts must reconcile exactly at the end.
func TestSoakPoolStorm(t *testing.T) {
	cfg := soakConfig(t)
	db := soakOpenDB(t, cfg)
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)

	setup, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tbl := soakUniq("go_soakstorm")
	soakDropTable(t, cfg, tbl)
	if _, err := db.ExecContext(setup,
		fmt.Sprintf("CREATE TABLE %s (g INT, i INT, s VARCHAR(64))", tbl)); err != nil {
		t.Fatal(err)
	}

	const goroutines = 64
	dur := soakSeconds()
	deadline := time.Now().Add(dur)
	var (
		committed   atomic.Int64 // rows that must be visible at the end
		ops         atomic.Int64
		lockSkips   atomic.Int64
		wg          sync.WaitGroup
		errCh       = make(chan error, goroutines)
		insertSQL   = fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?)", tbl)
		countMineQ  = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE g = ?", tbl)
		opTimeout   = 30 * time.Second
		reportFatal = func(err error) {
			select {
			case errCh <- err:
			default:
			}
		}
	)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
				var err error
				switch i % 3 {
				case 0: // autocommit INSERT
					_, err = db.ExecContext(ctx, insertSQL, int64(g), int64(i), fmt.Sprintf("storm-%d-%d", g, i))
					if err == nil {
						committed.Add(1)
					}
				case 1: // SELECT
					var n int64
					err = db.QueryRowContext(ctx, countMineQ, int64(g)).Scan(&n)
				case 2: // transaction: INSERT + SELECT, commit (rollback every 4th)
					var tx *sql.Tx
					tx, err = db.BeginTx(ctx, nil)
					if err == nil {
						_, err = tx.ExecContext(ctx, insertSQL, int64(g), int64(i), "tx")
						if err == nil {
							var n int64
							err = tx.QueryRowContext(ctx, countMineQ, int64(g)).Scan(&n)
						}
						if err != nil {
							tx.Rollback()
						} else if i%12 == 2 { // every 4th tx op rolls back
							err = tx.Rollback()
						} else if err = tx.Commit(); err == nil {
							committed.Add(1)
						}
					}
				}
				cancel()
				if err != nil {
					if isLockContention(err) {
						lockSkips.Add(1)
						continue
					}
					reportFatal(fmt.Errorf("goroutine %d op %d: %w", g, i, err))
					return
				}
				ops.Add(1)
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if ops.Load() == 0 {
		t.Fatal("storm completed zero operations")
	}

	verify, vcancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer vcancel()
	var rows int64
	if err := db.QueryRowContext(verify, fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl)).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != committed.Load() {
		t.Fatalf("table holds %d rows, want exactly %d committed", rows, committed.Load())
	}
	t.Logf("storm: %d ops / %v across %d goroutines (%d committed rows, %d lock-contention retries, pool stats %+v)",
		ops.Load(), dur, goroutines, committed.Load(), lockSkips.Load(), db.Stats())
}

// TestSoakBigFetch seeds >=100k rows server-side via INSERT ... SELECT
// doubling, then streams them through database/sql Rows with a small
// fetch_size, asserting exact count, content checksum, and bounded heap
// growth while streaming.
func TestSoakBigFetch(t *testing.T) {
	cfg := soakConfig(t)
	target := 100_000
	if s := os.Getenv("CUBRID_SOAK_FETCH_ROWS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			target = n
		}
	}

	db := soakOpenDB(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	tbl := soakUniq("go_soakfetch")
	soakDropTable(t, cfg, tbl)
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (i INT)", tbl)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", tbl)); err != nil {
		t.Fatal(err)
	}
	n := 1
	for n < target {
		// Values stay the distinct set 1..2n: each doubling shifts the
		// existing rows by the current count, entirely server-side.
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s SELECT i + ? FROM %s", tbl, tbl), int64(n)); err != nil {
			t.Fatalf("doubling at %d rows: %v", n, err)
		}
		n *= 2
	}
	t.Logf("seeded %d rows", n)

	// Stream through a small fetch window so memory stays bounded by the
	// window, not the result size.
	small := *cfg
	small.FetchSize = 512
	fdb := sql.OpenDB(cubridsql.NewConnector(&small))
	defer fdb.Close()

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	start := time.Now()
	rows, err := fdb.QueryContext(ctx, fmt.Sprintf("SELECT i FROM %s", tbl))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var (
		count   int
		sum     int64
		maxHeap uint64
	)
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("row %d: %v", count, err)
		}
		sum += v
		count++
		if count%8192 == 0 {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > maxHeap {
				maxHeap = ms.HeapAlloc
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("streamed %d rows, want %d", count, n)
	}
	if want := int64(n) * int64(n+1) / 2; sum != want {
		t.Fatalf("content checksum: sum = %d, want %d", sum, want)
	}
	// The fetch window is ~512 ints; tens of MiB of growth would mean the
	// driver is buffering the whole result. The bound is deliberately
	// generous to stay GC-schedule-proof.
	const heapBudget = 64 << 20
	if maxHeap > base.HeapAlloc && maxHeap-base.HeapAlloc > heapBudget {
		t.Fatalf("heap grew %d MiB while streaming (budget %d MiB)",
			(maxHeap-base.HeapAlloc)>>20, heapBudget>>20)
	}
	t.Logf("streamed %d rows in %v (fetch_size %d, heap growth %d KiB)",
		count, time.Since(start), small.FetchSize, int64(maxHeap-base.HeapAlloc)/1024)
}

// TestSoakBrokerRestartRecovery restarts the live broker mid-storm and
// asserts the pool sheds the poisoned connections (driver.ErrBadConn
// path) and serves queries again within 30 seconds. Needs two env vars:
// CUBRID_TEST_DSN_114 (the broker under test) and CUBRID_SOAK_RESTART_CMD
// (a shell command that restarts that broker, typically an ssh or
// container-exec wrapper around "cubrid broker restart"); skips when
// either is unset.
func TestSoakBrokerRestartRecovery(t *testing.T) {
	dsn := os.Getenv("CUBRID_TEST_DSN_114")
	if dsn == "" {
		t.Skip("broker-restart soak needs CUBRID_TEST_DSN_114")
	}
	restartCmd := os.Getenv("CUBRID_SOAK_RESTART_CMD")
	if restartCmd == "" {
		t.Skip("broker-restart soak needs CUBRID_SOAK_RESTART_CMD (shell command that restarts the broker)")
	}
	cfg, err := cubrid.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}

	db := soakOpenDB(t, cfg)
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)

	// Background storm: cheap SELECTs that tolerate (and count) errors
	// while the broker bounces.
	var (
		stop       atomic.Bool
		stormErrs  atomic.Int64
		stormOps   atomic.Int64
		wg         sync.WaitGroup
		goroutines = 16
	)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				var one int64
				if err := db.QueryRowContext(ctx, "SELECT 1 FROM db_root").Scan(&one); err != nil {
					stormErrs.Add(1)
				} else {
					stormOps.Add(1)
				}
				cancel()
			}
		}()
	}
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()

	// Let the storm saturate the pool before pulling the rug.
	warmDeadline := time.Now().Add(60 * time.Second)
	for stormOps.Load() < int64(goroutines) && time.Now().Before(warmDeadline) {
		time.Sleep(250 * time.Millisecond)
	}
	if stormOps.Load() == 0 {
		t.Fatal("storm never went green before the restart")
	}

	restartCtx, restartCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer restartCancel()
	out, err := exec.CommandContext(restartCtx, "sh", "-c", restartCmd).CombinedOutput()
	if err != nil {
		t.Fatalf("broker restart command failed: %v: %s", err, out)
	}
	t.Logf("broker restarted (storm errors so far: %d)", stormErrs.Load())

	// Recovery: within 30s of the restart completing, the pool must serve
	// 5 consecutive successes (shedding every poisoned conn on the way).
	recoverBy := time.Now().Add(30 * time.Second)
	consecutive := 0
	for consecutive < 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var one int64
		err := db.QueryRowContext(ctx, "SELECT 1 FROM db_root").Scan(&one)
		cancel()
		if err != nil {
			consecutive = 0
			if time.Now().After(recoverBy) {
				t.Fatalf("pool did not recover within 30s of broker restart: %v", err)
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}
		consecutive++
	}
	stop.Store(true)
	wg.Wait()
	t.Logf("recovered: %d green ops, %d errors during the bounce, pool stats %+v",
		stormOps.Load(), stormErrs.Load(), db.Stats())

	// And the pool stays green: a fresh burst must be error-free.
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		var one int64
		err := db.QueryRowContext(ctx, "SELECT 1 FROM db_root").Scan(&one)
		cancel()
		if err != nil {
			t.Fatalf("post-recovery query %d failed: %v", i, err)
		}
	}
}

// TestSoakQueryTimeoutStorm fires 100 sequential 1ms-deadline queries,
// each either dies before acquiring a connection or poisons one
// mid-protocol, then requires the pool to serve normal traffic.
func TestSoakQueryTimeoutStorm(t *testing.T) {
	cfg := soakConfig(t)
	db := soakOpenDB(t, cfg)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	// Prime the pool so some storm queries hit live connections (and get
	// interrupted mid-protocol) rather than all dying at dial time.
	warm, wcancel := context.WithTimeout(context.Background(), 30*time.Second)
	var one int64
	if err := db.QueryRowContext(warm, "SELECT 1 FROM db_root").Scan(&one); err != nil {
		wcancel()
		t.Fatalf("warmup: %v", err)
	}
	wcancel()

	expired := 0
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		var v int64
		if err := db.QueryRowContext(ctx, "SELECT 1 FROM db_root").Scan(&v); err != nil {
			expired++
		}
		cancel()
	}
	t.Logf("timeout storm: %d/100 queries died of the 1ms deadline, pool stats %+v", expired, db.Stats())

	// The pool must still be fully functional.
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		var v int64
		err := db.QueryRowContext(ctx, "SELECT 1 FROM db_root").Scan(&v)
		cancel()
		if err != nil {
			t.Fatalf("post-storm query %d: %v", i, err)
		}
		if v != 1 {
			t.Fatalf("post-storm query %d = %d", i, v)
		}
	}
}

// TestSoakBigLob is the opt-in multi-MiB LOB round-trip through the
// native API (the adapter caps eager LOB materialization at 1 MiB by
// design). Off by default, the dev link to the lab moves 128 KiB LOB
// chunks at ~5-25 KiB/s, set CUBRID_TEST_LOB_BIG_MIB=<n> and run it
// from inside the lab where the broker is LAN-local.
func TestSoakBigLob(t *testing.T) {
	mib, _ := strconv.Atoi(os.Getenv("CUBRID_TEST_LOB_BIG_MIB"))
	if mib <= 0 {
		t.Skip("CUBRID_TEST_LOB_BIG_MIB unset; the multi-MiB LOB soak is a documented in-lab job")
	}
	cfg := soakConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	conn, err := cubrid.Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := soakUniq("go_soaklob")
	soakDropTable(t, cfg, tbl)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, b BLOB)", tbl)); err != nil {
		t.Fatal(err)
	}
	conn.SetAutoCommit(false)

	size := mib << 20
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i>>8 ^ i)
	}
	blob, err := conn.NewBlob(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := blob.Append(ctx, data); err != nil || n != size {
		t.Fatalf("Append = %d, %v (want %d)", n, err, size)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, ?)", tbl), blob); err != nil {
		t.Fatal(err)
	}
	if err := conn.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	stmt, err := conn.Prepare(ctx, fmt.Sprintf("SELECT b FROM %s WHERE id = 1", tbl))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(context.Background())
	rs, err := stmt.Query(ctx)
	if err != nil {
		t.Fatal(err)
	}
	row, err := rs.Next()
	if err != nil {
		t.Fatal(err)
	}
	lob, ok := row[0].(*cubrid.Lob)
	if !ok {
		t.Fatalf("column = %#v, want *cubrid.Lob", row[0])
	}
	if lob.Size() != int64(size) {
		t.Fatalf("result LOB size = %d, want %d", lob.Size(), size)
	}
	got, err := lob.ReadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("%d MiB LOB round-trip mismatch (%d bytes back)", mib, len(got))
	}
	if err := conn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
