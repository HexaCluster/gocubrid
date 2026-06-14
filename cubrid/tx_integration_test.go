//go:build integration

package cubrid_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	cubrid "github.com/hexacluster/gocubrid/cubrid"
)

// Confirmed live on 11.4: rollback discards the insert and commit persists
// it, so the autocommit signaling (prepare/execute bytes + casInfo flag)
// matches UConnection.endTransaction.
func TestIntegrationRollbackCommit(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_tx")
		// Cleanup runs on a fresh conn after this conn's deferred Close has
		// released any open-transaction locks; no SetAutoCommit reset needed.
		dropTableCleanup(t, cfg, tbl)
		conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
		if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i INT)", tbl)); err != nil {
			t.Fatal(err)
		}

		count := func() int32 {
			stmt, err := conn.Prepare(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl))
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close(ctx)
			rows, err := stmt.Query(ctx)
			if err != nil {
				t.Fatal(err)
			}
			row, err := rows.Next()
			if err != nil {
				t.Fatal(err)
			}
			// COUNT(*) comes back as INT or BIGINT depending on version
			// (BIGINT/int64 observed live on 11.4)
			switch v := row[0].(type) {
			case int32:
				return v
			case int64:
				return int32(v)
			default:
				t.Fatalf("count type %#v", row[0])
				return -1
			}
		}

		conn.SetAutoCommit(false)
		if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", tbl)); err != nil {
			t.Fatal(err)
		}
		if err := conn.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if n := count(); n != 0 {
			t.Fatalf("after rollback count = %d", n)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (2)", tbl)); err != nil {
			t.Fatal(err)
		}
		if err := conn.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if n := count(); n != 1 {
			t.Fatalf("after commit count = %d", n)
		}
	})
}

// Wire facts VERIFY-LIVE (Plan 4 Task 1): DB_PARAM_ISOLATION_LEVEL = 1
// round-trips SET_DB_PARAMETER/GET_DB_PARAMETER on every broker in the
// matrix. Codes 4 to 6 round-trip on every version: 10.0+ names them READ
// COMMITTED / REPEATABLE READ / SERIALIZABLE, 9.x reuses the same codes
// for its lock-based levels.
func TestIntegrationIsolationLevel(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		initial, err := conn.IsolationLevel(ctx)
		if err != nil {
			t.Fatalf("initial isolation level: %v", err)
		}
		t.Logf("server default isolation level: %v", initial)

		// Start with SERIALIZABLE so every following set provably changes
		// the value (the server default is READ COMMITTED on 10.0+).
		for _, lv := range []cubrid.IsolationLevel{
			cubrid.IsolationSerializable,
			cubrid.IsolationRepeatableRead,
			cubrid.IsolationReadCommitted,
		} {
			if err := conn.SetIsolationLevel(ctx, lv); err != nil {
				t.Fatalf("set %v: %v", lv, err)
			}
			got, err := conn.IsolationLevel(ctx)
			if err != nil {
				t.Fatalf("get after set %v: %v", lv, err)
			}
			if got != lv {
				t.Fatalf("round-trip %v: server reports %v", lv, got)
			}
		}
	})
}

// DB_PARAM_LOCK_TIMEOUT = 2, value in milliseconds: VERIFY-LIVE
// behaviorally. The server default is -1 (wait forever), so a conflicting
// UPDATE would hang until the test context expires; with a 1.5 s lock
// timeout set via the wire the server must abort the waiter with its own
// error well before that. (Reading the value back is not possible through
// the CAS: GET TRANSACTION LOCK TIMEOUT prepares as a zero-column
// statement whose execute response carries no value, confirmed live on
// 11.4 with CUBRID_WIRE_DEBUG.) If the broker interpreted the value as
// seconds, 1500 would outlast the context and the waiter would die of
// ctx.Err instead of a *cubrid.Error.
func TestIntegrationLockTimeout(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		holder, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer holder.Close()
		waiter, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer waiter.Close()

		tbl := uniqName("go_lktmo")
		dropTableCleanup(t, cfg, tbl)
		if _, err := holder.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i INT)", tbl)); err != nil {
			t.Fatal(err)
		}
		if _, err := holder.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", tbl)); err != nil {
			t.Fatal(err)
		}

		// The holder grabs and keeps an exclusive lock on the row.
		holder.SetAutoCommit(false)
		if _, err := holder.Exec(ctx, fmt.Sprintf("UPDATE %s SET i = 2", tbl)); err != nil {
			t.Fatal(err)
		}

		if err := waiter.SetLockTimeout(ctx, 1500*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		_, err = waiter.Exec(ctx, fmt.Sprintf("UPDATE %s SET i = 3", tbl))
		elapsed := time.Since(start)
		var ce *cubrid.Error
		if !errors.As(err, &ce) {
			t.Fatalf("conflicting UPDATE with 1.5s lock timeout: err = %v (%T), want *cubrid.Error", err, err)
		}
		t.Logf("lock timeout fired after %v: %v", elapsed, ce)
		if elapsed < time.Second {
			t.Fatalf("lock timeout fired after %v, want >= 1s of waiting", elapsed)
		}
		if err := holder.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

// One observable semantic difference between the levels, MVCC (10.0+)
// only: READ COMMITTED takes a fresh snapshot per statement, so a reader
// mid-transaction sees another connection's committed insert; REPEATABLE
// READ pins its snapshot at the first statement and hides the insert
// until the reader's own transaction ends. 9.x is lock-based (2PL) where
// the reader's locks would block the writer instead, semantics are
// skipped there, the level codes still round-trip (asserted above).
func TestIntegrationIsolationSemantics(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, c caps) {
		if !c.hasMVCC() {
			t.Skip("pre-10.0 server: lock-based isolation, MVCC visibility asserts do not apply")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		reader, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		writer, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()

		tbl := uniqName("go_iso")
		dropTableCleanup(t, cfg, tbl)
		if _, err := writer.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i INT)", tbl)); err != nil {
			t.Fatal(err)
		}

		count := func() int64 {
			stmt, err := reader.Prepare(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl))
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close(ctx)
			rows, err := stmt.Query(ctx)
			if err != nil {
				t.Fatal(err)
			}
			row, err := rows.Next()
			if err != nil {
				t.Fatal(err)
			}
			switch v := row[0].(type) {
			case int32:
				return int64(v)
			case int64:
				return v
			default:
				t.Fatalf("count type %#v", row[0])
				return -1
			}
		}

		// READ COMMITTED: the writer's committed insert becomes visible to
		// the reader's open transaction.
		if err := reader.SetIsolationLevel(ctx, cubrid.IsolationReadCommitted); err != nil {
			t.Fatal(err)
		}
		reader.SetAutoCommit(false)
		if n := count(); n != 0 {
			t.Fatalf("baseline count = %d, want 0", n)
		}
		if _, err := writer.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", tbl)); err != nil {
			t.Fatal(err) // writer is in autocommit: the insert commits
		}
		if n := count(); n != 1 {
			t.Fatalf("READ COMMITTED: committed insert invisible mid-transaction (count = %d)", n)
		}
		if err := reader.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		// REPEATABLE READ: the snapshot pinned by the first statement
		// hides the concurrent commit until the reader's transaction ends.
		if err := reader.SetIsolationLevel(ctx, cubrid.IsolationRepeatableRead); err != nil {
			t.Fatal(err)
		}
		if n := count(); n != 1 { // pins this transaction's snapshot
			t.Fatalf("snapshot baseline count = %d, want 1", n)
		}
		if _, err := writer.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (2)", tbl)); err != nil {
			t.Fatal(err)
		}
		if n := count(); n != 1 {
			t.Fatalf("REPEATABLE READ: snapshot saw a concurrent commit (count = %d)", n)
		}
		if err := reader.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if n := count(); n != 2 {
			t.Fatalf("count after transaction end = %d, want 2", n)
		}
	})
}

// Savepoints ride plain SQL (SAVEPOINT / ROLLBACK TO SAVEPOINT) through the
// regular prepare+execute path: insert A, savepoint, insert B, rollback to
// the savepoint, commit, only A persists.
func TestIntegrationSavepoint(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_sp")
		dropTableCleanup(t, cfg, tbl)
		conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
		if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i INT)", tbl)); err != nil {
			t.Fatal(err)
		}

		conn.SetAutoCommit(false)
		if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", tbl)); err != nil {
			t.Fatal(err)
		}
		if err := conn.Savepoint(ctx, "go_sp1"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (2)", tbl)); err != nil {
			t.Fatal(err)
		}
		if err := conn.RollbackToSavepoint(ctx, "go_sp1"); err != nil {
			t.Fatal(err)
		}
		// The transaction is still open after rolling back to the
		// savepoint: the pre-savepoint insert must survive the commit.
		if err := conn.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		stmt, err := conn.Prepare(ctx, fmt.Sprintf("SELECT i FROM %s ORDER BY i", tbl))
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close(ctx)
		rows, err := stmt.Query(ctx)
		if err != nil {
			t.Fatal(err)
		}
		row, err := rows.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row[0].(int32) != 1 {
			t.Fatalf("surviving row = %#v, want 1", row[0])
		}
		if _, err := rows.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("want exactly one surviving row, next err = %v", err)
		}
	})
}
