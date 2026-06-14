//go:build integration

package cubridsql_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cubridsql "github.com/hexacluster/gocubrid"
	"github.com/hexacluster/gocubrid/cubrid"
)

// Local copies of the cubrid package's integration conventions (test
// helpers cannot be imported across packages): uniqName, forEachDSN, caps,
// dropTableCleanup.

var tblSeq atomic.Int64

func uniqName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), tblSeq.Add(1))
}

type caps struct{ major, minor int }

func capsOf(version string) caps {
	var c caps
	fmt.Sscanf(version, "%d.%d", &c.major, &c.minor)
	return c
}

func (c caps) hasMVCC() bool { return c.major >= 10 }

var matrixKeys = []string{"93", "101", "102", "110", "114"}

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
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			nc, err := cubrid.Connect(ctx, cfg)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			ver, err := nc.ServerVersion(ctx)
			nc.Close()
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

func openDB(t *testing.T, cfg *cubrid.Config) *sql.DB {
	t.Helper()
	db := sql.OpenDB(cubridsql.NewConnector(cfg))
	t.Cleanup(func() { db.Close() })
	return db
}

func dropTableCleanup(t *testing.T, cfg *cubrid.Config, tbl string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		db := sql.OpenDB(cubridsql.NewConnector(cfg))
		defer db.Close()
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			t.Logf("cleanup %s: drop: %v", tbl, err)
		}
	})
}

// TestIntegrationSQLCRUD mirrors the native CRUD round-trip through
// database/sql: typed INSERT binds, SELECT scans with adapter widening,
// RowsAffected for UPDATE/DELETE, and live ColumnTypes metadata.
func TestIntegrationSQLCRUD(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		db := openDB(t, cfg)

		tbl := uniqName("go_sqlcrud")
		dropTableCleanup(t, cfg, tbl)
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(40), price NUMERIC(10,2), ratio DOUBLE, big BIGINT, created DATETIME)", tbl)); err != nil {
			t.Fatal(err)
		}

		created := time.Date(2026, 6, 11, 12, 34, 56, 500e6, time.UTC)
		res, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?)", tbl),
			int64(1), "first", "12.34", 1.5, int64(1)<<40, created)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := res.RowsAffected(); err != nil || n != 1 {
			t.Fatalf("insert RowsAffected = (%d, %v)", n, err)
		}

		rows, err := db.QueryContext(ctx, fmt.Sprintf(
			"SELECT id, name, price, ratio, big, created FROM %s WHERE id = ?", tbl), int64(1))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		cts, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}
		wantNames := []string{"INTEGER", "VARCHAR", "NUMERIC", "DOUBLE", "BIGINT", "DATETIME"}
		for i, ct := range cts {
			if ct.DatabaseTypeName() != wantNames[i] {
				t.Errorf("column %d (%s) type name = %q, want %q", i, ct.Name(), ct.DatabaseTypeName(), wantNames[i])
			}
		}
		if n, ok := cts[0].Nullable(); !ok || n {
			t.Errorf("id nullable = (%v, %v), want (false, true): PK column", n, ok)
		}
		if n, ok := cts[1].Nullable(); !ok || !n {
			t.Errorf("name nullable = (%v, %v), want (true, true)", n, ok)
		}
		if p, s, ok := cts[2].DecimalSize(); !ok || p != 10 || s != 2 {
			t.Errorf("price decimal size = (%d, %d, %v), want (10, 2, true)", p, s, ok)
		}
		if l, ok := cts[1].Length(); !ok || l != 40 {
			t.Errorf("name length = (%d, %v), want (40, true)", l, ok)
		}

		if !rows.Next() {
			t.Fatalf("no row: %v", rows.Err())
		}
		var (
			id    int64
			name  string
			price string
			ratio float64
			big   int64
			when  time.Time
		)
		if err := rows.Scan(&id, &name, &price, &ratio, &big, &when); err != nil {
			t.Fatal(err)
		}
		if id != 1 || name != "first" || price != "12.34" || ratio != 1.5 || big != int64(1)<<40 || !when.Equal(created) {
			t.Fatalf("row = %d %q %q %v %d %v", id, name, price, ratio, big, when)
		}
		rows.Close()

		res, err = db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET name = ? WHERE id = ?", tbl), "renamed", int64(1))
		if err != nil {
			t.Fatal(err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			t.Fatalf("update RowsAffected = %d", n)
		}
		res, err = db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = ?", tbl), int64(1))
		if err != nil {
			t.Fatal(err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			t.Fatalf("delete RowsAffected = %d", n)
		}
		var left int64
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl)).Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left != 0 {
			t.Fatalf("rows left after delete = %d", left)
		}
	})
}

// TestIntegrationSQLNumeric round-trips cubrid.Numeric through database/sql:
// bound via its driver.Valuer (exact string over the wire) and scanned back
// through its sql.Scanner, asserting the server's exact decimal rendering.
func TestIntegrationSQLNumeric(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		db := openDB(t, cfg)

		tbl := uniqName("go_sqlnum")
		dropTableCleanup(t, cfg, tbl)
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INT PRIMARY KEY, n NUMERIC(12,3))", tbl)); err != nil {
			t.Fatal(err)
		}
		cases := []struct {
			id   int64
			bind cubrid.Numeric
			want cubrid.Numeric
		}{
			{1, "12345.678", "12345.678"}, // full scale
			{2, "7.5", "7.500"},           // server pads to the declared scale
			{3, "-99.95", "-99.950"},      // sign survives
		}
		for _, c := range cases {
			if _, err := db.ExecContext(ctx,
				fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tbl), c.id, c.bind); err != nil {
				t.Fatalf("insert %d (%s): %v", c.id, c.bind, err)
			}
		}
		for _, c := range cases {
			var got cubrid.Numeric
			if err := db.QueryRowContext(ctx,
				fmt.Sprintf("SELECT n FROM %s WHERE id = ?", tbl), c.id).Scan(&got); err != nil {
				t.Fatalf("scan %d: %v", c.id, err)
			}
			if got != c.want {
				t.Errorf("row %d: Numeric = %q, want %q", c.id, got, c.want)
			}
		}
	})
}

// TestIntegrationSQLTx drives commit/rollback and isolation through
// database/sql's BeginTx. Every version round-trips the three level codes;
// the visibility semantics are asserted on MVCC servers (10.0+) only,
// 9.x's lock-based levels share the codes but block instead of snapshot.
func TestIntegrationSQLTx(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, c caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		db := openDB(t, cfg)
		db.SetMaxOpenConns(2)
		db.SetMaxIdleConns(2)

		tbl := uniqName("go_sqltx")
		dropTableCleanup(t, cfg, tbl)
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (i INT)", tbl)); err != nil {
			t.Fatal(err)
		}
		count := func(q interface {
			QueryRowContext(context.Context, string, ...any) *sql.Row
		}) int64 {
			var n int64
			if err := q.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl)).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}

		// Rollback discards, commit persists.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", tbl)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if n := count(db); n != 0 {
			t.Fatalf("count after rollback = %d", n)
		}
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (2)", tbl)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if n := count(db); n != 1 {
			t.Fatalf("count after commit = %d", n)
		}

		// Every supported level begins, queries, and commits cleanly.
		for _, lv := range []sql.IsolationLevel{
			sql.LevelReadCommitted, sql.LevelRepeatableRead, sql.LevelSerializable,
		} {
			tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: lv})
			if err != nil {
				t.Fatalf("BeginTx(%v): %v", lv, err)
			}
			if n := count(tx); n != 1 {
				t.Fatalf("count in %v tx = %d", lv, n)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit %v: %v", lv, err)
			}
		}

		if !c.hasMVCC() {
			t.Skip("isolation visibility semantics asserted on MVCC servers (10.0+) only")
		}

		// REPEATABLE READ: the snapshot ignores a concurrent commit.
		tx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			t.Fatal(err)
		}
		if n := count(tx); n != 1 {
			t.Fatalf("RR snapshot count = %d, want 1", n)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (3)", tbl)); err != nil {
			t.Fatal(err) // autocommit writer on the pool's second conn
		}
		if n := count(tx); n != 1 {
			t.Fatalf("RR count after concurrent commit = %d, want still 1", n)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if n := count(db); n != 2 {
			t.Fatalf("count after RR tx end = %d, want 2", n)
		}

		// READ COMMITTED: each statement sees the latest committed state.
		tx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		if n := count(tx); n != 2 {
			t.Fatalf("RC initial count = %d, want 2", n)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (4)", tbl)); err != nil {
			t.Fatal(err)
		}
		if n := count(tx); n != 3 {
			t.Fatalf("RC count after concurrent commit = %d, want 3", n)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
}

// TestIntegrationSQLPool exercises database/sql pooling: 8 pooled
// connections shared by 64 goroutines running mixed INSERT/SELECT traffic.
func TestIntegrationSQLPool(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		db := openDB(t, cfg)
		db.SetMaxOpenConns(8)
		db.SetMaxIdleConns(8)

		tbl := uniqName("go_sqlpool")
		dropTableCleanup(t, cfg, tbl)
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (g INT, i INT)", tbl)); err != nil {
			t.Fatal(err)
		}

		const (
			goroutines = 64
			opsEach    = 4
		)
		errs := make(chan error, goroutines)
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < opsEach; i++ {
					if _, err := db.ExecContext(ctx,
						fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tbl), int64(g), int64(i)); err != nil {
						errs <- fmt.Errorf("goroutine %d insert %d: %w", g, i, err)
						return
					}
					var n int64
					if err := db.QueryRowContext(ctx,
						fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE g = ?", tbl), int64(g)).Scan(&n); err != nil {
						errs <- fmt.Errorf("goroutine %d count %d: %w", g, i, err)
						return
					}
					if n != int64(i+1) {
						errs <- fmt.Errorf("goroutine %d sees %d own rows, want %d", g, n, i+1)
						return
					}
				}
			}(g)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		if t.Failed() {
			return
		}
		var total int64
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl)).Scan(&total); err != nil {
			t.Fatal(err)
		}
		if total != goroutines*opsEach {
			t.Fatalf("total rows = %d, want %d", total, goroutines*opsEach)
		}
	})
}
