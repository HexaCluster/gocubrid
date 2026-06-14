//go:build integration

package cubrid_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	cubrid "github.com/hexacluster/gocubrid/cubrid"
)

// countRows runs SELECT COUNT(*) on tbl over conn.
func countRows(t *testing.T, ctx context.Context, conn *cubrid.Conn, tbl string) int64 {
	t.Helper()
	stmt, err := conn.Prepare(ctx, "SELECT COUNT(*) FROM "+tbl)
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

// 1,000 inserts in one round-trip: every per-row result must report one
// affected row and the table must hold all the data.
func TestIntegrationExecBatchInsert(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_batch")
		dropTableCleanup(t, cfg, tbl)
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(40))", tbl)); err != nil {
			t.Fatal(err)
		}
		stmt, err := conn.Prepare(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tbl))
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close(ctx)

		const n = 1000
		rows := make([][]any, n)
		for i := range rows {
			rows[i] = []any{int32(i), fmt.Sprintf("name-%d", i)}
		}
		res, err := stmt.ExecBatch(ctx, rows)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != n {
			t.Fatalf("results = %d, want %d", len(res), n)
		}
		for i, r := range res {
			if r.Err != nil {
				t.Fatalf("row %d: %v", i, r.Err)
			}
			if r.AffectedRows != 1 {
				t.Fatalf("row %d affected = %d, want 1", i, r.AffectedRows)
			}
		}
		if got := countRows(t, ctx, conn, tbl); got != n {
			t.Fatalf("table holds %d rows, want %d", got, n)
		}
	})
}

// A constraint violation mid-batch must surface on exactly that row while
// the server keeps executing the remaining rows (observed live across the
// whole matrix: the batch continues past a failed row; the other rows
// commit).
func TestIntegrationExecBatchErrorIsolation(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_batcherr")
		dropTableCleanup(t, cfg, tbl)
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INT PRIMARY KEY)", tbl)); err != nil {
			t.Fatal(err)
		}
		stmt, err := conn.Prepare(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?)", tbl))
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close(ctx)

		// Row 5 duplicates row 2's key: a unique-constraint violation.
		rows := make([][]any, 10)
		for i := range rows {
			rows[i] = []any{int32(i)}
		}
		rows[5] = []any{int32(2)}
		res, err := stmt.ExecBatch(ctx, rows)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != len(rows) {
			t.Fatalf("results = %d, want %d", len(res), len(rows))
		}
		for i, r := range res {
			if i == 5 {
				var ce *cubrid.Error
				if !errors.As(r.Err, &ce) {
					t.Fatalf("row 5 err = %v (%T), want *cubrid.Error", r.Err, r.Err)
				}
				if ce.Code >= 0 || ce.Message == "" {
					t.Fatalf("row 5 error = code %d, message %q", ce.Code, ce.Message)
				}
				t.Logf("constraint violation surfaced as code %d: %s", ce.Code, ce.Message)
				continue
			}
			if r.Err != nil || r.AffectedRows != 1 {
				t.Fatalf("row %d = %+v", i, r)
			}
		}
		// The server continued past the failed row: all 9 distinct keys exist.
		if got := countRows(t, ctx, conn, tbl); got != 9 {
			t.Fatalf("table holds %d rows, want 9 (server expected to continue past the failure)", got)
		}
	})
}

// Mixed DDL/DML through the plain-SQL batch, with a failing statement in
// the middle.
func TestIntegrationExecBatchSQLMixedDDLDML(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_batchsql")
		dropTableCleanup(t, cfg, tbl)

		res, err := conn.ExecBatchSQL(ctx, []string{
			fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))", tbl),
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'one')", tbl),
			fmt.Sprintf("INSERT INTO %s VALUES (2, 'two')", tbl),
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'dup')", tbl), // PK violation
			fmt.Sprintf("UPDATE %s SET v = 'uno' WHERE id = 1", tbl),
			fmt.Sprintf("DELETE FROM %s WHERE id = 2", tbl),
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 6 {
			t.Fatalf("results = %d, want 6", len(res))
		}
		for i, want := range []int64{0, 1, 1, -1, 1, 1} {
			if want == -1 {
				var ce *cubrid.Error
				if !errors.As(res[i].Err, &ce) || ce.Code >= 0 {
					t.Fatalf("statement %d err = %v, want *cubrid.Error", i, res[i].Err)
				}
				continue
			}
			if res[i].Err != nil {
				t.Fatalf("statement %d: %v", i, res[i].Err)
			}
			if res[i].AffectedRows != want {
				t.Fatalf("statement %d affected = %d, want %d", i, res[i].AffectedRows, want)
			}
		}
		if got := countRows(t, ctx, conn, tbl); got != 1 {
			t.Fatalf("table holds %d rows, want 1", got)
		}
	})
}
