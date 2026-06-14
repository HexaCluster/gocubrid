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

func TestIntegrationSelectOne(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		stmt, err := conn.Prepare(ctx, "SELECT 1 FROM db_root")
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
		if v, ok := row[0].(int32); !ok || v != 1 {
			t.Fatalf("row[0] = %#v, want int32(1)", row[0])
		}
		if _, err := rows.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("want EOF, got %v", err)
		}
	})
}

// Validates EncodeParam's type-byte layout for binds (confirmed live on 11.4).
func TestIntegrationBindRoundTrip(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_bind")
		dropTableCleanup(t, cfg, tbl)
		// IF EXISTS guards PID-reuse collisions when a sweep was skipped;
		// crashed-run leftovers are reclaimed by sweepStaleTables.
		conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
		if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i INT, v VARCHAR(50))", tbl)); err != nil {
			t.Fatal(err)
		}

		res, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tbl), int32(42), "hello")
		if err != nil {
			t.Fatal(err)
		}
		if res.AffectedRows != 1 {
			t.Fatalf("affected = %d", res.AffectedRows)
		}

		stmt, err := conn.Prepare(ctx, fmt.Sprintf("SELECT i, v FROM %s WHERE i = ?", tbl))
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close(ctx)
		rows, err := stmt.Query(ctx, int32(42))
		if err != nil {
			t.Fatal(err)
		}
		row, err := rows.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row[0].(int32) != 42 || row[1].(string) != "hello" {
			t.Fatalf("row = %#v", row)
		}
	})
}

// Plan 1 acceptance gate. Judgment calls settled live on 11.4: CHAR(4) pads
// with spaces, NULLs sort last in ORDER BY ... DESC (asserts stay
// order-agnostic for older versions), DATETIME keeps millisecond precision;
// COUNT(*) wire type and error codes are pinned in the tx and error tests.
func TestIntegrationScalarMatrix(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_types")
		dropTableCleanup(t, cfg, tbl)
		conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
		_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
			c_int INT, c_short SMALLINT, c_big BIGINT,
			c_float FLOAT, c_double DOUBLE, c_num NUMERIC(10,2),
			c_char CHAR(4), c_var VARCHAR(100), c_bit BIT VARYING(64),
			c_date DATE, c_time TIME, c_ts TIMESTAMP, c_dt DATETIME)`, tbl))
		if err != nil {
			t.Fatal(err)
		}

		dt := time.Date(2026, 6, 10, 13, 45, 30, 250e6, time.UTC)
		_, err = conn.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", tbl),
			int32(-7), int16(3), int64(1<<40),
			float32(2.5), 1.5, "12.34",
			"ab", "héllo wörld", []byte{0xDE, 0xAD},
			"2026-06-10", "13:45:30", "2026-06-10 13:45:30", dt)
		if err != nil {
			t.Fatal(err)
		}
		// all-NULL row
		args := make([]any, 13)
		if _, err := conn.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", tbl), args...); err != nil {
			t.Fatal(err)
		}

		stmt, err := conn.Prepare(ctx, fmt.Sprintf("SELECT * FROM %s ORDER BY c_int DESC", tbl))
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close(ctx)
		rows, err := stmt.Query(ctx)
		if err != nil {
			t.Fatal(err)
		}

		nullRow, err := rows.Next() // on 11.4 NULLs sort last in DESC, so this is the value row and the swap below applies
		if err != nil {
			t.Fatal(err)
		}
		valRow, err := rows.Next()
		if err != nil {
			t.Fatal(err)
		}
		// Identify which row is which without depending on NULL sort order.
		if valRow[0] == nil {
			nullRow, valRow = valRow, nullRow
		}
		for i, v := range nullRow {
			if v != nil {
				t.Errorf("null row col %d = %#v", i, v)
			}
		}
		if valRow[0].(int32) != -7 || valRow[1].(int16) != 3 || valRow[2].(int64) != 1<<40 {
			t.Errorf("ints: %#v", valRow[:3])
		}
		if valRow[3].(float32) != 2.5 || valRow[4].(float64) != 1.5 {
			t.Errorf("floats: %#v", valRow[3:5])
		}
		if valRow[5].(string) != "12.34" {
			t.Errorf("numeric: %#v", valRow[5])
		}
		if valRow[6].(string) != "ab  " { // CHAR(4) pads
			t.Errorf("char: %q", valRow[6])
		}
		if valRow[7].(string) != "héllo wörld" {
			t.Errorf("varchar: %q", valRow[7])
		}
		if got := valRow[12].(time.Time); !got.Equal(dt) {
			t.Errorf("datetime: %v want %v", got, dt)
		}

		// NUMERIC edges: NUMERIC(38,10) extremes and negative fractional
		// values must round-trip exactly as strings (the wire carries NUMERIC
		// as its decimal string).
		ntbl := uniqName("go_numedge")
		dropTableCleanup(t, cfg, ntbl)
		conn.Exec(ctx, "DROP TABLE IF EXISTS "+ntbl)
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s (k INT, n NUMERIC(38,10))", ntbl)); err != nil {
			t.Fatal(err)
		}
		numEdges := []string{
			"9999999999999999999999999999.9999999999",  // max NUMERIC(38,10)
			"-9999999999999999999999999999.9999999999", // min NUMERIC(38,10)
			"0.0000000001",      // smallest positive at scale 10
			"-0.0000000001",     // smallest negative at scale 10
			"-12345.6789000000", // negative value with scale padding
			"0.0000000000",      // zero rendered at full scale
		}
		for i, v := range numEdges {
			if _, err := conn.Exec(ctx,
				fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", ntbl), int32(i), v); err != nil {
				t.Fatalf("numeric edge insert %q: %v", v, err)
			}
		}
		nstmt, err := conn.Prepare(ctx, fmt.Sprintf("SELECT n FROM %s ORDER BY k", ntbl))
		if err != nil {
			t.Fatal(err)
		}
		defer nstmt.Close(ctx)
		nrows, err := nstmt.Query(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for i, want := range numEdges {
			nrow, err := nrows.Next()
			if err != nil {
				t.Fatalf("numeric edge row %d: %v", i, err)
			}
			if got, ok := nrow[0].(string); !ok || got != want {
				t.Errorf("numeric edge %d = %#v, want %q", i, nrow[0], want)
			}
		}
	})
}

// Error code values come from the live server (11.4 answers this syntax
// error with code -493); assert shape only to stay version-portable.
func TestIntegrationServerError(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		_, err = conn.Prepare(ctx, "SELEC bogus FROM nowhere")
		var cubErr *cubrid.Error
		if !errors.As(err, &cubErr) {
			t.Fatalf("want *cubrid.Error, got %T: %v", err, err)
		}
		if cubErr.Code >= 0 || cubErr.Message == "" {
			t.Fatalf("error = %+v", cubErr)
		}
		// connection must remain usable after a server error
		if err := conn.Ping(ctx); err != nil {
			t.Fatalf("ping after error: %v", err)
		}
	})
}

func TestIntegrationContextCancel(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		conn, err := cubrid.Connect(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()
		time.Sleep(5 * time.Millisecond) // ensure already expired
		_, err = conn.Prepare(ctx, "SELECT 1 FROM db_root")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want DeadlineExceeded, got %v", err)
		}
		// transport state is undefined mid-frame: conn must be poisoned
		if err := conn.Ping(context.Background()); !errors.Is(err, cubrid.ErrBadConn) {
			t.Fatalf("want ErrBadConn after cancellation, got %v", err)
		}
	})
}

// Pin that table cleanup survives the original flake's failure mode: a
// test dying of deadline expiry leaves its conn poisoned (ErrBadConn, see
// above), so a `defer conn.Exec(ctx, DROP ...)` on the test's own conn/ctx
// deterministically fails exactly when cleanup is needed. dropTableCleanup
// must drop the table anyway, on a fresh conn with its own deadline.
func TestIntegrationCleanupAfterPoisonedConn(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_poison")
		// Registered before dropTableCleanup so it runs after it (LIFO):
		// verify the drop really happened despite the poisoned test conn.
		t.Cleanup(func() {
			vctx, vcancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer vcancel()
			vc, err := cubrid.Connect(vctx, cfg)
			if err != nil {
				t.Fatalf("verify connect: %v", err)
			}
			defer vc.Close()
			stmt, err := vc.Prepare(vctx, "SELECT class_name FROM db_class WHERE class_name = ?")
			if err != nil {
				t.Fatalf("verify prepare: %v", err)
			}
			defer stmt.Close(vctx)
			rows, err := stmt.Query(vctx, tbl)
			if err != nil {
				t.Fatalf("verify query: %v", err)
			}
			if _, err := rows.Next(); !errors.Is(err, io.EOF) {
				t.Errorf("table %s survived cleanup after poisoned conn (next: %v)", tbl, err)
			}
		})
		dropTableCleanup(t, cfg, tbl)

		if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i INT)", tbl)); err != nil {
			t.Fatal(err)
		}

		// Poison the conn the way the original hang did: a request issued on
		// an already-expired context.
		expired, ecancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer ecancel()
		time.Sleep(5 * time.Millisecond)
		if _, err := conn.Prepare(expired, "SELECT 1 FROM db_root"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want DeadlineExceeded, got %v", err)
		}
		if err := conn.Ping(context.Background()); !errors.Is(err, cubrid.ErrBadConn) {
			t.Fatalf("want ErrBadConn, got %v", err)
		}
	})
}
