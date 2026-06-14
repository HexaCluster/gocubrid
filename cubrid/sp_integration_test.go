//go:build integration

package cubrid_test

import (
	"context"
	"os"
	"testing"
	"time"

	cubrid "github.com/hexacluster/gocubrid/cubrid"
)

// spConn connects to the JavaSP-enabled database. Stored procedures need
// server-side setup (the PL/JavaSP server plus the GoSpTest class), so
// these tests gate on CUBRID_TEST_SP_DSN, a DSN pointing at a broker
// whose database has the PL/JavaSP server enabled (11.x).
func spConn(t *testing.T) (context.Context, *cubrid.Conn) {
	t.Helper()
	dsn := os.Getenv("CUBRID_TEST_SP_DSN")
	if dsn == "" {
		t.Skip("CUBRID_TEST_SP_DSN not set (needs a broker with the PL/JavaSP server and the GoSpTest class loaded)")
	}
	cfg, err := cubrid.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	conn, err := cubrid.Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return ctx, conn
}

// A function SP's return value binds to the leading marker of
// "? = CALL fn()" when that marker is registered OUT.
func TestIntegrationCallFunctionReturnValue(t *testing.T) {
	ctx, conn := spConn(t)
	stmt, err := conn.PrepareCall(ctx, "? = CALL sphello()")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	if got := stmt.ParamCount(); got != 1 {
		t.Fatalf("ParamCount = %d, want 1", got)
	}
	if err := stmt.RegisterOutParam(1); err != nil {
		t.Fatal(err)
	}
	res, err := stmt.Exec(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.AffectedRows != 1 {
		t.Fatalf("AffectedRows = %d, want 1 (the value row)", res.AffectedRows)
	}
	out := stmt.OutValues()
	if len(out) != 1 {
		t.Fatalf("OutValues = %#v", out)
	}
	if got, ok := out[0].(string); !ok || got != "hello cubrid" {
		t.Fatalf("return value = %#v, want \"hello cubrid\"", out[0])
	}
}

// A procedure's OUT parameter comes back in the value row.
func TestIntegrationCallOutParam(t *testing.T) {
	ctx, conn := spConn(t)
	stmt, err := conn.PrepareCall(ctx, "CALL spoutint(?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	if err := stmt.RegisterOutParam(1); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	out := stmt.OutValues()
	if len(out) != 1 {
		t.Fatalf("OutValues = %#v", out)
	}
	if got, ok := out[0].(int32); !ok || got != 42 {
		t.Fatalf("OUT param = %#v, want int32(42)", out[0])
	}
}

// IN arguments and an OUT return marker mix in one call; the statement is
// reusable with fresh arguments.
func TestIntegrationCallInParams(t *testing.T) {
	ctx, conn := spConn(t)
	stmt, err := conn.PrepareCall(ctx, "? = CALL spconcat(?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	if got := stmt.ParamCount(); got != 3 {
		t.Fatalf("ParamCount = %d, want 3", got)
	}
	if err := stmt.RegisterOutParam(1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range [][3]string{
		{"foo", "bar", "foobar"},
		{"héllo ", "wörld", "héllo wörld"}, // multibyte UTF-8 through the SP
	} {
		if _, err := stmt.Exec(ctx, nil, tc[0], tc[1]); err != nil {
			t.Fatal(err)
		}
		out := stmt.OutValues()
		if len(out) != 1 {
			t.Fatalf("OutValues = %#v", out)
		}
		if got, ok := out[0].(string); !ok || got != tc[2] {
			t.Fatalf("spconcat(%q, %q) = %#v, want %q", tc[0], tc[1], out[0], tc[2])
		}
	}
}

// Query on a CALL yields the raw value row: [statement slot, marker 1..N].
func TestIntegrationCallQueryRow(t *testing.T) {
	ctx, conn := spConn(t)
	stmt, err := conn.PrepareCall(ctx, "? = CALL spconcat(?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	if err := stmt.RegisterOutParam(1); err != nil {
		t.Fatal(err)
	}
	rows, err := stmt.Query(ctx, nil, "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	row, err := rows.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(row) != 4 {
		t.Fatalf("call row = %#v, want 4 values (slot + 3 markers)", row)
	}
	if got, ok := row[1].(string); !ok || got != "ab" {
		t.Fatalf("marker 1 value = %#v, want \"ab\"", row[1])
	}
}
