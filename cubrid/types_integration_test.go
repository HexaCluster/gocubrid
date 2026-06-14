//go:build integration

package cubrid_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	cubrid "github.com/hexacluster/gocubrid/cubrid"
)

func TestIntegrationCollections(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_coll")
		dropTableCleanup(t, cfg, tbl)
		conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl)
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s (s SET(INT), m MULTISET(INT), q SEQUENCE(VARCHAR(10)))", tbl)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s VALUES ({1,2,3}, {1,1,2}, {'a','b'})", tbl)); err != nil {
			t.Fatal(err)
		}
		stmt, err := conn.Prepare(ctx, "SELECT s, m, q FROM "+tbl)
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

		// SET elements arrive in no contractual order; assert membership.
		s, ok := row[0].([]any)
		if !ok {
			t.Fatalf("set = %#v, want []any", row[0])
		}
		got := map[int32]bool{}
		for _, v := range s {
			got[v.(int32)] = true
		}
		if len(s) != 3 || !got[1] || !got[2] || !got[3] {
			t.Errorf("set = %#v, want {1,2,3}", s)
		}

		m, ok := row[1].([]any)
		if !ok || len(m) != 3 {
			t.Errorf("multiset = %#v, want 3 elements", row[1])
		}

		// SEQUENCE preserves insertion order.
		q, ok := row[2].([]any)
		if !ok || len(q) != 2 || q[0].(string) != "a" || q[1].(string) != "b" {
			t.Errorf("seq = %#v, want [a b]", row[2])
		}

		// Nested collection VALUES are legal expression results even though
		// nested collection DDL is rejected ("nested data type definition").
		// Live 11.4 serializes the value with element-type byte 18 (SEQUENCE)
		// wrapping inner INT collections, and PREPARE metadata arrives as
		// Collection=3 with TypeCode=18, pin that wire truth here.
		nstmt, err := conn.Prepare(ctx, "SELECT {{1,2},{3,4}}")
		if err != nil {
			t.Fatal(err)
		}
		defer nstmt.Close(ctx)
		nrows, err := nstmt.Query(ctx)
		if err != nil {
			t.Fatal(err)
		}
		nrow, err := nrows.Next()
		if err != nil {
			t.Fatal(err)
		}
		outer, ok := nrow[0].([]any)
		if !ok || len(outer) != 2 {
			t.Fatalf("nested literal = %#v, want 2-element []any", nrow[0])
		}
		for i, want := range [][]int32{{1, 2}, {3, 4}} {
			in, ok := outer[i].([]any)
			if !ok || len(in) != 2 || in[0].(int32) != want[0] || in[1].(int32) != want[1] {
				t.Errorf("nested literal[%d] = %#v, want %v", i, outer[i], want)
			}
		}
	})
}

// Collection BINDS (Plan 2 covered only decode): a []any argument goes out
// as a SEQUENCE-typed parameter carrying [total size][element type][sized
// elements] (JDBC writeCollection, no element count on the request side),
// and the server coerces it into SET/MULTISET/SEQUENCE columns alike.
func TestIntegrationCollectionBind(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_collbind")
		dropTableCleanup(t, cfg, tbl)
		conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl)
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s (s SET(INT), q SEQUENCE(VARCHAR(10)))", tbl)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tbl),
			[]any{int32(1), int32(2), int32(3)}, []any{"a", "b"}); err != nil {
			t.Fatal(err)
		}

		stmt, err := conn.Prepare(ctx, "SELECT s, q FROM "+tbl)
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

		// SET elements arrive in no contractual order; assert membership.
		s, ok := row[0].([]any)
		if !ok {
			t.Fatalf("set = %#v, want []any", row[0])
		}
		got := map[int32]bool{}
		for _, v := range s {
			got[v.(int32)] = true
		}
		if len(s) != 3 || !got[1] || !got[2] || !got[3] {
			t.Errorf("set = %#v, want {1,2,3}", s)
		}

		// SEQUENCE preserves insertion order.
		q, ok := row[1].([]any)
		if !ok || len(q) != 2 || q[0].(string) != "a" || q[1].(string) != "b" {
			t.Errorf("seq = %#v, want [a b]", row[1])
		}
	})
}

func TestIntegrationEnumAndJSON(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, c caps) {
		if !c.hasENUM() {
			t.Skip("ENUM requires CUBRID >= 9.0")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_ej")
		dropTableCleanup(t, cfg, tbl)
		conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl)
		// JSON columns exist only on >= 10.2; older versions run the
		// ENUM-only shape of the same flow.
		cols, vals, sel := "e ENUM('red','green','blue')", "'green'", "e"
		if c.hasJSON() {
			cols += ", j JSON"
			vals += `, '{"k": [1, 2]}'`
			sel += ", j"
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s (%s)", tbl, cols)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s VALUES (%s)", tbl, vals)); err != nil {
			t.Fatal(err)
		}
		stmt, err := conn.Prepare(ctx, fmt.Sprintf("SELECT %s FROM %s", sel, tbl))
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
		// ENUM must arrive as its label, never the 1-based index short.
		if s, ok := row[0].(string); !ok || s != "green" {
			t.Errorf("enum = %#v, want \"green\"", row[0])
		}
		if !c.hasJSON() {
			return
		}
		raw, ok := row[1].([]byte)
		if !ok {
			t.Fatalf("json = %#v, want []byte", row[1])
		}
		var parsed map[string]any
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatalf("json: %v (raw %q)", err, raw)
		}
		arr, ok := parsed["k"].([]any)
		if !ok || len(arr) != 2 || arr[0].(float64) != 1 || arr[1].(float64) != 2 {
			t.Errorf(`json content = %#v, want {"k": [1, 2]}`, parsed)
		}
	})
}

func TestIntegrationTZTypes(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, c caps) {
		if !c.hasTZ() {
			t.Skip("timezone types require CUBRID >= 10.0")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_tz")
		dropTableCleanup(t, cfg, tbl)
		conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl)
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s (ts TIMESTAMPTZ, dt DATETIMETZ, tsl TIMESTAMPLTZ, dtl DATETIMELTZ)", tbl)); err != nil {
			t.Fatal(err)
		}

		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s VALUES (TIMESTAMPTZ'2026-06-11 13:30:00 Asia/Seoul', DATETIMETZ'2026-06-11 13:30:00.250 Asia/Seoul',"+
				" TIMESTAMPTZ'2026-06-11 13:30:00 Asia/Seoul', DATETIMETZ'2026-06-11 13:30:00.250 Asia/Seoul')", tbl)); err != nil {
			t.Fatal(err)
		}
		stmt, err := conn.Prepare(ctx, "SELECT ts, dt, tsl, dtl FROM "+tbl)
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
		wantInstant := time.Date(2026, 6, 11, 4, 30, 0, 0, time.UTC) // 13:30 KST
		ts := row[0].(time.Time)
		if !ts.Equal(wantInstant) {
			t.Errorf("ts = %v (instant %v), want %v", ts, ts.UTC(), wantInstant)
		}
		if ts.Location().String() != "Asia/Seoul" {
			t.Errorf("ts location = %v, want Asia/Seoul", ts.Location())
		}
		dt := row[1].(time.Time)
		if !dt.Equal(wantInstant.Add(250 * time.Millisecond)) {
			t.Errorf("dt = %v", dt)
		}
		// LTZ columns store the same instant; the broker renders it in the
		// server session timezone, so assert the instant only.
		if tsl := row[2].(time.Time); !tsl.Equal(wantInstant) {
			t.Errorf("tsl = %v (instant %v), want %v", tsl, tsl.UTC(), wantInstant)
		}
		if dtl := row[3].(time.Time); !dtl.Equal(wantInstant.Add(250 * time.Millisecond)) {
			t.Errorf("dtl = %v (instant %v)", dtl, dtl.UTC())
		}
	})
}

// TestIntegrationNumericBind binds cubrid.Numeric through the native API
// (the encodeArg Numeric case, not database/sql's Valuer path) and asserts
// the exact string rendering the server hands back, including padding to
// the declared scale.
func TestIntegrationNumericBind(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_numeric")
		dropTableCleanup(t, cfg, tbl)
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INT PRIMARY KEY, n NUMERIC(12,3))", tbl)); err != nil {
			t.Fatal(err)
		}
		ins, err := conn.Prepare(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tbl))
		if err != nil {
			t.Fatal(err)
		}
		defer ins.Close(ctx)
		want := map[int32]string{
			1: "12345.678", // full scale
			2: "7.500",     // bound "7.5": server pads to the declared scale
			3: "-99.950",   // sign survives
		}
		for id, src := range map[int32]cubrid.Numeric{1: "12345.678", 2: "7.5", 3: "-99.95"} {
			if _, err := ins.Exec(ctx, id, src); err != nil {
				t.Fatalf("insert %d (%s): %v", id, src, err)
			}
		}
		stmt, err := conn.Prepare(ctx, fmt.Sprintf("SELECT id, n FROM %s ORDER BY id", tbl))
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close(ctx)
		rows, err := stmt.Query(ctx)
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		for {
			row, err := rows.Next()
			if err != nil {
				break
			}
			seen++
			id := row[0].(int32)
			got, ok := row[1].(string)
			if !ok {
				t.Fatalf("row %d: NUMERIC arrived as %T, want string", id, row[1])
			}
			if got != want[id] {
				t.Errorf("row %d: NUMERIC rendering = %q, want %q", id, got, want[id])
			}
		}
		if seen != 3 {
			t.Fatalf("rows = %d, want 3", seen)
		}
	})
}
