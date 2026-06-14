//go:build capture && integration

package cubrid_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cubrid "github.com/hexacluster/gocubrid/cubrid"
)

// framesRoot is the repo-root golden-frame store (go test runs with the
// package directory as cwd). Layout: testdata/frames/<version>/<flow>/.
const framesRoot = "../testdata/frames"

// flowManifest describes one captured flow for the offline replay harness
// (replay_test.go reads the same shape). SQL is recorded exactly as sent
// on the wire, including the run-unique table name.
type flowManifest struct {
	SQL          string `json:"sql"`
	WantRows     int    `json:"want_rows"`
	WantErrCode  int32  `json:"want_err_code,omitempty"`
	ProtoVersion int    `json:"proto_version"`
}

// TestCaptureGoldenFrames records real broker traffic into
// testdata/frames/<key>/<flow>/ for every CUBRID_TEST_DSN_<key> set in
// the environment. The frame directory is derived from the server's
// reported version (11.4 -> "114") and must agree with the DSN key, so a
// miswired DSN can never overwrite another version's frames. Regenerate
// only against a live broker, never hand-edit the .bin files. Fixture
// DDL/DML runs on a separate, uncaptured connection so each flow
// directory contains exactly the Prepare -> Execute -> drain (or error)
// round trips plus the trailing statement/connection close.
func TestCaptureGoldenFrames(t *testing.T) {
	ran := false
	for _, key := range matrixKeys {
		if os.Getenv("CUBRID_TEST_DSN_"+key) == "" {
			continue
		}
		ran = true
		t.Run("v"+key, func(t *testing.T) { captureVersionFlows(t, key) })
	}
	if !ran {
		t.Skip("no CUBRID_TEST_DSN_* set")
	}
}

func captureVersionFlows(t *testing.T, key string) {
	cfg := dsnConfig(t, key)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := cubrid.Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ver, err := conn.ServerVersion(ctx)
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	c := capsOf(ver)
	if derived := fmt.Sprintf("%d%d", c.major, c.minor); derived != key {
		t.Fatalf("server version %q derives frame dir %q, but DSN key is %q, refusing to mix versions", ver, derived, key)
	}

	t.Run("select_one", func(t *testing.T) {
		captureQueryFlow(t, cfg, key, "select_one", "SELECT 1 FROM db_root", 1)
	})

	t.Run("select_scalars", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg) // fixture conn: CUBRID_FRAME_DIR not yet set
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		// Column set is capability-gated so the one fixture works across
		// the matrix: TZ types need >=10.0, JSON >=10.2, ENUM >=9.x. One
		// fully-populated row (literals: SET has no bind encoding) and one
		// all-NULL row, so replay exercises both value and NULL paths.
		type column struct{ def, val string }
		cols := []column{
			{"c_int INT", "-7"},
			{"c_short SMALLINT", "3"},
			{"c_big BIGINT", "1099511627776"},
			{"c_float FLOAT", "2.5"},
			{"c_double DOUBLE", "1.5"},
			{"c_num NUMERIC(10,2)", "12.34"},
			{"c_char CHAR(4)", "'ab'"},
			{"c_var VARCHAR(100)", "'héllo wörld'"},
			{"c_bit BIT VARYING(64)", "X'DEAD'"},
			{"c_date DATE", "DATE'2026-06-10'"},
			{"c_time TIME", "TIME'13:45:30'"},
			{"c_ts TIMESTAMP", "TIMESTAMP'2026-06-10 13:45:30'"},
			{"c_dt DATETIME", "DATETIME'2026-06-10 13:45:30.250'"},
		}
		if c.hasTZ() {
			cols = append(cols,
				column{"c_tstz TIMESTAMPTZ", "TIMESTAMPTZ'2026-06-11 13:30:00 Asia/Seoul'"},
				column{"c_dttz DATETIMETZ", "DATETIMETZ'2026-06-11 13:30:00.250 Asia/Seoul'"})
		}
		cols = append(cols, column{"c_set SET(INT)", "{1,2,3}"})
		if c.hasENUM() {
			cols = append(cols, column{"c_enum ENUM('red','green','blue')", "'green'"})
		}
		if c.hasJSON() {
			cols = append(cols, column{"c_json JSON", `'{"k": [1, 2]}'`})
		}
		defs := make([]string, len(cols))
		vals := make([]string, len(cols))
		nulls := make([]string, len(cols))
		for i, col := range cols {
			defs[i], vals[i], nulls[i] = col.def, col.val, "NULL"
		}

		tbl := uniqName("go_frames")
		dropTableCleanup(t, cfg, tbl)
		conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl)
		if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (%s)",
			tbl, strings.Join(defs, ", "))); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (%s)",
			tbl, strings.Join(vals, ", "))); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (%s)",
			tbl, strings.Join(nulls, ", "))); err != nil {
			t.Fatal(err)
		}

		captureQueryFlow(t, cfg, key, "select_scalars",
			fmt.Sprintf("SELECT * FROM %s ORDER BY c_int", tbl), 2)
	})

	t.Run("server_error", func(t *testing.T) {
		captureErrorFlow(t, cfg, key, "server_error", "SELEC bogus FROM nowhere")
	})
}

// captureDir resets testdata/frames/<key>/<flow> so stale frames from a
// previous capture can never mix with this run's.
func captureDir(t *testing.T, key, flow string) string {
	t.Helper()
	dir := filepath.Join(framesRoot, key, flow)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// captureQueryFlow runs Prepare -> Query -> drain on a fresh capturing
// connection and writes the flow manifest next to the frames.
func captureQueryFlow(t *testing.T, cfg *cubrid.Config, key, flow, sql string, wantRows int) {
	t.Helper()
	dir := captureDir(t, key, flow)
	// Set after any fixture work: only connections opened from here on
	// capture. The t.Setenv cleanup restores the env before LIFO-earlier
	// cleanups (dropTableCleanup), so cleanup conns never write frames.
	t.Setenv("CUBRID_FRAME_DIR", dir)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := cubrid.Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stmt, err := conn.Prepare(ctx, sql)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	rows, err := stmt.Query(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		if _, err := rows.Next(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != wantRows {
		t.Fatalf("flow %s: drained %d rows, want %d", flow, n, wantRows)
	}
	writeManifest(t, dir, flowManifest{
		SQL:          sql,
		WantRows:     wantRows,
		ProtoVersion: conn.ProtocolVersion(),
	})
}

// captureErrorFlow records a flow whose Prepare fails server-side; the
// real error code lands in the manifest for replay to assert.
func captureErrorFlow(t *testing.T, cfg *cubrid.Config, key, flow, sql string) {
	t.Helper()
	dir := captureDir(t, key, flow)
	t.Setenv("CUBRID_FRAME_DIR", dir)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := cubrid.Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = conn.Prepare(ctx, sql)
	var cubErr *cubrid.Error
	if !errors.As(err, &cubErr) {
		t.Fatalf("flow %s: want *cubrid.Error from prepare, got %T: %v", flow, err, err)
	}
	if cubErr.Code >= 0 {
		t.Fatalf("flow %s: error code = %d, want negative", flow, cubErr.Code)
	}
	writeManifest(t, dir, flowManifest{
		SQL:          sql,
		WantErrCode:  cubErr.Code,
		ProtoVersion: conn.ProtocolVersion(),
	})
}

func writeManifest(t *testing.T, dir string, m flowManifest) {
	t.Helper()
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "flow.json"), append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
