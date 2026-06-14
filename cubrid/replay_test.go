package cubrid

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hexacluster/gocubrid/internal/prototest"
)

// replayManifest mirrors the flowManifest written by capture_test.go.
type replayManifest struct {
	SQL          string `json:"sql"`
	WantRows     int    `json:"want_rows"`
	WantErrCode  int32  `json:"want_err_code"`
	ProtoVersion int    `json:"proto_version"`
}

// TestReplayGoldenFrames re-runs every captured flow in
// testdata/frames/<version>/<flow>/ against the fake broker: the recorded
// responses are queued keyed by the function code of their paired request,
// the broker is pinned at the protocol version the flow was captured
// under, and the client must parse the real broker bytes offline exactly
// as it did live.
func TestReplayGoldenFrames(t *testing.T) {
	root := filepath.Join("..", "testdata", "frames")
	versions, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skip("testdata/frames absent; capture against a live broker first (go test -tags 'capture integration' -run TestCapture)")
	}
	if err != nil {
		t.Fatal(err)
	}
	ran := false
	for _, ver := range versions {
		if !ver.IsDir() {
			continue
		}
		flows, err := os.ReadDir(filepath.Join(root, ver.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, flow := range flows {
			if !flow.IsDir() {
				continue
			}
			ran = true
			dir := filepath.Join(root, ver.Name(), flow.Name())
			t.Run(ver.Name()+"/"+flow.Name(), func(t *testing.T) {
				replayFlow(t, dir)
			})
		}
	}
	if !ran {
		t.Skip("testdata/frames holds no flow directories")
	}
}

func replayFlow(t *testing.T, dir string) {
	raw, err := os.ReadFile(filepath.Join(dir, "flow.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m replayManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("flow.json: %v", err)
	}
	if m.ProtoVersion <= 0 {
		t.Fatalf("flow.json: invalid proto_version %d", m.ProtoVersion)
	}

	fb := prototest.Start(t)
	fb.SetProtocolVersion(m.ProtoVersion)
	queueCapturedFrames(t, fb, dir)

	cfg, err := ParseDSN("cubrid://dba:pw@" + fb.Addr() + "/testdb")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	conn, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if v := conn.ProtocolVersion(); v != m.ProtoVersion {
		t.Fatalf("negotiated protocol %d, want pinned %d", v, m.ProtoVersion)
	}

	stmt, err := conn.Prepare(ctx, m.SQL)
	if m.WantErrCode != 0 {
		if err == nil {
			_, err = stmt.Query(ctx)
		}
		var cubErr *Error
		if !errors.As(err, &cubErr) {
			t.Fatalf("want *Error(code %d), got %T: %v", m.WantErrCode, err, err)
		}
		if cubErr.Code != m.WantErrCode {
			t.Fatalf("error code = %d, want %d", cubErr.Code, m.WantErrCode)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	rows, err := stmt.Query(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		if _, err := rows.Next(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("row %d: %v", n, err)
		}
		n++
	}
	if n != m.WantRows {
		t.Fatalf("replayed %d rows, want %d", n, m.WantRows)
	}
}

// queueCapturedFrames pairs each NNN-res.bin with the request dumped just
// before it (capture numbers files in wire order) and queues the response
// under that request's function-code byte.
func queueCapturedFrames(t *testing.T, fb *prototest.FakeBroker, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir) // sorted by name == wire order
	if err != nil {
		t.Fatal(err)
	}
	var lastFn byte
	pendingReq := false
	frames := 0
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, "-req.bin"):
			if pendingReq {
				t.Fatalf("%s: two requests without a response between them", name)
			}
			p, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if len(p) == 0 {
				t.Fatalf("%s: empty request frame", name)
			}
			lastFn = p[0]
			pendingReq = true
		case strings.HasSuffix(name, "-res.bin"):
			if !pendingReq {
				t.Fatalf("%s: response without a preceding request", name)
			}
			p, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			fb.Queue(lastFn, p)
			pendingReq = false
			frames++
		}
	}
	if frames == 0 {
		t.Fatalf("%s: no captured frames", dir)
	}
}
