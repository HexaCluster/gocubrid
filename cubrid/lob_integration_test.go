//go:build integration

package cubrid_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	cubrid "github.com/hexacluster/gocubrid/cubrid"
)

// lobChunk mirrors the protocol's 128 KiB LOB transfer cap, for sizing
// fixtures that must cross a chunk boundary.
const lobChunk = 128 * 1024

// lobTestData returns n deterministic pseudo-random bytes (xorshift64),
// so corruption shows up at any offset without storing fixtures.
func lobTestData(n int) []byte {
	b := make([]byte, n)
	s := uint64(0x9E3779B97F4A7C15)
	for i := range b {
		s ^= s << 13
		s ^= s >> 7
		s ^= s << 17
		b[i] = byte(s)
	}
	return b
}

// fetchLob runs SELECT col FROM tbl WHERE id = ? and returns the single
// LOB value (nil for SQL NULL).
func fetchLob(ctx context.Context, t *testing.T, conn *cubrid.Conn, tbl, col string, id int32) *cubrid.Lob {
	t.Helper()
	stmt, err := conn.Prepare(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE id = ?", col, tbl))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	rows, err := stmt.Query(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	row, err := rows.Next()
	if err != nil {
		t.Fatal(err)
	}
	if row[0] == nil {
		return nil
	}
	lob, ok := row[0].(*cubrid.Lob)
	if !ok {
		t.Fatalf("%s value = %#v, want *cubrid.Lob", col, row[0])
	}
	return lob
}

// Live findings across the whole matrix (9.3.9 to 11.4.5):
//   - NEW_LOB/WRITE_LOB/READ_LOB and the packed-handle layout are
//     identical on all five versions.
//   - A READ_LOB reply can be SHORT of the requested chunk without
//     meaning end-of-LOB: live brokers answer a 128 KiB request with
//     ~80 KiB (observed 81908 bytes on 11.4). Only a 0-byte reply ends
//     the LOB; the client loops, exactly like JDBC CUBRIDBlob.getBytes.
//   - Locators stay readable within the inserting transaction and after
//     its COMMIT (the row references the same lob file).
//   - Read-after-ROLLBACK of an uncommitted NEW_LOB: the server deletes
//     the lob file and READ_LOB fails with a server error (-1020,
//     external file not found), surfaced as *cubrid.Error, no crash,
//     and the connection stays usable.
//   - Sizing: the dev link to the lab runs at ~0.5s RTT and ~5-25 KiB/s
//     bulk throughput (one 128 KiB WRITE_LOB chunk takes 20-40s), so
//     multi-MiB fixtures cannot fit any budget from here. The always-on
//     sizes top out at 200 KiB (which crosses both the 128 KiB request
//     cap and the ~80 KiB reply cap); CUBRID_TEST_LOB_BIG_MIB=<n>
//     enables an additional n-MiB round-trip, run it from a host
//     where the brokers are
//     LAN-local. Verified that way at 16 MiB on all five versions.
func TestIntegrationLobRoundTrip(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		tbl := uniqName("go_lob")
		dropTableCleanup(t, cfg, tbl)
		if _, err := conn.Exec(ctx,
			fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, b BLOB, c CLOB)", tbl)); err != nil {
			t.Fatal(err)
		}

		// LOB locators are transaction-scoped: do all LOB work with
		// autocommit off and explicit commits.
		conn.SetAutoCommit(false)

		t.Run("BlobSizes", func(t *testing.T) {
			// 0, 1 KiB, 200 KiB (crosses the chunk boundary); the
			// multi-MiB case is opt-in, see the sizing note above.
			sizes := []int{0, 1 << 10, 200 << 10}
			if mib, _ := strconv.Atoi(os.Getenv("CUBRID_TEST_LOB_BIG_MIB")); mib > 0 {
				sizes = append(sizes, mib<<20)
			} else {
				t.Log("CUBRID_TEST_LOB_BIG_MIB unset; skipping the multi-MiB round-trip")
			}
			for i, size := range sizes {
				id := int32(i)
				data := lobTestData(size)
				blob, err := conn.NewBlob(ctx)
				if err != nil {
					t.Fatalf("size %d: NewBlob: %v", size, err)
				}
				n, err := blob.Append(ctx, data)
				if err != nil || n != size {
					t.Fatalf("size %d: Append = %d, %v", size, n, err)
				}
				if blob.Size() != int64(size) {
					t.Fatalf("size %d: Size = %d", size, blob.Size())
				}
				if _, err := conn.Exec(ctx,
					fmt.Sprintf("INSERT INTO %s VALUES (?, ?, NULL)", tbl), id, blob); err != nil {
					t.Fatalf("size %d: insert: %v", size, err)
				}
				if err := conn.Commit(ctx); err != nil {
					t.Fatal(err)
				}

				lob := fetchLob(ctx, t, conn, tbl, "b", id)
				if lob == nil || lob.IsClob() {
					t.Fatalf("size %d: lob = %+v", size, lob)
				}
				if lob.Size() != int64(size) {
					t.Fatalf("size %d: result Size = %d", size, lob.Size())
				}
				got, err := lob.ReadAll(ctx)
				if err != nil {
					t.Fatalf("size %d: ReadAll: %v", size, err)
				}
				if !bytes.Equal(got, data) {
					t.Fatalf("size %d: read back %d bytes, mismatch", size, len(got))
				}
				if err := conn.Commit(ctx); err != nil { // end the read tx
					t.Fatal(err)
				}
			}
		})

		t.Run("ClobMultibyteAcrossChunk", func(t *testing.T) {
			// 3-byte runes: 128 KiB % 3 == 2, so the chunk boundary is
			// guaranteed to split a rune. Byte-level transfers must not
			// care; String() must still reassemble valid UTF-8.
			text := strings.Repeat("한", (lobChunk+30)/3)
			clob, err := conn.NewClob(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := clob.Append(ctx, []byte(text)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx,
				fmt.Sprintf("INSERT INTO %s VALUES (100, NULL, ?)", tbl), clob); err != nil {
				t.Fatal(err)
			}
			if err := conn.Commit(ctx); err != nil {
				t.Fatal(err)
			}

			lob := fetchLob(ctx, t, conn, tbl, "c", 100)
			if lob == nil || !lob.IsClob() {
				t.Fatalf("lob = %+v", lob)
			}
			got, err := lob.String(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got != text {
				t.Fatalf("CLOB mismatch: %d bytes vs %d", len(got), len(text))
			}
			if err := conn.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("NullLobColumns", func(t *testing.T) {
			if _, err := conn.Exec(ctx,
				fmt.Sprintf("INSERT INTO %s VALUES (200, NULL, NULL)", tbl)); err != nil {
				t.Fatal(err)
			}
			if err := conn.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if lob := fetchLob(ctx, t, conn, tbl, "b", 200); lob != nil {
				t.Fatalf("NULL BLOB = %+v", lob)
			}
			if lob := fetchLob(ctx, t, conn, tbl, "c", 200); lob != nil {
				t.Fatalf("NULL CLOB = %+v", lob)
			}
			if err := conn.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("ReadAfterRollback", func(t *testing.T) {
			blob, err := conn.NewBlob(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := blob.Append(ctx, []byte("doomed")); err != nil {
				t.Fatal(err)
			}
			if err := conn.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			// The locator's lob file is gone; the read must surface a
			// server error (not crash, not silently succeed).
			p := make([]byte, 6)
			if _, err := blob.ReadAt(ctx, p, 0); err == nil {
				t.Fatal("ReadAt after rollback succeeded; want an error")
			} else {
				var ce *cubrid.Error
				if !errors.As(err, &ce) {
					t.Fatalf("ReadAt after rollback: %v (want *cubrid.Error)", err)
				}
				t.Logf("read-after-rollback error: %v", err)
			}
			// The connection itself must remain usable.
			if err := conn.Ping(ctx); err != nil {
				t.Fatalf("conn unusable after LOB error: %v", err)
			}
		})
	})
}
