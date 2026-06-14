//go:build integration

package cubrid_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	cubrid "github.com/hexacluster/gocubrid/cubrid"
)

// Live findings across the whole matrix (9.3.9 to 11.4.5):
//   - The GET_SCHEMA_INFO response is [handle][total tuples][col count]
//     followed by the SHORT column form (type, scale, precision, name
//     only), confirmed on all five versions.
//   - PK KEY_SEQ arrives 1-BASED on every version (plan expected
//     0-based); the driver exposes the wire value unchanged.
//   - 9.3 sends 2-col CLASS and 13-col ATTRIBUTE rows; 10.1+ append a
//     REMARKS column to both. 9.3 sends NULL for column defaults where
//     10.1+ send the literal string "NULL".
//   - 9.3 sends the plain single-byte DOMAIN (e.g. 8 = INT); 10.1+ pack
//     the extended two-byte type form into the short (-32760 = 0x8008).
//   - Only 11.4 returns owner-qualified class names ("dba.t"); the
//     matched-argument side of FK rows stays unqualified even there.
//   - SCH_CONSTRAINT rows carry 8 columns on every version: TYPE
//     (short), NAME, ATTR_NAME, NUM_PAGES (int), NUM_KEYS (int),
//     PRIMARY_KEY (short), KEY_ORDER (short), ASC_DESC (varchar),
//     reachable through the low-level SchemaInfo API.
//   - The PK's backing index never appears in SCH_CONSTRAINT (any
//     version, any pattern-flag combination), and PRIMARY_KEY is 0 on
//     every row that does appear, Conn.Indexes therefore merges
//     SCH_PRIMARY_KEY to synthesize the PK index. Secondary-index TYPE
//     values confirmed live: 0=UNIQUE, 1=INDEX.
//   - SCH_PRIMARY_KEY really does pattern-match: with the pattern flags
//     set, a sibling class aligned on the '_' positions of the
//     requested name comes back in the same answer (CLASS_NAME
//     disambiguates), Conn.Indexes filters its merge to the requested
//     table (the sibling fixture below pins this on every version).
func TestIntegrationSchemaIntrospection(t *testing.T) {
	forEachDSN(t, func(t *testing.T, cfg *cubrid.Config, _ caps) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		conn, err := cubrid.Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		parent := uniqName("go_skp")
		child := uniqName("go_skc")
		// Parent cleanup first: t.Cleanup is LIFO, so the FK-bearing child
		// drops before the parent (the parent cannot drop while referenced).
		dropTableCleanup(t, cfg, parent)
		dropTableCleanup(t, cfg, child)
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INT NOT NULL, name VARCHAR(64) DEFAULT 'x', CONSTRAINT pk_%s PRIMARY KEY (id))",
			parent, parent)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s (a INT NOT NULL, b INT NOT NULL, pid INT,"+
				" CONSTRAINT pk_%s PRIMARY KEY (a, b),"+
				" CONSTRAINT fk_%s FOREIGN KEY (pid) REFERENCES %s (id) ON DELETE CASCADE)",
			child, child, child, parent)); err != nil {
			t.Fatal(err)
		}

		t.Run("Tables", func(t *testing.T) {
			tables, err := conn.Tables(ctx)
			if err != nil {
				t.Fatal(err)
			}
			found := map[string]int32{}
			for _, tb := range tables {
				found[tb.Name] = tb.Type
			}
			if typ, ok := found[parent]; !ok || typ != 2 {
				t.Fatalf("parent %s: type=%d found=%v (of %d tables)", parent, typ, ok, len(tables))
			}
			if typ, ok := found[child]; !ok || typ != 2 {
				t.Fatalf("child %s: type=%d found=%v", child, typ, ok)
			}
		})

		t.Run("Columns", func(t *testing.T) {
			cols, err := conn.Columns(ctx, child)
			if err != nil {
				t.Fatal(err)
			}
			if len(cols) != 3 {
				t.Fatalf("columns = %+v", cols)
			}
			for i, want := range []struct {
				name           string
				notNull, isKey bool
			}{{"a", true, true}, {"b", true, true}, {"pid", false, false}} {
				c := cols[i]
				if c.Name != want.name || c.NotNull != want.notNull || c.IsKey != want.isKey {
					t.Fatalf("column %d = %+v, want %+v", i, c, want)
				}
				if c.TypeCode != 8 { // protocol type INT
					t.Fatalf("column %d type = %d, want 8 (INT)", i, c.TypeCode)
				}
				if c.Order != int32(i+1) {
					t.Fatalf("column %d order = %d", i, c.Order)
				}
				if c.Table != child {
					t.Fatalf("column %d table = %q, want %q", i, c.Table, child)
				}
				if c.Precision != 10 { // INT precision as reported live on all versions
					t.Fatalf("column %d precision = %d", i, c.Precision)
				}
			}

			pcols, err := conn.Columns(ctx, parent)
			if err != nil {
				t.Fatal(err)
			}
			if len(pcols) != 2 || pcols[1].Name != "name" {
				t.Fatalf("parent columns = %+v", pcols)
			}
			// VARCHAR(64) DEFAULT 'x': precision tracks the declared
			// length and the default must surface (rendering varies by
			// version: 'x' vs x), never the no-default markers.
			if pcols[1].TypeCode != 2 { // protocol type VARCHAR
				t.Fatalf("name type = %d, want 2 (VARCHAR)", pcols[1].TypeCode)
			}
			if pcols[1].Precision != 64 {
				t.Fatalf("name precision = %d", pcols[1].Precision)
			}
			if d := pcols[1].Default; d == "" || d == "NULL" {
				t.Fatalf("name default = %q, want the declared 'x'", d)
			}
			if pcols[1].NotNull {
				t.Fatalf("name must be nullable: %+v", pcols[1])
			}
		})

		t.Run("PrimaryKey", func(t *testing.T) {
			pk, err := conn.PrimaryKey(ctx, child)
			if err != nil {
				t.Fatal(err)
			}
			want := []cubrid.KeyColumn{
				{Table: child, Column: "a", KeyName: "pk_" + child, Seq: 1},
				{Table: child, Column: "b", KeyName: "pk_" + child, Seq: 2},
			}
			if len(pk) != 2 || pk[0] != want[0] || pk[1] != want[1] {
				t.Fatalf("pk = %+v\nwant %+v", pk, want)
			}
		})

		wantFK := cubrid.ForeignKey{
			PKTable: parent, PKColumn: "id", FKTable: child, FKColumn: "pid",
			FKName: "fk_" + child, PKName: "pk_" + parent,
			Seq:        1,
			UpdateRule: 1, // RESTRICT (CUBRID default)
			DeleteRule: 0, // CASCADE per the fixture DDL
		}

		t.Run("ImportedKeys", func(t *testing.T) {
			fks, err := conn.ImportedKeys(ctx, child)
			if err != nil {
				t.Fatal(err)
			}
			if len(fks) != 1 || fks[0] != wantFK {
				t.Fatalf("imported = %+v\nwant       %+v", fks, wantFK)
			}
		})

		t.Run("ExportedKeys", func(t *testing.T) {
			fks, err := conn.ExportedKeys(ctx, parent)
			if err != nil {
				t.Fatal(err)
			}
			if len(fks) != 1 || fks[0] != wantFK {
				t.Fatalf("exported = %+v\nwant       %+v", fks, wantFK)
			}
		})

		t.Run("Indexes", func(t *testing.T) {
			tbl := uniqName("go_six")
			dropTableCleanup(t, cfg, tbl)
			if _, err := conn.Exec(ctx, fmt.Sprintf(
				"CREATE TABLE %s (id INT NOT NULL, u INT, c1 INT, c2 INT,"+
					" CONSTRAINT pk_%s PRIMARY KEY (id))", tbl, tbl)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, fmt.Sprintf(
				"CREATE UNIQUE INDEX u_%s ON %s (u)", tbl, tbl)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, fmt.Sprintf(
				"CREATE INDEX i_%s ON %s (c1, c2)", tbl, tbl)); err != nil {
				t.Fatal(err)
			}
			// Pattern-collision regression: the SCH_PRIMARY_KEY merge
			// request is pattern-flagged, where '_' matches any single
			// character, a sibling table aligned on tbl's underscore
			// positions (each '_' after the swept "go_" prefix becomes
			// 'x') must not contaminate the merged PK index below.
			sibling := "go_" + strings.ReplaceAll(tbl[3:], "_", "x")
			dropTableCleanup(t, cfg, sibling)
			if _, err := conn.Exec(ctx, fmt.Sprintf(
				"CREATE TABLE %s (a INT NOT NULL, b INT NOT NULL,"+
					" CONSTRAINT pk_%s PRIMARY KEY (a, b))", sibling, sibling)); err != nil {
				t.Fatal(err)
			}

			idx, err := conn.Indexes(ctx, tbl)
			if err != nil {
				t.Fatal(err)
			}
			if len(idx) != 3 {
				t.Fatalf("indexes = %+v, want exactly 3", idx)
			}
			byName := map[string]cubrid.IndexInfo{}
			for _, ix := range idx {
				byName[ix.Name] = ix
			}
			for _, want := range []cubrid.IndexInfo{
				{Name: "pk_" + tbl, Unique: true, Columns: []string{"id"}, IsPrimary: true},
				{Name: "u_" + tbl, Unique: true, Columns: []string{"u"}},
				{Name: "i_" + tbl, Unique: false, Columns: []string{"c1", "c2"}},
			} {
				got, ok := byName[want.Name]
				if !ok {
					t.Fatalf("index %s missing: %+v", want.Name, idx)
				}
				if got.Unique != want.Unique || got.IsPrimary != want.IsPrimary {
					t.Fatalf("index %s = %+v, want %+v", want.Name, got, want)
				}
				if len(got.Columns) != len(want.Columns) {
					t.Fatalf("index %s columns = %v, want %v", want.Name, got.Columns, want.Columns)
				}
				for i := range want.Columns {
					if got.Columns[i] != want.Columns[i] {
						t.Fatalf("index %s columns = %v, want %v (key order matters)",
							want.Name, got.Columns, want.Columns)
					}
				}
			}
		})

		// Low-level API: raw rows keep whatever qualification the server
		// sent, and the statement-shaped response (short column metadata)
		// must expose usable column names.
		t.Run("RawSchemaInfo", func(t *testing.T) {
			rows, err := conn.SchemaInfo(ctx, cubrid.SchemaPrimaryKey, &child, nil, true, true)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			cols := rows.Columns()
			if len(cols) != 4 || cols[2].Name != "KEY_SEQ" {
				t.Fatalf("raw pk columns = %+v", cols)
			}
			n := 0
			for {
				row, err := rows.Next()
				if err != nil {
					break
				}
				if len(row) != 4 {
					t.Fatalf("raw pk row = %#v", row)
				}
				n++
			}
			if n != 2 {
				t.Fatalf("raw pk rows = %d, want 2", n)
			}
		})
	})
}
