package cubrid

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
	"github.com/hexacluster/gocubrid/internal/prototest"
)

// schemaCol is one short-form column description for schemaInfoPayload.
type schemaCol struct {
	typ  byte
	name string
}

// schemaInfoPayload builds a GET_SCHEMA_INFO response: server handle,
// total tuple count, column count, then per column the short metadata
// form (type byte, scale, precision, name), no attr/class/default
// fields, per JDBC UStatement.readColumnInfo for non-NORMAL statements.
func schemaInfoPayload(handle, total int32, cols ...schemaCol) []byte {
	p := &prototest.RespWriter{}
	p.I32(handle)
	p.I32(total)
	p.I32(int32(len(cols)))
	for _, c := range cols {
		p.B(c.typ)
		p.I16(0)  // scale
		p.I32(64) // precision
		p.Str(c.name)
	}
	return p.Bytes()
}

// fval writes one length-prefixed column value into a fetch tuple.
type fval func(p *prototest.RespWriter)

func vStr(s string) fval    { return func(p *prototest.RespWriter) { p.Str(s) } }
func vInt(v int32) fval     { return func(p *prototest.RespWriter) { p.I32(4); p.I32(v) } }
func vShort(v int16) fval   { return func(p *prototest.RespWriter) { p.I32(2); p.I16(v) } }
func vNull() fval           { return func(p *prototest.RespWriter) { p.I32(-1) } }
func row(vs ...fval) []fval { return vs }

// fetchPayload builds a FETCH response: status, tuple count, then per
// tuple the cursor index, an 8-byte OID slot, and the column values,
// with the trailing fetch-completed byte (V5+).
func fetchPayload(rows ...[]fval) []byte {
	p := &prototest.RespWriter{}
	p.I32(0)
	p.I32(int32(len(rows)))
	for i, r := range rows {
		p.I32(int32(i + 1))
		p.Raw(make([]byte, 8))
		for _, v := range r {
			v(p)
		}
	}
	p.B(1)
	return p.Bytes()
}

func TestSchemaInfoRequestAndRows(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 2,
		schemaCol{protocol.TypeVarchar, "NAME"},
		schemaCol{protocol.TypeShort, "TYPE"}))
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vStr("t1"), vShort(2)),
		row(vStr("v1"), vShort(1))))

	pat := "go_%"
	rows, err := conn.SchemaInfo(context.Background(), SchemaClass, &pat, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}

	// The request must carry fn 9, schema type, the args, the pattern
	// flag, and the V12 shard id.
	w := protocol.NewWriter()
	protocol.SchemaInfoRequest(w, protocol.SchClass, &pat, nil, protocol.SchemaFlagArg1Pattern, protocol.Version(12))
	reqs := fb.Requests()
	if len(reqs) == 0 || !bytes.Equal(reqs[0], w.Bytes()) {
		t.Fatalf("request = % X\nwant      % X", reqs[0], w.Bytes())
	}

	r1, err := rows.Next()
	if err != nil {
		t.Fatal(err)
	}
	if r1[0] != "t1" || r1[1] != int16(2) {
		t.Fatalf("row 1 = %#v", r1)
	}
	if _, err := rows.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := rows.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}

	// Closing schema rows must release the server-side handle.
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	reqs = fb.Requests()
	last := reqs[len(reqs)-1]
	if last[0] != protocol.FnCloseStatement {
		t.Fatalf("last request fn = %d, want CLOSE_STATEMENT", last[0])
	}
}

func TestSchemaInfoZeroRows(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 0,
		schemaCol{protocol.TypeVarchar, "NAME"},
		schemaCol{protocol.TypeShort, "TYPE"}))

	rows, err := conn.SchemaInfo(context.Background(), SchemaClass, nil, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if _, err := rows.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF without a FETCH round-trip, got %v", err)
	}
}

func TestTablesSplitsOwnerQualifiedNames(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 2,
		schemaCol{protocol.TypeVarchar, "NAME"},
		schemaCol{protocol.TypeShort, "TYPE"}))
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vStr("dba.go_a"), vShort(2)), // 11.x sends owner-qualified names
		row(vStr("go_b"), vShort(1))))

	tables, err := conn.Tables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []TableInfo{{Name: "go_a", Type: 2}, {Name: "go_b", Type: 1}}
	if len(tables) != len(want) || tables[0] != want[0] || tables[1] != want[1] {
		t.Fatalf("tables = %#v", tables)
	}
}

// TestTablesToleratesRemarksColumn: newer servers append a remarks column
// to the CLASS family; the helper must not reject the wider row.
func TestTablesToleratesRemarksColumn(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 1,
		schemaCol{protocol.TypeVarchar, "NAME"},
		schemaCol{protocol.TypeShort, "TYPE"},
		schemaCol{protocol.TypeVarchar, "REMARKS"}))
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vStr("t"), vShort(0), vStr("a comment"))))

	tables, err := conn.Tables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 || tables[0].Name != "t" || tables[0].Type != 0 {
		t.Fatalf("tables = %#v", tables)
	}
}

// attributeCols is the 13-column SCH_ATTRIBUTE metadata (without remarks).
func attributeCols(extra ...schemaCol) []schemaCol {
	cols := []schemaCol{
		{protocol.TypeVarchar, "ATTR_NAME"},
		{protocol.TypeShort, "DOMAIN"},
		{protocol.TypeInt, "SCALE"},
		{protocol.TypeInt, "PRECISION"},
		{protocol.TypeShort, "INDEXED"},
		{protocol.TypeShort, "NON_NULL"},
		{protocol.TypeShort, "SHARED"},
		{protocol.TypeShort, "UNIQUE"},
		{protocol.TypeVarchar, "DEFAULT"},
		{protocol.TypeInt, "ATTR_ORDER"},
		{protocol.TypeVarchar, "CLASS_NAME"},
		{protocol.TypeVarchar, "SOURCE_CLASS"},
		{protocol.TypeShort, "IS_KEY"},
	}
	return append(cols, extra...)
}

func TestColumnsMapsAttributeRows(t *testing.T) {
	fb, conn := fakeConn(t)
	// Extended two-byte type form packed into the short's high byte
	// (JDBC confirmSchemaTypeInfo): 0x81<<8 | 22 -> DATETIME.
	extDatetime := uint16(0x8100 | protocol.TypeDatetime)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 2, attributeCols()...))
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vStr("id"), vShort(protocol.TypeInt), vInt(0), vInt(10),
			vShort(1), vShort(1), vShort(0), vShort(1),
			vNull(), vInt(1), vStr("dba.go_t"), vStr("go_t"), vShort(1)),
		row(vStr("ts"), vShort(int16(extDatetime)), vInt(0), vInt(23),
			vShort(0), vShort(0), vShort(0), vShort(0),
			vStr("CURRENT_TIMESTAMP"), vInt(2), vStr("dba.go_t"), vStr("go_t"), vShort(0))))

	cols, err := conn.Columns(context.Background(), "go_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 {
		t.Fatalf("columns = %#v", cols)
	}
	want0 := ColumnInfo{Name: "id", TypeCode: protocol.TypeInt, Scale: 0, Precision: 10,
		NotNull: true, Unique: true, IsKey: true, Default: "", Order: 1, Table: "go_t"}
	if cols[0] != want0 {
		t.Fatalf("col 0 = %#v\nwant    %#v", cols[0], want0)
	}
	want1 := ColumnInfo{Name: "ts", TypeCode: protocol.TypeDatetime, Scale: 0, Precision: 23,
		NotNull: false, Unique: false, IsKey: false, Default: "CURRENT_TIMESTAMP", Order: 2, Table: "go_t"}
	if cols[1] != want1 {
		t.Fatalf("col 1 = %#v\nwant    %#v", cols[1], want1)
	}
}

func TestPrimaryKeyMapsRows(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 2,
		schemaCol{protocol.TypeVarchar, "CLASS_NAME"},
		schemaCol{protocol.TypeVarchar, "ATTR_NAME"},
		schemaCol{protocol.TypeInt, "KEY_SEQ"},
		schemaCol{protocol.TypeVarchar, "KEY_NAME"}))
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vStr("dba.go_t"), vStr("a"), vInt(1), vStr("pk_go_t")),
		row(vStr("dba.go_t"), vStr("b"), vInt(2), vStr("pk_go_t"))))

	pk, err := conn.PrimaryKey(context.Background(), "go_t")
	if err != nil {
		t.Fatal(err)
	}
	want := []KeyColumn{
		{Table: "go_t", Column: "a", KeyName: "pk_go_t", Seq: 1},
		{Table: "go_t", Column: "b", KeyName: "pk_go_t", Seq: 2},
	}
	if len(pk) != 2 || pk[0] != want[0] || pk[1] != want[1] {
		t.Fatalf("pk = %#v", pk)
	}
}

func TestImportedKeysMapsRows(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 1,
		schemaCol{protocol.TypeVarchar, "PKTABLE_NAME"},
		schemaCol{protocol.TypeVarchar, "PKCOLUMN_NAME"},
		schemaCol{protocol.TypeVarchar, "FKTABLE_NAME"},
		schemaCol{protocol.TypeVarchar, "FKCOLUMN_NAME"},
		schemaCol{protocol.TypeShort, "KEY_SEQ"},
		schemaCol{protocol.TypeShort, "UPDATE_RULE"},
		schemaCol{protocol.TypeShort, "DELETE_RULE"},
		schemaCol{protocol.TypeVarchar, "FK_NAME"},
		schemaCol{protocol.TypeVarchar, "PK_NAME"}))
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vStr("dba.go_p"), vStr("id"), vStr("dba.go_c"), vStr("pid"),
			vShort(1), vShort(1), vShort(0), vStr("fk_c_p"), vStr("pk_p"))))

	fks, err := conn.ImportedKeys(context.Background(), "go_c")
	if err != nil {
		t.Fatal(err)
	}
	want := ForeignKey{PKTable: "go_p", PKColumn: "id", FKTable: "go_c", FKColumn: "pid",
		FKName: "fk_c_p", PKName: "pk_p", Seq: 1, UpdateRule: 1, DeleteRule: 0}
	if len(fks) != 1 || fks[0] != want {
		t.Fatalf("fks = %#v", fks)
	}
}

// constraintCols is the 8-column SCH_CONSTRAINT metadata, the shape every
// matrix broker (9.3.9 to 11.4.5) sends (asserted live in Plan 3).
func constraintCols() []schemaCol {
	return []schemaCol{
		{protocol.TypeShort, "TYPE"},
		{protocol.TypeVarchar, "NAME"},
		{protocol.TypeVarchar, "ATTR_NAME"},
		{protocol.TypeInt, "NUM_PAGES"},
		{protocol.TypeInt, "NUM_KEYS"},
		{protocol.TypeShort, "PRIMARY_KEY"},
		{protocol.TypeShort, "KEY_ORDER"},
		{protocol.TypeVarchar, "ASC_DESC"},
	}
}

// conRow builds one SCH_CONSTRAINT fetch tuple.
func conRow(typ int16, name, attr string, primary int16, order int16) []fval {
	return row(vShort(typ), vStr(name), vStr(attr), vInt(1), vInt(1),
		vShort(primary), vShort(order), vStr("ASC"))
}

// pkCols is the 4-column SCH_PRIMARY_KEY metadata.
func pkCols() []schemaCol {
	return []schemaCol{
		{protocol.TypeVarchar, "CLASS_NAME"},
		{protocol.TypeVarchar, "ATTR_NAME"},
		{protocol.TypeInt, "KEY_SEQ"},
		{protocol.TypeVarchar, "KEY_NAME"},
	}
}

func TestIndexesMergesConstraintAndPrimaryKey(t *testing.T) {
	fb, conn := fakeConn(t)
	// Live shape on every matrix version: the PK's backing index is
	// absent from SCH_CONSTRAINT and PRIMARY_KEY is 0 on every row.
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 4, constraintCols()...))
	fb.Queue(protocol.FnFetch, fetchPayload(
		conRow(0, "u_go_t", "b", 0, 1),   // plain unique index
		conRow(1, "i_go_t", "c1", 0, 1),  // composite non-unique index,
		conRow(1, "i_go_t", "c2", 0, 2),  // two columns in key order
		conRow(2, "ru_go_t", "d", 0, 1))) // reverse unique index
	// PK rows deliberately out of key order: the merge must sort by Seq.
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(8, 2, pkCols()...))
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vStr("dba.go_t"), vStr("a2"), vInt(2), vStr("pk_go_t")),
		row(vStr("dba.go_t"), vStr("a1"), vInt(1), vStr("pk_go_t"))))

	idx, err := conn.Indexes(context.Background(), "go_t")
	if err != nil {
		t.Fatal(err)
	}

	// First request: SCH_CONSTRAINT with the exact (non-pattern) table
	// name, index fixtures use names with '_', which is a SQL wildcard
	// under pattern matching. Then the SCH_PRIMARY_KEY merge request.
	tbl := "go_t"
	w := protocol.NewWriter()
	protocol.SchemaInfoRequest(w, protocol.SchConstraint, &tbl, nil, 0, protocol.Version(12))
	reqs := fb.Requests()
	if len(reqs) == 0 || !bytes.Equal(reqs[0], w.Bytes()) {
		t.Fatalf("request = % X\nwant      % X", reqs[0], w.Bytes())
	}
	w = protocol.NewWriter()
	protocol.SchemaInfoRequest(w, protocol.SchPrimaryKey, &tbl, nil,
		protocol.SchemaFlagArg1Pattern|protocol.SchemaFlagArg2Pattern, protocol.Version(12))
	var pkReq []byte
	for _, r := range reqs[1:] {
		if len(r) > 0 && r[0] == protocol.FnGetSchemaInfo {
			pkReq = r
		}
	}
	if !bytes.Equal(pkReq, w.Bytes()) {
		t.Fatalf("pk request = % X\nwant         % X", pkReq, w.Bytes())
	}

	want := []IndexInfo{
		{Name: "pk_go_t", Unique: true, Columns: []string{"a1", "a2"}, IsPrimary: true},
		{Name: "u_go_t", Unique: true, Columns: []string{"b"}},
		{Name: "i_go_t", Unique: false, Columns: []string{"c1", "c2"}},
		{Name: "ru_go_t", Unique: true, Columns: []string{"d"}},
	}
	assertIndexes(t, idx, want)
}

func assertIndexes(t *testing.T, got, want []IndexInfo) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("indexes = %#v\nwant      %#v", got, want)
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Name != w.Name || g.Unique != w.Unique || g.IsPrimary != w.IsPrimary {
			t.Fatalf("index %d = %#v\nwant     %#v", i, g, w)
		}
		if len(g.Columns) != len(w.Columns) {
			t.Fatalf("index %d columns = %#v, want %#v", i, g.Columns, w.Columns)
		}
		for j := range w.Columns {
			if g.Columns[j] != w.Columns[j] {
				t.Fatalf("index %d columns = %#v, want %#v", i, g.Columns, w.Columns)
			}
		}
	}
}

// TestIndexesFiltersPatternMatchedSiblingPK: the SCH_PRIMARY_KEY merge
// request is pattern-flagged ('_' in the table name is a SQL wildcard
// there), so the answer can include sibling classes whose names align
// on the underscore positions (goxt for go_t). The merge must keep only
// rows for the requested table, without the filter, the seq-sorted
// merge interleaves both tables' key columns and can even name the PK
// index after the wrong table.
func TestIndexesFiltersPatternMatchedSiblingPK(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 1, constraintCols()...))
	fb.Queue(protocol.FnFetch, fetchPayload(
		conRow(1, "i_go_t", "c", 0, 1)))
	// The sibling's seq-1 row arrives before the target's: the broken
	// merge picked pk_goxt as the index name and produced [a id b].
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(8, 3, pkCols()...))
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vStr("dba.goxt"), vStr("a"), vInt(1), vStr("pk_goxt")),
		row(vStr("dba.go_t"), vStr("id"), vInt(1), vStr("pk_go_t")),
		row(vStr("dba.goxt"), vStr("b"), vInt(2), vStr("pk_goxt"))))

	idx, err := conn.Indexes(context.Background(), "go_t")
	if err != nil {
		t.Fatal(err)
	}
	want := []IndexInfo{
		{Name: "pk_go_t", Unique: true, Columns: []string{"id"}, IsPrimary: true},
		{Name: "i_go_t", Columns: []string{"c"}},
	}
	assertIndexes(t, idx, want)
}

// TestIndexesSortsColumnsByKeyOrder: within-index column order follows
// the KEY_ORDER column (1-based, live-confirmed on the whole matrix),
// not server emission order, a server emitting composite-index rows
// out of key order must not scramble Columns.
func TestIndexesSortsColumnsByKeyOrder(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 2, constraintCols()...))
	fb.Queue(protocol.FnFetch, fetchPayload(
		conRow(1, "i_go_t", "c2", 0, 2),
		conRow(1, "i_go_t", "c1", 0, 1)))
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(8, 0, pkCols()...))

	idx, err := conn.Indexes(context.Background(), "go_t")
	if err != nil {
		t.Fatal(err)
	}
	want := []IndexInfo{
		{Name: "i_go_t", Columns: []string{"c1", "c2"}},
	}
	assertIndexes(t, idx, want)
}

// TestIndexesColumnsWithoutKeyOrder: a row too narrow for KEY_ORDER
// (six columns, the family minimum) keeps server emission order.
func TestIndexesColumnsWithoutKeyOrder(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 2, constraintCols()[:6]...))
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vShort(1), vStr("i_go_t"), vStr("x"), vInt(1), vInt(1), vShort(0)),
		row(vShort(1), vStr("i_go_t"), vStr("y"), vInt(1), vInt(1), vShort(0))))
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(8, 0, pkCols()...))

	idx, err := conn.Indexes(context.Background(), "go_t")
	if err != nil {
		t.Fatal(err)
	}
	want := []IndexInfo{
		{Name: "i_go_t", Columns: []string{"x", "y"}},
	}
	assertIndexes(t, idx, want)
}

// TestIndexesInterleavedRows: grouping is by name, not adjacency, a
// server interleaving constraint rows must not split an index in two.
// No PK on the table: the empty SCH_PRIMARY_KEY answer adds nothing.
func TestIndexesInterleavedRows(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 3, constraintCols()...))
	fb.Queue(protocol.FnFetch, fetchPayload(
		conRow(1, "i_a", "x", 0, 1),
		conRow(1, "i_b", "y", 0, 1),
		conRow(1, "i_a", "z", 0, 2)))
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(8, 0, pkCols()...))

	idx, err := conn.Indexes(context.Background(), "go_t")
	if err != nil {
		t.Fatal(err)
	}
	want := []IndexInfo{
		{Name: "i_a", Columns: []string{"x", "z"}},
		{Name: "i_b", Columns: []string{"y"}},
	}
	assertIndexes(t, idx, want)
}

// TestIndexesMarksListedPrimaryKey: defensive path, if a server ever
// does list the PK constraint in SCH_CONSTRAINT, the merge must mark
// the existing entry instead of synthesizing a duplicate.
func TestIndexesMarksListedPrimaryKey(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 2, constraintCols()...))
	fb.Queue(protocol.FnFetch, fetchPayload(
		conRow(1, "i_go_t", "x", 0, 1),
		conRow(0, "pk_go_t", "a", 1, 1)))
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(8, 1, pkCols()...))
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vStr("go_t"), vStr("a"), vInt(1), vStr("pk_go_t"))))

	idx, err := conn.Indexes(context.Background(), "go_t")
	if err != nil {
		t.Fatal(err)
	}
	want := []IndexInfo{
		{Name: "i_go_t", Columns: []string{"x"}},
		{Name: "pk_go_t", Unique: true, Columns: []string{"a"}, IsPrimary: true},
	}
	assertIndexes(t, idx, want)
}

func TestIndexesRejectsNarrowRow(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 1,
		schemaCol{protocol.TypeShort, "TYPE"},
		schemaCol{protocol.TypeVarchar, "NAME"}))
	fb.Queue(protocol.FnFetch, fetchPayload(row(vShort(0), vStr("i"))))

	if _, err := conn.Indexes(context.Background(), "go_t"); err == nil {
		t.Fatal("Indexes accepted a row narrower than the constraint family")
	}
}

// TestColumnsSelfDescribedDefault pins the live 9.3 shape: 13 attribute
// columns (no remarks), the DEFAULT column declared wire type NULL, and
// its non-null values carrying a per-value type prefix
// ([TypeVarchar]['x' NUL] for a column declared DEFAULT 'x').
func TestColumnsSelfDescribedDefault(t *testing.T) {
	fb, conn := fakeConn(t)
	cols := attributeCols()
	cols[8].typ = protocol.TypeNull // 9.3 declares DEFAULT as U_TYPE_NULL
	fb.Queue(protocol.FnGetSchemaInfo, schemaInfoPayload(7, 1, cols...))
	selfDescribed := func(p *prototest.RespWriter) {
		p.I32(3).B(protocol.TypeVarchar).Raw([]byte("x\x00"))
	}
	fb.Queue(protocol.FnFetch, fetchPayload(
		row(vStr("name"), vShort(protocol.TypeVarchar), vInt(0), vInt(64),
			vShort(0), vShort(0), vShort(0), vShort(0),
			selfDescribed, vInt(2), vStr("go_t"), vStr("go_t"), vShort(0))))

	out, err := conn.Columns(context.Background(), "go_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Default != "x" {
		t.Fatalf("columns = %#v", out)
	}
}

// TestSchemaInfoRejectsBadColumnCount mirrors the PREPARE hardening: a
// hostile column count must surface as an error, not a panic or a huge
// allocation.
func TestSchemaInfoRejectsBadColumnCount(t *testing.T) {
	fb, conn := fakeConn(t)
	p := &prototest.RespWriter{}
	p.I32(7).I32(0).I32(1 << 30)
	fb.Queue(protocol.FnGetSchemaInfo, p.Bytes())
	if _, err := conn.SchemaInfo(context.Background(), SchemaClass, nil, nil, false, false); err == nil {
		t.Fatal("SchemaInfo accepted a hostile column count")
	}
}
