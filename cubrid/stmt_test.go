package cubrid

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/hexacluster/gocubrid/internal/prototest"
)

// prepareHeader builds the fixed prefix of a PREPARE response (handle 33,
// SELECT, 1 param) up to and including the given column count.
func prepareHeader(nCols int32) *prototest.RespWriter {
	p := &prototest.RespWriter{}
	p.I32(33)    // statement handle
	p.I32(0)     // result cache lifetime
	p.B(21)      // command type: SELECT
	p.I32(1)     // parameter count
	p.B(0)       // updatable
	p.I32(nCols) // column count
	return p
}

// preparePayload builds a PREPARE response: handle 33, SELECT, 1 param,
// one INT column named "id" from table "t".
func preparePayload() []byte {
	p := prepareHeader(1)
	p.B(8)      // type: INT (legacy single-byte form)
	p.I16(0)    // scale
	p.I32(10)   // precision
	p.Str("id") // column label
	p.Str("id") // attribute name
	p.Str("t")  // class (table) name
	p.B(0)      // 0 = nullable
	p.Str("")   // default value
	p.B(1)      // auto increment
	p.B(0)      // unique key
	p.B(0)      // primary key
	p.B(0)      // reverse index
	p.B(0)      // reverse unique
	p.B(0)      // foreign key
	p.B(0)      // shared
	return p.Bytes()
}

func TestPrepareParsesMetadata(t *testing.T) {
	fb, conn := fakeConn(t)
	fb.Queue(2, preparePayload())
	stmt, err := conn.Prepare(context.Background(), "SELECT id FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	if stmt.ParamCount() != 1 {
		t.Fatalf("ParamCount = %d", stmt.ParamCount())
	}
	cols := stmt.Columns()
	if len(cols) != 1 {
		t.Fatalf("columns = %d", len(cols))
	}
	c := cols[0]
	if c.Name != "id" || c.TypeCode != 8 || c.TableName != "t" ||
		!c.Nullable || !c.AutoIncrement || c.Precision != 10 {
		t.Fatalf("column = %+v", c)
	}
	if err := stmt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestPrepareRejectsBadParamCount: the parameter count is validated for
// every statement type (the CALL-specific allocations are exercised in
// TestPrepareCallRejectsBadParamCount), so a corrupt count on a plain
// SELECT also surfaces as a parse error rather than poisoning the Stmt.
func TestPrepareRejectsBadParamCount(t *testing.T) {
	p := &prototest.RespWriter{}
	p.I32(33) // statement handle
	p.I32(0)  // result cache lifetime
	p.B(21)   // command type: SELECT
	p.I32(-1) // parameter count: corrupt
	p.B(0)    // updatable
	p.I32(0)  // column count
	fb, conn := fakeConn(t)
	fb.Queue(2, p.Bytes())
	stmt, err := conn.Prepare(context.Background(), "SELECT id FROM t WHERE id = ?")
	if err == nil {
		t.Fatalf("Prepare accepted parameter count -1: %+v", stmt)
	}
	if !strings.Contains(err.Error(), "parameter count") {
		t.Fatalf("error %q does not mention parameter count", err)
	}
}

// TestPrepareRejectsBadColumnCount feeds hostile column counts: a negative
// count must not panic (makeslice: cap out of range) and a huge count must
// not pre-allocate gigabytes; both must surface as parse errors.
func TestPrepareRejectsBadColumnCount(t *testing.T) {
	for _, tc := range []struct {
		name  string
		nCols int32
	}{
		{"negative", -1},
		{"most negative", math.MinInt32},
		{"huge", math.MaxInt32},
		{"exceeds payload", 2}, // header carries data for at most 0 columns
	} {
		t.Run(tc.name, func(t *testing.T) {
			fb, conn := fakeConn(t)
			fb.Queue(2, prepareHeader(tc.nCols).Bytes())
			stmt, err := conn.Prepare(context.Background(), "SELECT id FROM t")
			if err == nil {
				t.Fatalf("Prepare accepted column count %d: %+v", tc.nCols, stmt)
			}
			if !strings.Contains(err.Error(), "column count") {
				t.Fatalf("error %q does not mention column count", err)
			}
		})
	}
}

// TestColumnTypeName: the CUBRID type name for column metadata, used by
// the adapter's ColumnTypeDatabaseTypeName (collection kinds win over the
// demoted element TypeCode).
func TestColumnTypeName(t *testing.T) {
	for _, tc := range []struct {
		code, coll byte
		want       string
	}{
		{1, 0, "CHAR"},
		{2, 0, "VARCHAR"},
		{4, 0, "NCHAR VARYING"},
		{6, 0, "BIT VARYING"},
		{7, 0, "NUMERIC"},
		{8, 0, "INTEGER"},
		{9, 0, "SMALLINT"},
		{10, 0, "MONETARY"},
		{11, 0, "FLOAT"},
		{12, 0, "DOUBLE"},
		{13, 0, "DATE"},
		{14, 0, "TIME"},
		{15, 0, "TIMESTAMP"},
		{21, 0, "BIGINT"},
		{22, 0, "DATETIME"},
		{23, 0, "BLOB"},
		{24, 0, "CLOB"},
		{25, 0, "ENUM"},
		{29, 0, "TIMESTAMPTZ"},
		{31, 0, "DATETIMETZ"},
		{34, 0, "JSON"},
		{8, 1, "SET"},      // SET(INT): collection flag wins
		{2, 2, "MULTISET"}, // MULTISET(VARCHAR)
		{8, 3, "SEQUENCE"}, // SEQUENCE(INT)
		{0, 0, "NULL"},
		{99, 0, ""}, // unknown code: empty per RowsColumnTypeDatabaseTypeName contract
	} {
		c := Column{TypeCode: tc.code, Collection: tc.coll}
		if got := c.TypeName(); got != tc.want {
			t.Errorf("TypeName(code=%d, coll=%d) = %q, want %q", tc.code, tc.coll, got, tc.want)
		}
	}
}
