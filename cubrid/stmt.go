package cubrid

import (
	"context"
	"fmt"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
)

// CUBRID statement command types (subset; values confirmed live on 11.4:
// INSERT=20, SELECT=21, UPDATE=22, DELETE=23; CALL_SP confirmed live on
// 11.4 for prepared `CALL javasp(...)` statements).
const (
	cmdTypeInsert = 20
	cmdTypeSelect = 21
	cmdTypeUpdate = 22
	cmdTypeDelete = 23
	cmdTypeCall   = 24  // CUBRID_STMT_CALL (object method call)
	cmdTypeCallSP = 126 // CUBRID_STMT_CALL_SP (0x7e, stored procedure)
)

// prepareCallFlag is the PREPARE flag marking a CALL statement (JDBC
// UConnection.PREPARE_CALL).
const prepareCallFlag = 0x40

// Stored-procedure parameter modes (JDBC UBindParameter PARAM_MODE_*).
const (
	paramModeIn  = 1
	paramModeOut = 2
)

// Column describes one result column from PREPARE metadata.
type Column struct {
	Name          string
	TypeCode      byte
	Collection    byte
	Charset       byte
	Scale         int16
	Precision     int32
	AttrName      string
	TableName     string
	Nullable      bool
	DefaultValue  string
	AutoIncrement bool
	UniqueKey     bool
	PrimaryKey    bool
	ForeignKey    bool
}

// decodeType returns the wire type to dispatch value decoding on. PREPARE
// metadata marks collection columns via the type byte's collection flags
// (1=SET, 2=MULTISET, 3=SEQUENCE) with TypeCode demoted to the element
// type (JDBC UColumnInfo.confirmType); the value payload then carries its
// own element type, so only the collection kind matters here.
func (c Column) decodeType() byte {
	switch c.Collection {
	case 1:
		return protocol.TypeSet
	case 2:
		return protocol.TypeMultiset
	case 3:
		return protocol.TypeSequence
	default:
		return c.TypeCode
	}
}

// TypeName returns the CUBRID SQL type name for the column ("VARCHAR",
// "NUMERIC", ...). Collection columns report the collection kind ("SET",
// "MULTISET", "SEQUENCE"), PREPARE metadata demotes their TypeCode to the
// element type. Unknown codes return "" (the contract of database/sql's
// RowsColumnTypeDatabaseTypeName).
func (c Column) TypeName() string {
	switch c.Collection {
	case 1:
		return "SET"
	case 2:
		return "MULTISET"
	case 3:
		return "SEQUENCE"
	}
	switch c.TypeCode {
	case protocol.TypeNull:
		return "NULL"
	case protocol.TypeChar:
		return "CHAR"
	case protocol.TypeVarchar:
		return "VARCHAR"
	case protocol.TypeNchar:
		return "NCHAR"
	case protocol.TypeVarNchar:
		return "NCHAR VARYING"
	case protocol.TypeBit:
		return "BIT"
	case protocol.TypeVarbit:
		return "BIT VARYING"
	case protocol.TypeNumeric:
		return "NUMERIC"
	case protocol.TypeInt, protocol.TypeUInt:
		return "INTEGER"
	case protocol.TypeShort, protocol.TypeUShort:
		return "SMALLINT"
	case protocol.TypeMonetary:
		return "MONETARY"
	case protocol.TypeFloat:
		return "FLOAT"
	case protocol.TypeDouble:
		return "DOUBLE"
	case protocol.TypeDate:
		return "DATE"
	case protocol.TypeTime:
		return "TIME"
	case protocol.TypeTimestamp:
		return "TIMESTAMP"
	case protocol.TypeSet:
		return "SET"
	case protocol.TypeMultiset:
		return "MULTISET"
	case protocol.TypeSequence:
		return "SEQUENCE"
	case protocol.TypeObject:
		return "OBJECT"
	case protocol.TypeResultset:
		return "RESULTSET"
	case protocol.TypeBigint, protocol.TypeUBigint:
		return "BIGINT"
	case protocol.TypeDatetime:
		return "DATETIME"
	case protocol.TypeBlob:
		return "BLOB"
	case protocol.TypeClob:
		return "CLOB"
	case protocol.TypeEnum:
		return "ENUM"
	case protocol.TypeTimestampTZ:
		return "TIMESTAMPTZ"
	case protocol.TypeTimestampLTZ:
		return "TIMESTAMPLTZ"
	case protocol.TypeDatetimeTZ:
		return "DATETIMETZ"
	case protocol.TypeDatetimeLTZ:
		return "DATETIMELTZ"
	case protocol.TypeJSON:
		return "JSON"
	}
	return ""
}

// minColumnWireBytes is a conservative lower bound on one column's PREPARE
// metadata: type byte (1) + scale (2) + precision (4) + three empty strings
// (3*4) + nullable byte (1) + default string (4) + seven flag bytes (7) = 31;
// 28 leaves slack should a future protocol variant drop a field.
const minColumnWireBytes = 28

// Stmt is a prepared statement bound to its Conn.
type Stmt struct {
	conn       *Conn
	sql        string
	handle     int32
	cmdType    byte
	paramCount int
	cols       []Column
	closed     bool

	outParams []bool // 0-based markers registered with RegisterOutParam
	outRow    []any  // CALL value row from the last execution
}

// isCall reports whether the statement is a CALL: its result row carries
// per-value type prefixes and (for CALL_SP) the OUT-parameter values.
func (s *Stmt) isCall() bool {
	return s != nil && (s.cmdType == cmdTypeCall || s.cmdType == cmdTypeCallSP)
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// Prepare compiles sql on the server and returns the statement handle
// plus result-set column metadata.
func (c *Conn) Prepare(ctx context.Context, sql string) (*Stmt, error) {
	return c.prepare(ctx, sql, 0)
}

// PrepareCall prepares a stored-procedure CALL statement (e.g.
// "CALL sp(?)" or "? = CALL fn(?)"). Mark OUT/INOUT markers with
// Stmt.RegisterOutParam before executing; after Exec or Query the OUT
// values are available from Stmt.OutValues.
func (c *Conn) PrepareCall(ctx context.Context, sql string) (*Stmt, error) {
	return c.prepare(ctx, sql, prepareCallFlag)
}

func (c *Conn) prepare(ctx context.Context, sql string, flag byte) (*Stmt, error) {
	w := protocol.NewWriter()
	w.RawByte(protocol.FnPrepare)
	w.ArgString(sql)
	w.ArgByte(flag) // prepare flags: no OID, not updatable, not holdable
	w.ArgByte(boolByte(c.autoCommit))
	r, err := c.roundTrip(ctx, w.Bytes())
	if err != nil {
		return nil, err
	}
	handle, err := protocol.ReadStatus(r)
	if err != nil {
		return nil, err
	}
	s := &Stmt{conn: c, sql: sql, handle: handle}
	if _, err := r.Int32(); err != nil { // result cache lifetime
		return nil, err
	}
	if s.cmdType, err = r.Byte(); err != nil {
		return nil, err
	}
	pc, err := r.Int32()
	if err != nil {
		return nil, err
	}
	// Like the column count below, the parameter count arrives
	// unauthenticated yet is used as an allocation length (the CALL_SP
	// column padding, RegisterOutParam's marker slice): a negative value
	// would panic make and a huge one would pre-allocate gigabytes. Every
	// marker is a literal '?' in the SQL we just sent, so a legitimate
	// count can never exceed the statement length.
	if pc < 0 || int(pc) > len(sql) {
		return nil, fmt.Errorf("cubrid: prepare: invalid parameter count %d (statement is %d bytes)",
			pc, len(sql))
	}
	s.paramCount = int(pc)
	if _, err := r.Byte(); err != nil { // updatable flag
		return nil, err
	}
	nCols, err := r.Int32()
	if err != nil {
		return nil, err
	}
	// The count comes from the wire unauthenticated: a negative value would
	// panic make, and a huge one would pre-allocate gigabytes before any
	// column data is validated. Each column occupies at least
	// minColumnWireBytes, so the count can never exceed the bytes left.
	if nCols < 0 || int64(nCols)*minColumnWireBytes > int64(r.Remaining()) {
		return nil, fmt.Errorf("cubrid: prepare: invalid column count %d (%d bytes remaining)",
			nCols, r.Remaining())
	}
	s.cols = make([]Column, 0, nCols)
	for i := 0; i < int(nCols); i++ {
		col, err := readColumn(r)
		if err != nil {
			return nil, fmt.Errorf("cubrid: prepare column %d: %w", i, err)
		}
		s.cols = append(s.cols, col)
	}
	if s.cmdType == cmdTypeCallSP {
		// The CALL_SP value row is [statement slot, marker 1..N] regardless
		// of the column metadata the broker sent: force the column list to
		// that width (JDBC UStatement overrides columnNumber the same way).
		// Values are self-described on the wire, so padded entries carry no
		// type information by design.
		cols := make([]Column, s.paramCount+1)
		copy(cols, s.cols)
		s.cols = cols
	}
	return s, nil
}

// RegisterOutParam marks the 1-based parameter index of a CALL statement
// as an OUT parameter. A marker registered OUT whose bound argument is
// non-nil is sent as INOUT. After Exec or Query, read the returned values
// with OutValues.
func (s *Stmt) RegisterOutParam(index int) error {
	if !s.isCall() {
		return fmt.Errorf("cubrid: RegisterOutParam on a non-CALL statement (command type %d)", s.cmdType)
	}
	if index < 1 || index > s.paramCount {
		return fmt.Errorf("cubrid: OUT parameter index %d out of range [1, %d]", index, s.paramCount)
	}
	if s.outParams == nil {
		s.outParams = make([]bool, s.paramCount)
	}
	s.outParams[index-1] = true
	return nil
}

// OutValues returns the values of the parameters registered with
// RegisterOutParam, in ascending parameter order, from the last
// execution's value row. It returns nil before the first execution.
func (s *Stmt) OutValues() []any {
	if s.outRow == nil {
		return nil
	}
	var out []any
	for i, isOut := range s.outParams {
		if !isOut {
			continue
		}
		// Row layout: [statement slot, marker 1..N], marker i+1 sits at
		// row index i+1 (JDBC CallableStatement getXXX(index) reads tuple
		// attribute index directly).
		if i+1 < len(s.outRow) {
			out = append(out, s.outRow[i+1])
		} else {
			out = append(out, nil)
		}
	}
	return out
}

// paramModes builds the per-marker mode bytes for a CALL_SP EXECUTE:
// IN for plain markers, OUT for registered markers bound to nil, INOUT
// for registered markers carrying a value (mirrors JDBC UBindParameter,
// where binding sets the IN bit and registration the OUT bit).
func (s *Stmt) paramModes(args []any) []byte {
	modes := make([]byte, s.paramCount)
	for i := range modes {
		modes[i] = paramModeIn
		if i < len(s.outParams) && s.outParams[i] {
			if args[i] == nil {
				modes[i] = paramModeOut
			} else {
				modes[i] = paramModeIn | paramModeOut
			}
		}
	}
	return modes
}

// readColumnType parses the (possibly extended two-byte) column type
// field shared by every column-metadata form. Confirmed live on 11.4:
// the broker sends the extended form (0x80 set, charset in the low bits).
func readColumnType(r *protocol.Reader, col *Column) error {
	tb, err := r.Byte()
	if err != nil {
		return err
	}
	if tb&0x80 != 0 { // extended 2-byte form
		col.Charset = tb & 0x07
		col.Collection = (tb & 0x60) >> 5
		if col.TypeCode, err = r.Byte(); err != nil {
			return err
		}
	} else { // legacy single byte
		col.TypeCode = tb & 0x1F
		col.Collection = tb >> 5
	}
	return nil
}

// readColumn parses one column's PREPARE metadata. Confirmed live on
// 11.4: the nullable byte is 0 for nullable, 1 for NOT NULL.
func readColumn(r *protocol.Reader) (Column, error) {
	var col Column
	err := readColumnType(r, &col)
	if err != nil {
		return col, err
	}
	if col.Scale, err = r.Int16(); err != nil {
		return col, err
	}
	if col.Precision, err = r.Int32(); err != nil {
		return col, err
	}
	if col.Name, err = r.String(); err != nil {
		return col, err
	}
	if col.AttrName, err = r.String(); err != nil {
		return col, err
	}
	if col.TableName, err = r.String(); err != nil {
		return col, err
	}
	nb, err := r.Byte()
	if err != nil {
		return col, err
	}
	col.Nullable = nb == 0
	if col.DefaultValue, err = r.String(); err != nil {
		return col, err
	}
	flags := make([]byte, 7) // auto_inc, unique, pk, rev_idx, rev_uniq, fk, shared
	for i := range flags {
		if flags[i], err = r.Byte(); err != nil {
			return col, err
		}
	}
	col.AutoIncrement = flags[0] == 1
	col.UniqueKey = flags[1] == 1
	col.PrimaryKey = flags[2] == 1
	col.ForeignKey = flags[5] == 1
	return col, nil
}

// encodeArg encodes one bind argument, handling driver-level types
// (*Lob, Numeric) before delegating to the protocol scalar encoder.
func encodeArg(w *protocol.Writer, a any) error {
	switch v := a.(type) {
	case *Lob:
		return v.encodeParam(w)
	case Numeric:
		// Exact decimals bind as their string rendering; the server
		// coerces to the column's NUMERIC precision/scale.
		return protocol.EncodeParam(w, string(v))
	}
	return protocol.EncodeParam(w, a)
}

// ParamCount reports the number of ? placeholders.
func (s *Stmt) ParamCount() int { return s.paramCount }

// Columns returns a copy of the result column metadata.
func (s *Stmt) Columns() []Column {
	out := make([]Column, len(s.cols))
	copy(out, s.cols)
	return out
}

// Result reports the outcome of a non-query statement.
type Result struct {
	AffectedRows int64
}

// Exec runs a DML/DDL statement with the given bind args.
func (s *Stmt) Exec(ctx context.Context, args ...any) (*Result, error) {
	rc, _, err := s.execute(ctx, args)
	if err != nil {
		return nil, err
	}
	return &Result{AffectedRows: int64(rc)}, nil
}

// Query runs a SELECT and returns a row iterator.
func (s *Stmt) Query(ctx context.Context, args ...any) (*Rows, error) {
	rc, rows, err := s.execute(ctx, args)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		rows = &Rows{stmt: s, cols: s.cols, totalRows: rc, fetchedAll: true}
	}
	return rows, nil
}

// execute sends EXECUTE and parses the response. Field order follows the
// JDBC UStatement layout; confirmed live against the 11.4 broker
// (TestIntegrationScalarMatrix114, TestIntegrationBindRoundTrip114).
func (s *Stmt) execute(ctx context.Context, args []any) (int32, *Rows, error) {
	if s.closed {
		return 0, nil, fmt.Errorf("cubrid: statement is closed")
	}
	if len(args) != s.paramCount {
		return 0, nil, fmt.Errorf("cubrid: statement wants %d args, got %d", s.paramCount, len(args))
	}
	isQuery := s.cmdType == cmdTypeSelect
	s.outRow = nil

	w := protocol.NewWriter()
	w.RawByte(protocol.FnExecute)
	w.ArgInt(s.handle)
	w.ArgByte(0) // execute flags
	w.ArgInt(0)  // max col size (unlimited)
	w.ArgInt(0)  // max fetch size (JDBC always sends 0)
	if s.cmdType == cmdTypeCallSP && s.paramCount > 0 {
		w.ArgBytes(s.paramModes(args)) // per-marker IN/OUT/INOUT modes
	} else {
		w.ArgNull() // param modes (no OUT params)
	}
	w.ArgByte(boolByte(isQuery)) // fetch flag
	w.ArgByte(boolByte(s.conn.autoCommit))
	w.ArgByte(1)                // forward-only cursor
	w.ArgBytes(make([]byte, 8)) // cache time: one 8-byte arg (sec, usec) per JDBC addCacheTime
	if s.conn.version.AtLeast(protocol.ProtoV4) {
		w.ArgInt(0) // query timeout ms (ctx deadlines enforce client-side)
	}
	for i, a := range args {
		if err := encodeArg(w, a); err != nil {
			return 0, nil, fmt.Errorf("cubrid: bind %d: %w", i+1, err)
		}
	}

	r, err := s.conn.roundTrip(ctx, w.Bytes())
	if err != nil {
		return 0, nil, err
	}
	rc, err := protocol.ReadStatus(r)
	if err != nil {
		return 0, nil, err
	}
	if _, err := r.Byte(); err != nil { // cache reusable
		return 0, nil, err
	}
	nResults, err := r.Int32()
	if err != nil {
		return 0, nil, err
	}
	for i := 0; i < int(nResults); i++ {
		if _, err := r.Byte(); err != nil { // statement type
			return 0, nil, err
		}
		if _, err := r.Int32(); err != nil { // result count
			return 0, nil, err
		}
		if _, err := r.Bytes(8); err != nil { // result OID
			return 0, nil, err
		}
		if _, err := r.Int32(); err != nil { // cache time sec
			return 0, nil, err
		}
		if _, err := r.Int32(); err != nil { // cache time usec
			return 0, nil, err
		}
	}
	if s.conn.version.AtLeast(protocol.ProtoV2) {
		inc, err := r.Byte()
		if err != nil {
			return 0, nil, err
		}
		if inc == 1 {
			return 0, nil, fmt.Errorf("cubrid: unexpected inline column info (not implemented)")
		}
	}
	if s.conn.version.AtLeast(protocol.ProtoV5) {
		if _, err := r.Int32(); err != nil { // shard id
			return 0, nil, err
		}
	}
	if !isQuery {
		if s.isCall() && rc > 0 {
			// The CALL value row (statement slot + per-marker values) is
			// not inlined in the EXECUTE response: pull it with FETCH, the
			// way JDBC's CallableStatement does before any getXXX.
			rows := &Rows{stmt: s, cols: s.cols, totalRows: rc, nextIndex: 1}
			if err := rows.fetchNext(ctx); err != nil {
				return 0, nil, err
			}
			if rows.nextIndex > rc {
				rows.fetchedAll = true
			}
			if len(rows.tuples) > 0 {
				s.outRow = rows.tuples[0]
			}
			return rc, rows, nil
		}
		return rc, nil, nil
	}
	rows := &Rows{stmt: s, cols: s.cols, totalRows: rc, nextIndex: 1}
	if rc > 0 && r.Remaining() > 4 {
		if _, err := r.Int32(); err != nil { // inline fetch rescode
			return 0, nil, err
		}
		if err := rows.consumeFetch(r); err != nil {
			return 0, nil, err
		}
	} else {
		rows.fetchedAll = rc == 0
	}
	return rc, rows, nil
}

// Close releases the server-side statement handle.
func (s *Stmt) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}
	s.closed = true
	w := protocol.NewWriter()
	w.RawByte(protocol.FnCloseStatement)
	w.ArgInt(s.handle)
	w.ArgByte(boolByte(s.conn.autoCommit))
	r, err := s.conn.roundTrip(ctx, w.Bytes())
	if err != nil {
		return err
	}
	_, err = protocol.ReadStatus(r)
	return err
}
