package cubrid

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
)

// SchemaType selects a GET_SCHEMA_INFO result family.
type SchemaType int32

// The GET_SCHEMA_INFO families the matrix supports, used as the SchemaType
// argument to Conn.SchemaInfo; the typed helpers (Tables, Columns,
// PrimaryKey, ImportedKeys, ExportedKeys, Indexes) wrap the common ones.
const (
	SchemaClass        SchemaType = SchemaType(protocol.SchClass)
	SchemaVClass       SchemaType = SchemaType(protocol.SchVClass)
	SchemaAttribute    SchemaType = SchemaType(protocol.SchAttribute)
	SchemaConstraint   SchemaType = SchemaType(protocol.SchConstraint)
	SchemaPrimaryKey   SchemaType = SchemaType(protocol.SchPrimaryKey)
	SchemaImportedKeys SchemaType = SchemaType(protocol.SchImportedKeys)
	SchemaExportedKeys SchemaType = SchemaType(protocol.SchExportedKeys)
)

// minSchemaColumnWireBytes is a lower bound on one column's schema-info
// metadata: type byte (1) + scale (2) + precision (4) + empty name (4).
const minSchemaColumnWireBytes = 11

// SchemaInfo runs GET_SCHEMA_INFO and returns the raw rows (low-level).
// arg1/arg2 are class/attribute name filters; nil means no filter;
// arg1Pattern/arg2Pattern report whether each arg uses SQL wildcards.
// Row values arrive as the wire types the server picked (strings plus
// int/short variants); table names may come back owner-qualified
// ("dba.t") on 11.x, the typed helpers split them, this API keeps the
// raw form. Closing the returned Rows releases the server-side handle.
//
// Response shape confirmed live across the 9.3 to 11.4 matrix: status int
// = server handle, then [total tuple count int][column count int] and
// per-column short metadata; rows are pulled via the normal FETCH path.
func (c *Conn) SchemaInfo(ctx context.Context, st SchemaType, arg1, arg2 *string, arg1Pattern, arg2Pattern bool) (*Rows, error) {
	var flag byte
	if arg1Pattern {
		flag |= protocol.SchemaFlagArg1Pattern
	}
	if arg2Pattern {
		flag |= protocol.SchemaFlagArg2Pattern
	}
	w := protocol.NewWriter()
	protocol.SchemaInfoRequest(w, int32(st), arg1, arg2, flag, c.version)
	r, err := c.roundTrip(ctx, w.Bytes())
	if err != nil {
		return nil, err
	}
	handle, err := protocol.ReadStatus(r)
	if err != nil {
		return nil, err
	}
	total, err := r.Int32()
	if err != nil {
		return nil, err
	}
	nCols, err := r.Int32()
	if err != nil {
		return nil, err
	}
	if nCols < 0 || int64(nCols)*minSchemaColumnWireBytes > int64(r.Remaining()) {
		return nil, fmt.Errorf("cubrid: schema info: invalid column count %d (%d bytes remaining)",
			nCols, r.Remaining())
	}
	cols := make([]Column, 0, nCols)
	for i := 0; i < int(nCols); i++ {
		col, err := readSchemaColumn(r)
		if err != nil {
			return nil, fmt.Errorf("cubrid: schema info column %d: %w", i, err)
		}
		cols = append(cols, col)
	}
	stmt := &Stmt{conn: c, handle: handle, cmdType: cmdTypeSelect, cols: cols}
	return &Rows{
		stmt:       stmt,
		cols:       cols,
		totalRows:  total,
		nextIndex:  1,
		fetchedAll: total == 0,
		ownsStmt:   true,
	}, nil
}

// readSchemaColumn parses the short column-metadata form used by
// non-NORMAL (schema info) statements: type, scale, precision, name,
// no attr/class/default fields (JDBC UStatement.readColumnInfo).
// Confirmed live: every matrix broker (9.3.9 to 11.4.5) sends the short
// form for schema-info statements, never the NORMAL prepare form.
func readSchemaColumn(r *protocol.Reader) (Column, error) {
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
	return col, nil
}

// TableInfo is one SCH_CLASS row: 0 = system class, 1 = view, 2 = table.
type TableInfo struct {
	Name string
	Type int32
}

// ColumnInfo is one SCH_ATTRIBUTE row mapped to Go types. TypeCode is
// the element wire type (protocol type code) of the column's domain.
type ColumnInfo struct {
	Name      string
	TypeCode  byte
	Scale     int16
	Precision int32
	NotNull   bool
	Unique    bool
	IsKey     bool
	Default   string
	Order     int32
	Table     string
}

// KeyColumn is one SCH_PRIMARY_KEY row. Seq is 1-based.
type KeyColumn struct {
	Table, Column, KeyName string
	Seq                    int32
}

// ForeignKey is one SCH_IMPORTED_KEYS / SCH_EXPORTED_KEYS row. Update
// and delete rules: 0=CASCADE, 1=RESTRICT, 2=NO ACTION, 3=SET NULL.
type ForeignKey struct {
	PKTable, PKColumn, FKTable, FKColumn, FKName, PKName string
	Seq                                                  int32
	UpdateRule, DeleteRule                               int16
}

// forEachSchemaRow drains rows, invoking fn per row, and always closes
// them (releasing the server-side schema statement handle).
func forEachSchemaRow(rows *Rows, fn func(row []any) error) error {
	defer rows.Close()
	for {
		row, err := rows.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(row); err != nil {
			return err
		}
	}
}

// schemaInt coerces the integer wire types schema rows arrive with.
func schemaInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	}
	return 0, false
}

func schemaString(v any) string {
	s, _ := v.(string)
	return s
}

// unqualifyName strips the owner from an owner-qualified class name
// ("dba.t" -> "t"); 11.x servers return qualified names from schema info.
func unqualifyName(s string) string {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return s[i+1:]
	}
	return s
}

// schemaTypeCode maps a SCH_ATTRIBUTE domain value to a protocol type
// code. Servers sending the extended two-byte column type pack it into
// the value's high byte (JDBC UStatement.confirmSchemaTypeInfo): when
// bit 0x80 of the high byte is set, the low byte is the real type;
// otherwise the single-byte form applies (collection bits up top).
func schemaTypeCode(v int64) byte {
	if msb := byte(v >> 8); msb&0x80 != 0 {
		return byte(v)
	}
	return byte(v) & 0x1F
}

// rowShapeError reports a schema row narrower than the family requires.
func rowShapeError(family string, want int, row []any) error {
	return fmt.Errorf("cubrid: schema info %s row has %d columns, want at least %d", family, len(row), want)
}

// Tables lists the database's tables and views (SCH_CLASS, system
// classes excluded by Type: 0=system, 1=view, 2=table, all returned).
func (c *Conn) Tables(ctx context.Context) ([]TableInfo, error) {
	rows, err := c.SchemaInfo(ctx, SchemaClass, nil, nil, true, true)
	if err != nil {
		return nil, err
	}
	var out []TableInfo
	err = forEachSchemaRow(rows, func(row []any) error {
		if len(row) < 2 {
			return rowShapeError("class", 2, row)
		}
		typ, ok := schemaInt(row[1])
		if !ok {
			return fmt.Errorf("cubrid: schema info class type = %#v", row[1])
		}
		out = append(out, TableInfo{Name: unqualifyName(schemaString(row[0])), Type: int32(typ)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Columns lists table's columns (SCH_ATTRIBUTE) in definition order.
func (c *Conn) Columns(ctx context.Context, table string) ([]ColumnInfo, error) {
	rows, err := c.SchemaInfo(ctx, SchemaAttribute, &table, nil, false, true)
	if err != nil {
		return nil, err
	}
	var out []ColumnInfo
	err = forEachSchemaRow(rows, func(row []any) error {
		if len(row) < 13 {
			return rowShapeError("attribute", 13, row)
		}
		ci := ColumnInfo{
			Name:    schemaString(row[0]),
			Default: schemaString(row[8]),
			Table:   unqualifyName(schemaString(row[10])),
		}
		domain, ok := schemaInt(row[1])
		if !ok {
			return fmt.Errorf("cubrid: schema info attribute domain = %#v", row[1])
		}
		ci.TypeCode = schemaTypeCode(domain)
		if v, ok := schemaInt(row[2]); ok {
			ci.Scale = int16(v)
		}
		if v, ok := schemaInt(row[3]); ok {
			ci.Precision = int32(v)
		}
		if v, ok := schemaInt(row[5]); ok {
			ci.NotNull = v != 0
		}
		if v, ok := schemaInt(row[7]); ok {
			ci.Unique = v != 0
		}
		if v, ok := schemaInt(row[9]); ok {
			ci.Order = int32(v)
		}
		if v, ok := schemaInt(row[12]); ok {
			ci.IsKey = v != 0
		}
		out = append(out, ci)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PrimaryKey lists table's primary-key columns (SCH_PRIMARY_KEY) with
// 1-based Seq, in key order.
func (c *Conn) PrimaryKey(ctx context.Context, table string) ([]KeyColumn, error) {
	rows, err := c.SchemaInfo(ctx, SchemaPrimaryKey, &table, nil, true, true)
	if err != nil {
		return nil, err
	}
	var out []KeyColumn
	err = forEachSchemaRow(rows, func(row []any) error {
		if len(row) < 4 {
			return rowShapeError("primary key", 4, row)
		}
		seq, ok := schemaInt(row[2])
		if !ok {
			return fmt.Errorf("cubrid: schema info key_seq = %#v", row[2])
		}
		out = append(out, KeyColumn{
			Table:   unqualifyName(schemaString(row[0])),
			Column:  schemaString(row[1]),
			Seq:     int32(seq),
			KeyName: schemaString(row[3]),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// IndexInfo is one of table's indexes, grouped from SCH_CONSTRAINT rows
// and merged with SCH_PRIMARY_KEY (see Indexes).
type IndexInfo struct {
	Name      string
	Unique    bool
	Columns   []string // in key order
	IsPrimary bool
}

// Indexes lists table's indexes: SCH_CONSTRAINT rows grouped by index
// name with columns in key order, merged with SCH_PRIMARY_KEY. The
// merge exists because, asserted live across the whole matrix
// (9.3.9 to 11.4.5), the primary key's backing index never appears in
// SCH_CONSTRAINT rows, and the rows' PRIMARY_KEY column is always 0;
// the PK index is therefore synthesized from SCH_PRIMARY_KEY and
// listed first. Constraint TYPE values (JDBC getIndexInfo): 0=UNIQUE,
// 1=INDEX, 2=REVERSE UNIQUE, 3=REVERSE INDEX. The table name is
// matched exactly, not as a pattern ('_' in a table name is a SQL
// wildcard under pattern matching); the SCH_PRIMARY_KEY merge request
// IS pattern-flagged (matrix-pinned wire shape), so its rows are
// filtered back down to table. Within an index, columns follow the
// live-confirmed 1-based KEY_ORDER column, falling back to server
// emission order when a row is too narrow to carry it.
func (c *Conn) Indexes(ctx context.Context, table string) ([]IndexInfo, error) {
	rows, err := c.SchemaInfo(ctx, SchemaConstraint, &table, nil, false, false)
	if err != nil {
		return nil, err
	}
	type keyCol struct {
		name  string
		order int64
	}
	var out []IndexInfo
	var idxCols [][]keyCol
	byName := map[string]int{}
	err = forEachSchemaRow(rows, func(row []any) error {
		if len(row) < 6 {
			return rowShapeError("constraint", 6, row)
		}
		typ, ok := schemaInt(row[0])
		if !ok {
			return fmt.Errorf("cubrid: schema info constraint type = %#v", row[0])
		}
		name := schemaString(row[1])
		i, ok := byName[name]
		if !ok {
			i = len(out)
			byName[name] = i
			info := IndexInfo{
				Name:   name,
				Unique: typ == 0 || typ == 2, // UNIQUE / REVERSE UNIQUE
			}
			if v, ok := schemaInt(row[5]); ok {
				info.IsPrimary = v != 0
			}
			out = append(out, info)
			idxCols = append(idxCols, nil)
		}
		// KEY_ORDER (1-based); the arrival-position fallback keeps the
		// stable sort below a no-op for rows that don't carry it.
		order := int64(len(idxCols[i]))
		if len(row) > 6 {
			if v, ok := schemaInt(row[6]); ok {
				order = v
			}
		}
		idxCols[i] = append(idxCols[i], keyCol{name: schemaString(row[2]), order: order})
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i, kcs := range idxCols {
		sort.SliceStable(kcs, func(a, b int) bool { return kcs[a].order < kcs[b].order })
		cols := make([]string, len(kcs))
		for j, kc := range kcs {
			cols[j] = kc.name
		}
		out[i].Columns = cols
	}
	pk, err := c.PrimaryKey(ctx, table)
	if err != nil {
		return nil, err
	}
	// The pattern-flagged SCH_PRIMARY_KEY request can match sibling
	// classes through '_' wildcards in table (go_t also matches goxt);
	// keep only table's own rows. KeyColumn.Table is already unqualified.
	n := 0
	for _, k := range pk {
		if k.Table == unqualifyName(table) {
			pk[n] = k
			n++
		}
	}
	pk = pk[:n]
	if len(pk) == 0 {
		return out, nil
	}
	sort.SliceStable(pk, func(i, j int) bool { return pk[i].Seq < pk[j].Seq })
	if i, ok := byName[pk[0].KeyName]; ok {
		// Defensive: a server that does list the PK constraint just gets
		// its existing entry marked, never a duplicate.
		out[i].IsPrimary = true
		out[i].Unique = true
		return out, nil
	}
	cols := make([]string, len(pk))
	for i, k := range pk {
		cols[i] = k.Column
	}
	pkIdx := IndexInfo{Name: pk[0].KeyName, Unique: true, Columns: cols, IsPrimary: true}
	return append([]IndexInfo{pkIdx}, out...), nil
}

// ImportedKeys lists the foreign keys in table referencing other tables
// (SCH_IMPORTED_KEYS).
func (c *Conn) ImportedKeys(ctx context.Context, table string) ([]ForeignKey, error) {
	return c.foreignKeys(ctx, SchemaImportedKeys, table)
}

// ExportedKeys lists the foreign keys in other tables referencing table
// (SCH_EXPORTED_KEYS).
func (c *Conn) ExportedKeys(ctx context.Context, table string) ([]ForeignKey, error) {
	return c.foreignKeys(ctx, SchemaExportedKeys, table)
}

func (c *Conn) foreignKeys(ctx context.Context, st SchemaType, table string) ([]ForeignKey, error) {
	rows, err := c.SchemaInfo(ctx, st, &table, nil, true, true)
	if err != nil {
		return nil, err
	}
	var out []ForeignKey
	err = forEachSchemaRow(rows, func(row []any) error {
		if len(row) < 9 {
			return rowShapeError("foreign key", 9, row)
		}
		fk := ForeignKey{
			PKTable:  unqualifyName(schemaString(row[0])),
			PKColumn: schemaString(row[1]),
			FKTable:  unqualifyName(schemaString(row[2])),
			FKColumn: schemaString(row[3]),
			FKName:   schemaString(row[7]),
			PKName:   schemaString(row[8]),
		}
		if v, ok := schemaInt(row[4]); ok {
			fk.Seq = int32(v)
		}
		if v, ok := schemaInt(row[5]); ok {
			fk.UpdateRule = int16(v)
		}
		if v, ok := schemaInt(row[6]); ok {
			fk.DeleteRule = int16(v)
		}
		out = append(out, fk)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
