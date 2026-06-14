package cubridsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/hexacluster/gocubrid/cubrid"
)

// rows adapts a native result set. When produced by conn.QueryContext it
// also owns the implicit statement and releases its server handle on
// Close. ctx is the query's context: it scopes LOB materialization during
// Next (driver.Rows.Next itself carries no context).
type rows struct {
	ctx       context.Context
	nr        *cubrid.Rows
	cols      []cubrid.Column
	ownedStmt *cubrid.Stmt
}

var (
	_ driver.Rows                           = (*rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ driver.RowsColumnTypeLength           = (*rows)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*rows)(nil)
	_ driver.RowsColumnTypeNullable         = (*rows)(nil)
	_ driver.RowsColumnTypeScanType         = (*rows)(nil)
)

func (r *rows) Columns() []string {
	out := make([]string, len(r.cols))
	for i, c := range r.cols {
		out[i] = c.Name
	}
	return out
}

func (r *rows) Close() error {
	err := r.nr.Close()
	if r.ownedStmt != nil {
		if cerr := r.ownedStmt.Close(context.Background()); err == nil {
			err = cerr
		}
	}
	return mapErr(err)
}

func (r *rows) Next(dest []driver.Value) error {
	row, err := r.nr.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return mapErr(err)
	}
	for i, v := range row {
		dv, err := toDriverValue(r.ctx, v)
		if err != nil {
			return err
		}
		dest[i] = dv
	}
	return nil
}

// ColumnTypeDatabaseTypeName reports the CUBRID type name ("VARCHAR",
// "NUMERIC", ...; collection columns report "SET"/"MULTISET"/"SEQUENCE").
func (r *rows) ColumnTypeDatabaseTypeName(i int) string {
	return r.cols[i].TypeName()
}

// ColumnTypeLength reports the declared length of variable-length columns
// (LOB columns report MaxInt64; their byte length lives in the locator).
func (r *rows) ColumnTypeLength(i int) (int64, bool) {
	switch r.cols[i].TypeName() {
	case "VARCHAR", "NCHAR VARYING", "BIT VARYING":
		return int64(r.cols[i].Precision), true
	case "BLOB", "CLOB", "JSON":
		return math.MaxInt64, true
	}
	return 0, false
}

// ColumnTypePrecisionScale reports precision and scale for NUMERIC columns.
func (r *rows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	if c := r.cols[i]; c.TypeName() == "NUMERIC" {
		return int64(c.Precision), int64(c.Scale), true
	}
	return 0, 0, false
}

func (r *rows) ColumnTypeNullable(i int) (nullable, ok bool) {
	return r.cols[i].Nullable, true
}

var (
	scanTypeInt64   = reflect.TypeOf(int64(0))
	scanTypeFloat64 = reflect.TypeOf(float64(0))
	scanTypeString  = reflect.TypeOf("")
	scanTypeBytes   = reflect.TypeOf([]byte(nil))
	scanTypeTime    = reflect.TypeOf(time.Time{})
	scanTypeSlice   = reflect.TypeOf([]any(nil))
	scanTypeAny     = reflect.TypeOf((*any)(nil)).Elem()
)

// ColumnTypeScanType reports the Go type Next delivers for the column
// after adapter widening (int16/int32 -> int64, float32 -> float64, LOB -> // []byte). NUMERIC scans as string to keep the exact server rendering.
func (r *rows) ColumnTypeScanType(i int) reflect.Type {
	switch r.cols[i].TypeName() {
	case "SMALLINT", "INTEGER", "BIGINT":
		return scanTypeInt64
	case "FLOAT", "DOUBLE", "MONETARY":
		return scanTypeFloat64
	case "CHAR", "VARCHAR", "NCHAR", "NCHAR VARYING", "ENUM", "NUMERIC":
		return scanTypeString
	case "BIT", "BIT VARYING", "JSON", "BLOB", "CLOB":
		return scanTypeBytes
	case "DATE", "TIME", "TIMESTAMP", "DATETIME",
		"TIMESTAMPTZ", "TIMESTAMPLTZ", "DATETIMETZ", "DATETIMELTZ":
		return scanTypeTime
	case "SET", "MULTISET", "SEQUENCE":
		return scanTypeSlice
	}
	return scanTypeAny
}
