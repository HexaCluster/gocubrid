package cubridsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/hexacluster/gocubrid/cubrid"
)

// stmt adapts a native prepared statement.
type stmt struct {
	ns *cubrid.Stmt
}

var (
	_ driver.Stmt             = (*stmt)(nil)
	_ driver.StmtExecContext  = (*stmt)(nil)
	_ driver.StmtQueryContext = (*stmt)(nil)
)

func (s *stmt) Close() error {
	return mapErr(s.ns.Close(context.Background()))
}

// NumInput reports the ? placeholder count from PREPARE metadata;
// database/sql enforces it before calling Exec/Query.
func (s *stmt) NumInput() int { return s.ns.ParamCount() }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamed(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamed(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	vals, err := bindArgs(args)
	if err != nil {
		return nil, err
	}
	res, err := s.ns.Exec(ctx, vals...)
	if err != nil {
		return nil, mapErr(err)
	}
	return result{affected: res.AffectedRows}, nil
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	vals, err := bindArgs(args)
	if err != nil {
		return nil, err
	}
	nr, err := s.ns.Query(ctx, vals...)
	if err != nil {
		return nil, mapErr(err)
	}
	return &rows{ctx: ctx, nr: nr, cols: nr.Columns()}, nil
}

// valuesToNamed adapts the legacy driver.Stmt arg form.
func valuesToNamed(args []driver.Value) []driver.NamedValue {
	nvs := make([]driver.NamedValue, len(args))
	for i, v := range args {
		nvs[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return nvs
}

// bindArgs converts checked driver args to native bind values in ordinal
// order. Named parameters were already rejected by CheckNamedValue; the
// re-check guards direct driver users.
func bindArgs(args []driver.NamedValue) ([]any, error) {
	out := make([]any, len(args))
	for _, a := range args {
		if a.Name != "" {
			return nil, fmt.Errorf("cubridsql: named parameter %q is not supported (CUBRID binds by ? ordinal only)", a.Name)
		}
		if a.Ordinal < 1 || a.Ordinal > len(args) {
			return nil, fmt.Errorf("cubridsql: parameter ordinal %d out of range [1, %d]", a.Ordinal, len(args))
		}
		out[a.Ordinal-1] = a.Value
	}
	return out, nil
}

// result reports DML outcomes.
type result struct {
	affected int64
}

var errNoLastInsertId = errors.New(
	"cubridsql: LastInsertId is not supported by the CUBRID wire protocol path used here; run SELECT LAST_INSERT_ID() in the same session instead")

func (r result) LastInsertId() (int64, error) { return 0, errNoLastInsertId }

func (r result) RowsAffected() (int64, error) { return r.affected, nil }
