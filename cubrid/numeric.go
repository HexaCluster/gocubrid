package cubrid

import (
	"database/sql/driver"
	"fmt"
	"math"
	"strconv"
)

// Numeric holds a CUBRID NUMERIC (DECIMAL) value as the exact decimal
// string the server rendered, with no float rounding. NUMERIC result
// columns scan into string by default (the adapter's stdlib-friendly scan
// type); Numeric is the opt-in exact target via its sql.Scanner:
//
//	var price cubrid.Numeric
//	err := db.QueryRow("SELECT price FROM item WHERE id = ?", 1).Scan(&price)
//
// As a bind parameter it travels as its string rendering (driver.Valuer
// for database/sql, handled natively by Stmt.Exec/Query), which the server
// coerces to the column's NUMERIC precision and scale. For nullable
// columns wrap it: sql.Null[cubrid.Numeric].
type Numeric string

// String returns the exact decimal rendering.
func (n Numeric) String() string { return string(n) }

// Float64 converts the decimal to a float64. The conversion is lossy for
// values needing more than 53 bits of mantissa, that is the point of
// keeping NUMERIC as a string in the first place; use it only when float
// precision is acceptable.
func (n Numeric) Float64() (float64, error) {
	f, err := strconv.ParseFloat(string(n), 64)
	if err != nil {
		return 0, fmt.Errorf("cubrid: Numeric %q is not a valid decimal: %w", string(n), err)
	}
	return f, nil
}

// Scan implements sql.Scanner. It accepts string and []byte verbatim
// (the exact server rendering), and renders int64 and float64 sources in
// plain decimal notation (never scientific). NaN and infinities are
// rejected, NUMERIC has no representation for them. SQL NULL is rejected
// too: a Numeric has no null state, scan into sql.Null[Numeric] instead.
func (n *Numeric) Scan(v any) error {
	switch v := v.(type) {
	case string:
		*n = Numeric(v)
	case []byte:
		*n = Numeric(v)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("cubrid: cannot scan %v into Numeric: NUMERIC cannot represent NaN or infinities", v)
		}
		*n = Numeric(strconv.FormatFloat(v, 'f', -1, 64))
	case int64:
		*n = Numeric(strconv.FormatInt(v, 10))
	case nil:
		return fmt.Errorf("cubrid: cannot scan NULL into Numeric (use sql.Null[cubrid.Numeric])")
	default:
		return fmt.Errorf("cubrid: cannot scan %T into Numeric", v)
	}
	return nil
}

// Value implements driver.Valuer: the value travels as its exact string
// rendering and the server coerces it to NUMERIC.
func (n Numeric) Value() (driver.Value, error) { return string(n), nil }
