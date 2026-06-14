package cubridsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/hexacluster/gocubrid/cubrid"
)

// maxLobBytes caps eager LOB materialization in result sets. Bigger LOBs
// must be streamed through the native API (cubrid.Conn / Lob.ReadAt),
// where reads are chunked and context-aware; silently buffering them here
// would be a memory footgun.
const maxLobBytes = 1 << 20

// mapErr translates native errors for database/sql: a poisoned connection
// becomes driver.ErrBadConn (so the pool discards and retries); *cubrid.Error
// and context errors pass through unwrapped.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, cubrid.ErrBadConn) {
		return driver.ErrBadConn
	}
	return err
}

// toDriverValue widens one native result value to the driver.Value
// canonical set: int16/int32 -> int64, float32 -> float64, LOB locators
// materialize as []byte (<= maxLobBytes), collections stay []any with
// widened elements (scan them into *any).
func toDriverValue(ctx context.Context, v any) (driver.Value, error) {
	switch v := v.(type) {
	case nil, int64, float64, bool, string, []byte, time.Time:
		return v, nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float32:
		return float64(v), nil
	case *cubrid.Lob:
		if v.Size() > maxLobBytes {
			return nil, fmt.Errorf(
				"cubridsql: %d-byte LOB column exceeds the adapter's %d-byte materialization cap; stream it with the native API (cubrid.Conn, Lob.ReadAt)",
				v.Size(), maxLobBytes)
		}
		b, err := v.ReadAll(ctx)
		if err != nil {
			return nil, mapErr(err)
		}
		return b, nil
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			ev, err := toDriverValue(ctx, e)
			if err != nil {
				return nil, fmt.Errorf("cubridsql: collection element %d: %w", i, err)
			}
			out[i] = ev
		}
		return out, nil
	default:
		// Unanticipated native type: hand it through; database/sql can
		// still deliver it to a *any scan destination.
		return v, nil
	}
}
