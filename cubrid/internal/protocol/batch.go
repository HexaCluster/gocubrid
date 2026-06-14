package protocol

import "fmt"

// BatchResult is one statement's outcome within a batch response. Exactly
// one of AffectedRows / Err is meaningful: a failed statement carries a
// *Error and zero AffectedRows.
type BatchResult struct {
	AffectedRows int64
	Err          error
}

// ExecuteBatchPrepRequest writes the EXECUTE_BATCH_PREPAREDSTATEMENT
// (fn 21) request header: statement handle, query timeout (V4+ only; 0,
// ctx deadlines enforce client-side), autocommit flag. The caller appends
// the bind parameters: every batch row's params back-to-back as
// (type byte + value) pairs with no separators between rows, the server
// infers the row count from the argument count
// (JDBC UStatement.writeExecuteBatchRequest).
func ExecuteBatchPrepRequest(w *Writer, handle int32, autoCommit bool, ver Version) {
	w.RawByte(FnExecuteBatchPrep)
	w.ArgInt(handle)
	if ver.AtLeast(ProtoV4) {
		w.ArgInt(0) // query timeout ms
	}
	w.ArgByte(autoCommitByte(autoCommit))
}

// ExecuteBatchSQLRequest writes the complete EXECUTE_BATCH_STATEMENT
// (fn 20) request: autocommit flag, query timeout (V4+ only), then each
// SQL statement as a NUL-terminated string (JDBC UConnection.batchExecute).
func ExecuteBatchSQLRequest(w *Writer, autoCommit bool, sqls []string, ver Version) {
	w.RawByte(FnExecuteBatch)
	w.ArgByte(autoCommitByte(autoCommit))
	if ver.AtLeast(ProtoV4) {
		w.ArgInt(0) // query timeout ms
	}
	for _, s := range sqls {
		w.ArgString(s)
	}
}

func autoCommitByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// minBatchResultBytes is a lower bound on one batch result's wire size:
// a success is [result int32] + 8 reserved bytes = 12; an error is
// [result int32][err code int32][msg len int32] = 12 minimum.
const minBatchResultBytes = 12

// ParseBatchResponse parses the batch response body, shared by fn 20 and
// fn 21, positioned after the leading status int (consume it with
// ReadStatus first). Layout per JDBC UStatement.executeBatchInternal /
// UConnection.batchExecute: [result count int] then per result,
// a statement-type byte first when withStmtType (fn 20 only), [result
// int]; negative -> [err code int][err msg length-prefixed string];
// non-negative -> 8 reserved bytes (int+short+short, JCI 3.0). A trailing
// shard id (int) follows when AtLeast(V5). Confirmed live across the
// 9.3 to 11.4 matrix: the per-result error message is a length-prefixed
// string (NOT the raw-remainder form ReadStatus handles) and the 8
// reserved bytes follow every non-negative result on every version.
func ParseBatchResponse(r *Reader, ver Version, withStmtType bool) ([]BatchResult, error) {
	n, err := r.Int32()
	if err != nil {
		return nil, err
	}
	// The count comes from the wire unauthenticated: bound it by the bytes
	// actually present before allocating (same guard as prepare metadata).
	if n < 0 || int64(n)*minBatchResultBytes > int64(r.Remaining()) {
		return nil, fmt.Errorf("cubrid/protocol: invalid batch result count %d (%d bytes remaining)",
			n, r.Remaining())
	}
	out := make([]BatchResult, 0, n)
	for i := int32(0); i < n; i++ {
		if withStmtType {
			if _, err := r.Byte(); err != nil {
				return nil, err
			}
		}
		res, err := r.Int32()
		if err != nil {
			return nil, err
		}
		if res < 0 {
			code, err := r.Int32()
			if err != nil {
				return nil, err
			}
			msg, err := r.String()
			if err != nil {
				return nil, err
			}
			out = append(out, BatchResult{Err: &Error{Code: code, Message: msg}})
			continue
		}
		if _, err := r.Int32(); err != nil { // reserved (JCI 3.0)
			return nil, err
		}
		if _, err := r.Int16(); err != nil { // reserved
			return nil, err
		}
		if _, err := r.Int16(); err != nil { // reserved
			return nil, err
		}
		out = append(out, BatchResult{AffectedRows: int64(res)})
	}
	if ver.AtLeast(ProtoV5) {
		if _, err := r.Int32(); err != nil { // shard id (driver is shard-unaware)
			return nil, err
		}
	}
	return out, nil
}
