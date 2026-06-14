package cubrid

import (
	"context"
	"fmt"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
)

// BatchResult reports one statement's outcome within a batch execution.
// A failed statement carries a non-nil Err (an *Error from the server)
// and zero AffectedRows; the batch as a whole still succeeds, per-row
// errors do not abort the round-trip.
type BatchResult struct {
	AffectedRows int64
	Err          error
}

func batchResults(pres []protocol.BatchResult) []BatchResult {
	out := make([]BatchResult, len(pres))
	for i, p := range pres {
		out[i] = BatchResult{AffectedRows: p.AffectedRows, Err: p.Err}
	}
	return out
}

// ExecBatch executes the prepared statement once per row of binds in a
// single round-trip (EXECUTE_BATCH_PREPAREDSTATEMENT). Every row must
// have exactly ParamCount values. One result is returned per row, in
// order; a row that fails server-side gets its Err set while the other
// rows still report their outcomes (the server keeps executing past a
// failed row, confirmed live across the matrix). An empty rows slice is
// a no-op. Statements without parameters cannot be batched more than
// once: the wire format carries no row count, the server infers it from
// the bind values.
func (s *Stmt) ExecBatch(ctx context.Context, rows [][]any) ([]BatchResult, error) {
	if s.closed {
		return nil, fmt.Errorf("cubrid: statement is closed")
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if s.paramCount == 0 && len(rows) > 1 {
		return nil, fmt.Errorf("cubrid: cannot batch a statement without parameters %d times", len(rows))
	}
	for i, row := range rows {
		if len(row) != s.paramCount {
			return nil, fmt.Errorf("cubrid: batch row %d has %d args, statement wants %d",
				i+1, len(row), s.paramCount)
		}
	}
	w := protocol.NewWriter()
	protocol.ExecuteBatchPrepRequest(w, s.handle, s.conn.autoCommit, s.conn.version)
	for i, row := range rows {
		for j, a := range row {
			if err := encodeArg(w, a); err != nil {
				return nil, fmt.Errorf("cubrid: batch row %d bind %d: %w", i+1, j+1, err)
			}
		}
	}
	return s.conn.batchRoundTrip(ctx, w.Bytes(), false)
}

// ExecBatchSQL runs the given SQL statements (DDL and/or DML, without
// placeholders) in a single round-trip (EXECUTE_BATCH_STATEMENT). One
// result is returned per statement, in order, with per-statement errors
// isolated like ExecBatch. An empty slice is a no-op.
func (c *Conn) ExecBatchSQL(ctx context.Context, sqls []string) ([]BatchResult, error) {
	if len(sqls) == 0 {
		return nil, nil
	}
	w := protocol.NewWriter()
	protocol.ExecuteBatchSQLRequest(w, c.autoCommit, sqls, c.version)
	return c.batchRoundTrip(ctx, w.Bytes(), true)
}

// batchRoundTrip sends a batch request and parses the shared response
// shape (fn 20 responses carry an extra statement-type byte per result).
func (c *Conn) batchRoundTrip(ctx context.Context, payload []byte, withStmtType bool) ([]BatchResult, error) {
	r, err := c.roundTrip(ctx, payload)
	if err != nil {
		return nil, err
	}
	if _, err := protocol.ReadStatus(r); err != nil {
		return nil, err
	}
	pres, err := protocol.ParseBatchResponse(r, c.version, withStmtType)
	if err != nil {
		return nil, err
	}
	return batchResults(pres), nil
}
