package cubrid

import (
	"context"
	"fmt"
	"io"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
)

// Rows iterates a query result. The first batch arrives inline with the
// EXECUTE response; later batches are pulled with FETCH.
type Rows struct {
	stmt *Stmt
	cols []Column

	tuples     [][]any
	pos        int
	nextIndex  int32 // 1-based cursor index of the next un-fetched row
	totalRows  int32
	fetchedAll bool
	closed     bool
	ownsStmt   bool // Close also releases the internal statement (SchemaInfo)
}

// Columns returns a copy of the column metadata.
func (r *Rows) Columns() []Column {
	out := make([]Column, len(r.cols))
	copy(out, r.cols)
	return out
}

// Next returns the next row, or io.EOF after the last one.
func (r *Rows) Next() ([]any, error) {
	if r.closed {
		return nil, fmt.Errorf("cubrid: rows are closed")
	}
	if r.pos < len(r.tuples) {
		row := r.tuples[r.pos]
		r.pos++
		return row, nil
	}
	if r.fetchedAll {
		return nil, io.EOF
	}
	if err := r.fetchNext(contextBackground()); err != nil {
		return nil, err
	}
	if r.pos >= len(r.tuples) {
		return nil, io.EOF
	}
	row := r.tuples[r.pos]
	r.pos++
	return row, nil
}

// fetchNext pulls the next batch with FETCH. Argument order per JDBC
// UStatement.fetch, confirmed live on 11.4: a 1600-row result arrived as
// 584 inline tuples plus FETCH continuations, all rows in order.
func (r *Rows) fetchNext(ctx context.Context) error {
	w := protocol.NewWriter()
	w.RawByte(protocol.FnFetch)
	w.ArgInt(r.stmt.handle)
	w.ArgInt(r.nextIndex)                      // start cursor position
	w.ArgInt(int32(r.stmt.conn.cfg.FetchSize)) // rows to fetch
	w.ArgByte(0)                               // is_sensitive
	w.ArgInt(0)                                // result set index
	rd, err := r.stmt.conn.roundTrip(ctx, w.Bytes())
	if err != nil {
		return err
	}
	if _, err := protocol.ReadStatus(rd); err != nil {
		return err
	}
	r.tuples = r.tuples[:0]
	r.pos = 0
	return r.consumeFetch(rd)
}

// consumeFetch parses a fetch block: tuple count, then per tuple the
// cursor index and length-prefixed column values; trailing
// fetch-completed byte on V5+.
func (r *Rows) consumeFetch(rd *protocol.Reader) error {
	n, err := rd.Int32()
	if err != nil {
		return err
	}
	for i := 0; i < int(n); i++ {
		idx, err := rd.Int32()
		if err != nil {
			return err
		}
		r.nextIndex = idx + 1
		// Every tuple carries an 8-byte OID slot (zero-filled when the
		// statement was prepared without OID inclusion), JDBC
		// UStatement.readATuple reads it unconditionally.
		if _, err := rd.Bytes(8); err != nil {
			return err
		}
		row := make([]any, len(r.cols))
		for j := range r.cols {
			sz, err := rd.Int32()
			if err != nil {
				return err
			}
			if sz < 0 {
				row[j] = nil
				continue
			}
			data, err := rd.Bytes(int(sz))
			if err != nil {
				return err
			}
			var v any
			dt := r.cols[j].decodeType()
			if r.stmt.isCall() {
				// CALL/CALL_SP rows carry a per-value type prefix in
				// every column (JDBC UStatement.readTypeFromData
				// special-cases these command types).
				dt = protocol.TypeNull
			}
			switch dt {
			case protocol.TypeNull:
				// Columns declared U_TYPE_NULL carry per-value type
				// prefixes (live 9.3: schema-info DEFAULT column).
				v, err = protocol.DecodeSelfDescribedValue(data)
			case protocol.TypeBlob, protocol.TypeClob:
				// LOB column values are packed locators, not inline
				// data: surface them as read-only *Lob (JDBC
				// UInputBuffer.readBlob/readClob).
				v, err = resultLob(r.stmt.conn, dt, data)
			default:
				v, err = protocol.DecodeValue(dt, data)
			}
			if err != nil {
				return fmt.Errorf("cubrid: column %q: %w", r.cols[j].Name, err)
			}
			row[j] = v
		}
		r.tuples = append(r.tuples, row)
	}
	if r.stmt.conn.version.AtLeast(protocol.ProtoV5) && rd.Remaining() >= 1 {
		fc, err := rd.Byte()
		if err != nil {
			return err
		}
		r.fetchedAll = fc == 1
	} else if n == 0 {
		r.fetchedAll = true
	}
	return nil
}

// Close releases the iterator. Server-side cursor resources are freed when
// the statement closes or the transaction ends (cursor-close protocol op
// deferred to a later plan; not needed by any matrix version so far).
// Rows produced by Conn.SchemaInfo own their internal statement and
// release its server handle here.
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.ownsStmt {
		return r.stmt.Close(contextBackground())
	}
	return nil
}
