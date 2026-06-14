package cubrid

import (
	"context"
	"fmt"
	"io"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
)

// Lob is a BLOB or CLOB locator bound to its connection and transaction.
// Reads may be repeated at any offset; writes are append-only and only
// valid on Lobs created with NewBlob/NewClob before they are bound,
// locators read from result sets are read-only. Locators are
// transaction-scoped: after COMMIT/ROLLBACK the server may invalidate
// them, surfacing reads as *Error.
//
// Methods take a context.Context instead of implementing io.ReaderAt /
// io.Writer: every operation is one or more broker round-trips and must
// honor cancellation like the rest of the API.
type Lob struct {
	conn     *Conn
	handle   *protocol.LobHandle
	isClob   bool
	writable bool
}

// NewBlob creates an empty writable BLOB on the server.
func (c *Conn) NewBlob(ctx context.Context) (*Lob, error) {
	return c.newLob(ctx, protocol.TypeBlob)
}

// NewClob creates an empty writable CLOB on the server. CLOB content is
// byte-oriented on the wire; this driver reads and writes UTF-8.
func (c *Conn) NewClob(ctx context.Context) (*Lob, error) {
	return c.newLob(ctx, protocol.TypeClob)
}

func (c *Conn) newLob(ctx context.Context, typ int32) (*Lob, error) {
	w := protocol.NewWriter()
	protocol.NewLobRequest(w, typ)
	r, err := c.roundTrip(ctx, w.Bytes())
	if err != nil {
		return nil, err
	}
	n, err := protocol.ReadStatus(r) // status = packed handle byte length
	if err != nil {
		return nil, err
	}
	raw, err := r.Bytes(int(n))
	if err != nil {
		return nil, err
	}
	h, err := protocol.ParseLobHandle(raw)
	if err != nil {
		return nil, err
	}
	return &Lob{conn: c, handle: h, isClob: typ == protocol.TypeClob, writable: true}, nil
}

// resultLob wraps a result-column packed handle in a read-only Lob.
func resultLob(c *Conn, typ byte, packed []byte) (*Lob, error) {
	h, err := protocol.ParseLobHandle(packed)
	if err != nil {
		return nil, err
	}
	return &Lob{conn: c, handle: h, isClob: typ == protocol.TypeClob}, nil
}

// Size reports the LOB length in bytes as known client-side: the size
// carried by the locator, kept current across Append calls.
func (l *Lob) Size() int64 { return l.handle.Size }

// IsClob reports whether the locator is a CLOB (text) rather than a BLOB.
func (l *Lob) IsClob() bool { return l.isClob }

// ReadAt reads len(p) bytes at offset off, chunking round-trips at 128
// KiB. It follows the io.ReaderAt convention: n < len(p) only with a
// non-nil error, and reading past the end returns io.EOF. A short
// READ_LOB reply does NOT mean end-of-LOB, the live CAS caps one reply
// at ~80 KiB regardless of the 128 KiB request cap, so the loop keeps
// going until p is full and only a 0-byte reply is the end (matching
// JDBC CUBRIDBlob.getBytes, which loops until real_read_len == 0).
func (l *Lob) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("cubrid: negative LOB read offset %d", off)
	}
	n := 0
	for n < len(p) {
		chunk := min(len(p)-n, protocol.LobMaxChunk)
		w := protocol.NewWriter()
		protocol.ReadLobRequest(w, l.handle, off+int64(n), int32(chunk))
		r, err := l.conn.roundTrip(ctx, w.Bytes())
		if err != nil {
			return n, err
		}
		rc, err := protocol.ReadStatus(r) // status = bytes actually read
		if err != nil {
			return n, err
		}
		if int(rc) > chunk {
			return n, fmt.Errorf("cubrid: READ_LOB returned %d bytes for a %d-byte request", rc, chunk)
		}
		if rc == 0 { // nothing left: end of LOB
			return n, io.EOF
		}
		b, err := r.Bytes(int(rc))
		if err != nil {
			return n, err
		}
		n += copy(p[n:], b)
	}
	return n, nil
}

// Append writes p at the current end of the LOB (CAS writes are
// append-only), chunking at 128 KiB and advancing the locator size.
func (l *Lob) Append(ctx context.Context, p []byte) (int, error) {
	if !l.writable {
		return 0, fmt.Errorf("cubrid: LOB is read-only (locators from result sets cannot be written)")
	}
	n := 0
	for n < len(p) {
		chunk := min(len(p)-n, protocol.LobMaxChunk)
		w := protocol.NewWriter()
		protocol.WriteLobRequest(w, l.handle, l.handle.Size, p[n:n+chunk])
		r, err := l.conn.roundTrip(ctx, w.Bytes())
		if err != nil {
			return n, err
		}
		rc, err := protocol.ReadStatus(r) // status = bytes written
		if err != nil {
			return n, err
		}
		if rc <= 0 || int(rc) > chunk {
			return n, fmt.Errorf("cubrid: WRITE_LOB wrote %d bytes of a %d-byte chunk", rc, chunk)
		}
		n += int(rc)
		l.handle.SetSize(l.handle.Size + int64(rc))
	}
	return n, nil
}

// ReadAll reads the whole LOB from offset 0. Allocation grows with the
// bytes actually received, not the locator's declared size.
func (l *Lob) ReadAll(ctx context.Context) ([]byte, error) {
	var out []byte
	var off int64
	for off < l.handle.Size {
		chunk := min(l.handle.Size-off, protocol.LobMaxChunk)
		buf := make([]byte, chunk)
		n, err := l.ReadAt(ctx, buf, off)
		out = append(out, buf[:n]...)
		off += int64(n)
		if err == io.EOF || (err == nil && n == 0) {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// String reads the whole LOB as UTF-8 text, the CLOB convenience.
func (l *Lob) String(ctx context.Context) (string, error) {
	b, err := l.ReadAll(ctx)
	return string(b), err
}

// encodeParam writes the bind form: the LOB type byte then the packed
// handle (JDBC UOutputBuffer.addLob). A nil *Lob binds SQL NULL.
func (l *Lob) encodeParam(w *protocol.Writer) error {
	if l == nil {
		return protocol.EncodeParam(w, nil)
	}
	typ := byte(protocol.TypeBlob)
	if l.isClob {
		typ = protocol.TypeClob
	}
	w.ArgByte(typ)
	w.ArgBytes(l.handle.Packed())
	return nil
}
