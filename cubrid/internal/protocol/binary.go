// Package protocol implements the CUBRID CAS broker wire protocol:
// big-endian framing, request argument packing, response field parsing.
package protocol

import (
	"encoding/binary"
	"errors"
	"math"
)

// Writer accumulates one request payload. The function code is a single
// raw byte; every argument is prefixed with an int32 byte length.
type Writer struct {
	buf []byte
}

func NewWriter() *Writer { return &Writer{buf: make([]byte, 0, 256)} }

func (w *Writer) Bytes() []byte { return w.buf }

func (w *Writer) RawByte(b byte) { w.buf = append(w.buf, b) }

func (w *Writer) be32(v uint32) { w.buf = binary.BigEndian.AppendUint32(w.buf, v) }

func (w *Writer) ArgInt(v int32) {
	w.be32(4)
	w.be32(uint32(v))
}

func (w *Writer) ArgByte(b byte) {
	w.be32(1)
	w.buf = append(w.buf, b)
}

func (w *Writer) ArgShort(v int16) {
	w.be32(2)
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(v))
}

func (w *Writer) ArgLong(v int64) {
	w.be32(8)
	w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(v))
}

func (w *Writer) ArgFloat(v float32) {
	w.be32(4)
	w.be32(math.Float32bits(v))
}

func (w *Writer) ArgDouble(v float64) {
	w.be32(8)
	w.buf = binary.BigEndian.AppendUint64(w.buf, math.Float64bits(v))
}

// ArgString writes [len+1][bytes][NUL]; the length includes the NUL.
func (w *Writer) ArgString(s string) {
	w.be32(uint32(len(s) + 1))
	w.buf = append(w.buf, s...)
	w.buf = append(w.buf, 0)
}

// ArgBytes writes [len][bytes] with no terminator.
func (w *Writer) ArgBytes(p []byte) {
	w.be32(uint32(len(p)))
	w.buf = append(w.buf, p...)
}

// ArgNull writes a null argument: length 0 and no data, matching JDBC
// UOutputBuffer.addNull. (NULL on the wire is -1 only in response values;
// request arguments use a zero length.)
func (w *Writer) ArgNull() { w.be32(0) }

// ErrShortBuffer reports a response payload shorter than its declared fields.
var ErrShortBuffer = errors.New("cubrid/protocol: short response buffer")

// Reader walks a raw-packed response payload.
type Reader struct {
	buf []byte
	off int
}

func NewReader(p []byte) *Reader { return &Reader{buf: p} }

func (r *Reader) Remaining() int { return len(r.buf) - r.off }

func (r *Reader) take(n int) ([]byte, error) {
	if n < 0 || r.off+n > len(r.buf) {
		return nil, ErrShortBuffer
	}
	b := r.buf[r.off : r.off+n]
	r.off += n
	return b, nil
}

func (r *Reader) Byte() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *Reader) Int16() (int16, error) {
	b, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func (r *Reader) Int32() (int32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func (r *Reader) Int64() (int64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

func (r *Reader) Bytes(n int) ([]byte, error) { return r.take(n) }

// String reads [int32 len][bytes], stripping one trailing NUL if present.
func (r *Reader) String() (string, error) {
	n, err := r.Int32()
	if err != nil {
		return "", err
	}
	if n <= 0 {
		return "", nil
	}
	b, err := r.take(int(n))
	if err != nil {
		return "", err
	}
	if b[len(b)-1] == 0 {
		b = b[:len(b)-1]
	}
	return string(b), nil
}
