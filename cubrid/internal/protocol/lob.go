package protocol

import (
	"encoding/binary"
	"fmt"
)

// LobMaxChunk is the largest READ_LOB/WRITE_LOB transfer the CAS accepts
// in one round-trip (JDBC CUBRIDBlob.BLOB_MAX_IO_LENGTH, 128 KiB).
const LobMaxChunk = 128 * 1024

// lobHandleHeader is the fixed prefix of a packed LOB handle:
// db_type int32 + lob_size int64 + locator_len int32.
const lobHandleHeader = 16

// LobHandle is a parsed packed LOB locator. The packed layout is
// [0:4 db_type int32][4:12 lob_size int64][12:16 locator_len int32]
// [16: locator bytes incl trailing NUL] (JDBC CUBRIDLobHandle).
type LobHandle struct {
	DBType  int32
	Size    int64
	Locator string
	raw     []byte
}

// ParseLobHandle parses (and copies) a packed handle from a NEW_LOB
// response or a result-column value.
func ParseLobHandle(b []byte) (*LobHandle, error) {
	if len(b) < lobHandleHeader {
		return nil, fmt.Errorf("cubrid/protocol: packed LOB handle is %d bytes, want >= %d", len(b), lobHandleHeader)
	}
	locLen := int32(binary.BigEndian.Uint32(b[12:16]))
	if locLen < 1 || int64(lobHandleHeader)+int64(locLen) > int64(len(b)) {
		return nil, fmt.Errorf("cubrid/protocol: LOB locator length %d exceeds handle (%d bytes)", locLen, len(b))
	}
	h := &LobHandle{
		DBType:  int32(binary.BigEndian.Uint32(b[0:4])),
		Size:    int64(binary.BigEndian.Uint64(b[4:12])),
		Locator: string(trimNul(b[lobHandleHeader : lobHandleHeader+int(locLen)])),
		raw:     append([]byte(nil), b...),
	}
	return h, nil
}

// SetSize records a new LOB size both in the struct and in the packed
// bytes (the server reads the size from the handle we send back), per
// JDBC CUBRIDLobHandle.setLobSize.
func (h *LobHandle) SetSize(n int64) {
	h.Size = n
	binary.BigEndian.PutUint64(h.raw[4:12], uint64(n))
}

// Packed returns the wire form of the handle for requests and binds.
func (h *LobHandle) Packed() []byte { return h.raw }

// NewLobRequest writes a NEW_LOB request creating an empty LOB of the
// given type (TypeBlob or TypeClob). The response status is the packed
// handle's byte length; the payload is the handle itself.
func NewLobRequest(w *Writer, typ int32) {
	w.RawByte(FnNewLOB)
	w.ArgInt(typ)
}

// WriteLobRequest writes a WRITE_LOB request appending data at offset
// (writes are append-only: offset must equal the current size). The
// response status is the number of bytes written. len(data) must not
// exceed LobMaxChunk.
func WriteLobRequest(w *Writer, h *LobHandle, offset int64, data []byte) {
	w.RawByte(FnWriteLOB)
	w.ArgBytes(h.raw)
	w.ArgLong(offset)
	w.ArgBytes(data)
}

// ReadLobRequest writes a READ_LOB request for n bytes at offset. The
// response status is the byte count actually read (short at LOB end);
// the payload is the raw bytes. n must not exceed LobMaxChunk.
func ReadLobRequest(w *Writer, h *LobHandle, offset int64, n int32) {
	w.RawByte(FnReadLOB)
	w.ArgBytes(h.raw)
	w.ArgLong(offset)
	w.ArgInt(n)
}
