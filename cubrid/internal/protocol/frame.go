package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// maxPayloadSize guards against corrupt length headers (256 MiB).
const maxPayloadSize = 1 << 28

// casInfo response flag bits (byte 3).
const (
	CASInfoFlagAutocommit   = 0x01
	CASInfoFlagForceOutTran = 0x02
	CASInfoFlagNewSessionID = 0x04
)

// WriteRequest frames payload with the 8-byte header and writes it.
func WriteRequest(w io.Writer, payload []byte, casInfo [4]byte) error {
	hdr := make([]byte, 8, 8+len(payload))
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	copy(hdr[4:8], casInfo[:])
	_, err := w.Write(append(hdr, payload...))
	return err
}

// ReadResponse reads one framed response: header then payload.
func ReadResponse(r io.Reader) ([]byte, [4]byte, error) {
	var casInfo [4]byte
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, casInfo, err
	}
	n := binary.BigEndian.Uint32(hdr[0:4])
	if n > maxPayloadSize {
		return nil, casInfo, fmt.Errorf("cubrid/protocol: payload length %d exceeds limit", n)
	}
	copy(casInfo[:], hdr[4:8])
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, casInfo, err
	}
	return payload, casInfo, nil
}
