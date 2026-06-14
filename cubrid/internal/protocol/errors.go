package protocol

import "fmt"

// Error is an error reported by the CAS broker or the CUBRID server.
type Error struct {
	Code    int32
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("cubrid error %d: %s", e.Code, e.Message)
}

// ReadStatus consumes the leading int32 result code of a response payload.
// Non-negative codes are returned as-is. Negative codes carry an error: with
// the renewed-error-code format (we always request it in the greeting) the
// real code follows the indicator and the message is the raw NUL-terminated
// remainder of the payload, NOT length-prefixed (JDBC UInputBuffer reads
// remainedCapacity bytes). Confirmed live on the whole matrix: every broker
// from 9.3.9 (proto V6) through 11.4.5 (proto V12) honors the greeting flag
// and replies in the renewed shape, see the server_error golden frames in
// testdata/frames/<ver>/. The fallback below (code stays in rc when no
// second int32 follows) is kept defensively for brokers that ignore the
// flag; it was never exercised live.
func ReadStatus(r *Reader) (int32, error) {
	rc, err := r.Int32()
	if err != nil {
		return 0, err
	}
	if rc >= 0 {
		return rc, nil
	}
	code := rc
	var msg string
	if r.Remaining() >= 4 {
		if c, cerr := r.Int32(); cerr == nil && c < 0 {
			code = c
		}
		if rest, rerr := r.Bytes(r.Remaining()); rerr == nil {
			msg = string(trimNul(rest))
		}
	}
	return 0, &Error{Code: code, Message: msg}
}
