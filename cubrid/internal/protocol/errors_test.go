package protocol

import (
	"errors"
	"testing"
)

func TestReadStatusOK(t *testing.T) {
	r := NewReader([]byte{0, 0, 0, 5})
	rc, err := ReadStatus(r)
	if err != nil || rc != 5 {
		t.Fatalf("rc=%d err=%v", rc, err)
	}
}

func TestReadStatusError(t *testing.T) {
	// Live 11.4 layout: rc=-2 DBMS-error indicator, renewed error code -493,
	// then the message as the raw NUL-terminated remainder of the payload,
	// NOT length-prefixed (JDBC UInputBuffer reads remainedCapacity bytes).
	p := []byte{0xFF, 0xFF, 0xFF, 0xFE, 0xFF, 0xFF, 0xFE, 0x13}
	p = append(p, []byte("Syntax error\x00")...)
	r := NewReader(p)
	_, err := ReadStatus(r)
	var cubErr *Error
	if !errors.As(err, &cubErr) {
		t.Fatalf("want *Error, got %T: %v", err, err)
	}
	if cubErr.Code != -493 || cubErr.Message != "Syntax error" {
		t.Fatalf("got %+v", cubErr)
	}
}
