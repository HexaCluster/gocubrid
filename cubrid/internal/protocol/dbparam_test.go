package protocol

import (
	"bytes"
	"testing"
)

// Request layouts per JDBC UConnection.setIsolationLevel/setLockTimeout/
// getIsolationLevel: SET_DB_PARAMETER (fn 5) packs [param id int][value int];
// GET_DB_PARAMETER (fn 4) packs [param id int] and the response carries the
// value as a raw int32 after the status.
func TestSetDBParameterRequest(t *testing.T) {
	w := NewWriter()
	SetDBParameterRequest(w, DBParamIsolationLevel, 4)

	want := []byte{
		FnSetDBParameter,
		0, 0, 0, 4, 0, 0, 0, 1, // param id: isolation level
		0, 0, 0, 4, 0, 0, 0, 4, // value: TRAN_READ_COMMITTED
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

func TestSetDBParameterRequestNegativeValue(t *testing.T) {
	w := NewWriter()
	SetDBParameterRequest(w, DBParamLockTimeout, -1)

	want := []byte{
		FnSetDBParameter,
		0, 0, 0, 4, 0, 0, 0, 2, // param id: lock timeout
		0, 0, 0, 4, 0xFF, 0xFF, 0xFF, 0xFF, // value: -1 = infinite wait
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

func TestGetDBParameterRequest(t *testing.T) {
	w := NewWriter()
	GetDBParameterRequest(w, DBParamIsolationLevel)

	want := []byte{
		FnGetDBParameter,
		0, 0, 0, 4, 0, 0, 0, 1, // param id: isolation level
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}
