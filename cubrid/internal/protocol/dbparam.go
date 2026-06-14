package protocol

// DB parameter ids for GET_DB_PARAMETER / SET_DB_PARAMETER (JDBC
// UConnection.DB_PARAM_*). Confirmed live across the 9.3 to 11.4 matrix:
// param 1 round-trips isolation levels, and a param 2 value of 1500
// makes a lock-conflicted UPDATE fail with server error -75 after ~1.5 s
// of waiting, so the value is in milliseconds on every broker. (GET
// TRANSACTION LOCK TIMEOUT cannot echo it back: through the CAS it
// prepares as a zero-column statement carrying no value.)
const (
	DBParamIsolationLevel int32 = 1
	DBParamLockTimeout    int32 = 2
)

// SetDBParameterRequest writes a SET_DB_PARAMETER request: [param id]
// [value], both int32 arguments. The response is a bare status (JDBC
// UConnection.setIsolationLevel / setLockTimeout).
func SetDBParameterRequest(w *Writer, param, value int32) {
	w.RawByte(FnSetDBParameter)
	w.ArgInt(param)
	w.ArgInt(value)
}

// GetDBParameterRequest writes a GET_DB_PARAMETER request: [param id].
// The response carries the value as a raw int32 after the status (JDBC
// UConnection.getIsolationLevel).
func GetDBParameterRequest(w *Writer, param int32) {
	w.RawByte(FnGetDBParameter)
	w.ArgInt(param)
}
