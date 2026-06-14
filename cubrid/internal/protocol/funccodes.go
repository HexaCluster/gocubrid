package protocol

// CAS function codes (request payload first byte).
const (
	FnEndTransaction    = 1
	FnPrepare           = 2
	FnExecute           = 3
	FnGetDBParameter    = 4
	FnSetDBParameter    = 5
	FnCloseStatement    = 6
	FnCursor            = 7
	FnFetch             = 8
	FnGetSchemaInfo     = 9
	FnGetDBVersion      = 15
	FnNextResult        = 19
	FnExecuteBatch      = 20
	FnExecuteBatchPrep  = 21
	FnSavepoint         = 26
	FnParameterInfo     = 27
	FnConClose          = 31
	FnCheckCAS          = 32
	FnMakeOutResultSet  = 33
	FnGetGeneratedKeys  = 34
	FnNewLOB            = 35
	FnWriteLOB          = 36
	FnReadLOB           = 37
	FnEndSession        = 38
	FnPrepareAndExecute = 41
	FnCursorClose       = 42
)
