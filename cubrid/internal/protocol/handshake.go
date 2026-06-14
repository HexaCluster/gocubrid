package protocol

import "fmt"

const (
	GreetingSize  = 10
	SessionIDSize = 20
	dbInfoSize    = 628

	// We present as the JDBC client class; brokers gate features on client
	// type. Confirmed live across the full matrix (9.3.9, 10.1.8, 10.2.18,
	// 11.0.16, 11.4.5): every broker accepts the greeting and serves the
	// complete integration suite.
	clientTypeJDBC = 3

	// CASErrSSLTypeNotAllowed is the greeting reply when the client's SSL
	// mode (CUBRS vs CUBRK magic) does not match the broker's SSL setting
	// (JDBC UErrorCode.CAS_ER_SSL_TYPE_NOT_ALLOWED). Confirmed live on
	// 11.4.5: a plaintext broker answers a CUBRS greeting with -10103.
	CASErrSSLTypeNotAllowed = -10103
)

// Greeting returns the 10-byte client hello sent on socket connect.
func Greeting(ssl bool) []byte {
	g := make([]byte, GreetingSize)
	copy(g, "CUBRK")
	if ssl {
		copy(g, "CUBRS")
	}
	g[5] = clientTypeJDBC
	g[6] = protoIndicator | CurrentProtocolVersion
	g[7] = FlagRenewedErrorCode | FlagHoldableResult
	return g
}

// DBInfo builds the fixed 628-byte auth block sent after the greeting:
// dbname[32] user[32] password[32] url[512] sessionID[20], NUL padded.
func DBInfo(dbname, user, password, url string, sessionID []byte) ([]byte, error) {
	put := func(b []byte, s, field string) error {
		if len(s) >= len(b) {
			return fmt.Errorf("cubrid: %s longer than %d bytes", field, len(b)-1)
		}
		copy(b, s)
		return nil
	}
	b := make([]byte, dbInfoSize)
	if err := put(b[0:32], dbname, "dbname"); err != nil {
		return nil, err
	}
	if err := put(b[32:64], user, "user"); err != nil {
		return nil, err
	}
	if err := put(b[64:96], password, "password"); err != nil {
		return nil, err
	}
	if err := put(b[96:608], url, "url"); err != nil {
		return nil, err
	}
	if sessionID != nil {
		copy(b[608:628], sessionID)
	}
	return b, nil
}

// ConnectReply is the parsed authentication response.
type ConnectReply struct {
	CASPid    int32
	Broker    BrokerInfo
	Version   Version
	CASID     int32
	SessionID [SessionIDSize]byte
}

func ParseConnectReply(payload []byte) (*ConnectReply, error) {
	r := NewReader(payload)
	pid, err := ReadStatus(r)
	if err != nil {
		return nil, err
	}
	rep := &ConnectReply{CASPid: pid}
	bb, err := r.Bytes(8)
	if err != nil {
		return nil, err
	}
	if rep.Broker, err = ParseBrokerInfo(bb); err != nil {
		return nil, err
	}
	rep.Version = rep.Broker.ProtocolVersion()
	if rep.Version.AtLeast(ProtoV4) {
		if rep.CASID, err = r.Int32(); err != nil {
			return nil, err
		}
	}
	if rep.Version.AtLeast(ProtoV3) {
		sid, err := r.Bytes(SessionIDSize)
		if err != nil {
			return nil, err
		}
		copy(rep.SessionID[:], sid)
	} else if r.Remaining() >= 4 {
		_, _ = r.Int32() // legacy 4-byte session id, unused
	}
	return rep, nil
}
