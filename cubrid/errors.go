package cubrid

import (
	"errors"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
)

// Error is an error reported by the CAS broker or CUBRID server.
// Match with errors.As(err, &cubridErr).
type Error = protocol.Error

// ErrBadConn marks a connection that must not be reused, typically a
// context cancellation mid-protocol left the socket at an unknown wire
// position. The database/sql adapter translates it to driver.ErrBadConn
// so the pool discards the connection and retries elsewhere.
var ErrBadConn = errors.New("cubrid: connection in bad state")
