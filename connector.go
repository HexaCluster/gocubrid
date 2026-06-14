package cubridsql

import (
	"context"
	"database/sql/driver"

	"github.com/hexacluster/gocubrid/cubrid"
)

// Connector implements driver.Connector over a parsed native Config.
// Use it with sql.OpenDB to skip DSN re-parsing or to build the Config
// programmatically.
type Connector struct {
	cfg *cubrid.Config
}

var _ driver.Connector = (*Connector)(nil)

// NewConnector wraps a native Config for sql.OpenDB. The Config is used
// as-is for every connection the pool opens; do not mutate it afterwards.
func NewConnector(cfg *cubrid.Config) *Connector {
	return &Connector{cfg: cfg}
}

// Connect dials one native connection and wraps it for database/sql.
func (cn *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	nc, err := cubrid.Connect(ctx, cn.cfg)
	if err != nil {
		return nil, err
	}
	return &conn{nc: nc}, nil
}

// Driver returns the Driver the Connector belongs to.
func (cn *Connector) Driver() driver.Driver { return Driver{} }
