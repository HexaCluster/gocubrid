package cubridsql

import (
	"context"
	"database/sql/driver"

	"github.com/hexacluster/gocubrid/cubrid"
)

// tx adapts the native transaction control. database/sql guarantees one
// Commit or Rollback per BeginTx; autocommit is restored either way so the
// pooled connection returns in its neutral state.
type tx struct {
	nc *cubrid.Conn
}

var _ driver.Tx = (*tx)(nil)

func (t *tx) Commit() error {
	err := t.nc.Commit(context.Background())
	t.nc.SetAutoCommit(true)
	return mapErr(err)
}

func (t *tx) Rollback() error {
	err := t.nc.Rollback(context.Background())
	t.nc.SetAutoCommit(true)
	return mapErr(err)
}
