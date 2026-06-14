// Package cubridsql is the database/sql driver for CUBRID, registered
// under the driver name "cubrid":
//
//	import (
//		"database/sql"
//
//		_ "github.com/hexacluster/gocubrid"
//	)
//
//	db, err := sql.Open("cubrid", "cubrid://user:pass@host:33000/dbname")
//
// The module is split in two layers: this root package is the thin
// database/sql adapter (pooling and concurrency belong to database/sql),
// while the cubrid subpackage is the native client exposing the full
// protocol surface, schema introspection, LOB streaming, batch
// execution, savepoints, stored-procedure OUT parameters.
//
// Server and broker errors pass through the adapter unwrapped; match them
// with errors.As:
//
//	var cubridErr *cubrid.Error
//	if errors.As(err, &cubridErr) { ... cubridErr.Code ... }
//
// Result-set BLOB/CLOB columns are materialized eagerly as []byte up to a
// 1 MiB cap; larger LOBs return an error directing to the native API,
// which streams them in chunks. Result.LastInsertId is not supported
// (run SELECT LAST_INSERT_ID() instead).
package cubridsql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/hexacluster/gocubrid/cubrid"
)

func init() { sql.Register("cubrid", Driver{}) }

// Driver implements driver.Driver and driver.DriverContext over the
// native client. Most code never touches it: use sql.Open("cubrid", dsn)
// or sql.OpenDB(NewConnector(cfg)).
type Driver struct{}

var (
	_ driver.Driver        = Driver{}
	_ driver.DriverContext = Driver{}
)

// Open implements driver.Driver by parsing the DSN and dialing once.
func (d Driver) Open(dsn string) (driver.Conn, error) {
	cn, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return cn.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext: the DSN is parsed once,
// then every pooled connection dials from the same Config.
func (d Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := cubrid.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &Connector{cfg: cfg}, nil
}
