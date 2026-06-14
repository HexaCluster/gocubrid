# cubrid-go

`github.com/hexacluster/gocubrid` is the first **pure Go** driver for
[CUBRID](https://www.cubrid.org/): zero cgo, zero dependencies beyond the
Go standard library. It implements the CUBRID CAS broker wire protocol
natively and ships two layers:

- the **root package** (`cubridsql`): a thin `database/sql` driver
  registered under the name `"cubrid"`;
- the **`cubrid` subpackage**: a native client exposing the full
  protocol surface: schema introspection, streaming LOBs, single
  round-trip batches, savepoints, stored-procedure OUT parameters, and
  isolation-level control.

The official `CUBRID/cubrid-go` driver is a cgo wrapper around the CCI C
client library; it needs the C library installed, ties your build to it,
and disables cross-compilation. This driver speaks the wire protocol
itself: `CGO_ENABLED=0` builds, static binaries, and
`context.Context`-aware cancellation on every network operation.

## Install

```sh
go get github.com/hexacluster/gocubrid
```

Requires Go 1.24+. No C toolchain, no CCI library, no other modules.

## Quick start (database/sql)

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/hexacluster/gocubrid" // registers driver "cubrid"
)

func main() {
	db, err := sql.Open("cubrid", "cubrid://dba:password@localhost:33000/demodb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT code, name FROM athlete WHERE gender = ? LIMIT 5", "M")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var code int64
		var name string
		if err := rows.Scan(&code, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Println(code, name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
```

To skip DSN re-parsing per connection (or to build the configuration
programmatically), use the connector:

```go
import (
	cubridsql "github.com/hexacluster/gocubrid"
	"github.com/hexacluster/gocubrid/cubrid"
)

cfg, err := cubrid.ParseDSN("cubrid://dba:password@localhost:33000/demodb")
// ... or fill in a &cubrid.Config{...} by hand
db := sql.OpenDB(cubridsql.NewConnector(cfg))
```

Server and broker errors pass through the adapter unwrapped: match them
with `errors.As`:

```go
var cerr *cubrid.Error
if errors.As(err, &cerr) {
	fmt.Println(cerr.Code, cerr.Message)
}
```

`NUMERIC` columns scan into `string` by default (the exact server
rendering, no float rounding); `cubrid.Numeric` is the opt-in exact
decimal target, and binds back as a parameter:

```go
var price cubrid.Numeric
err := db.QueryRow("SELECT price FROM item WHERE id = ?", 1).Scan(&price)
```

Runnable, compile-checked versions of these flows live in
[`example_test.go`](example_test.go).

## DSN reference

```
cubrid://user:password@host:port/database?param=value&...
```

The user defaults to `public`, the port to `33000` (the broker port:
CUBRID clients connect to a broker, not the database server). Reserved
characters in the password must be percent-encoded. Parameters:

| parameter         | default | meaning                                                                                                                                       |
|-------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `fetch_size`      | `100`   | rows pulled per FETCH round-trip                                                                                                              |
| `connect_timeout` | none    | dial + handshake budget per connection attempt, as a Go duration (`5s`, `500ms`)                                                              |
| `altHosts`        | none       | comma-separated standby brokers (`host` or `host:port`, port defaults to 33000) tried in order after the primary; see [HA failover](#althosts-ha-failover) |
| `ssl`             | `false` | upgrade the connection to TLS right after the plaintext greeting; the broker must run `SSL=ON`                                                |
| `ssl_verify`      | `false` | verify the broker certificate against the system roots; see [Limitations](#limitations) for why it defaults off                              |

Unknown parameters are rejected, not ignored.

### altHosts HA failover

```
cubrid://dba:pw@broker1:33000/demodb?altHosts=broker2:33000,broker3&connect_timeout=3s
```

`Connect` tries the primary first, then each alternative in order. Every
attempt is bounded by `connect_timeout` (5 s per attempt when unset, so a
dead primary cannot stall the sweep); if all hosts fail, the error
reports every attempted address.

## Native API tour

The `cubrid` subpackage is the full-power client: use it when
database/sql's common denominator is too narrow. One `*cubrid.Conn` is
one socket and is **not** goroutine-safe; pooling is what the root
package and database/sql are for.

```go
cfg, _ := cubrid.ParseDSN(dsn)
conn, err := cubrid.Connect(ctx, cfg)
defer conn.Close()
```

**Schema introspection**: typed helpers over GET_SCHEMA_INFO:
`Tables`, `Columns`, `PrimaryKey`, `ImportedKeys`, `ExportedKeys`, and
`Indexes` (which merges the primary key's backing index, something no
broker version reports in its constraint rows). `SchemaInfo` exposes the
raw rows for everything else.

```go
idxs, err := conn.Indexes(ctx, "athlete")
// []IndexInfo{ {Name: "pk_athlete_code", Unique: true, IsPrimary: true, Columns: ["code"]}, ... }
```

**LOB streaming**: BLOB/CLOB locators with chunked (<=128 KiB) reads and
appends; `ReadAt` follows the `io.ReaderAt` convention. Locators are
transaction-scoped, so do LOB work with autocommit off. Methods take a
`context.Context` instead of implementing `io.Reader`/`io.Writer`
because every call is one or more broker round-trips.

```go
blob, err := conn.NewBlob(ctx)
_, err = blob.Append(ctx, payload)                       // append-only writes
_, err = conn.Exec(ctx, "INSERT INTO t VALUES (?, ?)", id, blob)
// reading: SELECT returns *cubrid.Lob -> ReadAt / ReadAll / String
```

**Batch execution**: one round-trip for many statements; per-row errors
are isolated in `BatchResult.Err` and the server keeps executing past a
failed row.

```go
stmt, _ := conn.Prepare(ctx, "INSERT INTO t VALUES (?, ?)")
results, err := stmt.ExecBatch(ctx, [][]any{{1, "a"}, {2, "b"}})
results, err = conn.ExecBatchSQL(ctx, []string{"CREATE TABLE ...", "INSERT ..."})
```

**Savepoints**: `conn.Savepoint(ctx, "sp1")` /
`conn.RollbackToSavepoint(ctx, "sp1")` inside an explicit transaction.

**Stored procedures**: `PrepareCall` + `RegisterOutParam` +
`OutValues` for `CALL sp(?)` / `? = CALL fn(?)` with scalar OUT/INOUT
parameters:

```go
st, _ := conn.PrepareCall(ctx, "CALL inc_and_get(?)")
st.RegisterOutParam(1)
_, err = st.Exec(ctx, 41)
out := st.OutValues() // [42]
```

**Isolation and locking**: `SetIsolationLevel` / `IsolationLevel`
(READ COMMITTED, REPEATABLE READ, SERIALIZABLE) and `SetLockTimeout`.
Through database/sql, pass the level in `sql.TxOptions` to `BeginTx`.

**Collections**: SET/MULTISET/SEQUENCE columns decode to `[]any`, and a
`[]any` argument binds as a collection parameter.

## Supported CUBRID versions

The driver advertises broker protocol V12 and gates features at runtime
on what the broker negotiates down to: no per-version code forks. Every
release below is exercised live by the integration suite:

| CUBRID  | broker protocol | notes                                            |
|---------|-----------------|--------------------------------------------------|
| 9.3.9   | V6              | ENUM works; no TZ types, no JSON                 |
| 10.1.8  | V9              | + timezone types (`TIMESTAMPTZ`, `DATETIMETZ`, ...)|
| 10.2.18 | V10             | + JSON                                           |
| 11.0.16 | V10             | same surface as 10.2                             |
| 11.4.5  | V12             | primary target; SSL and Java SP verified here    |

Capability gates: timezone types need >=10.0, JSON needs >=10.2; SSL
brokers and Java stored procedures are verified on 11.x.

## Limitations

- **`Result.LastInsertId` is unsupported**: the protocol path used here
  does not return generated keys. Run `SELECT LAST_INSERT_ID()` on the
  same connection instead.
- **Named parameters are unsupported**: CUBRID binds by `?` ordinal
  only; `sql.Named` arguments are rejected with a clear error.
- **Read-only transactions are rejected**: CUBRID has no read-only
  transaction mode; `BeginTx` with `ReadOnly: true` errors.
- **LOB columns through database/sql materialize eagerly as `[]byte` up
  to 1 MiB.** Larger LOBs return an error directing you to the native
  API, which streams them in chunks instead of buffering.
- **RESULTSET-typed OUT parameters are unsupported** (scalar OUT/INOUT
  work); they fail with a clear error.
- **`ssl=true` encrypts but does not authenticate by default.** This
  matches the CUBRID client ecosystem: the JDBC driver ships an
  accept-all trust manager and brokers serve self-signed certificates
  out of the box. Set `ssl_verify=true` once your broker presents a
  CA-issued certificate.

## Testing

The driver is verified in rings, from hermetic to hostile:

- **Unit + golden frames** (`make test`): codec and protocol tests
  against byte-exact fake brokers, plus golden wire frames captured from
  all five live broker versions (`testdata/frames/`) replayed offline.
  No network needed; this is what CI runs.
- **Live integration matrix** (`make integration` / `make matrix`): the
  full suite against real brokers for every supported version, DSNs via
  `CUBRID_TEST_DSN_<ver>` environment variables.
- **Soak ring** (`make soak`): race-enabled pool storm (64 goroutines),
  100k-row streaming fetch, broker-restart recovery, and query-timeout
  storms.
- **Protocol fuzzing** (`make fuzz`): native Go fuzzing of every wire
  parser, corpora seeded from the golden frames; parsers must error
  cleanly, never panic or overallocate.

## Authors

- Suman Michael <michael@hexacluster.ai>

## License

BSD-3-Clause; see [LICENSE](LICENSE). Wire-protocol behavior was
implemented with reference to the BSD-licensed CUBRID JDBC driver: facts
only, no code copied; see [NOTICE](NOTICE).
