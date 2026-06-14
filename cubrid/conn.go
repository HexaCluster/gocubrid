package cubrid

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
)

var wireDebug = os.Getenv("CUBRID_WIRE_DEBUG") != ""

// Conn is a single connection to a CAS process. It owns one socket and
// must not be used concurrently from multiple goroutines.
type Conn struct {
	cfg *Config
	// host/port are the broker actually connected, cfg.Host:cfg.Port or
	// an AltHosts entry after failover. The handshake (redirect dial, TLS
	// ServerName, connection URL) must use these, never cfg directly.
	host string
	port int
	nc   net.Conn
	br   *bufio.Reader

	version   protocol.Version
	broker    protocol.BrokerInfo
	casPid    int32
	sessionID [protocol.SessionIDSize]byte

	autoCommit bool
	bad        bool
	closed     bool

	frames *frameSink
}

// frameSink, when non-nil, receives every post-connect request/response
// payload pair for golden-frame capture (enabled by CUBRID_FRAME_DIR).
// Files are numbered in wire order: a request dumps as NNN-req.bin and its
// response as NNN+1-res.bin, so replay can re-pair them by adjacency.
type frameSink struct {
	dir string
	seq int
}

func (c *Conn) dumpFrame(kind string, payload []byte) {
	if c.frames == nil {
		return
	}
	c.frames.seq++
	name := fmt.Sprintf("%03d-%s.bin", c.frames.seq, kind)
	_ = os.WriteFile(filepath.Join(c.frames.dir, name), payload, 0o644)
}

// defaultFailoverTimeout bounds each connection attempt during an
// AltHosts sweep when ConnectTimeout is unset: failover must not hang
// indefinitely on one dead host.
const defaultFailoverTimeout = 5 * time.Second

// Connect dials the broker and authenticates. With cfg.AltHosts set it
// tries Host:Port first and then each alternative in order, bounding
// every attempt by cfg.ConnectTimeout (5s when unset); the aggregate
// error reports each failed address.
func Connect(ctx context.Context, cfg *Config) (*Conn, error) {
	if len(cfg.AltHosts) == 0 {
		return connectHost(ctx, cfg, cfg.Host, cfg.Port, cfg.ConnectTimeout)
	}
	timeout := cfg.ConnectTimeout
	if timeout <= 0 {
		timeout = defaultFailoverTimeout
	}
	type hostPort struct {
		host string
		port int
	}
	targets := []hostPort{{cfg.Host, cfg.Port}}
	for _, alt := range cfg.AltHosts {
		host, port, err := splitAltHost(alt)
		if err != nil {
			return nil, err
		}
		targets = append(targets, hostPort{host, port})
	}
	var errs []error
	for _, tg := range targets {
		c, err := connectHost(ctx, cfg, tg.host, tg.port, timeout)
		if err == nil {
			return c, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", net.JoinHostPort(tg.host, strconv.Itoa(tg.port)), err))
		if ctx.Err() != nil {
			break // the caller's context is dead: stop sweeping
		}
	}
	return nil, fmt.Errorf("cubrid: all hosts unreachable: %w", errors.Join(errs...))
}

// connectHost dials one broker address and authenticates, bounding the
// whole attempt by timeout when positive.
func connectHost(ctx context.Context, cfg *Config, host string, port int, timeout time.Duration) (*Conn, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cubrid: dial %s: %w", addr, err)
	}
	c := &Conn{cfg: cfg, host: host, port: port, nc: nc, br: bufio.NewReader(nc), autoCommit: true}
	if dir := os.Getenv("CUBRID_FRAME_DIR"); dir != "" {
		c.frames = &frameSink{dir: dir}
	}
	if err := c.handshake(ctx); err != nil {
		// Close c.nc, not nc: handshake may have replaced the socket on a
		// CAS port redirect, in which case the original is already closed.
		c.nc.Close()
		return nil, err
	}
	return c, nil
}

func (c *Conn) handshake(ctx context.Context) error {
	defer c.guard(ctx)()
	if _, err := c.nc.Write(protocol.Greeting(c.cfg.SSL)); err != nil {
		return fmt.Errorf("cubrid: greeting: %w", err)
	}
	var port int32
	if err := binary.Read(c.br, binary.BigEndian, &port); err != nil {
		return fmt.Errorf("cubrid: greeting reply: %w", err)
	}
	if port < 0 {
		msg := "broker rejected connection"
		if port == protocol.CASErrSSLTypeNotAllowed {
			msg = "broker rejected the connection's SSL mode (ssl=true against an SSL=OFF broker, or plaintext against SSL=ON)"
		}
		return &Error{Code: port, Message: msg}
	}
	if port > 0 {
		// Windows-style CAS port redirect; Linux brokers pass the fd and
		// reply 0 (confirmed live across the 9.3 to 11.4 matrix, only 0
		// observed). The matrix has no Windows broker, so this branch is
		// untestable here: VERIFY-LIVE if a redirect is ever observed.
		c.nc.Close()
		var d net.Dialer
		nc, err := d.DialContext(ctx, "tcp", net.JoinHostPort(c.host, strconv.Itoa(int(port))))
		if err != nil {
			return fmt.Errorf("cubrid: redirect dial: %w", err)
		}
		c.nc, c.br = nc, bufio.NewReader(nc)
		defer c.guard(ctx)()
	}
	if c.cfg.SSL {
		// TLS upgrade sits between the plaintext greeting exchange and
		// dbInfo (after a port redirect it wraps the new socket with no
		// second greeting, JDBC BrokerHandler.connectBroker does the
		// same): auth and everything later travel encrypted. The SSL
		// broker on 11.4 also replied 0 (fd passing) in every live run,
		// so the redirect+TLS combination shares the Windows-only
		// VERIFY-LIVE above.
		tc := tls.Client(c.nc, &tls.Config{
			InsecureSkipVerify: !c.cfg.SSLVerify, // see Config.SSLVerify: default matches the JDBC accept-all ecosystem
			ServerName:         c.host,
		})
		if err := tc.HandshakeContext(ctx); err != nil {
			return fmt.Errorf("cubrid: TLS handshake: %w", err)
		}
		c.nc, c.br = tc, bufio.NewReader(tc)
	}
	url := fmt.Sprintf("cubrid://%s@%s:%d/%s", c.cfg.User, c.host, c.port, c.cfg.Database)
	db, err := protocol.DBInfo(c.cfg.Database, c.cfg.User, c.cfg.Password, url, nil)
	if err != nil {
		return err
	}
	if _, err := c.nc.Write(db); err != nil {
		return fmt.Errorf("cubrid: auth: %w", err)
	}
	payload, _, err := protocol.ReadResponse(c.br)
	if err != nil {
		return fmt.Errorf("cubrid: auth reply: %w", err)
	}
	rep, err := protocol.ParseConnectReply(payload)
	if err != nil {
		return err
	}
	c.version, c.broker = rep.Version, rep.Broker
	c.casPid, c.sessionID = rep.CASPid, rep.SessionID
	return nil
}

// guard applies ctx to the socket for the duration of one operation.
func (c *Conn) guard(ctx context.Context) func() {
	if dl, ok := ctx.Deadline(); ok {
		c.nc.SetDeadline(dl)
	} else {
		c.nc.SetDeadline(time.Time{})
	}
	stop := context.AfterFunc(ctx, func() { c.nc.SetDeadline(time.Unix(1, 0)) })
	return func() {
		stop()
		c.nc.SetDeadline(time.Time{})
	}
}

func (c *Conn) requestCasInfo() [4]byte {
	var ci [4]byte
	if c.autoCommit {
		ci[3] = protocol.CASInfoFlagAutocommit
	}
	return ci
}

// roundTrip sends one request payload and returns a reader over the
// response payload. Any transport failure poisons the connection.
func (c *Conn) roundTrip(ctx context.Context, payload []byte) (*protocol.Reader, error) {
	if c.closed || c.bad {
		return nil, ErrBadConn
	}
	defer c.guard(ctx)()
	if wireDebug {
		log.Printf("cubrid >>\n%s", hex.Dump(payload))
	}
	c.dumpFrame("req", payload)
	if err := protocol.WriteRequest(c.nc, payload, c.requestCasInfo()); err != nil {
		c.bad = true
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("cubrid: write: %w", err)
	}
	resp, _, err := protocol.ReadResponse(c.br)
	if err != nil {
		c.bad = true
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("cubrid: read: %w", err)
	}
	if wireDebug {
		log.Printf("cubrid <<\n%s", hex.Dump(resp))
	}
	c.dumpFrame("res", resp)
	return protocol.NewReader(resp), nil
}

// ProtocolVersion reports the broker-negotiated CAS protocol version.
func (c *Conn) ProtocolVersion() int { return int(c.version) }

// Valid reports whether the connection is usable: not closed and not
// poisoned by a mid-protocol failure (ErrBadConn state). It performs no
// wire operation; the database/sql adapter's driver.Validator uses it to
// decide pool reuse.
func (c *Conn) Valid() bool { return !c.bad && !c.closed }

// Ping checks broker liveness with CHECK_CAS.
func (c *Conn) Ping(ctx context.Context) error {
	w := protocol.NewWriter()
	w.RawByte(protocol.FnCheckCAS)
	r, err := c.roundTrip(ctx, w.Bytes())
	if err != nil {
		return err
	}
	// Live finding (11.4): a healthy CAS answers CHECK_CAS with an EMPTY
	// payload, no status int at all. Only a failing check carries a status.
	if r.Remaining() == 0 {
		return nil
	}
	_, err = protocol.ReadStatus(r)
	return err
}

// ServerVersion asks the broker for the CUBRID server version string
// (e.g. "11.4.5.1866..."). Use it to capability-gate features that track
// server releases (JSON >= 10.2, TZ types >= 10.0) as opposed to broker
// protocol versions.
func (c *Conn) ServerVersion(ctx context.Context) (string, error) {
	w := protocol.NewWriter()
	w.RawByte(protocol.FnGetDBVersion)
	w.ArgByte(boolByte(c.autoCommit)) // JDBC UConnection.getDatabaseProductVersion sends the autocommit flag
	r, err := c.roundTrip(ctx, w.Bytes())
	if err != nil {
		return "", err
	}
	if _, err := protocol.ReadStatus(r); err != nil {
		return "", err
	}
	// The version is the raw remainder of the payload, NUL-terminated
	// (JDBC reads remainedCapacity bytes), not a length-prefixed string.
	b, err := r.Bytes(r.Remaining())
	if err != nil {
		return "", err
	}
	for len(b) > 0 && b[len(b)-1] == 0 {
		b = b[:len(b)-1]
	}
	return string(b), nil
}

// Exec prepares, runs, and closes a statement in one call, convenient
// for DDL and one-shot DML.
func (c *Conn) Exec(ctx context.Context, sql string, args ...any) (*Result, error) {
	stmt, err := c.Prepare(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close(ctx)
	return stmt.Exec(ctx, args...)
}

// Close sends CON_CLOSE best-effort and closes the socket. Safe to call twice.
func (c *Conn) Close() error {
	if c.closed {
		return nil
	}
	if !c.bad {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		w := protocol.NewWriter()
		w.RawByte(protocol.FnConClose)
		_, _ = c.roundTrip(ctx, w.Bytes())
		cancel()
	}
	c.closed = true
	return c.nc.Close()
}

func contextBackground() context.Context { return context.Background() }
