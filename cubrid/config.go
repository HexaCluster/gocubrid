// Package cubrid is a pure Go native client for CUBRID. It speaks the CAS
// broker wire protocol directly; see the repository root for the
// database/sql driver built on top of it.
package cubrid

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Defaults applied when the Config (or DSN) does not say otherwise.
const (
	DefaultPort      = 33000 // standard CUBRID broker port
	DefaultFetchSize = 100   // rows pulled per FETCH round-trip
)

// Config holds connection parameters. Build it directly or via ParseDSN.
type Config struct {
	Host           string
	Port           int
	Database       string
	User           string
	Password       string
	FetchSize      int
	ConnectTimeout time.Duration

	// AltHosts lists standby brokers for HA failover (DSN:
	// altHosts=host2:33000,host3, the JDBC parameter name, kept
	// case-sensitive for ecosystem familiarity). Connect tries Host:Port
	// first, then each entry in order; entries are host or host:port
	// (port defaults to 33000). With AltHosts set, every attempt is
	// bounded by ConnectTimeout (or 5s when unset) so one dead host
	// cannot stall the sweep.
	AltHosts []string

	// SSL upgrades the connection to TLS immediately after the plaintext
	// greeting exchange (DSN: ssl=true). Requires a broker configured
	// with SSL=ON; a plaintext broker rejects the connection with
	// CAS_ER_SSL_TYPE_NOT_ALLOWED (-10103).
	SSL bool
	// SSLVerify verifies the broker certificate against the system roots
	// (DSN: ssl_verify=true). SECURITY: it defaults to FALSE to match the
	// CUBRID client ecosystem (the JDBC driver ships an accept-all trust
	// manager and brokers serve self-signed certificates out of the box),
	// so by default the link is encrypted but the broker is NOT
	// authenticated. Opt in when your broker has a CA-issued certificate.
	SSLVerify bool
}

// ParseDSN parses cubrid://user:pass@host:port/dbname?fetch_size=N&connect_timeout=5s
func ParseDSN(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("cubrid: invalid DSN: %w", err)
	}
	if u.Scheme != "cubrid" {
		return nil, fmt.Errorf("cubrid: DSN scheme must be cubrid://, got %q", u.Scheme)
	}
	cfg := &Config{
		Host:      u.Hostname(),
		Port:      DefaultPort,
		User:      "public",
		FetchSize: DefaultFetchSize,
	}
	if cfg.Host == "" {
		return nil, fmt.Errorf("cubrid: DSN missing host")
	}
	if p := u.Port(); p != "" {
		if cfg.Port, err = strconv.Atoi(p); err != nil {
			return nil, fmt.Errorf("cubrid: invalid port %q", p)
		}
	}
	cfg.Database = strings.TrimPrefix(u.Path, "/")
	if cfg.Database == "" || strings.Contains(cfg.Database, "/") {
		return nil, fmt.Errorf("cubrid: DSN must name exactly one database, got path %q", u.Path)
	}
	if u.User != nil {
		cfg.User = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}
	for k, vs := range u.Query() {
		v := vs[len(vs)-1]
		switch k {
		case "fetch_size":
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("cubrid: invalid fetch_size %q", v)
			}
			cfg.FetchSize = n
		case "connect_timeout":
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 {
				return nil, fmt.Errorf("cubrid: invalid connect_timeout %q", v)
			}
			cfg.ConnectTimeout = d
		case "ssl":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("cubrid: invalid ssl %q", v)
			}
			cfg.SSL = b
		case "altHosts":
			for _, e := range strings.Split(v, ",") {
				host, port, err := splitAltHost(strings.TrimSpace(e))
				if err != nil {
					return nil, err
				}
				cfg.AltHosts = append(cfg.AltHosts, net.JoinHostPort(host, strconv.Itoa(port)))
			}
		case "ssl_verify":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("cubrid: invalid ssl_verify %q", v)
			}
			cfg.SSLVerify = b
		default:
			return nil, fmt.Errorf("cubrid: unknown DSN parameter %q", k)
		}
	}
	return cfg, nil
}

// splitAltHost parses one AltHosts entry, host or host:port, IPv6
// literals always bracketed ("[::1]" or "[::1]:33099"), defaulting the
// port to DefaultPort. Shared by ParseDSN validation and the Connect
// sweep, so directly-built Configs get the same forms and errors as DSNs;
// parse -> net.JoinHostPort -> parse must stay a fixed point because the sweep
// re-parses the joined form (see TestSplitAltHostRoundTrip).
func splitAltHost(s string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		var aerr *net.AddrError
		if !errors.As(err, &aerr) || !strings.Contains(aerr.Err, "missing port") {
			return "", 0, fmt.Errorf("cubrid: invalid altHosts entry %q", s)
		}
		// Bare host, no port part at all: default it. A bracketed IPv6
		// literal ("[::1]") sheds its brackets so the returned host is the
		// raw address, both ParseDSN and the Connect dial re-join with
		// net.JoinHostPort, which brackets colon-bearing hosts itself.
		// (Returning s verbatim used to double-bracket the stored entry
		// into an undialable "[[::1]]:33000".)
		host = s
		if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
			host = host[1 : len(host)-1]
		}
		if host == "" {
			return "", 0, fmt.Errorf("cubrid: invalid altHosts entry %q: empty host", s)
		}
		if strings.ContainsAny(host, "[]") {
			return "", 0, fmt.Errorf("cubrid: invalid altHosts entry %q: stray bracket", s)
		}
		return host, DefaultPort, nil
	}
	if host == "" {
		return "", 0, fmt.Errorf("cubrid: invalid altHosts entry %q: empty host", s)
	}
	// An explicit colon demands an explicit valid port ("h:" is an error).
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("cubrid: invalid altHosts entry %q: bad port", s)
	}
	return host, port, nil
}
