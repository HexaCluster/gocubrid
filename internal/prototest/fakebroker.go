// Package prototest provides an in-process scripted CAS broker so the
// native client can be unit-tested without a CUBRID server.
package prototest

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"
)

// FakeBroker answers the greeting with code 0 (same-socket session, the
// Linux fd-passing path), accepts any credentials, then serves queued
// responses keyed by function code. Unqueued requests get a generic OK.
type FakeBroker struct {
	t  *testing.T
	ln net.Listener

	mu       sync.Mutex
	queues   map[byte][][]byte
	requests [][]byte
	greeting []byte
	tlsConf  *tls.Config

	BrokerInfo [8]byte
	CASPid     int32
}

func Start(t *testing.T) *FakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fb := &FakeBroker{
		t:          t,
		ln:         ln,
		queues:     map[byte][][]byte{},
		BrokerInfo: [8]byte{0, 1, 1, 0, 0x4C, 0, 0, 0}, // protocol V12
		CASPid:     4242,
	}
	go fb.serve()
	t.Cleanup(func() { ln.Close() })
	return fb
}

func (f *FakeBroker) Addr() string { return f.ln.Addr().String() }

// SetProtocolVersion rewrites the handshake BrokerInfo to advertise the
// given CAS protocol version (with the 0x40 indicator bit, as real brokers
// send). Call before the client connects; the session goroutine reads
// BrokerInfo under the broker mutex.
func (f *FakeBroker) SetProtocolVersion(v int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.BrokerInfo[4] = 0x40 | byte(v&0x3F)
}

// EnableTLS makes the broker honor SSL greetings the way a real SSL=ON
// broker does: the int32 0 reply stays plaintext, then the session is
// wrapped in tls.Server before dbInfo. The certificate is a fresh
// self-signed one (live brokers also serve self-signed certs out of the
// box), so only ssl_verify=false clients can complete the handshake.
// Call before the client connects.
func (f *FakeBroker) EnableTLS() {
	f.t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		f.t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fakebroker"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		f.t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tlsConf = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	}
}

// Greeting returns the 10-byte client hello of the most recent session.
func (f *FakeBroker) Greeting() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.greeting...)
}

// Queue registers the next response payload for a function code (FIFO).
func (f *FakeBroker) Queue(fn byte, payload []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queues[fn] = append(f.queues[fn], payload)
}

// Requests returns the post-connect request payloads received so far.
func (f *FakeBroker) Requests() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.requests))
	for i, r := range f.requests {
		out[i] = append([]byte(nil), r...)
	}
	return out
}

func (f *FakeBroker) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.session(c)
	}
}

func (f *FakeBroker) session(c net.Conn) {
	defer c.Close()
	greeting := make([]byte, 10)
	if _, err := io.ReadFull(c, greeting); err != nil {
		return
	}
	f.mu.Lock()
	f.greeting = append([]byte(nil), greeting...)
	tlsConf := f.tlsConf
	f.mu.Unlock()
	ssl := bytes.HasPrefix(greeting, []byte("CUBRS"))
	if ssl && tlsConf == nil {
		// Mirror a plaintext broker refusing an SSL client: confirmed
		// live on 11.4.5 (SSL=OFF broker answers CUBRS with -10103,
		// CAS_ER_SSL_TYPE_NOT_ALLOWED), see TestIntegrationSSLRejected.
		binary.Write(c, binary.BigEndian, int32(-10103))
		return
	}
	binary.Write(c, binary.BigEndian, int32(0))
	if ssl {
		tc := tls.Server(c, tlsConf)
		if err := tc.Handshake(); err != nil {
			return
		}
		c = tc
	}
	dbInfo := make([]byte, 628)
	if _, err := io.ReadFull(c, dbInfo); err != nil {
		return
	}
	// Snapshot handshake fields under the mutex: SetProtocolVersion may
	// have rewritten BrokerInfo from the test goroutine.
	f.mu.Lock()
	brokerInfo, casPid := f.BrokerInfo, f.CASPid
	f.mu.Unlock()
	var p bytes.Buffer
	binary.Write(&p, binary.BigEndian, casPid)
	p.Write(brokerInfo[:])
	binary.Write(&p, binary.BigEndian, int32(7)) // CAS id (V4+)
	p.Write(make([]byte, 20))                    // session id (V3+)
	writeFrame(c, p.Bytes())

	for {
		hdr := make([]byte, 8)
		if _, err := io.ReadFull(c, hdr); err != nil {
			return
		}
		payload := make([]byte, binary.BigEndian.Uint32(hdr[:4]))
		if _, err := io.ReadFull(c, payload); err != nil {
			return
		}
		if len(payload) == 0 {
			return
		}
		fn := payload[0]
		f.mu.Lock()
		f.requests = append(f.requests, payload)
		var resp []byte
		if q := f.queues[fn]; len(q) > 0 {
			resp, f.queues[fn] = q[0], q[1:]
		} else {
			resp = []byte{0, 0, 0, 0} // generic OK status
		}
		f.mu.Unlock()
		writeFrame(c, resp)
		if fn == 31 { // CON_CLOSE
			return
		}
	}
}

func writeFrame(c net.Conn, payload []byte) {
	hdr := make([]byte, 8)
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)))
	hdr[4] = 1 // CAS status: active
	c.Write(hdr)
	c.Write(payload)
}

// RespWriter builds response payloads for Queue (raw-packed fields).
type RespWriter struct{ buf bytes.Buffer }

func (w *RespWriter) Bytes() []byte { return w.buf.Bytes() }

func (w *RespWriter) B(v byte) *RespWriter { w.buf.WriteByte(v); return w }

func (w *RespWriter) I16(v int16) *RespWriter {
	binary.Write(&w.buf, binary.BigEndian, v)
	return w
}

func (w *RespWriter) I32(v int32) *RespWriter {
	binary.Write(&w.buf, binary.BigEndian, v)
	return w
}

func (w *RespWriter) I64(v int64) *RespWriter {
	binary.Write(&w.buf, binary.BigEndian, v)
	return w
}

// Str writes a response string: [int32 len incl NUL][bytes][NUL].
func (w *RespWriter) Str(s string) *RespWriter {
	binary.Write(&w.buf, binary.BigEndian, int32(len(s)+1))
	w.buf.WriteString(s)
	w.buf.WriteByte(0)
	return w
}

func (w *RespWriter) Raw(p []byte) *RespWriter { w.buf.Write(p); return w }
