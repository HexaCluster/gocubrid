package protocol

import "fmt"

// CAS protocol versions. The driver advertises CurrentProtocolVersion; the
// broker's handshake reply carries the version actually spoken.
const (
	ProtoV0  = 0
	ProtoV1  = 1
	ProtoV2  = 2
	ProtoV3  = 3
	ProtoV4  = 4
	ProtoV5  = 5
	ProtoV6  = 6
	ProtoV7  = 7
	ProtoV8  = 8
	ProtoV9  = 9
	ProtoV11 = 11
	ProtoV12 = 12

	CurrentProtocolVersion = ProtoV12
)

// Greeting capability bits.
const (
	protoIndicator       = 0x40
	protoVerMask         = 0x3F
	FlagRenewedErrorCode = 0x80
	FlagHoldableResult   = 0x40
)

// Version is the broker's negotiated protocol version.
type Version int

func (v Version) AtLeast(n int) bool { return int(v) >= n }

// BrokerInfo is the 8-byte broker metadata block from the connect reply.
// Byte 4 carries the protocol version (index per JDBC
// BROKER_INFO_PROTO_VERSION; confirmed live, the 11.4 broker sets the
// 0x40 indicator and negotiates V12).
type BrokerInfo [8]byte

func ParseBrokerInfo(p []byte) (BrokerInfo, error) {
	var bi BrokerInfo
	if len(p) != len(bi) {
		return bi, fmt.Errorf("cubrid/protocol: broker info is %d bytes, want %d", len(p), len(bi))
	}
	copy(bi[:], p)
	return bi, nil
}

func (bi BrokerInfo) ProtocolVersion() Version {
	v := bi[4]
	if v&protoIndicator != 0 {
		return Version(v & protoVerMask)
	}
	return Version(ProtoV0)
}
