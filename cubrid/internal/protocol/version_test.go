package protocol

import "testing"

func TestBrokerInfoProtocolVersion(t *testing.T) {
	bi, err := ParseBrokerInfo([]byte{0, 1, 1, 0, 0x4C, 0, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	if v := bi.ProtocolVersion(); v != ProtoV12 {
		t.Fatalf("version = %d, want 12", v)
	}
	if !bi.ProtocolVersion().AtLeast(ProtoV4) {
		t.Fatal("AtLeast(V4) should hold for V12")
	}
}

func TestBrokerInfoLegacy(t *testing.T) {
	bi, _ := ParseBrokerInfo([]byte{0, 1, 1, 0, 0x00, 0, 0, 0})
	if v := bi.ProtocolVersion(); v != ProtoV0 {
		t.Fatalf("version = %d, want 0", v)
	}
}

func TestParseBrokerInfoLength(t *testing.T) {
	if _, err := ParseBrokerInfo([]byte{1, 2, 3}); err == nil {
		t.Fatal("want error for short broker info")
	}
}
