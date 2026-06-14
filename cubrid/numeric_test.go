package cubrid

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/hexacluster/gocubrid/cubrid/internal/protocol"
)

func TestNumericString(t *testing.T) {
	if got := Numeric("12.34").String(); got != "12.34" {
		t.Fatalf("String() = %q, want %q", got, "12.34")
	}
}

func TestNumericFloat64(t *testing.T) {
	f, err := Numeric("-0.50").Float64()
	if err != nil {
		t.Fatal(err)
	}
	if f != -0.5 {
		t.Fatalf("Float64() = %v, want -0.5", f)
	}
	if _, err := Numeric("not a number").Float64(); err == nil {
		t.Fatal("Float64() of garbage succeeded, want error")
	}
}

func TestNumericScanSources(t *testing.T) {
	tests := []struct {
		name string
		src  any
		want Numeric
	}{
		{"string", "12.34", "12.34"},
		{"bytes", []byte("-0.50"), "-0.50"},
		{"float64", float64(1.5), "1.5"},
		{"float64 no exponent", float64(1e21), "1000000000000000000000"},
		{"int64", int64(-42), "-42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n Numeric
			if err := n.Scan(tt.src); err != nil {
				t.Fatal(err)
			}
			if n != tt.want {
				t.Fatalf("Scan(%#v) = %q, want %q", tt.src, n, tt.want)
			}
		})
	}
}

func TestNumericScanRejects(t *testing.T) {
	tests := []struct {
		name string
		src  any
	}{
		{"NaN", math.NaN()},
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
		{"nil", nil},
		{"bool", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := Numeric("sentinel")
			if err := n.Scan(tt.src); err == nil {
				t.Fatalf("Scan(%#v) succeeded, want error", tt.src)
			}
			if n != "sentinel" {
				t.Fatalf("failed Scan overwrote the receiver: %q", n)
			}
		})
	}
}

func TestNumericScanNilErrorMentionsNull(t *testing.T) {
	var n Numeric
	err := n.Scan(nil)
	if err == nil {
		t.Fatal("Scan(nil) succeeded, want error")
	}
	if !strings.Contains(err.Error(), "NULL") {
		t.Fatalf("Scan(nil) error %q does not mention NULL", err)
	}
}

func TestNumericValue(t *testing.T) {
	v, err := Numeric("99.95").Value()
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.(string)
	if !ok || s != "99.95" {
		t.Fatalf("Value() = %#v, want string %q", v, "99.95")
	}
}

// A Numeric native bind must travel byte-identically to binding its
// string rendering (the server coerces VARCHAR to NUMERIC).
func TestNumericBindsAsString(t *testing.T) {
	got := protocol.NewWriter()
	if err := encodeArg(got, Numeric("12.34")); err != nil {
		t.Fatal(err)
	}
	want := protocol.NewWriter()
	if err := protocol.EncodeParam(want, "12.34"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatalf("Numeric bind bytes = % x, want % x", got.Bytes(), want.Bytes())
	}
}
