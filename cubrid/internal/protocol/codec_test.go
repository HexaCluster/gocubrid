package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestDecodeScalars(t *testing.T) {
	cases := []struct {
		name string
		typ  byte
		data []byte
		want any
	}{
		{"int", TypeInt, []byte{0, 0, 0, 7}, int32(7)},
		{"short", TypeShort, []byte{0xFF, 0xFE}, int16(-2)},
		{"bigint", TypeBigint, []byte{0, 0, 0, 0, 0, 0, 0, 9}, int64(9)},
		{"float", TypeFloat, []byte{0x40, 0x20, 0, 0}, float32(2.5)},
		{"double", TypeDouble, []byte{0x3F, 0xF8, 0, 0, 0, 0, 0, 0}, 1.5},
		{"monetary", TypeMonetary, []byte{0x3F, 0xF8, 0, 0, 0, 0, 0, 0}, 1.5},
		{"varchar", TypeVarchar, []byte("hi\x00"), "hi"},
		{"numeric", TypeNumeric, []byte("12.34\x00"), "12.34"},
		// Confirmed live on 11.4 (TestIntegrationEnumAndJSON): ENUM values
		// arrive as the NUL-terminated label string, not the 1-based index.
		{"enum", TypeEnum, []byte("green\x00"), "green"},
		{"varbit", TypeVarbit, []byte{0xDE, 0xAD}, []byte{0xDE, 0xAD}},
		{"datetime", TypeDatetime,
			[]byte{0x07, 0xD9, 0, 6, 0, 15, 0, 13, 0, 45, 0, 30, 0x00, 0xFA},
			time.Date(2009, 6, 15, 13, 45, 30, 250e6, time.UTC)},
		// Live 11.4 layouts (JDBC UInputBuffer readDate/readTime/readTimestamp):
		// DATE and TIME are 3 shorts, TIMESTAMP is 6 shorts.
		{"date", TypeDate,
			[]byte{0x07, 0xD9, 0, 6, 0, 15},
			time.Date(2009, 6, 15, 0, 0, 0, 0, time.UTC)},
		{"time", TypeTime,
			[]byte{0, 13, 0, 45, 0, 30},
			time.Date(1, 1, 1, 13, 45, 30, 0, time.UTC)},
		{"timestamp", TypeTimestamp,
			[]byte{0x07, 0xD9, 0, 6, 0, 15, 0, 13, 0, 45, 0, 30},
			time.Date(2009, 6, 15, 13, 45, 30, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeValue(tc.typ, tc.data)
			if err != nil {
				t.Fatal(err)
			}
			if b, ok := tc.want.([]byte); ok {
				if !bytes.Equal(got.([]byte), b) {
					t.Fatalf("got % X want % X", got, b)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("got %#v want %#v", got, tc.want)
			}
		})
	}
}

// Wire truth captured live on 11.4: the shorts are wall-clock fields IN
// the attached zone, and the zone string is "<IANA name> <abbrev>" with a
// trailing NUL ("Asia/Seoul KST\x00" for TIMESTAMPTZ'... Asia/Seoul').
func TestDecodeTimestampTZ(t *testing.T) {
	b := []byte{
		0x07, 0xEA, 0, 6, 0, 11, 0, 13, 0, 30, 0, 0, // 2026-06-11 13:30:00 wall (KST)
	}
	b = append(b, []byte("Asia/Seoul KST\x00")...)
	got, err := DecodeValue(TypeTimestampTZ, b)
	if err != nil {
		t.Fatal(err)
	}
	ts := got.(time.Time)
	if !ts.Equal(time.Date(2026, 6, 11, 4, 30, 0, 0, time.UTC)) { // 13:30 KST
		t.Fatalf("instant = %v", ts)
	}
	if ts.Location().String() != "Asia/Seoul" {
		t.Fatalf("location = %v", ts.Location())
	}
}

func TestDecodeDatetimeTZOffsetZone(t *testing.T) {
	b := []byte{
		0x07, 0xEA, 0, 6, 0, 11, 0, 13, 0, 30, 0, 15, 0x00, 0xFA, // + 250ms
	}
	b = append(b, []byte("+09:00\x00")...)
	got, err := DecodeValue(TypeDatetimeTZ, b)
	if err != nil {
		t.Fatal(err)
	}
	ts := got.(time.Time)
	_, off := ts.Zone()
	if off != 9*3600 {
		t.Fatalf("offset = %d", off)
	}
	want := time.Date(2026, 6, 11, 4, 30, 15, 250e6, time.UTC) // 13:30:15.250 +09:00
	if !ts.Equal(want) {
		t.Fatalf("instant = %v, want %v", ts, want)
	}
}

// LTZ codes share the TZ wire layout (JDBC dispatches 29/30 and 31/32 to
// the same readers); unresolvable abbreviation-only zones keep the wall
// fields and degrade the location to UTC.
func TestDecodeTimestampLTZUnknownZone(t *testing.T) {
	b := []byte{
		0x07, 0xEA, 0, 6, 0, 11, 0, 4, 0, 30, 0, 0,
	}
	b = append(b, []byte("No/Such_Zone\x00")...)
	got, err := DecodeValue(TypeTimestampLTZ, b)
	if err != nil {
		t.Fatal(err)
	}
	ts := got.(time.Time)
	if !ts.Equal(time.Date(2026, 6, 11, 4, 30, 0, 0, time.UTC)) {
		t.Fatalf("instant = %v", ts)
	}
	if ts.Location() != time.UTC {
		t.Fatalf("location = %v, want UTC fallback", ts.Location())
	}
}

// Collection wire layout per JDBC UStatement.readData (U_TYPE_SET branch):
// element-type byte, element count int32, then per element
// [size int32][element bytes]; size <= 0 is a NULL element.
func TestDecodeCollectionOfInt(t *testing.T) {
	b := []byte{TypeInt}
	b = append(b, 0, 0, 0, 3)             // element count
	b = append(b, 0, 0, 0, 4, 0, 0, 0, 7) // [size=4][int 7]
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF) // NULL element (size -1)
	b = append(b, 0, 0, 0, 4, 0, 0, 0, 9)
	for _, typ := range []byte{TypeSet, TypeMultiset, TypeSequence} {
		got, err := DecodeValue(typ, b)
		if err != nil {
			t.Fatal(err)
		}
		vs := got.([]any)
		if len(vs) != 3 || vs[0].(int32) != 7 || vs[1] != nil || vs[2].(int32) != 9 {
			t.Fatalf("type %d: got %#v", typ, vs)
		}
	}
}

func TestDecodeCollectionOfString(t *testing.T) {
	b := []byte{TypeVarchar}
	b = append(b, 0, 0, 0, 2)
	b = append(b, 0, 0, 0, 2)
	b = append(b, []byte("a\x00")...)
	b = append(b, 0, 0, 0, 2)
	b = append(b, []byte("b\x00")...)
	got, err := DecodeValue(TypeSequence, b)
	if err != nil {
		t.Fatal(err)
	}
	vs := got.([]any)
	if len(vs) != 2 || vs[0].(string) != "a" || vs[1].(string) != "b" {
		t.Fatalf("got %#v", vs)
	}
}

func TestDecodeCollectionEmpty(t *testing.T) {
	got, err := DecodeValue(TypeSet, []byte{TypeInt, 0, 0, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	if vs := got.([]any); len(vs) != 0 {
		t.Fatalf("got %#v, want empty", vs)
	}
}

func TestDecodeCollectionTruncated(t *testing.T) {
	cases := [][]byte{
		{},                                      // missing element type
		{TypeInt, 0, 0},                         // short count
		{TypeInt, 0, 0, 0, 1},                   // count promises an element, none present
		{TypeInt, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0}, // element shorter than its size
	}
	for i, b := range cases {
		if _, err := DecodeValue(TypeSet, b); err == nil {
			t.Fatalf("case %d: want error for truncated collection", i)
		}
	}
}

// CUBRID rejects nested collections only in column DDL ("nested data type
// definition"); nested collection VALUES are legal expression results.
// Live 11.4 evaluates SELECT {{1,2},{3,4}} and serializes it with
// element-type byte 18 (SEQUENCE) wrapping inner [TypeInt] collections,
// confirmed via CUBRID_WIRE_DEBUG hex dump; JDBC decodes it via recursive
// readData (UStatement, U_TYPE_SET branch). This pins the depth-2 layout.
func TestDecodeCollectionNestedElements(t *testing.T) {
	inner := func(vals ...int32) []byte {
		b := []byte{TypeInt, 0, 0, 0, byte(len(vals))}
		for _, v := range vals {
			b = append(b, 0, 0, 0, 4)
			b = binary.BigEndian.AppendUint32(b, uint32(v))
		}
		return b
	}
	for _, et := range []byte{TypeSet, TypeMultiset, TypeSequence} {
		i1, i2 := inner(1, 2), inner(3, 4)
		b := []byte{et, 0, 0, 0, 2}
		b = binary.BigEndian.AppendUint32(b, uint32(len(i1)))
		b = append(b, i1...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(i2)))
		b = append(b, i2...)
		got, err := DecodeValue(TypeSequence, b)
		if err != nil {
			t.Fatalf("element type %d: %v", et, err)
		}
		vs := got.([]any)
		if len(vs) != 2 {
			t.Fatalf("element type %d: got %#v, want 2 nested collections", et, vs)
		}
		a, b2 := vs[0].([]any), vs[1].([]any)
		if len(a) != 2 || a[0].(int32) != 1 || a[1].(int32) != 2 ||
			len(b2) != 2 || b2[0].(int32) != 3 || b2[1].(int32) != 4 {
			t.Fatalf("element type %d: got %#v, want [[1 2] [3 4]]", et, vs)
		}
	}
}

// nestedChain builds a collection payload nested depth levels deep: depth 1
// is a flat empty INT collection, each further level wraps the previous as
// the single element of a SET.
func nestedChain(depth int) []byte {
	b := []byte{TypeInt, 0, 0, 0, 0}
	for i := 1; i < depth; i++ {
		inner := b
		b = []byte{TypeSet, 0, 0, 0, 1}
		b = binary.BigEndian.AppendUint32(b, uint32(len(inner)))
		b = append(b, inner...)
	}
	return b
}

// Nesting is capped at maxCollectionDepth: a payload exactly at the cap
// decodes, one level beyond errors. The cap bounds DecodeValue's recursion
// (each level costs 9 payload bytes, so legal values are shallow) while
// still admitting every nesting a real broker produces.
func TestDecodeCollectionDepthCap(t *testing.T) {
	if _, err := DecodeValue(TypeSet, nestedChain(maxCollectionDepth)); err != nil {
		t.Fatalf("depth %d: %v", maxCollectionDepth, err)
	}
	if _, err := DecodeValue(TypeSet, nestedChain(maxCollectionDepth+1)); err == nil {
		t.Fatalf("depth %d: want error beyond depth cap", maxCollectionDepth+1)
	}
}

// A deeply nested chain of collection headers must come back as an error,
// not a stack overflow. Before the depth cap this payload (~9 MB of
// headers, far under maxPayloadSize) was a process-killing crash; now it
// dies at maxCollectionDepth.
func TestDecodeCollectionDeepNestingNoCrash(t *testing.T) {
	const depth = 1 << 20
	b := make([]byte, 0, depth*9)
	for i := 0; i < depth; i++ {
		// [TypeSet][count=1][size covering the rest of the chain]
		b = append(b, TypeSet, 0, 0, 0, 1, 0, 0, 0, 0)
	}
	for i := depth - 1; i >= 0; i-- {
		rest := len(b) - (i+1)*9
		binary.BigEndian.PutUint32(b[i*9+5:], uint32(rest))
	}
	if _, err := DecodeValue(TypeSet, b); err == nil {
		t.Fatal("want error for deeply nested collection")
	}
}

func TestDecodeJSONIsRawBytes(t *testing.T) {
	got, err := DecodeValue(TypeJSON, []byte(`{"a":1}`+"\x00"))
	if err != nil {
		t.Fatal(err)
	}
	b, ok := got.([]byte)
	if !ok || string(b) != `{"a":1}` {
		t.Fatalf("got %#v, want []byte JSON", got)
	}
}

func TestEncodeParamRoundTrip(t *testing.T) {
	w := NewWriter()
	if err := EncodeParam(w, int64(9)); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0, 0, 0, 1, TypeBigint,
		0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 9,
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got % X want % X", w.Bytes(), want)
	}
}

func TestEncodeParamNilAndTime(t *testing.T) {
	w := NewWriter()
	if err := EncodeParam(w, nil); err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2009, 6, 15, 13, 45, 30, 250e6, time.UTC)
	if err := EncodeParam(w, ts); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0, 0, 0, 1, TypeNull, 0, 0, 0, 0,
		0, 0, 0, 1, TypeDatetime,
		0, 0, 0, 14, 0x07, 0xD9, 0, 6, 0, 15, 0, 13, 0, 45, 0, 30, 0x00, 0xFA,
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got % X\nwant % X", w.Bytes(), want)
	}
}

func TestEncodeParamUnsupported(t *testing.T) {
	if err := EncodeParam(NewWriter(), struct{}{}); err == nil {
		t.Fatal("want error for unsupported type")
	}
}

// Collection binds (JDBC UBindParameter.writeParameter +
// UOutputBuffer.writeCollection + ByteArrayBuffer.merge): the type-byte
// argument is always U_TYPE_SEQUENCE, UStatement.bindCollection binds
// SEQUENCE no matter the target column's collection kind, and the value
// argument is [int32 total size][element type byte][per element:
// int32 size + bytes], with NULL elements as a zero size. Unlike the
// RESPONSE collection layout there is no element count int.
func TestEncodeParamCollectionInts(t *testing.T) {
	w := NewWriter()
	if err := EncodeParam(w, []any{int32(1), nil, int32(3)}); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0, 0, 0, 1, TypeSequence,
		0, 0, 0, 21, // 1 type byte + (4+4) + 4 + (4+4)
		TypeInt,
		0, 0, 0, 4, 0, 0, 0, 1,
		0, 0, 0, 0, // NULL element
		0, 0, 0, 4, 0, 0, 0, 3,
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got % X\nwant % X", w.Bytes(), want)
	}
}

func TestEncodeParamCollectionStrings(t *testing.T) {
	w := NewWriter()
	if err := EncodeParam(w, []any{"a", "bc"}); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0, 0, 0, 1, TypeSequence,
		0, 0, 0, 14, // 1 type byte + (4+2) + (4+3)
		TypeVarchar,
		0, 0, 0, 2, 'a', 0,
		0, 0, 0, 3, 'b', 'c', 0,
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got % X\nwant % X", w.Bytes(), want)
	}
}

// An empty collection still names a base type: JDBC's CUBRIDArray uses
// U_TYPE_OBJECT for a zero-length array, and writeCollection emits just
// [size 1][base type byte] with no elements.
func TestEncodeParamCollectionEmpty(t *testing.T) {
	w := NewWriter()
	if err := EncodeParam(w, []any{}); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0, 0, 0, 1, TypeSequence,
		0, 0, 0, 1, TypeObject,
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got % X\nwant % X", w.Bytes(), want)
	}
}

// Go ints and int64s share the BIGINT wire type, so mixing them in one
// collection is legal.
func TestEncodeParamCollectionIntWidths(t *testing.T) {
	w := NewWriter()
	if err := EncodeParam(w, []any{7, int64(8)}); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0, 0, 0, 1, TypeSequence,
		0, 0, 0, 25, // 1 type byte + 2*(4+8)
		TypeBigint,
		0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 7,
		0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 8,
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got % X\nwant % X", w.Bytes(), want)
	}
}

func TestEncodeParamCollectionErrors(t *testing.T) {
	cases := []struct {
		name string
		v    []any
	}{
		{"mixed element types", []any{int32(1), "a"}},
		{"all-nil elements", []any{nil, nil}},
		{"nested collection", []any{[]any{int32(1)}}},
		{"unsupported element", []any{struct{}{}}},
	}
	for _, tc := range cases {
		if err := EncodeParam(NewWriter(), tc.v); err == nil {
			t.Errorf("%s: want error", tc.name)
		}
	}
}

// Self-described values: when a column's metadata declares U_TYPE_NULL,
// each non-null value carries its own type prefix before the payload
// (JDBC UStatement.readTypeFromData). Observed live on 9.3: the
// SCH_ATTRIBUTE DEFAULT column is declared type NULL and its values
// arrive as [type byte]['x' 0x00].
func TestDecodeSelfDescribedValue(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want any
	}{
		{"legacy varchar", append([]byte{TypeVarchar}, []byte("x\x00")...), "x"},
		{"legacy int", []byte{TypeInt, 0, 0, 0, 7}, int32(7)},
		// Extended two-byte prefix: 0x80|charset then the type byte.
		{"extended varchar", append([]byte{0x84, TypeVarchar}, []byte("hi\x00")...), "hi"},
		{"typed null", []byte{TypeNull}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeSelfDescribedValue(tc.data)
			if err != nil {
				t.Fatal(err)
			}
			switch w := tc.want.(type) {
			case nil:
				if got != nil {
					t.Fatalf("got %#v, want nil", got)
				}
			default:
				if got != w {
					t.Fatalf("got %#v, want %#v", got, w)
				}
			}
		})
	}
}

func TestDecodeSelfDescribedValueExtendedCollection(t *testing.T) {
	// Extended prefix with collection bits 01 (SET) and element type INT:
	// the payload is a collection block ([element type][count][sized items]).
	data := []byte{0xA0, TypeInt, TypeInt, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 5}
	got, err := DecodeSelfDescribedValue(data)
	if err != nil {
		t.Fatal(err)
	}
	vals, ok := got.([]any)
	if !ok || len(vals) != 1 || vals[0] != int32(5) {
		t.Fatalf("got %#v", got)
	}
}

func TestDecodeSelfDescribedValueEmpty(t *testing.T) {
	if _, err := DecodeSelfDescribedValue(nil); err == nil {
		t.Fatal("want error for empty self-described value")
	}
	if _, err := DecodeSelfDescribedValue([]byte{0x84}); err == nil {
		t.Fatal("want error for truncated extended prefix")
	}
}
