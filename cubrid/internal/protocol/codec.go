package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

func trimNul(b []byte) []byte {
	for len(b) > 0 && b[len(b)-1] == 0 {
		b = b[:len(b)-1]
	}
	return b
}

// DecodeValue decodes one non-NULL column value of wire type typ.
// Date/time values carry no zone on the wire and are returned in UTC.
func DecodeValue(typ byte, data []byte) (any, error) {
	return decodeValue(typ, data, 0)
}

// decodeValue is DecodeValue with the current collection nesting depth;
// depth only advances through decodeCollection elements.
func decodeValue(typ byte, data []byte, depth int) (any, error) {
	switch typ {
	case TypeChar, TypeVarchar, TypeNchar, TypeVarNchar, TypeEnum, TypeNumeric:
		return string(trimNul(data)), nil
	case TypeJSON:
		// JSON is returned as raw bytes (json.RawMessage-compatible),
		// not string, per the spec's type table.
		return append([]byte(nil), trimNul(data)...), nil
	case TypeInt, TypeUInt:
		if len(data) < 4 {
			return nil, ErrShortBuffer
		}
		return int32(binary.BigEndian.Uint32(data)), nil
	case TypeShort, TypeUShort:
		if len(data) < 2 {
			return nil, ErrShortBuffer
		}
		return int16(binary.BigEndian.Uint16(data)), nil
	case TypeBigint, TypeUBigint:
		if len(data) < 8 {
			return nil, ErrShortBuffer
		}
		return int64(binary.BigEndian.Uint64(data)), nil
	case TypeFloat:
		if len(data) < 4 {
			return nil, ErrShortBuffer
		}
		return math.Float32frombits(binary.BigEndian.Uint32(data)), nil
	case TypeDouble, TypeMonetary:
		if len(data) < 8 {
			return nil, ErrShortBuffer
		}
		return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
	case TypeBit, TypeVarbit:
		return append([]byte(nil), data...), nil
	case TypeDate, TypeTime, TypeTimestamp, TypeDatetime:
		return decodeDateTime(typ, data)
	case TypeTimestampTZ, TypeTimestampLTZ, TypeDatetimeTZ, TypeDatetimeLTZ:
		return decodeTZ(typ, data)
	case TypeSet, TypeMultiset, TypeSequence:
		return decodeCollection(data, depth)
	case TypeResultset:
		// A server-side result-set handle (stored-procedure OUT cursor,
		// resolved via MAKE_OUT_RS in JDBC). Out of scope for v1; name it
		// clearly instead of falling into the generic decode error.
		return nil, fmt.Errorf("cubrid/protocol: RESULTSET (server-side result set) values are not supported")
	default:
		return nil, fmt.Errorf("cubrid/protocol: decoding type %d is not supported", typ)
	}
}

// DecodeSelfDescribedValue decodes a value whose column metadata declared
// U_TYPE_NULL: the value carries its own type prefix before the payload,
// one raw type byte on legacy servers, or the extended two-byte form
// (0x80|collection|charset, then the type byte) on newer ones (JDBC
// UStatement.readTypeFromData). Observed live on 9.3: the SCH_ATTRIBUTE
// DEFAULT column is declared type NULL and prefixes its values this way.
func DecodeSelfDescribedValue(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, ErrShortBuffer
	}
	typ, rest := data[0], data[1:]
	if typ&0x80 != 0 { // extended two-byte prefix
		if len(data) < 2 {
			return nil, ErrShortBuffer
		}
		switch (typ & 0x60) >> 5 {
		case 1:
			typ = TypeSet
		case 2:
			typ = TypeMultiset
		case 3:
			typ = TypeSequence
		default:
			typ = data[1]
		}
		rest = data[2:]
	}
	if typ == TypeNull {
		return nil, nil
	}
	return DecodeValue(typ, rest)
}

// decodeDateTime parses the per-type big-endian int16 layouts confirmed live
// on 11.4 (JDBC UInputBuffer readDate/readTime/readTimestamp/readDatetime):
// DATE = [Y,M,D], TIME = [h,m,s], TIMESTAMP = [Y,M,D,h,m,s],
// DATETIME = [Y,M,D,h,m,s,ms].
func decodeDateTime(typ byte, b []byte) (time.Time, error) {
	var want int
	switch typ {
	case TypeDate, TypeTime:
		want = 6
	case TypeTimestamp:
		want = 12
	default: // TypeDatetime
		want = 14
	}
	if len(b) < want {
		return time.Time{}, ErrShortBuffer
	}
	f := func(i int) int { return int(int16(binary.BigEndian.Uint16(b[2*i:]))) }
	switch typ {
	case TypeDate:
		return time.Date(f(0), time.Month(f(1)), f(2), 0, 0, 0, 0, time.UTC), nil
	case TypeTime:
		return time.Date(1, 1, 1, f(0), f(1), f(2), 0, time.UTC), nil
	case TypeTimestamp:
		return time.Date(f(0), time.Month(f(1)), f(2), f(3), f(4), f(5), 0, time.UTC), nil
	default: // TypeDatetime
		return time.Date(f(0), time.Month(f(1)), f(2), f(3), f(4), f(5), f(6)*1e6, time.UTC), nil
	}
}

// decodeTZ parses a TZ-typed value: base-type time shorts (TIMESTAMP=6,
// DATETIME=7) followed by the NUL-terminated timezone string in the
// remaining bytes. Confirmed live on 11.4: the shorts are the wall-clock
// fields IN the attached zone (TIMESTAMPTZ'... 13:30:00 Asia/Seoul'
// arrives as 13:30 + "Asia/Seoul KST"), matching JDBC, whose
// CUBRIDTimestamptz.getUnixTime reparses the wall fields with the zone
// to recover the instant.
func decodeTZ(typ byte, b []byte) (time.Time, error) {
	var n int
	var base byte
	switch typ {
	case TypeTimestampTZ, TypeTimestampLTZ:
		n, base = 12, TypeTimestamp
	default: // TypeDatetimeTZ, TypeDatetimeLTZ
		n, base = 14, TypeDatetime
	}
	if len(b) < n {
		return time.Time{}, ErrShortBuffer
	}
	t, err := decodeDateTime(base, b[:n])
	if err != nil {
		return time.Time{}, err
	}
	zone := string(trimNul(b[n:]))
	// Reinterpret the wall-clock fields in the value's own zone.
	return time.Date(t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(),
		lookupZone(zone)), nil
}

// lookupZone resolves a CUBRID timezone string: an IANA name optionally
// followed by its abbreviation ("Asia/Seoul KST", live 11.4 format) or a
// fixed offset ("+09:00"). Unresolvable zones degrade to UTC, the wall
// fields are then taken as UTC, which keeps them readable but may shift
// the instant; CUBRID and Go share the IANA database, so in practice
// names resolve wherever the host has tzdata.
func lookupZone(zone string) *time.Location {
	if i := strings.IndexByte(zone, ' '); i >= 0 {
		zone = zone[:i]
	}
	if zone == "" {
		return time.UTC
	}
	if loc, err := time.LoadLocation(zone); err == nil {
		return loc
	}
	if t, err := time.Parse("-07:00", zone); err == nil {
		return t.Location()
	}
	return time.UTC
}

// maxCollectionDepth caps collection nesting. CUBRID forbids nested
// collections in column DDL but nested collection VALUES are legal
// expression results (live 11.4 serializes SELECT {{1,2},{3,4}} with a
// SEQUENCE element-type byte wrapping inner INT collections; JDBC decodes
// them via recursive readData). Each nesting level costs at least 9
// payload bytes, so legal values are intrinsically shallow; the cap turns
// a hostile millions-deep header chain (well under the frame size cap,
// otherwise an unrecoverable stack overflow) into an immediate error.
const maxCollectionDepth = 32

// decodeCollection parses a SET/MULTISET/SEQUENCE value: element type
// byte, element count int32, then per element [size int32][element bytes],
// recursing into decodeValue for each. A size <= 0 is a NULL element,
// JDBC UStatement.readData (U_TYPE_SET branch) treats both 0 and -1 as
// null. All three collection kinds decode to []any; the wire carries the
// element type itself, so column metadata is not consulted. depth is the
// number of enclosing collections, capped at maxCollectionDepth.
func decodeCollection(b []byte, depth int) ([]any, error) {
	if depth >= maxCollectionDepth {
		return nil, fmt.Errorf("cubrid/protocol: collection nested deeper than %d levels", maxCollectionDepth)
	}
	r := NewReader(b)
	et, err := r.Byte()
	if err != nil {
		return nil, err
	}
	n, err := r.Int32()
	if err != nil {
		return nil, err
	}
	if n < 0 || int64(n)*4 > int64(r.Remaining()) {
		// Each element costs at least its 4-byte size prefix; an
		// unauthenticated huge count must not pre-allocate gigabytes.
		return nil, fmt.Errorf("cubrid/protocol: invalid collection element count %d (%d bytes remaining)",
			n, r.Remaining())
	}
	out := make([]any, 0, n)
	for i := 0; i < int(n); i++ {
		sz, err := r.Int32()
		if err != nil {
			return nil, err
		}
		if sz <= 0 {
			out = append(out, nil)
			continue
		}
		eb, err := r.Bytes(int(sz))
		if err != nil {
			return nil, err
		}
		v, err := decodeValue(et, eb, depth+1)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func encodeDateTime(t time.Time) []byte {
	b := make([]byte, 14)
	put := func(i, v int) { binary.BigEndian.PutUint16(b[2*i:], uint16(v)) }
	put(0, t.Year())
	put(1, int(t.Month()))
	put(2, t.Day())
	put(3, t.Hour())
	put(4, t.Minute())
	put(5, t.Second())
	put(6, t.Nanosecond()/1e6)
	return b
}

// paramWireType returns the type byte EncodeParam announces for one scalar
// bind value (the Go mapping of JDBC UUType.getObjectDBtype).
func paramWireType(v any) (byte, error) {
	switch v.(type) {
	case nil:
		return TypeNull, nil
	case bool, int16:
		return TypeShort, nil
	case int32:
		return TypeInt, nil
	case int, int64:
		return TypeBigint, nil
	case float32:
		return TypeFloat, nil
	case float64:
		return TypeDouble, nil
	case string:
		return TypeVarchar, nil
	case []byte:
		return TypeVarbit, nil
	case time.Time:
		return TypeDatetime, nil
	}
	return 0, fmt.Errorf("cubrid: unsupported bind parameter type %T", v)
}

// encodeScalarValue writes just the value argument for a scalar already
// vetted by paramWireType.
func encodeScalarValue(w *Writer, v any) {
	switch v := v.(type) {
	case nil:
		w.ArgNull()
	case bool:
		if v {
			w.ArgShort(1)
		} else {
			w.ArgShort(0)
		}
	case int:
		w.ArgLong(int64(v))
	case int16:
		w.ArgShort(v)
	case int32:
		w.ArgInt(v)
	case int64:
		w.ArgLong(v)
	case float32:
		w.ArgFloat(v)
	case float64:
		w.ArgDouble(v)
	case string:
		w.ArgString(v)
	case []byte:
		w.ArgBytes(v)
	case time.Time:
		w.ArgBytes(encodeDateTime(v))
	}
}

// EncodeParam appends one bind parameter: a type-byte argument followed by
// the value argument, per JDBC UBindParameter.writeParameter, confirmed
// live on 11.4 (TestIntegrationBindRoundTrip114 and the scalar matrix
// round-trip every branch, including nil). []any values encode as
// collections (TestIntegrationCollectionBind).
func EncodeParam(w *Writer, v any) error {
	if vs, ok := v.([]any); ok {
		return encodeCollectionParam(w, vs)
	}
	typ, err := paramWireType(v)
	if err != nil {
		return err
	}
	w.ArgByte(typ)
	encodeScalarValue(w, v)
	return nil
}

// encodeCollectionParam appends one collection bind parameter. Wire layout
// per JDBC (UStatement.bindCollection, UOutputBuffer.writeCollection,
// ByteArrayBuffer.merge), confirmed live across the 9.3 to 11.4 matrix
// (TestIntegrationCollectionBind): the type-byte argument is always
// U_TYPE_SEQUENCE regardless of the target column's collection kind (the
// server coerces), and the value argument is [int32 total size][element
// type byte][per element: int32 size + value bytes], NULL elements as a
// zero size. Unlike the response collection layout there is NO element
// count int, the server infers it from the sizes. The element type comes
// from the first non-nil element; mixed element types are an error (JDBC
// arrays are homogeneous by construction); an empty collection carries
// JDBC's U_TYPE_OBJECT base type; an all-nil collection has no derivable
// element type and errors, matching CUBRIDArray's ER_INVALID_ARGUMENT.
func encodeCollectionParam(w *Writer, vs []any) error {
	elemType := byte(TypeObject)
	seen := false
	for _, v := range vs {
		if v == nil {
			continue
		}
		if _, nested := v.([]any); nested {
			return fmt.Errorf("cubrid: nested collection bind parameters are not supported")
		}
		t, err := paramWireType(v)
		if err != nil {
			return fmt.Errorf("cubrid: collection element: %w", err)
		}
		if !seen {
			elemType, seen = t, true
		} else if t != elemType {
			return fmt.Errorf("cubrid: mixed element types in collection bind (wire types %d and %d)", elemType, t)
		}
	}
	if len(vs) > 0 && !seen {
		return fmt.Errorf("cubrid: cannot derive the element type of an all-NULL collection bind")
	}
	inner := NewWriter()
	inner.RawByte(elemType)
	for _, v := range vs {
		if v == nil {
			inner.ArgNull()
			continue
		}
		encodeScalarValue(inner, v)
	}
	w.ArgByte(TypeSequence)
	w.ArgBytes(inner.Bytes())
	return nil
}
