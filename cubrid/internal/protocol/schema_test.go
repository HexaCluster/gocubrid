package protocol

import (
	"bytes"
	"testing"
)

func strp(s string) *string { return &s }

// Request layout per JDBC UConnection.getSchemaInfo: fn 9, [type int]
// [arg1 string-or-null][arg2 string-or-null][flag byte] and [shard id int]
// only when the broker speaks V5+.
func TestSchemaInfoRequestV12(t *testing.T) {
	w := NewWriter()
	SchemaInfoRequest(w, SchClass, strp("go_t%"), nil, SchemaFlagArg1Pattern, Version(ProtoV12))

	want := []byte{
		FnGetSchemaInfo,
		0, 0, 0, 4, 0, 0, 0, 1, // schema type CLASS
		0, 0, 0, 6, 'g', 'o', '_', 't', '%', 0, // arg1 with NUL
		0, 0, 0, 0, // arg2 null
		0, 0, 0, 1, 0x01, // flag: arg1 is a pattern
		0, 0, 0, 4, 0, 0, 0, 0, // shard id (V5+)
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

func TestSchemaInfoRequestV4HasNoShardID(t *testing.T) {
	w := NewWriter()
	SchemaInfoRequest(w, SchAttribute, strp("t"), strp("c%"), SchemaFlagArg2Pattern, Version(ProtoV4))

	want := []byte{
		FnGetSchemaInfo,
		0, 0, 0, 4, 0, 0, 0, 4, // schema type ATTRIBUTE
		0, 0, 0, 2, 't', 0, // arg1
		0, 0, 0, 3, 'c', '%', 0, // arg2
		0, 0, 0, 1, 0x02, // flag: arg2 is a pattern
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}

func TestSchemaInfoRequestBothArgsNull(t *testing.T) {
	w := NewWriter()
	SchemaInfoRequest(w, SchPrimaryKey, nil, nil, 0, Version(ProtoV12))

	want := []byte{
		FnGetSchemaInfo,
		0, 0, 0, 4, 0, 0, 0, 16, // schema type PRIMARY_KEY
		0, 0, 0, 0, // arg1 null
		0, 0, 0, 0, // arg2 null
		0, 0, 0, 1, 0x00, // flag
		0, 0, 0, 4, 0, 0, 0, 0, // shard id
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("got  % X\nwant % X", w.Bytes(), want)
	}
}
