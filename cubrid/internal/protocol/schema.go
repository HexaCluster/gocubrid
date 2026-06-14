package protocol

// GET_SCHEMA_INFO schema type selectors (JDBC USchType).
const (
	SchClass            int32 = 1
	SchVClass           int32 = 2
	SchQuerySpec        int32 = 3
	SchAttribute        int32 = 4
	SchClassAttribute   int32 = 5
	SchMethod           int32 = 6
	SchClassMethod      int32 = 7
	SchMethodFile       int32 = 8
	SchSuperclass       int32 = 9
	SchSubclass         int32 = 10
	SchConstraint       int32 = 11
	SchTrigger          int32 = 12
	SchClassPrivilege   int32 = 13
	SchAttrPrivilege    int32 = 14
	SchDirectSuperClass int32 = 15
	SchPrimaryKey       int32 = 16
	SchImportedKeys     int32 = 17
	SchExportedKeys     int32 = 18
	SchCrossReference   int32 = 19
)

// GET_SCHEMA_INFO flag bits: the corresponding argument is a SQL LIKE
// pattern rather than an exact name.
const (
	SchemaFlagArg1Pattern byte = 0x01
	SchemaFlagArg2Pattern byte = 0x02
)

// SchemaInfoRequest writes a GET_SCHEMA_INFO request: schema type, two
// optional filter arguments (nil writes a null argument), the pattern
// flag byte, and (only when the broker speaks V5+) a shard id of 0.
// Layout per JDBC UConnection.getSchemaInfo.
func SchemaInfoRequest(w *Writer, schType int32, arg1, arg2 *string, flag byte, ver Version) {
	w.RawByte(FnGetSchemaInfo)
	w.ArgInt(schType)
	for _, arg := range []*string{arg1, arg2} {
		if arg == nil {
			w.ArgNull()
		} else {
			w.ArgString(*arg)
		}
	}
	w.ArgByte(flag)
	if ver.AtLeast(ProtoV5) {
		w.ArgInt(0) // shard id (driver is shard-unaware)
	}
}
