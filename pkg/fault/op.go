package fault

// Op is the pipeline operation at which a fault was raised. Closed set; a metric
// label. Classifying per operation is the one error idea worth transplanting
// wholesale from the surveyed field.
type Op uint8

const (
	OpUnknown Op = iota
	OpOpen
	OpRead
	OpDecompress
	OpDeframe
	OpDecode
	OpTransform
	OpEncode
	OpFrame
	OpCompress
	OpWrite
	OpFlush
	OpPrepare

	// OpCommitSink is a Committer.Commit call.
	OpCommitSink
	// OpCommitSource is a Source.Commit call.
	OpCommitSource
	// OpPersist is canal's own durable write — phase two of the commit protocol.
	OpPersist

	OpBuffer
	OpDiscover
	OpValidate
	OpSchemaApply
	OpClose
)

var opNames = [...]string{
	OpUnknown:      "unknown",
	OpOpen:         "open",
	OpRead:         "read",
	OpDecompress:   "decompress",
	OpDeframe:      "deframe",
	OpDecode:       "decode",
	OpTransform:    "transform",
	OpEncode:       "encode",
	OpFrame:        "frame",
	OpCompress:     "compress",
	OpWrite:        "write",
	OpFlush:        "flush",
	OpPrepare:      "prepare",
	OpCommitSink:   "commit_sink",
	OpCommitSource: "commit_source",
	OpPersist:      "persist",
	OpBuffer:       "buffer",
	OpDiscover:     "discover",
	OpValidate:     "validate",
	OpSchemaApply:  "schema_apply",
	OpClose:        "close",
}

// String returns the stable snake_case token for o.
func (o Op) String() string {
	if int(o) < len(opNames) {
		return opNames[o]
	}
	return "unknown"
}
