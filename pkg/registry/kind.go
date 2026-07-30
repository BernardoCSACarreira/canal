package registry

// Kind is the component kind. It is a STRING, not an iota, and it is open.
//
// Deliberately: a closed iota used as a metric label and as a persisted config discriminator
// means adding a component kind is a contract change plus a coordinated frontend edit, which is
// design rule R1 violated one level up. The registry validates a kind against its registered
// set, so an unknown kind is still a diagnostic — but it is a data problem, not a compile
// problem.
type Kind string

const (
	KindSource     Kind = "source"
	KindSink       Kind = "sink"
	KindTransform  Kind = "transform"
	KindBuffer     Kind = "buffer"
	KindEncoder    Kind = "encoder"
	KindDecoder    Kind = "decoder"
	KindFramer     Kind = "framer"
	KindDeframer   Kind = "deframer"
	KindCompressor Kind = "compressor"
)

// Kinds is every kind this build knows about, in a stable order for rendering.
var Kinds = []Kind{
	KindSource, KindSink, KindTransform, KindBuffer,
	KindEncoder, KindDecoder, KindFramer, KindDeframer, KindCompressor,
}

// Produces reports whether a node of this kind may have no inputs — that is, whether it is a
// graph source. Graph validation asks the registry rather than switching on a name.
func (k Kind) Produces() bool { return k == KindSource }

// Consumes reports whether a node of this kind may be terminal.
func (k Kind) Consumes() bool { return k == KindSink }

// Support is the maturity badge shown next to a component. It is scaffolding LABELLED as such
// (design rule R10) rather than presented as finished.
type Support uint8

const (
	SupportCommunity Support = iota
	SupportBeta
	SupportCertified
	SupportDeprecated
)

var supportNames = [...]string{
	SupportCommunity:  "community",
	SupportBeta:       "beta",
	SupportCertified:  "certified",
	SupportDeprecated: "deprecated",
}

// String returns the stable snake_case token for s.
func (s Support) String() string {
	if int(s) < len(supportNames) {
		return supportNames[s]
	}
	return "community"
}

// MarshalText makes Support serialise as its token rather than as a number, so the wire form and
// the metric label value are the same string (design rule R9).
func (s Support) MarshalText() ([]byte, error) { return []byte(s.String()), nil }
