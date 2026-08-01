package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// codecChain is one byte sink's resolved encode → frame → compress path.
//
// It is resolved in Build, as a pure function of config, for the same reason the guarantee is: a
// spec naming an encoder this build does not have should be refused on the submit screen, not
// discovered by the first record.
//
// ADR 0022 makes a codec a stage-standard FIELD of the node that needs it rather than a graph node
// of its own. That is what makes per-sink wire formats expressible — a fan-out to two sinks in two
// formats is two sink nodes with two codec blocks — and it is why nothing in the graph loop
// resolves one.
type codecChain struct {
	encoder    connector.Encoder
	framer     connector.Framer
	compressor connector.Compressor

	contentType     string
	contentEncoding string
}

// batches reports whether several records may share one request body.
//
// A FRAMER IS WHAT MAKES BATCHING LEGAL, and this one-line method is the whole rule. Without one,
// concatenating encoded payloads produces a body with no boundaries in it: three lines arrive as
// "line-00000line-00001line-00002" and no reader can recover them. That is not a hypothetical —
// it is what the first version of the engine did, and what one-record-per-request exists to avoid.
//
// So a framerless byte sink pays one Write per record. That is bad for throughput and honest about
// why, and the fix is to configure a framer rather than for the core to invent a delimiter for
// somebody else's data format.
func (c *codecChain) batches() bool { return c != nil && c.framer != nil }

// defaultEncoder is the encoder a byte sink gets when the operator names none. It matches the
// declared Default of the stage-standard codec field, so the form and the engine agree.
const defaultEncoder = "json"

// resolveCodec builds the chain for one sink node.
//
// A structured sink gets nil: it is handed records, and attaching an encoder to it would be a
// double encoding. Build already refuses a codec block configured on one.
//
// Every constructed component is returned as a closer, so a build that fails later closes exactly
// what it made. An encoder is a component like any other: it has Open and Close, its config is
// validated against its own spec, and it is constructed by the registry rather than by name here.
func resolveCodec(ctx context.Context, r *registry.Registry, node record.NodeID,
	cfg *config.Config, rk *registry.ResolvedSink,
) (*codecChain, []closer, []telemetry.DefaultNote, config.Diagnostics) {
	var (
		diags    config.Diagnostics
		built    []closer
		defaults []telemetry.DefaultNote
	)
	if rk.Caps.Structured {
		return nil, nil, nil, nil
	}

	// A missing codec block is not an error: every field in it is optional. Codec returns an error
	// only when the block is absent, and the zero CodecRef is exactly the right reading of that.
	ref, _ := cfg.Codec(config.FieldCodec)

	encName := ref.Encoder
	if encName == "" {
		encName = defaultEncoder
		// R10: a value the core supplied is labelled as such. This one is worth labelling loudly,
		// because json on a byte payload base64-encodes it — correct, declared in the encoder's
		// caps, and not what somebody tailing a log file expects. Seeing it in the negotiated
		// output before the run is the difference between a surprise and a decision.
		defaults = append(defaults, telemetry.DefaultNote{
			Path:  []string{config.FieldCodec, "encoder"},
			Value: encName,
			From:  "core default",
		})
	}

	ee, ok := r.Encoder(encName)
	if !ok {
		return nil, nil, nil, append(diags, nodeDiag(node, config.CodeUnknownComponent,
			fmt.Sprintf("no encoder named %q is registered in this build", encName),
			"available encoders: "+strings.Join(r.Names(registry.KindEncoder), ", ")))
	}

	// An encoder feeding a byte sink must produce bytes. Checking the declared capability rather
	// than trusting the name is the same discipline every other negotiation here follows.
	if !producesBytes(ee.Caps) {
		return nil, nil, nil, append(diags, nodeDiag(node, config.CodeCapability,
			fmt.Sprintf("encoder %q does not produce bytes, so it cannot feed a byte sink", encName), ""))
	}

	encCfg, nd := ee.Spec.Validate(ref.EncoderConfig)
	diags = anchor(diags, node, nd)
	if nd.HasErrors() {
		return nil, nil, nil, diags
	}
	enc, err := ee.New(ctx, encCfg)
	if err != nil {
		return nil, nil, nil, append(diags, nodeDiag(node, config.CodeCustom,
			fmt.Sprintf("encoder %q could not be constructed: %v", encName, err), ""))
	}
	built = append(built, closer{id: node, close: enc.Close})

	chain := &codecChain{encoder: enc, contentType: enc.ContentType()}

	if ref.Framer != "" {
		fe, ok := r.Framer(ref.Framer)
		if !ok {
			return nil, built, defaults, append(diags, nodeDiag(node, config.CodeUnknownComponent,
				fmt.Sprintf("no framer named %q is registered in this build", ref.Framer),
				"available framers: "+strings.Join(r.Names(registry.KindFramer), ", ")))
		}
		fCfg, nd := fe.Spec.Validate(ref.FramerConfig)
		diags = anchor(diags, node, nd)
		if nd.HasErrors() {
			return nil, built, defaults, diags
		}
		// A framer has no Open or Close: it is a pure function of bytes, so there is nothing to
		// release and nothing to add to built.
		if chain.framer, err = fe.New(ctx, fCfg); err != nil {
			return nil, built, defaults, append(diags, nodeDiag(node, config.CodeCustom,
				fmt.Sprintf("framer %q could not be constructed: %v", ref.Framer, err), ""))
		}
	}

	if ref.Compressor != "" {
		ce, ok := r.Compressor(ref.Compressor)
		if !ok {
			return nil, built, defaults, append(diags, nodeDiag(node, config.CodeUnknownComponent,
				fmt.Sprintf("no compressor named %q is registered in this build", ref.Compressor),
				"available compressors: "+strings.Join(r.Names(registry.KindCompressor), ", ")))
		}
		cCfg, nd := ce.Spec.Validate(ref.CompressorConfig)
		diags = anchor(diags, node, nd)
		if nd.HasErrors() {
			return nil, built, defaults, diags
		}
		if chain.compressor, err = ce.New(ctx, cCfg); err != nil {
			return nil, built, defaults, append(diags, nodeDiag(node, config.CodeCustom,
				fmt.Sprintf("compressor %q could not be constructed: %v", ref.Compressor, err), ""))
		}
		chain.contentEncoding = chain.compressor.ContentEncoding()
	}

	return chain, built, defaults, diags
}

func producesBytes(c connector.CodecCaps) bool {
	for _, k := range c.Produces {
		if k == record.KindBytes {
			return true
		}
	}
	return false
}

// open starts the encoder. Framers and compressors are pure and have no lifecycle.
func (c *codecChain) open(ctx context.Context, rt connector.CodecRuntime) error {
	if c == nil {
		return nil
	}
	return c.encoder.Open(ctx, rt)
}

// encode turns one record into its wire payload, framed if a framer is configured.
//
// dst is appended to and returned, so one buffer is reused per request rather than allocated per
// record. The encoder is contractually forbidden from retaining it.
func (c *codecChain) encode(ctx context.Context, dst []byte, rec *record.Record) ([]byte, error) {
	start := len(dst)
	out, err := c.encoder.Encode(ctx, dst, rec)
	if err != nil {
		return nil, err
	}
	if c.framer == nil {
		return out, nil
	}
	// Frame appends the payload to dst, so the encoded bytes have to be lifted out of the buffer
	// they were just written into before being framed back onto it. Reslicing rather than copying
	// would alias the very region Frame writes to.
	payload := append([]byte(nil), out[start:]...)
	return c.framer.Frame(out[:start], payload)
}

// describe renders the chain for the negotiated contract, so an operator can see the wire format a
// sink will actually produce without reading the config back.
func (c *codecChain) describe() string {
	if c == nil {
		return "structured (no codec)"
	}
	parts := []string{c.contentType}
	if c.framer == nil {
		parts = append(parts, "no framer, so one record per request")
	} else {
		parts = append(parts, "framed")
	}
	if c.contentEncoding != "" {
		parts = append(parts, c.contentEncoding)
	}
	return strings.Join(parts, ", ")
}
