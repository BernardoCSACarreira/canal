// Package config is one declaration with five consumers: the validator, the
// defaulter, the docs source, the JSON Schema for editors, and the UI form model.
//
// The frontend goal is satisfied ONLY if capabilities and config are declarative
// data. If any capability is a Go callback the UI must instantiate a connector to
// learn about it; if any config metadata lives in code the UI needs per-connector
// knowledge. Break that and the frontend becomes N frontends. So nothing on [Spec]
// is a callback: [Field.Choices] names a hook, and the core forwards the name.
//
// Dependency direction: config imports fault, schema and record, and NEVER
// connector. Its composite extractors therefore return types owned by config
// ([BatchPolicy], [CodecRef], [BufferRef]) or by fault (fault.RetryPolicy). The
// engine turns a [CodecRef] into a live codec chain; config does not know that
// codecs are objects. That one rule is what keeps config out of an import cycle.
package config
