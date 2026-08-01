package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
)

// pkg/config is 2,400 lines that every connector in existence depends on and it had no test of its
// own. Spec.Validate is the function that decides whether an operator's YAML is accepted, what the
// defaults are, and which diagnostics they see; Redacted is the function that decides whether a
// password reaches a log line.

func spec() *config.Spec {
	return config.NewSpec().
		Field(config.Field{Name: "host", Type: config.TypeString,
			Description: "Hostname to connect to."}).
		Field(config.Field{Name: "port", Type: config.TypeInt, Optional: true, Default: 5432,
			Description: "Port."}).
		Field(config.Field{Name: "password", Type: config.TypeString, Secret: true, Optional: true,
			Description: "Password."}).
		Field(config.Field{Name: "timeout", Type: config.TypeDuration, Optional: true, Default: "30s",
			Description: "Dial timeout."}).
		Field(config.Field{Name: "tls", Type: config.TypeBool, Optional: true, Default: false,
			Description: "Use TLS."})
}

func has(d config.Diagnostics, sub string) bool {
	for i := range d {
		if strings.Contains(d[i].Message, sub) || strings.Contains(strings.Join(d[i].Path, "."), sub) {
			return true
		}
	}
	return false
}

// A REQUIRED FIELD LEFT OUT IS AN ERROR, and it is the single most common thing an operator does
// wrong.
func TestARequiredFieldIsRequired(t *testing.T) {
	_, d := spec().Validate(map[string]any{})
	if !d.HasErrors() {
		t.Fatal("a config missing its only required field was accepted")
	}
	if !has(d, "host") {
		t.Errorf("the diagnostics do not name the missing field: %v", d)
	}
}

// EVERY PROBLEM AT ONCE. A form that surfaces one error at a time is a form operators fight, and
// fail-fast validation is how that happens.
func TestValidationReportsEveryProblemNotJustTheFirst(t *testing.T) {
	_, d := spec().Validate(map[string]any{
		"port":    "not a number",
		"tls":     "not a bool",
		"unknown": 1,
	})
	if !d.HasErrors() {
		t.Fatal("three bad fields were accepted")
	}
	for _, want := range []string{"host", "port", "tls", "unknown"} {
		if !has(d, want) {
			t.Errorf("the diagnostics do not mention %q, so the operator fixes one field per run:\n%v", want, d)
		}
	}
}

// A typo silently ignored is a setting the operator believes is in force.
func TestAnUnknownFieldIsRefused(t *testing.T) {
	_, d := spec().Validate(map[string]any{"host": "db", "hsot": "db"})
	if !d.HasErrors() {
		t.Fatal("a misspelled field was accepted")
	}
	if !has(d, "hsot") {
		t.Errorf("the diagnostics do not quote the unknown field: %v", d)
	}
}

func TestDefaultsAreAppliedForAbsentOptionalFields(t *testing.T) {
	c, d := spec().Validate(map[string]any{"host": "db"})
	if d.HasErrors() {
		t.Fatalf("a valid config was refused: %v", d)
	}
	if got, err := config.Get[int](c, "port"); err != nil || got != 5432 {
		t.Errorf("port is (%v, %v), want the default 5432", got, err)
	}
	if got, err := config.Get[time.Duration](c, "timeout"); err != nil || got != 30*time.Second {
		t.Errorf("timeout is (%v, %v), want the default 30s", got, err)
	}
	if got, err := config.Get[bool](c, "tls"); err != nil || got != false {
		t.Errorf("tls is (%v, %v), want the default false", got, err)
	}
	// An explicit value beats the default.
	c2, _ := spec().Validate(map[string]any{"host": "db", "port": 6543})
	if got, _ := config.Get[int](c2, "port"); got != 6543 {
		t.Errorf("port is %v, want the operator's 6543", got)
	}
}

// A duration is written the way a human writes one, and read as a time.Duration.
func TestDurationsAreParsedNotPassedThrough(t *testing.T) {
	c, d := spec().Validate(map[string]any{"host": "db", "timeout": "1m30s"})
	if d.HasErrors() {
		t.Fatalf("a valid duration was refused: %v", d)
	}
	got, err := config.Get[time.Duration](c, "timeout")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != 90*time.Second {
		t.Errorf("got %v, want 90s", got)
	}

	_, bad := spec().Validate(map[string]any{"host": "db", "timeout": "not a duration"})
	if !bad.HasErrors() {
		t.Error("an unparseable duration was accepted")
	}
}

// Get is typed, and asking for the wrong type must fail rather than return a zero value that looks
// like a legitimate answer.
func TestGetRefusesTheWrongType(t *testing.T) {
	c, _ := spec().Validate(map[string]any{"host": "db"})

	if _, err := config.Get[int](c, "host"); err == nil {
		t.Error("Get[int] on a string field succeeded")
	}
	if _, err := config.Get[string](c, "nonexistent"); err == nil {
		t.Error("Get on an undeclared field succeeded")
	}
	// The error is also recorded on the config, so a connector that ignores every individual error
	// still fails at Err().
	if c.Err() == nil {
		t.Error("Config.Err() is nil after two failed Gets; a connector ignoring errors would proceed")
	}
}

// A SECRET MUST NOT REACH A LOG LINE. Redacted is the only thing standing between a password in a
// config and a password in the read model, every log line and every API response.
func TestRedactedHidesEverySecret(t *testing.T) {
	c, d := spec().Validate(map[string]any{"host": "db", "password": "hunter2"})
	if d.HasErrors() {
		t.Fatalf("Validate: %v", d)
	}

	red := c.Redacted()
	flat := strings.ToLower(strings.TrimSpace(sprint(red)))
	if strings.Contains(flat, "hunter2") {
		t.Fatalf("the password survived redaction:\n%v", red)
	}
	if red["host"] != "db" {
		t.Errorf("redaction removed a non-secret field: %v", red)
	}
	if red["password"] == nil {
		t.Error("the secret field vanished entirely; an operator cannot see it is set")
	}

	// R13: the result is a value, so mutating it cannot reach the live config.
	red["host"] = "tampered"
	if got, _ := config.Get[string](c, "host"); got != "db" {
		t.Errorf("mutating the redacted tree reached the config: host is now %q", got)
	}
}

// Secret is the typed accessor for a secret, and using Get on a declared secret should not be the
// path of least resistance — the point is that the redaction and the accessor agree on what is
// sensitive.
func TestSecretRefusesANonSecretField(t *testing.T) {
	c, _ := spec().Validate(map[string]any{"host": "db", "password": "hunter2"})

	if got, err := c.Secret("password"); err != nil || got != "hunter2" {
		t.Errorf("Secret on a declared secret is (%q, %v), want the value", got, err)
	}
	if _, err := c.Secret("host"); err == nil {
		t.Error("Secret succeeded on a field that is not declared secret")
	}
}

// An enum accepts exactly its declared choices, and refusing an undeclared one is what keeps a
// closed vocabulary closed.
func TestEnumsAcceptOnlyTheirChoices(t *testing.T) {
	s := config.NewSpec().Field(config.Field{
		Name: "mode", Type: config.TypeEnum, Description: "Mode.",
		Enum: []config.EnumValue{{Value: "append"}, {Value: "upsert"}},
	})

	if _, d := s.Validate(map[string]any{"mode": "append"}); d.HasErrors() {
		t.Errorf("a declared choice was refused: %v", d)
	}
	_, d := s.Validate(map[string]any{"mode": "delete"})
	if !d.HasErrors() {
		t.Fatal("an undeclared enum value was accepted")
	}
	if !has(d, "mode") {
		t.Errorf("the diagnostic does not name the field: %v", d)
	}
}

// Has answers whether a value is present, which a connector uses to tell "unset" from "set to the
// zero value" — a distinction Get alone cannot make.
func TestHasDistinguishesUnsetFromZero(t *testing.T) {
	set, _ := spec().Validate(map[string]any{"host": "db", "tls": false})
	unset, _ := spec().Validate(map[string]any{"host": "db"})

	if !set.Has("tls") {
		t.Error("an explicitly false value reports as absent")
	}
	if unset.Has("tls") {
		t.Error("an unset field with a default reports as present")
	}
}

// A nil spec and a nil config must not panic. Both are reachable: a component registered with no
// fields validates against nothing, and Get on a nil config happens in error paths.
func TestNilsAreSafe(t *testing.T) {
	var s *config.Spec
	c, d := s.Validate(map[string]any{"anything": 1})
	if d.HasErrors() {
		t.Errorf("a nil spec produced diagnostics: %v", d)
	}
	_ = c.Redacted()

	var nilCfg *config.Config
	if _, err := config.Get[string](nilCfg, "x"); err == nil {
		t.Error("Get on a nil config succeeded")
	}
	if nilCfg.Has("x") {
		t.Error("Has on a nil config answered true")
	}
	if got := nilCfg.Redacted(); got == nil {
		t.Error("Redacted on a nil config returned nil rather than an empty map")
	}
}

// An empty spec accepts an empty config and refuses anything else, which is what every connector
// with no fields of its own relies on.
func TestAnEmptySpecAcceptsNothingButEmptiness(t *testing.T) {
	s := config.NewSpec()
	if _, d := s.Validate(nil); d.HasErrors() {
		t.Errorf("an empty spec refused an empty config: %v", d)
	}
	if _, d := s.Validate(map[string]any{"x": 1}); !d.HasErrors() {
		t.Error("an empty spec accepted an undeclared field")
	}
}

// sprint renders a value for substring searching without importing fmt into the assertions.
func sprint(v any) string {
	var b strings.Builder
	write(&b, v)
	return b.String()
}

func write(b *strings.Builder, v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			b.WriteString(k)
			b.WriteByte('=')
			write(b, val)
			b.WriteByte(' ')
		}
	case []any:
		for _, val := range x {
			write(b, val)
			b.WriteByte(' ')
		}
	case string:
		b.WriteString(x)
	default:
		b.WriteString("?")
	}
}
