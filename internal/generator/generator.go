package generator

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// Generator creates random test data from JSON Schema. It walks the schema and
// owns structure; every value and random decision comes from the Synthesizer
// (ADR-0008).
type Generator struct {
	synth      *synth.Synthesizer
	resolveRef RefResolver
}

// RefResolver resolves a $ref string to its schema node. The schema module
// provides the implementation; it is injected so the generator can follow
// preserved cyclic $ref nodes within its depth budget.
type RefResolver func(ref string) (map[string]any, error)

// maxRecursionDepth bounds how many nested $ref expansions the generator walks
// for cyclic schemas (ADR-0005). Enough for realistic trees, small enough to
// stay fast.
const maxRecursionDepth = 8

// RootPath is the JSON Path prefix for the schema root, consistent with the
// JSON Path standard (RFC 9535): "$" denotes the root value. Error paths are
// reported relative to it, e.g. "$.orderId" or "$.items[0].sku".
const RootPath = "$"

// errAbsent marks a node the generator deliberately omits rather than errors on:
// a $ref whose depth budget is exhausted (ADR-0005 shape truncation).
// object() and array() swallow it to skip the field or item; it never
// indicates a schema the generator cannot honour.
var errAbsent = errors.New("generator: absent node (skipped)")

// UnsupportedSchemaError reports a Message-schema construct the generator
// cannot convert into a value. It names the offending keyword and the JSON
// path to the construct so callers can locate the failure in their spec.
type UnsupportedSchemaError struct {
	// Keyword is the offending JSON Schema keyword, e.g. "type", "$ref",
	// "properties", "oneOf", "allOf".
	Keyword string
	// Path is the JSON path to the construct, rooted at the schema root ("$").
	Path string
	// Detail is an optional human-readable explanation.
	Detail string
}

func (e *UnsupportedSchemaError) Error() string {
	msg := fmt.Sprintf("generator: unsupported %s at %s", e.Keyword, e.Path)
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

// UnsupportedPatternError reports a regex construct outside the documented
// subset the pattern synthesizer supports (ADR-0006 Decision 2). It names the
// offending construct and the field path so no silently-nonconforming string is
// ever produced.
type UnsupportedPatternError struct {
	// Pattern is the full pattern that could not be synthesized.
	Pattern string
	// Construct is the offending substring outside the documented subset.
	Construct string
	// Path is the JSON path to the string field, rooted at the schema root ("$").
	Path string
}

func (e *UnsupportedPatternError) Error() string {
	return fmt.Sprintf("generator: unsupported pattern construct %q in %q at %s",
		e.Construct, e.Pattern, e.Path)
}

// New creates a Generator drawing from the given Synthesizer, which carries the
// run's seed and clock (ADR-0008).
func New(s *synth.Synthesizer) *Generator {
	return &Generator{synth: s}
}

// SetRefResolver wires the schema module's $ref resolution into the generator.
func (g *Generator) SetRefResolver(r RefResolver) {
	g.resolveRef = r
}

// Value generates a random value matching the given JSON Schema, or a typed
// error when it contains a construct the generator cannot honour (ADR-0006).
// The error names the offending keyword and its JSON path.
func (g *Generator) Value(schema map[string]any) (any, error) {
	return g.value(schema, "", RootPath, 0)
}

// value generates for schema at path. field is the nearest enclosing property
// name, inherited by array items and oneOf/anyOf/allOf/$ref branches for
// field-name heuristics (#31 decision 5).
func (g *Generator) value(schema map[string]any, field, path string, depth int) (any, error) {
	schema = normalizeSchema(schema)

	if ref, ok := schema["$ref"].(string); ok {
		return g.refValue(ref, field, path, depth)
	}

	if ex, ok := schema["example"]; ok {
		return ex, nil
	}
	if ex, ok := schema["examples"]; ok {
		if arr, ok := ex.([]any); ok && len(arr) > 0 {
			return arr[0], nil
		}
	}
	if c, ok := schema["const"]; ok {
		return c, nil
	}
	if enums, ok := schema["enum"].([]any); ok && len(enums) > 0 {
		return enums[g.synth.Pick(len(enums))], nil
	}

	if allOf, ok := schema["allOf"].([]any); ok {
		return g.mergeAllOf(allOf, field, path, depth)
	}
	if oneOf, ok := schema["oneOf"].([]any); ok && len(oneOf) > 0 {
		sub, ok := oneOf[g.synth.Pick(len(oneOf))].(map[string]any)
		if !ok {
			return nil, &UnsupportedSchemaError{Keyword: "oneOf", Path: path, Detail: "branch is not a schema object"}
		}
		return g.value(sub, field, path, depth)
	}
	if anyOf, ok := schema["anyOf"].([]any); ok && len(anyOf) > 0 {
		sub, ok := anyOf[g.synth.Pick(len(anyOf))].(map[string]any)
		if !ok {
			return nil, &UnsupportedSchemaError{Keyword: "anyOf", Path: path, Detail: "branch is not a schema object"}
		}
		return g.value(sub, field, path, depth)
	}

	typ, ok := schema["type"].(string)
	if !ok {
		return nil, &UnsupportedSchemaError{Keyword: "type", Path: path, Detail: "type is missing or not a string"}
	}
	switch typ {
	case "object":
		return g.object(schema, path, depth)
	case "array":
		return g.array(schema, field, path, depth)
	case "string":
		return g.string(schema, field, path)
	case "integer":
		return g.integer(schema)
	case "number":
		return g.number(schema)
	case "boolean":
		return g.synth.Chance(50), nil
	default:
		return nil, &UnsupportedSchemaError{Keyword: "type", Path: path, Detail: fmt.Sprintf("unsupported type %q", typ)}
	}
}

// refValue follows a preserved $ref node. When the depth budget is exhausted
// the node is treated as absent so the caller can skip the field or empty the
// array, mirroring how optional-field sampling truncates shape (ADR-0005). A
// missing resolver or a resolver error surfaces as a typed error, since the
// schema cannot be honoured at all in either case (ADR-0006 Decision 1).
func (g *Generator) refValue(ref, field, path string, depth int) (any, error) {
	if g.resolveRef == nil {
		return nil, &UnsupportedSchemaError{Keyword: "$ref", Path: path, Detail: "no ref resolver wired"}
	}
	if depth >= maxRecursionDepth {
		return nil, errAbsent
	}
	target, err := g.resolveRef(ref)
	if err != nil {
		return nil, &UnsupportedSchemaError{Keyword: "$ref", Path: path, Detail: fmt.Sprintf("resolving %s: %v", ref, err)}
	}
	return g.value(target, field, path, depth+1)
}

func (g *Generator) object(schema map[string]any, path string, depth int) (any, error) {
	result := make(map[string]any)
	props, _ := schema["properties"].(map[string]any)
	required, _ := schema["required"].([]any)

	// Sort property names for deterministic generation
	names := make([]string, 0, len(props))
	for name := range props {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		ps, ok := props[name].(map[string]any)
		if !ok {
			return nil, &UnsupportedSchemaError{Keyword: "properties", Path: path + "." + name, Detail: "property schema is not an object"}
		}
		if g.shouldInclude(name, required) {
			v, err := g.value(ps, name, path+"."+name, depth)
			if err != nil {
				// An absent node is a deliberate shape truncation: an optional
				// field is skipped, and an exhausted subtree drops its required
				// field (an incomplete object beats an infinite one).
				if errors.Is(err, errAbsent) {
					continue
				}
				return nil, err
			}
			result[name] = v
		}
	}

	return result, nil
}

func (g *Generator) shouldInclude(fieldName string, required []any) bool {
	for _, r := range required {
		if r.(string) == fieldName {
			return true
		}
	}
	return g.synth.Chance(85)
}

func (g *Generator) array(schema map[string]any, field, path string, depth int) (any, error) {
	items, _ := schema["items"].(map[string]any)
	if items == nil {
		return []any{}, nil
	}

	minItems := 1
	maxItems := 5
	if v, ok := schema["minItems"].(float64); ok {
		minItems = int(v)
	}
	if v, ok := schema["maxItems"].(float64); ok {
		maxItems = int(v)
	}

	count := int(g.synth.Int(int64(minItems), int64(maxItems)))
	result := make([]any, 0, count)
	for i := 0; i < count; i++ {
		item, err := g.value(items, field, fmt.Sprintf("%s[%d]", path, i), depth)
		if err != nil {
			if errors.Is(err, errAbsent) {
				continue
			}
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

func (g *Generator) string(schema map[string]any, fieldName, path string) (string, error) {
	if format, ok := schema["format"].(string); ok {
		switch format {
		// Every string format JSON Schema 2020-12 defines is honoured. A format
		// outside that set is an annotation, not a constraint: it falls through
		// to pattern and then the field-name heuristics (ADR-0008 amendment).
		case "date-time":
			return g.synth.Instant().Format(time.RFC3339), nil
		case "date":
			return g.synth.Instant().Format("2006-01-02"), nil
		case "time":
			return g.synth.Instant().Format("15:04:05Z07:00"), nil
		case "duration":
			return g.synth.Semantic(synth.Duration), nil
		case "email", "idn-email":
			return g.synth.Semantic(synth.Email), nil
		case "hostname", "idn-hostname":
			return g.synth.Semantic(synth.Hostname), nil
		case "ipv4":
			return g.synth.Semantic(synth.IPv4), nil
		case "ipv6":
			return g.synth.Semantic(synth.IPv6), nil
		case "uuid":
			return g.synth.Semantic(synth.UUID), nil
		case "uri", "url", "iri":
			return g.synth.Semantic(synth.URL), nil
		case "uri-reference", "iri-reference":
			return g.synth.Semantic(synth.URIReference), nil
		case "uri-template":
			return g.synth.Semantic(synth.URITemplate), nil
		case "json-pointer":
			return g.synth.Semantic(synth.JSONPointer), nil
		case "relative-json-pointer":
			return g.synth.Semantic(synth.RelativeJSONPointer), nil
		case "regex":
			return g.synth.Semantic(synth.Regex), nil
		}
	}

	if pattern, ok := schema["pattern"].(string); ok {
		v, err := g.synth.Pattern(pattern)
		var pe *synth.PatternError
		if errors.As(err, &pe) {
			return "", &UnsupportedPatternError{Pattern: pe.Pattern, Construct: pe.Construct, Path: path}
		}
		return v, err
	}

	return g.synth.Text(fieldName), nil
}

func (g *Generator) integer(schema map[string]any) (int64, error) {
	min := int64(0)
	max := int64(1000)

	if v, ok := toFloat64(schema["minimum"]); ok {
		min = int64(v)
	}
	if v, ok := toFloat64(schema["exclusiveMinimum"]); ok {
		min = int64(v) + 1
	}
	if v, ok := toFloat64(schema["maximum"]); ok {
		max = int64(v)
	}
	if v, ok := toFloat64(schema["exclusiveMaximum"]); ok {
		max = int64(v) - 1
	}

	return g.synth.Int(min, max), nil
}

func (g *Generator) number(schema map[string]any) (float64, error) {
	min := 0.0
	max := 1000.0

	if v, ok := toFloat64(schema["minimum"]); ok {
		min = v
	}
	if v, ok := toFloat64(schema["exclusiveMinimum"]); ok {
		min = v + 0.01
	}
	if v, ok := toFloat64(schema["maximum"]); ok {
		max = v
	}
	if v, ok := toFloat64(schema["exclusiveMaximum"]); ok {
		max = v - 0.01
	}

	return g.synth.Float(min, max), nil
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

func normalizeSchema(schema map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range schema {
		result[k] = v
	}
	return result
}
