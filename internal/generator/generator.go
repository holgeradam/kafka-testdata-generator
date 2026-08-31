package generator

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Generator creates random test data from JSON Schema.
type Generator struct {
	rng        *rand.Rand
	baseTime   time.Time
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

// RootField is the fieldName passed to Value at the top level of a Message
// schema, replacing the historical empty-string sentinel. It also roots the
// JSON path reported on UnsupportedSchemaError.
const RootField = "root"

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
	// Path is the JSON path to the construct, rooted at RootField.
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
	// Path is the JSON path to the string field, rooted at RootField.
	Path string
}

func (e *UnsupportedPatternError) Error() string {
	return fmt.Sprintf("generator: unsupported pattern construct %q in %q at %s",
		e.Construct, e.Pattern, e.Path)
}

// New creates a new Generator with the given seed and explicit clock. The now
// value feeds date/date-time format synthesis, so determinism requires both a
// fixed seed and a fixed now (ADR-0006 Decision 4); there is no hidden clock.
func New(seed int64, now time.Time) *Generator {
	return &Generator{
		rng:      rand.New(rand.NewSource(seed)),
		baseTime: now,
	}
}

// SetRefResolver wires the schema module's $ref resolution into the generator.
func (g *Generator) SetRefResolver(r RefResolver) {
	g.resolveRef = r
}

// Value generates a random value matching the given JSON Schema, or a typed
// error when it contains a construct the generator cannot honour (ADR-0006).
// The error names the offending keyword and its JSON path.
func (g *Generator) Value(schema map[string]any, fieldName string) (any, error) {
	return g.value(schema, fieldName, RootField, 0)
}

func (g *Generator) value(schema map[string]any, fieldName, path string, depth int) (any, error) {
	schema = normalizeSchema(schema)

	if ref, ok := schema["$ref"].(string); ok {
		return g.refValue(ref, fieldName, path, depth)
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
		return enums[g.rng.Intn(len(enums))], nil
	}

	if allOf, ok := schema["allOf"].([]any); ok {
		return g.mergeAllOf(allOf, fieldName, path, depth)
	}
	if oneOf, ok := schema["oneOf"].([]any); ok && len(oneOf) > 0 {
		sub, ok := oneOf[g.rng.Intn(len(oneOf))].(map[string]any)
		if !ok {
			return nil, &UnsupportedSchemaError{Keyword: "oneOf", Path: path, Detail: "branch is not a schema object"}
		}
		return g.value(sub, fieldName, path, depth)
	}
	if anyOf, ok := schema["anyOf"].([]any); ok && len(anyOf) > 0 {
		sub, ok := anyOf[g.rng.Intn(len(anyOf))].(map[string]any)
		if !ok {
			return nil, &UnsupportedSchemaError{Keyword: "anyOf", Path: path, Detail: "branch is not a schema object"}
		}
		return g.value(sub, fieldName, path, depth)
	}

	typ, ok := schema["type"].(string)
	if !ok {
		return nil, &UnsupportedSchemaError{Keyword: "type", Path: path, Detail: "type is missing or not a string"}
	}
	switch typ {
	case "object":
		return g.object(schema, fieldName, path, depth)
	case "array":
		return g.array(schema, fieldName, path, depth)
	case "string":
		return g.string(schema, fieldName, path)
	case "integer":
		return g.integer(schema)
	case "number":
		return g.number(schema)
	case "boolean":
		return g.rng.Intn(2) == 1, nil
	default:
		return nil, &UnsupportedSchemaError{Keyword: "type", Path: path, Detail: fmt.Sprintf("unsupported type %q", typ)}
	}
}

// refValue follows a preserved $ref node. When the depth budget is exhausted
// the node is treated as absent so the caller can skip the field or empty the
// array, mirroring how optional-field sampling truncates shape (ADR-0005). A
// missing resolver or a resolver error surfaces as a typed error, since the
// schema cannot be honoured at all in either case (ADR-0006 Decision 1).
func (g *Generator) refValue(ref, fieldName, path string, depth int) (any, error) {
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
	return g.value(target, fieldName, path, depth+1)
}

func (g *Generator) object(schema map[string]any, parentFieldName, path string, depth int) (any, error) {
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
	return g.rng.Intn(100) < 85
}

func (g *Generator) array(schema map[string]any, parentFieldName, path string, depth int) (any, error) {
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

	count := minItems + g.rng.Intn(maxItems-minItems+1)
	result := make([]any, 0, count)
	for i := 0; i < count; i++ {
		item, err := g.value(items, parentFieldName, fmt.Sprintf("%s[%d]", path, i), depth)
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
		case "date-time":
			return g.baseTime.Add(-time.Duration(g.rng.Intn(365*24)) * time.Hour).Format(time.RFC3339), nil
		case "date":
			return g.baseTime.Add(-time.Duration(g.rng.Intn(365)) * 24 * time.Hour).Format("2006-01-02"), nil
		case "email":
			return g.generateEmail(fieldName), nil
		case "uuid":
			return g.generateUUID(), nil
		case "uri", "url":
			return g.generateURL(), nil
		}
	}

	if pattern, ok := schema["pattern"].(string); ok {
		return g.synthesizePattern(pattern, path)
	}

	return g.generateByFieldName(fieldName), nil
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

	if min >= max {
		return min, nil
	}
	return min + g.rng.Int63n(max-min+1), nil
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

	if min >= max {
		return min, nil
	}
	return min + g.rng.Float64()*(max-min), nil
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

// mergeAllOf folds every allOf branch into one schema using ADR-0006 Decision 5
// rules - numeric bounds intersect, required unions, properties merge
// recursively - then generates a value from the merged schema. Irreconcilable
// branches (distinct const values, clashing type) surface a typed
// UnsupportedSchemaError rather than a silently-overridden constraint.
func (g *Generator) mergeAllOf(allOf []any, fieldName, path string, depth int) (any, error) {
	merged, err := mergeSchemas(allOf, path)
	if err != nil {
		return nil, err
	}
	return g.value(merged, fieldName, path, depth)
}

// mergeSchemas folds the allOf branches into a single schema map. Each branch
// must be a schema object; anything else is an unsupported allOf construct.
func mergeSchemas(branches []any, path string) (map[string]any, error) {
	merged := make(map[string]any)
	for _, sub := range branches {
		m, ok := sub.(map[string]any)
		if !ok {
			return nil, &UnsupportedSchemaError{Keyword: "allOf", Path: path, Detail: "branch is not a schema object"}
		}
		var err error
		merged, err = mergeTwoSchemas(merged, m, path)
		if err != nil {
			return nil, err
		}
	}
	return merged, nil
}

// mergeTwoSchemas intersects two schemas keyword by keyword (ADR-0006
// Decision 5). a is the accumulated merge so far, b the next branch.
func mergeTwoSchemas(a, b map[string]any, path string) (map[string]any, error) {
	out := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		existing, ok := out[k]
		switch k {
		case "minimum", "exclusiveMinimum":
			if ok {
				ev, eok := toFloat64(existing)
				nv, nok := toFloat64(v)
				if eok && nok {
					out[k] = math.Max(ev, nv)
					continue
				}
			}
			out[k] = v
		case "maximum", "exclusiveMaximum":
			if ok {
				ev, eok := toFloat64(existing)
				nv, nok := toFloat64(v)
				if eok && nok {
					out[k] = math.Min(ev, nv)
					continue
				}
			}
			out[k] = v
		case "required":
			out[k] = unionRequired(existing, v, ok)
		case "properties":
			merged, err := mergeProperties(existing, v, ok, path)
			if err != nil {
				return nil, err
			}
			out[k] = merged
		case "type":
			if ok {
				if !reflect.DeepEqual(existing, v) {
					return nil, &UnsupportedSchemaError{Keyword: "allOf", Path: path,
						Detail: fmt.Sprintf("clashing type %v and %v", existing, v)}
				}
				continue
			}
			out[k] = v
		case "const":
			if ok {
				if !reflect.DeepEqual(existing, v) {
					return nil, &UnsupportedSchemaError{Keyword: "allOf", Path: path,
						Detail: fmt.Sprintf("distinct const %v and %v", existing, v)}
				}
				continue
			}
			out[k] = v
		default:
			out[k] = v
		}
	}
	return out, nil
}

// mergeProperties recursively merges two properties maps. A property named in
// both branches is itself merged with mergeTwoSchemas; a property in only one
// branch is taken as-is.
func mergeProperties(existing, v any, exists bool, path string) (any, error) {
	if !exists {
		return v, nil
	}
	em, eok := existing.(map[string]any)
	vm, vok := v.(map[string]any)
	if !eok || !vok {
		return nil, &UnsupportedSchemaError{Keyword: "allOf", Path: path, Detail: "properties in allOf branch is not an object"}
	}
	out := make(map[string]any, len(em)+len(vm))
	for name, ps := range em {
		out[name] = ps
	}
	for name, ps := range vm {
		if ep, ok := out[name]; ok {
			epm, eok := ep.(map[string]any)
			psm, pok := ps.(map[string]any)
			if !eok || !pok {
				return nil, &UnsupportedSchemaError{Keyword: "allOf", Path: path + "." + name, Detail: "property schema in allOf branch is not an object"}
			}
			m, err := mergeTwoSchemas(epm, psm, path+"."+name)
			if err != nil {
				return nil, err
			}
			out[name] = m
		} else {
			out[name] = ps
		}
	}
	return out, nil
}

// unionRequired returns the set union of two required lists, preserving order.
func unionRequired(existing, v any, exists bool) any {
	if !exists {
		return v
	}
	ea, eok := existing.([]any)
	va, vok := v.([]any)
	if !eok || !vok {
		// One side is not a required list; fall back to what we already have.
		return existing
	}
	seen := make(map[string]bool, len(ea)+len(va))
	out := make([]any, 0, len(ea)+len(va))
	for _, x := range ea {
		s, ok := x.(string)
		if !ok || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	for _, x := range va {
		s, ok := x.(string)
		if !ok || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func (g *Generator) generateByFieldName(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "email"):
		return g.generateEmail(name)
	case strings.Contains(lower, "id"):
		return g.generateUUID()
	case strings.Contains(lower, "name") && strings.Contains(lower, "first"):
		return g.pick(firstNames)
	case strings.Contains(lower, "name") && strings.Contains(lower, "last"):
		return g.pick(surnames)
	case strings.Contains(lower, "name"):
		return g.pick(firstNames) + " " + g.pick(surnames)
	case strings.Contains(lower, "phone"):
		return g.generatePhone()
	case strings.Contains(lower, "city"):
		return g.pick(cities)
	case strings.Contains(lower, "country"):
		return g.pick(countries)
	case strings.Contains(lower, "street"):
		return fmt.Sprintf("%d %s", g.rng.Intn(9999)+1, g.pick(streets))
	case strings.Contains(lower, "status"):
		return g.pick(statuses)
	case strings.Contains(lower, "description"):
		return g.pick(descriptions)
	case strings.Contains(lower, "currency"):
		return g.pick(currencies)
	case strings.Contains(lower, "url") || strings.Contains(lower, "uri"):
		return g.generateURL()
	case strings.Contains(lower, "sku"):
		return g.generateSKU()
	default:
		return g.randomString(8)
	}
}

func (g *Generator) generateEmail(name string) string {
	first := strings.ToLower(g.pick(firstNames))
	last := strings.ToLower(g.pick(surnames))
	domains := []string{"example.com", "test.org", "demo.net"}
	return fmt.Sprintf("%s.%s@%s", first, last, g.pick(domains))
}

func (g *Generator) generateUUID() string {
	b := make([]byte, 16)
	g.rng.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (g *Generator) generatePhone() string {
	return fmt.Sprintf("+1-%03d-%03d-%04d",
		g.rng.Intn(900)+100,
		g.rng.Intn(900)+100,
		g.rng.Intn(10000))
}

func (g *Generator) generateURL() string {
	paths := []string{"api", "users", "products", "orders", "docs"}
	return fmt.Sprintf("https://%s.example.com/%s/%d",
		g.pick([]string{"app", "api", "service"}),
		g.pick(paths),
		g.rng.Intn(10000))
}

func (g *Generator) generateSKU() string {
	return fmt.Sprintf("%s-%s-%04d",
		g.randomUpper(3),
		g.randomUpper(2),
		g.rng.Intn(10000))
}

// synthesizePattern builds a value that conforms to the documented regex
// subset (ADR-0006 Decision 2): literals, character classes [A-Z] [a-z] [0-9],
// escapes \d \w \s, quantifiers {n} {n,m} * + ?, groups, alternation, and
// anchors (ignored for synthesis). Anything outside the subset yields a
// UnsupportedPatternError naming the construct, never a silently-nonconforming
// string.
func (g *Generator) synthesizePattern(pattern, path string) (string, error) {
	p := &patternParser{pattern: pattern, path: path}
	node, err := p.parseAlternation()
	if err != nil {
		return "", err
	}
	if p.i < len(pattern) {
		return "", p.errConstruct(pattern[p.i:])
	}
	var sb strings.Builder
	if err := g.genNode(node, &sb); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// reNode is one parsed piece of the documented regex subset.
type reNode interface{ isReNode() }

type reEmpty struct{}        // anchor ^ $ , ignored for synthesis
type reLit struct{ ch rune } // literal character
type reClass struct {
	chars []rune // pool of characters, one drawn independently per occurrence
}
type reSeq struct {
	items []reNode
}
type reAlt struct {
	branches []reSeq
}
type reQuant struct {
	node reNode
	min  int
	max  int
}

func (reEmpty) isReNode() {}
func (reLit) isReNode()   {}
func (reClass) isReNode() {}
func (reSeq) isReNode()   {}
func (reAlt) isReNode()   {}
func (reQuant) isReNode() {}

// maxRepeat caps how many times an unbounded quantifier ( * or + ) repeats a
// node, keeping generated strings short while still conforming.
const maxRepeat = 6

var (
	digitsPool = []rune("0123456789")
	wordPool   = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
	spacePool  = []rune(" \t\n\r\f")
)

// patternParser is a recursive-descent parser for the documented regex subset.
type patternParser struct {
	pattern string
	path    string
	i       int
}

// errConstruct reports that a construct outside the documented subset was hit.
func (p *patternParser) errConstruct(construct string) error {
	return &UnsupportedPatternError{Pattern: p.pattern, Construct: construct, Path: p.path}
}

// alternation ::= concat ( '|' concat )*
func (p *patternParser) parseAlternation() (reNode, error) {
	var branches []reSeq
	first, err := p.parseConcat()
	if err != nil {
		return nil, err
	}
	branches = append(branches, reSeq{items: first})
	for p.i < len(p.pattern) && p.pattern[p.i] == '|' {
		p.i++
		seq, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		branches = append(branches, reSeq{items: seq})
	}
	if len(branches) == 1 {
		return reSeq{items: branches[0].items}, nil
	}
	return reAlt{branches: branches}, nil
}

// concat ::= piece*   (stops at '|' or ')')
func (p *patternParser) parseConcat() ([]reNode, error) {
	var items []reNode
	for p.i < len(p.pattern) && p.pattern[p.i] != '|' && p.pattern[p.i] != ')' {
		atom, err := p.parseAtom()
		if err != nil {
			return nil, err
		}
		if p.i < len(p.pattern) {
			switch p.pattern[p.i] {
			case '{':
				q, err := p.parseQuant()
				if err != nil {
					return nil, err
				}
				atom = reQuant{node: atom, min: q[0], max: q[1]}
			case '*':
				p.i++
				atom = reQuant{node: atom, min: 0, max: maxRepeat}
			case '+':
				p.i++
				atom = reQuant{node: atom, min: 1, max: maxRepeat}
			case '?':
				p.i++
				atom = reQuant{node: atom, min: 0, max: 1}
			}
		}
		items = append(items, atom)
	}
	return items, nil
}

// atom ::= anchor | literal | escape | class | group | (unsupported -> error)
func (p *patternParser) parseAtom() (reNode, error) {
	if p.i >= len(p.pattern) {
		return nil, p.errConstruct("")
	}
	ch := p.pattern[p.i]
	switch {
	case ch == '^' || ch == '$':
		p.i++
		return reEmpty{}, nil
	case ch == '.':
		return nil, p.errConstruct(".")
	case ch == '\\':
		return p.parseEscape()
	case ch == '[':
		return p.parseClass()
	case ch == '(':
		p.i++
		inner, err := p.parseAlternation()
		if err != nil {
			return nil, err
		}
		if p.i >= len(p.pattern) || p.pattern[p.i] != ')' {
			return nil, p.errConstruct("(")
		}
		p.i++
		return inner, nil
	default:
		p.i++
		return reLit{ch: rune(ch)}, nil
	}
}

// escape ::= '\' ( d | w | s | escaped literal ) | (unsupported -> error)
func (p *patternParser) parseEscape() (reNode, error) {
	p.i++ // consume '\'
	if p.i >= len(p.pattern) {
		return nil, p.errConstruct(`\`)
	}
	c := p.pattern[p.i]
	switch c {
	case 'd':
		p.i++
		return reClass{chars: digitsPool}, nil
	case 'w':
		p.i++
		return reClass{chars: wordPool}, nil
	case 's':
		p.i++
		return reClass{chars: spacePool}, nil
	default:
		if strings.ContainsRune(`.^$[]()|*+?{}\\-`, rune(c)) {
			p.i++
			return reLit{ch: rune(c)}, nil
		}
		return nil, p.errConstruct(`\` + string(c))
	}
}

// class ::= '[' range* ']'   (a leading '^' is outside the documented subset)
func (p *patternParser) parseClass() (reNode, error) {
	start := p.i
	p.i++ // consume '['
	if p.i < len(p.pattern) && p.pattern[p.i] == '^' {
		for p.i < len(p.pattern) && p.pattern[p.i] != ']' {
			p.i++
		}
		if p.i < len(p.pattern) {
			p.i++
		}
		return nil, p.errConstruct(p.pattern[start:p.i])
	}
	var pool []rune
	for p.i < len(p.pattern) && p.pattern[p.i] != ']' {
		lo := p.pattern[p.i]
		p.i++
		if p.i+1 < len(p.pattern) && p.pattern[p.i] == '-' && p.pattern[p.i+1] != ']' {
			hi := p.pattern[p.i+1]
			p.i += 2
			if hi < lo {
				return nil, p.errConstruct(p.pattern[start:p.i])
			}
			for c := lo; c <= hi; c++ {
				pool = append(pool, rune(c))
			}
		} else {
			pool = append(pool, rune(lo))
		}
	}
	if p.i >= len(p.pattern) || p.pattern[p.i] != ']' {
		return nil, p.errConstruct(p.pattern[start:p.i])
	}
	p.i++ // consume ']'
	if len(pool) == 0 {
		return nil, p.errConstruct(p.pattern[start:p.i])
	}
	return reClass{chars: pool}, nil
}

// quant ::= '{' digits '}' | '{' digits ',' digits '}' | (outside subset -> error)
func (p *patternParser) parseQuant() ([2]int, error) {
	start := p.i
	p.i++ // consume '{'
	i := p.i
	for p.i < len(p.pattern) && p.pattern[p.i] >= '0' && p.pattern[p.i] <= '9' {
		p.i++
	}
	nStr := p.pattern[i:p.i]
	if nStr == "" {
		p.skipToCloseBrace()
		return [2]int{}, p.errConstruct(p.pattern[start:p.i])
	}
	n, _ := strconv.Atoi(nStr)
	if p.i < len(p.pattern) && p.pattern[p.i] == ',' {
		p.i++
		i = p.i
		for p.i < len(p.pattern) && p.pattern[p.i] >= '0' && p.pattern[p.i] <= '9' {
			p.i++
		}
		mStr := p.pattern[i:p.i]
		if p.i < len(p.pattern) && p.pattern[p.i] == '}' && mStr != "" {
			m, _ := strconv.Atoi(mStr)
			if m < n {
				p.i++ // consume '}'
				return [2]int{}, p.errConstruct(p.pattern[start:p.i])
			}
			p.i++
			return [2]int{n, m}, nil
		}
		p.skipToCloseBrace()
		return [2]int{}, p.errConstruct(p.pattern[start:p.i])
	}
	if p.i < len(p.pattern) && p.pattern[p.i] == '}' {
		p.i++
		return [2]int{n, n}, nil
	}
	p.skipToCloseBrace()
	return [2]int{}, p.errConstruct(p.pattern[start:p.i])
}

// skipToCloseBrace advances the parser to just past the next '}' (or the end)
// so an error Construct captures the whole offending {...} token.
func (p *patternParser) skipToCloseBrace() {
	for p.i < len(p.pattern) && p.pattern[p.i] != '}' {
		p.i++
	}
	if p.i < len(p.pattern) {
		p.i++
	}
}

// genNode writes a synthesized occurrence of node into sb.
func (g *Generator) genNode(node reNode, sb *strings.Builder) error {
	switch n := node.(type) {
	case reEmpty:
	case reLit:
		sb.WriteRune(n.ch)
	case reClass:
		sb.WriteRune(n.chars[g.rng.Intn(len(n.chars))])
	case reSeq:
		for _, it := range n.items {
			if err := g.genNode(it, sb); err != nil {
				return err
			}
		}
	case reAlt:
		return g.genNode(n.branches[g.rng.Intn(len(n.branches))], sb)
	case reQuant:
		count := n.min
		if n.max > n.min {
			count += g.rng.Intn(n.max - n.min + 1)
		}
		for i := 0; i < count; i++ {
			if err := g.genNode(n.node, sb); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *Generator) randomString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[g.rng.Intn(len(chars))]
	}
	return string(b)
}

func (g *Generator) randomUpper(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = byte('A' + g.rng.Intn(26))
	}
	return string(b)
}

func (g *Generator) pick(items []string) string {
	return items[g.rng.Intn(len(items))]
}

func normalizeSchema(schema map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range schema {
		result[k] = v
	}
	return result
}

var firstNames = []string{
	"Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George", "Hannah",
	"Ivan", "Julia", "Kevin", "Laura", "Michael", "Nancy", "Oscar", "Patricia",
	"Quentin", "Rachel", "Steven", "Tina", "Uma", "Victor", "Wendy", "Xavier",
	"Yvonne", "Zachary",
}

var surnames = []string{
	"Anderson", "Brown", "Clark", "Davis", "Evans", "Fisher", "Garcia", "Harris",
	"Irwin", "Johnson", "King", "Lee", "Miller", "Nelson", "Ortiz", "Park",
	"Quinn", "Roberts", "Smith", "Taylor", "Upton", "Vargas", "Wilson", "Young",
}

var cities = []string{
	"New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
	"Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
	"Seattle", "Denver", "Boston", "Nashville", "Portland",
}

var countries = []string{
	"United States", "Canada", "United Kingdom", "Germany", "France",
	"Japan", "Australia", "Brazil", "India", "Mexico",
}

var streets = []string{
	"Main St", "Oak Ave", "Pine Rd", "Maple Dr", "Cedar Ln",
	"Elm St", "Walnut Ave", "Spruce Rd", "Birch Dr", "Willow Ln",
}

var statuses = []string{
	"pending", "active", "completed", "cancelled", "processing",
	"delivered", "shipped", "returned", "refunded", "on-hold",
}

var descriptions = []string{
	"Standard order", "Express delivery", "Bulk purchase", "Trial subscription",
	"Premium service", "Seasonal offer", "Corporate account", "New customer",
}

var currencies = []string{
	"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY",
}
