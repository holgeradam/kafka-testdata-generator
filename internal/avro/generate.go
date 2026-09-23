package avro

import (
	"fmt"
	"reflect"

	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// maxRecursionDepth bounds how many nested records/containers the generator
// walks. Avro schemas may be recursive (linked lists, trees); every Avro field
// is mandatory, so a cycle without a null escape cannot be bounded by shape
// truncation the way JSON's $ref walk can (ADR-0005). Deep enough for realistic
// avsc, small enough that a truly cyclic schema stops fast with a typed error
// instead of producing a value that violates the avsc.
const maxRecursionDepth = 32

// GenerateError reports an avsc model the generator cannot turn into a
// conforming value (a self-recursive schema without a null escape, or a type
// too deeply nested). ADR-0007 decision 4: anything the avsc cannot be
// honoured by stops with a typed error rather than emitting non-conforming
// data.
type GenerateError struct {
	// Detail is a human-readable explanation of the offending construct.
	Detail string
	// Err is the underlying error, when one exists.
	Err error
}

func (e *GenerateError) Error() string {
	msg := "avro: cannot generate a conforming value"
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

// Unwrap exposes the underlying error for errors.Is/As.
func (e *GenerateError) Unwrap() error {
	return e.Err
}

// Generator creates deterministic random Avro data values honouring the parsed
// Avro model (ADR-0007 decision 3: generation follows the avsc). It produces
// the value convention in convention.go - the codec's own native Go values -
// so an encoded payload is always registry-valid. It walks the model and owns
// structure; every value and random decision comes from the Synthesizer
// (ADR-0008).
type Generator struct {
	synth *synth.Synthesizer
}

// NewGenerator creates a Generator drawing from the given Synthesizer, which
// carries the run's seed and clock (ADR-0008).
func NewGenerator(s *synth.Synthesizer) *Generator {
	return &Generator{synth: s}
}

// Value generates a random value honouring the given Avro model type, or a
// typed *GenerateError when the avsc cannot be honoured.
func (g *Generator) Value(t Type) (any, error) {
	return g.value(t, "", 0)
}

// value generates for t. name is the nearest enclosing field name, inherited by
// union branches, array items and map values for field-name heuristics (#31
// decision 5).
func (g *Generator) value(t Type, name string, depth int) (any, error) {
	if depth > maxRecursionDepth {
		switch v := t.(type) {
		// A cycle through a null-optional branch is Avro's sanctioned escape:
		// emit null and stop descending.
		case *Union:
			if v.NullIndex() >= 0 {
				return map[string]any{"null": nil}, nil
			}
		// Empty containers are always conforming, mirroring how the JSON
		// generator truncates a subtree whose depth budget is exhausted.
		case *Array:
			return []any{}, nil
		case *Map:
			return map[string]any{}, nil
		}
		return nil, &GenerateError{Detail: fmt.Sprintf(
			"value depth exceeds %d; the avsc nests %s (or a cycle through it) without a null escape",
			maxRecursionDepth, describe(t))}
	}

	switch v := t.(type) {
	case *Primitive:
		return g.primitive(v, name)
	case *Record:
		return g.record(v, depth)
	case *Union:
		return g.union(v, name, depth)
	case *Array:
		return g.array(v, name, depth)
	case *Map:
		return g.mapValue(v, name, depth)
	case *Enum:
		return v.Symbols[g.synth.Pick(len(v.Symbols))], nil
	case *Fixed:
		return g.fixed(v)
	default:
		return nil, &GenerateError{Detail: fmt.Sprintf("unsupported model node %T", t)}
	}
}

// describe names a Type for error messages.
func describe(t Type) string {
	switch v := t.(type) {
	case *Primitive:
		return string(v.Kind)
	case *Record:
		return fmt.Sprintf("record %s", v.FullName())
	case *Array:
		return "array"
	case *Map:
		return "map"
	case *Union:
		return "union"
	case *Enum:
		return fmt.Sprintf("enum %s", v.FullName())
	case *Fixed:
		return fmt.Sprintf("fixed %s", v.FullName())
	default:
		return fmt.Sprintf("%T", t)
	}
}

func (g *Generator) record(rec *Record, depth int) (map[string]any, error) {
	result := make(map[string]any, len(rec.Fields))
	for _, f := range rec.Fields {
		v, err := g.value(f.Type, f.Name, depth+1)
		if err != nil {
			return nil, err
		}
		result[f.Name] = v
	}
	return result, nil
}

func (g *Generator) union(u *Union, name string, depth int) (any, error) {
	valueBranches := make([]Type, 0, len(u.Branches))
	for _, b := range u.Branches {
		if p, ok := b.(*Primitive); ok && p.Kind == KindNull {
			continue
		}
		valueBranches = append(valueBranches, b)
	}

	if u.NullIndex() >= 0 && g.synth.Chance(30) {
		return map[string]any{"null": nil}, nil
	}

	branch := valueBranches[g.synth.Pick(len(valueBranches))]
	v, err := g.value(branch, name, depth+1)
	if err != nil {
		return nil, err
	}
	// A generic union value is a single-entry map keyed by the branch name the
	// marshaller resolves: "null", "string", "long.timestamp-millis", the
	// named type's fullname, "array", "map", ...
	return map[string]any{unionBranchName(branch): v}, nil
}

func (g *Generator) array(arr *Array, name string, depth int) (any, error) {
	count := int(g.synth.Int(1, 5))
	result := make([]any, 0, count)
	for i := 0; i < count; i++ {
		item, err := g.value(arr.Items, name, depth+1)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

func (g *Generator) mapValue(m *Map, name string, depth int) (any, error) {
	count := int(g.synth.Int(1, 4))
	result := make(map[string]any, count)
	for i := 0; i < count; i++ {
		v, err := g.value(m.Values, name, depth+1)
		if err != nil {
			return nil, err
		}
		result[fmt.Sprintf("k%d", i)] = v
	}
	return result, nil
}

// fixed returns a [Size]byte array for a plain fixed, or the value the
// convention names for its logical overlay (a fixed decimal's *big.Rat).
func (g *Generator) fixed(f *Fixed) (any, error) {
	if f.Logical != nil {
		return g.logical(f.Logical)
	}
	arr := reflect.New(reflect.ArrayOf(f.Size, reflect.TypeOf(byte(0)))).Elem()
	for i, b := range g.synth.Bytes(f.Size) {
		arr.Index(i).SetUint(uint64(b))
	}
	return arr.Interface(), nil
}

func (g *Generator) primitive(p *Primitive, name string) (any, error) {
	if p.Logical != nil {
		return g.logical(p.Logical)
	}
	switch p.Kind {
	case KindNull:
		return nil, nil
	case KindBoolean:
		return g.synth.Chance(50), nil
	case KindInt:
		return int32(g.synth.Int(0, 999)), nil
	case KindLong:
		return g.synth.Int(0, 999), nil
	case KindFloat:
		return float32(g.synth.Float(0, 1000)), nil
	case KindDouble:
		return g.synth.Float(0, 1000), nil
	case KindBytes:
		return g.synth.Bytes(int(g.synth.Int(4, 8))), nil
	case KindString:
		return g.synth.Text(name), nil
	default:
		return nil, &GenerateError{Detail: fmt.Sprintf("unsupported primitive kind %q", p.Kind)}
	}
}

// logical synthesizes the value the convention table names for a logical
// type overlay.
func (g *Generator) logical(lt *LogicalType) (any, error) {
	l := lookupLogical(lt.Kind)
	if l == nil {
		return nil, &GenerateError{Detail: fmt.Sprintf("unsupported logical type %q", lt.Kind)}
	}
	return l.synth(g.synth, lt)
}
