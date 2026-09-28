package avro

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/holgeradam/kafka-testdata-generator/internal/planting"
)

// Walk is the avsc adapter of planting.Walk: where generation puts a value in
// every record of a value avsc, and whether the type there holds what is
// planted into it, so the planted Payload still encodes against its own
// schema (ADR-0009, #83). The JSON Schema adapter lives in internal/generator.
//
// Only record fields are guaranteed in Avro. A union branch varies per record
// (including the null/optional idiom), an array has no minimum length, and map
// keys are generated, so a step into any of them, or a union at the end, is
// refused before the run. A pointer token therefore always names a field.
type Walk struct {
	value *Schema
	key   *Schema
}

// NewWalk walks the value avsc, judging the Key against the key avsc, which
// is nil where no Key is planted.
func NewWalk(value, key *Schema) *Walk {
	return &Walk{value: value, key: key}
}

// Locate follows path through the value avsc.
func (w *Walk) Locate(path []planting.Step) ([]planting.Step, planting.Field, error) {
	steps := make([]planting.Step, len(path))
	current := w.value.Root
	for i, step := range path {
		steps[i] = step
		if step.Token {
			steps[i] = planting.Step{Field: step.Field, Index: -1}
		}
		next, err := field(current, steps[i])
		if err != nil {
			return nil, nil, &planting.StepError{Step: i, Err: err}
		}
		current = next
	}
	// A union at the end of the path is the nullable-field idiom: the value is
	// absent in some records, so it is refused with that reason rather than as
	// a type mismatch.
	if _, ok := current.(*Union); ok {
		return nil, nil, &planting.StepError{Step: len(path) - 1, Err: fmt.Errorf("the schema here is a union, so the branch differs per record and the value may be null")}
	}
	return steps, &located{at: current, walk: w}, nil
}

// located is the type at the end of a located path.
type located struct {
	at   Type
	walk *Walk
}

// HoldsKey reports whether the key avsc's values encode as the type here.
func (l *located) HoldsKey() error {
	return holdsKey(l.at, l.walk.key.Root)
}

// uuidText is the textual form a uuid logical type holds.
var uuidText = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Holds reports whether the type here can hold a string value as generated
// for it: a string, a uuid in its textual form, or an enum symbol. Anything
// else would not encode against the avsc.
func (l *located) Holds(value string) error {
	switch v := l.at.(type) {
	case *Primitive:
		if v.Kind != KindString {
			break
		}
		if logicalKind(v.Logical) == "uuid" && !uuidText.MatchString(value) {
			return fmt.Errorf("%s requires a uuid", describeLogical(l.at))
		}
		return nil
	case *Enum:
		for _, s := range v.Symbols {
			if s == value {
				return nil
			}
		}
		return fmt.Errorf("not a symbol of enum %s [%s]", v.FullName(), strings.Join(v.Symbols, ", "))
	}
	return fmt.Errorf("%s cannot hold a string", describeLogical(l.at))
}

// field takes one step into the model, refusing every shape whose value is not
// present in every record.
func field(t Type, step planting.Step) (Type, error) {
	switch v := t.(type) {
	case *Union:
		return nil, fmt.Errorf("the schema here is a union, so the branch differs per record")
	case *Map:
		return nil, fmt.Errorf("the schema here is a map, whose keys are generated, so no entry is guaranteed")
	case *Array:
		if step.Index >= 0 {
			return nil, fmt.Errorf("the schema here is an array, which has no minimum length, so no index is guaranteed")
		}
		return nil, fmt.Errorf("the schema here is an array, not a record")
	case *Record:
		if step.Index >= 0 {
			return nil, fmt.Errorf("the schema here is %s, not an array", describe(t))
		}
		for _, f := range v.Fields {
			if f.Name == step.Field {
				return f.Type, nil
			}
		}
		return nil, fmt.Errorf("record %s has no field %q", v.FullName(), step.Field)
	default:
		if step.Index >= 0 {
			return nil, fmt.Errorf("the schema here is %s, not an array", describe(t))
		}
		return nil, fmt.Errorf("the schema here is %s, not a record", describe(t))
	}
}

// holdsKey reports whether a value generated from the key avsc conforms to the
// type at the key path. Avro encodes against the exact type, so a primitive
// must match in kind and logical overlay, and a named type by full name.
func holdsKey(at, key Type) error {
	mismatch := fmt.Errorf("the schema here is %s but the key avsc is %s", describeLogical(at), describeLogical(key))
	switch k := key.(type) {
	case *Primitive:
		p, ok := at.(*Primitive)
		if !ok || p.Kind != k.Kind || logicalKind(p.Logical) != logicalKind(k.Logical) {
			return mismatch
		}
		return nil
	case *Record:
		r, ok := at.(*Record)
		if !ok || r.FullName() != k.FullName() {
			return mismatch
		}
		return nil
	case *Enum:
		e, ok := at.(*Enum)
		if !ok || e.FullName() != k.FullName() {
			return mismatch
		}
		return nil
	case *Fixed:
		f, ok := at.(*Fixed)
		if !ok || f.FullName() != k.FullName() {
			return mismatch
		}
		return nil
	default:
		return fmt.Errorf("a key avsc of type %s cannot be planted", describe(key))
	}
}

func logicalKind(lt *LogicalType) LogicalTypeKind {
	if lt == nil {
		return ""
	}
	return lt.Kind
}

// describeLogical names a type including its logical overlay, so a mismatch
// between a plain long and a timestamp reads as the difference it is.
func describeLogical(t Type) string {
	if p, ok := t.(*Primitive); ok && p.Logical != nil {
		return fmt.Sprintf("%s (%s)", p.Kind, p.Logical.Kind)
	}
	return describe(t)
}
