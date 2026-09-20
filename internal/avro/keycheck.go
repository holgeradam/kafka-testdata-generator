package avro

import (
	"fmt"

	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
)

// KeyChecker validates a key path against an Avro model: that generation puts a
// value at that path in every record, and that the type there is the key avsc's
// type, so the planted Payload still encodes against its own schema (ADR-0009).
// It is the avsc adapter of keyplan.Checker; the JSON Schema adapter lives in
// internal/generator.
//
// Only record fields are guaranteed in Avro. A union branch varies per record
// (including the null/optional idiom), an array has no minimum length, and map
// keys are generated, so a step into any of them is refused before the run.
type KeyChecker struct {
	value *Schema
	key   *Schema
}

// NewKeyChecker builds the checker from the parsed value and key avsc.
func NewKeyChecker(value, key *Schema) *KeyChecker {
	return &KeyChecker{value: value, key: key}
}

// Check reports whether path is usable, naming the offending step otherwise.
func (c *KeyChecker) Check(path []keyplan.Step) error {
	current := c.value.Root
	for i, step := range path {
		next, err := field(current, step)
		if err != nil {
			return keyPathError(path, i, err)
		}
		current = next
	}
	// A union at the end of the path is the nullable-field idiom: the value is
	// absent in some records, so it is refused with that reason rather than as
	// a type mismatch.
	if _, ok := current.(*Union); ok {
		return keyPathError(path, len(path)-1, fmt.Errorf("the schema here is a union, so the branch differs per record and the value may be null"))
	}
	if err := holdsKey(current, c.key.Root); err != nil {
		return keyPathError(path, len(path)-1, err)
	}
	return nil
}

// field takes one step into the model, refusing every shape whose value is not
// present in every record.
func field(t Type, step keyplan.Step) (Type, error) {
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

// keyPathError names the step that failed, so the message points at the part of
// the path that is wrong rather than the whole of it.
func keyPathError(path []keyplan.Step, i int, err error) error {
	return &keyplan.PathError{
		Path:   keyplan.PathString(path),
		Detail: fmt.Sprintf("at %q: %v", keyplan.PathString(path[:i+1]), err),
	}
}
