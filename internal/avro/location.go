package avro

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
)

// Locate walks a JSON Pointer into the value avsc, as the location of a Topic
// parameter (#83), and returns the steps to plant along and the type of the
// field there. Every token names a record field: as for a key path
// (KeyChecker), only record fields are present in every record, so a union,
// map or array on the way, or a union at the end, is refused.
func Locate(value *Schema, pointer []string) ([]keyplan.Step, Type, error) {
	steps := make([]keyplan.Step, len(pointer))
	current := value.Root
	for i, token := range pointer {
		steps[i] = keyplan.Step{Field: token, Index: -1}
		next, err := field(current, steps[i])
		if err != nil {
			return nil, nil, locationError(pointer, i, err)
		}
		current = next
	}
	if _, ok := current.(*Union); ok {
		return nil, nil, locationError(pointer, len(pointer)-1, fmt.Errorf("the schema here is a union, so the branch differs per record and the value may be null"))
	}
	return steps, current, nil
}

// uuidText is the textual form a uuid logical type holds.
var uuidText = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// HoldsString reports whether the Avro type at a location, written as at, can
// hold a string value as generated for it: a string, a uuid in its textual
// form, or an enum symbol. Anything else would not encode against the avsc.
func HoldsString(t Type, value, at string) error {
	switch v := t.(type) {
	case *Primitive:
		if v.Kind != KindString {
			break
		}
		if logicalKind(v.Logical) == "uuid" && !uuidText.MatchString(value) {
			return fmt.Errorf("value %s is not a uuid, which the Payload field at %s (%s) requires", value, at, describeLogical(t))
		}
		return nil
	case *Enum:
		for _, s := range v.Symbols {
			if s == value {
				return nil
			}
		}
		return fmt.Errorf("value %s is not a symbol of enum %s [%s]", value, v.FullName(), strings.Join(v.Symbols, ", "))
	}
	return fmt.Errorf("the Payload field at %s is %s, which cannot hold the parameter's string value", at, describeLogical(t))
}

// locationError names the token that failed, as the pointer up to it.
func locationError(pointer []string, i int, err error) error {
	escaped := make([]string, i+1)
	for j, token := range pointer[:i+1] {
		escaped[j] = strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
	}
	return fmt.Errorf("at %q: %w", "/"+strings.Join(escaped, "/"), err)
}
