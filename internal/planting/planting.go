// Package planting owns every Planting of a run: a value written into every
// message at a location generation guarantees - the Key at the Key path
// (ADR-0009), a Topic parameter's value at its location in the Payload or the
// Headers (#83, #93). New checks all of them before any record exists, against
// each Message type, overlaps included, and names the flag each refusal
// belongs to; from then on each record is one Set.Plant.
//
// What a path may legally traverse is the business of a Walk, one per schema
// language, which follows the generator's own rules: the JSON Schema walk in
// internal/generator, the avsc walk in internal/avro. Planting itself is
// format-blind: both generators emit plain maps and slices along a guaranteed
// path.
package planting

import (
	"fmt"
)

// Walk is one schema language's account of where generation guarantees a
// value.
type Walk interface {
	// Locate follows path from the schema's root, resolving each Token step
	// to a field or an index as the schema reads it, and returns the
	// resolved steps and the field at the end. A step generation does not
	// guarantee in every record is refused with a *StepError naming it.
	Locate(path []Step) ([]Step, Field, error)
}

// Field is the schema at the end of a located path.
type Field interface {
	// HoldsKey reports whether every Key the run's key schema generates
	// conforms to the field.
	HoldsKey() error
	// Holds reports whether a Topic parameter's string value conforms to the
	// field.
	Holds(value string) error
}

// StepError is a Walk's refusal of the step at index Step of a path. New
// renders the path up to it in the syntax its owner wrote it in.
type StepError struct {
	Step int
	Err  error
}

func (e *StepError) Error() string { return fmt.Sprintf("step %d: %v", e.Step, e.Err) }

func (e *StepError) Unwrap() error { return e.Err }

// MessageType is what New walks for one Message type: its Payload schema, and
// its headers schema, nil when it declares none.
type MessageType struct {
	Name    string
	Payload Walk
	Headers Walk
}

// Parameter is a Topic parameter with the value -topic fills it with, and its
// location: Pointer's tokens into the Payload, or into the Headers when
// InHeaders; a nil Pointer plants nothing.
type Parameter struct {
	Name      string
	Value     string
	Location  string
	Pointer   []string
	InHeaders bool
}

// Error is a Planting the run cannot honour. Flag names the option it
// belongs to, without its dash: keyPath for the Key's, topic for a Topic
// parameter's.
type Error struct {
	Flag string
	Err  error
}

func (e *Error) Error() string { return e.Err.Error() }

func (e *Error) Unwrap() error { return e.Err }

// Set is the Plantings of one Message type.
type Set struct {
	keyPath string
	key     []Step
	payload []planted
	headers []planted
}

// planted is one Topic parameter's value and the path it is planted along.
type planted struct {
	param Parameter
	path  []Step
}

// New checks every Planting of a run against each Message type, before any
// record exists, and returns each type's Set, in the order of types. Every
// location must be guaranteed and hold its value there; no two Plantings may
// land in the same field. Headers are no place for the Key, so -keyPath never
// clashes with a header location. keyPath is empty when the Key is not
// planted.
func New(types []MessageType, keyPath string, params []Parameter) ([]*Set, error) {
	sets := make([]*Set, len(types))
	for i := range sets {
		sets[i] = &Set{keyPath: keyPath}
	}
	// A malformed -keyPath overlaps nothing; it is reported after the Topic
	// parameters, as its own error.
	var keySteps []Step
	var keyErr error
	if keyPath != "" {
		keySteps, keyErr = ParsePath(keyPath)
	}

	for _, headers := range []bool{false, true} {
		for _, p := range params {
			if p.Pointer == nil || p.InHeaders != headers {
				continue
			}
			for i, mt := range types {
				path, err := locate(mt, p, len(types) > 1)
				if err != nil {
					return nil, err
				}
				set := sets[i]
				if !headers && keyErr == nil && keySteps != nil && Overlap(path, keySteps) {
					return nil, &Error{Flag: "keyPath", Err: fmt.Errorf("Topic parameter %s: location %s overlaps -keyPath %s; both would plant into the same field", p.Name, p.Location, keyPath)}
				}
				others := &set.payload
				if headers {
					others = &set.headers
				}
				for _, other := range *others {
					if Overlap(path, other.path) {
						return nil, &Error{Flag: "topic", Err: fmt.Errorf("Topic parameters %s and %s plant into the same field (%s and %s)", other.param.Name, p.Name, other.param.Location, p.Location)}
					}
				}
				*others = append(*others, planted{param: p, path: path})
			}
		}
	}

	if keyPath == "" {
		return sets, nil
	}
	if keyErr != nil {
		return nil, &Error{Flag: "keyPath", Err: keyErr}
	}
	for i, mt := range types {
		path, err := locateKey(mt.Payload, keyPath, keySteps)
		if err != nil {
			if len(types) > 1 {
				err = fmt.Errorf("in Message type %s: %w", mt.Name, err)
			}
			return nil, &Error{Flag: "keyPath", Err: err}
		}
		sets[i].key = path
	}
	return sets, nil
}

// locate checks a Topic parameter's location in one Message type: guaranteed,
// and holding the value there.
func locate(mt MessageType, p Parameter, several bool) ([]Step, error) {
	inType := ""
	if several {
		inType = "in Message type " + mt.Name + ": "
	}
	refuse := func(format string, args ...any) error {
		return &Error{Flag: "topic", Err: fmt.Errorf("Topic parameter %s: "+format, append([]any{p.Name}, args...)...)}
	}
	walk, part := mt.Payload, "Payload field"
	if p.InHeaders {
		walk, part = mt.Headers, "header"
		if walk == nil {
			return nil, refuse("location %s: %s%s declares no headers", p.Location, inType, mt.Name)
		}
	}
	path, field, err := walk.Locate(tokens(p.Pointer))
	if err != nil {
		return nil, refuse("location %s: %s%v", p.Location, inType, stepError(err, func(i int) string { return pointerString(p.Pointer[:i+1]) }))
	}
	if err := field.Holds(p.Value); err != nil {
		return nil, refuse("value %s does not conform to the %s at %s: %s%v", p.Value, part, p.Location, inType, err)
	}
	return path, nil
}

// locateKey checks the Key path in one Message type's Payload: guaranteed,
// and holding the Key there.
func locateKey(walk Walk, keyPath string, steps []Step) ([]Step, error) {
	at := func(i int) string { return PathString(steps[:i+1]) }
	path, field, err := walk.Locate(steps)
	if err != nil {
		return nil, &PathError{Path: keyPath, Detail: stepError(err, at).Error()}
	}
	if err := field.HoldsKey(); err != nil {
		return nil, &PathError{Path: keyPath, Detail: fmt.Sprintf("at %q: %v", at(len(steps)-1), err)}
	}
	return path, nil
}

// stepError renders a Walk's refusal with the path up to the refused step,
// as at renders it.
func stepError(err error, at func(i int) string) error {
	if se, ok := err.(*StepError); ok {
		return fmt.Errorf("at %q: %w", at(se.Step), se.Err)
	}
	return err
}

// Plant writes every Planting of the Set into one record: the Topic
// parameters' values into its Payload and Headers, the Key into its Payload.
// headers is nil when the Message type declares none. A miss is a defect,
// since New checked every path, and is reported against its Planting.
func (s *Set) Plant(payload, headers, key any) error {
	for _, p := range s.payload {
		if err := plantParam(payload, p); err != nil {
			return err
		}
	}
	for _, p := range s.headers {
		if err := plantParam(headers, p); err != nil {
			return err
		}
	}
	if s.key == nil {
		return nil
	}
	if i, err := plant(payload, s.key, key); err != nil {
		return &Error{Flag: "keyPath", Err: &PathError{Path: s.keyPath, Detail: fmt.Sprintf("at %q: the Payload %v", PathString(s.key[:i+1]), err)}}
	}
	return nil
}

func plantParam(into any, p planted) error {
	if i, err := plant(into, p.path, p.param.Value); err != nil {
		return &Error{Flag: "topic", Err: fmt.Errorf("Topic parameter %s: planting at location %s: at %q: the value %w", p.param.Name, p.param.Location, pointerString(p.param.Pointer[:i+1]), err)}
	}
	return nil
}
