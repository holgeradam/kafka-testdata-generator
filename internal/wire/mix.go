package wire

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/planting"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// ValueSource generates one value from the schema it is bound to.
type ValueSource interface {
	Value() (any, error)
}

// Bound is one Message type as a run binds it: everything a message of that
// type is generated, planted and encoded with (#109). Each Wire format builds
// one slice of them, once, and hands that same slice to its Mix and its
// Encoder, so pipeline.Generated.Type indexes exactly one list. E is the
// format's own encoding data for the type.
type Bound[E any] struct {
	// Name is the Message type's name; empty for the records of
	// -avro-schema, which are of no Message type in the spec.
	Name string
	// Payload generates the type's Payload.
	Payload ValueSource
	// Walk is the Payload schema's walk, which Plant checks Plantings with.
	Walk planting.Walk
	// Headers is the type's headers schema, a JSON Schema object in every
	// Wire format (#85 decision 1); nil when it declares none.
	Headers map[string]any
	// Planting is the type's Plantings, set by Plant.
	Planting *planting.Set
	// Encoding is what the format's Encoder needs to encode the type.
	Encoding E
}

// Plant checks every Planting of the run against each bound Message type
// before any record exists (internal/planting) - -keyPath, and each Topic
// parameter's location in the Payload or the Headers - and gives each type
// its Plantings. A refusal is an Error on the flag the Planting belongs to.
func Plant[E any](types []Bound[E], keyPath string, params []asyncapi.TopicParameter) error {
	walks := make([]planting.MessageType, len(types))
	for i, b := range types {
		walks[i] = planting.MessageType{Name: b.Name, Payload: b.Walk}
		if b.Headers != nil {
			walks[i].Headers = generator.NewWalk(b.Headers, nil)
		}
	}
	ps := make([]planting.Parameter, len(params))
	for i, tp := range params {
		ps[i] = planting.Parameter{Name: tp.Name, Value: tp.Value, Location: tp.Location, Pointer: tp.Pointer, InHeaders: tp.InHeaders}
	}
	sets, err := planting.New(walks, keyPath, ps)
	var pe *planting.Error
	if errors.As(err, &pe) {
		return &Error{Flag: pe.Flag, Err: pe.Err}
	}
	if err != nil {
		return err
	}
	for i := range types {
		types[i].Planting = sets[i]
	}
	return nil
}

// Mix generates each message whole (#108): it picks a bound Message type from
// the seeded stream (#74), generates that type's Payload, then its Headers,
// then the Key, and plants every Planting of the type before encoding the
// Headers. A single Message type draws no pick, so its output is exactly what
// generating from it alone gives. Both Wire formats mix this way, each over
// its own bound types.
type Mix[E any] struct {
	synth   *synth.Synthesizer
	types   []Bound[E]
	headers *generator.Generator
	key     keyplan.Generator
}

// NewMix mixes types, which Plant has given their Plantings, drawing from the
// run's Synthesizer. key generates each message's Key; nil when the run has
// no key schema, and every record carries a null Key.
func NewMix[E any](s *synth.Synthesizer, types []Bound[E], key keyplan.Generator) *Mix[E] {
	return &Mix[E]{synth: s, types: types, headers: generator.New(s), key: key}
}

func (m *Mix[E]) Generate() (pipeline.Generated, error) {
	i := 0
	if len(m.types) > 1 {
		i = m.synth.Pick(len(m.types))
	}
	b := &m.types[i]
	payload, err := b.Payload.Value()
	if err != nil {
		return pipeline.Generated{}, err
	}
	var headers any
	if b.Headers != nil {
		if headers, err = m.headers.Value(b.Headers); err != nil {
			return pipeline.Generated{}, fmt.Errorf("headers of %s: %w", b.Name, err)
		}
	}
	var key any
	if m.key != nil {
		if key, err = m.key.Value(); err != nil {
			return pipeline.Generated{}, err
		}
	}
	if err := b.Planting.Plant(payload, headers, key); err != nil {
		return pipeline.Generated{}, err
	}
	g := pipeline.Generated{Type: i, Key: key, Payload: payload}
	if b.Headers != nil {
		values, _ := headers.(map[string]any)
		g.Headers, err = EncodeHeaders(b.Headers, values)
	}
	return g, err
}

// SharedKeyBinding returns the Key binding every Message type declares, in
// the Message types' format, nil when none declares one, or an error naming
// the Message types grouped by the binding they declare: a Key identifies one
// Entity across them (#34 decision 3).
func SharedKeyBinding(types []asyncapi.MessageType) (asyncapi.Schema, error) {
	var bindings []asyncapi.Schema
	var groups [][]string
	for _, mt := range types {
		i := 0
		for i < len(bindings) && !reflect.DeepEqual(bindings[i], mt.Key) {
			i++
		}
		if i == len(bindings) {
			bindings = append(bindings, mt.Key)
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], mt.Name)
	}
	switch len(bindings) {
	case 0:
		return nil, nil
	case 1:
		return bindings[0], nil
	}
	described := make([]string, len(groups))
	for i, g := range groups {
		described[i] = strings.Join(g, ", ")
		if bindings[i] == nil {
			described[i] += " (none)"
		}
	}
	return nil, &Error{Flag: "topic", Detail: fmt.Sprintf("the Message types of the Kafka topic declare different Key bindings (%s); a Key identifies one Entity across them, so they must declare the same one, or none", strings.Join(described, " vs "))}
}
