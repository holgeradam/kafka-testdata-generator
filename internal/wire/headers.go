package wire

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/planting"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// ValueSource generates one value from the schema it is bound to.
type ValueSource interface {
	Value() (any, error)
}

// Mix generates each message whole (#108): it picks a Message type from the
// seeded stream (#74), generates that type's Payload, then its Headers, then
// the Key, and plants every Planting of the type before encoding the Headers.
// A single Message type draws no pick, so its output is exactly what
// generating from it alone gives. Both Wire formats mix this way, each binding
// its own schemas.
type Mix struct {
	Synth *synth.Synthesizer
	// Types generate each Message type's Payload, in Message type order.
	Types []ValueSource
	// Headers generate each Message type's Headers; nil when none declares
	// any.
	Headers *HeaderSource
	// Key generates each message's Key; nil when the run has no key schema,
	// and every record carries a null Key.
	Key keyplan.Generator
	// Plantings are each Message type's Plantings, in Message type order.
	Plantings []*planting.Set
}

func (m *Mix) Generate() (pipeline.Generated, error) {
	i := 0
	if len(m.Types) > 1 {
		i = m.Synth.Pick(len(m.Types))
	}
	payload, err := m.Types[i].Value()
	if err != nil {
		return pipeline.Generated{}, err
	}
	headers, err := m.Headers.value(i)
	if err != nil {
		return pipeline.Generated{}, err
	}
	var key any
	if m.Key != nil {
		if key, err = m.Key.Value(); err != nil {
			return pipeline.Generated{}, err
		}
	}
	if err := m.Plantings[i].Plant(payload, headers, key); err != nil {
		return pipeline.Generated{}, err
	}
	encoded, err := m.Headers.encode(i, headers)
	return pipeline.Generated{Type: i, Key: key, Payload: payload, Headers: encoded}, err
}

// HeaderSource generates each record's Headers from its Message type's
// headers schema, a JSON Schema object in every Wire format (#85 decision 1),
// and encodes them: one Kafka record header per property.
type HeaderSource struct {
	gen     *generator.Generator
	types   []asyncapi.MessageType
	schemas []map[string]any
}

// NewHeaderSource binds each Message type's headers schema to the generator,
// in the order the Wire format holds them, or returns nil when none declares
// headers: such a run draws nothing for Headers from the seeded stream, so its
// records are what they were before Headers existed.
func NewHeaderSource(s *synth.Synthesizer, types []asyncapi.MessageType) *HeaderSource {
	schemas := make([]map[string]any, len(types))
	declared := false
	for i, mt := range types {
		schemas[i] = mt.Headers
		declared = declared || mt.Headers != nil
	}
	if !declared {
		return nil
	}
	return &HeaderSource{gen: generator.New(s), types: types, schemas: schemas}
}

// value generates the Headers of a record of Message type i, as an object
// yet to be planted into and encoded: nil when that type declares none.
func (h *HeaderSource) value(i int) (any, error) {
	if h == nil || h.schemas[i] == nil {
		return nil, nil
	}
	v, err := h.gen.Value(h.schemas[i])
	if err != nil {
		return nil, fmt.Errorf("headers of %s: %w", h.types[i].Name, err)
	}
	return v, nil
}

// encode turns the Headers of a record of Message type i into Kafka record
// headers.
func (h *HeaderSource) encode(i int, v any) ([]pipeline.Header, error) {
	if h == nil || h.schemas[i] == nil {
		return nil, nil
	}
	values, _ := v.(map[string]any)
	return EncodeHeaders(h.schemas[i], values)
}

// EncodeHeaders turns a generated headers object into Kafka record headers:
// one per property, in the order the headers schema declares them (#96), an
// undeclared one after them by name, each value plain-scalar - an object's
// JSON text in its own declared order - and a null value a null header (#85
// decision 2).
func EncodeHeaders(schema map[string]any, values map[string]any) ([]pipeline.Header, error) {
	obj, _ := generator.Ordered(schema, values).(ordered.Object)
	headers := make([]pipeline.Header, len(obj.Keys))
	for i, name := range obj.Keys {
		value, err := PlainScalar(obj.Values[i])
		if err != nil {
			return nil, fmt.Errorf("header %s: %w", name, err)
		}
		headers[i] = pipeline.Header{Name: name, Value: value}
	}
	return headers, nil
}

// PlainScalar renders a value by the plain-scalar contract (CONTEXT.md Key)
// that JSON Keys and all Headers follow: a string as UTF-8, a number as
// decimal text, a boolean as true or false, and a structured value as JSON -
// never a JSON-wrapped scalar. nil yields nil bytes: a null Key or header.
func PlainScalar(value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case string:
		return []byte(v), nil
	case float64:
		// The JSON generator produces numbers as float64; any other numeric
		// type falls through to JSON, which renders integers as decimal text.
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("cannot encode non-finite number %v", v)
		}
		return []byte(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case bool:
		return []byte(strconv.FormatBool(v)), nil
	case []byte:
		return v, nil
	default:
		// Objects, arrays, and any other structured value become JSON.
		return json.Marshal(v)
	}
}
