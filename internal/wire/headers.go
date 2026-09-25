package wire

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// ValueSource generates one value from the schema it is bound to.
type ValueSource interface {
	Value() (any, error)
}

// Mix generates each Payload from one Message type picked from the seeded
// stream (#74), then its Headers. A single Message type draws no pick, so its
// output is exactly what generating from it alone gives. Both Wire formats
// mix this way, each binding its own schemas.
type Mix struct {
	Synth *synth.Synthesizer
	// Types generate each Message type's Payload, in Message type order.
	Types []ValueSource
	// Headers generate each Message type's Headers; nil when none declares
	// any.
	Headers *HeaderSource
}

func (m *Mix) Generate() (pipeline.Generated, error) {
	i := 0
	if len(m.Types) > 1 {
		i = m.Synth.Pick(len(m.Types))
	}
	v, err := m.Types[i].Value()
	if err != nil {
		return pipeline.Generated{}, err
	}
	headers, err := m.Headers.Generate(i)
	return pipeline.Generated{Type: i, Payload: v, Headers: headers}, err
}

// HeaderSource generates each record's Headers from its Message type's
// headers schema, a JSON Schema object in every Wire format (#85 decision 1),
// and encodes them: one Kafka record header per property.
type HeaderSource struct {
	gen     *generator.Generator
	types   []asyncapi.MessageType
	schemas []map[string]any
	// plants are the Topic parameter values planted into each Message
	// type's Headers (#93).
	plants []Plants
}

// NewHeaderSource returns the Headers source for the Message types, in the
// order the Wire format holds them, or nil when none declares headers: such a
// run draws nothing for Headers from the seeded stream, so its records are
// what they were before Headers existed.
//
// Each Topic parameter with a header location is checked against every
// Message type's headers schema before any record exists, as a payload
// location is against the Payload's (#83): the location must be guaranteed,
// the value must conform to the header's schema there, and no two plantings
// may land in the same header. Headers are no place for the Key, so -keyPath
// never clashes with them.
func NewHeaderSource(s *synth.Synthesizer, types []asyncapi.MessageType, params []asyncapi.TopicParameter) (*HeaderSource, error) {
	schemas := make([]map[string]any, len(types))
	declared := false
	for i, mt := range types {
		schemas[i] = mt.Headers
		declared = declared || mt.Headers != nil
	}
	plants, err := headerPlants(types, params)
	if err != nil {
		return nil, err
	}
	if !declared {
		return nil, nil
	}
	return &HeaderSource{gen: generator.New(s), types: types, schemas: schemas, plants: plants}, nil
}

// headerPlants checks each header location against every Message type and
// returns what to plant into each type's Headers.
func headerPlants(types []asyncapi.MessageType, params []asyncapi.TopicParameter) ([]Plants, error) {
	plants := make([]Plants, len(types))
	for _, tp := range params {
		if !tp.InHeaders {
			continue
		}
		refuse := func(format string, args ...any) error {
			return &Error{Flag: "topic", Detail: fmt.Sprintf("Topic parameter %s: ", tp.Name) + fmt.Sprintf(format, args...)}
		}
		if len(types) == 0 {
			return nil, refuse("location %s: under -avro-schema the records are of no Message type in the spec, so they have no Headers", tp.Location)
		}
		for i, mt := range types {
			inType := ""
			if len(types) > 1 {
				inType = "in Message type " + mt.Name + ": "
			}
			if mt.Headers == nil {
				return nil, refuse("location %s: %s%s declares no headers", tp.Location, inType, mt.Name)
			}
			steps, field, err := generator.Locate(mt.Headers, tp.Pointer)
			if err != nil {
				return nil, refuse("location %s: %s%v", tp.Location, inType, err)
			}
			if err := generator.Conforms(field, tp.Value); err != nil {
				return nil, refuse("value %s does not conform to the header at %s: %s%v", tp.Value, tp.Location, inType, err)
			}
			if plants[i], err = plants[i].Add(tp, steps, ""); err != nil {
				return nil, err
			}
		}
	}
	return plants, nil
}

// Generate returns the encoded Headers of a record of Message type i: none
// when that type declares no headers.
func (h *HeaderSource) Generate(i int) ([]pipeline.Header, error) {
	if h == nil || h.schemas[i] == nil {
		return nil, nil
	}
	v, err := h.gen.Value(h.schemas[i])
	if err != nil {
		return nil, fmt.Errorf("headers of %s: %w", h.types[i].Name, err)
	}
	if err := h.plants[i].Apply(v); err != nil {
		return nil, fmt.Errorf("headers of %s: %w", h.types[i].Name, err)
	}
	values, _ := v.(map[string]any)
	return EncodeHeaders(values)
}

// EncodeHeaders turns a generated headers object into Kafka record headers:
// one per property, sorted by name so a seeded run is byte-identical, each
// value plain-scalar and a null value a null header (#85 decision 2).
func EncodeHeaders(values map[string]any) ([]pipeline.Header, error) {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	headers := make([]pipeline.Header, len(names))
	for i, name := range names {
		value, err := PlainScalar(values[name])
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
