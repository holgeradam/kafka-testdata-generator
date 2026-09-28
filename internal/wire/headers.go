package wire

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
)

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
