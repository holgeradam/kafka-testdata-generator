package avro_test

import (
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
	"github.com/holgeradam/kafka-testdata-generator/internal/planting"
)

// TestAvroWalkKeyPathNamedAndParameterisedTypes covers the shapes beyond
// primitives and records (#113): an avsc carries no constraint keywords, so a
// Key is safe exactly when it is the same type - a named type by full name,
// a decimal by precision and scale too.
func TestAvroWalkKeyPathNamedAndParameterisedTypes(t *testing.T) {
	const value = `{"type":"record","name":"Order","namespace":"com.acme","fields":[
		{"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},
		{"name":"hash","type":{"type":"fixed","name":"Hash","size":4}},
		{"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}`
	cases := []struct {
		name, key, path string
		wantErr         bool
	}{
		{"same enum", `{"type":"enum","name":"Status","namespace":"com.acme","symbols":["NEW","PAID"]}`, "status", false},
		{"other enum", `{"type":"enum","name":"Other","namespace":"com.acme","symbols":["NEW"]}`, "status", true},
		{"same fixed", `{"type":"fixed","name":"Hash","namespace":"com.acme","size":4}`, "hash", false},
		{"other fixed", `{"type":"fixed","name":"Digest","namespace":"com.acme","size":4}`, "hash", true},
		{"same decimal", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, "price", false},
		{"other precision", `{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}`, "price", true},
		{"other scale", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":4}`, "price", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkPath(t, value, c.key, c.path)
			if c.wantErr != (err != nil) {
				t.Errorf("Check error = %v, want error: %v", err, c.wantErr)
			}
		})
	}
}

// checkPath checks a -keyPath against a value avsc and a key avsc through the
// Planting module, as a run does.
func checkPath(t *testing.T, valueAvsc, keyAvsc, path string) error {
	t.Helper()
	parse := func(avsc string) *avro.Schema {
		s, err := avro.Parse([]byte(avsc))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		return s
	}
	walk := avro.NewWalk(parse(valueAvsc), parse(keyAvsc))
	_, err := planting.New([]planting.MessageType{{Name: "T", Payload: walk}}, path, nil)
	return err
}
