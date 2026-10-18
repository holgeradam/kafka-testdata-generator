package avro

import (
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/planting"
)

// checkAvroPath is the shim these tests drive: check a -keyPath against a
// value avsc and a key avsc through the Planting module, exactly as a run
// does.
func checkAvroPath(t *testing.T, valueAvsc, keyAvsc, path string) error {
	t.Helper()
	walk := NewWalk(mustParse(t, valueAvsc), mustParse(t, keyAvsc))
	_, err := planting.New([]planting.MessageType{{Name: "T", Payload: walk}}, path, nil)
	return err
}

const valueAvsc = `{"type":"record","name":"Order","namespace":"com.acme","fields":[
	{"name":"ref","type":{"type":"string","logicalType":"uuid"}},
	{"name":"plain","type":"string"},
	{"name":"seq","type":"long"},
	{"name":"when","type":{"type":"long","logicalType":"timestamp-millis"}},
	{"name":"maybe","type":["null","string"]},
	{"name":"tags","type":{"type":"array","items":"string"}},
	{"name":"attrs","type":{"type":"map","values":"string"}},
	{"name":"customer","type":{"type":"record","name":"Customer","fields":[
		{"name":"id","type":"string"}]}},
	{"name":"key","type":{"type":"record","name":"OrderKey","fields":[
		{"name":"id","type":"string"}]}}
]}`

const uuidKeyAvsc = `{"type":"string","logicalType":"uuid"}`
const stringKeyAvsc = `{"type":"string"}`

func TestAvroWalkKeyPathAcceptsGuaranteedPaths(t *testing.T) {
	cases := []struct{ key, path string }{
		{uuidKeyAvsc, "ref"},
		{stringKeyAvsc, "plain"},
		{stringKeyAvsc, "customer.id"},
		{`{"type":"long"}`, "seq"},
		{`{"type":"long","logicalType":"timestamp-millis"}`, "when"},
		{`{"type":"record","name":"OrderKey","namespace":"com.acme","fields":[{"name":"id","type":"string"}]}`, "key"},
	}
	for _, c := range cases {
		if err := checkAvroPath(t, valueAvsc, c.key, c.path); err != nil {
			t.Errorf("Check(%q) = %v, want nil", c.path, err)
		}
	}
}

// TestAvroWalkKeyPathRejectsUnguaranteedPaths covers every Avro shape whose
// value is not present in every record, plus a field that does not exist.
func TestAvroWalkKeyPathRejectsUnguaranteedPaths(t *testing.T) {
	cases := []struct{ path, want string }{
		{"maybe", "differs per record"},
		{"maybe.x", "differs per record"},
		{"tags[0]", "array"},
		{"attrs.anything", "map"},
		{"missing", "no field"},
		{"plain.deeper", "not a record"},
		{"seq[0]", "not an array"},
	}
	for _, c := range cases {
		err := checkAvroPath(t, valueAvsc, stringKeyAvsc, c.path)
		if err == nil {
			t.Errorf("Check(%q) = nil, want an error mentioning %q", c.path, c.want)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("Check(%q) error = %v, want it to mention %q", c.path, err, c.want)
		}
	}
}

// TestAvroWalkKeyPathTypeCompatibility proves the type at the path must be the
// key's type: same primitive kind and logical overlay, or the same named type
// for a record key, since that is what the serializer encodes against.
func TestAvroWalkKeyPathTypeCompatibility(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		path    string
		wantErr bool
	}{
		{"uuid into uuid", uuidKeyAvsc, "ref", false},
		{"plain string into uuid", stringKeyAvsc, "ref", true},
		{"uuid into plain string", uuidKeyAvsc, "plain", true},
		{"long into string", `{"type":"long"}`, "plain", true},
		{"timestamp into plain long", `{"type":"long","logicalType":"timestamp-millis"}`, "seq", true},
		{"matching record", `{"type":"record","name":"OrderKey","namespace":"com.acme","fields":[{"name":"id","type":"string"}]}`, "key", false},
		{"differently named record", `{"type":"record","name":"OtherKey","namespace":"com.acme","fields":[{"name":"id","type":"string"}]}`, "key", true},
		// A nested record inherits its parent's namespace, so an unqualified
		// key avsc names a different Avro type even with identical fields.
		{"same name, no namespace", `{"type":"record","name":"OrderKey","fields":[{"name":"id","type":"string"}]}`, "key", true},
		{"record into string", `{"type":"record","name":"OrderKey","namespace":"com.acme","fields":[{"name":"id","type":"string"}]}`, "plain", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkAvroPath(t, valueAvsc, c.key, c.path)
			if c.wantErr && err == nil {
				t.Errorf("Check = nil, want a type error")
			}
			if !c.wantErr && err != nil {
				t.Errorf("Check = %v, want nil", err)
			}
			if c.wantErr && err != nil && !strings.Contains(err.Error(), "key avsc") {
				t.Errorf("Check error = %v, want it to name the key avsc", err)
			}
		})
	}
}

// locateAvro checks a Topic parameter's location and value against a value
// avsc through the Planting module, as a run does.
func locateAvro(t *testing.T, valueAvsc, value string, pointer ...string) error {
	t.Helper()
	walk := NewWalk(mustParse(t, valueAvsc), nil)
	_, err := planting.New([]planting.MessageType{{Name: "T", Payload: walk}}, "", []planting.Parameter{
		{Name: "p", Value: value, Location: "$message.payload#/" + strings.Join(pointer, "/"), Pointer: pointer},
	})
	return err
}

// TestAvroWalkLocations proves a location's pointer tokens always name record
// fields, and that the type at the end must hold the parameter's string: a
// string, a uuid in its textual form, or an enum symbol.
func TestAvroWalkLocations(t *testing.T) {
	const avsc = `{"type":"record","name":"Order","fields":[
		{"name":"region","type":"string"},
		{"name":"ref","type":{"type":"string","logicalType":"uuid"}},
		{"name":"zone","type":{"type":"enum","name":"Zone","symbols":["eu","us"]}},
		{"name":"count","type":"int"},
		{"name":"note","type":["null","string"]},
		{"name":"meta","type":{"type":"record","name":"Meta","fields":[{"name":"tenant","type":"string"}]}}
	]}`
	for _, c := range []struct {
		value   string
		pointer []string
		want    string
	}{
		{"eu", []string{"region"}, ""},
		{"0b7e8c3a-6f2d-4a51-9c1e-2d3f4a5b6c7d", []string{"ref"}, ""},
		{"us", []string{"zone"}, ""},
		{"acme", []string{"meta", "tenant"}, ""},
		{"eu", []string{"ref"}, "string (uuid) requires a uuid"},
		{"apac", []string{"zone"}, "not a symbol of enum Zone [eu, us]"},
		{"7", []string{"count"}, "int cannot hold a string"},
		{"x", []string{"note"}, `at "/note": the schema here is a union`},
		{"x", []string{"meta", "nope"}, `at "/meta/nope": record Meta has no field "nope"`},
	} {
		err := locateAvro(t, avsc, c.value, c.pointer...)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%v = %s: %v, want it accepted", c.pointer, c.value, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%v = %s: %v, want it to mention %q", c.pointer, c.value, err, c.want)
		}
	}
}
