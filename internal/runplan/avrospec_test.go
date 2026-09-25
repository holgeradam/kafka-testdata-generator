package runplan

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
)

// avroSpec declares its Payload, and optionally its Key, in Avro (#84): the
// run needs no AVRO flags.
const avroSpec = `
asyncapi: '2.6.0'
info: {title: Avro, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        payload:
          type: record
          name: OrderCreated
          fields:
            - {name: id, type: string}
            - {name: region, type: string}
`

// avroKeyedSpec adds an Avro Key binding to avroSpec.
const avroKeyedSpec = `
asyncapi: '2.6.0'
info: {title: Avro, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        bindings: {kafka: {key: {type: string}}}
        payload: {type: record, name: OrderCreated, fields: [{name: id, type: string}]}
`

// TestPlanInfersWireFormat is the inference table (#84 decisions 2 and 9):
// the Wire format follows wherever the Payload's schema comes from, and
// -format only confirms it.
func TestPlanInfersWireFormat(t *testing.T) {
	jsonSpec := write(t, "spec.yaml", plainSpec)
	avroPayloads := write(t, "avro.yaml", avroSpec)
	value := write(t, "value.avsc", valueAvsc)

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"json spec", []string{"-spec", jsonSpec}, "json"},
		{"json spec, -format json", []string{"-spec", jsonSpec, "-format", "json"}, "json"},
		{"json spec, -avro-schema", []string{"-spec", jsonSpec, "-avro-schema", value}, "avro"},
		{"json spec, -format avro, -avro-schema", []string{"-spec", jsonSpec, "-format", "avro", "-avro-schema", value}, "avro"},
		{"avro spec", []string{"-spec", avroPayloads}, "avro"},
		{"avro spec, -format avro", []string{"-spec", avroPayloads, "-format", "avro"}, "avro"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := Plan(append(c.args, "-topic", "orders", "-dry-run"))
			if err != nil {
				t.Fatalf("Plan: %v", err)
			}
			if r.Format != c.want {
				t.Errorf("Format = %q, want %q", r.Format, c.want)
			}
		})
	}
}

// TestPlanAvroSpecRejects is the rule table for Avro payloads in the spec:
// flag conflicts, formats a Kafka topic cannot mix, and registry bindings the
// tool does not honour.
func TestPlanAvroSpecRejects(t *testing.T) {
	jsonSpec := write(t, "spec.yaml", plainSpec)
	avroPayloads := write(t, "avro.yaml", avroSpec)
	keyed := write(t, "keyed.yaml", avroKeyedSpec)
	value := write(t, "value.avsc", valueAvsc)
	key := write(t, "key.avsc", keyAvsc)
	mixed := write(t, "mixed.yaml", `
asyncapi: '2.6.0'
info: {title: Mixed, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        oneOf:
          - {name: OrderCreated, schemaFormat: 'application/vnd.apache.avro;version=1.9.0', payload: {type: record, name: OrderCreated, fields: []}}
          - {name: OrderUpdated, payload: {type: object}}
          - {name: OrderPaid, schemaFormat: 'application/vnd.apache.avro;version=1.9.0', payload: {type: record, name: OrderPaid, fields: []}}
`)
	several := write(t, "several.yaml", `
asyncapi: '2.6.0'
info: {title: Several, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        oneOf:
          - {name: OrderCreated, schemaFormat: 'application/vnd.apache.avro;version=1.9.0', payload: {type: record, name: OrderCreated, fields: []}}
          - {name: OrderPaid, schemaFormat: 'application/vnd.apache.avro;version=1.9.0', payload: {type: record, name: OrderPaid, fields: []}}
`)
	brokenAvsc := write(t, "broken.yaml", `
asyncapi: '2.6.0'
info: {title: Broken, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        payload: {type: record, name: OrderCreated, fields: [{name: n, type: nope}]}
`)

	cases := []struct {
		name     string
		args     []string
		wantFlag string
		wantText string
		wantAs   any
	}{
		{"-format json against Avro payloads", []string{"-spec", avroPayloads, "-format", "json"}, "format", "payload of OrderCreated is Avro, so the Wire format is avro; drop -format json", nil},
		{"-format json beside -avro-schema", []string{"-spec", jsonSpec, "-format", "json", "-avro-schema", value}, "format", "-avro-schema makes the Wire format avro; drop -format json", nil},
		{"-avro-schema beside Avro payloads", []string{"-spec", avroPayloads, "-avro-schema", value}, "avro-schema", "the spec declares the Payload's avsc (payload of OrderCreated is Avro) and -avro-schema gives another", nil},
		{"-avro-key-schema beside an Avro Key binding", []string{"-spec", keyed, "-avro-key-schema", key}, "avro-key-schema", "the spec declares the Key's avsc (bindings.kafka.key of OrderCreated) and -avro-key-schema gives another", nil},
		{"-avro-key-schema without an avsc", []string{"-spec", jsonSpec, "-avro-key-schema", key}, "avro-key-schema", "-avro-key-schema needs the avro Wire format: pass -avro-schema, or use a spec with Avro payloads", nil},
		{"-format avro without an avsc", []string{"-spec", jsonSpec, "-format", "avro"}, "avro-schema", "-avro-schema is required with -format avro when the spec's payloads are JSON Schema", nil},
		{"-keyPath without a key avsc", []string{"-spec", avroPayloads, "-keyPath", "id"}, "keyPath", "-keyPath requires a key avsc: -avro-key-schema, or a Key binding beside the spec's Avro payload", nil},
		{"producing without a registry", []string{"-spec", avroPayloads, "-produce"}, "registry", "-registry is required to produce with the avro Wire format", nil},
		{"mixed payload formats", []string{"-spec", mixed}, "topic", `Kafka topic "orders" mixes payload formats: Avro (OrderCreated, OrderPaid) and JSON Schema (OrderUpdated); a Kafka topic is produced in one Wire format`, nil},
		{"several Avro Message types", []string{"-spec", several}, "topic", "several Avro Message types (OrderCreated, OrderPaid) are not supported yet", nil},
		{"malformed spec avsc", []string{"-spec", brokenAvsc}, "topic", "payload of OrderCreated", new(*avro.ParseError)},
		{"key path not in the spec avsc", []string{"-spec", keyed, "-keyPath", "missing"}, "keyPath", "no field", new(*keyplan.PathError)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			args := append([]string{"-topic", "orders"}, c.args...)
			if i := indexOf(args, "-produce"); i >= 0 {
				args = append(args[:i], args[i+1:]...)
			} else {
				args = append(args, "-dry-run")
			}
			r, err := Plan(args)
			if err == nil {
				t.Fatalf("Plan(%v) = %+v, want an error", args, r)
			}
			var pe *Error
			if !errors.As(err, &pe) {
				t.Fatalf("error %v is %T, want *runplan.Error", err, err)
			}
			if pe.Flag != c.wantFlag {
				t.Errorf("Flag = %q, want %q (%v)", pe.Flag, c.wantFlag, err)
			}
			if !strings.Contains(err.Error(), c.wantText) {
				t.Errorf("error %v, want it to mention %q", err, c.wantText)
			}
			if c.wantAs != nil && !errors.As(err, c.wantAs) {
				t.Errorf("error %v does not wrap %T", err, c.wantAs)
			}
		})
	}
}

func indexOf(args []string, s string) int {
	for i, a := range args {
		if a == s {
			return i
		}
	}
	return -1
}

// TestPlanRegistryBinding proves the registry fields of a Kafka message
// binding pass when the tool honours them and stop an AVRO run otherwise,
// since Confluent framing would give consumers records they cannot read (#84
// decision 8). A JSON run has no schema ID, so it ignores them.
func TestPlanRegistryBinding(t *testing.T) {
	spec := func(binding string) string {
		return write(t, "spec.yaml", `
asyncapi: '2.6.0'
info: {title: Registry, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        bindings: {kafka: `+binding+`}
        payload: {type: record, name: OrderCreated, fields: [{name: id, type: string}]}
`)
	}
	for _, ok := range []string{
		`{schemaIdLocation: payload}`,
		`{schemaIdPayloadEncoding: confluent}`,
		`{schemaIdPayloadEncoding: 4}`,
		`{schemaIdPayloadEncoding: '4'}`,
		`{schemaLookupStrategy: TopicNameStrategy}`,
		`{schemaLookupStrategy: TopicIdStrategy}`,
	} {
		if _, err := Plan([]string{"-spec", spec(ok), "-topic", "orders", "-dry-run"}); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for binding, want := range map[string]string{
		`{schemaIdLocation: header}`:                 "message OrderCreated: bindings.kafka.schemaIdLocation is header; the tool frames the schema ID in the payload (Confluent wire format), so it honours only payload",
		`{schemaIdPayloadEncoding: apicurio-new}`:    "message OrderCreated: bindings.kafka.schemaIdPayloadEncoding is apicurio-new; the tool encodes the schema ID the Confluent way, so it honours only confluent or 4",
		`{schemaLookupStrategy: RecordNameStrategy}`: "message OrderCreated: bindings.kafka.schemaLookupStrategy is RecordNameStrategy; the tool registers under <topic>-value, so it honours only TopicNameStrategy or TopicIdStrategy",
	} {
		_, err := Plan([]string{"-spec", spec(binding), "-topic", "orders", "-dry-run"})
		var pe *Error
		if !errors.As(err, &pe) || pe.Flag != "topic" || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want a topic error mentioning %q", binding, err, want)
		}
	}

	jsonSpec := write(t, "json.yaml", `
asyncapi: '2.6.0'
info: {title: Registry, version: '1.0.0'}
channels:
  orders:
    publish: {message: {bindings: {kafka: {schemaIdLocation: header}}, payload: {type: object}}}
`)
	if _, err := Plan([]string{"-spec", jsonSpec, "-topic", "orders", "-dry-run"}); err != nil {
		t.Errorf("JSON run: %v, want the registry fields ignored", err)
	}
	value := write(t, "value.avsc", valueAvsc)
	_, err := Plan([]string{"-spec", jsonSpec, "-topic", "orders", "-dry-run", "-avro-schema", value})
	if err == nil || !strings.Contains(err.Error(), "schemaIdLocation is header") {
		t.Errorf("AVRO run from -avro-schema: err = %v, want the header location refused", err)
	}
}

// TestPlanAvroSpecAccepts proves an AVRO run from the spec alone: the spec's
// avsc generates the Payload, its Key binding the Key, -avro-key-schema gives
// the Key when the spec declares none, -keyPath plants into the spec's avsc,
// and a Topic parameter is checked against it.
func TestPlanAvroSpecAccepts(t *testing.T) {
	avroPayloads := write(t, "avro.yaml", avroSpec)
	keyed := write(t, "keyed.yaml", avroKeyedSpec)
	key := write(t, "key.avsc", keyAvsc)
	templated := write(t, "templated.yaml", `
asyncapi: '2.6.0'
info: {title: Regional, version: '1.0.0'}
channels:
  orders.{region}:
    parameters: {region: {location: '$message.payload#/region'}}
    publish:
      message:
        name: OrderCreated
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        payload: {type: record, name: OrderCreated, fields: [{name: id, type: string}, {name: region, type: string}]}
`)

	plan := func(args ...string) *Run {
		t.Helper()
		r, err := Plan(append(args, "-dry-run", "-seed", "1"))
		if err != nil {
			t.Fatalf("Plan(%v): %v", args, err)
		}
		return r
	}
	payload := func(r *Run) map[string]any {
		t.Helper()
		v, err := r.Config.Generator.Value()
		if err != nil {
			t.Fatalf("generating: %v", err)
		}
		return v.(map[string]any)
	}

	r := plan("-spec", avroPayloads, "-topic", "orders")
	if p := payload(r); p["id"] == nil || p["region"] == nil {
		t.Errorf("payload = %v, want the spec avsc's record", p)
	}
	if r.Config.KeyPlan != nil {
		t.Error("no Key binding and no -avro-key-schema: KeyPlan must be nil")
	}
	if len(r.Warnings) != 0 {
		t.Errorf("warnings = %v, want none: the Key binding is read, not ignored", r.Warnings)
	}

	r = plan("-spec", keyed, "-topic", "orders", "-keyPath", "id")
	if r.Config.KeyPlan == nil {
		t.Fatal("an Avro Key binding must produce a KeyPlan")
	}
	p := payload(r)
	k, err := r.Config.KeyPlan.Apply(p)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if p["id"] != k {
		t.Errorf("planted %v, key %v; want the Key planted at -keyPath", p["id"], k)
	}

	if r := plan("-spec", avroPayloads, "-topic", "orders", "-avro-key-schema", key); r.Config.KeyPlan == nil {
		t.Error("-avro-key-schema beside Avro payloads without a Key binding must produce a KeyPlan")
	}

	if p := payload(plan("-spec", templated, "-topic", "orders.eu")); p["region"] != "eu" {
		t.Errorf("region = %v, want the Topic parameter planted", p["region"])
	}
	if _, err := Plan([]string{"-spec", templated, "-topic", "orders.eu", "-dry-run", "-keyPath", "region", "-avro-key-schema", key}); err == nil ||
		!strings.Contains(err.Error(), "overlaps -keyPath") {
		t.Errorf("err = %v, want the Topic parameter's location to clash with -keyPath", err)
	}
}

// TestNewEncoderRegistersSpecAvsc proves producing registers the spec's avsc
// under <topic>-value and its Key binding under <topic>-key, exactly as
// -avro-schema and -avro-key-schema are registered.
func TestNewEncoderRegistersSpecAvsc(t *testing.T) {
	spec := write(t, "keyed.yaml", avroKeyedSpec)
	registered := map[string]any{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subject := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/subjects/"), "/versions")
		body, _ := io.ReadAll(r.Body)
		var req struct{ Schema string }
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("registration body %s: %v", body, err)
		}
		var avsc any
		if err := json.Unmarshal([]byte(req.Schema), &avsc); err != nil {
			t.Errorf("registered schema %s is not JSON: %v", req.Schema, err)
		}
		registered[subject] = avsc
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		w.Write([]byte(`{"id":7}`))
	}))
	defer srv.Close()

	r, err := Plan([]string{"-spec", spec, "-topic", "orders", "-registry", srv.URL})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if _, err := r.NewEncoder(context.Background()); err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	want := map[string]any{
		"orders-value": map[string]any{"type": "record", "name": "OrderCreated", "fields": []any{map[string]any{"name": "id", "type": "string"}}},
		"orders-key":   map[string]any{"type": "string"},
	}
	if !reflect.DeepEqual(registered, want) {
		t.Errorf("registered %v, want %v", registered, want)
	}
}
