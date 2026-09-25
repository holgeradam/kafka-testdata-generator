package asyncapi

import (
	"reflect"
	"testing"
)

const head2 = "asyncapi: '2.6.0'\ninfo: {title: T, version: '1'}\n"

// parameterValues reads the Topic parameters of a Kafka topic as name=value.
func parameterValues(t *testing.T, doc *Document, name string) map[string]string {
	t.Helper()
	got := map[string]string{}
	for _, p := range readTopic(t, doc, name).Parameters {
		got[p.Name] = p.Value
	}
	return got
}

// TestTopicParameter2Matching proves -topic fills a templated 2.x spec entry
// key as it fills a 3.0 address: one parameter, several, a template that
// cannot match, and a literal key, which matches only itself.
func TestTopicParameter2Matching(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders.{region}:
    publish: {message: {name: created, payload: {type: object}}}
  '{env}.payments.{region}':
    publish: {message: {name: paid, payload: {type: object}}}
  audit.eu:
    publish: {message: {name: logged, payload: {type: object}}}
`)
	cases := []struct {
		topic, types string
		want         map[string]string
	}{
		{"orders.eu", "created", map[string]string{"region": "eu"}},
		{"orders.us-east.1", "created", map[string]string{"region": "us-east.1"}},
		{"prod.payments.eu", "paid", map[string]string{"env": "prod", "region": "eu"}},
		{"audit.eu", "logged", map[string]string{}},
	}
	for _, c := range cases {
		types, err := messageTypes(doc, c.topic)
		if err != nil {
			t.Fatalf("%s: %v", c.topic, err)
		}
		if got := names(types); got != c.types {
			t.Errorf("%s: Message types %s, want %s", c.topic, got, c.types)
		}
		if got := parameterValues(t, doc, c.topic); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: Topic parameters %v, want %v", c.topic, got, c.want)
		}
	}
	for _, miss := range []string{"orders.", "payments.eu", "audit.us"} {
		_, err := doc.Topic(miss)
		wantErr(t, err, `Kafka topic "`+miss+`" not found`)
	}
}

// TestTopicParameter2Ambiguous proves the run stops rather than choosing when
// -topic fills 2.x keys in different ways, a literal key beside a template it
// fills included.
func TestTopicParameter2Ambiguous(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  '{a}.{b}':
    publish: {message: {name: created, payload: {type: object}}}
`)
	_, err := doc.Topic("x.y.z")
	wantErr(t, err, `Kafka topic "x.y.z"`, "spec entry {a}.{b}", "more than one way")

	doc = loadSpec(t, head2+`
channels:
  orders.{region}:
    publish: {message: {name: created, payload: {type: object}}}
  orders.eu:
    publish: {message: {name: paid, payload: {type: object}}}
`)
	_, err = doc.Topic("orders.eu")
	wantErr(t, err, "different ways", "orders.eu (orders.eu: no Topic parameters)", "orders.{region} (orders.{region}: region=eu)")
}

// TestTopicParameter2Binding proves a declared bindings.kafka.topic names the
// Kafka topic literally, as in 3.0: the templated key is then only an id and
// gives no Topic parameters.
func TestTopicParameter2Binding(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders.{region}:
    bindings: {kafka: {topic: orders.all}}
    publish: {message: {name: created, payload: {type: object}}}
  payments.{region}:
    bindings: {kafka: {topic: 'payments.{region}'}}
    publish: {message: {name: paid, payload: {type: object}}}
`)
	if got := parameterValues(t, doc, "orders.all"); len(got) != 0 {
		t.Errorf("orders.all: Topic parameters %v, want none", got)
	}
	_, err := doc.Topic("orders.eu")
	wantErr(t, err, `Kafka topic "orders.eu" not found`)
	if got := parameterValues(t, doc, "payments.{region}"); len(got) != 0 {
		t.Errorf("payments.{region}: Topic parameters %v, want none", got)
	}
	_, err = doc.Topic("payments.eu")
	wantErr(t, err, `Kafka topic "payments.eu" not found`)
}

// TestTopicParameter2Schema proves the value must conform to the parameter's
// full schema, with the parameter and its schema reachable through $refs.
func TestTopicParameter2Schema(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders.{region}:
    parameters:
      region: {$ref: '#/components/parameters/region'}
    publish: {message: {name: created, payload: {type: object}}}
components:
  parameters:
    region: {schema: {$ref: '#/components/schemas/Region'}}
  schemas:
    Region: {type: string, enum: [eu, us, useast], pattern: '^[a-z]{2}$'}
`)
	if got := parameterValues(t, doc, "orders.eu"); got["region"] != "eu" {
		t.Errorf("region = %q, want eu", got["region"])
	}
	_, err := doc.Topic("orders.apac")
	wantErr(t, err, `Kafka topic "orders.apac": region value apac does not conform to the parameter's schema`, "eu", "us")
	_, err = doc.Topic("orders.useast")
	wantErr(t, err, `Kafka topic "orders.useast": region value useast does not conform to the parameter's schema`, "^[a-z]{2}$")
}

// TestTopicParameter2Strings proves a Topic parameter is a string: a schema
// that does not allow one stops the run, and one that does may allow more.
func TestTopicParameter2Strings(t *testing.T) {
	load := func(schema string) *Document {
		return loadSpec(t, head2+`
channels:
  users.{userId}:
    parameters:
      userId: {schema: `+schema+`}
    publish: {message: {name: created, payload: {type: object}}}
`)
	}
	for _, schema := range []string{`{type: integer}`, `{type: [integer, "null"]}`} {
		_, err := load(schema).Topic("users.42")
		wantErr(t, err, "spec entry users.{userId}: parameter userId: its schema does not allow a string, and a Topic parameter is a string")
	}
	for _, schema := range []string{`{type: string}`, `{type: [string, "null"]}`, `{minLength: 1}`} {
		if got := parameterValues(t, load(schema), "users.42"); got["userId"] != "42" {
			t.Errorf("%s: userId = %q, want 42", schema, got["userId"])
		}
	}
}

// TestTopicParameter2Location proves a 2.x parameter's location is read as a
// 3.0 one's: a payload location gives a JSON Pointer into the Payload, and a
// header location one into the Headers (#93).
func TestTopicParameter2Location(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders.{region}:
    parameters:
      region: {schema: {type: string}, location: '$message.payload#/meta/region'}
    publish: {message: {name: created, payload: {type: object}}}
  tenants.{tenant}:
    parameters:
      tenant: {location: '$message.header#/meta/tenant'}
    publish: {message: {name: joined, payload: {type: object}}}
`)
	got := readTopic(t, doc, "orders.eu").Parameters
	want := []TopicParameter{{Name: "region", Value: "eu", Location: "$message.payload#/meta/region", Pointer: []string{"meta", "region"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parameters = %+v, want %+v", got, want)
	}
	got = readTopic(t, doc, "tenants.acme").Parameters
	want = []TopicParameter{{Name: "tenant", Value: "acme", Location: "$message.header#/meta/tenant", Pointer: []string{"meta", "tenant"}, InHeaders: true}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parameters = %+v, want %+v", got, want)
	}
}

// TestTopicParameter2RejectMistakes proves a malformed 2.x parameter stops
// the run with its own error, and a parameter of an unmatched key is never
// read.
func TestTopicParameter2RejectMistakes(t *testing.T) {
	cases := map[string]struct{ parameters, want string }{
		"schema not object":     {`{tenant: {schema: string}}`, "parameters.tenant: schema: must be an object"},
		"parameter not object":  {`{tenant: acme}`, "parameters.tenant: must be an object"},
		"parameters not object": {`[tenant]`, "spec entry orders.{tenant}: parameters: must be an object"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, head2+`
channels:
  orders.{tenant}:
    parameters: `+c.parameters+`
    publish: {message: {name: created, payload: {type: object}}}
  payments:
    publish: {message: {name: paid, payload: {type: object}}}
`)
			_, err := doc.Topic("orders.acme")
			wantErr(t, err, c.want)
			readTopic(t, doc, "payments")
		})
	}
}
