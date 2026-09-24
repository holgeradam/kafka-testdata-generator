package asyncapi

import (
	"reflect"
	"testing"
)

// readTopic reads a Kafka topic, failing the test on an error.
func readTopic(t *testing.T, doc *Document, name string) *Topic {
	t.Helper()
	topic, err := doc.Topic(name)
	if err != nil {
		t.Fatalf("Topic(%q): %v", name, err)
	}
	return topic
}

// TestTopicParameterMatching proves -topic fills a templated address: one
// parameter, several, a template that cannot match, and a literal address,
// which matches only itself and has no Topic parameters.
func TestTopicParameterMatching(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  regional:
    address: 'orders.{region}'
    messages: {created: {payload: {type: object}}}
  staged:
    address: '{env}.payments.{region}'
    messages: {paid: {payload: {type: object}}}
  literal:
    address: audit.eu
    messages: {logged: {payload: {type: object}}}
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
		topic := readTopic(t, doc, c.topic)
		if got := names(topic.MessageTypes); got != c.types {
			t.Errorf("%s: Message types %s, want %s", c.topic, got, c.types)
		}
		got := map[string]string{}
		for _, p := range topic.Parameters {
			got[p.Name] = p.Value
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: Topic parameters %v, want %v", c.topic, got, c.want)
		}
	}
	for _, miss := range []string{"orders.", "payments.eu", "audit.us"} {
		_, err := doc.Topic(miss)
		wantErr(t, err, `Kafka topic "`+miss+`" not found`)
	}
}

// TestTopicParameterOrder proves parameters come in address order, so the
// run treats them in a stable order.
func TestTopicParameterOrder(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  staged:
    address: '{zone}.{app}.{env}'
    messages: {paid: {payload: {type: object}}}
`)
	var got []string
	for _, p := range readTopic(t, doc, "z.a.e").Parameters {
		got = append(got, p.Name+"="+p.Value)
	}
	if want := []string{"zone=z", "app=a", "env=e"}; !reflect.DeepEqual(got, want) {
		t.Errorf("parameters = %v, want %v", got, want)
	}
}

// TestTopicParameterAmbiguous proves the run stops rather than choosing when
// -topic fills templates in different ways: one template in more than one
// way, or two spec entries each their own way.
func TestTopicParameterAmbiguous(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  pair:
    address: '{a}.{b}'
    messages: {created: {payload: {type: object}}}
`)
	_, err := doc.Topic("x.y.z")
	wantErr(t, err, `Kafka topic "x.y.z"`, "spec entry pair", "{a}.{b}", "more than one way")

	doc = loadSpec(t, head3+`
channels:
  byRegion:
    address: 'orders.{region}'
    messages: {created: {payload: {type: object}}}
  byEnv:
    address: '{env}.eu'
    messages: {paid: {payload: {type: object}}}
`)
	_, err = doc.Topic("orders.eu")
	wantErr(t, err, `Kafka topic "orders.eu"`, "byEnv", "env=orders", "byRegion", "region=eu", "different ways")

	doc = loadSpec(t, head3+`
channels:
  templated:
    address: 'orders.{region}'
    messages: {created: {payload: {type: object}}}
  literal:
    address: orders.eu
    messages: {paid: {payload: {type: object}}}
`)
	_, err = doc.Topic("orders.eu")
	wantErr(t, err, "different ways", "literal", "templated", "region=eu")
}

// TestTopicParameterSameWay proves spec entries filling the template the same
// way all contribute their messages, and share each parameter once.
func TestTopicParameterSameWay(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  created:
    address: 'orders.{region}'
    messages: {created: {payload: {type: object}}}
  updated:
    address: 'orders.{region}'
    parameters: {region: {location: '$message.payload#/region'}}
    messages: {updated: {payload: {type: object}}}
`)
	topic := readTopic(t, doc, "orders.eu")
	if got := names(topic.MessageTypes); got != "created, updated" {
		t.Errorf("Message types = %s, want both entries'", got)
	}
	want := []TopicParameter{{Name: "region", Value: "eu", Location: "$message.payload#/region", Pointer: []string{"region"}}}
	if !reflect.DeepEqual(topic.Parameters, want) {
		t.Errorf("parameters = %+v, want %+v", topic.Parameters, want)
	}
}

// TestTopicParameterEnum proves a declared enum must hold the matched value,
// with the parameter reachable through $refs.
func TestTopicParameterEnum(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  regional:
    address: 'orders.{region}'
    parameters: {$ref: '#/components/x-parameters'}
    messages: {created: {payload: {type: object}}}
components:
  x-parameters:
    region: {$ref: '#/components/parameters/region'}
  parameters:
    region: {enum: [eu, us], description: Region}
`)
	readTopic(t, doc, "orders.eu")
	_, err := doc.Topic("orders.apac")
	want := `Kafka topic "orders.apac": region value apac is not in the parameter's enum [eu, us]`
	if err == nil || err.Error() != want {
		t.Errorf("err = %v, want %q", err, want)
	}
}

// TestTopicParameterLocation proves a payload location reads as a JSON
// Pointer into the Payload, escapes decoded.
func TestTopicParameterLocation(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  regional:
    address: 'orders.{region}'
    parameters:
      region: {location: '$message.payload#/meta/a~1b~0c'}
    messages: {created: {payload: {type: object}}}
`)
	got := readTopic(t, doc, "orders.eu").Parameters
	want := []TopicParameter{{Name: "region", Value: "eu", Location: "$message.payload#/meta/a~1b~0c", Pointer: []string{"meta", "a/b~c"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parameters = %+v, want %+v", got, want)
	}
}

// TestTopicParameterRejectMistakes proves every Topic parameter the run cannot
// honour stops it with its own error.
func TestTopicParameterRejectMistakes(t *testing.T) {
	cases := map[string]struct{ parameters, want string }{
		"header location": {`{tenant: {location: '$message.header#/tenant'}}`,
			"parameter tenant lives in message headers, which the tool does not generate"},
		"whole payload": {`{tenant: {location: '$message.payload'}}`,
			"parameter tenant: location $message.payload names the whole Payload, not a field in it"},
		"root pointer": {`{tenant: {location: '$message.payload#'}}`,
			"parameter tenant: location $message.payload# names the whole Payload, not a field in it"},
		"not an expression": {`{tenant: {location: 'payload.tenant'}}`,
			"parameter tenant: location payload.tenant is not a $message.payload#/... runtime expression"},
		"location not a string": {`{tenant: {location: 7}}`, "parameter tenant: location must be a string"},
		"enum not a list":       {`{tenant: {enum: acme}}`, "parameter tenant: enum must be a list of strings"},
		"parameter not object":  {`{tenant: acme}`, "parameters.tenant: must be an object"},
		"parameters not object": {`[tenant]`, "spec entry orders: parameters: must be an object"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, head3+`
channels:
  orders:
    address: 'orders.{tenant}'
    parameters: `+c.parameters+`
    messages: {created: {payload: {type: object}}}
`)
			_, err := doc.Topic("orders.acme")
			wantErr(t, err, c.want)
		})
	}
}

// TestTopicParameterUnmatchedIgnored proves a declared parameter of a
// template -topic does not fill is never read, so its mistakes do not stop an
// unrelated run.
func TestTopicParameterUnmatchedIgnored(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  orders:
    address: 'orders.{tenant}'
    parameters: {tenant: {location: '$message.header#/tenant'}}
    messages: {created: {payload: {type: object}}}
  payments:
    address: payments
    messages: {paid: {payload: {type: object}}}
`)
	readTopic(t, doc, "payments")
}
