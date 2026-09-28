package wire

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/planting"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// source is a ValueSource that logs its draw and yields a fresh value.
type source struct {
	name  string
	log   *[]string
	value func() any
	err   error
}

func (s source) Value() (any, error) {
	*s.log = append(*s.log, s.name)
	return s.value(), s.err
}

func order() map[string]any { return map[string]any{"id": "x", "region": "xx"} }

// orderPayload is the Payload schema order() conforms to.
func orderPayload() map[string]any {
	return map[string]any{"type": "object", "required": []any{"id", "region"}, "properties": map[string]any{
		"id": map[string]any{"type": "string"}, "region": map[string]any{"type": "string"},
	}}
}

func testSynth() *synth.Synthesizer { return synth.New(1, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) }

// mustPlantings checks the run's Plantings over JSON Schema walks of each
// Message type's Payload and headers schema.
func mustPlantings(t *testing.T, types []asyncapi.MessageType, keyPath string, key map[string]any, params ...asyncapi.TopicParameter) []*planting.Set {
	t.Helper()
	sets, err := Plantings(walksOf(types, key), keyPath, params)
	if err != nil {
		t.Fatalf("Plantings: %v", err)
	}
	return sets
}

func walksOf(types []asyncapi.MessageType, key map[string]any) []planting.MessageType {
	walks := make([]planting.MessageType, len(types))
	for i, mt := range types {
		walks[i] = planting.MessageType{Name: mt.Name, Payload: generator.NewWalk(mt.Payload, key)}
		if mt.Headers != nil {
			walks[i].Headers = generator.NewWalk(mt.Headers, nil)
		}
	}
	return walks
}

// keySource generates Keys from a key schema on the run's Synthesizer.
type keySource struct {
	gen    *generator.Generator
	schema map[string]any
}

func (k keySource) Value() (any, error) { return k.gen.Value(k.schema) }

// TestMixMakesTheWholeMessage proves the Mix generates a message's Payload,
// then its Headers, then its Key - the order the seeded stream has always
// been drawn in - and plants every Planting before the Headers are encoded
// (#108).
func TestMixMakesTheWholeMessage(t *testing.T) {
	var log []string
	types := []asyncapi.MessageType{{Name: "A", Payload: orderPayload(), Headers: tenantHeaders()}}
	key := map[string]any{"type": "string", "format": "uuid"}
	s := testSynth()
	m := &Mix{
		Synth:   s,
		Types:   []ValueSource{source{name: "payload", log: &log, value: func() any { return order() }}},
		Headers: NewHeaderSource(s, types),
		Key:     keySource{gen: generator.New(s), schema: key},
		Plantings: mustPlantings(t, types, "id", key, headerParam("tenant", "acme", "tenant"),
			asyncapi.TopicParameter{Name: "region", Value: "eu", Location: "$message.payload#/region", Pointer: []string{"region"}}),
	}
	g, err := m.Generate()
	if err != nil {
		t.Fatal(err)
	}

	// The same seed, drawn Headers first and Key second, must give this Key.
	replay := testSynth()
	if _, err := generator.New(replay).Value(tenantHeaders()); err != nil {
		t.Fatal(err)
	}
	wantKey, err := generator.New(replay).Value(key)
	if err != nil {
		t.Fatal(err)
	}
	if g.Key != wantKey {
		t.Errorf("Key = %v, want %v: the Key is drawn after the Headers", g.Key, wantKey)
	}
	if want := map[string]any{"id": wantKey, "region": "eu"}; !reflect.DeepEqual(g.Payload, want) {
		t.Errorf("Payload = %v, want %v: the Key and region planted", g.Payload, want)
	}
	if len(g.Headers) == 0 || !reflect.DeepEqual(g.Headers[0], pipeline.Header{Name: "tenant", Value: []byte("acme")}) {
		t.Errorf("Headers = %v, want tenant=acme planted", g.Headers)
	}
}

// TestMixWithoutKey proves a run without a key schema generates messages with
// a null Key and draws nothing for one.
func TestMixWithoutKey(t *testing.T) {
	var log []string
	types := []asyncapi.MessageType{{Name: "A", Payload: orderPayload()}}
	m := &Mix{Synth: testSynth(), Types: []ValueSource{source{name: "payload", log: &log, value: func() any { return order() }}}, Plantings: mustPlantings(t, types, "", nil)}
	g, err := m.Generate()
	if err != nil || g.Key != nil || g.Headers != nil {
		t.Errorf("Generate = %+v, %v; want a null Key and no Headers", g, err)
	}
	if want := []string{"payload"}; !reflect.DeepEqual(log, want) {
		t.Errorf("draws = %v, want %v", log, want)
	}
}

// TestMixKeyErrorAborts proves a Key the key schema cannot produce stops the
// message, with the generator's error.
func TestMixKeyErrorAborts(t *testing.T) {
	var log []string
	boom := errors.New("key schema cannot be honoured")
	types := []asyncapi.MessageType{{Name: "A", Payload: orderPayload()}}
	m := &Mix{Synth: testSynth(),
		Types:     []ValueSource{source{name: "payload", log: &log, value: func() any { return order() }}},
		Key:       source{name: "key", log: &log, value: func() any { return nil }, err: boom},
		Plantings: mustPlantings(t, types, "", nil)}
	if _, err := m.Generate(); !errors.Is(err, boom) {
		t.Errorf("Generate = %v, want the Key generator's error", err)
	}
}

// TestMixPlantsHeaders proves a Topic parameter with a header location has its
// value planted into the Headers of every record of every Message type,
// before they are encoded (#93).
func TestMixPlantsHeaders(t *testing.T) {
	s := testSynth()
	types := []asyncapi.MessageType{{Name: "A", Payload: orderPayload(), Headers: tenantHeaders()}, {Name: "B", Payload: orderPayload(), Headers: tenantHeaders()}}
	var log []string
	m := &Mix{Synth: s,
		Types:     []ValueSource{source{name: "A", log: &log, value: func() any { return order() }}, source{name: "B", log: &log, value: func() any { return order() }}},
		Headers:   NewHeaderSource(s, types),
		Plantings: mustPlantings(t, types, "", nil, headerParam("tenant", "acme", "tenant")),
	}
	for n := 0; n < 20; n++ {
		g, err := m.Generate()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(g.Headers[0], pipeline.Header{Name: "tenant", Value: []byte("acme")}) {
			t.Fatalf("type %d: headers %v, want tenant=acme planted", g.Type, g.Headers)
		}
	}
	if !strings.Contains(strings.Join(log, ","), "A") || !strings.Contains(strings.Join(log, ","), "B") {
		t.Errorf("types drawn %v, want both", log)
	}
}

// TestPlantingsRefuseHeaderLocations proves every header Planting the run
// cannot honour stops it before any record exists, as an Error of -topic.
func TestPlantingsRefuseHeaderLocations(t *testing.T) {
	both := []asyncapi.MessageType{{Name: "A", Payload: orderPayload(), Headers: tenantHeaders()}, {Name: "B", Payload: orderPayload(), Headers: tenantHeaders()}}
	cases := map[string]struct {
		types  []asyncapi.MessageType
		params []asyncapi.TopicParameter
		want   string
	}{
		"a Message type without headers": {[]asyncapi.MessageType{both[0], {Name: "B", Payload: orderPayload()}},
			[]asyncapi.TopicParameter{headerParam("tenant", "acme", "tenant")},
			"Topic parameter tenant: location $message.header#/tenant: in Message type B: B declares no headers"},
		"no Message type declares headers": {[]asyncapi.MessageType{{Name: "A", Payload: orderPayload()}},
			[]asyncapi.TopicParameter{headerParam("tenant", "acme", "tenant")},
			"Topic parameter tenant: location $message.header#/tenant: A declares no headers"},
		"not guaranteed": {both,
			[]asyncapi.TopicParameter{headerParam("trace", "t1", "trace")},
			"Topic parameter trace: location $message.header#/trace: in Message type A:"},
		"value the header refuses": {both,
			[]asyncapi.TopicParameter{headerParam("tenant", "ACME", "tenant")},
			"Topic parameter tenant: value ACME does not conform to the header at $message.header#/tenant: in Message type A:"},
		"two plantings in one header": {both,
			[]asyncapi.TopicParameter{headerParam("tenant", "acme", "tenant"), headerParam("org", "acme", "tenant")},
			"Topic parameters tenant and org plant into the same field"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Plantings(walksOf(c.types, nil), "", c.params)
			var we *Error
			if !errors.As(err, &we) || we.Flag != "topic" || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want a topic error mentioning %q", err, c.want)
			}
		})
	}
}

// tenantHeaders is a headers schema whose tenant header is required, of
// lower-case letters, and whose trace header is optional.
func tenantHeaders() map[string]any {
	return map[string]any{"type": "object", "required": []any{"tenant"}, "properties": map[string]any{
		"tenant": map[string]any{"type": "string", "pattern": "^[a-z]+$"},
		"trace":  map[string]any{"type": "string"},
	}}
}

func headerParam(name, value, pointer string) asyncapi.TopicParameter {
	return asyncapi.TopicParameter{Name: name, Value: value, Location: "$message.header#/" + pointer, Pointer: []string{pointer}, InHeaders: true}
}
